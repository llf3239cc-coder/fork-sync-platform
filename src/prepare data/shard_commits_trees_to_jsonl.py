#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shard commits_trees.json into per-repo JSONL files (one commit per line).

Why:
- commits_trees.json can be very large; json.load may OOM
- Downstream (type classification, PR lag) often needs only specific repos
- Per-repo JSONL allows on-demand reads and natural parallelism/incrementality

Input (default):
- commits_trees.json (from generate_commits_tree.py)

Output (default):
- commits_trees_shards/
    - owner__repo.jsonl
    - ...
    - index.json   # Line counts per repo, generation time, etc.

Dependencies:
- Recommended: pip install ijson
  Without ijson, falls back to json.load (may use lots of memory for large files)
"""

from __future__ import annotations

import argparse
import json
import os
from collections import OrderedDict, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, IO, Optional, Tuple

try:
    import ijson  # type: ignore
except Exception:
    ijson = None


def repo_to_filename(repo_full_name: str) -> str:
    """owner/repo -> owner__repo.jsonl"""
    return repo_full_name.replace("/", "__") + ".jsonl"


class LRUFileWriter:
    """LRU cache for file handles; avoids 'too many open files'."""

    def __init__(self, output_dir: Path, max_open_files: int = 256, encoding: str = "utf-8"):
        self.output_dir = output_dir
        self.max_open_files = max_open_files
        self.encoding = encoding
        self._cache: "OrderedDict[str, IO[str]]" = OrderedDict()

    def _open(self, filename: str) -> IO[str]:
        path = self.output_dir / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        return open(path, "a", encoding=self.encoding, newline="\n")

    def get(self, filename: str) -> IO[str]:
        if filename in self._cache:
            fh = self._cache.pop(filename)
            self._cache[filename] = fh
            return fh

        # Over limit: close oldest
        if len(self._cache) >= self.max_open_files:
            _, old_fh = self._cache.popitem(last=False)
            try:
                old_fh.close()
            except Exception:
                pass

        fh = self._open(filename)
        self._cache[filename] = fh
        return fh

    def close_all(self) -> None:
        for _, fh in self._cache.items():
            try:
                fh.close()
            except Exception:
                pass
        self._cache.clear()


def minimal_commit_view(commit: Dict[str, Any]) -> Dict[str, Any]:
    """Minimal fields for PR lag / type calculation (smaller files)."""
    out = {
        "hash": commit.get("hash"),
        "parents": commit.get("parents", []),
        "author_date": None,
        "committer_date": None,
    }
    author = commit.get("author") or {}
    committer = commit.get("committer") or {}
    if isinstance(author, dict):
        out["author_date"] = author.get("date")
    if isinstance(committer, dict):
        out["committer_date"] = committer.get("date")
    # tree optional
    if "tree" in commit:
        out["tree"] = commit.get("tree")
    return out


def full_commit_view(commit: Dict[str, Any]) -> Dict[str, Any]:
    """Keep all commit info from generate_commits_tree.py (repos excluded; filename encodes it)."""
    # Note: message can be large and significantly increase file size
    keys = ["hash", "author", "committer", "message", "parents", "tree"]
    out: Dict[str, Any] = {}
    for k in keys:
        if k in commit:
            out[k] = commit.get(k)
    return out


def iter_commits_streaming(commits_tree_path: Path):
    """Stream over each commit dict in commits_trees.json."""
    if ijson is None:
        # Fallback: load all at once (large files may use lots of memory)
        with commits_tree_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for family in data.get("families", []) or []:
            for commit in family.get("commits", []) or []:
                if isinstance(commit, dict):
                    yield commit
        return

    with commits_tree_path.open("rb") as f:
        for commit in ijson.items(f, "families.item.commits.item"):
            if isinstance(commit, dict):
                yield commit


def shard_commits(
    commits_tree_path: Path,
    output_dir: Path,
    max_open_files: int,
    minimal: bool,
    target_repos: Optional[set],
) -> Tuple[Dict[str, int], int]:
    """
    Shard: for each commit, write to corresponding repo jsonl per repos field.

    Returns:
        (repo_line_counts, total_lines_written)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    writer = LRUFileWriter(output_dir=output_dir, max_open_files=max_open_files)
    per_repo_counts: Dict[str, int] = defaultdict(int)
    total_lines = 0

    try:
        for idx, commit in enumerate(iter_commits_streaming(commits_tree_path), 1):
            repos = commit.get("repos", []) or []
            if not repos:
                continue

            if target_repos:
                repos = [r for r in repos if r in target_repos]
                if not repos:
                    continue

            payload = minimal_commit_view(commit) if minimal else full_commit_view(commit)
            line = json.dumps(payload, ensure_ascii=False)

            for repo_name in repos:
                filename = repo_to_filename(repo_name)
                fh = writer.get(filename)
                fh.write(line + "\n")
                per_repo_counts[repo_name] += 1
                total_lines += 1

            if idx % 50000 == 0:
                print(f"Progress: {idx} commits processed, {total_lines} lines written")

    finally:
        writer.close_all()

    return dict(per_repo_counts), total_lines


def parse_repo_list(path: Optional[Path]) -> Optional[set]:
    """Read repo list file; one owner/repo per line (# comments, blank lines ignored)."""
    if not path:
        return None
    if not path.exists():
        raise FileNotFoundError(f"Repo list file not found: {path}")
    out = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            out.add(s)
    return out


def main():
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))

    parser = argparse.ArgumentParser(description="Shard commits_trees.json into per-repo JSONL")
    parser.add_argument(
        "--input",
        type=Path,
        default=current_dir / "commits_trees.json",
        help="commits_trees.json path",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=current_dir / "commits_trees_shards",
        help="Output directory (default: commits_trees_shards)",
    )
    parser.add_argument(
        "--max-open-files",
        type=int,
        default=256,
        help="Max open shard files (default 256; too small causes frequent open/close, too large may exhaust handles)",
    )
    parser.add_argument(
        "--minimal",
        action="store_true",
        help="Output minimal fields only (hash/parents/author_date/committer_date[/tree]); smaller files for lag/type",
    )
    parser.add_argument(
        "--repo-list",
        type=Path,
        default=None,
        help="Optional: only write shards for repos in list (one owner/repo per line)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"Input file not found: {args.input}")
        return

    if ijson is None:
        print("ijson not installed; will fall back to json.load (large files may use lots of memory)")
        print("  Recommended: pip install ijson")

    target_repos = parse_repo_list(args.repo_list)

    print("=" * 80)
    print("commits_trees.json -> per-repo JSONL sharding")
    print("=" * 80)
    print(f"Input: {args.input}")
    print(f"Output dir: {args.output_dir}")
    print(f"max_open_files: {args.max_open_files}")
    print(f"minimal: {args.minimal}")
    print(f"repo-list: {args.repo_list if args.repo_list else '(all)'}")
    print()

    counts, total_lines = shard_commits(
        commits_tree_path=args.input,
        output_dir=args.output_dir,
        max_open_files=args.max_open_files,
        minimal=args.minimal,
        target_repos=target_repos,
    )

    index = {
        "generated_at": datetime.now().isoformat(),
        "input": str(args.input),
        "output_dir": str(args.output_dir),
        "minimal": args.minimal,
        "repos": len(counts),
        "total_lines": total_lines,
        "per_repo_lines": counts,
    }
    index_path = args.output_dir / "index.json"
    with index_path.open("w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)

    print()
    print("=" * 80)
    print("Done!")
    print("=" * 80)
    print(f"Sharded repos: {index['repos']}")
    print(f"Total lines written: {index['total_lines']}")
    print(f"Index file: {index_path}")
    print("=" * 80)


if __name__ == "__main__":
    main()


