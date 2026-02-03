#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Shard large repos_prs.json into per-repo JSONL to avoid downstream scripts (e.g. pr_lag_from_json.py)
loading the whole file with json.load and exhausting memory.

Input (large file):
- repos_prs.json

Output directory:
- repos_prs_shards/
  - owner__repo.jsonl   (one PR per line, minimal fields)
  - index.json          (line counts per shard)

Example line (minimal fields, sufficient for pr_lag_from_json.py):
{"number": 123, "url": "...", "created_at": "...", "merged_at": "...", "closed_at": "..."}
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, Any


def repo_to_filename(repo_full_name: str) -> str:
    return repo_full_name.replace("/", "__") + ".jsonl"


def minimize_pr(pr: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "number": pr.get("number"),
        "url": pr.get("url") or pr.get("html_url"),
        "created_at": pr.get("created_at"),
        "merged_at": pr.get("merged_at"),
        "closed_at": pr.get("closed_at"),
        # For local PR->commits (merge-base needs base_ref)
        "base_ref": pr.get("base_ref") or pr.get("base") or (pr.get("base") or {}).get("ref"),
        # Optional: for Lag3 or contribution source
        "head_repo_full_name": pr.get("head_repo_full_name"),
        # Keep commits for extracting author_date to compute Lag1
        "commits": pr.get("commits"),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Shard repos_prs.json -> repos_prs_shards/*.jsonl")
    ap.add_argument("--input", type=str, default="repos_prs.json")
    ap.add_argument("--out-dir", type=str, default="repos_prs_shards")
    ap.add_argument("--repo-limit", type=int, default=0, help="Process only first N repos (0 = no limit)")
    args = ap.parse_args()

    inp = Path(args.input)
    out_dir = Path(args.out_dir)
    if not inp.exists():
        print(f"Input file not found: {inp}")
        return 2
    out_dir.mkdir(parents=True, exist_ok=True)

    try:
        import ijson  # type: ignore
    except Exception as e:
        print(f"ijson required for streaming large JSON: {e}")
        print("Run: python -m pip install ijson")
        return 2

    index: Dict[str, int] = {}
    repos_seen = 0
    prs_seen = 0

    with inp.open("rb") as f:
        for repo in ijson.items(f, "repositories.item"):
            repos_seen += 1
            repo_full_name = repo.get("full_name")
            prs = repo.get("prs") or []
            if not repo_full_name or not prs:
                if args.repo_limit and repos_seen >= args.repo_limit:
                    break
                continue

            shard_path = out_dir / repo_to_filename(repo_full_name)
            cnt = 0
            with shard_path.open("w", encoding="utf-8") as wf:
                for pr in prs:
                    wf.write(json.dumps(minimize_pr(pr), ensure_ascii=False) + "\n")
                    cnt += 1
                    prs_seen += 1
            index[repo_full_name] = cnt

            if repos_seen % 50 == 0:
                print(f"Progress: repos={repos_seen} prs={prs_seen} last={repo_full_name} lines={cnt}")

            if args.repo_limit and repos_seen >= args.repo_limit:
                break

    (out_dir / "index.json").write_text(json.dumps(index, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=" * 80)
    print("Done!")
    print(f"Input: {inp} ({round(inp.stat().st_size/1024/1024,2)} MB)")
    print(f"Output dir: {out_dir}")
    print(f"Repos: {len(index)}")
    print(f"Total PR lines: {sum(index.values())}")
    print("=" * 80)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


