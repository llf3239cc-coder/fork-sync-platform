#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Apply syncability_pipeline logic to commits marked security by keyword_then_deepseek_security.

Unit = (commit, repo): Each commit has 1 origin + N other forks (excluding source fork); unit count = 1 + N.
Per unit: syncable / already_synced / unsyncable, then aggregate.

Sync check: against local clones (origin_repo_path / other_forks_paths):
- File existence, diff context match, optional git apply --check
- already_synced: only (1) commit hash exists in local repo, or (2) file content 100% contains modified lines.
  No 80% match or post-git-apply logic to avoid small-change false positives.
No GitHub API or remote URLs; local clone only.

*_repos names use owner/repo format (matches origin_repo/fork_repo) for GitHub mapping.

Usage:
  python security_syncability_units.py --commits-dir security_commits_results -o security_sync_units_out
  python security_syncability_units.py --csv keyword_deepseek_summary.csv --commits-dir ... -o out --projects github_projects_filtered_stars_10.json
"""

from __future__ import annotations

import json
import os
import sys
from multiprocessing import Process, JoinableQueue, Manager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple
from collections import defaultdict

# Reuse pipeline path resolution
def _get_fork_repo_path(fork_repo: str) -> Optional[Path]:
    repo_path_standard = fork_repo.replace("/", os.sep)
    repo_path_underscore = fork_repo.replace("/", "__")
    base_dir = os.getenv("FORK_REPOS_BASE_DIR", "")
    if base_dir:
        for p in (repo_path_standard, repo_path_underscore):
            candidate = Path(base_dir) / p
            if candidate.exists() and (candidate / ".git").exists():
                return candidate
    for p in (repo_path_underscore, repo_path_standard):
        candidate = Path("cloned_repos") / p
        if candidate.exists() and (candidate / ".git").exists():
            return candidate
    return None


def _load_origin_forks(projects_json: Path) -> Dict[str, List[str]]:
    """origin -> [fork1, fork2, ...]"""
    out: Dict[str, List[str]] = defaultdict(list)
    if not projects_json.exists():
        return dict(out)
    with open(projects_json, "r", encoding="utf-8") as f:
        data = json.load(f)
    for repo in data.get("repositories", []) or []:
        orig = repo.get("original_repo", {})
        origin = orig.get("full_name")
        if not origin:
            continue
        for fork in repo.get("forks", []) or []:
            fn = fork.get("full_name")
            if fn:
                out[origin].append(fn)
    return dict(out)


def _is_security(d: Dict[str, Any]) -> bool:
    v = d.get("deepseek_security")
    if v is True:
        return True
    if isinstance(v, str) and str(v).strip().lower() in ("true", "1"):
        return True
    return False


def _normalize_repo_name_for_output(name: str) -> str:
    """Stage 2 repo_name often is repo_path.name (e.g. owner__repo). Normalize to owner/repo for GitHub mapping."""
    if not name:
        return name
    return name.replace("__", "/")


def _commit_filename(commit: Dict[str, Any]) -> str:
    origin = (commit.get("origin_repo") or "unknown_origin").replace("/", "__")
    fork = (commit.get("fork_repo") or "unknown_fork").replace("/", "__")
    h = (
        commit.get("commit_hash") or commit.get("item_id") or commit.get("sha")
        or commit.get("hash") or (commit.get("raw_data") or {}).get("hash")
        or (commit.get("raw_data") or {}).get("sha")
        or "unknown"
    )
    return f"{origin}__{fork}__{str(h)[:64]}.json"


def _normalize_for_pipeline(commit: Dict[str, Any]) -> Dict[str, Any]:
    """Convert to pipeline commit format (hash, diff, files, message, etc.)."""
    raw = commit.get("raw_data") or {}
    msg = (
        raw.get("message") or raw.get("commit_message")
        or commit.get("message") or commit.get("commit_message") or ""
    )
    diff = raw.get("diff") or raw.get("patch") or commit.get("diff") or ""
    files = commit.get("files_changed") or raw.get("files") or raw.get("files_changed") or []
    if isinstance(files, list) and files and isinstance(files[0], dict):
        files = [x.get("path") or x.get("filename") or str(x) for x in files]
    h = (
        commit.get("commit_hash") or commit.get("item_id") or commit.get("sha")
        or commit.get("hash") or raw.get("hash") or raw.get("sha") or raw.get("id") or "unknown"
    )
    skip = {"keyword_matched", "deepseek_security", "deepseek_reason", "deepseek_error",
            "fork_repo", "origin_repo", "hash", "sha", "id", "message", "commit_message",
            "diff", "patch", "files", "files_changed", "raw_data"}
    extra = {k: v for k, v in commit.items() if k not in skip}
    pipeline_commit = {
        "fork_repo": commit.get("fork_repo") or "unknown",
        "origin_repo": commit.get("origin_repo") or "unknown",
        "hash": h,
        "sha": h,
        "id": h,
        "message": msg,
        "commit_message": msg,
        "diff": diff,
        "patch": diff,
        "files": files,
        "files_changed": files,
        "raw_data": raw,
        "author": raw.get("author") or commit.get("author") or {},
        **extra,
    }
    return pipeline_commit


def _collect_security_fnames(csv_path: Optional[Path]) -> Set[str]:
    """Collect commit filenames with deepseek_security=True from CSV."""
    security_fnames: Set[str] = set()
    if not csv_path or not csv_path.exists():
        return security_fnames
    import csv as csv_module
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        r = csv_module.DictReader(f)
        for row in r:
            if _is_security(row):
                o = (row.get("origin_repo") or "unknown").replace("/", "__")
                fr = (row.get("fork_repo") or "unknown").replace("/", "__")
                cid = (row.get("commit_id") or "unknown")[:64]
                security_fnames.add(f"{o}__{fr}__{cid}.json")
    return security_fnames


def _iter_security_commits_stream(
    commits_dir: Path,
    csv_path: Optional[Path],
    limit: Optional[int],
) -> Iterator[Tuple[Path, Dict[str, Any]]]:
    """
    Stream commits_dir; read one JSON, check security; yield (path, commit) if pass.
    If limit set, yield at most limit items.
    """
    security_fnames = _collect_security_fnames(csv_path)
    n = 0
    if not commits_dir.exists():
        return
    paths = sorted(commits_dir.glob("*.json"), key=lambda p: p.name)
    for p in paths:
        if "__" not in p.name or p.name.startswith("."):
            continue
        try:
            with open(p, "r", encoding="utf-8") as f:
                d = json.load(f)
        except Exception:
            continue
        if not _is_security(d):
            continue
        if security_fnames and p.name not in security_fnames:
            continue
        yield (p, d)
        n += 1
        if limit is not None and limit > 0 and n >= limit:
            return


def _collect_security_commits(commits_dir: Path, csv_path: Optional[Path]) -> List[Tuple[Path, Dict[str, Any]]]:
    """Collect deepseek_security=True commits from commits_dir JSON/CSV. Returns [(path, commit), ...]."""
    return list(_iter_security_commits_stream(commits_dir, csv_path, limit=None))


def _process_one_commit(
    path: Path,
    commit: Dict[str, Any],
    work_dir: Path,
    config: Dict[str, Any],
    origin_forks: Dict[str, List[str]],
    start_stage: int,
    end_stage: int,
) -> Dict[str, Any]:
    """Process single security commit; return per_commit stats."""
    origin = commit.get("origin_repo") or "unknown"
    fork = commit.get("fork_repo") or "unknown"
    other_forks = [f for f in (origin_forks.get(origin) or []) if f != fork]
    origin_path = _get_fork_repo_path(origin)
    other_paths: List[str] = []
    for f in other_forks:
        fp = _get_fork_repo_path(f)
        if fp:
            other_paths.append(str(fp))

    payload = _normalize_for_pipeline(commit)
    payload["origin_repo_path"] = str(origin_path) if origin_path else ""
    payload["other_forks_paths"] = other_paths

    res = _run_pipeline_for_commit(payload, work_dir, config, start_stage=start_stage, end_stage=end_stage)
    units = syncable = already_synced = unsyncable = 0
    syncable_repos: List[str] = []
    already_synced_repos: List[str] = []
    unsyncable_repos: List[str] = []
    if res:
        s2 = res.get("stage2_result") or {}
        for r in (s2.get("repos_checked") or []):
            repo_name = (r.get("repo_name") or "").strip()
            if not repo_name:
                continue
            norm_repo = repo_name.replace("__", "/")
            norm_fork = fork.replace("__", "/")
            if norm_repo == norm_fork:
                continue  # Skip source fork, same as pipeline analyze
            st = (r.get("sync_status") or "").strip().lower()
            # Path missing but already_synced -> unsyncable (consistent with pipeline fix)
            repo_path_str = r.get("repo_path") or ""
            if repo_path_str and st == "already_synced":
                if not Path(repo_path_str).exists():
                    st = "unsyncable"
            units += 1
            out_name = _normalize_repo_name_for_output(repo_name)
            if st == "syncable":
                syncable += 1
                syncable_repos.append(out_name)
            elif st == "already_synced":
                already_synced += 1
                already_synced_repos.append(out_name)
            else:
                unsyncable += 1
                unsyncable_repos.append(out_name)

    return {
        "commit_file": path.name,
        "origin_repo": origin,
        "fork_repo": fork,
        "units": units,
        "syncable": syncable,
        "already_synced": already_synced,
        "unsyncable": unsyncable,
        "syncable_repos": syncable_repos,
        "already_synced_repos": already_synced_repos,
        "unsyncable_repos": unsyncable_repos,
    }


def _write_commit_result(rec: Dict[str, Any], results_dir: Path) -> None:
    """Write commit result to results_dir/<commit_file>.json immediately after processing."""
    fname = rec.get("commit_file") or "unknown.json"
    out_path = results_dir / fname
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _worker_process(
    task_queue: JoinableQueue,
    result_list: List[Dict[str, Any]],
    work_dir_str: str,
    results_dir_str: str,
    config: Dict[str, Any],
    origin_forks: Dict[str, List[str]],
    start_stage: int,
    end_stage: int,
) -> None:
    """
    Worker: take (path, commit) from task_queue, process, append to result_list,
    write to results_dir/<commit_file>.json. Exit on None.
    """
    work_dir = Path(work_dir_str)
    results_dir = Path(results_dir_str)
    while True:
        item = task_queue.get()
        try:
            if item is None:
                return
            path_str, commit = item
            path = Path(path_str)
            rec = _process_one_commit(path, commit, work_dir, config, origin_forks, start_stage, end_stage)
            result_list.append(rec)
            _write_commit_result(rec, results_dir)
        finally:
            task_queue.task_done()


def _run_pipeline_for_commit(
    commit_payload: Dict[str, Any],
    work_dir: Path,
    config: Dict[str, Any],
    start_stage: int = 1,
    end_stage: int = 2,
) -> Optional[Dict[str, Any]]:
    """Run pipeline (Stage 1-2) for single commit; return to_dict(); None if failed or not reached Stage2."""
    try:
        from syncability_pipeline import SyncabilityPipeline
    except ImportError:
        return None

    fname = _commit_filename(commit_payload)
    commit_file = work_dir / "input" / (fname or "commit.json")
    commit_file.parent.mkdir(parents=True, exist_ok=True)
    with open(commit_file, "w", encoding="utf-8") as f:
        json.dump(commit_payload, f, ensure_ascii=False, indent=2)

    try:
        res = SyncabilityPipeline.process_single_commit_file(
            commit_file,
            work_dir=work_dir,
            config=config,
            start_stage=start_stage,
            end_stage=end_stage,
            origin_to_metadata=None,
        )
    except Exception:
        res = None
    return res


def _default_config(enable_git_apply: bool = False) -> Dict[str, Any]:
    return {
        "stage0": {},
        "stage1": {},
        "stage2": {
            "origin_repo_path": "",
            "other_forks_paths": [],
            "enable_classification": False,
            "enable_git_apply": enable_git_apply,
        },
    }


def _load_existing_summary(output_dir: Path) -> Tuple[List[Dict[str, Any]], Set[str]]:
    """Load existing summary; return (per_commit list, set of existing commit_files)."""
    out_json = output_dir / "security_sync_units_summary.json"
    if not out_json.exists():
        return [], set()
    try:
        with open(out_json, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return [], set()
    per = data.get("per_commit") or []
    names = {r.get("commit_file") or "" for r in per}
    return per, names


def run(
    commits_dir: Path,
    output_dir: Path,
    *,
    csv_path: Optional[Path] = None,
    projects_json: Optional[Path] = None,
    limit: Optional[int] = None,
    start_stage: int = 1,
    end_stage: int = 2,
    enable_git_apply: bool = False,
    workers: int = 10,
) -> Dict[str, Any]:
    """
    Stream security commits (read one, process one). Start N workers; each takes next, processes, repeat.
    On re-run, skip commits with existing results in output_dir/results/, merge into final summary.
    """
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    work_dir = output_dir / "pipeline_work"
    work_dir.mkdir(parents=True, exist_ok=True)
    results_dir = output_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    # Already done (existing JSON in results dir counts as run)
    already_done: Set[str] = set()
    if results_dir.exists():
        already_done = {f.name for f in results_dir.glob("*.json")}
    if already_done:
        print(f"Re-run: {len(already_done)} commits already have results, will skip")

    projects = Path(projects_json or "github_projects_filtered_stars_10.json")
    origin_forks = _load_origin_forks(projects)
    print(f"Origin-forks mapping: {len(origin_forks)} origins")

    config = _default_config(enable_git_apply=enable_git_apply)
    n_workers = max(1, int(workers))
    stream = _iter_security_commits_stream(commits_dir, csv_path, limit)
    total_units = total_syncable = total_already_synced = total_unsyncable = 0
    per_commit: List[Dict[str, Any]] = []
    n_skipped = 0

    if n_workers <= 1:
        print("Mode: stream, single-process (read one, process one)")
        n_commits = 0
        for path, c in stream:
            if path.name in already_done:
                n_skipped += 1
                continue
            rec = _process_one_commit(path, c, work_dir, config, origin_forks, start_stage, end_stage)
            per_commit.append(rec)
            _write_commit_result(rec, results_dir)
            total_units += rec["units"]
            total_syncable += rec["syncable"]
            total_already_synced += rec["already_synced"]
            total_unsyncable += rec["unsyncable"]
            n_commits += 1
            if n_commits % 10 == 0 or n_commits == 1:
                print(f"  Processed {n_commits} | units {total_units} | syncable {total_syncable} | already_synced {total_already_synced} | unsyncable {total_unsyncable}")
    else:
        print(f"Mode: stream, {n_workers} workers, linear (enqueue one, worker takes one, processes one)")
        task_queue: JoinableQueue = JoinableQueue()
        with Manager() as mgr:
            result_list = mgr.list()
            procs: List[Process] = []
            for _ in range(n_workers):
                p = Process(
                    target=_worker_process,
                    args=(task_queue, result_list, str(work_dir), str(results_dir), config, origin_forks, start_stage, end_stage),
                )
                p.start()
                procs.append(p)

            n_commits = 0
            for path, c in stream:
                if path.name in already_done:
                    n_skipped += 1
                    continue
                task_queue.put((str(path), c))
                n_commits += 1
                if n_commits % 100 == 0 or n_commits == 1:
                    print(f"  Enqueued {n_commits} security commits...")

            for _ in range(n_workers):
                task_queue.put(None)
            task_queue.join()
            for p in procs:
                p.join()

            per_commit = list(result_list)
            for rec in per_commit:
                total_units += rec["units"]
                total_syncable += rec["syncable"]
                total_already_synced += rec["already_synced"]
                total_unsyncable += rec["unsyncable"]

    if n_skipped:
        print(f"  Skipped already done: {n_skipped} commits")

    # Merge existing summary (from previous runs)
    existing_per, existing_names = _load_existing_summary(output_dir)
    if existing_per:
        # Newly processed commit_files this run (avoid duplicates)
        new_names = {r.get("commit_file") or "" for r in per_commit}
        # Keep existing not overwritten + all from this run
        merged_per = [r for r in existing_per if (r.get("commit_file") or "") not in new_names] + per_commit
        per_commit = merged_per
        total_units = total_syncable = total_already_synced = total_unsyncable = 0
        for rec in per_commit:
            total_units += rec["units"]
            total_syncable += rec["syncable"]
            total_already_synced += rec["already_synced"]
            total_unsyncable += rec["unsyncable"]
        print(f"  Merged: {len(existing_per)} existing + {len(new_names)} new = {len(per_commit)} total")

    n_commits = len(per_commit)
    if n_commits == 0 and not existing_per:
        print("No security commits, exiting")
        return {"security_commits_count": 0, "total_units": 0, "syncable": 0, "already_synced": 0, "unsyncable": 0, "per_commit": []}

    summary = {
        "security_commits_count": n_commits,
        "total_units": total_units,
        "syncable": total_syncable,
        "already_synced": total_already_synced,
        "unsyncable": total_unsyncable,
        "per_commit": per_commit,
    }
    out_json = output_dir / "security_sync_units_summary.json"
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print()
    print("Summary (unit = (commit, repo), origin + other forks excluding self):")
    print(f"  Security commits: {n_commits}")
    print(f"  Total units: {total_units}")
    print(f"  Syncable: {total_syncable}")
    print(f"  Already synced: {total_already_synced}")
    print(f"  Unsyncable: {total_unsyncable}")
    print(f"  Summary: {out_json}")
    print(f"  Per-commit results: {results_dir} (<commit_file>.json)")
    return summary


def main() -> int:
    import argparse
    ap = argparse.ArgumentParser(description="Security commits -> syncability pipeline -> per (commit,repo) unit stats: syncable/already_synced/unsyncable")
    ap.add_argument("--commits-dir", type=str, required=True, help="keyword_then_deepseek commits dir (JSON with deepseek_security)")
    ap.add_argument("-o", "--output-dir", type=str, required=True, help="Output directory")
    ap.add_argument("--csv", type=str, default=None, help="Optional: keyword_deepseek summary CSV; only process security rows")
    ap.add_argument("--projects", type=str, default=None, help="origin->forks mapping JSON (default github_projects_filtered_stars_10.json)")
    ap.add_argument("--limit", type=int, default=None, help="Max security commits to process")
    ap.add_argument("--start-stage", type=int, default=1, help="Pipeline start stage")
    ap.add_argument("--end-stage", type=int, default=2, help="Pipeline end stage")
    ap.add_argument("--full-check", action="store_true", help="Enable full check (e.g. git apply)")
    ap.add_argument("--workers", type=int, default=10, help="Worker processes (default 10); 1 = single-process stream")
    args = ap.parse_args()

    commits_dir = Path(args.commits_dir)
    if not commits_dir.exists():
        print(f"Error: commits dir not found: {commits_dir}")
        return 1
    run(
        commits_dir,
        Path(args.output_dir),
        csv_path=Path(args.csv) if args.csv else None,
        projects_json=Path(args.projects) if args.projects else None,
        limit=args.limit,
        start_stage=args.start_stage,
        end_stage=args.end_stage,
        enable_git_apply=args.full_check,
        workers=args.workers,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
