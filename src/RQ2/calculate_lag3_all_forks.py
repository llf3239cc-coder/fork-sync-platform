#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compute Lag3 for all downloaded forks.

Lag3 computation:
1. Load origin and fork commits
2. Find commits in fork that intersect with origin (by commit hash)
3. Extract committer_date only for those intersecting commits
4. Deduplicate with set (same date = same pull)
5. Convert to list and sort by time
6. Compute time intervals between consecutive dates
7. Filter out intervals < 1 day (keep only >= 1 day)
8. Compute mean, median, min, max
"""

import json
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string (supports multiple formats).

    Args:
        date_str: Date string (e.g. "2019-10-23T20:42:11Z" or "2016-08-10 14:52:56 +1000").

    Returns:
        datetime object, or None if parse fails.
    """
    if not date_str:
        return None

    # Handle UTC "Z" suffix
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")

    # Handle "+1000" timezone (convert to "+10:00")
    import re
    tz_pattern = r'([+-])(\d{4})$'
    match = re.search(tz_pattern, date_str)
    if match:
        sign = match.group(1)
        tz_str = match.group(2)
        hour_offset = tz_str[:2]
        min_offset = tz_str[2:]
        date_str = re.sub(tz_pattern, f'{sign}{hour_offset}:{min_offset}', date_str)
    
    # Try multiple formats
    formats = [
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt
        except ValueError:
            continue
    
    return None


def load_commits_for_repo_from_shard(shards_dir: Path, repo_full_name: str) -> List[Dict[str, Any]]:
    """
    Load all commits for a repo from shard files (supports two formats).

    Args:
        shards_dir: Path to commits_trees_shards directory.
        repo_full_name: Repo name (format: owner/repo).

    Returns:
        List of commit dicts with hash, author_date, committer_date.
    """
    commits = []
    
    if not shards_dir.exists():
        return commits
    
    # Build JSONL path: owner/repo -> owner__repo.jsonl
    jsonl_filename = repo_full_name.replace("/", "__") + ".jsonl"
    jsonl_file = shards_dir / jsonl_filename
    
    if not jsonl_file.exists():
        return commits
    
    try:
        with jsonl_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    obj = json.loads(line)
                    commit_info = {}
                    
                    # commit hash
                    commit_hash = obj.get("hash")
                    if commit_hash:
                        commit_info["hash"] = commit_hash
                    
                    # author_date
                    author_date = obj.get("author_date")
                    if not author_date:
                        author = obj.get("author")
                        if isinstance(author, dict):
                            author_date = author.get("date")
                    if author_date:
                        commit_info["author_date"] = author_date
                    
                    # committer_date
                    committer_date = obj.get("committer_date")
                    if not committer_date:
                        committer = obj.get("committer")
                        if isinstance(committer, dict):
                            committer_date = committer.get("date")
                    if committer_date:
                        commit_info["committer_date"] = committer_date
                    
                    if commit_info:
                        commits.append(commit_info)
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"  Warning: failed to process commit: {e}")
                    continue
    except Exception as e:
        print(f"  Warning: failed to read JSONL {jsonl_file}: {e}")
    
    return commits


def filter_pull_intervals(pull_intervals: List[float]) -> List[float]:
    """
    Filter pull intervals: drop intervals < 1 day.

    Args:
        pull_intervals: List of pull intervals (days).

    Returns:
        Filtered list (only >= 1 day).
    """
    if not pull_intervals:
        return []

    filtered = [interval for interval in pull_intervals if interval >= 1.0]
    
    return filtered


def calculate_lag3_for_fork(
    origin_repo: str,
    fork_repo: str,
    commits_shards_dir: Path
) -> Optional[Dict[str, Any]]:
    """
    Compute Lag3 for a single fork.

    Logic:
    1. Load origin and fork commits
    2. Find fork commits that intersect with origin (by hash)
    3. Extract committer_date only for those
    4. Deduplicate by date (same date = same pull)
    5. Sort by time
    6. Compute intervals between consecutive dates
    7. Filter out intervals < 1 day

    Args:
        origin_repo: Origin repo name.
        fork_repo: Fork repo name.
        commits_shards_dir: Path to commits_trees_shards.

    Returns:
        Lag3 stats dict, or None if not computable.
    """
    origin_commits = load_commits_for_repo_from_shard(commits_shards_dir, origin_repo)
    fork_commits = load_commits_for_repo_from_shard(commits_shards_dir, fork_repo)
    
    if not origin_commits or not fork_commits:
        return None
    
    # Build set of origin commit hashes
    origin_commit_hashes: Set[str] = set()
    for commit in origin_commits:
        commit_hash = commit.get("hash")
        if commit_hash:
            origin_commit_hashes.add(commit_hash)
    
    if not origin_commit_hashes:
        return None
    
    # Find fork commits that intersect with origin; extract committer_date; dedupe by date
    commit_dates_set: Set[datetime] = set()
    for commit in fork_commits:
        commit_hash = commit.get("hash")
        if commit_hash and commit_hash in origin_commit_hashes:
            # Support both formats: {"committer": {"date": "..."}} or {"committer_date": "..."}
            committer_date_str = commit.get("committer_date")
            if not committer_date_str:
                committer = commit.get("committer", {})
                if isinstance(committer, dict):
                    committer_date_str = committer.get("date")
            
            if committer_date_str:
                committer_date_dt = parse_date(committer_date_str)
                if committer_date_dt:
                    commit_dates_set.add(committer_date_dt)
    
    if len(commit_dates_set) < 2:
        return None

    commit_dates = sorted(commit_dates_set)

    # Compute intervals between consecutive commits (days)
    pull_intervals: List[float] = []
    for i in range(1, len(commit_dates)):
        delta = commit_dates[i] - commit_dates[i-1]
        delta_days = delta.total_seconds() / 86400.0
        if delta_days >= 0:
            pull_intervals.append(delta_days)

    if not pull_intervals:
        return None

    filtered_intervals = filter_pull_intervals(pull_intervals)
    
    if not filtered_intervals:
        return None

    filtered_intervals.sort()
    pull_count = len(filtered_intervals)
    lag3_avg = sum(filtered_intervals) / pull_count
    lag3_median = (
        filtered_intervals[pull_count // 2] if pull_count % 2 == 1
        else (filtered_intervals[pull_count // 2 - 1] + filtered_intervals[pull_count // 2]) / 2
    )
    
    return {
        "origin_repo": origin_repo,
        "fork_repo": fork_repo,
        "pull_count": pull_count,
        "pull_count_before_filter": len(pull_intervals),
        "lag3_avg": round(lag3_avg, 2),
        "lag3_median": round(lag3_median, 2),
        "lag3_min": round(min(filtered_intervals), 2),
        "lag3_max": round(max(filtered_intervals), 2),
    }


def process_single_origin_fork(
    args: Tuple[str, str, str]
) -> Optional[Dict[str, Any]]:
    """
    Process a single origin-fork pair (for multiprocessing).

    Args:
        args: (origin_repo, fork_repo, commits_shards_dir_str) tuple.

    Returns:
        Lag3 stats dict, or None.
    """
    origin_repo, fork_repo, commits_shards_dir_str = args
    commits_shards_dir = Path(commits_shards_dir_str)

    try:
        return calculate_lag3_for_fork(origin_repo, fork_repo, commits_shards_dir)
    except Exception as e:
        print(f"  Warning: failed {origin_repo} -> {fork_repo}: {e}")
        return None


def get_all_downloaded_forks(commits_shards_dir: Path, projects_json: Path) -> Dict[str, List[str]]:
    """
    Get all downloaded forks from commits_trees_shards and github_projects.json.

    Args:
        commits_shards_dir: Path to commits_trees_shards.
        projects_json: Path to github_projects.json (origin-fork mapping).

    Returns:
        {origin_repo: [fork1, fork2, ...]}.
    """
    origin_forks_map = defaultdict(list)

    if not commits_shards_dir.exists():
        print(f"Warning: commits_shards_dir does not exist: {commits_shards_dir}")
        return origin_forks_map

    if not projects_json.exists():
        print(f"Warning: projects_json does not exist: {projects_json}")
        return origin_forks_map

    try:
        with projects_json.open("r", encoding="utf-8") as f:
            data = json.load(f)

        repositories = data.get("repositories", [])
        print(f"Read {len(repositories):,} repos from {projects_json}")

        if not repositories:
            print(f"Warning: no 'repositories' field or empty list; top-level keys: {list(data.keys())[:10]}")
            return origin_forks_map

        checked_forks = 0
        downloaded_forks = 0

        for repo_data in repositories:
            original_repo_data = repo_data.get("original_repo", {})
            if isinstance(original_repo_data, dict):
                origin_repo = original_repo_data.get("full_name", "")
            else:
                origin_repo = repo_data.get("full_name", "")
            
            forks = repo_data.get("forks", [])
            
            if origin_repo and forks:
                for fork_data in forks:
                    if isinstance(fork_data, dict):
                        fork_repo = fork_data.get("full_name", "")
                    else:
                        fork_repo = str(fork_data) if fork_data else ""

                    if fork_repo and fork_repo != origin_repo:
                        checked_forks += 1
                        jsonl_filename = fork_repo.replace("/", "__") + ".jsonl"
                        jsonl_file = commits_shards_dir / jsonl_filename
                        if jsonl_file.exists():
                            origin_forks_map[origin_repo].append(fork_repo)
                            downloaded_forks += 1

        print(f"Checked {checked_forks:,} forks; {downloaded_forks:,} downloaded")

    except Exception as e:
        print(f"Warning: failed to read {projects_json}: {e}")
        import traceback
        traceback.print_exc()
    
    return origin_forks_map


def generate_csv_from_json(json_file: Path) -> None:
    """
    Generate CSV from existing JSON (analysis output with 'results' field).

    Args:
        json_file: Path to JSON file.
    """
    if not json_file.exists():
        raise FileNotFoundError(f"JSON file does not exist: {json_file}")

    print(f"Reading JSON: {json_file}")

    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    results = data.get("results", [])
    if not results:
        print(f"Warning: no 'results' in JSON")
        return

    print(f"Found {len(results):,} result records")

    origin_fork_count: Dict[str, int] = defaultdict(int)
    for result in results:
        origin_repo = result.get("origin_repo", "")
        if origin_repo:
            origin_fork_count[origin_repo] += 1
    
    for result in results:
        origin_repo = result.get("origin_repo", "")
        result["origin_fork_count"] = origin_fork_count.get(origin_repo, 0)

    csv_file = json_file.with_suffix(".csv")
    fieldnames = [
        "origin_repo", "fork_repo",
        "pull_count", "pull_count_before_filter",
        "lag3_avg", "lag3_median", "lag3_min", "lag3_max",
        "origin_lag3_threshold", "has_lag", "origin_fork_count"
    ]
    
    try:
        with csv_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        print(f"[OK] CSV saved: {csv_file}")
    except Exception as e:
        print(f"[Error] CSV save failed: {e}")
        import traceback
        traceback.print_exc()


def calculate_all_forks_lag3(
    commits_shards_dir: Path,
    projects_json: Path,
    output_file: Path,
    max_workers: Optional[int] = None
) -> None:
    """
    Compute Lag3 for all downloaded forks.

    Args:
        commits_shards_dir: Path to commits_trees_shards.
        projects_json: Path to github_projects.json.
        output_file: Output JSON path.
        max_workers: Max processes (None = auto).
    """
    print("Getting list of downloaded forks...")
    origin_forks_map = get_all_downloaded_forks(commits_shards_dir, projects_json)

    total_origins = len(origin_forks_map)
    total_forks = sum(len(forks) for forks in origin_forks_map.values())
    print(f"Found {total_origins:,} origins, {total_forks:,} downloaded forks")

    tasks = []
    for origin_repo, forks in origin_forks_map.items():
        jsonl_filename = origin_repo.replace("/", "__") + ".jsonl"
        origin_jsonl_file = commits_shards_dir / jsonl_filename
        if not origin_jsonl_file.exists():
            continue

        for fork_repo in forks:
            tasks.append((origin_repo, fork_repo, str(commits_shards_dir)))

    total_tasks = len(tasks)
    print(f"Processing {total_tasks:,} origin-fork pairs")

    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    use_multiprocessing = max_workers > 1 and total_tasks > 50

    results = []
    processed_count = 0
    success_count = 0

    if use_multiprocessing:
        print(f"Using {max_workers} processes")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_origin_fork, task): task for task in tasks}

            for future in as_completed(futures):
                processed_count += 1
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        success_count += 1

                    if processed_count % 100 == 0:
                        print(f"  Processed {processed_count:,}/{total_tasks:,} pairs ({processed_count*100//total_tasks}%)...")
                except Exception as e:
                    origin_repo, fork_repo, _ = futures[future]
                    print(f"  Warning: failed {origin_repo} -> {fork_repo}: {e}")
                    import traceback
                    traceback.print_exc()
    else:
        print("Using single process")

        for origin_repo, fork_repo, commits_shards_dir_str in tasks:
            processed_count += 1
            try:
                result = calculate_lag3_for_fork(origin_repo, fork_repo, commits_shards_dir)
                if result:
                    results.append(result)
                    success_count += 1

                if processed_count % 100 == 0:
                    print(f"  Processed {processed_count:,}/{total_tasks:,} pairs ({processed_count*100//total_tasks}%)...")
            except Exception as e:
                print(f"  Warning: failed {origin_repo} -> {fork_repo}: {e}")

    # Group by origin; threshold = mean of lag3_avg across forks for that origin
    origin_thresholds: Dict[str, float] = {}
    origin_forks_map: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    
    for result in results:
        origin_repo = result.get("origin_repo", "")
        if origin_repo:
            origin_forks_map[origin_repo].append(result)
    
    for origin_repo, fork_results in origin_forks_map.items():
        lag3_avg_values = [r.get("lag3_avg") for r in fork_results if r.get("lag3_avg") is not None]
        if lag3_avg_values:
            threshold = sum(lag3_avg_values) / len(lag3_avg_values)
            origin_thresholds[origin_repo] = round(threshold, 2)
    
    origin_fork_count: Dict[str, int] = defaultdict(int)
    for result in results:
        origin_repo = result.get("origin_repo", "")
        if origin_repo:
            origin_fork_count[origin_repo] += 1

    for result in results:
        origin_repo = result.get("origin_repo", "")
        threshold = origin_thresholds.get(origin_repo)
        result["origin_lag3_threshold"] = threshold
        result["origin_fork_count"] = origin_fork_count.get(origin_repo, 0)

        lag3_avg = result.get("lag3_avg")
        if lag3_avg is not None and threshold is not None:
            result["has_lag"] = lag3_avg > threshold
        else:
            result["has_lag"] = None
    
    output_data = {
        "analysis_at": datetime.now().isoformat(),
        "total_origin_forks_pairs": total_tasks,
        "processed_pairs": processed_count,
        "successful_pairs": success_count,
        "origin_thresholds": origin_thresholds,
        "results": results
    }
    
    print(f"\nSaving results to: {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"[OK] JSON saved: {output_file}")

    csv_file = output_file.with_suffix(".csv")
    fieldnames = [
        "origin_repo", "fork_repo",
        "pull_count", "pull_count_before_filter",
        "lag3_avg", "lag3_median", "lag3_min", "lag3_max",
        "origin_lag3_threshold", "has_lag", "origin_fork_count"
    ]
    
    try:
        with csv_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            if results:
                writer.writerows(results)
        
        print(f"[OK] CSV saved: {csv_file}")
        if not results:
            print(f"  (CSV has header only; no successful results)")
    except Exception as e:
        print(f"[Error] CSV save failed: {e}")
        import traceback
        traceback.print_exc()

    print(f"\nComputed thresholds for {len(origin_thresholds):,} origins")

    print(f"\n[OK] Analysis done.")
    print(f"  Processed {processed_count:,} origin-fork pairs")
    print(f"  Successful Lag3: {success_count:,}")
    print(f"  JSON: {output_file}")
    print(f"  CSV: {csv_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Compute Lag3 for all downloaded forks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  python calculate_lag3_all_forks.py \\
      --commits-shards-dir commits_trees_shards \\
      --projects-json github_projects.json \\
      --output lag3_all_forks.json \\
      --max-workers 8
        """
    )

    parser.add_argument(
        "--commits-shards-dir",
        type=str,
        default="commits_trees_shards",
        help="Path to commits_trees_shards (default: commits_trees_shards)"
    )

    parser.add_argument(
        "--projects-json",
        type=str,
        default="github_projects_filtered_stars_10.json",
        help="Path to github_projects JSON (default: github_projects_filtered_stars_10.json)"
    )

    parser.add_argument(
        "--output",
        type=str,
        default="lag3_all_forks.json",
        help="Output JSON path (default: lag3_all_forks.json)"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max processes (default: CPU count - 1)"
    )

    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Only generate CSV from existing JSON; skip analysis"
    )

    args = parser.parse_args()

    try:
        if args.csv_only:
            json_file = Path(args.output)
            generate_csv_from_json(json_file)
        else:
            calculate_all_forks_lag3(
                Path(args.commits_shards_dir),
                Path(args.projects_json),
                Path(args.output),
                args.max_workers
            )
    except Exception as e:
        print(f"[Error] Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
