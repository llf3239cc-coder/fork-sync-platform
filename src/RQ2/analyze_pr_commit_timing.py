#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze commits in sync_prs_fork_analysis_filtered.json:
1. Analyze at commit granularity
2. Associate each commit with its PR
3. Compute time difference between last commit date in other forks and this commit's date in origin
4. Save both timestamps
"""

import json
import csv
import argparse
import re
import copy
import multiprocessing
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed


def parse_date(date_str: str) -> Optional[datetime]:
    """
    Parse date string (supports multiple formats).

    Args:
        date_str: date string (e.g. "2019-10-23T20:42:11Z" or "2016-08-10 14:52:56 +1000")

    Returns:
        datetime object, or None on parse failure
    """
    if not date_str:
        return None
    if date_str.endswith("Z"):
        date_str = date_str.replace("Z", "+00:00")
    tz_pattern = r'([+-])(\d{4})$'
    match = re.search(tz_pattern, date_str)
    if match:
        sign = match.group(1)
        tz_str = match.group(2)
        hour_offset = tz_str[:2]
        min_offset = tz_str[2:]
        date_str = re.sub(tz_pattern, f'{sign}{hour_offset}:{min_offset}', date_str)
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


def load_commit_info_from_jsonl(
    repo_name: str,
    commit_shas: Set[str],
    commits_shards_dir: Path
) -> Dict[str, Dict[str, datetime]]:
    """
    Load date info (author_date, committer_date) for given commits from JSONL.

    Args:
        repo_name: repo name (owner/repo)
        commit_shas: set of commit SHAs to look up
        commits_shards_dir: path to commits_trees_shards

    Returns:
        {commit_sha: {"author_date": datetime, "committer_date": datetime}} for found commits only
    """
    commit_info = {}
    
    if not commits_shards_dir.exists():
        return commit_info
    jsonl_filename = repo_name.replace("/", "__") + ".jsonl"
    jsonl_file = commits_shards_dir / jsonl_filename
    
    if not jsonl_file.exists():
        return commit_info
    
    try:
        with jsonl_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    commit_data = json.loads(line)
                    commit_hash = commit_data.get("hash")
                    
                    if commit_hash in commit_shas:
                        commit_dates = {}
                        author = commit_data.get("author", {})
                        author_date_str = author.get("date") if isinstance(author, dict) else None
                        if not author_date_str:
                            author_date_str = commit_data.get("author_date")
                        
                        if author_date_str:
                            author_date = parse_date(author_date_str)
                            if author_date:
                                commit_dates["author_date"] = author_date
                        committer = commit_data.get("committer", {})
                        committer_date_str = committer.get("date") if isinstance(committer, dict) else None
                        if not committer_date_str:
                            committer_date_str = commit_data.get("committer_date")
                        
                        if committer_date_str:
                            committer_date = parse_date(committer_date_str)
                            if committer_date:
                                commit_dates["committer_date"] = committer_date
                        
                        if commit_dates:
                            commit_info[commit_hash] = commit_dates
                        if len(commit_info) == len(commit_shas):
                            break
                
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    print(f"  Warning: failed to process commit: {e}")
                    continue
    except Exception as e:
        print(f"  Warning: failed to read JSONL {jsonl_file}: {e}")
    
    return commit_info


def find_pr_file(origin_repo: str, pr_number: str, pr_classification_dir: Path) -> Optional[Path]:
    """
    Find PR classification result file.

    Args:
        origin_repo: origin repo name (owner/repo)
        pr_number: PR number
        pr_classification_dir: path to pr_classification_results

    Returns:
        PR file path, or None if not found
    """
    if not pr_classification_dir.exists():
        return None
    repo_part = origin_repo.replace("/", "__")
    pr_filename = f"{repo_part}__PR_{pr_number}.json"
    for repo_dir in pr_classification_dir.iterdir():
        if not repo_dir.is_dir():
            continue
        pr_file = repo_dir / pr_filename
        if pr_file.exists():
            return pr_file
        for pr_file in repo_dir.glob(f"*__PR_{pr_number}.json"):
            return pr_file
    
    return None


def get_pr_merged_at(origin_repo: str, pr_number: str, pr_classification_dir: Path) -> Optional[datetime]:
    """
    Get PR merge time from PR classification result file.

    Args:
        origin_repo: origin repo name
        pr_number: PR number
        pr_classification_dir: path to pr_classification_results

    Returns:
        PR merge time (datetime), or None if not found
    """
    pr_file = find_pr_file(origin_repo, pr_number, pr_classification_dir)
    
    if not pr_file:
        return None
    
    try:
        with pr_file.open("r", encoding="utf-8") as f:
            pr_data = json.load(f)
        
        pr = pr_data.get("pr", {})
        merged_at_str = pr.get("merged_at")
        
        if merged_at_str:
            return parse_date(merged_at_str)
    
    except Exception as e:
        print(f"  Warning: failed to read PR file {pr_file}: {e}")
    return None


def process_single_pr_commits(args: Tuple[Dict[str, Any], str, str]) -> List[Dict[str, Any]]:
    """
    Process timing for all commits of a single PR (for multiprocessing).

    Args:
        args: (result, commits_shards_dir_str, pr_classification_dir_str)

    Returns:
        List of commit analysis result dicts
    """
    result, commits_shards_dir_str, pr_classification_dir_str = args
    commits_shards_dir = Path(commits_shards_dir_str)
    pr_classification_dir = Path(pr_classification_dir_str)
    
    return analyze_pr_commits(result, commits_shards_dir, pr_classification_dir)


def analyze_pr_commits(
    result: Dict[str, Any],
    commits_shards_dir: Path,
    pr_classification_dir: Path
) -> List[Dict[str, Any]]:
    """
    Analyze timing for all commits of a single PR.

    Args:
        result: PR analysis result (from sync_prs_fork_analysis_filtered.json)
        commits_shards_dir: path to commits_trees_shards
        pr_classification_dir: path to pr_classification_results

    Returns:
        List of commit analysis result dicts (one per commit)
    """
    pr_id = result.get("pr_id", "")
    origin_repo = result.get("origin_repo", "")
    fork_repo = result.get("fork_repo", "")
    pr_number = result.get("pr_number", "")
    pr_url = result.get("pr_url", "")
    pr_title = result.get("pr_title", "")
    fork_commit_details = result.get("fork_commit_details", {})
    all_commits = set()
    for fork_name, details in fork_commit_details.items():
        commit_shas = details.get("commits", [])
        if commit_shas:
            all_commits.update(commit_shas)
    
    if not all_commits:
        return []
    origin_commit_info = load_commit_info_from_jsonl(
        origin_repo,
        all_commits,
        commits_shards_dir
    )
    fork_commit_info_map = {}
    for fork_name, details in fork_commit_details.items():
        if fork_name == origin_repo:
            continue
        
        commit_shas = set(details.get("commits", []))
        if commit_shas:
            commit_info = load_commit_info_from_jsonl(
                fork_name,
                commit_shas,
                commits_shards_dir
            )
            fork_commit_info_map[fork_name] = commit_info
    commit_results = []
    for commit_sha in all_commits:
        origin_info = origin_commit_info.get(commit_sha, {})
        commit_author_date = origin_info.get("author_date")
        origin_committer_date = origin_info.get("committer_date")
        fork_commit_dates = {}
        for fork_name, commit_info_map in fork_commit_info_map.items():
            if commit_sha in commit_info_map:
                fork_info = commit_info_map[commit_sha]
                fork_committer_date = fork_info.get("committer_date")
                if fork_committer_date:
                    fork_commit_dates[fork_name] = fork_committer_date
        fork_committer_dates_list = list(fork_commit_dates.values())
        last_fork_committer_date = max(fork_committer_dates_list) if fork_committer_dates_list else None
        time_diff_days = None
        time_diff_seconds = None
        if commit_author_date and last_fork_committer_date:
            time_diff = commit_author_date - last_fork_committer_date
            time_diff_days = time_diff.total_seconds() / 86400.0
            time_diff_seconds = time_diff.total_seconds()
        fork_commit_dates_str = {
            fork_name: date.isoformat()
            for fork_name, date in fork_commit_dates.items()
        }
        
        commit_result = {
            "commit_sha": commit_sha,
            "pr_id": pr_id,
            "origin_repo": origin_repo,
            "fork_repo": fork_repo,
            "pr_number": pr_number,
            "pr_url": pr_url,
            "pr_title": pr_title,
            "commit_author_date": commit_author_date.isoformat() if commit_author_date else None,
            "origin_committer_date": origin_committer_date.isoformat() if origin_committer_date else None,
            "fork_commit_dates": fork_commit_dates_str,  # {fork_name: committer_date_iso_string}
            "fork_repos": list(fork_commit_dates_str.keys()),
            "last_fork_committer_date": last_fork_committer_date.isoformat() if last_fork_committer_date else None,
            "time_diff_days": round(time_diff_days, 2) if time_diff_days is not None else None,
            "time_diff_seconds": int(time_diff_seconds) if time_diff_seconds is not None else None,
        }
        
        commit_results.append(commit_result)
    
    return commit_results


def generate_csv_from_json(json_file: Path) -> None:
    """
    Generate CSV from existing JSON file.

    Args:
        json_file: path to JSON file
    """
    if not json_file.exists():
        raise FileNotFoundError(f"JSON file not found: {json_file}")
    print(f"Reading JSON: {json_file}")
    with open(json_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    all_commit_results = data.get("commits", [])
    if not all_commit_results:
        print("No commits in JSON file")
        return
    print(f"Found {len(all_commit_results):,} commit records")
    csv_file = json_file.with_suffix(".csv")
    csv_rows = []
    for commit_result in all_commit_results:
        csv_row = commit_result.copy()
        fork_dates = csv_row.get("fork_commit_dates", {})
        if fork_dates:
            csv_row["fork_commit_dates"] = "; ".join([f"{fork_name}:{date}" for fork_name, date in fork_dates.items()])
        else:
            csv_row["fork_commit_dates"] = ""
        fork_repos = csv_row.get("fork_repos", [])
        if fork_repos:
            csv_row["fork_repos"] = "; ".join(fork_repos)
        else:
            csv_row["fork_repos"] = ""
        csv_rows.append(csv_row)
    
    fieldnames = [
        "commit_sha", "pr_id", "origin_repo", "fork_repo", "pr_number",
        "pr_url", "pr_title",
        "commit_author_date", "origin_committer_date",
        "fork_repos", "fork_commit_dates", "last_fork_committer_date",
        "time_diff_days", "time_diff_seconds"
    ]
    
    with csv_file.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)
    
    print(f"CSV saved to: {csv_file}")


def analyze_all_prs(
    input_file: Path,
    output_file: Path,
    commits_shards_dir: Path,
    pr_classification_dir: Path,
    max_workers: Optional[int] = None
) -> None:
    """
    Analyze commit timing for all PRs; output commit-level result file (multiprocessing supported).

    Args:
        input_file: path to sync_prs_fork_analysis_filtered.json
        output_file: path to output JSON (new file)
        commits_shards_dir: path to commits_trees_shards
        pr_classification_dir: path to pr_classification_results
        max_workers: max processes (None = auto)
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    print(f"Reading input: {input_file}")
    with open(input_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    results = data.get("results", [])
    total = len(results)
    print(f"Found {total:,} PR results")
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    use_multiprocessing = max_workers > 1 and total > 50
    all_commit_results = []
    processed_count = 0
    success_count = 0
    if use_multiprocessing:
        print(f"Using {max_workers} processes")
        tasks = [
            (copy.deepcopy(result), str(commits_shards_dir), str(pr_classification_dir))
            for result in results
        ]
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(process_single_pr_commits, task): i for i, task in enumerate(tasks)}
            
            for future in as_completed(futures):
                idx = futures[future]
                processed_count += 1
                try:
                    commit_results = future.result()
                    all_commit_results.extend(commit_results)
                    success_count += sum(1 for cr in commit_results if cr.get("time_diff_days") is not None)
                    if processed_count % 100 == 0:
                        print(f"  Processed {processed_count:,}/{total:,} PRs ({processed_count*100//total}%)...")
                except Exception as e:
                    print(f"  Warning: PR processing failed: {e}")
                    import traceback
                    traceback.print_exc()
    else:
        print("Using single process")
        for i, result in enumerate(results):
            if (i + 1) % 100 == 0:
                print(f"  Processed {i + 1}/{total} PRs ({i*100//total}%)...")
            try:
                commit_results = analyze_pr_commits(
                    result,
                    commits_shards_dir,
                    pr_classification_dir
                )
                all_commit_results.extend(commit_results)
                processed_count += 1
                success_count += sum(1 for cr in commit_results if cr.get("time_diff_days") is not None)
            except Exception as e:
                print(f"  Warning: PR {result.get('pr_id', 'unknown')} failed: {e}")
                import traceback
                traceback.print_exc()
                processed_count += 1
    output_data = {
        "source_file": str(input_file),
        "analysis_at": datetime.now().isoformat(),
        "total_prs": total,
        "processed_prs": processed_count,
        "total_commits": len(all_commit_results),
        "commits_with_time_diff": success_count,
        "commits": all_commit_results
    }
    print(f"\nSaving JSON: {output_file}")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    print(f"JSON saved to: {output_file}")
    csv_file = output_file.with_suffix(".csv")
    if all_commit_results:
        csv_rows = []
        for commit_result in all_commit_results:
            csv_row = commit_result.copy()
            fork_dates = csv_row.get("fork_commit_dates", {})
            if fork_dates:
                csv_row["fork_commit_dates"] = "; ".join([f"{fork_name}:{date}" for fork_name, date in fork_dates.items()])
            else:
                csv_row["fork_commit_dates"] = ""
            fork_repos = csv_row.get("fork_repos", [])
            if fork_repos:
                csv_row["fork_repos"] = "; ".join(fork_repos)
            else:
                csv_row["fork_repos"] = ""
            csv_rows.append(csv_row)
        fieldnames = [
            "commit_sha", "pr_id", "origin_repo", "fork_repo", "pr_number",
            "pr_url", "pr_title",
            "commit_author_date", "origin_committer_date",
            "fork_repos", "fork_commit_dates", "last_fork_committer_date",
            "time_diff_days", "time_diff_seconds"
        ]
        
        with csv_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        print(f"CSV saved to: {csv_file}")
    print(f"\nDone.")
    print(f"  Processed {processed_count:,} PRs")
    print(f"  Analyzed {len(all_commit_results):,} commits")
    print(f"  Commits with time diff: {success_count:,}")
    print(f"  JSON: {output_file}")
    print(f"  CSV: {csv_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Analyze commit time diff between other forks and origin; output commit-level result file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analyze_pr_commit_timing.py \\
      --input sync_prs_fork_analysis_filtered.json \\
      --output commit_timing_analysis.json \\
      --commits-shards-dir commits_trees_shards \\
      --pr-classification-dir pr_classification_results
        """
    )
    parser.add_argument(
        "--input",
        type=str,
        default="sync_prs_fork_analysis_filtered.json",
        help="Input JSON path (default: sync_prs_fork_analysis_filtered.json)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default="commit_timing_analysis.json",
        help="Output JSON path (default: commit_timing_analysis.json)"
    )
    parser.add_argument(
        "--commits-shards-dir",
        type=str,
        default="commits_trees_shards",
        help="commits_trees_shards directory (default: commits_trees_shards)"
    )
    parser.add_argument(
        "--pr-classification-dir",
        type=str,
        default="pr_classification_results",
        help="pr_classification_results directory (default: pr_classification_results)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max processes (default: auto, CPU count - 1)"
    )
    parser.add_argument(
        "--csv-only",
        action="store_true",
        help="Only generate CSV from existing JSON, skip analysis (default: False)"
    )
    args = parser.parse_args()
    try:
        if args.csv_only:
            json_file = Path(args.input)
            generate_csv_from_json(json_file)
        else:
            analyze_all_prs(
                Path(args.input),
                Path(args.output),
                Path(args.commits_shards_dir),
                Path(args.pr_classification_dir),
                args.max_workers
            )
    except Exception as e:
        print(f"Error: analysis failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
