#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filter PRs from repos_prs_from_downloaded_forks.json or split PR directory:
keep only PRs from forks that have unique commits.
Optionally analyze each PR's commits (code/comment/doc).

Filter criteria:
1. PR is from a fork (head_repo_full_name != base_repo_full_name)
2. head_repo_full_name is in the forks-with-unique-commits list

Supports:
- Single JSON input (legacy)
- Split directory input (recommended, resumable)
- Output to directory, one file per PR (resumable)

Note: repo state is restored before/after run.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Set, Any, Tuple, Optional
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

# Commit analysis module
from analyze_pr_commits import (
    analyze_pr_commits,
    get_pr_modification_types,
    restore_single_repo,
    checkout_commit,
    load_repo_states,
)

try:
    import ijson  # type: ignore
except ImportError:
    ijson = None
    print("Warning: ijson not installed, will use json.load (may use more memory)")
    print("  Suggested: pip install ijson")


def sanitize_filename(name: str) -> str:
    """Sanitize filename, remove invalid characters."""
    import re
    name = name.replace("/", "__")
    name = re.sub(r'[<>:"|?*]', "_", name)
    return name


def get_pr_status(pr: Dict[str, Any]) -> str:
    """Determine PR status from merged_at.
    
    Args:
        pr: PR data dict
        
    Returns:
        "merged" or "closed"
    """
    merged_at = pr.get("merged_at")
    if merged_at and merged_at.strip():
        return "merged"
    else:
        return "closed"


def load_forks_with_unique_commits(json_path: Path) -> Set[str]:
    """Load forks-with-unique-commits set from forks_with_unique_commits.json.
    
    Returns: {fork_full_name, ...}
    """
    if not json_path.exists():
        print(f"Error: file not found: {json_path}")
        return set()
    
    print(f"Loading forks with unique commits: {json_path}")
    try:
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        forks_set = set()
        if isinstance(data, list):
            for item in data:
                fork = item.get("fork")
                if isinstance(fork, str) and fork:
                    forks_set.add(fork)
        
        print(f"Loaded {len(forks_set)} forks with unique commits")
        return forks_set
    except Exception as e:
        print(f"Error loading forks_with_unique_commits.json: {e}")
        return set()


def filter_prs_streaming(
    input_json_path: Path,
    output_dir: Path,
    forks_with_unique_commits_set: Set[str],
    repos_dir: Path,
    states_json: Path,
    analyze_commits: bool = True
) -> Dict[str, Any]:
    """Stream-parse and filter PRs with ijson (simplified).
    
    Writes one file per PR to output directory.
    Returns stats.
    """
    print(f"Streaming read: {input_json_path}")
    print("Filter: PR from fork and fork has unique commits")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    total_prs = 0
    filtered_count = 0
    skipped_not_from_fork = 0
    skipped_no_unique_commits = 0
    already_processed = 0
    
    # Stream-parse repositories array with ijson.items
    with input_json_path.open("rb") as f:
        repos = ijson.items(f, "repositories.item")
        
        for repo_data in repos:
            repo_full_name = repo_data.get("full_name", "unknown")
            prs = repo_data.get("prs", [])
            
            if not prs:
                continue
            
            for pr in prs:
                total_prs += 1
                head_repo = pr.get("head_repo_full_name")
                base_repo = pr.get("base_repo_full_name")
                pr_number = pr.get("number")
                
                # Condition 1: PR from fork (head_repo != base_repo)
                if not head_repo or not base_repo or head_repo == base_repo:
                    skipped_not_from_fork += 1
                    continue
                
                # Condition 2: fork in unique-commits list
                if head_repo not in forks_with_unique_commits_set:
                    skipped_no_unique_commits += 1
                    continue
                
                # Skip if output already exists (resume)
                output_repo_dir = output_dir / sanitize_filename(repo_full_name)
                output_repo_dir.mkdir(parents=True, exist_ok=True)
                output_filename = f"{sanitize_filename(repo_full_name)}__PR_{pr_number}.json"
                output_file = output_repo_dir / output_filename
                
                if output_file.exists():
                    already_processed += 1
                    continue
                
                # Passed filter
                filtered_count += 1
                
                # Commit analysis not supported in streaming (needs local repos)
                if analyze_commits:
                    print("  Warning: commit analysis not supported in streaming; use load or directory mode")
                    pr_copy = pr.copy()
                    # Ensure title, body, user
                    if "title" not in pr_copy:
                        pr_copy["title"] = pr.get("title", "")
                    if "body" not in pr_copy:
                        pr_copy["body"] = pr.get("body", "")
                    if "user" not in pr_copy:
                        pr_copy["user"] = pr.get("user", "")
                    pr_copy["pr_status"] = get_pr_status(pr)
                    pr_copy["modification_types"] = []
                    pr_copy["commit_analyses"] = []
                else:
                    pr_copy = pr.copy()
                    # Ensure title, body, user
                    if "title" not in pr_copy:
                        pr_copy["title"] = pr.get("title", "")
                    if "body" not in pr_copy:
                        pr_copy["body"] = pr.get("body", "")
                    if "user" not in pr_copy:
                        pr_copy["user"] = pr.get("user", "")
                    pr_copy["pr_status"] = get_pr_status(pr)
                    pr_copy["modification_types"] = []
                    pr_copy["commit_analyses"] = []
                
                # Save to single file
                output_data = {
                    "repo_full_name": repo_full_name,
                    "pr": pr_copy,
                    "filter_time": datetime.now().isoformat(),
                }
                
                with output_file.open("w", encoding="utf-8") as f:
                    json.dump(output_data, f, ensure_ascii=False, indent=2)
                
                if filtered_count % 1000 == 0:
                    print(f"  Filtered: {filtered_count} PRs (total processed: {total_prs})")
    
    return {
        "total_prs": total_prs,
        "filtered_prs": filtered_count,
        "skipped_not_from_fork": skipped_not_from_fork,
        "skipped_no_unique_commits": skipped_no_unique_commits,
        "already_processed": already_processed,
    }


def filter_prs_loaded(
    input_json_path: Path,
    output_dir: Path,
    forks_with_unique_commits_set: Set[str],
    repos_dir: Path,
    states_json: Path,
    analyze_commits: bool = True
) -> Dict[str, Any]:
    """Load JSON once and filter PRs (higher memory).
    
    Writes one file per PR to output directory.
    Returns stats.
    """
    print(f"Reading: {input_json_path}")
    print("Warning: large file, may take a while...")
    
    with input_json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    print("File read complete")
    print("Filter: PR from fork and fork has unique commits")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    total_prs = 0
    filtered_count = 0
    skipped_not_from_fork = 0
    skipped_no_unique_commits = 0
    already_processed = 0
    error_count = 0
    
    for repo_data in data.get("repositories", []):
        repo_full_name = repo_data.get("full_name", "unknown")
        prs = repo_data.get("prs", [])
        if not prs:
            continue
        
        for pr in prs:
            total_prs += 1
            head_repo = pr.get("head_repo_full_name")
            base_repo = pr.get("base_repo_full_name")
            pr_number = pr.get("number")
            
            # Condition 1: PR from fork
            if not head_repo or not base_repo or head_repo == base_repo:
                skipped_not_from_fork += 1
                continue
            
            # Condition 2: fork in unique-commits list
            if head_repo not in forks_with_unique_commits_set:
                skipped_no_unique_commits += 1
                continue
            
            # Skip if output exists (resume)
            output_repo_dir = output_dir / sanitize_filename(repo_full_name)
            output_repo_dir.mkdir(parents=True, exist_ok=True)
            output_filename = f"{sanitize_filename(repo_full_name)}__PR_{pr_number}.json"
            output_file = output_repo_dir / output_filename
            
            if output_file.exists():
                already_processed += 1
                if already_processed % 100 == 0:
                    print(f"  Skipped already processed: {already_processed} PRs")
                continue
            
            # Passed filter
            filtered_count += 1
            pr_with_analysis = pr.copy()
            
            # Ensure title, body, user
            if "title" not in pr_with_analysis:
                pr_with_analysis["title"] = pr.get("title", "")
            if "body" not in pr_with_analysis:
                pr_with_analysis["body"] = pr.get("body", "")
            if "user" not in pr_with_analysis:
                pr_with_analysis["user"] = pr.get("user", "")
            
            pr_with_analysis["pr_status"] = get_pr_status(pr)
            
            if analyze_commits:
                fork_repo_name = head_repo
                repo_dir = repos_dir / fork_repo_name.replace("/", "__")
                
                try:
                    commit_analyses = analyze_pr_commits(
                        repo_dir, pr, fork_repo_name, repos_dir
                    )
                    modification_types = get_pr_modification_types(commit_analyses)
                    pr_with_analysis["modification_types"] = modification_types
                    pr_with_analysis["commit_analyses"] = commit_analyses
                    restore_single_repo(repos_dir, states_json, fork_repo_name)
                except Exception as e:
                    print(f"  Warning: analyze PR {pr_number} ({fork_repo_name}) failed: {e}")
                    error_count += 1
                    pr_with_analysis["modification_types"] = []
                    pr_with_analysis["commit_analyses"] = []
                    try:
                        restore_single_repo(repos_dir, states_json, fork_repo_name)
                    except:
                        pass
            else:
                pr_with_analysis["modification_types"] = []
                pr_with_analysis["commit_analyses"] = []
            
            output_data = {
                "repo_full_name": repo_full_name,
                "pr": pr_with_analysis,
                "filter_time": datetime.now().isoformat(),
            }
            
            with output_file.open("w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            if filtered_count % 100 == 0:
                print(f"  Processed: {filtered_count} PRs, skipped: {already_processed}")
        
        if total_prs % 10000 == 0:
            print(f"  Progress: {total_prs} PRs, filtered: {filtered_count}")
    
    return {
        "total_prs": total_prs,
        "filtered_prs": filtered_count,
        "skipped_not_from_fork": skipped_not_from_fork,
        "skipped_no_unique_commits": skipped_no_unique_commits,
        "already_processed": already_processed,
        "error_count": error_count,
    }


def process_single_pr_file(
    pr_file_path_str: str,
    output_dir_str: str,
    forks_with_unique_commits_set: Set[str],
    repos_dir_str: str,
    states_json_str: str,
    analyze_commits: bool
) -> Tuple[str, Dict[str, Any]]:
    """Worker to process a single PR file (for multiprocessing).
    
    Returns:
        (pr_file_path, result_dict) or (pr_file_path, None) on failure
    """
    pr_file_path = Path(pr_file_path_str)
    output_dir = Path(output_dir_str)
    repos_dir = Path(repos_dir_str)
    states_json = Path(states_json_str)
    
    try:
        with pr_file_path.open("r", encoding="utf-8") as f:
            pr_data = json.load(f)
        
        repo_full_name = pr_data.get("repo_full_name", "unknown")
        pr = pr_data.get("pr", {})
        
        if not pr:
            return (str(pr_file_path), {"status": "no_pr_data"})
        
        head_repo = pr.get("head_repo_full_name")
        base_repo = pr.get("base_repo_full_name")
        pr_number = pr.get("number")
        
        if not head_repo or not base_repo or head_repo == base_repo:
            return (str(pr_file_path), {"status": "skipped", "reason": "not_from_fork"})
        
        if head_repo not in forks_with_unique_commits_set:
            return (str(pr_file_path), {"status": "skipped", "reason": "no_unique_commits"})
        
        # Skip if output exists (resume)
        output_repo_dir = output_dir / sanitize_filename(repo_full_name)
        output_repo_dir.mkdir(parents=True, exist_ok=True)
        output_filename = f"{sanitize_filename(repo_full_name)}__PR_{pr_number}.json"
        output_file = output_repo_dir / output_filename
        
        if output_file.exists():
            return (str(pr_file_path), {"status": "already_processed"})
        
        pr_with_analysis = pr.copy()
        
        # Ensure title, body, user
        if "title" not in pr_with_analysis:
            pr_with_analysis["title"] = pr.get("title", "")
        if "body" not in pr_with_analysis:
            pr_with_analysis["body"] = pr.get("body", "")
        if "user" not in pr_with_analysis:
            pr_with_analysis["user"] = pr.get("user", "")
        
        pr_with_analysis["pr_status"] = get_pr_status(pr)
        
        if analyze_commits:
            fork_repo_name = head_repo
            repo_dir = repos_dir / fork_repo_name.replace("/", "__")
            
            try:
                commit_analyses = analyze_pr_commits(
                    repo_dir, pr, fork_repo_name, repos_dir
                )
                
                modification_types = get_pr_modification_types(commit_analyses)
                pr_with_analysis["modification_types"] = modification_types
                pr_with_analysis["commit_analyses"] = commit_analyses
                
                restore_single_repo(repos_dir, states_json, fork_repo_name)
            except Exception as e:
                pr_with_analysis["modification_types"] = []
                pr_with_analysis["commit_analyses"] = []
                try:
                    restore_single_repo(repos_dir, states_json, fork_repo_name)
                except:
                    pass
                return (str(pr_file_path), {"status": "error", "error": str(e)})
        else:
            pr_with_analysis["modification_types"] = []
            pr_with_analysis["commit_analyses"] = []
        
        output_data = {
            "repo_full_name": repo_full_name,
            "pr": pr_with_analysis,
            "filter_time": datetime.now().isoformat(),
        }
        
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        return (str(pr_file_path), {"status": "success"})
    
    except Exception as e:
        return (str(pr_file_path), {"status": "error", "error": str(e)})


def filter_prs_from_directory(
    input_dir: Path,
    output_dir: Path,
    forks_with_unique_commits_set: Set[str],
    repos_dir: Path,
    states_json: Path,
    analyze_commits: bool = True,
    max_workers: Optional[int] = None
) -> Dict[str, Any]:
    """Read PR files from split directory, filter and analyze (resumable, multiprocess)."""
    print(f"Reading PR files from directory: {input_dir}")
    print("Filter: PR from fork and fork has unique commits")
    
    if max_workers is None:
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    print(f"Using {max_workers} worker processes")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    stats = {
        "total_prs": 0,
        "filtered_prs": 0,
        "skipped_not_from_fork": 0,
        "skipped_no_unique_commits": 0,
        "already_processed": 0,
        "processed_count": 0,
        "error_count": 0,
    }
    
    pr_files = []
    for repo_dir in input_dir.iterdir():
        if not repo_dir.is_dir():
            continue
        for pr_file in repo_dir.glob("*__PR_*.json"):
            pr_files.append(pr_file)
    
    print(f"Found {len(pr_files)} PR files")
    stats["total_prs"] = len(pr_files)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_file = {
            executor.submit(
                process_single_pr_file,
                str(pr_file),
                str(output_dir),
                forks_with_unique_commits_set,
                str(repos_dir),
                str(states_json),
                analyze_commits
            ): pr_file
            for pr_file in pr_files
        }
        
        for future in as_completed(future_to_file):
            pr_file = future_to_file[future]
            try:
                pr_file_str, result = future.result()
                status = result.get("status")
                
                if status == "success":
                    stats["processed_count"] += 1
                    stats["filtered_prs"] += 1
                elif status == "already_processed":
                    stats["already_processed"] += 1
                elif status == "skipped":
                    reason = result.get("reason")
                    if reason == "not_from_fork":
                        stats["skipped_not_from_fork"] += 1
                    elif reason == "no_unique_commits":
                        stats["skipped_no_unique_commits"] += 1
                elif status == "error":
                    stats["error_count"] += 1
                    error_msg = result.get("error", "unknown error")
                    print(f"  Warning: failed to process {pr_file.name}: {error_msg}")
                
                total_processed = (
                    stats["processed_count"] + 
                    stats["already_processed"] + 
                    stats["skipped_not_from_fork"] + 
                    stats["skipped_no_unique_commits"] +
                    stats["error_count"]
                )
                if total_processed % 100 == 0:
                    skipped_filter = stats["skipped_not_from_fork"] + stats["skipped_no_unique_commits"]
                    print(f"  Progress: {total_processed}/{stats['total_prs']} | "
                          f"Processed: {stats['processed_count']} | "
                          f"Skipped (done): {stats['already_processed']} | "
                          f"Skipped (filter): {skipped_filter} | "
                          f"Errors: {stats['error_count']}")
            
            except Exception as e:
                stats["error_count"] += 1
                print(f"  Warning: exception processing {pr_file}: {e}")
    
    return stats


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Filter PRs from forks that have unique commits")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input JSON path (mutually exclusive with --input-dir)"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        default=None,
        help="Input directory (split PR files; mutually exclusive with --input; recommended)"
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output directory path (if ends with .json, stem used as dir; mutually exclusive with --output-dir)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (one file per PR; mutually exclusive with --output; recommended; resumable)"
    )
    parser.add_argument(
        "--forks-list",
        type=str,
        default="forks_with_unique_commits.json",
        help="Path to forks-with-unique-commits list file"
    )
    parser.add_argument(
        "--repos-dir",
        type=str,
        default="cloned_repos",
        help="Local repos directory (default: cloned_repos)"
    )
    parser.add_argument(
        "--states-file",
        type=str,
        default="repo_states.json",
        help="Repo state file path (default: repo_states.json)"
    )
    parser.add_argument(
        "--no-analyze",
        action="store_true",
        help="Do not analyze commits (only filter PRs)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (default: CPU count - 1)"
    )
    args = parser.parse_args()
    
    current_dir = Path(__file__).parent
    forks_list_path = current_dir / args.forks_list
    repos_dir = (Path(args.repos_dir) if Path(args.repos_dir).is_absolute() else current_dir / args.repos_dir).resolve()
    states_json = (Path(args.states_file) if Path(args.states_file).is_absolute() else current_dir / args.states_file).resolve()
    analyze_commits = not args.no_analyze
    
    use_directory_mode = False
    if args.input_dir:
        input_dir = (Path(args.input_dir) if Path(args.input_dir).is_absolute() else current_dir / args.input_dir).resolve()
        use_directory_mode = True
        if not input_dir.exists():
            print(f"Error: input directory does not exist: {input_dir}")
            sys.exit(1)
    elif args.input:
        input_json_path = current_dir / args.input
        if not input_json_path.exists():
            print(f"Error: input file does not exist: {input_json_path}")
            sys.exit(1)
    else:
        default_dir = current_dir / "prs_split"
        default_file = current_dir / "repos_prs_from_downloaded_forks.json"
        if default_dir.exists():
            input_dir = default_dir
            use_directory_mode = True
            print(f"Info: no input specified, using default dir: {input_dir}")
        elif default_file.exists():
            input_json_path = default_file
            print(f"Info: no input specified, using default file: {input_json_path}")
        else:
            print("Error: no input file or directory specified and no default found")
            sys.exit(1)
    
    if args.output_dir:
        output_dir = (Path(args.output_dir) if Path(args.output_dir).is_absolute() else current_dir / args.output_dir).resolve()
    elif args.output:
        output_path = Path(args.output)
        if output_path.suffix == ".json":
            output_dir = current_dir / output_path.stem
        else:
            output_dir = current_dir / args.output
        print(f"Info: --output interpreted as output directory: {output_dir}")
    else:
        output_dir = current_dir / "filtered_prs_split"
        print(f"Info: no output dir specified, using default: {output_dir}")
    
    print("=" * 80)
    print("Filter PRs from forks with unique commits")
    if analyze_commits:
        print("(and analyze commit modification types)")
    print("(Output: one file per PR, resumable)")
    print("=" * 80)
    if use_directory_mode:
        print(f"Input dir: {input_dir}")
        if args.workers is not None:
            print(f"Workers: {args.workers}")
        else:
            print("Workers: auto (CPU count - 1)")
    else:
        print(f"Input file: {input_json_path}")
    print(f"Output dir: {output_dir}")
    print(f"Forks list: {forks_list_path}")
    if analyze_commits:
        print(f"Repos dir: {repos_dir}")
        print(f"States file: {states_json}")
    print(f"Analyze commits: {analyze_commits}")
    print()
    
    if analyze_commits:
        if not states_json.exists():
            print(f"Warning: states file not found: {states_json}")
            print("  Run 'python manage_repo_states.py save' first to save repo state")
            sys.exit(1)
        
        if not repos_dir.exists():
            print(f"Warning: repos directory not found: {repos_dir}")
            print("  Ensure repos are cloned there")
            sys.exit(1)
        
        print("Info: repo state will be restored after each PR analysis")
    
    forks_with_unique_commits_set = load_forks_with_unique_commits(forks_list_path)
    if not forks_with_unique_commits_set:
        print("Error: could not load forks-with-unique-commits list, aborting")
        return
    
    if not use_directory_mode:
        if not input_json_path.exists():
            print(f"Error: input file not found: {input_json_path}")
            return
    
    print()
    print("=" * 80)
    print("Starting filter...")
    print("=" * 80)
    
    if use_directory_mode:
        stats = filter_prs_from_directory(
            input_dir, output_dir, forks_with_unique_commits_set, 
            repos_dir, states_json, analyze_commits, 
            max_workers=args.workers
        )
    else:
        if analyze_commits:
            stats = filter_prs_loaded(input_json_path, output_dir, forks_with_unique_commits_set, repos_dir, states_json, analyze_commits)
        elif ijson:
            try:
                stats = filter_prs_streaming(input_json_path, output_dir, forks_with_unique_commits_set, repos_dir, states_json, analyze_commits)
            except Exception as e:
                print(f"Warning: streaming parse failed: {e}")
                print("  Falling back to load mode...")
                stats = filter_prs_loaded(input_json_path, output_dir, forks_with_unique_commits_set, repos_dir, states_json, analyze_commits)
        else:
            stats = filter_prs_loaded(input_json_path, output_dir, forks_with_unique_commits_set, repos_dir, states_json, analyze_commits)
    
    print()
    print("=" * 80)
    print("Filter complete.")
    print("=" * 80)
    print(f"Total PRs: {stats['total_prs']:,}")
    print(f"Filtered PRs: {stats['filtered_prs']:,}")
    print(f"Skipped (not from fork): {stats['skipped_not_from_fork']:,}")
    print(f"Skipped (fork has no unique commits): {stats['skipped_no_unique_commits']:,}")
    print(f"Filter rate: {stats['filtered_prs'] / stats['total_prs'] * 100:.2f}%" if stats['total_prs'] > 0 else "N/A")
    print()
    print(f"Results saved to: {output_dir}")
    if stats.get("already_processed", 0) > 0:
        print(f"  Skipped {stats['already_processed']} already-processed PRs (resume)")
    
    if analyze_commits:
        print()
        print("Info: repo state was restored after each PR analysis")


if __name__ == "__main__":
    main()

