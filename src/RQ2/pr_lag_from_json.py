#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PR Lag classification from sharded JSON (equivalent to old lag classification logic).

Data sources:
1. REPOS_PRS_SHARDS_DIR (repos_prs_commits_shards/): PR shards (main source)
2. commits_trees.json / commits_trees_shards/: Commit details (optional, for Lag1)
3. github_projects*.json: Repo info (updated_at, pushed_at for Lag3).

Lag3: time between origin update and fork sync. Computed from pushed_at or shared commits.
Script reads PR data from shards to avoid high memory use.
"""

import json
import csv
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any, Tuple, Set
import traceback
from pathlib import Path
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

try:
    import ijson  # type: ignore
except Exception:
    ijson = None


class Config:
    FORKS_WITH_UNIQUE_COMMITS_JSON = "forks_with_unique_commits.json"
    REPOS_PRS_SHARDS_DIR = "repos_prs_commits_shards"
    COMMITS_TREES_JSON = "commits_trees.json"
    COMMITS_SHARDS_DIR = "commits_trees_shards"
    PROJECTS_JSON = "github_projects_filtered_stars_10.json"
    PROJECTS_JSON_ALL = "github_projects.json"
    CLONE_REPORT_JSON = os.path.join("cloned_repos", "clone_report.json")

    OUTPUT_CSV = "pr_lag_classification_from_json.csv"
    LOG_FILE = "pr_lag_from_json_log.txt"
    DEBUG_SAMPLE = 10
    MAX_COMMIT_DATES_IN_CELL = 0


CSV_COLUMNS = [
    "PR_ID",
    "repo_name",
    "PR_number",
    "PR_url",
    "all_commit_author_dates_ISO",
    "PR_created_at",
    "PR_merged_at",
    "PR_closed_at",
    "fork_last_updated",
    "origin_last_updated",
    "has_Lag1",
    "commit_count",
    "commits_over_threshold",
    "max_Lag1_days",
    "avg_Lag1_days",
    "Lag1_desc",
    "has_Lag2",
    "Lag2_days",
    "Lag2_desc",
    "has_Lag3",
    "Lag3_days",
    "Lag3_desc",
    "PR_head_repo",
    "Lag_combination",
    "analysis_time",
    "PR_user_type",
]


def load_successful_repos_from_clone_report(report_path: Path) -> set:
    """Load set of successfully cloned repos from clone_report.json (commit_count=0 treated as failed)."""
    if not report_path.exists():
        write_log(f"Warning: clone_report.json not found: {report_path}; no clone filter", level="WARNING")
        return set()
    try:
        with report_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        out = set()
        for item in data.get("success_repos", []) or []:
            name = item.get("repo_name")
            if not name:
                continue
            cc = item.get("commit_count", None)
            if cc == 0:
                continue
            out.add(name)
        write_log(f"Loaded {len(out)} successful repos from clone_report.json")
        return out
    except Exception as e:
        write_log(f"Warning: failed to read clone_report.json: {e}; no clone filter", level="WARNING")
        return set()


# ---------------------- Data loading ----------------------
def load_repos_prs(json_path: Path) -> Dict[str, List[Dict]]:
    """Load PR data from JSON file (deprecated, no longer used).
    
    Returns: {repo_full_name: [pr1, pr2, ...]}
    """
    if not json_path.exists():
        write_log(f"File not found: {json_path}", level="ERROR")
        return {}
    
    write_log(f"Loading PR data: {json_path}")
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    repos_prs = {}
    for repo_data in data.get("repositories", []):
        full_name = repo_data.get("full_name", "")
        prs = repo_data.get("prs", [])
        if full_name and prs:
            repos_prs[full_name] = prs
    
    write_log(f"OK: Loaded PR data for {len(repos_prs)} repos, {sum(len(prs) for prs in repos_prs.values())} PRs total")
    return repos_prs


def load_forks_with_unique_commits(json_path: Path) -> set:
    """Load set of forks with unique commits from forks_with_unique_commits.json.
    
    Returns: {fork_full_name, ...}
    """
    if not json_path.exists():
        write_log(f"Warning: File not found: {json_path}; will not filter forks", level="WARNING")
        return set()
    
    write_log(f"Loading forks with unique commits: {json_path}")
    try:
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        forks_set = set()
        if isinstance(data, list):
            for item in data:
                fork = item.get("fork")
                if isinstance(fork, str) and fork:
                    forks_set.add(fork)
        
        write_log(f"OK: Loaded {len(forks_set)} forks with unique commits")
        return forks_set
    except Exception as e:
        write_log(f"Warning: Failed to load forks_with_unique_commits.json: {e}; will not filter forks", level="WARNING")
        return set()


def repo_to_prs_shard_path(shards_dir: Path, repo_full_name: str) -> Path:
    """owner/repo -> repos_prs_commits_shards/owner__repo.jsonl"""
    return shards_dir / (repo_full_name.replace("/", "__") + ".jsonl")


def _load_prs_for_repo_from_shard(shards_dir: Path, repo_full_name: str) -> List[Dict]:
    """Read a single repo's PR shard (JSONL) and return list of PRs."""
    p = repo_to_prs_shard_path(shards_dir, repo_full_name)
    if not p.exists():
        return []
    prs: List[Dict] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                prs.append(json.loads(line))
            except Exception:
                continue
    return prs


def load_commits_from_trees(json_path: Path, target_repos: Optional[set] = None) -> Dict[str, List[Dict]]:
    """Load commits data from commits_trees.json.
    
    Returns: {repo_full_name: [commit1, commit2, ...]}
    """
    if not json_path.exists():
        write_log(f"Warning: commits_trees.json not found; Lag1 cannot be computed (needs commit author_date)", level="WARNING")
        return {}
    
    # Target repo filter: only load repos that have PRs to save memory
    if target_repos:
        write_log(f"Loading Commits data (streaming, target repos {len(target_repos)}): {json_path}")
    else:
        write_log(f"Loading Commits data (streaming, no target limit; may use more memory): {json_path}")

    repo_commits: Dict[str, List[Dict]] = {}

    def add_commit(repo_name: str, author_date: Optional[str]):
        if target_repos and repo_name not in target_repos:
            return
        if repo_name not in repo_commits:
            repo_commits[repo_name] = []
        # Keep only minimal fields needed for Lag1 to reduce memory
        if author_date:
            repo_commits[repo_name].append({"author": {"date": author_date}})

    if ijson is None:
        write_log("Warning: ijson not installed; cannot stream commits_trees.json, falling back to json.load (may use a lot of memory)", level="WARNING")
        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        for family in data.get("families", []):
            commits_list = family.get("commits", []) or []
            for commit in commits_list:
                author_date = None
                author = commit.get("author", {})
                if isinstance(author, dict):
                    author_date = author.get("date")
                repos = commit.get("repos", []) or []
                for repo_name in repos:
                    add_commit(repo_name, author_date)
        write_log(f"OK: Loaded Commits data for {len(repo_commits)} repos")
        return repo_commits

    # ijson streaming parse (does not load entire file at once)
    current_author_date: Optional[str] = None
    current_repos: List[str] = []

    with json_path.open("rb") as f:
        for prefix, event, value in ijson.parse(f):
            # commit start
            if prefix == "families.item.commits.item" and event == "start_map":
                current_author_date = None
                current_repos = []
                continue
            # author.date
            if prefix == "families.item.commits.item.author.date" and event == "string":
                current_author_date = value
                continue
            # repos list items
            if prefix == "families.item.commits.item.repos.item" and event == "string":
                current_repos.append(value)
                continue
            # commit end => distribute minimal info
            if prefix == "families.item.commits.item" and event == "end_map":
                if current_repos and current_author_date:
                    for repo_name in current_repos:
                        add_commit(repo_name, current_author_date)
                continue

    write_log(f"OK: Loaded Commits data for {len(repo_commits)} repos")
    return repo_commits


def repo_to_shard_path(shards_dir: Path, repo_full_name: str) -> Path:
    """owner/repo -> commits_trees_shards/owner__repo.jsonl"""
    return shards_dir / (repo_full_name.replace("/", "__") + ".jsonl")


def load_commits_from_shards(
    shards_dir: Path,
    target_repos: Optional[set] = None,
) -> Dict[str, List[Dict]]:
    """Load commits from per-repo JSONL shards (only author_date needed for Lag1).

    Example minimal line format:
    {"hash": "...", "parents": [...], "author_date": "...", "committer_date": "...", "tree": "...?"}
    """
    if not shards_dir.exists() or not shards_dir.is_dir():
        write_log(f"Warning: Shards dir not found: {shards_dir}; will fall back to commits_trees.json", level="WARNING")
        return {}

    if not target_repos:
        write_log("Warning: target_repos not provided; full load in shard mode not recommended (many files). Falling back to commits_trees.json", level="WARNING")
        return {}

    write_log(f"Loading Commits from shards (target repos {len(target_repos)}): {shards_dir}")

    repo_commits: Dict[str, List[Dict]] = {}
    loaded_repos = 0

    for repo_name in target_repos:
        shard_path = repo_to_shard_path(shards_dir, repo_name)
        if not shard_path.exists():
            continue
        loaded_repos += 1
        repo_commits.setdefault(repo_name, [])
        try:
            with shard_path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    author_date = obj.get("author_date")
                    if author_date:
                        repo_commits[repo_name].append({"author": {"date": author_date}})
        except Exception as e:
            write_log(f"Warning: Failed to read shard: {shard_path} -> {e}", level="WARNING")

    write_log(f"OK: Shard load done: hit {loaded_repos}/{len(target_repos)} repo shards")
    write_log(f"OK: Got author_date lists for {len(repo_commits)} repos")
    return repo_commits


def _load_author_dates_for_repo_from_shard(shards_dir: Path, repo_full_name: str) -> List[str]:
    """Read a single repo's shard file and return list of author_date strings."""
    shard_path = repo_to_shard_path(shards_dir, repo_full_name)
    if not shard_path.exists():
        return []
    author_dates: List[str] = []
    with shard_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            # Support two shard formats:
            # 1) minimal: {"author_date": "..."}
            # 2) full:    {"author": {"date": "..."}}
            d = obj.get("author_date")
            if not d:
                author = obj.get("author")
                if isinstance(author, dict):
                    d = author.get("date")
            if d:
                author_dates.append(str(d))
    return author_dates


def _load_commits_for_repo_from_shard(shards_dir: Path, repo_full_name: str) -> List[Dict]:
    """Read a single repo's shard file and return full commit info (author_date and committer_date)."""
    shard_path = repo_to_shard_path(shards_dir, repo_full_name)
    if not shard_path.exists():
        return []
    commits: List[Dict] = []
    with shard_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            # Extract author_date and committer_date
            commit_info = {}
            author_date = obj.get("author_date")
            if not author_date:
                author = obj.get("author")
                if isinstance(author, dict):
                    author_date = author.get("date")
            if author_date:
                commit_info["author_date"] = author_date
            
            committer_date = obj.get("committer_date")
            if not committer_date:
                committer = obj.get("committer")
                if isinstance(committer, dict):
                    committer_date = committer.get("date")
            if committer_date:
                commit_info["committer_date"] = committer_date
            
            if commit_info:
                commits.append(commit_info)
    return commits


def process_repo_prs_worker(
    repo_full_name: str,
    prs_shard_path_str: str,
    commits_shards_dir_str: str,
    repo_metadata_entry: Optional[Dict],
    forks_with_unique_commits_set: Optional[set] = None,  # Single-thread: pass set directly
    prs_list: Optional[List[Dict]] = None,  # Single-thread: pass list directly
    forks_with_unique_commits_set_str: Optional[str] = None,  # Multi-process: serialized string (deprecated)
    prs_from_main_json_str: Optional[str] = None,  # Multi-process: serialized string (deprecated)
    repo_metadata: Optional[Dict[str, Dict]] = None,  # Full repo metadata dict (all forks)
) -> Tuple[str, List[Dict[str, Any]], Dict[str, int], int]:
    """Process all PRs for one repo; return CSV rows and N/A stats.
    
    Reads PR data from REPOS_PRS_SHARDS_DIR shards.
    Only processes PRs from forks that have unique commits.
    """
    commits_shards_dir = Path(commits_shards_dir_str)
    
    # Handle forks_with_unique_commits_set (single-thread: pass directly; multi-process: deserialize)
    if forks_with_unique_commits_set is None:
        # Multi-process: deserialize from string (deprecated, kept for compatibility)
        forks_with_unique_commits_set = set()
        if forks_with_unique_commits_set_str:
            try:
                forks_list = json.loads(forks_with_unique_commits_set_str)
                if isinstance(forks_list, list):
                    forks_with_unique_commits_set = set(forks_list)
            except Exception:
                pass
    
    # 1. Prefer PR data passed from main process (single-thread: direct; multi-process: deserialize)
    prs: List[Dict] = []
    prs_loaded_from_main = False
    prs_count_before_filter = 0
    
    # Single-thread: use passed list directly
    if prs_list is not None:
        prs = prs_list
        prs_loaded_from_main = True
        prs_count_before_filter = len(prs)
    # Multi-process: deserialize from string (deprecated, kept for compatibility)
    elif prs_from_main_json_str:
        try:
            prs = json.loads(prs_from_main_json_str)
            if not isinstance(prs, list):
                prs = []
            else:
                prs_loaded_from_main = True
                prs_count_before_filter = len(prs)
        except Exception as e:
            # If deserialize fails, log error (log for all repos as this is serious)
            write_log(f"Warning: Worker: Failed to deserialize PR data from main process ({repo_full_name}): {e}", level="WARNING")
            prs = []
    
    # 2. If no data from main process, read from REPOS_PRS_SHARDS_DIR
    if not prs:
        try:
            prs_shard_path = Path(prs_shard_path_str)
            if prs_shard_path.exists():
                with prs_shard_path.open("r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            prs.append(json.loads(line))
                        except Exception:
                            continue
        except Exception:
            pass
    
    # 3. Filter: keep only PRs from forks with unique commits
    # Note: if loaded from main (prs_loaded_from_main=True), already filtered; if from shards (False), filter here
    original_count = len(prs)  # For diagnostics
    if not prs_loaded_from_main:
        # Only filter data loaded from shards
        if not forks_with_unique_commits_set:
            # No forks set -> return empty (should not happen; main process checks)
            return repo_full_name, [], {}, 0
        
        prs_without_head_repo = 0
        prs_head_repo_not_in_set = 0
        filtered_prs = []
        for pr in prs:
            head_repo = pr.get("head_repo_full_name")
            if not head_repo:
                prs_without_head_repo += 1
                continue
            if head_repo in forks_with_unique_commits_set:
                filtered_prs.append(pr)
            else:
                prs_head_repo_not_in_set += 1
        prs = filtered_prs
        filtered_count = len(prs)
        # Diagnostic: if count changed a lot after filter, log (only first few repos to avoid log spam)
        if original_count > 0 and filtered_count < original_count:
            write_log(
                f"Worker: repo {repo_full_name} PRs from shards filtered: {original_count} -> {filtered_count}",
                level="WARNING"
            )
    # If loaded from main process, use as-is; no filter
    
    # 6. Build pr_metrics_map (for Lag1 computation)
    pr_metrics_map: Dict[int, Dict[str, Any]] = {}
    for pr in prs:
        n = pr.get("number")
        if isinstance(n, int):
            # If PR is from shard, it may have commits list and commit_count but no lag1_*
            # Need to extract author_date from commits for Lag1
            if "lag1_max_days" in pr:
                # Has full metrics, use directly
                pr_metrics_map[n] = pr
            elif "commits" in pr and isinstance(pr.get("commits"), list):
                # Has commits list but no lag1_*; compute from commits later
                # Do not add to pr_metrics_map; let later logic compute from commits list
                pass

    author_dates_dt: List[datetime] = []
    if not pr_metrics_map:
        author_dates_str = _load_author_dates_for_repo_from_shard(commits_shards_dir, repo_full_name)
        for d in author_dates_str:
            dt = parse_date(d)
            if dt:
                author_dates_dt.append(dt)

    repo_commits = {repo_full_name: []}  # Compatible signature
    # Use passed full repo_metadata; if None, use only current origin metadata
    if repo_metadata is None:
        repo_metadata = {}
        if repo_metadata_entry:
            repo_metadata[repo_full_name] = repo_metadata_entry
    # If full repo_metadata passed, ensure current origin is in it
    elif repo_metadata_entry and repo_full_name not in repo_metadata:
        repo_metadata[repo_full_name] = repo_metadata_entry

    # Step 1: Compute average lag values for all PRs of this origin (as thresholds)
    lag1_avg_values: List[float] = []
    lag2_values: List[float] = []
    lag3_values: List[float] = []
    
    # Lag3 threshold: based on sync time gap between main repo and fork
    # One fork and one origin have one lag3; threshold is the average of this interval for this origin and all its forks
    # First collect all unique forks (from PRs of this origin)
    forks_for_this_origin: Set[str] = set()
    for pr_data in prs:
        head_repo_full_name = pr_data.get("head_repo_full_name")
        if isinstance(head_repo_full_name, str) and head_repo_full_name and head_repo_full_name != repo_full_name:
            # Only consider forks with unique commits
            if forks_with_unique_commits_set and head_repo_full_name in forks_with_unique_commits_set:
                forks_for_this_origin.add(head_repo_full_name)
    
    # For each fork, compute its Lag3 with origin
    # Lag3 = average interval of fork pulling from origin
    # Record: pull count, average, median
    fork_lag3_map: Dict[str, Dict[str, Any]] = {}  # {fork_full_name: {"lag3_avg": float, "lag3_median": float, "pull_count": int}}
    
    for fork_full_name in forks_for_this_origin:
        # Compute each pull interval from shared commits' timestamps
        pull_intervals: List[float] = []  # Store each pull interval (days)
        
        # Try to load full commit info (including committer_date) from shards
        commits_shards_dir = Path(commits_shards_dir_str)
        origin_commits_full = _load_commits_for_repo_from_shard(commits_shards_dir, repo_full_name)
        fork_commits_full = _load_commits_for_repo_from_shard(commits_shards_dir, fork_full_name)
        
        if origin_commits_full and fork_commits_full:
            # Build map: origin commit author_date str -> datetime
            origin_commit_map: Dict[str, datetime] = {}  # {author_date_str: author_date_datetime}
            for commit in origin_commits_full:
                author_date_str = commit.get("author_date")
                if author_date_str:
                    author_date_dt = parse_date(author_date_str)
                    if author_date_dt:
                        # If same author_date appears multiple times, keep earliest (more accurate)
                        if author_date_str not in origin_commit_map or author_date_dt < origin_commit_map[author_date_str]:
                            origin_commit_map[author_date_str] = author_date_dt
            
            # Find commits shared between fork and origin; compute each pull interval
            # Use committer_date to identify pull time
            for commit in fork_commits_full:
                author_date_str = commit.get("author_date")
                if author_date_str and author_date_str in origin_commit_map:
                    # Found shared commit
                    origin_author_time = origin_commit_map[author_date_str]
                    
                    # Use committer_date as pull time (if available)
                    fork_committer_date_str = commit.get("committer_date")
                    if fork_committer_date_str:
                        fork_committer_time = parse_date(fork_committer_date_str)
                        if fork_committer_time:
                            # Interval: fork commit committer_date - origin commit author_date (origin commit to fork pull)
                            delta_days = (fork_committer_time - origin_author_time).total_seconds() / 86400
                            if delta_days >= 0:  # Only record non-negative
                                pull_intervals.append(delta_days)
                    else:
                        # No committer_date; use author_date (less accurate fallback); interval 0, still count one pull
                        pull_intervals.append(0.0)
        
        # If cannot compute from shared commits, try pushed_at
        if not pull_intervals:
            origin_pushed = None
            if repo_metadata_entry:
                origin_pushed = parse_date(repo_metadata_entry.get("pushed_at") or repo_metadata_entry.get("updated_at"))
            
            if origin_pushed:
                fork_metadata = repo_metadata.get(fork_full_name) if repo_metadata else None
                if fork_metadata:
                    fork_pushed = parse_date(fork_metadata.get("pushed_at") or fork_metadata.get("updated_at"))
                    if fork_pushed:
                        # Use pushed_at as one pull interval
                        delta_days = (origin_pushed - fork_pushed).total_seconds() / 86400
                        if delta_days >= 0:
                            pull_intervals.append(delta_days)
        
        # Compute average, median, and pull count
        if pull_intervals:
            pull_intervals.sort()  # Sort for median
            pull_count = len(pull_intervals)
            lag3_avg = sum(pull_intervals) / pull_count
            lag3_median = pull_intervals[pull_count // 2] if pull_count % 2 == 1 else (pull_intervals[pull_count // 2 - 1] + pull_intervals[pull_count // 2]) / 2
            
            fork_lag3_map[fork_full_name] = {
                "lag3_avg": lag3_avg,
                "lag3_median": lag3_median,
                "pull_count": pull_count
            }
            # Use average as threshold basis
            lag3_values.append(lag3_avg)
    
    # First pass: compute lag1 and lag2 for all PRs (no threshold check)
    for pr_data in prs:
        pr_times_temp = extract_pr_times(pr_data, repo_full_name, repo_commits, repo_metadata)
        pr_num_int_temp = pr_data.get("number")
        m_temp = pr_metrics_map.get(pr_num_int_temp) if isinstance(pr_num_int_temp, int) else None
        
        # If PR has commits list but no lag1_* metrics, extract author_date from commits
        if not m_temp and pr_data.get("commits") and isinstance(pr_data.get("commits"), list):
            commits_list = pr_data.get("commits", [])
            commit_author_dates = []
            for commit in commits_list:
                if isinstance(commit, dict):
                    # Support multiple commit formats:
                    # 1. GitHub API: commit.commit.author.date
                    commit_obj = commit.get("commit")
                    if isinstance(commit_obj, dict):
                        author = commit_obj.get("author")
                        if isinstance(author, dict):
                            author_date_str = author.get("date")
                            if author_date_str:
                                author_date_dt = parse_date(author_date_str)
                                if author_date_dt:
                                    commit_author_dates.append(author_date_dt)
                                    continue
                    # 2. Direct: commit.author.date
                    author = commit.get("author")
                    if isinstance(author, dict):
                        author_date_str = author.get("date")
                        if author_date_str:
                            author_date_dt = parse_date(author_date_str)
                            if author_date_dt:
                                commit_author_dates.append(author_date_dt)
                                continue
                    # 3. Flat: commit.author_date
                    author_date_str = commit.get("author_date")
                    if author_date_str:
                        author_date_dt = parse_date(author_date_str)
                        if author_date_dt:
                            commit_author_dates.append(author_date_dt)
            if commit_author_dates:
                pr_times_temp["all_author_dates"] = commit_author_dates
        # Note: if PR has no commits field, do not use whole-repo commits; mark N/A
        # elif not m_temp:
        #     pr_times_temp["all_author_dates"] = author_dates_dt  # Wrong; would break lag1
        
        # Compute lag values (no threshold check)
        # Note: even with metrics, recompute Lag1 (last commit time) for consistency
        # Lag1 = PR created - last commit created
        if pr_times_temp["all_author_dates"] and pr_times_temp["pr_created"]:
            pr_created = pr_times_temp["pr_created"]
            # Only commits before PR created
            commit_dates = [d for d in pr_times_temp["all_author_dates"] if d <= pr_created]
            if commit_dates:
                # Last (newest) commit time
                last_commit_datetime = max(commit_dates)
                # Lag1 = PR created - last commit created
                lag1_delta = (pr_created - last_commit_datetime).total_seconds() / 86400
                if lag1_delta >= 0:
                    lag1_avg_values.append(lag1_delta)
        
        # Compute lag2_days
        if pr_times_temp["pr_created"] and pr_times_temp["pr_merged"]:
            delta_days = (pr_times_temp["pr_merged"] - pr_times_temp["pr_created"]).total_seconds() / 86400
            if delta_days >= 0:
                    lag2_values.append(delta_days)
    
    # Average thresholds
    lag1_threshold = sum(lag1_avg_values) / len(lag1_avg_values) if lag1_avg_values else None
    lag2_threshold = sum(lag2_values) / len(lag2_values) if lag2_values else None
    lag3_threshold = sum(lag3_values) / len(lag3_values) if lag3_values else None
    
    rows: List[Dict[str, Any]] = []
    missing_field_stats = {
        "no_any_commit_date": 0,
        "no_pr_created": 0,
        "no_pr_merged": 0,
        "no_fork_origin": 0,
        "no_origin_pushed": 0,
        "no_fork_pushed": 0,
        "no_head_repo_full_name": 0,
        "fork_not_in_metadata": 0,
    }
    processed = 0
    failed_prs = []  # Track failed PRs
    
    total_prs_for_repo = len(prs)
    
    # Diagnostic: log per-repo PR status (worker process; may not show in main log immediately)
    if total_prs_for_repo == 0:
        if prs_loaded_from_main and prs_count_before_filter > 0:
            write_log(
                f"Worker: repo {repo_full_name} had {prs_count_before_filter} PRs from main but 0 after processing",
                level="WARNING"
            )
        elif not prs_loaded_from_main and original_count > 0:
            write_log(
                f"Worker: repo {repo_full_name} had {original_count} PRs from shards but 0 after filter",
                level="WARNING"
            )
        elif not prs_loaded_from_main and original_count == 0:
            write_log(
                f"Worker: repo {repo_full_name} loaded no PR data (from_main={prs_loaded_from_main})",
                level="WARNING"
            )
    else:
        if prs_loaded_from_main:
            pass

    # Step 2: Apply average thresholds to each PR
    for pr_data in prs:
        processed += 1
        pr_id = f"{repo_full_name}#{pr_data.get('number', processed)}"
        pr_number = pr_data.get("number", "unknown")
        try:
            pr_number = pr_data.get("number", "unknown")
            pr_url = pr_data.get("url", "no link")

            pr_times = extract_pr_times(pr_data, repo_full_name, repo_commits, repo_metadata)
            pr_num_int = pr_data.get("number")
            m = pr_metrics_map.get(pr_num_int) if isinstance(pr_num_int, int) else None
            
            # If PR has commits list but no lag1_* metrics, extract author_date from commits
            if not m and pr_data.get("commits") and isinstance(pr_data.get("commits"), list):
                commits_list = pr_data.get("commits", [])
                commit_author_dates = []
                for commit in commits_list:
                    if isinstance(commit, dict):
                        # Support multiple commit formats: 1. GitHub API commit.commit.author.date
                        commit_obj = commit.get("commit")
                        if isinstance(commit_obj, dict):
                            author = commit_obj.get("author")
                            if isinstance(author, dict):
                                author_date_str = author.get("date")
                                if author_date_str:
                                    author_date_dt = parse_date(author_date_str)
                                    if author_date_dt:
                                        commit_author_dates.append(author_date_dt)
                                        continue
                        # 2. Direct: commit.author.date
                        author = commit.get("author")
                        if isinstance(author, dict):
                            author_date_str = author.get("date")
                            if author_date_str:
                                author_date_dt = parse_date(author_date_str)
                                if author_date_dt:
                                    commit_author_dates.append(author_date_dt)
                                    continue
                        # 3. Flat: commit.author_date
                        author_date_str = commit.get("author_date")
                        if author_date_str:
                            author_date_dt = parse_date(author_date_str)
                            if author_date_dt:
                                commit_author_dates.append(author_date_dt)
                if commit_author_dates:
                    pr_times["all_author_dates"] = commit_author_dates
                    # Debug: if commit count mismatch, log (only first few PRs)
                    if processed <= Config.DEBUG_SAMPLE and len(commits_list) != len(commit_author_dates):
                        write_log(
                            f"PR {pr_number} ({repo_full_name}): "
                            f"commits list has {len(commits_list)} items but only {len(commit_author_dates)} author_date extracted",
                            level="WARNING"
                        )
            
            if m:
                # Build Lag1 result from PR-level metrics
                lag_result = {
                    "is_lag1": bool((m.get("lag1_over_threshold_count") or 0) > 0),
                    "lag1_max_days": float(m.get("lag1_max_days") or 0.0),
                    "lag1_avg_days": float(m.get("lag1_avg_days") or 0.0),
                    "lag1_commit_count": int(m.get("commit_count") or 0),
                    "lag1_over_threshold_count": int(m.get("lag1_over_threshold_count") or 0),
                    "lag1_desc": (
                        f"{int(m.get('lag1_over_threshold_count') or 0)}/"
                        f"{int(m.get('commit_count') or 0)} commits over threshold, "
                        f"max lag {float(m.get('lag1_max_days') or 0.0)} days"
                        if int(m.get("commit_count") or 0) > 0
                        else "no valid commit data"
                    ),
                    # Lag2/Lag3 use classify_lag
                    "is_lag2": False,
                    "lag2_days": 0.0,
                    "lag2_desc": "no lag",
                    "is_lag3": False,
                    "lag3_days": 0.0,
                    "lag3_desc": "no lag",
                    "lag_combination": "None",
                }
                # Re-evaluate Lag1 with average threshold (recompute per-commit lag if needed)
                # If pr_times has no all_author_dates, try to extract from commits list
                if lag1_threshold is not None:
                    if not pr_times.get("all_author_dates") and pr_data.get("commits") and isinstance(pr_data.get("commits"), list):
                        commits_list = pr_data.get("commits", [])
                        commit_author_dates = []
                        for commit in commits_list:
                            if isinstance(commit, dict):
                                # Support multiple commit formats: 1. GitHub API commit.commit.author.date
                                commit_obj = commit.get("commit")
                                if isinstance(commit_obj, dict):
                                    author = commit_obj.get("author")
                                    if isinstance(author, dict):
                                        author_date_str = author.get("date")
                                        if author_date_str:
                                            author_date_dt = parse_date(author_date_str)
                                            if author_date_dt:
                                                commit_author_dates.append(author_date_dt)
                                                continue
                                # 2. Direct: commit.author.date
                                author = commit.get("author")
                                if isinstance(author, dict):
                                    author_date_str = author.get("date")
                                    if author_date_str:
                                        author_date_dt = parse_date(author_date_str)
                                        if author_date_dt:
                                            commit_author_dates.append(author_date_dt)
                                            continue
                                # 3. Flat: commit.author_date
                                author_date_str = commit.get("author_date")
                                if author_date_str:
                                    author_date_dt = parse_date(author_date_str)
                                    if author_date_dt:
                                        commit_author_dates.append(author_date_dt)
                        if commit_author_dates:
                            pr_times["all_author_dates"] = commit_author_dates
                    
                    if pr_times.get("all_author_dates") and pr_times.get("pr_created"):
                        pr_created = pr_times["pr_created"]
                        # Only commits before PR created
                        commit_dates = [d for d in pr_times["all_author_dates"] if d <= pr_created]
                        
                        if commit_dates:
                            last_commit_datetime = max(commit_dates)
                            lag1_delta = (pr_created - last_commit_datetime).total_seconds() / 86400
                            
                            lag_result["lag1_commit_count"] = len(commit_dates)
                            lag_result["lag1_avg_days"] = round(lag1_delta, 2)
                            lag_result["lag1_max_days"] = round(lag1_delta, 2)  # Single value so max=avg
                            
                            if lag1_delta > lag1_threshold:
                                lag_result["is_lag1"] = True
                                lag_result["lag1_over_threshold_count"] = 1
                            else:
                                lag_result["is_lag1"] = False
                                lag_result["lag1_over_threshold_count"] = 0
                            
                            lag_result["lag1_desc"] = (
                                f"lag {lag_result['lag1_avg_days']} days ({'over' if lag_result['is_lag1'] else 'under'} threshold {round(lag1_threshold, 2)} days), "
                                f"{lag_result['lag1_commit_count']} commits"
                            )
                
                # Reuse classify_lag for Lag2/Lag3 (average threshold)
                # Handle Lag2 and Lag3 separately; one threshold None does not affect the other
                lag23 = classify_lag({**pr_times, "all_author_dates": []}, lag1_threshold=None, lag2_threshold=lag2_threshold, lag3_threshold=lag3_threshold, repo_full_name=repo_full_name, repo_commits=repo_commits, fork_lag3_map=fork_lag3_map)
                lag_result["is_lag2"] = lag23["is_lag2"]
                lag_result["lag2_days"] = lag23["lag2_days"]
                lag_result["lag2_desc"] = lag23["lag2_desc"]
                lag_result["is_lag3"] = lag23["is_lag3"]
                lag_result["lag3_days"] = lag23["lag3_days"]
                lag_result["lag3_desc"] = lag23["lag3_desc"]
                lag_list = [x for x, ok in [("Lag1", lag_result["is_lag1"]), ("Lag2", lag_result["is_lag2"]), ("Lag3", lag_result["is_lag3"])] if ok]
                lag_result["lag_combination"] = ",".join(lag_list) if lag_list else "None"

                # Summary for "all commit author dates (ISO)" column (from PR-level)
                min_ad = m.get("min_author_date")
                max_ad = m.get("max_author_date")
                pr_times["all_author_dates"] = []  # Do not output full list
                pr_times["_commit_dates_summary"] = f"count={int(m.get('commit_count') or 0)};min={min_ad};max={max_ad}"
            else:
                # If PR has no commits field and no metrics, do not use whole-repo commits; keep all_author_dates empty
                if not pr_times.get("all_author_dates"):
                    pr_times["all_author_dates"] = []  # Explicit N/A, not whole-repo commits
                # Handle Lag1, Lag2, Lag3 separately; one threshold None does not affect others
                lag_result = classify_lag(pr_times, lag1_threshold=lag1_threshold, lag2_threshold=lag2_threshold, lag3_threshold=lag3_threshold, repo_full_name=repo_full_name, repo_commits=repo_commits, fork_lag3_map=fork_lag3_map)

            if not pr_times["all_author_dates"]:
                missing_field_stats["no_any_commit_date"] += 1
            if not pr_times["pr_created"]:
                missing_field_stats["no_pr_created"] += 1
            if not pr_times["pr_merged"]:
                missing_field_stats["no_pr_merged"] += 1
            if not pr_times["origin_pushed"]:
                missing_field_stats["no_origin_pushed"] += 1
            if not pr_times["fork_pushed"]:
                missing_field_stats["no_fork_pushed"] += 1
            if not (pr_times["fork_pushed"] and pr_times["origin_pushed"]):
                missing_field_stats["no_fork_origin"] += 1
            # Diagnostic: why fork_pushed is N/A
            head_repo_full_name = pr_data.get("head_repo_full_name")
            if not head_repo_full_name:
                missing_field_stats["no_head_repo_full_name"] += 1
            elif isinstance(head_repo_full_name, str) and head_repo_full_name != repo_full_name:
                # Has head_repo_full_name != origin but fork_pushed still None
                if not pr_times["fork_pushed"] and head_repo_full_name not in repo_metadata:
                    missing_field_stats["fork_not_in_metadata"] += 1

            # Extract and classify PR user
            pr_user = pr_data.get("user")
            pr_user_type = classify_pr_user(pr_user)
            
            rows.append(
                {
                    "PR_ID": pr_id,
                    "repo_name": repo_full_name,
                    "PR_number": pr_number,
                    "PR_url": pr_url,
                    "all_commit_author_dates_ISO": (
                        pr_times.get("_commit_dates_summary")
                        if pr_times.get("_commit_dates_summary")
                        else (
                            "N/A"
                            if not pr_times["all_author_dates"]
                            else (
                                f"count={len(pr_times['all_author_dates'])};"
                                f"min={min(pr_times['all_author_dates']).isoformat()};"
                                f"max={max(pr_times['all_author_dates']).isoformat()}"
                                if Config.MAX_COMMIT_DATES_IN_CELL == 0
                                else ";".join(
                                    d.isoformat()
                                    for d in pr_times["all_author_dates"][: Config.MAX_COMMIT_DATES_IN_CELL]
                                )
                            )
                        )
                    ),
                    "PR_created_at": pr_times["pr_created"].isoformat() if pr_times["pr_created"] else "N/A",
                    "PR_merged_at": pr_times["pr_merged_at"].isoformat() if pr_times["pr_merged_at"] else "N/A",
                    "PR_closed_at": pr_times["pr_closed_at"].isoformat() if pr_times["pr_closed_at"] else "N/A",
                    "fork_last_updated": pr_times["fork_pushed"].isoformat() if pr_times["fork_pushed"] else "N/A",
                    "origin_last_updated": pr_times["origin_pushed"].isoformat() if pr_times["origin_pushed"] else "N/A",
                    "has_Lag1": lag_result["is_lag1"],
                    "commit_count": lag_result["lag1_commit_count"],
                    "commits_over_threshold": lag_result["lag1_over_threshold_count"],
                    "max_Lag1_days": lag_result["lag1_max_days"],
                    "avg_Lag1_days": lag_result["lag1_avg_days"],
                    "Lag1_desc": lag_result["lag1_desc"],
                    "has_Lag2": lag_result["is_lag2"],
                    "Lag2_days": lag_result["lag2_days"],
                    "Lag2_desc": lag_result["lag2_desc"],
                    "has_Lag3": lag_result["is_lag3"],
                    "Lag3_days": lag_result["lag3_days"],
                    "Lag3_desc": lag_result["lag3_desc"],
                    "PR_head_repo": head_repo_full_name if head_repo_full_name else "N/A",
                    "Lag_combination": lag_result["lag_combination"],
                    "analysis_time": datetime.now(timezone.utc).isoformat(),
                    "PR_user_type": pr_user_type,
                }
            )
        except Exception as e:
            failed_prs.append({
                "pr_id": pr_id,
                "pr_number": pr_number,
                "error": str(e)
            })
            continue

    # Log failed PRs to stats
    if failed_prs:
        missing_field_stats["failed_prs"] = len(failed_prs)
        # Log only first 10 failed PRs to avoid log spam
        if len(failed_prs) <= 10:
            for failed in failed_prs:
                write_log(f"Warning: PR processing failed: {failed['pr_id']} -> {failed['error']}", level="WARNING")
        else:
            for failed in failed_prs[:5]:
                write_log(f"Warning: PR processing failed: {failed['pr_id']} -> {failed['error']}", level="WARNING")
            write_log(f"Warning: ... and {len(failed_prs) - 5} more PRs failed", level="WARNING")

    return repo_full_name, rows, missing_field_stats, processed


def load_repo_metadata(json_path: Path, json_path_all: Optional[Path] = None) -> Dict[str, Dict]:
    """Load repo metadata from JSON file.
    
    Load from json_path (stars >= 10) first, then supplement from json_path_all (all forks).
    Returns: {repo_full_name: {"updated_at": ..., "pushed_at": ...}}
    """
    repo_metadata = {}
    
    # 1. Load repos with stars >= 10 first (main source)
    if json_path.exists():
        write_log(f"Loading repo metadata (stars >= 10): {json_path}")
        try:
            with json_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
            
            for repo_data in data.get("repositories", []):
                # Handle origin
                original = repo_data.get("original_repo")
                if original:
                    full_name = original.get("full_name", "")
                    if full_name:
                        repo_metadata[full_name] = {
                            "updated_at": original.get("updated_at"),
                            "pushed_at": original.get("pushed_at") or original.get("updated_at")
                        }
                
                # Handle forks
                for fork in repo_data.get("forks", []):
                    full_name = fork.get("full_name", "")
                    if full_name:
                        repo_metadata[full_name] = {
                            "updated_at": fork.get("updated_at"),
                            "pushed_at": fork.get("pushed_at") or fork.get("updated_at")
                        }
            write_log(f"OK: Loaded metadata for {len(repo_metadata)} repos from {json_path.name}")
        except Exception as e:
            write_log(f"Warning: Failed to load {json_path}: {e}", level="WARNING")
    else:
        write_log(f"Warning: Repo metadata file not found: {json_path}", level="WARNING")
    
    # 2. Supplement with forks (stars < 10) from full file if provided
    if json_path_all and json_path_all.exists() and json_path_all != json_path:
        write_log(f"Supplementing repo metadata (all forks): {json_path_all}")
        try:
            # Use ijson streaming to avoid loading entire large file
            if ijson:
                added_count = 0
                with json_path_all.open("rb") as f:
                    for prefix, event, value in ijson.parse(f):
                        if prefix == "repositories.item.original_repo.full_name" and event == "string":
                            full_name = value
                            pass
                file_size_mb = json_path_all.stat().st_size / (1024 * 1024)
                if file_size_mb > 500:  # Only stream if > 500MB
                    write_log(f"Warning: {json_path_all.name} too large ({file_size_mb:.1f}MB), skipping full load", level="WARNING")
                else:
                    with json_path_all.open("r", encoding="utf-8") as f:
                        data_all = json.load(f)
                    for repo_data in data_all.get("repositories", []):
                        # Only add forks not already in repo_metadata
                        for fork in repo_data.get("forks", []):
                            full_name = fork.get("full_name", "")
                            if full_name and full_name not in repo_metadata:
                                repo_metadata[full_name] = {
                                    "updated_at": fork.get("updated_at"),
                                    "pushed_at": fork.get("pushed_at") or fork.get("updated_at")
                                }
                                added_count += 1
                    write_log(f"OK: Supplemented {added_count} fork metadata from {json_path_all.name}")
            else:
                # No ijson; use json.load
                file_size_mb = json_path_all.stat().st_size / (1024 * 1024)
                if file_size_mb > 500:
                    write_log(f"Warning: {json_path_all.name} too large ({file_size_mb:.1f}MB) and ijson not installed, skipping full load", level="WARNING")
                else:
                    with json_path_all.open("r", encoding="utf-8") as f:
                        data_all = json.load(f)
                    added_count = 0
                    for repo_data in data_all.get("repositories", []):
                        for fork in repo_data.get("forks", []):
                            full_name = fork.get("full_name", "")
                            if full_name and full_name not in repo_metadata:
                                repo_metadata[full_name] = {
                                    "updated_at": fork.get("updated_at"),
                                    "pushed_at": fork.get("pushed_at") or fork.get("updated_at")
                                }
                                added_count += 1
                    write_log(f"OK: Supplemented {added_count} fork metadata from {json_path_all.name}")
        except Exception as e:
            write_log(f"Warning: Failed to load {json_path_all}: {e}", level="WARNING")
    
    if not repo_metadata:
        write_log(f"Warning: No repo metadata loaded; Lag3 cannot be computed", level="WARNING")
    else:
        write_log(f"OK: Loaded metadata for {len(repo_metadata)} repos total")
    return repo_metadata


# ---------------------- Utility functions ----------------------
def get_nested_field(doc: Dict, path: List[str]) -> Optional[Any]:
    """
    Safely get nested field by path, e.g.:
    get_nested_field(doc, ["Data", "PR Info", "base", "repo", "pushed_at"])
    """
    current: Any = doc
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def classify_pr_user(user: Optional[Any]) -> str:
    """
    Classify user type from PR's user field (bot classification).
    
    Args:
        user: PR's user field (may be string username or dict with login)
    
    Returns:
        Classification name, or empty string if not in any category
    """
    if not user:
        return ""
    
    # If user is dict, try login field
    if isinstance(user, dict):
        user = user.get("login") or user.get("name") or ""
    
    user_str = str(user).strip().lower()
    if not user_str:
        return ""
    
    # 1. Common Dependency & Security Update Bots
    dependency_bots = [
        "dependabot", "dependabot-preview", "dependabot[bot]",
        "renovate", "renovate-bot", "renovate[bot]",
        "snyk-bot", "snyk-bot[bot]",
        "pyup-bot",
        "greenkeeper", "greenkeeper[bot]",
        "white-source-bolt", "whitesource-bolt-for-github[bot]"
    ]
    if user_str in dependency_bots:
        return "Common Dependency & Security Update Bots"
    
    # 2. CI / Automation / Maintenance Bots
    ci_bots = [
        "github-actions", "github-actions[bot]",
        "travis-ci", "travis-ci[bot]",
        "circleci", "circleci[bot]",
        "appveyor", "appveyor[bot]",
        "bors", "bors[bot]",
        "mergify", "mergify[bot]",
        "semantic-release-bot", "semantic-release[bot]"
    ]
    if user_str in ci_bots:
        return "CI / Automation / Maintenance Bots"
    
    # 3. Formatting / Linting / Refactoring Bots
    formatting_bots = [
        "prettier-bot", "prettier[bot]",
        "clang-format-bot", "clang-format[bot]",
        "styleci", "styleci[bot]",
        "rubocop-bot",
        "eslint-bot",
        "black[bot]",
        "autopep8-bot"
    ]
    if user_str in formatting_bots:
        return "Formatting / Linting / Refactoring Bots"
    
    # 4. Documentation & Localization Bots
    doc_bots = [
        "allcontributors", "allcontributors[bot]",
        "readthedocs", "readthedocs-bot",
        "crowdin", "crowdin[bot]",
        "transifex", "transifex[bot]"
    ]
    if user_str in doc_bots:
        return "Documentation & Localization Bots"
    
    # 5. Code Quality / Analysis Bots
    quality_bots = [
        "codecov", "codecov[bot]",
        "coveralls", "coveralls[bot]",
        "sonarqube",
        "sonarcloud", "sonarcloud[bot]",
        "lgtm-com[bot]"
    ]
    if user_str in quality_bots:
        return "Code Quality / Analysis Bots"
    
    # 6. Other (username ending with "bot" or "bots")
    if user_str.endswith("bot") or user_str.endswith("bots"):
        return "Other"
    
    # Not in any category
    return ""


def parse_date(d: Optional[Any]) -> Optional[datetime]:
    """
    Try to convert various time formats to datetime with UTC.
    Supports: datetime, timestamp (sec/ms), ISO8601 / common date strings.
    """
    if not d:
        return None

    if isinstance(d, datetime):
        return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(
            timezone.utc
        )

    if isinstance(d, (int, float)):
        try:
            # Distinguish sec vs ms
            ts = d / 1000 if d > 10**12 else d
            return datetime.fromtimestamp(ts, tz=timezone.utc)
        except Exception:
            return None

    s = str(d).strip().replace("Z", "+00:00")
    fmts = [
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%dT%H:%M:%S%z",
        # git common: 2025-12-17 07:20:39 +0000
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass

    try:
        return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def write_log(content: str, level: str = "INFO") -> None:
    log_line = f"[{datetime.now(timezone.utc).isoformat()}] [{level}] {content}\n"
    with open(Config.LOG_FILE, "a", encoding="utf-8") as f:
        f.write(log_line)
    try:
        print(log_line.strip())
    except UnicodeEncodeError:
        # Windows console may not support some Unicode; use safe encoding
        import sys
        if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
            safe_line = log_line.encode('utf-8', errors='replace').decode('utf-8', errors='replace')
            print(safe_line.strip())
        else:
            print(log_line.strip())


# ---------------------- Extract time info from single PR record ----------------------
def extract_pr_times(
    pr_data: Dict,
    repo_full_name: str,
    repo_commits: Dict[str, List[Dict]],
    repo_metadata: Dict[str, Dict]
) -> Dict[str, Any]:
    """
    Extract time info from PR data.
    
    Args:
        pr_data: PR data (from repos_prs.json)
        repo_full_name: Repo full name (origin)
        repo_commits: Commits from commits_trees.json
        repo_metadata: Repo metadata from github_projects_filtered_stars_10.json
    
    Returns:
        Dict with all time fields
    """
    times = {
        "all_author_dates": [],
        "pr_created": None,
        "pr_merged": None,  # Backward compat: prefer merged_at, else closed_at (for Lag2)
        "pr_merged_at": None,  # PR merge time (merged_at)
        "pr_closed_at": None,  # PR close time (closed_at)
        "fork_pushed": None,
        "origin_pushed": None,
        "head_repo_full_name": None,  # Fork repo name for Lag3
    }

    # 0) Extract head_repo_full_name (fork repo) for Lag3
    head_repo_full_name = pr_data.get("head_repo_full_name")
    times["head_repo_full_name"] = head_repo_full_name

    # 1) Commit author_date list is pre-parsed per repo and injected into times["all_author_dates"] by worker.
    # 2) PR times (created/merged/closed) from pr_data
    times["pr_created"] = parse_date(pr_data.get("created_at"))
    times["pr_merged_at"] = parse_date(pr_data.get("merged_at"))
    times["pr_closed_at"] = parse_date(pr_data.get("closed_at"))
    # pr_merged for Lag2: prefer merged_at, else closed_at
    times["pr_merged"] = times["pr_merged_at"] or times["pr_closed_at"]

    # 3) Repo pushed_at from repo_metadata
    # origin_pushed: current repo (origin) pushed_at
    if repo_full_name in repo_metadata:
        metadata = repo_metadata[repo_full_name]
        times["origin_pushed"] = parse_date(metadata.get("pushed_at") or metadata.get("updated_at"))
    
    # fork_pushed: from PR head_repo_full_name (fork repo pushed_at)
    if isinstance(head_repo_full_name, str) and head_repo_full_name and head_repo_full_name != repo_full_name:
        if head_repo_full_name in repo_metadata:
            fork_metadata = repo_metadata[head_repo_full_name]
            times["fork_pushed"] = parse_date(fork_metadata.get("pushed_at") or fork_metadata.get("updated_at"))
        else:
            # Fork not in metadata (e.g. stars < 10); cannot get pushed_at
            times["fork_pushed"] = None
    else:
        # Self-PR or head_repo N/A; use origin pushed_at
        times["fork_pushed"] = times["origin_pushed"]

    return times


# ---------------------- Lag classification logic ----------------------
def classify_lag(times: Dict[str, Any], lag1_threshold: Optional[float] = None, lag2_threshold: Optional[float] = None, lag3_threshold: Optional[float] = None, repo_full_name: Optional[str] = None, repo_commits: Optional[Dict[str, List[Dict]]] = None, fork_lag3_map: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Same logic as classify_lag in old lag-classification script.
    Uses dynamic thresholds (per-origin average); threshold args required.
    
    Args:
        times: Time info dict
        lag1_threshold: Lag1 threshold (origin avg lag1_days). Lag1 = PR created - last commit created
        lag2_threshold: Lag2 threshold (origin avg lag2_days)
        lag3_threshold: Lag3 threshold (origin avg lag3_days)
        fork_lag3_map: {fork_full_name: {"lag3_avg", "lag3_median", "pull_count"}} for Lag3
    """
    lag_result = {
        "is_lag1": False,
        "lag1_max_days": 0.0,
        "lag1_avg_days": 0.0,
        "lag1_commit_count": len(times["all_author_dates"]),
        "lag1_over_threshold_count": 0,
        "lag1_desc": "no valid commit data",
        "is_lag2": False,
        "lag2_days": 0.0,
        "lag2_desc": "no lag",
        "is_lag3": False,
        "lag3_days": 0.0,
        "lag3_desc": "no lag",
        "lag_combination": "None",
    }

    # Lag1: time between last commit and PR created
    if times["all_author_dates"] and times["pr_created"]:
        pr_created = times["pr_created"]
        commit_dates = [d for d in times["all_author_dates"] if d <= pr_created]
        
        if commit_dates:
            last_commit_datetime = max(commit_dates)
            lag1_delta = (pr_created - last_commit_datetime).total_seconds() / 86400
            lag_result["lag1_commit_count"] = len(commit_dates)
            lag_result["lag1_avg_days"] = round(lag1_delta, 2)
            lag_result["lag1_max_days"] = round(lag1_delta, 2)  # Single value so max=avg
            
            if lag1_threshold is not None:
                lag1_thresh = lag1_threshold
                if lag1_delta > lag1_thresh:
                    lag_result["is_lag1"] = True
                    lag_result["lag1_over_threshold_count"] = 1
                    lag_result["lag1_desc"] = (
                        f"lag {lag_result['lag1_avg_days']} days (over threshold {round(lag1_thresh, 2)} days), "
                        f"{lag_result['lag1_commit_count']} commits"
                    )
                else:
                    lag_result["lag1_over_threshold_count"] = 0
                    lag_result["lag1_desc"] = (
                        f"lag {lag_result['lag1_avg_days']} days (under threshold {round(lag1_thresh, 2)} days), "
                        f"{lag_result['lag1_commit_count']} commits"
                    )
            else:
                lag_result["lag1_over_threshold_count"] = 0
                lag_result["lag1_desc"] = (
                    f"threshold not computable (origin has insufficient PR data), "
                    f"lag {lag_result['lag1_avg_days']} days, {lag_result['lag1_commit_count']} commits"
                )
        else:
            lag_result["lag1_desc"] = "all commits after PR created (invalid)"
            lag_result["lag1_commit_count"] = len(times["all_author_dates"])
            lag_result["lag1_over_threshold_count"] = 0
    else:
        if not times["all_author_dates"]:
            lag_result["lag1_desc"] = "no commit author_date data"
        else:
            lag_result["lag1_desc"] = "no PR created time"

    # Lag2: PR created -> PR merged/closed
    if times["pr_created"] and times["pr_merged"]:
        delta_days = (times["pr_merged"] - times["pr_created"]).total_seconds() / 86400
        lag_result["lag2_days"] = round(delta_days, 2)
        if lag2_threshold is not None:
            lag2_thresh = lag2_threshold
            if delta_days > lag2_thresh:
                lag_result["is_lag2"] = True
                lag_result["lag2_desc"] = (
                    f"lag {lag_result['lag2_days']} days (over threshold {round(lag2_thresh, 2)} days)"
                )
            else:
                lag_result["lag2_desc"] = (
                    f"no lag ({lag_result['lag2_days']} days, under threshold {round(lag2_thresh, 2)} days)"
                )
        else:
            lag_result["lag2_desc"] = (
                f"threshold not computable (origin has insufficient PR data), lag {lag_result['lag2_days']} days"
            )
    else:
        lag_result["lag2_desc"] = "N/A PR created or merged/closed time"

    # Lag3: sync time gap between main repo and fork (from fork_lag3_map)
    head_repo_full_name = times.get("head_repo_full_name")
    
    if head_repo_full_name and repo_full_name and head_repo_full_name != repo_full_name:
        if fork_lag3_map and head_repo_full_name in fork_lag3_map:
            fork_lag3_info = fork_lag3_map[head_repo_full_name]
            delta_days = fork_lag3_info.get("lag3_avg", 0.0)  # Use average
            lag_result["lag3_days"] = round(delta_days, 2)
            
            pull_count = fork_lag3_info.get("pull_count", 0)
            lag3_median = fork_lag3_info.get("lag3_median", 0.0)
            
            if lag3_threshold is not None:
                lag3_thresh = lag3_threshold
                if delta_days > lag3_thresh:
                    lag_result["is_lag3"] = True
                    lag_result["lag3_desc"] = (
                        f"lag {lag_result['lag3_days']} days (over threshold {round(lag3_thresh, 2)} days), "
                        f"pull_count={pull_count}, median={round(lag3_median, 2)} days"
                    )
                else:
                    lag_result["lag3_desc"] = (
                        f"no lag ({lag_result['lag3_days']} days, under threshold {round(lag3_thresh, 2)} days), "
                        f"pull_count={pull_count}, median={round(lag3_median, 2)} days"
                    )
            else:
                lag_result["lag3_desc"] = (
                    f"threshold not computable (origin has insufficient PR data), "
                    f"lag {lag_result['lag3_days']} days, pull_count={pull_count}, median={round(lag3_median, 2)} days"
                )
        else:
            lag_result["lag3_desc"] = "cannot get Lag3 for this fork (data N/A)"
    else:
        if not head_repo_full_name:
            lag_result["lag3_desc"] = "N/A fork repo info"
        elif head_repo_full_name == repo_full_name:
            lag_result["lag3_desc"] = "PR from same repo (not fork)"
        else:
            lag_result["lag3_desc"] = "N/A repo info"

    # Lag combination label
    lag_list = [
        f"Lag{i + 1}"
        for i, is_lag in enumerate(
            [lag_result["is_lag1"], lag_result["is_lag2"], lag_result["is_lag3"]]
        )
        if is_lag
    ]
    lag_result["lag_combination"] = ",".join(lag_list) if lag_list else "None"

    return lag_result


# ---------------------- Main: read from JSON and classify ----------------------
def main():
    write_log("=== Start PR Lag classification from repos_prs (shards) ===")

    import argparse

    parser = argparse.ArgumentParser(description="PR Lag classification (single-thread)")
    parser.add_argument(
        "--filter-only",
        action="store_true",
        help="Run only up to filter stage, no further processing (to check filter result)",
    )
    parser.add_argument(
        "--input-json",
        type=str,
        default=None,
        help="Input JSON path (e.g. repos_prs_from_downloaded_forks.json); if not set, use shard dir",
    )
    args = parser.parse_args()

    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    
    # Load all required data
    forks_with_unique_commits_json = current_dir / Config.FORKS_WITH_UNIQUE_COMMITS_JSON
    repos_prs_shards_dir = current_dir / Config.REPOS_PRS_SHARDS_DIR
    commits_trees_json = current_dir / Config.COMMITS_TREES_JSON
    commits_shards_dir = current_dir / Config.COMMITS_SHARDS_DIR
    projects_json = current_dir / Config.PROJECTS_JSON
    projects_json_all = current_dir / Config.PROJECTS_JSON_ALL
    clone_report_json = current_dir / Config.CLONE_REPORT_JSON
    
    write_log(f"Input files:")
    write_log(f"  - PR data: {repos_prs_shards_dir} (PR shard dir, read per shard)")
    write_log(f"  - Forks filter: {forks_with_unique_commits_json} (only PRs from forks with unique commits)")
    write_log(f"  - Commits shards: {commits_shards_dir}")
    write_log(f"  - Commits JSON: {commits_trees_json}")
    write_log(f"  - Repo metadata (stars >= 10): {projects_json}")
    write_log(f"  - Repo metadata (full): {projects_json_all}")
    write_log(f"  - Clone report: {clone_report_json}")
    write_log("")
    
    forks_with_unique_commits_set = load_forks_with_unique_commits(forks_with_unique_commits_json)
    if not forks_with_unique_commits_set:
        write_log("Error: No forks with unique commits loaded; cannot continue", level="ERROR")
        write_log("   Hint: run list_forks_with_unique_commits.py to generate forks_with_unique_commits.json", level="ERROR")
        return
    else:
        write_log(f"OK: Will process only PRs from {len(forks_with_unique_commits_set)} forks with unique commits")
        if len(forks_with_unique_commits_set) != 3823:
            write_log(
                f"   Warning: expected 3823 forks but loaded {len(forks_with_unique_commits_set)}; "
                f"check forks_with_unique_commits.json",
                level="WARNING"
            )
    
    repos_list = []
    
    try:
        if not repos_prs_shards_dir.exists():
            write_log(f"Warning: PR shard dir not found: {repos_prs_shards_dir}", level="WARNING")
            write_log(f"   Hint: run shard_repos_prs_to_jsonl.py to generate PR shards", level="WARNING")
            write_log(f"   Will fall back to repos_prs.json", level="WARNING")
        else:
            prs_file_count = len(list(repos_prs_shards_dir.glob("*.jsonl")))
            write_log(f"OK: PR shard dir exists, {prs_file_count} files")
        
        if not commits_shards_dir.exists():
            write_log(f"Warning: Commits shard dir not found: {commits_shards_dir}", level="WARNING")
            write_log(f"   Hint: run shard_commits_trees_to_jsonl.py to generate commits shards", level="WARNING")
        else:
            commits_file_count = len(list(commits_shards_dir.glob("*.jsonl")))
            write_log(f"OK: Commits shard dir exists, {commits_file_count} files")
        
        repo_metadata = load_repo_metadata(projects_json, projects_json_all)

        successful_repos = load_successful_repos_from_clone_report(clone_report_json)
        if successful_repos:
            write_log(f"Info: clone_report.json found ({len(successful_repos)} successful clones); will not filter PR data by it")
            write_log(f"   (all forks with unique commits are cloned and PR data exists)")

        if repos_prs_shards_dir.exists():
            for p in repos_prs_shards_dir.glob("*.jsonl"):
                name = p.stem.replace("__", "/")
                repos_list.append(name)
            if repos_list:
                write_log(f"OK: Got {len(repos_list)} repos from PR shard dir")
                write_log(f"Info: Will read PR data per shard to save memory")
            else:
                write_log("Warning: PR shard dir exists but no shard files found", level="WARNING")
        else:
            write_log("Warning: PR shard dir not found; cannot process PR data", level="WARNING")
        
        if not repos_list:
            write_log("Error: No repos to process", level="ERROR")
            write_log("   Hint: run shard_repos_prs_to_jsonl.py to generate PR shards", level="ERROR")
            return
        
        write_log("")
        write_log("=" * 80)
        write_log("Final stats (prep)")
        write_log(f"   - Forks (unique commits): {len(forks_with_unique_commits_set)}")
        write_log(f"   - Repos to process: {len(repos_list)}")
        write_log(f"   - PR source: REPOS_PRS_SHARDS_DIR (read per shard)")
        write_log(f"   - Expected PR count: 158384 (actual counted during processing)")
        
        if args.filter_only:
            write_log("")
            write_log("=" * 80)
            write_log("Filter-check mode")
            write_log(f"   - Repo list from REPOS_PRS_SHARDS_DIR")
            write_log(f"   - Repos to process: {len(repos_list)}")
            write_log(f"   - Filter: only PRs from forks with unique commits")
            write_log(f"   - PR count unknown until processing (shard data)")
            write_log("=" * 80)
            write_log("Filter check done; exiting (--filter-only)")
            write_log("   To see full stats, run without --filter-only")
            return
        
        write_log("=" * 80)
        write_log("")
    except Exception as e:
        write_log(f"Failed to load data: {e}\n{traceback.format_exc()}", level="ERROR")
        return

    total_repos = len(repos_list)
    total_prs = 0
    write_log(f"Preparing: {total_repos} repos (PR count will be counted during processing)")
    output_csv = current_dir / Config.OUTPUT_CSV

    # Main process: stream write CSV to avoid loading all rows into memory
    with output_csv.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()

        missing_field_stats = {
            "no_any_commit_date": 0,
            "no_pr_created": 0,
            "no_pr_merged": 0,
            "no_fork_origin": 0,
            "no_origin_pushed": 0,
            "no_fork_pushed": 0,
            "no_head_repo_full_name": 0,
            "fork_not_in_metadata": 0,
        }
        processed_prs = 0
        written_rows = 0
        
        repo_processing_stats = {}  # {repo_name: {"expected": int, "processed": int}}
        
        write_log("")
        write_log("=" * 80)
        write_log("Processing mode")
        write_log(f"   - Repo list: REPOS_PRS_SHARDS_DIR")
        write_log(f"   - Repos to process: {len(repos_list)}")
        write_log(f"   - PR source: read per shard (to save memory)")
        write_log(f"   - Filter: only PRs from forks with unique commits")
        write_log("=" * 80)
        write_log("")

        write_log(f"Starting single-thread processing: {total_repos} repos, {total_prs} PRs")
        
        completed_repos = 0
        for repo_name in repos_list:
            try:
                meta_entry = repo_metadata.get(repo_name)
                if repos_prs_shards_dir.exists():
                    prs_shard_path = repo_to_prs_shard_path(repos_prs_shards_dir, repo_name)
                else:
                    prs_shard_path = Path("")
                
                prs_list = None  # Worker reads from shard
                
                _, rows, miss_stats, repo_processed = process_repo_prs_worker(
                    repo_name,
                    str(prs_shard_path),
                    str(commits_shards_dir),
                    meta_entry,
                    forks_with_unique_commits_set,
                    prs_list,
                    repo_metadata=repo_metadata,
                )
            except Exception as e:
                write_log(f"Warning: Repo task failed: {repo_name} -> {e}", level="WARNING")
                repo_processed = 0
                rows = []
                miss_stats = {}

            completed_repos += 1
            processed_prs += repo_processed
            for k in missing_field_stats:
                missing_field_stats[k] += miss_stats.get(k, 0)

            for r in rows:
                writer.writerow(r)
            written_rows += len(rows)
            
            expected_prs = 0
            repo_processing_stats[repo_name] = {
                "expected": expected_prs,
                "processed": repo_processed
            }
            
            if repo_processed == 0:
                write_log(
                    f"Warning: repo {repo_name} processed 0 PRs (all filtered or shard missing)",
                    level="WARNING"
                )

            if completed_repos % 10 == 0 or completed_repos == total_repos:
                write_log(
                    f"Progress: repos {completed_repos}/{total_repos} | PRs {processed_prs} | written {written_rows} rows"
                )
                if completed_repos >= total_repos * 0.1 and processed_prs < 1000:
                    write_log(
                        f"Warning: processed {completed_repos}/{total_repos} repos but only {processed_prs} PRs; "
                        f"some repos may have all PRs filtered or failed",
                        level="WARNING"
                    )

    write_log(f"\nCSV written: {output_csv} ({written_rows} rows)")
    write_log("=" * 80)
    write_log("Final processing stats")
    write_log(f"   - Repos processed: {total_repos}")
    write_log(f"   - PRs processed: {processed_prs}")
    write_log(f"   - Rows written to CSV: {written_rows}")
    if processed_prs > 0:
        write_log(f"   - Avg PRs per repo: {round(processed_prs / total_repos, 2)}")
    if written_rows != processed_prs:
        write_log(f"   Note: written rows ({written_rows}) != processed PRs ({processed_prs}); some may have failed")
    
    if repo_processing_stats:
        repos_with_zero_processed = []
        repos_with_less_processed = []
        total_expected_prs = 0
        for repo_name, stats in repo_processing_stats.items():
            total_expected_prs += stats["expected"]
            if stats["expected"] > 0 and stats["processed"] == 0:
                repos_with_zero_processed.append((repo_name, stats["expected"]))
            elif stats["expected"] > 0 and stats["processed"] < stats["expected"]:
                repos_with_less_processed.append((repo_name, stats["expected"], stats["processed"]))
        
        write_log("")
        write_log("=" * 80)
        write_log("Repo processing stats")
        write_log(f"   - PRs actually processed: {processed_prs}")
        write_log(f"   - Repos with 0 PRs processed: {len(repos_with_zero_processed)}")
        if repos_with_zero_processed:
            write_log(f"   - These repos may have all PRs filtered or shard missing")
            write_log(f"   - First 20 repos with 0 PRs:")
            for repo_name in repos_with_zero_processed[:20]:
                write_log(f"      {repo_name}")
            if len(repos_with_zero_processed) > 20:
                write_log(f"      ... and {len(repos_with_zero_processed) - 20} more")
        
        write_log("=" * 80)
    
    write_log("=" * 80)

    write_log("\nData quality stats:")
    for field, count in missing_field_stats.items():
        if field == "failed_prs":
            rate = round(count / processed_prs * 100, 2) if processed_prs > 0 else 0
            write_log(f"   - Failed PRs: {count} ({rate}%)")
        else:
            rate = round(count / processed_prs * 100, 2) if processed_prs > 0 else 0
            field_name = field.replace('no_', 'N/A').replace('_', ' ')
            write_log(f"   - {field_name}: {count} ({rate}%)")
    
    if processed_prs > written_rows:
        failed_count = processed_prs - written_rows
        write_log("")
        write_log("=" * 80)
        write_log("Warning: Processing stats mismatch:")
        write_log(f"   - PRs attempted: {processed_prs}")
        write_log(f"   - PRs written to CSV: {written_rows}")
        write_log(f"   - Failed PRs: {failed_count} ({round(failed_count/processed_prs*100, 2)}%)")
        write_log(f"   Possible causes:")
        write_log(f"     1. PR data format issues")
        write_log(f"     2. Time parse failures")
        write_log(f"     3. Other processing errors")
        write_log("=" * 80)

    write_log("=== Task finished ===")


if __name__ == "__main__":
    main()


