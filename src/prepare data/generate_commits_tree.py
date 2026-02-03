#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Commits Tree.

For each origin and its forks, build a unified commits tree
recording each commit's info and which repos contain it.
"""

import json
import os
import shutil
import subprocess
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from threading import Lock
from typing import Dict, List, Set, Optional

# ===================== Config =====================
# Multi-threading: ThreadPoolExecutor can use multiple cores because:
# 1. git log and git clone are external processes, not limited by GIL
# 2. Main time is I/O wait for git log output, which releases GIL
# 3. String parsing is fast and not the bottleneck
# For CPU-intensive tasks, consider ProcessPoolExecutor
MAX_WORKERS = 10  # Max worker threads, tune per CPU cores

# Thread lock for print output
print_lock = Lock()


def thread_safe_print(*args, **kwargs):
    """Thread-safe print"""
    with print_lock:
        print(*args, **kwargs)


def is_valid_git_repo(repo_dir: Path) -> bool:
    """Check if directory is a valid git repo"""
    if not repo_dir.exists() or not repo_dir.is_dir():
        return False
    
    git_dir = repo_dir / ".git"
    if not git_dir.exists() or not git_dir.is_dir():
        return False
    
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-dir"],
            cwd=repo_dir,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5
        )
        return result.returncode == 0
    except Exception:
        return False


def get_all_commits(repo_dir: Path) -> List[Dict]:
    """Get all commits in the repo.

    Returns:
        List[Dict]: Commit list with hash, author, date, message, parents
    """
    if not is_valid_git_repo(repo_dir):
        return []
    
    try:
        # Special delimiter to avoid clashes with commit message
        # Format: hash|||author_name|||author_email|||author_date|||committer_name|||committer_email|||committer_date|||parent_hashes|||message
        result = subprocess.run(
            ["git", "log", "--all", "--format=%H|||%an|||%ae|||%ai|||%cn|||%ce|||%ci|||%P|||%B", "--reverse"],
            cwd=repo_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
            # No timeout to allow large repos
        )
        
        # Manual decode for encoding errors
        try:
            output_text = result.stdout.decode('utf-8', errors='replace')
        except Exception as e:
            thread_safe_print(f"  Warning: decode failed, trying other encoding: {e}")
            try:
                output_text = result.stdout.decode('latin-1', errors='replace')
            except Exception:
                output_text = result.stdout.decode('utf-8', errors='ignore')
        
        if result.returncode != 0:
            stderr_text = result.stderr.decode('utf-8', errors='replace') if result.stderr else ""
            thread_safe_print(f"  Warning: get commits failed: {stderr_text[:100]}")
            return []
        
        commits = []
        # Use |||ENDCOMMIT||| as end marker per commit
        output = output_text
        
        # Split by |||; message is last and may contain newlines
        lines = output.split('\n')
        current_commit = None
        current_message_lines = []
        
        for line in lines:
            if not line:
                if current_commit:
                    # End current commit message
                    current_commit["message"] = '\n'.join(current_message_lines).strip()
                    commits.append(current_commit)
                    current_commit = None
                    current_message_lines = []
                continue
            
            # Check if new commit line (contains |||)
            if '|||' in line:
                # Save previous commit
                if current_commit:
                    current_commit["message"] = '\n'.join(current_message_lines).strip()
                    commits.append(current_commit)
                
                # Parse new commit
                parts = line.split('|||', 7)  # Max 7 splits, rest is message start
                if len(parts) < 8:
                    continue
                
                commit_hash = parts[0]
                author_name = parts[1]
                author_email = parts[2]
                author_date = parts[3]
                committer_name = parts[4]
                committer_email = parts[5]
                committer_date = parts[6]
                parent_hashes_str = parts[7]
                message_start = parts[8] if len(parts) > 8 else ""
                
                parent_hashes = parent_hashes_str.strip().split() if parent_hashes_str.strip() else []
                
                current_commit = {
                    "hash": commit_hash,
                    "author": {
                        "name": author_name,
                        "email": author_email,
                        "date": author_date
                    },
                    "committer": {
                        "name": committer_name,
                        "email": committer_email,
                        "date": committer_date
                    },
                    "message": "",
                    "parents": parent_hashes
                }
                current_message_lines = [message_start] if message_start else []
            else:
                # Message continuation line
                if current_commit:
                    current_message_lines.append(line)
        
        # Handle last commit
        if current_commit:
            current_commit["message"] = '\n'.join(current_message_lines).strip()
            commits.append(current_commit)
        
        return commits
    except Exception as e:
        thread_safe_print(f"  Warning: get commits error: {e}")
        return []


def load_downloaded_repos(report_path: Path) -> Set[str]:
    """Load set of successfully cloned repos from clone_report.json"""
    if not report_path.exists():
        return set()
    
    with report_path.open("r", encoding="utf-8") as f:
        report_data = json.load(f)
    
    downloaded = set()
    for repo_info in report_data.get("success_repos", []):
        repo_name = repo_info.get("repo_name", "")
        if repo_name:
            downloaded.add(repo_name)
    
    return downloaded


def delete_and_redownload_repo(repo_name: str, cloned_dir: Path) -> bool:
    """Delete and re-clone repo (no timeout).

    Args:
        repo_name: Repo name
        cloned_dir: Clone directory

    Returns:
        bool: Success or not
    """
    repo_dir = cloned_dir / repo_name.replace("/", "__")
    repo_url = f"https://github.com/{repo_name}.git"
    
    thread_safe_print(f"      Found 0 commits, deleting and re-cloning: {repo_name}")
    
    # Remove existing directory
    if repo_dir.exists():
        try:
            thread_safe_print(f"        Removing dir: {repo_dir}")
            shutil.rmtree(repo_dir)
        except Exception as e:
            thread_safe_print(f"        Failed to remove dir: {e}")
            return False
    
    # Disable git interactive prompts
    env = os.environ.copy()
    env['GIT_TERMINAL_PROMPT'] = '0'
    env['GIT_ASKPASS'] = 'echo'
    
    # Re-clone (no timeout)
    try:
        thread_safe_print(f"        Starting re-clone: {repo_url}")
        result = subprocess.run(
            ["git", "clone", repo_url, str(repo_dir)],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
            # No timeout for long clones
        )
        
        if result.returncode == 0:
            thread_safe_print(f"        Re-clone success: {repo_name}")
            return True
        else:
            error_msg = result.stderr.decode('utf-8', errors='replace') if result.stderr else ""
            thread_safe_print(f"        Re-clone failed: {error_msg[:200]}")
            return False
    except Exception as e:
        thread_safe_print(f"        Re-clone exception: {e}")
        return False


def load_repo_families(json_path: Path, downloaded_repos: Set[str]) -> Dict[str, Dict]:
    """Load repo families (origin and its forks)
    
    Returns:
        Dict[str, Dict]: {origin_name: {"original": {...}, "forks": [...]}}
    """
    thread_safe_print(f"Reading file: {json_path}")
    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    
    families = {}
    
    for repo_data in data.get("repositories", []):
        original = repo_data.get("original_repo")
        if not original:
            continue
        
        original_name = original.get("full_name", "")
        if not original_name:
            continue
        
        # Only process downloaded origins
        if original_name not in downloaded_repos:
            continue
        
        # Get downloaded forks
        forks = []
        for fork in repo_data.get("forks", []):
            fork_name = fork.get("full_name", "")
            if fork_name and fork_name in downloaded_repos:
                forks.append(fork)
        
        families[original_name] = {
            "original": original,
            "forks": forks
        }
    
    return families


def process_single_repo(repo_name: str, cloned_dir: Path) -> tuple:
    """Process single repo, get all commits (for multi-threading).

    Args:
        repo_name: Repo name
        cloned_dir: Clone directory

    Returns:
        tuple: (repo_name, commits_list) or (repo_name, None) if failed
    """
    repo_dir = cloned_dir / repo_name.replace("/", "__")
    
    if not is_valid_git_repo(repo_dir):
        thread_safe_print(f"    Skip invalid repo: {repo_name}")
        return (repo_name, None)
    
    thread_safe_print(f"    Processing repo: {repo_name}")
    commits = get_all_commits(repo_dir)
    thread_safe_print(f"      Found {len(commits)} commits")
    
    # If 0 commits, download likely failed; delete and re-clone
    if len(commits) == 0:
        if delete_and_redownload_repo(repo_name, cloned_dir):
            # Re-fetch commits after re-clone
            thread_safe_print(f"      Re-fetching commits...")
            commits = get_all_commits(repo_dir)
            thread_safe_print(f"      Found {len(commits)} commits after re-fetch")
            
            # If still 0 after re-clone, skip
            if len(commits) == 0:
                thread_safe_print(f"      Warning: still no commits after re-clone, skipping: {repo_name}")
                return (repo_name, None)
        else:
            thread_safe_print(f"      Re-clone failed, skipping: {repo_name}")
            return (repo_name, None)
    
    return (repo_name, commits)


def build_commits_tree(family_name: str, repos: List[str], cloned_dir: Path, max_workers: int = 4) -> Dict:
    """Build unified commits tree for a repo family (multi-threaded within family).

    Args:
        family_name: Family name (origin name)
        repos: [repo_name, ...] repo names
        cloned_dir: Root clone directory
        max_workers: Max threads for processing repos in family

    Returns:
        Dict: Commits tree data
    """
    print(f"  Processing family: {family_name} ({len(repos)} repos)")
    
    # Collect commits, merge by hash
    commits_dict = {}  # {hash: commit_info}
    commit_repos = defaultdict(list)  # {hash: [repo_names]}
    
    # Process each repo in family with thread pool
    if len(repos) > 1:
        print(f"    Using {max_workers} threads for {len(repos)} repos...")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_repo = {
                executor.submit(process_single_repo, repo_name, cloned_dir): repo_name
                for repo_name in repos
            }
            
            # Collect results
            for future in as_completed(future_to_repo):
                repo_name, commits = future.result()
                if commits is None:
                    continue
                
                # Process commits from this repo
                for commit in commits:
                    commit_hash = commit["hash"]
                    
                    # If commit exists, just add repo name
                    if commit_hash in commits_dict:
                        if repo_name not in commit_repos[commit_hash]:
                            commit_repos[commit_hash].append(repo_name)
                    else:
                        # New commit, add full info
                        commits_dict[commit_hash] = commit
                        commit_repos[commit_hash] = [repo_name]
    else:
        # Single repo, process directly (no thread pool)
        for repo_name in repos:
            _, commits = process_single_repo(repo_name, cloned_dir)
            if commits is None:
                continue
            
            for commit in commits:
                commit_hash = commit["hash"]
                
                # If commit exists, just add repo name
                if commit_hash in commits_dict:
                    if repo_name not in commit_repos[commit_hash]:
                        commit_repos[commit_hash].append(repo_name)
                else:
                    # New commit, add full info
                    commits_dict[commit_hash] = commit
                    commit_repos[commit_hash] = [repo_name]
    
    # Build commits list with repos field
    commits_list = []
    for commit_hash, commit_info in commits_dict.items():
        commit_data = commit_info.copy()
        commit_data["repos"] = commit_repos[commit_hash]
        commits_list.append(commit_data)
    
    # Sort by author_date
    try:
        commits_list.sort(key=lambda x: x["author"]["date"])
    except Exception:
        pass
    
    return {
        "family_name": family_name,
        "total_commits": len(commits_list),
        "total_repos": len(repos),
        "commits": commits_list
    }


def main():
    import argparse
    
    # Current script directory
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    
    parser = argparse.ArgumentParser(description="Generate Commits Tree (multi-threaded)")
    parser.add_argument(
        "--workers",
        type=int,
        default=MAX_WORKERS,
        help=f"Max threads for parallel repo processing within family (default: {MAX_WORKERS}, families processed serially)"
    )
    parser.add_argument(
        "--projects-json",
        type=Path,
        default=current_dir / "github_projects_filtered_stars_10.json",
        help="Projects JSON file path"
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=current_dir / "cloned_repos" / "clone_report.json",
        help="Clone report JSON file path"
    )
    parser.add_argument(
        "--cloned-dir",
        type=Path,
        default=current_dir / "cloned_repos",
        help="Cloned repos directory"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=current_dir / "commits_trees.json",
        help="Output JSON file path"
    )
    args = parser.parse_args()
    
    projects_json = args.projects_json
    report_json = args.report_json
    cloned_dir = args.cloned_dir
    output_file = args.output
    max_workers = args.workers
    
    print("=" * 80)
    print("Generate Commits Tree")
    print("=" * 80)
    print(f"Projects file: {projects_json}")
    print(f"Report file: {report_json}")
    print(f"Clone dir: {cloned_dir}")
    print(f"Output file: {output_file}")
    print()
    
    # Check files exist
    if not projects_json.exists():
        print(f"Error: projects file not found: {projects_json}")
        return
    
    if not report_json.exists():
        print(f"Error: report file not found: {report_json}")
        return
    
    if not cloned_dir.exists():
        print(f"Error: clone dir not found: {cloned_dir}")
        return
    
    # Load downloaded repos
    print("Loading downloaded repos...")
    downloaded_repos = load_downloaded_repos(report_json)
    print(f"Downloaded {len(downloaded_repos)} repos")
    print()
    
    # Load repo families
    print("Loading repo families...")
    families = load_repo_families(projects_json, downloaded_repos)
    print(f"Found {len(families)} downloaded origin families")
    print()
    
    # Process each family serially (multi-threaded within family)
    results = []
    total_families = len(families)
    
    print(f"Families processed serially, {max_workers} threads per family...")
    print()
    
    for idx, (origin_name, family_data) in enumerate(families.items(), 1):
        print(f"[{idx}/{total_families}] Processing family: {origin_name}")
        
        # Collect all repos for this family (origin + forks)
        repos = []
        
        # Add origin
        repos.append(origin_name)
        
        # Add forks
        for fork in family_data["forks"]:
            fork_name = fork.get("full_name", "")
            if fork_name:
                repos.append(fork_name)
        
        # Build commits tree (multi-threaded within family)
        try:
            tree_data = build_commits_tree(origin_name, repos, cloned_dir, max_workers)
            results.append(tree_data)
            print(f"  [{idx}/{total_families}] Done: {tree_data['total_commits']} commits")
        except Exception as e:
            print(f"  [{idx}/{total_families}] Error: {e}")
            import traceback
            traceback.print_exc()
        
        print()
    
    # Save results
    output_data = {
        "generated_at": datetime.now().isoformat(),
        "total_families": len(results),
        "total_commits": sum(r["total_commits"] for r in results),
        "families": results
    }
    
    print("Saving results...")
    try:
        temp_file = output_file.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        
        if output_file.exists():
            temp_file.replace(output_file)
        else:
            temp_file.rename(output_file)
        
        print()
        print("=" * 80)
        print("Done!")
        print("=" * 80)
        print(f"Output file: {output_file}")
        print(f"Processed {len(results)} repo families")
        print(f"Total {output_data['total_commits']} commits")
        print("=" * 80)
    except Exception as e:
        print(f"Save failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

