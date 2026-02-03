#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Batch clone GitHub repositories

Read repository list from github_projects.json and fully clone to the specified directory.
Supports multi-threaded parallel download.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Tuple, Set, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed


def load_repos(json_path: Path, include_forks: bool = True) -> Tuple[List[Tuple[str, str]], int, int]:
    """Load repository info from JSON data (includes original repos and forks by default)
    
    Returns:
        Tuple[List[Tuple[str, str]], int, int]: (repo list, original repo count, fork count)
    """
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    repos = []
    original_count = 0
    fork_count = 0
    
    for repo_data in data.get("repositories", []):
        # Add original repository
        original = repo_data.get("original_repo")
        if original:
            full_name = original.get("full_name")
            if full_name:
                repos.append((full_name, f"https://github.com/{full_name}.git"))
                original_count += 1

        # Add forks (included by default)
        if include_forks:
            for fork in repo_data.get("forks", []):
                full_name = fork.get("full_name")
                if full_name:
                    repos.append((full_name, f"https://github.com/{full_name}.git"))
                    fork_count += 1

    return repos, original_count, fork_count


def is_valid_git_repo(repo_dir: Path) -> bool:
    """Check if directory is a complete git repository"""
    if not repo_dir.exists() or not repo_dir.is_dir():
        return False
    
    # Check if .git directory exists
    git_dir = repo_dir / ".git"
    if not git_dir.exists() or not git_dir.is_dir():
        return False
    
    # Check if .git directory is complete (should have at least HEAD or config file)
    if not (git_dir / "HEAD").exists() and not (git_dir / "config").exists():
        return False
    
    # Try running git command to verify repository integrity
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


def clone_repo(repo_name: str, repo_url: str, dest_dir: Path, timeout: int = 1800, max_retries: int = 2, timeout_repos: Optional[Set[str]] = None) -> Tuple[str, bool, str, bool]:
    """Deep clone a single repository (preserves full git history)
    
    Args:
        repo_name: Repository name
        repo_url: Repository URL
        dest_dir: Destination directory
        timeout: Timeout in seconds, default 1800 (30 minutes)
        max_retries: Maximum retry count, default 2
        timeout_repos: Set of timed-out repos (for recording timed-out repos)
    
    Returns:
        Tuple[str, bool, str, bool]: (repo_name, success, message, is_timeout)
    """
    target_dir = dest_dir / repo_name.replace("/", "__")
    
    # Check if directory already exists
    if target_dir.exists():
        # Check if it's a complete git repository
        if is_valid_git_repo(target_dir):
            return (repo_name, True, f"[Skip] {repo_name} already exists and complete -> {target_dir}", False)
        else:
            # Directory exists but is incomplete (e.g., from previous timeout), remove and re-download
            print(f"[Check] {repo_name} directory exists but incomplete, removing and re-downloading...")
            try:
                shutil.rmtree(target_dir)
                print(f"[Delete] Removed incomplete directory: {target_dir}")
            except Exception as e:
                print(f"[Warning] Failed to remove incomplete directory: {e}, will try to continue")

    # Set env vars to disable git interactive prompts (avoid waiting for username/password)
    env = os.environ.copy()
    env['GIT_TERMINAL_PROMPT'] = '0'
    env['GIT_ASKPASS'] = 'echo'  # Disable password prompt
    
    # Retry mechanism
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            # If failed before, clean up possibly incomplete directory
            if attempt > 0 and target_dir.exists():
                try:
                    shutil.rmtree(target_dir)
                except Exception:
                    pass
            
            # Use full clone (no --depth, preserve all history)
            result = subprocess.run(
                ["git", "clone", repo_url, str(target_dir)],
                check=False,  # Don't raise on non-zero, handle manually
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=timeout,
                env=env
            )
            
            if result.returncode == 0:
                # If success, remove from timeout list
                if timeout_repos is not None:
                    timeout_repos.discard(repo_name)
                if attempt > 0:
                    return (repo_name, True, f"[Success] {repo_name} -> {target_dir} (succeeded after {attempt} retries)", False)
                return (repo_name, True, f"[Success] {repo_name} -> {target_dir}", False)
            else:
                # Get error message
                error_msg = result.stderr.decode('utf-8', errors='ignore') if result.stderr else ""
                error_msg_lower = error_msg.lower()
                last_error = error_msg.strip()
                
                # Check if authentication is required (private repo or needs credentials)
                auth_keywords = [
                    'authentication failed',
                    'permission denied',
                    'could not read username',
                    'could not read password',
                    'username for',
                    'password for',
                    'repository not found',
                    'access denied',
                    'requires authentication',
                    'fatal: could not read',
                    'remote: invalid username or password',
                    'remote: support for password authentication was removed',
                    'terminal prompts disabled',
                    'could not read password for'
                ]
                
                # If error contains auth-related keywords, skip repo (no retry)
                if any(keyword in error_msg_lower for keyword in auth_keywords):
                    return (repo_name, True, f"[Skip] {repo_name} requires authentication (private repo or needs credentials)", False)
                
                # If last attempt, return failure
                if attempt == max_retries:
                    return (repo_name, False, f"[Fail] Error cloning {repo_name} (retried {max_retries} times): {last_error}", False)
                
                # Otherwise continue retry
                continue
                
        except subprocess.TimeoutExpired:
            last_error = f"Timeout ({timeout}s)"
            if attempt == max_retries:
                # Record timed-out repo
                if timeout_repos is not None:
                    timeout_repos.add(repo_name)
                return (repo_name, False, f"[Timeout] Clone {repo_name} timed out (retried {max_retries} times, {timeout}s each), will retry on next run", True)
            # Continue retry
            continue
        except Exception as e:
            last_error = str(e)
            if attempt == max_retries:
                return (repo_name, False, f"[Fail] Exception cloning {repo_name} (retried {max_retries} times): {e}", False)
            # Continue retry
            continue
    
    return (repo_name, False, f"[Fail] Clone {repo_name} failed: {last_error}", False)


def save_checkpoint(checkpoint_file: Path, processed_repos: Set[str], timeout_repos: Set[str], stats: dict):
    """Save checkpoint"""
    try:
        checkpoint_data = {
            'processed_repos': list(processed_repos),
            'timeout_repos': list(timeout_repos),
            'stats': stats,
            'last_update': datetime.now().isoformat()
        }
        
        temp_file = checkpoint_file.with_suffix('.tmp')
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
        
        if checkpoint_file.exists():
            checkpoint_file.replace(temp_file)
        else:
            temp_file.rename(checkpoint_file)
    except Exception as e:
        print(f"Failed to save checkpoint: {e}")


def load_checkpoint(checkpoint_file: Path) -> Optional[dict]:
    """Load checkpoint"""
    if not checkpoint_file.exists():
        return None
    
    try:
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to load checkpoint: {e}")
        return None


def main():
    # Get directory containing this script
    current_dir = Path(os.path.dirname(os.path.abspath(__file__)))
    
    parser = argparse.ArgumentParser(description="Batch clone repos from github_projects_filtered_stars_10.json (multi-threaded, resumable)")
    parser.add_argument(
        "--json",
        type=Path,
        default=current_dir / "github_projects_filtered_stars_10.json",
        help="Path to repo list JSON file (default: github_projects_filtered_stars_10.json in current dir)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=current_dir / "cloned_repos",
        help="Output directory for cloned repos (default: cloned_repos in current dir)",
    )
    parser.add_argument(
        "--exclude-forks",
        action="store_true",
        help="Exclude fork repos (default: clone both originals and all forks)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of repos to clone (default: all)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=15,
        help="Number of worker threads (default: 15)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=1800,
        help="Clone timeout per repo in seconds (default: 1800 = 30 min)",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="Max retries on clone failure (default: 2)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume from checkpoint, start fresh",
    )
    args = parser.parse_args()

    try:
        # Include forks by default unless user specifies --exclude-forks
        repos, original_count, fork_count = load_repos(args.json, include_forks=not args.exclude_forks)
    except FileNotFoundError as exc:
        print(exc)
        sys.exit(1)

    if args.limit:
        repos = repos[: args.limit]

    if not repos:
        print("Repository list is empty, check if JSON file is correct.")
        return

    args.output.mkdir(parents=True, exist_ok=True)
    
    # Checkpoint file
    checkpoint_file = args.output / "clone_checkpoint.json"
    timeout_repos: Set[str] = set()
    processed_repos: Set[str] = set()
    
    # Load checkpoint
    if not args.no_resume:
        checkpoint = load_checkpoint(checkpoint_file)
        if checkpoint:
            processed_repos = set(checkpoint.get('processed_repos', []))
            timeout_repos = set(checkpoint.get('timeout_repos', []))
            print(f"Resumed from checkpoint: {len(processed_repos)} repos already processed")
            if timeout_repos:
                print(f"Found {len(timeout_repos)} timed-out repos, will retry")
    
    # Validate processed repos (handle case where incomplete dirs were created without checkpoint)
    invalid_repos = []
    for repo_name in list(processed_repos):
        target_dir = args.output / repo_name.replace("/", "__")
        if target_dir.exists() and not is_valid_git_repo(target_dir):
            # Dir exists but incomplete, remove from processed_repos and re-download
            processed_repos.discard(repo_name)
            invalid_repos.append(repo_name)
            print(f"[Check] Found incomplete repo {repo_name}, removing from processed list and re-downloading")
            # Try to remove incomplete directory
            try:
                shutil.rmtree(target_dir)
                print(f"[Delete] Removed incomplete directory: {target_dir}")
            except Exception as e:
                print(f"[Warning] Failed to remove incomplete directory: {e}")
    
    if invalid_repos:
        print(f"Found {len(invalid_repos)} incomplete repos, will re-download")
    
    # Filter out already processed repos (but don't skip timed-out repos, allow retry)
    repos_to_process = []
    for repo_name, repo_url in repos:
        if repo_name not in processed_repos:
            repos_to_process.append((repo_name, repo_url))
    
    if not repos_to_process:
        print("All repositories have been processed!")
        return
    
    print(
        f"Starting deep clone of {len(repos_to_process)} new repos (total {len(repos)} repos, preserving full git history)\n"
        f"  - Original repos: {original_count}\n"
        f"  - Fork repos: {fork_count}\n"
        f"  - Workers: {args.workers}\n"
        f"  - Timeout: {args.timeout}s ({args.timeout // 60} min)\n"
        f"  - Max retries: {args.max_retries}\n"
        f"Output dir: {args.output}\n"
        f"JSON file: {args.json}\n"
    )
    if not args.no_resume:
        print(f"Checkpoint file: {checkpoint_file}\n")

    # Clone in parallel using thread pool
    success_count = 0
    skip_count = 0
    fail_count = 0
    timeout_count = 0
    completed = 0
    last_checkpoint_save = time.time()
    checkpoint_interval = 60  # Save checkpoint every 60 seconds

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            # Submit all tasks
            future_to_repo = {
                executor.submit(clone_repo, name, url, args.output, args.timeout, args.max_retries, timeout_repos): (name, url)
                for name, url in repos_to_process
            }

            # Collect results
            for future in as_completed(future_to_repo):
                completed += 1
                try:
                    repo_name, success, message, is_timeout = future.result()
                    
                    # Check if this is a retry of a previously timed-out repo
                    is_retry = repo_name in timeout_repos and success
                    if is_retry:
                        print(f"[{completed}/{len(repos_to_process)}] [Retry success] {message}")
                    else:
                        print(f"[{completed}/{len(repos_to_process)}] {message}")
                    
                    if success:
                        processed_repos.add(repo_name)
                        if "[Skip]" in message:
                            skip_count += 1
                        else:
                            success_count += 1
                    else:
                        fail_count += 1
                        if is_timeout:
                            timeout_count += 1
                    
                    # Periodically save checkpoint
                    current_time = time.time()
                    if current_time - last_checkpoint_save >= checkpoint_interval:
                        stats = {
                            'success_count': success_count,
                            'skip_count': skip_count,
                            'fail_count': fail_count,
                            'timeout_count': timeout_count
                        }
                        save_checkpoint(checkpoint_file, processed_repos, timeout_repos, stats)
                        last_checkpoint_save = current_time
                        print(f"Checkpoint saved ({len(processed_repos)} repos processed)")
                        
                except Exception as e:
                    name, url = future_to_repo[future]
                    print(f"[{completed}/{len(repos_to_process)}] [Exception] Error processing {name}: {e}")
                    fail_count += 1
    except KeyboardInterrupt:
        print("\n\nClone interrupted by user, saving checkpoint...")
        stats = {
            'success_count': success_count,
            'skip_count': skip_count,
            'fail_count': fail_count,
            'timeout_count': timeout_count
        }
        save_checkpoint(checkpoint_file, processed_repos, timeout_repos, stats)
        print("Checkpoint saved, will resume from breakpoint on next run")
        raise

    # Save final checkpoint
    stats = {
        'success_count': success_count,
        'skip_count': skip_count,
        'fail_count': fail_count,
        'timeout_count': timeout_count
    }
    save_checkpoint(checkpoint_file, processed_repos, timeout_repos, stats)
    
    # If all repos processed, remove checkpoint file
    if len(processed_repos) >= len(repos):
        if checkpoint_file.exists():
            checkpoint_file.unlink()
            print(f"Removed checkpoint file: {checkpoint_file}")
    
    print("\n" + "=" * 80)
    print("Clone complete!")
    print(f"Total: {len(repos)} repos")
    print(f"Success: {success_count}")
    print(f"Skipped: {skip_count} (already exists or requires auth)")
    print(f"Failed: {fail_count} (timeouts: {timeout_count})")
    if timeout_repos:
        print(f"Timed-out repos (will retry on next run): {len(timeout_repos)}")
    print("=" * 80)


if __name__ == "__main__":
    main()

