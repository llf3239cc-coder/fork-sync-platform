#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Run git pull for all downloaded repositories.
"""

import subprocess
import argparse
from pathlib import Path
from typing import List, Tuple, Optional, Set, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import os
import json
import time
import shutil

def cpu_count():
    """Return the number of CPU cores."""
    return os.cpu_count() or 1


def get_repos_from_json(json_file: Path) -> List[Dict[str, str]]:
    """
    Read all repository info (origin + forks) from a JSON file.

    Args:
        json_file: Path to the JSON file.

    Returns:
        List of repo info dicts, each with {"full_name": "owner/repo", "repo_type": "origin"|"fork"}.
    """
    repos = []

    if not json_file.exists():
        print(f"Warning: JSON file does not exist: {json_file}")
        return repos

    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        repositories = data.get("repositories", [])

        for repo_group in repositories:
            # Add origin repo
            original_repo = repo_group.get("original_repo", {})
            if original_repo and original_repo.get("full_name"):
                repos.append({
                    "full_name": original_repo["full_name"],
                    "repo_type": "origin"
                })
            
            # Add all forks
            forks = repo_group.get("forks", [])
            for fork in forks:
                if fork.get("full_name"):
                    repos.append({
                        "full_name": fork["full_name"],
                        "repo_type": "fork"
                    })
        
        return repos
    except Exception as e:
        print(f"Error: Failed to read JSON file: {e}")
        return repos


def get_repo_path_from_name(repo_name: str, base_dir: Path) -> Path:
    """
    Get local path from repo name (owner/repo).

    Args:
        repo_name: Repo name (format: owner/repo).
        base_dir: Base directory path.

    Returns:
        Local repo path (format: base_dir/owner__repo).
    """
    # Convert owner/repo to owner__repo
    safe_name = repo_name.replace("/", "__")
    return base_dir / safe_name


def get_all_repo_dirs_from_json(json_file: Path, base_dir: Path) -> List[Path]:
    """
    Read repo list from JSON and check which exist locally.

    Args:
        json_file: Path to the JSON file.
        base_dir: Base directory path.

    Returns:
        List of existing repo directories.
    """
    repo_dirs = []

    print(f"Reading repo list from {json_file.name}...")
    repos = get_repos_from_json(json_file)

    if not repos:
        print("No repository info found.")
        return repo_dirs

    print(f"JSON file contains {len(repos)} repos (origin + forks).")

    existing_count = 0
    for repo_info in repos:
        repo_name = repo_info["full_name"]
        repo_path = get_repo_path_from_name(repo_name, base_dir)

        if repo_path.exists() and (repo_path / ".git").exists():
            repo_dirs.append(repo_path)
            existing_count += 1

    print(f"Found {existing_count} repos locally (of {len(repos)} total).")

    return repo_dirs


def get_all_repo_dirs(base_dir: Path) -> List[Path]:
    """
    Get all repo directories by scanning the folder (kept for backward compatibility).

    Args:
        base_dir: Base directory path.

    Returns:
        List of repo directories.
    """
    repo_dirs = []

    if not base_dir.exists():
        return repo_dirs

    # Iterate subdirs and find those containing .git
    for item in base_dir.iterdir():
        if item.is_dir():
            git_dir = item / ".git"
            if git_dir.exists() and git_dir.is_dir():
                repo_dirs.append(item)
    
    return repo_dirs


def get_current_branch(repo_path: Path) -> Optional[str]:
    """
    Get the current branch name.

    Args:
        repo_path: Repo path.

    Returns:
        Branch name, or None if not on a branch.
    """
    try:
        result = subprocess.run(
            ["git", "branch", "--show-current"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            branch = result.stdout.strip()
            return branch if branch else None
    except Exception:
        pass
    return None


def get_default_branch(repo_path: Path) -> Optional[str]:
    """
    Get default branch name (main or master).

    Args:
        repo_path: Repo path.

    Returns:
        Default branch name, or None if not found.
    """
    try:
        # Try to get remote default branch
        result = subprocess.run(
            ["git", "symbolic-ref", "refs/remotes/origin/HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            ref = result.stdout.strip()
            if ref:
                # Format: refs/remotes/origin/main or refs/remotes/origin/master
                branch = ref.split("/")[-1]
                return branch
    except Exception:
        pass

    # If remote failed, try local branches
    try:
        # Check main branch
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "main"],
            cwd=repo_path,
            capture_output=True
        )
        if result.returncode == 0:
            return "main"

        # Check master branch
        result = subprocess.run(
            ["git", "rev-parse", "--verify", "master"],
            cwd=repo_path,
            capture_output=True
        )
        if result.returncode == 0:
            return "master"
    except Exception:
        pass
    
    return None


def checkout_branch(repo_path: Path, branch: str) -> bool:
    """
    Checkout the given branch.

    Args:
        repo_path: Repo path.
        branch: Branch name.

    Returns:
        True if successful.
    """
    try:
        result = subprocess.run(
            ["git", "checkout", branch],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        return result.returncode == 0
    except Exception:
        return False


def has_local_changes(repo_path: Path) -> bool:
    """
    Check if repo has local changes (modified, staged, or untracked).

    Args:
        repo_path: Repo path.

    Returns:
        True if there are changes, else False.
    """
    try:
        # Check working tree and index
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return bool(result.stdout.strip())
        return False
    except Exception:
        return False


def discard_local_changes(repo_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Discard all local changes (modified, staged, untracked).

    Args:
        repo_path: Repo path.

    Returns:
        (success, error_message) tuple.
    """
    try:
        # 1. Reset all modified and staged files
        reset_result = subprocess.run(
            ["git", "reset", "--hard", "HEAD"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if reset_result.returncode != 0:
            return (False, f"git reset failed: {reset_result.stderr.strip()[:100]}")
        
        # 2. Remove untracked files and directories
        clean_result = subprocess.run(
            ["git", "clean", "-fd"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if clean_result.returncode != 0:
            # clean failure does not block pull; log warning only
            return (True, f"git clean warning: {clean_result.stderr.strip()[:100]}")
        
        return (True, None)
    except Exception as e:
        return (False, str(e)[:200])


def get_repo_url_from_name(repo_name: str) -> str:
    """
    Build GitHub URL from repo name.

    Args:
        repo_name: Repo name (format: owner__repo, double underscore).

    Returns:
        GitHub URL.
    """
    # Repo name format: owner__repo (double underscore)
    # e.g. zzanehip__The-OldOS-Project -> zzanehip/The-OldOS-Project
    if "__" in repo_name:
        owner_repo = repo_name.replace("__", "/")
    else:
        # No __: may be old format; use as-is
        owner_repo = repo_name
    
    return f"https://github.com/{owner_repo}.git"


def clone_repo(repo_url: str, target_dir: Path) -> Tuple[bool, Optional[str]]:
    """
    Clone a repository.

    Args:
        repo_url: Repository URL.
        target_dir: Target directory.

    Returns:
        (success, error_message) tuple.
    """
    try:
        # Ensure parent directory exists
        target_dir.parent.mkdir(parents=True, exist_ok=True)
        
        result = subprocess.run(
            ["git", "clone", repo_url, str(target_dir)],
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            return (True, None)
        else:
            error_msg = result.stderr.strip() or result.stdout.strip()
            return (False, error_msg[:200])
    except Exception as e:
        return (False, str(e)[:200])


def delete_and_reclone_repo(repo_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Delete repo and clone again.

    Args:
        repo_path: Repo path.

    Returns:
        (success, error_message) tuple.
    """
    repo_name = repo_path.name
    repo_url = get_repo_url_from_name(repo_name)
    parent_dir = repo_path.parent

    try:
        # Remove old directory if it exists
        if repo_path.exists():
            shutil.rmtree(repo_path)

        # Re-clone (clone_repo creates dir if missing)
        success, error = clone_repo(repo_url, repo_path)
        if success:
            return (True, f"Re-cloned successfully")
        else:
            # Clone failed; dir may be removed; add to pending reclone list
            return (False, f"Clone failed: {error}")
    except Exception as e:
        # Delete succeeded but clone failed; add to pending reclone list
        return (False, f"Error: {str(e)[:200]}")


def pull_single_repo(repo_path: Path) -> Tuple[Path, bool, Optional[str]]:
    """
    Run git pull for a single repository.

    Args:
        repo_path: Repo path.

    Returns:
        (repo_path, success, error_message) tuple.
    """
    try:
        # Check if currently on a branch
        current_branch = get_current_branch(repo_path)

        # If not on a branch, try to checkout default branch
        if not current_branch:
            default_branch = get_default_branch(repo_path)
            if default_branch:
                if not checkout_branch(repo_path, default_branch):
                    return (repo_path, False, "Not on a branch and cannot checkout default branch")
                current_branch = default_branch
            else:
                return (repo_path, False, "Not on a branch and no default branch found")
        
        # If there are local changes, discard them (re-check after checkout)
        if has_local_changes(repo_path):
            discard_success, discard_msg = discard_local_changes(repo_path)
            if not discard_success:
                discard_msg = discard_msg or "Failed to discard local changes"
            # Continue with pull even if discard had warnings

        # Run git pull
        result = subprocess.run(
            ["git", "pull"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0:
            # Check if there was an update
            output = result.stdout.strip()
            if "Already up to date" in output or "already up to date" in output.lower():
                return (repo_path, True, None)
            else:
                return (repo_path, True, f"Updated: {output[:100]}")
        else:
            error_msg = result.stderr.strip() or result.stdout.strip()
            error_lower = error_msg.lower()
            
            # If error is due to local changes blocking merge, discard and retry
            if "your local changes" in error_lower and "overwritten by merge" in error_lower:
                discard_success, _ = discard_local_changes(repo_path)
                if discard_success:
                    # Retry pull after cleanup
                    retry_result = subprocess.run(
                        ["git", "pull"],
                        cwd=repo_path,
                        capture_output=True,
                        text=True
                    )
                    if retry_result.returncode == 0:
                        output = retry_result.stdout.strip()
                        if "Already up to date" in output or "already up to date" in output.lower():
                            return (repo_path, True, "Fixed local changes conflict, already up to date")
                        else:
                            return (repo_path, True, f"Fixed local changes conflict, Updated: {output[:100]}")
            
            # If error is "not on a branch", try to fix
            if "not currently on a branch" in error_lower or "not on a branch" in error_lower:
                default_branch = get_default_branch(repo_path)
                if default_branch:
                    if checkout_branch(repo_path, default_branch):
                        # Retry pull
                        retry_result = subprocess.run(
                            ["git", "pull"],
                            cwd=repo_path,
                            capture_output=True,
                            text=True
                        )
                        if retry_result.returncode == 0:
                            output = retry_result.stdout.strip()
                            if "Already up to date" in output or "already up to date" in output.lower():
                                return (repo_path, True, f"Fixed detached HEAD, switched to {default_branch}")
                            else:
                                return (repo_path, True, f"Fixed detached HEAD, Updated: {output[:100]}")
            return (repo_path, False, error_msg[:200])
    
    except Exception as e:
        return (repo_path, False, str(e)[:200])


def load_progress(progress_file: Path) -> Tuple[Set[str], Set[str]]:
    """
    Load progress file; return sets of completed repo paths and pending reclone paths.

    Args:
        progress_file: Path to progress file.

    Returns:
        (completed_repos, pending_reclone_repos) tuple.
    """
    if not progress_file.exists():
        return set(), set()
    
    try:
        with open(progress_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            completed = set(data.get("completed_repos", []))
            pending_reclone = set(data.get("pending_reclone_repos", []))
            return completed, pending_reclone
    except Exception:
        return set(), set()


def save_progress(
    progress_file: Path,
    completed_repos: Set[str],
    total_repos: int,
    pending_reclone_repos: Optional[Set[str]] = None
):
    """
    Save progress to file.

    Args:
        progress_file: Path to progress file.
        completed_repos: Set of completed repo paths.
        total_repos: Total number of repos.
        pending_reclone_repos: Set of repo paths pending reclone (optional).
    """
    try:
        data = {
            "completed_repos": list(completed_repos),
            "total_repos": total_repos,
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        if pending_reclone_repos is not None:
            data["pending_reclone_repos"] = list(pending_reclone_repos)
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        pass  # Progress save failure does not affect main flow


def pull_all_repos(
    base_dir: Path,
    max_workers: Optional[int] = None,
    parallel: bool = True,
    resume: bool = True,
    progress_file: Optional[Path] = None,
    auto_reclone: bool = False,
    json_file: Optional[Path] = None
) -> None:
    """
    Run git pull for all repositories.

    Args:
        base_dir: Base directory path.
        max_workers: Max parallel workers (None = auto-detect).
        parallel: Whether to use parallel processing.
        json_file: JSON path (if given, read repo list from JSON; else scan folder).
    """
    # If JSON file provided, read from it; else scan folder
    if json_file:
        repo_dirs = get_all_repo_dirs_from_json(json_file, base_dir)
    else:
        print(f"Scanning repo directory: {base_dir}")
        repo_dirs = get_all_repo_dirs(base_dir)

    if not repo_dirs:
        print(f"No repos found in {base_dir}")
        return

    total_repos = len(repo_dirs)
    print(f"Found {total_repos} repos")

    # Load progress if resume enabled
    completed_repos_set = set()
    pending_reclone_repos_set = set()
    if resume and progress_file:
        completed_repos_set, pending_reclone_repos_set = load_progress(progress_file)
        if completed_repos_set:
            skipped_count = sum(1 for repo_dir in repo_dirs if str(repo_dir) in completed_repos_set)
            print(f"Progress file found: {len(completed_repos_set)} repos completed, skipping {skipped_count}")

        if pending_reclone_repos_set:
            print(f"Found {len(pending_reclone_repos_set)} repos pending reclone (from last run)")
            for repo_path_str in pending_reclone_repos_set:
                repo_path = Path(repo_path_str)
                if not repo_path.exists() or not (repo_path / ".git").exists():
                    if repo_path not in repo_dirs:
                        repo_dirs.append(repo_path)

        repo_dirs = [repo_dir for repo_dir in repo_dirs if str(repo_dir) not in completed_repos_set]
        remaining_count = len(repo_dirs)
        print(f"Remaining: {remaining_count} repos to process")

    if not repo_dirs:
        print("All repos already processed.")
        if progress_file and progress_file.exists():
            progress_file.unlink()
        return

    success_count = 0
    failed_count = 0
    updated_count = 0
    completed_count = len(completed_repos_set)
    failed_repos = []
    
    if parallel and len(repo_dirs) > 1:
        if max_workers is None:
            workers = min(cpu_count() * 2, len(repo_dirs))
        else:
            workers = min(max_workers, len(repo_dirs))

        print(f"\nRunning git pull with {workers} threads...")
        print("=" * 80)

        reclone_workers = min(5, workers)

        with ThreadPoolExecutor(max_workers=workers) as executor, \
             ThreadPoolExecutor(max_workers=reclone_workers) as reclone_executor:

            future_to_repo = {executor.submit(pull_single_repo, repo_path): repo_path
                             for repo_path in repo_dirs}

            immediate_reclone_futures = {}
            all_futures = {}

            for future, repo_path in future_to_repo.items():
                all_futures[future] = ('pull', repo_path)

            for future in as_completed(all_futures):
                task_type, repo_path = all_futures[future]
                
                if task_type == 'pull':
                    try:
                        repo_path, success, message = future.result()
                        completed_count += 1
                        repo_name = repo_path.name
                        percentage = (completed_count * 100) // total_repos

                        if success:
                            success_count += 1
                            if message and "Updated" in message:
                                updated_count += 1
                                status = "✓ [Updated]"
                                msg = message
                            else:
                                status = "✓ [Up to date]"
                                msg = "Already up to date"

                            print(f"[{completed_count}/{total_repos} ({percentage}%)] {status} {repo_name}: {msg[:80]}")

                            completed_repos_set.add(str(repo_path))
                        else:
                            failed_count += 1
                            status = "✗ [Failed]"
                            msg = message

                            print(f"[{completed_count}/{total_repos} ({percentage}%)] {status} {repo_name}: {msg[:80]}")

                            if auto_reclone:
                                print(f"[Reclone] ⏳ [{repo_name}] Recloning (high priority)...")
                                reclone_future = reclone_executor.submit(delete_and_reclone_repo, repo_path)
                                immediate_reclone_futures[reclone_future] = (repo_path, msg)
                                all_futures[reclone_future] = ('reclone', repo_path)
                            else:
                                failed_repos.append(repo_path)
                        
                        if progress_file and completed_count % 10 == 0:
                            save_progress(progress_file, completed_repos_set, total_repos, pending_reclone_repos_set)
                    except Exception as e:
                        completed_count += 1
                        repo_name = repo_path.name if isinstance(repo_path, Path) else str(repo_path)
                        percentage = (completed_count * 100) // total_repos
                        failed_count += 1

                        error_msg = f"Exception: {str(e)[:150]}"
                        print(f"[{completed_count}/{total_repos} ({percentage}%)] ✗ [Error] {repo_name}: {error_msg}")

                        if auto_reclone and isinstance(repo_path, Path):
                            print(f"[Reclone] ⏳ [{repo_name}] Recloning (high priority)...")
                            reclone_future = reclone_executor.submit(delete_and_reclone_repo, repo_path)
                            immediate_reclone_futures[reclone_future] = (repo_path, error_msg)
                            all_futures[reclone_future] = ('reclone', repo_path)
                        else:
                            if isinstance(repo_path, Path):
                                failed_repos.append(repo_path)

                        if progress_file and completed_count % 10 == 0:
                            save_progress(progress_file, completed_repos_set, total_repos, pending_reclone_repos_set)

                elif task_type == 'reclone':
                    repo_path, original_error_msg = immediate_reclone_futures[future]
                    repo_name = repo_path.name
                    try:
                        reclone_success, reclone_msg = future.result()
                        if reclone_success:
                            print(f"[Reclone] ✓✓✓ [{repo_name}] Reclone succeeded.")
                            _, pull_success, pull_msg = pull_single_repo(repo_path)
                            if pull_success:
                                print(f"[Reclone] ✓✓✓ [{repo_name}] Pull OK: {pull_msg or 'Already up to date'}")
                                success_count += 1
                                failed_count -= 1
                                completed_repos_set.add(str(repo_path))
                                pending_reclone_repos_set.discard(str(repo_path))
                            else:
                                print(f"[Reclone] ✗✗✗ [{repo_name}] Pull failed: {pull_msg[:100]}")
                                failed_repos.append(repo_path)
                        else:
                            print(f"[Reclone] ✗✗✗ [{repo_name}] Reclone failed: {reclone_msg[:100]}")
                            failed_repos.append(repo_path)
                            pending_reclone_repos_set.add(str(repo_path))

                        if progress_file:
                            save_progress(progress_file, completed_repos_set, total_repos, pending_reclone_repos_set)
                    except Exception as e:
                        print(f"[Reclone] ✗✗✗ [{repo_name}] Reclone error: {str(e)[:150]}")
                        failed_repos.append(repo_path)
                        pending_reclone_repos_set.add(str(repo_path))

                        if progress_file:
                            save_progress(progress_file, completed_repos_set, total_repos, pending_reclone_repos_set)
    else:
        print(f"\nRunning git pull in single thread...")
        print("=" * 80)

        for idx, repo_path in enumerate(repo_dirs, 1):
            repo_name = repo_path.name
            percentage = (idx * 100) // total_repos

            print(f"[{idx}/{total_repos} ({percentage}%)] Processing: {repo_name}...", end=" ", flush=True)
            
            repo_path, success, message = pull_single_repo(repo_path)
            
            if success:
                success_count += 1
                if message and "Updated" in message:
                    updated_count += 1
                    print(f"✓ [Updated] {message[:60]}")
                else:
                    print("✓ [Up to date]")
                completed_repos_set.add(str(repo_path))
            else:
                failed_count += 1
                print(f"✗ [Failed] {message[:60]}")

                if auto_reclone:
                    print(f"[Reclone] ⏳ [{repo_name}] Recloning...")
                    reclone_success, reclone_msg = delete_and_reclone_repo(repo_path)
                    if reclone_success:
                        print(f"[Reclone] ✓✓✓ [{repo_name}] Reclone succeeded.")
                        _, pull_success, pull_msg = pull_single_repo(repo_path)
                        if pull_success:
                            print(f"[Reclone] ✓✓✓ [{repo_name}] Pull OK: {pull_msg or 'Already up to date'}")
                            success_count += 1
                            failed_count -= 1
                            completed_repos_set.add(str(repo_path))
                        else:
                            print(f"[Reclone] ✗✗✗ [{repo_name}] Pull failed: {pull_msg[:100]}")
                            failed_repos.append(repo_path)
                    else:
                        print(f"[Reclone] ✗✗✗ [{repo_name}] Reclone failed: {reclone_msg[:100]}")
                        failed_repos.append(repo_path)
                else:
                    failed_repos.append(repo_path)

            if progress_file:
                save_progress(progress_file, completed_repos_set, total_repos, pending_reclone_repos_set)
    
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    print(f"Total repos: {total_repos}")
    print(f"Success: {success_count}")
    print(f"Failed: {failed_count}")
    print(f"Updated: {updated_count}")
    print(f"No update: {success_count - updated_count}")
    print("=" * 80)

    if auto_reclone and failed_repos:
        print("\n" + "=" * 80)
        print("Processing remaining failed repos: delete and re-clone")
        print("=" * 80)
        print(f"Found {len(failed_repos)} remaining failed repos")
        
        reclone_success = 0
        reclone_failed = 0
        
        for failed_repo_path in failed_repos:
            repo_name = failed_repo_path.name
            print(f"\nProcessing: {repo_name}...", end=" ", flush=True)

            success, message = delete_and_reclone_repo(failed_repo_path)
            if success:
                reclone_success += 1
                print(f"✓ {message}")
                repo_path, pull_success, pull_msg = pull_single_repo(failed_repo_path)
                if pull_success:
                    print(f"  → Pull: ✓ {pull_msg or 'Already up to date'}")
                    success_count += 1
                    failed_count -= 1
                    completed_repos_set.add(str(failed_repo_path))
                else:
                    print(f"  → Pull: ✗ {pull_msg}")
            else:
                reclone_failed += 1
                print(f"✗ {message}")
        
        print("\n" + "=" * 80)
        print("Re-clone result")
        print("=" * 80)
        print(f"Success: {reclone_success}")
        print(f"Failed: {reclone_failed}")
        print("=" * 80)

        if progress_file:
            save_progress(progress_file, completed_repos_set, total_repos, pending_reclone_repos_set)
    
    if pending_reclone_repos_set:
        print(f"\nNote: {len(pending_reclone_repos_set)} repos still need reclone; progress file kept.")

    if progress_file and progress_file.exists():
        if completed_count == total_repos:
            print(f"\nAll repos done; removing progress file: {progress_file.name}")
            progress_file.unlink()
        else:
            print(f"\nProgress saved to: {progress_file.name}")
            print(f"  Next run will resume from {len(completed_repos_set)} completed repos.")
            if pending_reclone_repos_set:
                print(f"  {len(pending_reclone_repos_set)} repos still pending reclone (from last run).")


def main():
    parser = argparse.ArgumentParser(
        description="Run git pull for all downloaded repositories."
    )

    parser.add_argument(
        "--base-dir",
        type=str,
        default=None,
        help="Base directory for repos (default: env FORK_REPOS_BASE_DIR or cloned_repos)"
    )

    parser.add_argument(
        "--cloned-repos-dir",
        type=str,
        default="cloned_repos",
        help="Cloned repos directory path (default: cloned_repos)"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max parallel worker threads (default: auto-detect CPU count)"
    )

    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Disable parallel; use single thread"
    )

    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume; process all repos from scratch"
    )

    parser.add_argument(
        "--progress-file",
        type=str,
        default=".pull_progress.json",
        help="Progress file path (default: .pull_progress.json)"
    )

    parser.add_argument(
        "--auto-reclone",
        action="store_true",
        help="Auto delete and re-clone failed repos"
    )

    parser.add_argument(
        "--json-file",
        type=str,
        default=None,
        help="JSON path for repo list (origin + forks); default: github_projects_filtered_stars_10.json"
    )

    args = parser.parse_args()

    base_dir = None

    if args.base_dir:
        base_dir = Path(args.base_dir)
    else:
        env_dir = os.getenv("FORK_REPOS_BASE_DIR", "")
        if env_dir:
            base_dir = Path(env_dir)
        else:
            base_dir = Path(args.cloned_repos_dir)

    if not base_dir.exists():
        print(f"Error: Directory does not exist: {base_dir}")
        return

    print(f"Using repo directory: {base_dir}")

    json_file = None
    if args.json_file:
        json_file = Path(args.json_file)
    else:
        default_json = Path("github_projects_filtered_stars_10.json")
        if default_json.exists():
            json_file = default_json
            print(f"Using default JSON file: {json_file.name}")
        else:
            print(f"Note: --json-file not set and default {default_json.name} not found; will scan folder")

    progress_file = Path(args.progress_file) if not args.no_resume else None

    pull_all_repos(
        base_dir,
        max_workers=args.max_workers,
        parallel=not args.no_parallel,
        resume=not args.no_resume,
        progress_file=progress_file,
        auto_reclone=args.auto_reclone,
        json_file=json_file
    )


if __name__ == "__main__":
    main()
