#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate syncability pipeline input from forks_with_unique_commits.json

Output format (families format):
{
  "families": [
    {
      "origin_repo": "owner/origin1",
      "forks": [
        {
          "fork_repo": "owner/fork1",
          "unique_commits": [...]  // loaded from commits_trees_shards dir
        }
      ]
    }
  ]
}

Features:
- Read fork and origin commits from commits_trees_shards dir (JSONL format)
- Auto-detect unique commits (in fork but not in origin)
- Fill unique_commits field with unique commits
"""

import json
import argparse
import os
import subprocess
import re
import time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple, Set
from collections import defaultdict
from multiprocessing import Pool, cpu_count
from functools import partial


def load_commits_from_shard(shard_path: Path) -> List[Dict[str, Any]]:
    """
    Load commits from shard file (JSONL format, one commit per line).
    
    Args:
        shard_path: Path to shard file
    
    Returns:
        List of commits
    """
    commits = []
    if not shard_path.exists():
        return commits
    
    try:
        with shard_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    commit = json.loads(line)
                    commits.append(commit)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"  Warning: failed to load shard file {shard_path}: {e}")
    
    return commits


def load_commit_hashes_from_shard(shard_path: Path) -> Set[str]:
    """
    Load only commit hashes from shard file (for fast lookup, save memory).
    
    Args:
        shard_path: Path to shard file
    
    Returns:
        Set of commit hashes
    """
    commit_hashes = set()
    if not shard_path.exists():
        return commit_hashes
    
    try:
        with shard_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    commit = json.loads(line)
                    commit_hash = commit.get("hash") or commit.get("sha") or commit.get("id")
                    if commit_hash:
                        commit_hashes.add(commit_hash)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"  Warning: failed to load shard file {shard_path}: {e}")
    
    return commit_hashes


def load_unique_commits_from_shard(shard_path: Path, exclude_hashes: Set[str], max_commits: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Stream-load unique commits from shard (only commits not in exclude_hashes).
    
    Args:
        shard_path: Path to shard file
        exclude_hashes: Set of commit hashes to exclude
        max_commits: Max commits to return (None = no limit, to avoid OOM)
    
    Returns:
        List of unique commits
    """
    unique_commits = []
    if not shard_path.exists():
        return unique_commits
    
    count = 0
    try:
        with shard_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    commit = json.loads(line)
                    commit_hash = commit.get("hash") or commit.get("sha") or commit.get("id")
                    if commit_hash and commit_hash not in exclude_hashes:
                        unique_commits.append(commit)
                        count += 1
                        
                        # If max count set, stop when reached
                        if max_commits and count >= max_commits:
                            break
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        print(f"  Warning: failed to load shard file {shard_path}: {e}")
    
    return unique_commits


def get_author_date_from_git(repo_path: Path, commit_hash: str) -> Optional[str]:
    """
    Get commit author date from local git repo.
    
    Args:
        repo_path: Local repo path
        commit_hash: Commit hash
        
    Returns:
        Author date (ISO 8601), or None on failure
    """
    if not repo_path.exists() or not (repo_path / ".git").exists():
        return None
    
    try:
        # Use git show to get author date (ISO 8601)
        result = subprocess.run(
            ["git", "show", "-s", "--format=%aI", commit_hash],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )
        
        if result.returncode == 0 and result.stdout:
            date_str = result.stdout.strip()
            if date_str:
                return date_str
        
        # If failed, try other format
        result = subprocess.run(
            ["git", "log", "-1", "--format=%ai", commit_hash],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )
        
        if result.returncode == 0 and result.stdout:
            date_str = result.stdout.strip()
            if date_str:
                return date_str
    except Exception:
        pass
    
    return None


def get_diff_from_git(repo_path: Path, commit_hash: str, verbose: bool = False) -> Optional[str]:
    """
    Get commit diff from local git repo.
    
    Args:
        repo_path: Local repo path
        commit_hash: Commit hash
        verbose: Whether to print detailed errors
        
    Returns:
        Diff content, or None on failure
    """
    if not repo_path.exists() or not (repo_path / ".git").exists():
        if verbose:
            print(f"    Warning: repo path does not exist or is not a git repo: {repo_path}")
        return None
    
    try:
        # Method 1: use git show for commit diff (most common)
        result = subprocess.run(
            ["git", "show", commit_hash, "--format=", "--no-color"],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )
        
        if result.returncode == 0 and result.stdout:
            diff_content = result.stdout.strip()
            # Extract diff part (skip commit info, keep from diff --git)
            if "diff --git" in diff_content:
                diff_start = diff_content.find("diff --git")
                if diff_start >= 0:
                    return diff_content[diff_start:].strip()
            if diff_content:
                return diff_content
        elif verbose and result.returncode != 0:
            error_msg = result.stderr.strip()[:100] if result.stderr else "Unknown error"
            print(f"    Warning: git show failed ({commit_hash[:8]}): {error_msg}")
        
        # Method 2: if git show failed, try git diff
        result = subprocess.run(
            ["git", "diff", f"{commit_hash}^..{commit_hash}", "--no-color"],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )
        
        if result.returncode == 0 and result.stdout:
            return result.stdout.strip()
        elif verbose and result.returncode != 0:
            error_msg = result.stderr.strip()[:100] if result.stderr else "Unknown error"
            print(f"    Warning: git diff failed ({commit_hash[:8]}): {error_msg}")
        
        # Method 3: use git show then parse
        result = subprocess.run(
            ["git", "show", commit_hash, "--no-color"],
            cwd=repo_path,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            encoding="utf-8",
            errors="ignore"
        )
        
        if result.returncode == 0 and result.stdout:
            diff_content = result.stdout.strip()
            if "diff --git" in diff_content:
                diff_start = diff_content.find("diff --git")
                if diff_start >= 0:
                    return diff_content[diff_start:].strip()
        elif verbose and result.returncode != 0:
            error_msg = result.stderr.strip()[:100] if result.stderr else "Unknown error"
            print(f"    Warning: git show (full) failed ({commit_hash[:8]}): {error_msg}")
        
    except Exception as e:
        if verbose:
            print(f"    Warning: exception getting diff ({commit_hash[:8]}): {str(e)[:100]}")
    
    return None


def _check_commit_exists_in_repo(repo_path: Path, commit_hash: str) -> bool:
    """
    Check if commit exists in repo.
    
    Args:
        repo_path: Repo path
        commit_hash: Commit hash
        
    Returns:
        True if commit exists, else False
    """
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--verify", commit_hash, "--quiet"],
            cwd=repo_path,
            capture_output=True
        )
        return result.returncode == 0
    except Exception:
        return False


def _check_commit_was_reverted(
    repo_path: Path,
    commit_hash: str
) -> bool:
    """
    Check if commit was reverted.
    
    Args:
        repo_path: Repo path
        commit_hash: Commit hash
        
    Returns:
        True if commit was reverted, else False
    """
    try:
        # Check if commit exists
        if not _check_commit_exists_in_repo(repo_path, commit_hash):
            return False
        
        # Use git log to find revert commits
        result = subprocess.run(
            ["git", "log", "--all", "--grep", f"Revert.*{commit_hash[:8]}", "--oneline"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            # Found revert commit; further check revert commit content
            revert_lines = result.stdout.strip().split('\n')
            for line in revert_lines:
                if commit_hash[:8] in line or commit_hash[:12] in line:
                    # Check if this revert commit is in current HEAD
                    revert_commit_hash = line.split()[0]
                    if revert_commit_hash:
                        # Check if revert commit is reachable
                        check_result = subprocess.run(
                            ["git", "branch", "--contains", revert_commit_hash],
                            cwd=repo_path,
                            capture_output=True,
                            text=True
                        )
                        if check_result.returncode == 0 and check_result.stdout.strip():
                            # Revert commit in current branch, so already reverted
                            return True
        
        # Alternative: check if commit changes were undone in later commits
        # Get files modified by commit
        diff_result = subprocess.run(
            ["git", "show", "--name-only", "--pretty=format:", commit_hash],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if diff_result.returncode == 0:
            # Could check further here; for performance only basic revert check
            pass
        
        return False
    except Exception:
        return False


def get_fork_repo_path(fork_repo: str) -> Optional[Path]:
    """
    Get local repo path from fork_repo name.
    
    Tries, in order:
    1. Env FORK_REPOS_BASE_DIR + fork_repo (two path formats)
    2. cloned_repos dir (default repo storage, two formats)
    3. Common path patterns
    
    Path formats:
    - owner/repo -> owner/repo (standard)
    - owner/repo -> owner__repo (underscore, for some storage)
    
    Args:
        fork_repo: Fork repo name (owner/repo)
        
    Returns:
        Repo path, or None if not found
    """
    # Build two path formats
    repo_path_standard = fork_repo.replace("/", os.sep)  # owner/repo
    repo_path_underscore = fork_repo.replace("/", "__")  # owner__repo
    
    # Method 1: base dir from env
    base_dir = os.getenv("FORK_REPOS_BASE_DIR", "")
    if base_dir:
        # Try standard format
        repo_path = Path(base_dir) / repo_path_standard
        if repo_path.exists() and (repo_path / ".git").exists():
            return repo_path
        # Try underscore format
        repo_path = Path(base_dir) / repo_path_underscore
        if repo_path.exists() and (repo_path / ".git").exists():
            return repo_path
    
    # Method 2: try cloned_repos dir (default repo storage)
    # Prefer underscore format (server storage)
    cloned_repos_path = Path("cloned_repos") / repo_path_underscore
    if cloned_repos_path.exists() and (cloned_repos_path / ".git").exists():
        return cloned_repos_path
    # Try standard format
    cloned_repos_path = Path("cloned_repos") / repo_path_standard
    if cloned_repos_path.exists() and (cloned_repos_path / ".git").exists():
        return cloned_repos_path
    
    # Method 3: try common path patterns
    common_paths = [
        Path(repo_path_underscore),  # owner__repo in cwd (prefer)
        Path(repo_path_standard),  # owner/repo in cwd
        Path("repos") / repo_path_underscore,  # repos/owner__repo
        Path("repos") / repo_path_standard,  # repos/owner/repo
        Path("forks") / repo_path_underscore,  # forks/owner__repo
        Path("forks") / repo_path_standard,  # forks/owner/repo
        Path.home() / "repos" / repo_path_underscore,  # ~/repos/owner__repo
        Path.home() / "repos" / repo_path_standard,  # ~/repos/owner/repo
    ]
    
    for path in common_paths:
        if path.exists() and (path / ".git").exists():
            return path
    
    return None


def parse_files_from_diff(diff_content: str) -> List[str]:
    """
    Parse modified file list from diff content.
    
    Args:
        diff_content: Diff content
        
    Returns:
        List of file paths
    """
    if not diff_content:
        return []
    
    files = []
    # Match "diff --git a/path/to/file b/path/to/file" format
    pattern = r'^diff --git\s+a/(.+?)\s+b/(.+?)$'
    for line in diff_content.split('\n'):
        match = re.match(pattern, line)
        if match:
            # Use b/ path (new file path)
            file_path = match.group(2)
            if file_path not in files:
                files.append(file_path)
    
    return files


def is_test_file(file_path: str, test_patterns: List[str]) -> bool:
    """
    Check if file is a test file.
    
    Args:
        file_path: File path
        test_patterns: List of test file patterns
        
    Returns:
        True if test file
    """
    for pattern in test_patterns:
        if re.search(pattern, file_path, re.IGNORECASE):
            return True
    return False


def get_commit_sort_key(commit: Dict[str, Any], test_patterns: List[str]) -> Tuple[int, bool]:
    """
    Get sort key for commit.
    
    Args:
        commit: Commit data
        test_patterns: Test file patterns
        
    Returns:
        (code file count, not has_test)
        - Fewer code files first
        - If same count, prefer commits with test files (hence not has_test)
    """
    # Try to get from files field
    files = commit.get("files", [])
    if not files:
        # Try to parse from diff
        diff_content = commit.get("diff", "")
        if diff_content:
            files = parse_files_from_diff(diff_content)
    
    if not files:
        return (0, True)  # No files, no test file
    
    # Count code files and whether has test file
    code_file_count = 0
    has_test_file = False
    
    for file_path in files:
        if isinstance(file_path, str):
            if is_test_file(file_path, test_patterns):
                has_test_file = True
            else:
                code_file_count += 1
        elif isinstance(file_path, dict):
            # If files is list of objects, try path field
            path = file_path.get("path") or file_path.get("filename") or str(file_path)
            if is_test_file(path, test_patterns):
                has_test_file = True
            else:
                code_file_count += 1
    
    # Return sort key: (code file count, not has_test)
    # Fewer first; same count then prefer test file (not has_test so False < True)
    return (code_file_count, not has_test_file)


def sort_commits_by_file_count(commits: List[Dict[str, Any]], test_patterns: List[str]) -> List[Dict[str, Any]]:
    """
    Sort commits by code file count (exclude test files; fewer files first).
    If same count, prefer commits with test files.
    
    Args:
        commits: List of commits
        test_patterns: Test file patterns
        
    Returns:
        Sorted list of commits
    """
    return sorted(commits, key=lambda c: get_commit_sort_key(c, test_patterns))


def process_fork_for_index(
    item: Dict[str, Any],
    commits_shards_dir: Optional[Path],
    test_patterns: List[str]
) -> Tuple[int, Set[str]]:
    """
    Process single fork and build index key set (for multiprocessing).
    
    Args:
        item: Fork data item
        commits_shards_dir: Path to commits_trees_shards dir
        test_patterns: Test file patterns
    
    Returns:
        (commit_count, index_keys_set) tuple
    """
    try:
        fork_repo = item.get("fork", "")
        origin_repo = item.get("origin", "")
        if not fork_repo or not origin_repo:
            return (0, set())
        
        # Use generator to stream; avoid loading all commits at once
        index_keys = set()
        commit_count = 0
        max_commits_for_index = 100000  # At most 100k commits for index, avoid OOM
        
        for commit in get_unique_commits_generator(fork_repo, origin_repo, commits_shards_dir, use_git_diff=False):
            commit_count += 1
            
            # If over max count, stop
            if commit_count > max_commits_for_index:
                break
            
            commit_hash = commit.get("hash") or commit.get("sha") or commit.get("id")
            if not commit_hash:
                continue
            
            # Quick file count (prefer files field, avoid parsing diff)
            code_file_count = 0
            files = commit.get("files", [])
            if files:
                # Quick count from files field (no diff parse)
                for file_path in files:
                    if isinstance(file_path, str):
                        if not is_test_file(file_path, test_patterns):
                            code_file_count += 1
                    elif isinstance(file_path, dict):
                        path = file_path.get("path") or file_path.get("filename") or str(file_path)
                        if not is_test_file(path, test_patterns):
                            code_file_count += 1
            else:
                # If no files field, quick parse from diff (file list only)
                diff_content = commit.get("diff", "")
                if diff_content:
                    # Match only diff --git lines
                    pattern = r'^diff --git\s+a/(.+?)\s+b/(.+?)$'
                    for line in diff_content.split('\n')[:100]:  # First 100 lines only
                        match = re.match(pattern, line)
                        if match:
                            file_path = match.group(2)
                            if not is_test_file(file_path, test_patterns):
                                code_file_count += 1
            
            # Filename: owner__repo__commit_hash.json
            filename = f"{fork_repo.replace('/', '__')}__{commit_hash[:12]}.json"
            # Index key: file_count_dir/filename
            index_key = f"file_count_{code_file_count}/{filename}"
            
            index_keys.add(index_key)
            
            # Release commit data immediately to save memory
            del commit
        
        return (commit_count, index_keys)
    except Exception as e:
        # On failure return empty to avoid blocking
        print(f"  Warning: error processing fork {item.get('fork', 'unknown')} index: {e}")
        return (0, set())


def get_unique_commits_generator(
    fork_repo: str,
    origin_repo: str,
    commits_shards_dir: Optional[Path] = None,
    use_git_diff: bool = True,
    max_diff_size: int = 10 * 1024 * 1024  # Default max diff size: 10MB
):
    """
    Stream unique commits from commits_trees_shards (generator, save memory).
    
    Args:
        fork_repo: Fork repo name (owner/repo)
        origin_repo: Origin repo name (owner/repo)
        commits_shards_dir: Path to commits_trees_shards dir
        use_git_diff: Whether to get diff via git
        max_diff_size: Max diff size (bytes); larger diffs truncated or skipped
    
    Yields:
        Unique commit dict (in fork but not in origin)
    """
    if not commits_shards_dir or not commits_shards_dir.exists():
        return
    
    # Build shard paths (owner/repo -> owner__repo.jsonl)
    fork_shard = commits_shards_dir / f"{fork_repo.replace('/', '__')}.jsonl"
    origin_shard = commits_shards_dir / f"{origin_repo.replace('/', '__')}.jsonl"
    
    # Load only origin commit hashes (save memory)
    origin_commit_hashes = load_commit_hashes_from_shard(origin_shard)
    
    if not fork_shard.exists():
        return
    
    # Get fork repo path
    fork_repo_path = get_fork_repo_path(fork_repo) if use_git_diff else None
    
    # Stream fork commits, only unique
    try:
        with fork_shard.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    commit = json.loads(line)
                    commit_hash = commit.get("hash") or commit.get("sha") or commit.get("id")
                    if commit_hash and commit_hash not in origin_commit_hashes:
                        # Mark as unique commit
                        commit["is_unique"] = True
                        
                        # Handle diff
                        if use_git_diff:
                            # Get diff from git
                            if fork_repo_path:
                                git_diff = get_diff_from_git(fork_repo_path, commit_hash, verbose=False)
                                if git_diff:
                                    # Check diff size; truncate if too large
                                    if len(git_diff.encode('utf-8')) > max_diff_size:
                                        # Truncate to first max_diff_size bytes
                                        git_diff = git_diff[:max_diff_size]
                                        commit["diff_truncated"] = True
                                    commit["diff"] = git_diff
                                    commit["diff_source"] = "git"
                                else:
                                    commit["diff_source"] = "missing"
                                    commit["diff"] = ""
                            else:
                                commit["diff_source"] = "missing"
                                commit["diff"] = ""
                        else:
                            # Use diff from raw data
                            original_diff = commit.get("diff") or commit.get("patch")
                            if original_diff:
                                # Check diff size
                                diff_size = len(original_diff.encode('utf-8'))
                                if diff_size > max_diff_size:
                                    # Truncate to first max_diff_size bytes
                                    original_diff = original_diff[:max_diff_size]
                                    commit["diff_truncated"] = True
                                commit["diff"] = original_diff
                                commit["diff_source"] = "original"
                            else:
                                commit["diff_source"] = "missing"
                                commit["diff"] = ""
                        
                        # Check if commit was reverted (if repo path available)
                        if fork_repo_path:
                            is_reverted = _check_commit_was_reverted(fork_repo_path, commit_hash)
                            commit["is_reverted"] = is_reverted
                        else:
                            commit["is_reverted"] = False  # Cannot check, default False
                        
                        # Ensure author date exists (from git if missing)
                        if fork_repo_path and use_git_diff:
                            # Check author.date
                            author = commit.get("author")
                            if isinstance(author, dict):
                                # If author is dict, check date field
                                if "date" not in author:
                                    author_date = get_author_date_from_git(fork_repo_path, commit_hash)
                                    if author_date:
                                        author["date"] = author_date
                            elif not author:
                                # If no author, get full author from git
                                author_date = get_author_date_from_git(fork_repo_path, commit_hash)
                                if author_date:
                                    # Try author name and email
                                    try:
                                        result = subprocess.run(
                                            ["git", "show", "-s", "--format=%an|%ae", commit_hash],
                                            cwd=fork_repo_path,
                                            stdout=subprocess.PIPE,
                                            stderr=subprocess.PIPE,
                                            encoding="utf-8",
                                            errors="ignore"
                                        )
                                        if result.returncode == 0 and result.stdout:
                                            parts = result.stdout.strip().split("|", 1)
                                            if len(parts) == 2:
                                                commit["author"] = {
                                                    "name": parts[0],
                                                    "email": parts[1],
                                                    "date": author_date
                                                }
                                            else:
                                                commit["author"] = {"date": author_date}
                                        else:
                                            commit["author"] = {"date": author_date}
                                    except Exception:
                                        commit["author"] = {"date": author_date}
                        
                        # Yield immediately, do not accumulate
                        yield commit
                except json.JSONDecodeError:
                    continue
                except Exception as e:
                    # Skip bad commit, continue
                    continue
    except Exception as e:
        print(f"  Warning: failed to read shard file {fork_shard}: {e}")


def get_unique_commits(
    fork_repo: str,
    origin_repo: str,
    commits_shards_dir: Optional[Path] = None,
    use_git_diff: bool = True,
    max_commits: Optional[int] = None,
    max_diff_size: int = 10 * 1024 * 1024  # Default max diff size: 10MB
) -> List[Dict[str, Any]]:
    """
    Get unique commits from commits_trees_shards (legacy API, uses generator for memory).
    
    Args:
        fork_repo: Fork repo name (owner/repo)
        origin_repo: Origin repo name (owner/repo)
        commits_shards_dir: Path to commits_trees_shards dir
        use_git_diff: Whether to get diff via git
        max_commits: Max commits to return (None = no limit, avoid OOM)
        max_diff_size: Max diff size (bytes); larger diffs truncated
    
    Returns:
        List of unique commits (in fork but not in origin)
    """
    unique_commits = []
    count = 0
    
    for commit in get_unique_commits_generator(fork_repo, origin_repo, commits_shards_dir, use_git_diff, max_diff_size):
        unique_commits.append(commit)
        count += 1
        
        # If max count set, stop when reached
        if max_commits and count >= max_commits:
            break
    
    return unique_commits


def iter_all_commits_stream(
    input_file: Path,
    commits_shards_dir: Optional[Path] = None,
    use_git_diff: bool = True,
    limit_forks: Optional[int] = None,
    max_diff_size: int = 10 * 1024 * 1024,
):
    """
    Stream-iterate unique commits for all forks; yield one per parsed commit.
    For pipelines like security_workflow that process one at a time.

    Args:
        input_file: Path to forks_with_unique_commits.json
        commits_shards_dir: commits_trees_shards dir
        use_git_diff: Whether to get diff via git
        limit_forks: Max forks to process (None = no limit)
        max_diff_size: Max diff size per commit (bytes)

    Yields:
        (fork_repo: str, origin_repo: str, commit: dict)
    """
    if not input_file.exists():
        return
    with open(input_file, "r", encoding="utf-8") as f:
        forks_data = json.load(f)
    if limit_forks and limit_forks > 0:
        forks_data = forks_data[:limit_forks]
    for item in forks_data:
        fork_repo = item.get("fork", "")
        origin_repo = item.get("origin", "")
        if not fork_repo or not origin_repo:
            continue
        for commit in get_unique_commits_generator(
            fork_repo,
            origin_repo,
            commits_shards_dir,
            use_git_diff=use_git_diff,
            max_diff_size=max_diff_size,
        ):
            yield (fork_repo, origin_repo, commit)


def process_single_fork(
    item: Dict[str, Any],
    commits_shards_dir: Optional[Path],
    include_empty_commits: bool,
    use_git_diff: bool,
    output_dir: Optional[Path],
    test_patterns: List[str],
    skip_completed: bool = True
) -> Tuple[str, Dict[str, Any], Dict[str, int]]:
    """
    Process single fork: save each commit as a separate JSON file.
    
    Args:
        item: Fork data item
        commits_shards_dir: Path to commits_trees_shards dir
        include_empty_commits: Whether to include empty unique_commits
        use_git_diff: Whether to get diff via git
        output_dir: Output dir (if set, one file per commit)
        test_patterns: Test file patterns (for sorting)
        
    Returns:
        (fork_repo, fork_data, stats) tuple
    """
    origin_repo = item.get("origin", "")
    fork_repo = item.get("fork", "")
    
    if not origin_repo or not fork_repo:
        return fork_repo, None, {}
    
    # Build fork data (for merged output)
    fork_data = {
        "fork_repo": fork_repo,
        "origin_repo": origin_repo,
    }
    
    # Load unique commits from commits_trees_shards
    stats = {"git_diff_count": 0, "missing_diff_count": 0, "saved_commits": 0, "updated_commits": 0, "skipped_large": 0, "skipped_too_many_files": 0}
    
    # If output_dir set, stream process (one commit at a time)
    if output_dir and commits_shards_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load index to see which commits are done
        completed_files = set()
        if skip_completed:
            commits_index = load_commits_index(output_dir)
            # Index format: {"file_count_dir/filename": "completed"|"pending"}
            completed_files = {k for k, v in commits_index.items() if v == "completed"}
        
        # Stream commits via generator
        commit_count = 0
        max_commits_per_fork = 50000  # At most 50k commits per fork
        max_files_per_commit = 50  # At most 50 files per commit; skip if over
        
        for commit in get_unique_commits_generator(fork_repo, origin_repo, commits_shards_dir, use_git_diff=use_git_diff):
            commit_count += 1
            
            # If over max count, skip rest
            if commit_count > max_commits_per_fork:
                stats["skipped_large"] += 1
                continue
            
            commit_hash = commit.get("hash") or commit.get("sha") or commit.get("id")
            if not commit_hash:
                continue
            
            # Compute file count (for dir)
            sort_key = get_commit_sort_key(commit, test_patterns)
            code_file_count = sort_key[0]
            
            # Total file count (including test files)
            total_file_count = 0
            files = commit.get("files", [])
            if files:
                total_file_count = len(files)
            else:
                # If no files field, parse from diff
                diff_content = commit.get("diff", "")
                if diff_content:
                    parsed_files = parse_files_from_diff(diff_content)
                    total_file_count = len(parsed_files)
            
            # If file count over limit, write skip marker and skip
            if total_file_count > max_files_per_commit:
                # Still write a JSON file but mark as skipped
                file_count_dir = output_dir / f"file_count_{code_file_count}"
                file_count_dir.mkdir(parents=True, exist_ok=True)
                
                filename = f"{fork_repo.replace('/', '__')}__{commit_hash[:12]}.json"
                index_key = f"file_count_{code_file_count}/{filename}"
                output_file = file_count_dir / filename
                
                # Check if already exists
                if skip_completed:
                    if index_key in completed_files and output_file.exists():
                        stats["saved_commits"] += 1
                        continue
                    elif output_file.exists() and check_file_integrity(output_file):
                        stats["saved_commits"] += 1
                        continue
                
                # Build skip marker file
                skipped_commit_data = {
                    "fork_repo": fork_repo,
                    "origin_repo": origin_repo,
                    "commit_hash": commit_hash,
                    "skipped": True,
                    "skip_reason": f"Too many files ({total_file_count} files, limit {max_files_per_commit}), skip analysis",
                    "total_files": total_file_count,
                    "code_files": code_file_count,
                    "max_files_limit": max_files_per_commit
                }
                
                # Add basic info if present
                if commit.get("author"):
                    skipped_commit_data["author"] = commit.get("author")
                if commit.get("committer"):
                    skipped_commit_data["committer"] = commit.get("committer")
                if commit.get("message"):
                    skipped_commit_data["message"] = commit.get("message")
                
                # Save skip marker file (with retry)
                max_retries = 3
                retry_delay = 0.1
                saved_successfully = False
                
                for attempt in range(max_retries):
                    try:
                        # Write to temp file then atomic replace
                        temp_file = output_file.with_suffix(output_file.suffix + ".tmp")
                        
                        with open(temp_file, "w", encoding="utf-8") as f:
                            json.dump(skipped_commit_data, f, ensure_ascii=False, indent=2)
                        
                        try:
                            temp_file.replace(output_file)
                            saved_successfully = True
                            break
                        except (PermissionError, IOError, OSError) as e:
                            # Replace failed, wait and retry
                            if attempt < max_retries - 1:
                                try:
                                    temp_file.unlink()
                                except Exception:
                                    pass
                                time.sleep(retry_delay * (attempt + 1))
                                continue
                            else:
                                # Last attempt: write directly
                                try:
                                    temp_file.unlink()
                                except Exception:
                                    pass
                                with open(output_file, "w", encoding="utf-8") as f:
                                    json.dump(skipped_commit_data, f, ensure_ascii=False, indent=2)
                                saved_successfully = True
                                break
                    except (PermissionError, IOError, OSError) as e:
                        if attempt < max_retries - 1:
                            time.sleep(retry_delay * (attempt + 1))
                            continue
                        else:
                            print(f"   Warning: failed to save skip marker file {output_file}: {e}")
                            break
                    except Exception as e:
                        print(f"   Warning: failed to save skip marker file {output_file}: {e}")
                        break
                
                if saved_successfully:
                    # Verify file integrity
                    try:
                        with open(output_file, "r", encoding="utf-8") as f:
                            json.load(f)
                        stats["saved_commits"] += 1
                        stats["skipped_too_many_files"] = stats.get("skipped_too_many_files", 0) + 1
                    except json.JSONDecodeError:
                        try:
                            output_file.unlink()
                            print(f"   Warning: saved file incomplete, removed: {output_file}")
                        except Exception:
                            pass
                
                continue  # Skip rest
            
            # Stats for diff (only for non-skipped commits)
            if use_git_diff:
                if commit.get("diff_source") == "git":
                    stats["git_diff_count"] += 1
                elif commit.get("diff_source") == "missing":
                    stats["missing_diff_count"] += 1
            
            # Dir name by file count (file_count_0, file_count_1, ...)
            file_count_dir = output_dir / f"file_count_{code_file_count}"
            file_count_dir.mkdir(parents=True, exist_ok=True)
            
            # Filename: owner__repo__commit_hash.json
            filename = f"{fork_repo.replace('/', '__')}__{commit_hash[:12]}.json"
            # Index key: file_count_dir/filename
            index_key = f"file_count_{code_file_count}/{filename}"
            output_file = file_count_dir / filename
            
            # If exists and completed, skip (but check and update fields first)
            if skip_completed:
                # Check if index marks as completed
                if index_key in completed_files:
                    # For completed file, check exists and update fields
                    if output_file.exists():
                        # Try to update fields if missing
                        was_updated = update_existing_file_fields(output_file)
                        stats["saved_commits"] += 1
                        if was_updated:
                            stats["updated_commits"] += 1
                        continue
                    # If file missing, may have been deleted; re-process (index updated on next scan)
                # If file exists but not in index, check integrity and update fields
                elif output_file.exists() and check_file_integrity(output_file):
                    # File exists and valid; try update fields then skip
                    was_updated = update_existing_file_fields(output_file)
                    stats["saved_commits"] += 1
                    if was_updated:
                        stats["updated_commits"] += 1
                    continue
            
            # Get repo path info
            origin_repo_path = get_fork_repo_path(origin_repo)
            fork_repo_path = get_fork_repo_path(fork_repo)
            
            # Build commit data (fork + origin)
            commit_data = {
                "fork_repo": fork_repo,
                "origin_repo": origin_repo,
                **commit  # All commit fields
            }
            
            # Add repo path info if found
            if origin_repo_path:
                commit_data["origin_repo_path"] = str(origin_repo_path)
            if fork_repo_path:
                # Add fork path to other_forks_paths
                # Note: syncability_pipeline.py will exclude current fork
                commit_data["other_forks_paths"] = [str(fork_repo_path)]
            
            # Save file (retry to avoid multiprocess conflict)
            max_retries = 3
            retry_delay = 0.1  # 100ms
            saved_successfully = False
            
            for attempt in range(max_retries):
                try:
                    # Write to temp then atomic replace
                    temp_file = output_file.with_suffix(output_file.suffix + ".tmp")
                    
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(commit_data, f, ensure_ascii=False, indent=2)
                    
                    try:
                        temp_file.replace(output_file)
                        saved_successfully = True
                        break
                    except (PermissionError, IOError, OSError) as e:
                        # Replace failed (locked or permission)
                        if attempt < max_retries - 1:
                            # Remove temp, wait, retry
                            try:
                                temp_file.unlink()
                            except Exception:
                                pass
                            time.sleep(retry_delay * (attempt + 1))
                            continue
                        else:
                            # Last attempt: write directly (no temp)
                            try:
                                temp_file.unlink()
                            except Exception:
                                pass
                            # Direct write as fallback
                            with open(output_file, "w", encoding="utf-8") as f:
                                json.dump(commit_data, f, ensure_ascii=False, indent=2)
                            saved_successfully = True
                            break
                    
                except (PermissionError, IOError, OSError) as e:
                    # Write failed
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                    else:
                        # Last attempt failed
                        print(f"   Warning: failed to save file {output_file}: {e}")
                        break
                except Exception as e:
                    # Other error
                    print(f"   Warning: failed to save file {output_file}: {e}")
                    break
            
            if saved_successfully:
                # Verify file integrity (reload)
                try:
                    with open(output_file, "r", encoding="utf-8") as f:
                        json.load(f)
                    # File valid (index updated on next scan)
                    stats["saved_commits"] += 1
                except json.JSONDecodeError:
                    # File invalid, remove it
                    try:
                        output_file.unlink()
                        print(f"   Warning: saved file incomplete, removed: {output_file}")
                    except Exception:
                        pass
    
    return fork_repo, fork_data, stats


def load_validated_files(output_dir: Path) -> Dict[str, bool]:
    """
    Load validated files record.
    
    Args:
        output_dir: Output dir
        
    Returns:
        Dict filename -> is_valid
    """
    validated_file = output_dir / ".validated_files.json"
    if not validated_file.exists():
        return {}
    
    try:
        with open(validated_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_validated_files(output_dir: Path, validated_files: Dict[str, bool]):
    """
    Save validated files record.
    
    Args:
        output_dir: Output dir
        validated_files: Dict filename -> is_valid
    """
    validated_file = output_dir / ".validated_files.json"
    try:
        with open(validated_file, "w", encoding="utf-8") as f:
            json.dump(validated_files, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: failed to save validation record: {e}")


def save_commits_index(output_dir: Path, commits_index: Dict[str, str]):
    """
    Save index of all commit files (simplified: status only).
    
    Args:
        output_dir: Output dir
        commits_index: Dict "file_count_N/filename" -> "completed"|"pending"
    """
    index_file = output_dir / ".commits_index.json"
    try:
        with open(index_file, "w", encoding="utf-8") as f:
            json.dump(commits_index, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Warning: failed to save index file: {e}")


def load_commits_index(output_dir: Path) -> Dict[str, str]:
    """
    Load index of all commit files.
    
    Args:
        output_dir: Output dir
        
    Returns:
        Dict "file_count_N/filename" -> "completed"|"pending"
    """
    index_file = output_dir / ".commits_index.json"
    if not index_file.exists():
        return {}
    
    try:
        with open(index_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            # Backward compat (list format)
            if isinstance(data, list):
                # Convert to new format
                new_index = {}
                for item in data:
                    filename = item.get("filename", "")
                    status = item.get("status", "pending")
                    if filename:
                        # Infer dir from old format
                        new_index[filename] = status
                return new_index
            return data
    except Exception:
        return {}


# Note: update_commit_status_in_index removed
# Index is updated only when scanning existing files at runtime, not on save
# This avoids save conflicts in multiprocess


def check_file_integrity(json_file: Path) -> bool:
    """
    Check if JSON file is complete.
    
    Args:
        json_file: Path to JSON file
        
    Returns:
        True if file is complete, else False
    """
    try:
        # Try to parse JSON
        with open(json_file, "r", encoding="utf-8") as f:
            json.load(f)
        
        # Check file ends with } or ]
        with open(json_file, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
            if lines:
                last_line = lines[-1].strip()
                if last_line.endswith("}") or last_line.endswith("]"):
                    return True
        
        return False
    except json.JSONDecodeError:
        return False
    except Exception:
        return False


def update_existing_file_fields(json_file: Path) -> bool:
    """
    Update is_reverted and author.date in existing file if missing.
    
    Note: Each process handles different fork (different files), but we use
    file lock to avoid concurrent updates (e.g. scan and process overlap).
    
    Args:
        json_file: Path to JSON file
        
    Returns:
        True if file was updated, else False
    """
    # Use file lock to avoid concurrent updates (defensive)
    # Scan and process phases may overlap
    lock_file = json_file.with_suffix(json_file.suffix + ".lock")
    lock_acquired = False
    
    try:
        # Try to acquire lock (.lock file, wait up to 0.5s)
        lock_attempts = 0
        max_lock_attempts = 5  # 5 attempts, 0.1s each, 0.5s total
        while lock_attempts < max_lock_attempts:
            try:
                # Create lock file (atomic)
                lock_file.touch(exist_ok=False)
                lock_acquired = True
                break
            except FileExistsError:
                # Lock exists, wait and retry
                time.sleep(0.1)
                lock_attempts += 1
            except Exception:
                # Other error (e.g. permission), return
                return False
        
        if not lock_acquired:
            # Could not get lock, skip (do not block main flow)
            return False
        
        try:
            # Read file
            with open(json_file, "r", encoding="utf-8") as f:
                commit_data = json.load(f)
            
            # Check if update needed
            needs_update = False
            
            # Check is_reverted
            if "is_reverted" not in commit_data:
                needs_update = True
            
            # Check author.date
            author = commit_data.get("author")
            if isinstance(author, dict):
                if "date" not in author:
                    needs_update = True
            elif not author:
                # No author, need to fetch
                needs_update = True
            
            if not needs_update:
                return False
            
            # Get required info
            fork_repo = commit_data.get("fork_repo", "")
            commit_hash = commit_data.get("hash") or commit_data.get("sha") or commit_data.get("id")
            
            if not fork_repo or not commit_hash:
                return False
            
            # Get repo path
            fork_repo_path = get_fork_repo_path(fork_repo)
            if not fork_repo_path:
                return False
            
            # Update is_reverted
            if "is_reverted" not in commit_data:
                is_reverted = _check_commit_was_reverted(fork_repo_path, commit_hash)
                commit_data["is_reverted"] = is_reverted
            
            # Update author.date
            author = commit_data.get("author")
            if isinstance(author, dict):
                if "date" not in author:
                    author_date = get_author_date_from_git(fork_repo_path, commit_hash)
                    if author_date:
                        author["date"] = author_date
            elif not author:
                # No author, get full author from git
                author_date = get_author_date_from_git(fork_repo_path, commit_hash)
                if author_date:
                    # Try author name and email
                    try:
                        result = subprocess.run(
                            ["git", "show", "-s", "--format=%an|%ae", commit_hash],
                            cwd=fork_repo_path,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            encoding="utf-8",
                            errors="ignore"
                        )
                        if result.returncode == 0 and result.stdout:
                            parts = result.stdout.strip().split("|", 1)
                            if len(parts) == 2:
                                commit_data["author"] = {
                                    "name": parts[0],
                                    "email": parts[1],
                                    "date": author_date
                                }
                            else:
                                commit_data["author"] = {"date": author_date}
                        else:
                            commit_data["author"] = {"date": author_date}
                    except Exception:
                        commit_data["author"] = {"date": author_date}
            
            # Save updated file (retry to avoid multiprocess conflict)
            temp_file = json_file.with_suffix(json_file.suffix + ".tmp")
            max_retries = 3
            retry_delay = 0.1  # 100ms
            
            for attempt in range(max_retries):
                try:
                    # Write to temp file
                    with open(temp_file, "w", encoding="utf-8") as f:
                        json.dump(commit_data, f, ensure_ascii=False, indent=2)
                    
                    # Atomic replace (may need retry on Windows)
                    try:
                        if json_file.exists():
                            try:
                                with open(json_file, "r+b") as f:
                                    pass
                            except (PermissionError, IOError):
                                # File locked, wait and retry
                                if attempt < max_retries - 1:
                                    time.sleep(retry_delay * (attempt + 1))
                                    continue
                                else:
                                    try:
                                        temp_file.unlink()
                                    except Exception:
                                        pass
                                    return False
                        
                        temp_file.replace(json_file)
                        return True
                    except (PermissionError, IOError, OSError) as e:
                        # Replace failed (file locked)
                        if attempt < max_retries - 1:
                            try:
                                temp_file.unlink()
                            except Exception:
                                pass
                            time.sleep(retry_delay * (attempt + 1))
                            continue
                        else:
                            try:
                                temp_file.unlink()
                            except Exception:
                                pass
                            return False
                except (PermissionError, IOError, OSError) as e:
                    # Write to temp failed
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                        continue
                    else:
                        # Last attempt failed
                        try:
                            temp_file.unlink()
                        except Exception:
                            pass
                        return False
            
            return False
        
        finally:
            # Release file lock
            if lock_acquired:
                try:
                    if lock_file.exists():
                        lock_file.unlink()
                except Exception:
                    pass
        
    except Exception as e:
        # Update failed but do not affect integrity check
        # Clean up temp file if exists
        try:
            temp_file = json_file.with_suffix(json_file.suffix + ".tmp")
            if temp_file.exists():
                temp_file.unlink()
        except Exception:
            pass
        return False


def process_single_file_for_index(
    file_info: Tuple[Path, str, str]
) -> Tuple[str, bool, bool, Path]:
    """
    Process single file: check integrity and return result (for multiprocessing).
    
    Args:
        file_info: (file path, index key, in existing index) tuple
        
    Returns:
        (index key, is complete, in existing index, file path) tuple
    """
    json_file, index_key, in_existing = file_info
    is_complete = check_file_integrity(json_file)
    
    # If file complete, check and update fields
    if is_complete:
        update_existing_file_fields(json_file)
    
    return (index_key, is_complete, in_existing, json_file)


def generate_input_data(
    input_file: Path,
    output_file: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    commits_shards_dir: Optional[Path] = None,
    include_empty_commits: bool = True,
    use_git_diff: bool = True,
    limit: Optional[int] = None,
    workers: Optional[int] = None,
    skip_existing: bool = True,
    skip_index_pregen: bool = True
) -> None:
    """
    Generate syncability pipeline input data.
    
    Args:
        input_file: Path to forks_with_unique_commits.json
        output_file: Output data file path (optional; ignored if output_dir set)
        output_dir: Output dir (if set, one file per commit per fork)
        commits_shards_dir: Path to commits_trees_shards dir (optional)
        include_empty_commits: Include empty unique_commits (default True if no commits)
        use_git_diff: Whether to get diff via git
        limit: Max forks to process
        workers: Number of worker processes (default: CPU count)
    """
    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Read input file
    with open(input_file, "r", encoding="utf-8") as f:
        forks_data = json.load(f)
    
    total_forks = len(forks_data)
    
    # If limit set, process first limit forks
    if limit and limit > 0:
        forks_data = forks_data[:limit]
        print(f"Processing first {limit} forks (of {total_forks})...")
    else:
        print(f"Processing {total_forks} forks...")
    
    # Determine output
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        print(f"Output dir: {output_dir}")
        print(f"Each commit saved as: {output_dir}/owner__repo__commit_hash.json")
    elif output_file:
        output_file = Path(output_file)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        print(f"Output file: {output_file}")
    else:
        raise ValueError("Must specify output_file or output_dir")
    
    # Test file patterns (for sorting)
    test_patterns = [
        ".*test.*",
        ".*spec\\.",
        ".*_test\\.",
        ".*\\.test\\.",
        "test/.*",
        "tests/.*",
        ".*Test\\.",
        ".*Spec\\."
    ]
    
    # If output_dir set, update index (scan existing files, update status)
    all_commits_index = {}
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load existing index
        existing_index = load_commits_index(output_dir)
        
        # Scan existing files, update index (multiprocess)
        print(f"Scanning existing files, updating index (multiprocess)...")
        
        # Scan all file_count_N dirs, collect files
        file_count_dirs = [d for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("file_count_")]
        files_to_check = []  # [(file path, index key, in existing index), ...]
        
        for file_count_dir in file_count_dirs:
            file_count = file_count_dir.name  # file_count_0, file_count_1, ...
            json_files = list(file_count_dir.glob("*.json"))
            
            for json_file in json_files:
                filename = json_file.name
                index_key = f"{file_count}/{filename}"
                in_existing = index_key in existing_index
                files_to_check.append((json_file, index_key, in_existing))
        
        scanned_count = len(files_to_check)
        completed_count = 0
        added_count = 0  # Newly added to index
        deleted_count = 0  # Deleted incomplete files
        updated_count = 0  # Files with updated fields
        
        # Multiprocess: check integrity then update index
        if files_to_check:
            scan_workers = min(len(files_to_check), workers if workers else cpu_count())
            print(f"  Using {scan_workers} processes to scan {scanned_count} files and check fields...")
            
            # Collect all scan results
            scan_results = []  # [(index key, is complete, in existing, file path), ...]
            
            with Pool(processes=scan_workers) as pool:
                results = pool.imap(process_single_file_for_index, files_to_check)
                
                processed_count = 0
                for index_key, is_complete, in_existing, json_file in results:
                    processed_count += 1
                    
                    # Show progress
                    if processed_count % 1000 == 0 or processed_count == scanned_count:
                        percentage = processed_count * 100 // scanned_count if scanned_count > 0 else 0
                        print(f"    Scanned {processed_count}/{scanned_count} files ({percentage})%...", flush=True)
                    
                    scan_results.append((index_key, is_complete, in_existing, json_file))
            
            # After scan, update index and remove incomplete files
            print(f"  Scan done, updating index...")
            for index_key, is_complete, in_existing, json_file in scan_results:
                if in_existing:
                    if is_complete:
                        existing_index[index_key] = "completed"
                        completed_count += 1
                    else:
                        existing_index[index_key] = "pending"
                        if json_file:
                            try:
                                json_file.unlink()
                                deleted_count += 1
                            except Exception:
                                pass
                else:
                    if is_complete:
                        existing_index[index_key] = "completed"
                        completed_count += 1
                        added_count += 1
                    else:
                        if json_file:
                            try:
                                json_file.unlink()
                                deleted_count += 1
                            except Exception:
                                pass
        
        save_commits_index(output_dir, existing_index)
        all_commits_index = existing_index
        
        pending_count = len(existing_index) - completed_count
        if scanned_count > 0:
            print(f"Index updated: scanned {scanned_count} files, {len(existing_index)} commits, {completed_count} completed, {pending_count} pending")
            if added_count > 0:
                print(f"   New in index: {added_count} commits (validated)")
            if deleted_count > 0:
                print(f"   Deleted incomplete: {deleted_count}")
            print(f"   Checked/updated fields (is_reverted, author.date): {scanned_count} files")
        else:
            print(f"Index loaded: {len(existing_index)} commits (no existing files)")
    
    # Check existing files (resume)
    existing_forks = set()
    validated_files = {}
    
    if output_dir:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if skip_existing:
            # Skip pre-check of all forks (too slow)
            # Each commit will check index at process time; skip if done
            print(f"Skipping pre-check; completed commits will be skipped during processing")
    
    # Filter out already-processed forks
    if existing_forks:
        original_count = len(forks_data)
        forks_data = [item for item in forks_data if item.get("fork", "") not in existing_forks]
        skipped_count = original_count - len(forks_data)
        if skipped_count > 0:
            print(f"Skipped {skipped_count} already-processed forks, {len(forks_data)} remaining")
    
    # Update total_to_process (after skipping)
    total_to_process = len(forks_data)
    
    # Prepare multiprocess args
    if workers is None:
        workers = cpu_count()
    print(f"Using {workers} processes for {total_to_process} forks...")
    print(f"Each process handles one fork, then picks next")
    
    # Create process function (fixed args)
    process_func = partial(
        process_single_fork,
        commits_shards_dir=commits_shards_dir,
        include_empty_commits=include_empty_commits,
        use_git_diff=use_git_diff,
        output_dir=output_dir,
        test_patterns=test_patterns,
        skip_completed=skip_existing
    )
    
    # Multiprocess
    processed_count = 0
    forks_with_commits = 0
    total_git_diff_count = 0
    total_missing_diff_count = 0
    total_skipped_too_many_files = 0  # Commits skipped (too many files)
    all_fork_data = []  # All fork data for merge
    total_to_process = limit if limit and limit > 0 else total_forks
    new_validated_files = {}  # Newly validated files
    
    # Total commit count (for progress)
    # If index exists and has enough info, use it to avoid re-scan
    total_commits_to_process = 0
    if output_dir and commits_shards_dir:
        # Check if index has enough info
        # Many pending commits => already scanned
        pending_in_index = sum(1 for v in all_commits_index.values() if v == "pending")
        completed_in_index = sum(1 for v in all_commits_index.values() if v == "completed")
        total_in_index = len(all_commits_index)
        
        # If index has many commits (>1000), skip re-count
        if total_in_index > 1000:
            print(f"Index has {total_in_index} commits, skip re-count (completed {completed_in_index}, pending {pending_in_index})")
            total_commits_to_process = pending_in_index
        elif skip_index_pregen:
            # Skip index pregen, start processing (index built on the fly)
            print(f"Skipping index pregen (index empty or small), starting process")
            print(f"Index will be built and updated during processing")
            total_commits_to_process = 0  # Unknown total; progress by fork count
        else:
            # Index small or empty; re-count new forks
            print(f"Counting commits for new forks and updating index (for progress, {workers} processes)...")
            print(f"Note: this step can be slow; use --skip-index-pregen to skip")
            
            # Create process function (fixed args; no other_forks_for_origin for index)
            process_func_index = partial(
                process_fork_for_index,
                commits_shards_dir=commits_shards_dir,
                test_patterns=test_patterns
            )
            
            # Process all forks with multiprocess
            all_index_keys = set()
            total_forks = len(forks_data)
            
            # Chunksize to batch work and avoid stalls
            chunksize = max(1, total_forks // (workers * 4))
            
            with Pool(processes=workers) as pool:
                # imap_unordered for faster progress (order not guaranteed)
                results = pool.imap_unordered(process_func_index, forks_data, chunksize=chunksize)
            
            processed_index_count = 0
            last_print_time = time.time()
            print_interval = 5.0  # Print progress at least every 5s
            
            for commit_count, index_keys in results:
                processed_index_count += 1
                total_commits_to_process += commit_count
                all_index_keys.update(index_keys)
                
                # Show progress (frequent so user does not think it is stuck)
                current_time = time.time()
                should_print = (
                    processed_index_count % 50 == 0 or
                    processed_index_count == total_forks or
                    (current_time - last_print_time) >= print_interval
                )
                
                if should_print:
                    percentage = processed_index_count * 100 // total_forks if total_forks > 0 else 0
                    print(f"  Processed {processed_index_count}/{total_forks} forks ({percentage})%, {len(all_index_keys)} commit index keys...", flush=True)
                    last_print_time = current_time
            
            # Add new index keys to index (status: pending)
            new_commits_added = 0
            for index_key in all_index_keys:
                if index_key not in all_commits_index:
                    all_commits_index[index_key] = "pending"
                    new_commits_added += 1
            
            # Save updated index (all commits to process)
            if output_dir and new_commits_added > 0:
                save_commits_index(output_dir, all_commits_index)
                print(f"Added {new_commits_added} new commits to index (status: pending)")
            
            if total_commits_to_process > 0:
                completed_in_index = sum(1 for v in all_commits_index.values() if v == "completed")
                pending_in_index = sum(1 for v in all_commits_index.values() if v == "pending")
                print(f"Index: {len(all_commits_index)} commits, {completed_in_index} completed, {pending_in_index} pending")
                print(f"Estimated to process {total_commits_to_process} commits")
    
    completed_commits_count = 0  # Completed (new + updated)
    updated_commits_count = 0  # Updated
    new_commits_count = 0  # New
    
    with Pool(processes=workers) as pool:
        results = pool.imap(process_func, forks_data)
        
        for fork_repo, fork_data, stats in results:
            processed_count += 1
            
            # Count completed commits
            saved_count = stats.get("saved_commits", 0)
            updated_count = stats.get("updated_commits", 0)
            completed_commits_count += saved_count
            updated_commits_count += updated_count
            new_commits_count += (saved_count - updated_count)  # New = total - updated
            
            # Note: do not update index during run; index updated on next run when scanning
            
            # Show progress (adjust frequency by total; flush)
            # Prefer commit progress, else fork progress
            should_print = False
            if total_commits_to_process > 0:
                # Use commit progress
                if total_commits_to_process <= 100:
                    should_print = (completed_commits_count % 10 == 0 or completed_commits_count == total_commits_to_process)
                elif total_commits_to_process <= 1000:
                    should_print = (completed_commits_count % 50 == 0 or completed_commits_count == total_commits_to_process)
                else:
                    if completed_commits_count <= 1000:
                        should_print = (completed_commits_count % 50 == 0 or completed_commits_count == total_commits_to_process)
                    else:
                        should_print = (completed_commits_count % 100 == 0 or completed_commits_count == total_commits_to_process)
                
                if should_print:
                    percentage = completed_commits_count * 100 // total_commits_to_process if total_commits_to_process > 0 else 0
                    update_info = f" (new: {new_commits_count}, updated: {updated_commits_count})" if updated_commits_count > 0 else ""
                    print(f"  Completed {completed_commits_count}/{total_commits_to_process} commits ({percentage})%{update_info}...", flush=True)
            else:
                # Use fork progress (backward compat)
                if total_to_process <= 100:
                    should_print = (processed_count % 10 == 0 or processed_count == total_to_process)
                elif total_to_process <= 1000:
                    should_print = (processed_count % 50 == 0 or processed_count == total_to_process)
                else:
                    if processed_count <= 1000:
                        should_print = (processed_count % 50 == 0 or processed_count == total_to_process)
                    else:
                        should_print = (processed_count % 100 == 0 or processed_count == total_to_process)
                
                if should_print:
                    percentage = processed_count * 100 // total_to_process if total_to_process > 0 else 0
                    print(f"  Processed {processed_count}/{total_to_process} forks ({percentage})%...", flush=True)
            
            if fork_data is None:
                continue
            
            # If save ok, record as validated (per commit file)
            # Files are under file_count_N; index updated on save
            
            all_fork_data.append(fork_data)
            
            if fork_data.get("unique_commits"):
                forks_with_commits += 1
            
            if use_git_diff:
                total_git_diff_count += stats.get("git_diff_count", 0)
                total_missing_diff_count += stats.get("missing_diff_count", 0)
            
            # Count commits skipped (too many files)
            total_skipped_too_many_files += stats.get("skipped_too_many_files", 0)
    
    # Note: do not update index after run; index updated on next run when scanning
    
    # Update validation record
    if output_dir and new_validated_files:
        validated_files.update(new_validated_files)
        save_validated_files(output_dir, validated_files)
    
    # If output_file set, merge into single file
    if output_file:
        # Group by origin and fork
        families_dict = defaultdict(lambda: defaultdict(list))
        
        if output_dir:
            # Read all commit files from dir (by file_count_N order)
            print(f"\nMerging commit files into single file...")
            commit_files = []
            
            # Read in file count dir order (file_count_0, file_count_1, ...)
            file_count_dirs = sorted(
                [d for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("file_count_")],
                key=lambda x: int(x.name.split("_")[-1]) if x.name.split("_")[-1].isdigit() else 999999
            )
            
            for file_count_dir in file_count_dirs:
                json_files = list(file_count_dir.glob("*.json"))
                commit_files.extend(json_files)
            
            for commit_file in commit_files:
                try:
                    with open(commit_file, "r", encoding="utf-8") as f:
                        commit_data = json.load(f)
                    
                    fork_repo = commit_data.get("fork_repo", "")
                    origin_repo = commit_data.get("origin_repo", "")
                    
                    if fork_repo and origin_repo:
                        # Remove fork_repo and origin_repo from commit_data, keep commit info only
                        commit_info = {k: v for k, v in commit_data.items() if k not in ["fork_repo", "origin_repo"]}
                        families_dict[origin_repo][fork_repo].append(commit_info)
                except Exception as e:
                    print(f"Warning: failed to read file {commit_file}: {e}")
            
            # Convert to families format
            families = []
            for origin_repo, forks_dict in families_dict.items():
                forks = []
                for fork_repo, commits in forks_dict.items():
                    # Sort by file count
                    commits = sort_commits_by_file_count(commits, test_patterns)
                    forks.append({
                        "fork_repo": fork_repo,
                        "unique_commits": commits
                    })
                families.append({
                    "origin_repo": origin_repo,
                    "forks": forks
                })
        else:
            # If no output_dir, use in-memory data
            for fork_data in all_fork_data:
                origin_repo = fork_data.get("origin_repo", "")
                fork_repo = fork_data.get("fork_repo", "")
                unique_commits = fork_data.get("unique_commits", [])
                
                if origin_repo and fork_repo and unique_commits:
                    # Sort by file count
                    unique_commits = sort_commits_by_file_count(unique_commits, test_patterns)
                    families_dict[origin_repo][fork_repo] = unique_commits
        
        # If not read from dir, convert to families format
        if not output_dir:
            families = []
            for origin_repo, forks_dict in families_dict.items():
                forks = []
                for fork_repo, commits in forks_dict.items():
                    forks.append({
                        "fork_repo": fork_repo,
                        "unique_commits": commits
                    })
                families.append({
                    "origin_repo": origin_repo,
                    "forks": forks
                })
        
        # Build output data
        output_data = {
            "families": families
        }
        
        # Save output file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    # Stats
    total_commits = 0
    if output_dir:
        # Count commit files from dir (by file_count_N)
        file_count_dirs = [d for d in output_dir.iterdir() if d.is_dir() and d.name.startswith("file_count_")]
        for file_count_dir in file_count_dirs:
            json_files = list(file_count_dir.glob("*.json"))
            total_commits += len(json_files)
    
    print(f"\n[Done] Processing complete")
    if output_dir:
        print(f"   - Output dir: {output_dir}")
        print(f"   - {processed_count} forks processed")
        print(f"   - {total_commits} commit files")
    if output_file:
        print(f"   - Output file: {output_file}")
    print(f"   - {forks_with_commits} forks with commits")
    print(f"   - {total_commits} unique commits")
    if completed_commits_count > 0:
        print(f"   - {completed_commits_count} commits processed (new: {new_commits_count}, updated: {updated_commits_count})")
    if use_git_diff:
        print(f"   - {total_git_diff_count} commits got diff from git")
        if total_missing_diff_count > 0:
            print(f"   - Warning: {total_missing_diff_count} commits could not get diff")
    if total_skipped_too_many_files > 0:
        print(f"   - Warning: {total_skipped_too_many_files} commits skipped (too many files, >50), marker files written")


def main():
    parser = argparse.ArgumentParser(
        description="Generate syncability pipeline input from forks_with_unique_commits.json",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example:
  # Generate input (load unique commits from commits_trees_shards dir)
  python generate_syncability_input.py \\
      --input forks_with_unique_commits.json \\
      --output syncability_input.json \\
      --commits-shards-dir commits_trees_shards
  
  # Generate input (no commits_trees_shards, structure only)
  python generate_syncability_input.py \\
      --input forks_with_unique_commits.json \\
      --output syncability_input.json
  
  # Generate input (no empty unique_commits if no commits found)
  python generate_syncability_input.py \\
      --input forks_with_unique_commits.json \\
      --output syncability_input.json \\
      --commits-shards-dir commits_trees_shards \\
      --no-empty-commits
  
  # Limit to first 100 forks
  python generate_syncability_input.py \\
      --input forks_with_unique_commits.json \\
      --output syncability_input.json \\
      --commits-shards-dir commits_trees_shards \\
      --limit 100
  
  # Multiprocess, one file per commit (sorted by file count)
  python generate_syncability_input.py \\
      --input forks_with_unique_commits.json \\
      --output-dir syncability_input_dir \\
      --commits-shards-dir commits_trees_shards \\
      --workers 8
  
  # Multiprocess, merged file and per-commit files
  python generate_syncability_input.py \\
      --input forks_with_unique_commits.json \\
      --output syncability_input.json \\
      --output-dir syncability_input_dir \\
      --commits-shards-dir commits_trees_shards \\
      --workers 8
  
        """
    )
    
    parser.add_argument(
        "--input",
        type=str,
        default="forks_with_unique_commits.json",
        help="Path to forks_with_unique_commits.json (default: forks_with_unique_commits.json)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output data file path (optional; ignored if --output-dir set)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output dir (if set, one file per fork, format: owner__repo.json)"
    )
    
    parser.add_argument(
        "--commits-shards-dir",
        type=str,
        default="commits_trees_shards",
        help="Path to commits_trees_shards dir (default: commits_trees_shards)"
    )
    
    parser.add_argument(
        "--no-empty-commits",
        action="store_true",
        help="Do not include empty unique_commits (if no commits found)"
    )
    parser.add_argument(
        "--use-git-diff",
        action="store_true",
        default=True,
        help="Get diff from local git repo (default: on)"
    )
    parser.add_argument(
        "--no-git-diff",
        action="store_false",
        dest="use_git_diff",
        help="Do not use git for diff; use diff from raw data"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of forks to process (default: all)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of worker processes (default: CPU count)"
    )
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Do not skip existing files (default: skip for resume)"
    )
    parser.add_argument(
        "--skip-index-pregen",
        action="store_true",
        default=True,
        help="Skip index pregen step (default: on, faster startup)"
    )
    parser.add_argument(
        "--no-skip-index-pregen",
        action="store_false",
        dest="skip_index_pregen",
        help="Enable index pregen (may be slow, more accurate progress)"
    )
    args = parser.parse_args()
    
    # Validate args
    if not args.output and not args.output_dir:
        parser.error("Must specify --output or --output-dir")
    
    # Handle commits_shards_dir
    commits_shards_dir = None
    if args.commits_shards_dir:
        commits_shards_path = Path(args.commits_shards_dir)
        if commits_shards_path.exists():
            commits_shards_dir = commits_shards_path
            print(f"Using commits_trees_shards dir: {commits_shards_dir}")
        else:
            print(f"Warning: commits_trees_shards dir not found: {commits_shards_path}, will not load commits")
    
    try:
        output_file = Path(args.output) if args.output else None
        output_dir = Path(args.output_dir) if args.output_dir else None
        
        generate_input_data(
            Path(args.input),
            output_file=output_file,
            output_dir=output_dir,
            commits_shards_dir=commits_shards_dir,
            include_empty_commits=not args.no_empty_commits,
            use_git_diff=args.use_git_diff,
            limit=args.limit,
            workers=args.workers,
            skip_existing=not args.no_skip_existing,
            skip_index_pregen=args.skip_index_pregen
        )
    except Exception as e:
        print(f"[Error] Generation failed: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()

