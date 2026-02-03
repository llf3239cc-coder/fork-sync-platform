#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Syncability Pipeline - Multi-stage workflow main script.

Conservative multi-stage pipeline to assess if fork changes can sync to origin.

Stages:
- Stage 0: Candidate extraction (fork-local changes)
- Stage 1: Hard filters (cheap, high precision)
- Stage 2: Technical portability (C1) - file existence + diff context match
"""

import json
import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Set, Tuple
import traceback
from multiprocessing import Pool, cpu_count
from functools import partial
import subprocess
import csv
from collections import defaultdict
import tempfile

# Stage modules
from syncability_common import CandidateItem
from syncability_stage0_candidate_extraction import Stage0CandidateExtraction
from syncability_stage1_hard_filters import Stage1HardFilters
from syncability_stage2_technical_portability import Stage2TechnicalPortability

# Code file extensions (for file content match checks)
CODE_FILE_EXTENSIONS = {
    # C/C++
    '.c', '.cc', '.cpp', '.cxx', '.c++', '.h', '.hh', '.hpp', '.hxx', '.h++',
    # Java/Kotlin/Scala
    '.java', '.kt', '.kts', '.scala',
    # Python
    '.py', '.pyw', '.pyx', '.pxd', '.pxi',
    # JavaScript/TypeScript
    '.js', '.jsx', '.ts', '.tsx', '.mjs', '.cjs',
    # Go
    '.go',
    # Rust
    '.rs',
    # Ruby
    '.rb', '.rake',
    # PHP
    '.php', '.phtml', '.php3', '.php4', '.php5', '.phps',
    # Swift/Objective-C
    '.swift', '.m', '.mm',
    # C#/F#
    '.cs', '.fs', '.fsx',
    # Shell
    '.sh', '.bash', '.zsh', '.fish',
    # Perl
    '.pl', '.pm', '.pod',
    # Lua
    '.lua',
    # R
    '.r', '.R',
    # SQL
    '.sql',
    # Assembly
    '.asm', '.s', '.S',
    # Erlang/Elixir
    '.erl', '.ex', '.exs',
    # Haskell
    '.hs', '.lhs',
    # Clojure
    '.clj', '.cljs', '.cljc',
    # Groovy
    '.groovy', '.gvy', '.gy', '.gsh',
    # Dart
    '.dart',
    # Julia
    '.jl',
    # Zig
    '.zig',
    # Nim
    '.nim',
    # V
    '.v',
    # CUDA
    '.cu', '.cuh',
}


def _is_code_file(file_path: str) -> bool:
    """Check if file is code file"""
    if not file_path:
        return False
    ext = Path(file_path).suffix.lower()
    return ext in CODE_FILE_EXTENSIONS


def _get_fork_repo_path(fork_repo: str) -> Optional[Path]:
    """
    Get local repo path from fork_repo name.
    Args: fork_repo (owner/repo)
    Returns: Path or None
    """
    repo_path_standard = fork_repo.replace("/", os.sep)
    repo_path_underscore = fork_repo.replace("/", "__")
    
    # Method 1: base dir from env
    base_dir = os.getenv("FORK_REPOS_BASE_DIR", "")
    if base_dir:
        repo_path = Path(base_dir) / repo_path_standard
        if repo_path.exists() and (repo_path / ".git").exists():
            return repo_path
        repo_path = Path(base_dir) / repo_path_underscore
        if repo_path.exists() and (repo_path / ".git").exists():
            return repo_path
    
    # Method 2: cloned_repos dir
    cloned_repos_path = Path("cloned_repos") / repo_path_underscore
    if cloned_repos_path.exists() and (cloned_repos_path / ".git").exists():
        return cloned_repos_path
    cloned_repos_path = Path("cloned_repos") / repo_path_standard
    if cloned_repos_path.exists() and (cloned_repos_path / ".git").exists():
        return cloned_repos_path
    
    return None


def _check_commit_exists_in_repo(repo_path: Path, commit_hash: str) -> bool:
    """
    Check if commit exists in repo.
    Args: repo_path, commit_hash
        
    Returns:
        Returns: bool
    """
    try:
        # Check path exists and is valid git repo
        if not repo_path.exists() or not (repo_path / ".git").exists():
            return False
        
        # Strict check: full hash match
        # Use --verify --quiet, verify full hash
        result = subprocess.run(
            ["git", "rev-parse", "--verify", commit_hash, "--quiet"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode != 0:
            return False
        
        # Further verify: get full hash and compare
        # Avoid short hash matching wrong commit
        full_hash_result = subprocess.run(
            ["git", "rev-parse", commit_hash],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if full_hash_result.returncode != 0:
            return False
        
        # Verify full hash starts with input (short hash support)
        # Or exact match (full hash)
        full_hash = full_hash_result.stdout.strip()
        if len(commit_hash) >= 7:
            # If input hash len >= 7, check match
            return full_hash.startswith(commit_hash) or commit_hash.startswith(full_hash[:len(commit_hash)])
        else:
            # If input too short, check prefix only
            return full_hash.startswith(commit_hash)
    except Exception:
        return False


def _get_file_content(repo_path: Path, file_path: str, branch: str = "main") -> Optional[str]:
    """
    Get file content from repo.
    Args: repo_path, file_path, branch (default main)
    Returns: content or None
    """
    try:
        # Try reading from branch
        result = subprocess.run(
            ["git", "show", f"{branch}:{file_path}"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            return result.stdout
        
        # If branch missing, try master
        if branch != "master":
            result = subprocess.run(
                ["git", "show", f"master:{file_path}"],
                cwd=repo_path,
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                return result.stdout
        
        # If both fail, try workspace
        full_path = repo_path / file_path
        if full_path.exists():
            return full_path.read_text(encoding="utf-8", errors="ignore")
        
        return None
    except Exception:
        return None


def _extract_files_from_diff(diff_content: str) -> List[str]:
    """
    Extract modified file paths from diff.
    Supports: diff --git a/path b/path, diff --cc path
    
    Args:
        diff_content: diff content
        
    Returns:
        file path list
    """
    import re
    files = []
    if not diff_content:
        return files
    
    for line in diff_content.split('\n'):
        # Standard: diff --git
        if line.startswith('diff --git '):
            match = re.match(r'diff --git a/(.+?) b/(.+?)(?:\s|$)', line)
            if match:
                file_path = match.group(2).strip()
                if file_path and file_path not in files:
                    files.append(file_path)
        # Merge: diff --cc
        elif line.startswith('diff --cc '):
            file_path = line[10:].strip()
            if file_path and file_path not in files:
                files.append(file_path)
        # Fallback: +++ b/
        elif line.startswith('+++ b/'):
            file_path = line[6:].strip()
            if file_path and file_path not in files:
                files.append(file_path)
    
    return files


def _extract_code_after_commit(diff_content: str) -> Dict[str, List[str]]:
    """
    Extract new code lines from diff (post-commit)
    
    Args:
        diff_content: diff content
        
    Returns:
        Dict: {file_path: [new_line1, ...]}
    """
    result = {}
    current_file = None
    new_lines = []
    
    for line in diff_content.split('\n'):
        # File header: +++ b/
        if line.startswith('+++ b/'):
            # Save previous file result
            if current_file and new_lines:
                result[current_file] = new_lines
            
            # Extract file path
            current_file = line[6:].strip()
            new_lines = []
        
        # New code: starts with + but not +++
        elif line.startswith('+') and not line.startswith('+++'):
            # Strip + prefix
            new_line = line[1:]
            # Keep raw for matching
            new_lines.append(new_line)
    
    # Save last file result
    if current_file and new_lines:
        result[current_file] = new_lines
    
    return result


def _check_file_contains_modified_code(
    repo_path: Path,
    file_path: str,
    modified_code_lines: List[str]
) -> bool:
    """
    Check if file contains modified code from commit.
    Returns: bool
    """
    if not modified_code_lines:
        return False
    
    # Get file content
    file_content = _get_file_content(repo_path, file_path)
    if not file_content:
        return False
    
    file_lines = file_content.split('\n')
    
    # Single line: check exists
    if len(modified_code_lines) == 1:
        return modified_code_lines[0] in file_lines
    
    # Multi-line: check consecutive match
    for i in range(len(file_lines) - len(modified_code_lines) + 1):
        window = file_lines[i:i + len(modified_code_lines)]
        # Check window matches modified lines (ignore whitespace)
        matches = 0
        for j, modified_line in enumerate(modified_code_lines):
            if j < len(window):
                # Ignore leading/trailing whitespace
                if modified_line.strip() == window[j].strip():
                    matches += 1
        
        # All lines must match (no 80% threshold)
        if matches == len(modified_code_lines):
            return True
    
    return False


def _check_file_matches_after_sync(
    repo_path: Path,
    file_path: str,
    diff_content: str
) -> bool:
    """
    Check if file matches post-sync code (simulate by applying diff)
    
    Args:
        repo_path
        file_path
        diff_content: diff content
    Returns: bool
    """
    if not diff_content:
        return False
    
    # Get file content
    file_content = _get_file_content(repo_path, file_path)
    if not file_content:
        return False
    
    # Try apply diff and check result
    try:
        # Use git apply --check to verify
        with tempfile.NamedTemporaryFile(mode='w', suffix='.patch', delete=False) as tmp_file:
            tmp_file.write(diff_content)
            tmp_patch_path = tmp_file.name
        
        try:
            # Check if can apply diff
            result = subprocess.run(
                ["git", "apply", "--check", "--3way", tmp_patch_path],
                cwd=repo_path,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                # Can apply: file same as pre-sync
                # Check if applied matches current file
                # Create temp branch to apply diff
                with tempfile.TemporaryDirectory() as tmp_dir:
                    # Copy file to temp dir
                    tmp_file_path = Path(tmp_dir) / Path(file_path).name
                    tmp_file_path.parent.mkdir(parents=True, exist_ok=True)
                    tmp_file_path.write_text(file_content, encoding="utf-8")
                    
                    # Init temp git repo and apply diff
                    subprocess.run(["git", "init"], cwd=tmp_dir, capture_output=True)
                    subprocess.run(["git", "add", str(tmp_file_path)], cwd=tmp_dir, capture_output=True)
                    subprocess.run(["git", "commit", "-m", "temp"], cwd=tmp_dir, capture_output=True)
                    
                    # Apply diff
                    apply_result = subprocess.run(
                        ["git", "apply", "--3way", tmp_patch_path],
                        cwd=tmp_dir,
                        capture_output=True,
                        text=True
                    )
                    
                    if apply_result.returncode == 0:
                        # Read applied content
                        after_sync_content = tmp_file_path.read_text(encoding="utf-8", errors="ignore")
                        # Compare with current (ignore whitespace)
                        if file_content.strip() == after_sync_content.strip():
                            return True
        finally:
            try:
                os.unlink(tmp_patch_path)
            except Exception:
                pass
    except Exception:
        pass
    
    return False


def _update_input_commit_file(commit_file: Path, updated_fields: Dict[str, Any]) -> bool:
    """
    Update fields in commit file.
    Args: commit_file, updated_fields dict
    Returns: bool
    """
    if not commit_file.exists():
        return False
    
    try:
        # Read existing data
        with open(commit_file, "r", encoding="utf-8") as f:
            commit_data = json.load(f)
        
        # Update fields
        updated = False
        for key, value in updated_fields.items():
            if key == "author" and isinstance(value, dict):
                # Special handling for author (nested)
                if "author" in commit_data:
                    if isinstance(commit_data["author"], dict):
                        # Merge author dict
                        commit_data["author"].update(value)
                    else:
                        commit_data["author"] = value
                else:
                    commit_data["author"] = value
                updated = True
            elif key == "is_reverted":
                # Update is_reverted
                if commit_data.get(key) != value:
                    commit_data[key] = value
                    updated = True
            else:
                # Other fields direct update
                if commit_data.get(key) != value:
                    commit_data[key] = value
                    updated = True
        
        # If any updates, write back
        if updated:
            # Temp file for atomic write
            temp_file = commit_file.with_suffix(commit_file.suffix + ".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(commit_data, f, ensure_ascii=False, indent=2)
            
            # Atomic replace
            temp_file.replace(commit_file)
            return True
        
        return False
    except Exception as e:
        # Update fail: log only, continue
        print(f"Warning: failed to update input file {commit_file}: {e}")
        return False


def _get_author_date_from_git(repo_path: Path, commit_hash: str) -> Optional[str]:
    """
    Get author date from local git repo
    
    Args:
        repo_path
        commit_hash: commit hash
        
    Returns:
        author date ISO8601 or None
    """
    if not repo_path.exists() or not (repo_path / ".git").exists():
        return None
    
    try:
        # git show for author date
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
        
        # On fail: try other format
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


def _check_commit_was_reverted(
    repo_path: Path,
    commit_hash: str
) -> bool:
    """
    Check if commit was reverted
    
    Args:
        repo_path
        commit_hash: commit hash
        
    Returns:
        Returns True if reverted
    """
    try:
        # Check commit exists
        if not _check_commit_exists_in_repo(repo_path, commit_hash):
            return False
        
        # git log for revert commits
        result = subprocess.run(
            ["git", "log", "--all", "--grep", f"Revert.*{commit_hash[:8]}", "--oneline"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            # Found revert commit
            # Further check revert content
            revert_lines = result.stdout.strip().split('\n')
            for line in revert_lines:
                if commit_hash[:8] in line or commit_hash[:12] in line:
                    # Check revert in HEAD
                    revert_commit_hash = line.split()[0]
                    if revert_commit_hash:
                        # Check revert reachable
                        check_result = subprocess.run(
                            ["git", "branch", "--contains", revert_commit_hash],
                            cwd=repo_path,
                            capture_output=True,
                            text=True
                        )
                        if check_result.returncode == 0 and check_result.stdout.strip():
                            # Revert in branch: reverted
                            return True
        
        # Alt: check if changes undone later
        # Get modified files
        diff_result = subprocess.run(
            ["git", "show", "--name-only", "--pretty=format:", commit_hash],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if diff_result.returncode == 0:
            # Could check more, keep basic for perf
            pass
        
        return False
    except Exception:
        return False


def _get_repo_last_updated_time(repo_path: Path) -> Optional[str]:
    """
    Get repo last update time
    
    Args:
        repo_path
        
    Returns:
        ISO format or None
    """
    try:
        # Get last commit time
        result = subprocess.run(
            ["git", "log", "-1", "--format=%aI"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        
        # No commit: try remote
        result = subprocess.run(
            ["git", "fetch", "--dry-run"],
            cwd=repo_path,
            capture_output=True,
            text=True
        )
        
        # All fail: return None
        return None
    except Exception:
        return None


# Cache GitHub projects JSON
_github_projects_cache = None
_github_projects_cache_file = None

def _get_repo_stars(repo_name: str, github_projects_json: Optional[Path] = None) -> Optional[int]:
    """
    Get repo stars (cached)
    
    Args:
        repo_name (owner/repo)
        github_projects_json path
        
    Returns:
        stars count or None
    """
    global _github_projects_cache, _github_projects_cache_file
    
    if not github_projects_json or not github_projects_json.exists():
        return None
    
    # Check cache valid
    if _github_projects_cache is None or _github_projects_cache_file != github_projects_json:
        try:
            with open(github_projects_json, "r", encoding="utf-8") as f:
                _github_projects_cache = json.load(f)
            _github_projects_cache_file = github_projects_json
        except Exception:
            return None
    
    try:
        if "repositories" in _github_projects_cache:
            for repo_entry in _github_projects_cache["repositories"]:
                # Check origin
                original_repo = repo_entry.get("original_repo", {})
                if original_repo.get("full_name") == repo_name:
                    return original_repo.get("stars")
                
                # Check forks
                fork_list = repo_entry.get("forks", [])
                for fork in fork_list:
                    if fork.get("full_name") == repo_name:
                        return fork.get("stars")
    except Exception:
        pass
    
    return None


def _get_all_forks_for_origin(origin_repo: str, github_projects_json: Optional[Path] = None) -> List[str]:
    """
    Get forks under origin
    
    Args:
        origin_repo
        github_projects_json path
        
    Returns:
        List of fork names
    """
    forks = []
    
    # From github_projects JSON
    if github_projects_json and github_projects_json.exists():
        try:
            with open(github_projects_json, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            if "repositories" in data:
                for repo_entry in data["repositories"]:
                    original_repo = repo_entry.get("original_repo", {})
                    if original_repo.get("full_name") == origin_repo:
                        fork_list = repo_entry.get("forks", [])
                        for fork in fork_list:
                            fork_name = fork.get("full_name")
                            if fork_name:
                                forks.append(fork_name)
                        break
        except Exception as e:
            print(f"Warning: failed to read GitHub projects JSON: {e}")
    
    return forks


class SyncabilityPipeline:
    """Syncability pipeline"""
    
    def __init__(
        self,
        work_dir: Path,
        config: Optional[Dict] = None
    ):
        """
        Initialize pipeline
        
        Args:
            work_dir
            config dict
        """
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        
        # Config
        self.config = config or {}
        
        # Stage dirs
        self.stage_dirs = {
            0: self.work_dir / "stage0_candidates",
            1: self.work_dir / "stage1_hard_filters",
            2: self.work_dir / "stage2_technical_portability",
        }
        for stage_dir in self.stage_dirs.values():
            stage_dir.mkdir(parents=True, exist_ok=True)
        
        # Checkpoint file
        self.checkpoint_file = self.work_dir / "pipeline_checkpoint.json"
        self.results_file = self.work_dir / "pipeline_results.json"
        self.log_file = self.work_dir / "pipeline.log"
        
        # Init stages
        self.stages = {
            0: Stage0CandidateExtraction(self.stage_dirs[0], self.config.get("stage0", {})),
            1: Stage1HardFilters(self.stage_dirs[1], self.config.get("stage1", {})),
            2: Stage2TechnicalPortability(self.stage_dirs[2], self.config.get("stage2", {})),
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log"""
        timestamp = datetime.now().isoformat()
        log_line = f"[{timestamp}] [{level}] {message}\n"
        with open(self.log_file, "a", encoding="utf-8") as f:
            f.write(log_line)
        print(log_line.strip())
    
    def load_checkpoint(self) -> Optional[Dict]:
        """Load checkpoint"""
        if not self.checkpoint_file.exists():
            return None
        
        try:
            with open(self.checkpoint_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            self.log(f"Load checkpoint failed: {e}", "WARNING")
            return None
    
    def load_processed_commits(self) -> Set[str]:
        """
        Load processed commits
        
        Returns:
            Set of processed item_ids
        """
        processed_ids = set()
        
        # Prefer results_by_commit (new format)
        results_dir = self.work_dir / "results_by_commit"
        if results_dir.exists():
            try:
                # Scan file_count_N dirs
                for file_count_dir in results_dir.iterdir():
                    if file_count_dir.is_dir() and file_count_dir.name.startswith("file_count_"):
                        # Read JSON files in dir
                        for result_file in file_count_dir.glob("*.json"):
                            try:
                                with open(result_file, "r", encoding="utf-8") as f:
                                    candidate_data = json.load(f)
                                item_id = candidate_data.get("item_id")
                                if item_id:
                                    processed_ids.add(item_id)
                            except Exception as e:
                                self.log(f"Load result file failed {result_file}: {e}", "WARNING")
                
                if processed_ids:
                    self.log(f"Loaded from results_by_commit {len(processed_ids)} processed commits")
                    return processed_ids
            except Exception as e:
                self.log(f"Scan results_by_commit failed: {e}", "WARNING")
        
        # Backward compat: pipeline_results.json
        if self.results_file.exists():
            try:
                with open(self.results_file, "r", encoding="utf-8") as f:
                    results_data = json.load(f)
                
                # If results in candidates
                if "candidates" in results_data:
                    for candidate in results_data["candidates"]:
                        item_id = candidate.get("item_id")
                        if item_id:
                            processed_ids.add(item_id)
                
                # If results in JSONL
                if "candidates_file" in results_data:
                    candidates_file = Path(results_data["candidates_file"])
                    if candidates_file.exists():
                        try:
                            with open(candidates_file, "r", encoding="utf-8") as f:
                                for line in f:
                                    line = line.strip()
                                    if not line:
                                        continue
                                    candidate = json.loads(line)
                                    item_id = candidate.get("item_id")
                                    if item_id:
                                        processed_ids.add(item_id)
                        except Exception as e:
                            self.log(f"Load JSONL result failed: {e}", "WARNING")
            except Exception as e:
                self.log(f"Load result file failed: {e}", "WARNING")
        
        # Backward compat: batch JSONL
        batch_results_file = self.work_dir / "pipeline_results_batch.jsonl"
        if batch_results_file.exists():
            try:
                with open(batch_results_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        candidate = json.loads(line)
                        item_id = candidate.get("item_id")
                        if item_id:
                            processed_ids.add(item_id)
            except Exception as e:
                self.log(f"Load batch result failed: {e}", "WARNING")
        
        return processed_ids
    
    def clean_debug_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively remove debug from dict
        
        Args:
            data to clean
            
        Returns:
            cleaned dict
        """
        if not isinstance(data, dict):
            return data
        
        cleaned = {}
        debug_keys = {
            "debug_info", "hunks_details", "match_attempts",
            "expected_search_pattern", "best_match_window", "best_match_comparison",
            "search_method", "attempted_positions", "best_match_score", "best_match_position",
            "context_before_sample", "old_lines_sample", "context_after_sample",
            "search_pattern_count", "context_before_count", "old_lines_count", "context_after_count",
            "file_lines_count", "match_details", "hunk_debug", "repo_debug"
        }
        # repos_checked is not debug, keep for stats
        
        for key, value in data.items():
            # Skip debug keys
            if key in debug_keys:
                continue
            
            # Recurse nested dicts
            if isinstance(value, dict):
                cleaned[key] = self.clean_debug_info(value)
            elif isinstance(value, list):
                # Process dicts in list
                cleaned[key] = [
                    self.clean_debug_info(item) if isinstance(item, dict) else item
                    for item in value
                ]
            else:
                cleaned[key] = value
        
        return cleaned
    
    def analyze_fork_sync_status_per_commit(
        self,
        candidate: CandidateItem,
        github_projects_json: Optional[Path] = None,
        input_commit_file: Optional[Path] = None
    ) -> Dict[str, Any]:
        """
        Analyze fork sync status for single commit
        
        Args:
            candidate
            github_projects_json path
            input_commit_file
            
        Returns:
            Dict with fork sync status
        """
        origin_repo = candidate.origin_repo
        fork_repo = candidate.fork_repo
        commit_hash = candidate.item_id
        
        # Get diff content
        diff_content = candidate.raw_data.get("diff", "") or candidate.raw_data.get("patch", "")
        files_changed = candidate.raw_data.get("files", [])
        if isinstance(files_changed, list) and files_changed:
            if isinstance(files_changed[0], dict):
                files_changed = [f.get("path") or f.get("filename") or str(f) for f in files_changed]
        
        # If files_changed empty, extract from diff
        if not files_changed and diff_content:
            files_changed = _extract_files_from_diff(diff_content)
        
        # Get checked forks from Stage 2
        stage2_result = candidate.stage2_result or {}
        repos_checked = stage2_result.get("repos_checked", [])  # All checked forks with status
        
        # Extract modified code for already_synced check
        code_after_commit = {}
        if diff_content:
            code_after_commit = _extract_code_after_commit(diff_content)
        
        # Init stats
        syncable_repos = []
        unsyncable_repos = []
        already_synced_repos = []
        checked_repos_set = set()
        
        # Origin status
        origin_status = None  # {"status": "syncable|unsyncable|already_synced", "is_reverted": bool, ...}
        
        # Store detail per repo
        repo_details = {}
        
        # Check if commit reverted (in source fork)
        # Prefer is_reverted from input
        is_reverted = candidate.raw_data.get("is_reverted", False)
        
        # Fallback: check + update author date
        if not isinstance(is_reverted, bool):
            source_fork_path = _get_fork_repo_path(fork_repo)
            is_reverted = False
            if source_fork_path and commit_hash:
                # Check revert status
                is_reverted = _check_commit_was_reverted(source_fork_path, commit_hash)
                # Update is_reverted in input
                candidate.raw_data["is_reverted"] = is_reverted
                
                # Check and update author date if missing
                author = candidate.raw_data.get("author")
                need_update_author_date = False
                
                if isinstance(author, dict):
                    # author is dict, check date
                    if "date" not in author or not author.get("date"):
                        need_update_author_date = True
                elif not author:
                    # author missing
                    need_update_author_date = True
                
                if need_update_author_date:
                    author_date = _get_author_date_from_git(source_fork_path, commit_hash)
                    if author_date:
                        # If author missing: get full author
                        if not author:
                            try:
                                result = subprocess.run(
                                    ["git", "show", "-s", "--format=%an|%ae", commit_hash],
                                    cwd=source_fork_path,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    encoding="utf-8",
                                    errors="ignore"
                                )
                                if result.returncode == 0 and result.stdout:
                                    parts = result.stdout.strip().split("|", 1)
                                    if len(parts) == 2:
                                        candidate.raw_data["author"] = {
                                            "name": parts[0],
                                            "email": parts[1],
                                            "date": author_date
                                        }
                                    else:
                                        candidate.raw_data["author"] = {"date": author_date}
                                else:
                                    candidate.raw_data["author"] = {"date": author_date}
                            except Exception:
                                candidate.raw_data["author"] = {"date": author_date}
                        else:
                            # author exists: add/update date
                            author["date"] = author_date
                
                # If input file: sync update
                if input_commit_file and input_commit_file.exists():
                    updated_fields = {
                        "is_reverted": is_reverted
                    }
                    if need_update_author_date and author_date:
                        # Merge author update
                        if candidate.raw_data.get("author"):
                            updated_fields["author"] = candidate.raw_data["author"]
                        else:
                            updated_fields["author"] = {"date": author_date}
                    
                    _update_input_commit_file(input_commit_file, updated_fields)
        
        # Check origin sync status
        origin_path = _get_fork_repo_path(origin_repo)
        if origin_path:
            origin_detail = {
                "repo_name": origin_repo,
                "repo_type": "origin",
                "passed": False,  # origin not in Stage 2, set False
                "is_reverted": False,
                "last_updated": _get_repo_last_updated_time(origin_path),
                "stars": _get_repo_stars(origin_repo, github_projects_json)
            }
            
            # Check if commit reverted in origin
            if commit_hash:
                origin_detail["is_reverted"] = _check_commit_was_reverted(origin_path, commit_hash)
            
            # Check if origin has commit
            # Fix: verify code files still in current
            origin_already_synced = False
            origin_commit_exists = False
            if commit_hash and not origin_detail["is_reverted"]:
                origin_commit_exists = _check_commit_exists_in_repo(origin_path, commit_hash)
                if origin_commit_exists:
                    # Hash exists: verify code files present
                    code_files_in_commit = [
                        fp for fp in (files_changed or [])
                        if _is_code_file(fp)
                    ]
                    
                    if code_files_in_commit:
                        # Check all code files exist
                        all_code_files_exist = True
                        for fp in code_files_in_commit:
                            if not (origin_path / fp).exists():
                                all_code_files_exist = False
                                break
                        
                        if all_code_files_exist:
                            origin_already_synced = True
                        # If any missing: continue to method 3
                    # else: empty files_changed
                    # Continue method 3 (code_after_commit)
            
            # Check if files missing
            origin_files_missing = False
            
            # Method 2 removed: caused false positives
            # Method 3: check file contains modified code
            # Only when hash not present
            # Fix: all code files must exist and match
            if not origin_already_synced and code_after_commit:
                # Filter code files
                code_files_to_check = {
                    fp: lines for fp, lines in code_after_commit.items()
                    if _is_code_file(fp) and (fp in files_changed or not files_changed)
                }
                
                if code_files_to_check:
                    # All code files must exist and match
                    all_code_files_matched = True
                    any_code_file_missing = False
                    
                    for file_path, modified_lines in code_files_to_check.items():
                        full_file_path = origin_path / file_path
                        if not full_file_path.exists():
                            # Code file missing
                            any_code_file_missing = True
                            all_code_files_matched = False
                            break
                        
                        if not _check_file_contains_modified_code(origin_path, file_path, modified_lines):
                            # File exists but content mismatch
                            all_code_files_matched = False
                            break
                    
                    if any_code_file_missing:
                        origin_files_missing = True
                    
                    if all_code_files_matched and not any_code_file_missing:
                        origin_already_synced = True
                # No code files: don't mark already_synced
            
            # Record origin status
            if origin_already_synced:
                # Hash or content match: already_synced
                origin_status = "already_synced"
                already_synced_repos.append(origin_repo)
            elif origin_files_missing:
                # File missing and no hash: unsyncable
                origin_status = "unsyncable"
                unsyncable_repos.append(origin_repo)
            else:
                # Origin not in Stage 2: unsyncable
                origin_status = "unsyncable"
                unsyncable_repos.append(origin_repo)
            
            origin_detail["sync_status"] = origin_status
            repo_details[origin_repo] = origin_detail
            checked_repos_set.add(origin_repo)
        
        # Process Stage 2 checked repos
        # Stage 2 already determined status
        for repo_info in repos_checked:
            repo_name = repo_info.get("repo_name", "")
            if not repo_name or repo_name == fork_repo:
                continue  # Skip source fork
            
            # Validate: path format + missing -> not already_synced
            repo_path_str = repo_info.get("repo_path", "")
            if repo_path_str:
                repo_path = Path(repo_path_str)
                # Path missing but already_synced: invalid
                # Fix: path missing -> not already_synced
                if not repo_path.exists():
                    sync_status_from_stage2 = repo_info.get("sync_status")
                    if sync_status_from_stage2 == "already_synced":
                        # Path missing: change to unsyncable
                        self.log(f"Warning: repo {repo_name} path {repo_path_str} missing but marked already_synced, fix to unsyncable", "WARNING")
                        repo_info["sync_status"] = "unsyncable"
            
            checked_repos_set.add(repo_name)
            
            # Get status from Stage 2
            sync_status = repo_info.get("sync_status")  # "syncable" | "unsyncable" | "already_synced"
            repo_passed = repo_info.get("passed", False)
            
            # Build repo detail from Stage 2
            repo_detail = {
                "repo_name": repo_name,
                "repo_type": repo_info.get("repo_type", "unknown"),
                "passed": repo_passed,
                "sync_status": sync_status,  # From Stage 2 (fixed)
                "is_reverted": repo_info.get("is_reverted", False),  # From Stage 2
                "last_updated": repo_info.get("last_updated"),  # From Stage 2
                "stars": repo_info.get("stars")  # From Stage 2
            }
            
            # If no stars: try GitHub projects
            # If path format: convert to owner/repo
            if repo_detail["stars"] is None and github_projects_json:
                # If path format: convert
                if "__" in repo_name and "/" not in repo_name:
                    # acidanthera__OpenCorePkg -> acidanthera/OpenCorePkg
                    standard_repo_name = repo_name.replace("__", "/")
                    repo_detail["stars"] = _get_repo_stars(standard_repo_name, github_projects_json)
                else:
                    repo_detail["stars"] = _get_repo_stars(repo_name, github_projects_json)
            
            # Classify by sync status
            if sync_status == "already_synced":
                already_synced_repos.append(repo_name)
            elif sync_status == "syncable":
                syncable_repos.append(repo_name)
            elif sync_status == "unsyncable":
                unsyncable_repos.append(repo_name)
            
            # Record repo detail
            repo_details[repo_name] = repo_detail
            
            # If origin: record status separately
            if repo_name == origin_repo:
                origin_status = sync_status
        
        # Count (exclude origin, origin separate)
        syncable_forks_list = [r for r in syncable_repos if r != origin_repo]
        unsyncable_forks_list = [r for r in unsyncable_repos if r != origin_repo]
        already_synced_forks_list = [r for r in already_synced_repos if r != origin_repo]
        
        # Extract author date
        author_date = None
        author = candidate.raw_data.get("author")
        if isinstance(author, dict):
            author_date = author.get("date")
        
        return {
            "commit_hash": commit_hash,
            "origin_repo": origin_repo,
            "fork_repo": fork_repo,
            "is_reverted": is_reverted,
            "author_date": author_date,
            # All repos stats
            "syncable_repos": syncable_repos,
            "unsyncable_repos": unsyncable_repos,
            "already_synced_repos": already_synced_repos,
            "syncable_repos_count": len(syncable_repos),
            "unsyncable_repos_count": len(unsyncable_repos),
            "already_synced_repos_count": len(already_synced_repos),
            # Forks stats (exclude origin)
            "syncable_forks": syncable_forks_list,
            "unsyncable_forks": unsyncable_forks_list,
            "already_synced_forks": already_synced_forks_list,
            "syncable_forks_count": len(syncable_forks_list),
            "unsyncable_forks_count": len(unsyncable_forks_list),
            "already_synced_forks_count": len(already_synced_forks_list),
            # Origin status
            "origin_status": origin_status,
            "checked_repos": list(checked_repos_set),
            "repo_details": repo_details
        }
    
    def aggregate_fork_sync_status_by_origin(
        self,
        all_commits_status: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Aggregate fork sync status by origin
        
        Args:
            all_commits_status: list of commit sync statuses
            
        Returns:
            Stats grouped by origin
        """
        origin_stats = defaultdict(lambda: {
            "syncable_forks": set(),
            "unsyncable_forks": set(),
            "already_synced_forks": set(),
            "checked_forks": set(),
            "total_commits": 0,
            "reverted_commits": 0,
            "origin_statuses": [],
            "fork_info": {}
        })
        
        # Aggregate all commit stats
        for commit_status in all_commits_status:
            origin_repo = commit_status.get("origin_repo", "")
            if not origin_repo:
                continue
            
            stats = origin_stats[origin_repo]
            stats["total_commits"] += 1
            
            # Check if reverted
            if commit_status.get("is_reverted", False):
                stats["reverted_commits"] += 1
            
            # Record origin status
            origin_status = commit_status.get("origin_status")
            if origin_status:
                stats["origin_statuses"].append(origin_status)
            
            # Merge fork list (dedup)
            stats["syncable_forks"].update(commit_status.get("syncable_forks", []))
            stats["unsyncable_forks"].update(commit_status.get("unsyncable_forks", []))
            stats["already_synced_forks"].update(commit_status.get("already_synced_forks", []))
            stats["checked_forks"].update(commit_status.get("checked_repos", []))
            
            # Merge repo details (keep latest)
            repo_details = commit_status.get("repo_details", {})
            for repo_name, repo_detail in repo_details.items():
                if repo_name not in stats["fork_info"]:
                    stats["fork_info"][repo_name] = {
                        "passed_repos": [],
                        "last_updated": repo_detail.get("last_updated"),
                        "stars": repo_detail.get("stars"),
                        "is_reverted": repo_detail.get("is_reverted", False)
                    }
                
                # If fork passed: record it
                if repo_detail.get("passed", False):
                    if repo_name not in stats["fork_info"][repo_name]["passed_repos"]:
                        stats["fork_info"][repo_name]["passed_repos"].append(commit_status.get("commit_hash"))
                
                # Update info (use latest)
                if repo_detail.get("last_updated"):
                    if not stats["fork_info"][repo_name]["last_updated"] or \
                       repo_detail.get("last_updated") > stats["fork_info"][repo_name]["last_updated"]:
                        stats["fork_info"][repo_name]["last_updated"] = repo_detail.get("last_updated")
                
                if repo_detail.get("stars") is not None:
                    stats["fork_info"][repo_name]["stars"] = repo_detail.get("stars")
                
                if repo_detail.get("is_reverted", False):
                    stats["fork_info"][repo_name]["is_reverted"] = True
        
        # Convert to list and return
        result = {}
        for origin_repo, stats in origin_stats.items():
            # Recalc unsyncable forks (exclude synced)
            unsyncable = stats["unsyncable_forks"] - stats["already_synced_forks"]
            
            # Count origin status
            origin_statuses = stats["origin_statuses"]
            origin_syncable_count = sum(1 for s in origin_statuses if s == "syncable")
            origin_unsyncable_count = sum(1 for s in origin_statuses if s == "unsyncable")
            origin_already_synced_count = sum(1 for s in origin_statuses if s == "already_synced")
            
            # Prepare fork details
            fork_info_list = []
            for repo_name in sorted(stats["checked_forks"]):
                info = stats["fork_info"].get(repo_name, {})
                fork_info_list.append({
                    "repo_name": repo_name,
                    "repo_type": info.get("repo_type", "fork"),
                    "passed_commits_count": len(info.get("passed_repos", [])),
                    "last_updated": info.get("last_updated"),
                    "stars": info.get("stars"),
                    "is_reverted": info.get("is_reverted", False),
                    "sync_statuses": info.get("sync_statuses", [])
                })
            
            result[origin_repo] = {
                # Forks stats (exclude origin)
                "syncable_forks": sorted(list(stats["syncable_forks"])),
                "unsyncable_forks": sorted(list(unsyncable)),
                "already_synced_forks": sorted(list(stats["already_synced_forks"])),
                "syncable_forks_count": len(stats["syncable_forks"]),
                "unsyncable_forks_count": len(unsyncable),
                "already_synced_forks_count": len(stats["already_synced_forks"]),
                # Origin stats
                "origin_status": {
                    "syncable_count": origin_syncable_count,
                    "unsyncable_count": origin_unsyncable_count,
                    "already_synced_count": origin_already_synced_count,
                    "total_commits": len(origin_statuses)
                },
                # Total stats
                "checked_forks": sorted(list(stats["checked_forks"])),
                "total_forks_count": len(stats["checked_forks"]),
                "total_commits": stats["total_commits"],
                "reverted_commits": stats["reverted_commits"],
                "fork_info": fork_info_list
            }
        
        return result
    
    def save_commits_sync_status_csv(
        self,
        all_commits_status: List[Dict[str, Any]],
        output_dir: Optional[Path] = None
    ) -> None:
        """
        Save per-commit fork sync stats to CSV
        
        Args:
            all_commits_status: list of commit sync statuses
            output_dir: output dir (None = work_dir)
        """
        if output_dir is None:
            output_dir = self.work_dir
        else:
            output_dir = Path(output_dir)
        
        output_csv = output_dir / "commits_sync_status.csv"
        
        # Generate CSV rows (one per commit)
        csv_rows = []
        for commit_status in all_commits_status:
            commit_hash = commit_status.get("commit_hash", "")
            origin_repo = commit_status.get("origin_repo", "")
            fork_repo = commit_status.get("fork_repo", "")
            
            # Get fork list
            syncable_forks_list = commit_status.get("syncable_forks", [])
            unsyncable_forks_list = commit_status.get("unsyncable_forks", [])
            already_synced_forks_list = commit_status.get("already_synced_forks", [])
            
            # Ensure count matches list length
            syncable_forks_count = len(syncable_forks_list)
            unsyncable_forks_count = len(unsyncable_forks_list)
            already_synced_forks_count = len(already_synced_forks_list)
            
            # Get origin status
            origin_status = commit_status.get("origin_status", "")
            is_reverted = commit_status.get("is_reverted", False)
            
            # Get author_date
            author_date = commit_status.get("author_date")
            
            row = {
                "commit_hash": commit_hash,
                "origin_repo": origin_repo,
                "fork_repo": fork_repo,
                "is_reverted": is_reverted,
                "author_date": author_date or "",
                # Forks stats
                "syncable_forks_count": syncable_forks_count,
                "unsyncable_forks_count": unsyncable_forks_count,
                "already_synced_forks_count": already_synced_forks_count,
                "syncable_forks": "; ".join(syncable_forks_list) if syncable_forks_list else "",
                "unsyncable_forks": "; ".join(unsyncable_forks_list) if unsyncable_forks_list else "",
                "already_synced_forks": "; ".join(already_synced_forks_list) if already_synced_forks_list else "",
                # Origin status
                "origin_status": origin_status,
                # All repos stats
                "syncable_repos_count": commit_status.get("syncable_repos_count", 0),
                "unsyncable_repos_count": commit_status.get("unsyncable_repos_count", 0),
                "already_synced_repos_count": commit_status.get("already_synced_repos_count", 0),
                # Repo details as JSON
                "repo_details": json.dumps(commit_status.get("repo_details", {}), ensure_ascii=False)
            }
            
            csv_rows.append(row)
        
        # Save to CSV
        with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
            fieldnames = [
                "commit_hash",
                "origin_repo",
                "fork_repo",
                "is_reverted",
                "author_date",
                # Forks stats
                "syncable_forks_count",
                "unsyncable_forks_count",
                "already_synced_forks_count",
                "syncable_forks",
                "unsyncable_forks",
                "already_synced_forks",
                # Origin status
                "origin_status",
                # All repos stats
                "syncable_repos_count",
                "unsyncable_repos_count",
                "already_synced_repos_count",
                # Repo details
                "repo_details"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        self.log(f"Per-commit sync stats saved to: {output_csv}")
    
    def save_fork_sync_status_statistics(
        self,
        origin_stats: Dict[str, Dict[str, Any]],
        output_csv: Optional[Path] = None
    ) -> None:
        """
        Save fork sync stats to CSV
        
        Args:
            origin_stats: stats by origin
            output_csv: output path (None = default)
        """
        if output_csv is None:
            output_csv = self.work_dir / "fork_sync_status_statistics.csv"
        
        # Generate CSV rows
        csv_rows = []
        for origin_repo, stats in origin_stats.items():
            # Get fork list
            syncable_forks_list = stats.get("syncable_forks", [])
            unsyncable_forks_list = stats.get("unsyncable_forks", [])
            already_synced_forks_list = stats.get("already_synced_forks", [])
            
            # Ensure count matches list length
            syncable_forks_count = len(syncable_forks_list)
            unsyncable_forks_count = len(unsyncable_forks_list)
            already_synced_forks_count = len(already_synced_forks_list)
            
            # Get origin status stats
            origin_status = stats.get("origin_status", {})
            origin_syncable_count = origin_status.get("syncable_count", 0)
            origin_unsyncable_count = origin_status.get("unsyncable_count", 0)
            origin_already_synced_count = origin_status.get("already_synced_count", 0)
            
            # Per origin: one summary row
            row = {
                "origin_repo": origin_repo,
                # Forks stats
                "syncable_forks_count": syncable_forks_count,
                "unsyncable_forks_count": unsyncable_forks_count,
                "already_synced_forks_count": already_synced_forks_count,
                "syncable_forks": "; ".join(syncable_forks_list) if syncable_forks_list else "",
                "unsyncable_forks": "; ".join(unsyncable_forks_list) if unsyncable_forks_list else "",
                "already_synced_forks": "; ".join(already_synced_forks_list) if already_synced_forks_list else "",
                # Origin stats
                "origin_syncable_count": origin_syncable_count,
                "origin_unsyncable_count": origin_unsyncable_count,
                "origin_already_synced_count": origin_already_synced_count,
                # Total stats
                "total_forks_count": stats.get("total_forks_count", 0),
                "total_commits": stats.get("total_commits", 0),
                "reverted_commits": stats.get("reverted_commits", 0)
            }
            
            # Add fork details as JSON
            fork_info_json = json.dumps(stats.get("fork_info", []), ensure_ascii=False)
            row["fork_info"] = fork_info_json
            
            csv_rows.append(row)
        
        # Save to CSV
        with open(output_csv, "w", encoding="utf-8-sig", newline="") as f:
            fieldnames = [
                "origin_repo",
                # Forks stats
                "syncable_forks_count",
                "unsyncable_forks_count",
                "already_synced_forks_count",
                "syncable_forks",
                "unsyncable_forks",
                "already_synced_forks",
                # Origin count
                "origin_syncable_count",
                "origin_unsyncable_count",
                "origin_already_synced_count",
                # Total stats
                "total_forks_count",
                "total_commits",
                "reverted_commits",
                "fork_info"
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(csv_rows)
        
        self.log(f"Fork sync stats saved to: {output_csv}")
    
    def save_commit_results(
        self, 
        candidates: List[CandidateItem],
        github_projects_json: Optional[Path] = None,
        analyze_sync_status: bool = True
    ) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Save per-commit results to separate files (by file count)
        Save all results to avoid re-analysis
        
        Args:
            candidates
            github_projects_json path
            analyze_sync_status: whether to analyze sync status
            
        Returns:
            Tuple of (saved count, sync status list)
        """
        results_dir = self.work_dir / "results_by_commit"
        results_dir.mkdir(parents=True, exist_ok=True)
        
        # Save all results including failures
        # But only high-confidence to pipeline_results.json
        all_candidates = candidates
        high_confidence_candidates = [
            c for c in candidates 
            if c.final_decision == "high-confidence"
        ]
        
        # Get test file patterns
        import re
        test_patterns = self.config.get("stage0", {}).get("test_file_patterns", [
            ".*test.*",
            ".*spec\\.",
            ".*_test\\.",
            ".*\\.test\\.",
            "test/.*",
            "tests/.*",
            ".*Test\\.",
            ".*Spec\\."
        ])
        test_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in test_patterns]
        
        def is_test_file(file_path: str) -> bool:
            """Check if test file"""
            for regex in test_regexes:
                if regex.search(file_path):
                    return True
            return False
        
        def get_code_file_count(candidate: CandidateItem) -> int:
            """Count code files (exclude test)"""
            files = candidate.raw_data.get("files", [])
            if not isinstance(files, list):
                return 0
            
            code_file_count = 0
            for file_path in files:
                if isinstance(file_path, str):
                    path = file_path
                elif isinstance(file_path, dict):
                    path = file_path.get("path") or file_path.get("filename") or str(file_path)
                else:
                    continue
                
                if not is_test_file(path):
                    code_file_count += 1
            
            return code_file_count
        
        # Save per-commit to separate files (all results)
        saved_count = 0
        total_to_save = len(all_candidates)
        all_commits_status_for_csv = []
        
        for idx, candidate in enumerate(all_candidates, 1):
            # Show progress (every 100 or last)
            if idx % 100 == 0 or idx == total_to_save:
                percentage = idx * 100 // total_to_save if total_to_save > 0 else 0
                self.log(f"  Saving result files: {idx}/{total_to_save} ({percentage}%)")
            
            # Count files
            code_file_count = get_code_file_count(candidate)
            
            # Create dir
            file_count_dir = results_dir / f"file_count_{code_file_count}"
            file_count_dir.mkdir(parents=True, exist_ok=True)
            
            # Filename: owner__repo__commit_hash.json
            fork_repo = candidate.fork_repo.replace("/", "__")
            commit_hash = candidate.item_id[:12] if len(candidate.item_id) >= 12 else candidate.item_id
            filename = f"{fork_repo}__{commit_hash}.json"
            result_file = file_count_dir / filename
            
            # Analyze fork sync status if needed
            commit_sync_status = None
            if analyze_sync_status and github_projects_json:
                try:
                    # Get input file path if exists
                    input_file_str = candidate.raw_data.get("_input_file")
                    input_commit_file = Path(input_file_str) if input_file_str else None
                    
                    commit_sync_status = self.analyze_fork_sync_status_per_commit(
                        candidate,
                        github_projects_json,
                        input_commit_file=input_commit_file
                    )
                    all_commits_status_for_csv.append(commit_sync_status)
                except Exception as e:
                    self.log(f"Analyze fork sync for commit {candidate.item_id} failed: {e}", "WARNING")
            
            # Save all results including failures
            # Debug info already cleaned in Stage 2
            try:
                candidate_dict = candidate.to_dict()
                # Add sync status to candidate_dict
                if commit_sync_status:
                    candidate_dict["sync_status"] = commit_sync_status
                
                with open(result_file, "w", encoding="utf-8") as f:
                    json.dump(candidate_dict, f, ensure_ascii=False, indent=2)
                saved_count += 1
            except Exception as e:
                self.log(f"Save result file failed {result_file}: {e}", "ERROR")
        
        # Stats
        high_confidence_count = len(high_confidence_candidates)
        failed_count = total_to_save - high_confidence_count
        self.log(f"Saved {saved_count} result files (high-confidence: {high_confidence_count}, not-recommended: {failed_count})")
        
        # If sync status collected: save to CSV
        if all_commits_status_for_csv:
            self.save_commits_sync_status_csv(all_commits_status_for_csv, results_dir.parent)
        
        return saved_count, all_commits_status_for_csv
    
    def save_checkpoint(self, state: Dict):
        """Save checkpoint"""
        try:
            with open(self.checkpoint_file, "w", encoding="utf-8") as f:
                json.dump(state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.log(f"Save checkpoint failed: {e}", "ERROR")
    
    def load_existing_results(self) -> Dict[str, Any]:
        """
        Load existing result file if present
        
        Returns:
            Existing result data or empty dict
        """
        if not self.results_file.exists():
            return {}
        
        try:
            with open(self.results_file, "r", encoding="utf-8") as f:
                existing_data = json.load(f)
            return existing_data
        except Exception as e:
            self.log(f"Load existing result file failed: {e}", "WARNING")
            return {}
    
    def merge_results_data(self, existing_data: Dict[str, Any], new_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge existing and new results
        
        Args:
            existing_data: existing result data
            new_data: new result data
        Returns:
            merged result data
        """
        # If existing empty: return new
        if not existing_data:
            return new_data
        
        # Merge commits (dedup by item_id)
        existing_commits = {c.get("item_id"): c for c in existing_data.get("commits", [])}
        new_commits = {c.get("item_id"): c for c in new_data.get("commits", [])}
        
        # Merge: new overwrites old
        merged_commits = {**existing_commits, **new_commits}
        merged_commits_list = list(merged_commits.values())
        
        # Merge commits_sync_status (dedup by commit_hash)
        existing_status = {s.get("commit_hash"): s for s in existing_data.get("commits_sync_status", [])}
        new_status = {s.get("commit_hash"): s for s in new_data.get("commits_sync_status", [])}
        
        # Merge: new overwrites old
        merged_status = {**existing_status, **new_status}
        merged_status_list = list(merged_status.values())
        
        # Update stats
        merged_data = {
            "timestamp": new_data.get("timestamp", datetime.now().isoformat()),
            "last_update": datetime.now().isoformat(),
            "total_candidates": len(merged_commits_list),
            "high_confidence": len([c for c in merged_commits_list if c.get("final_decision") == "high-confidence"]),
            "not_recommended": len([c for c in merged_commits_list if c.get("final_decision") == "not-recommended"]),
            "results_dir": new_data.get("results_dir", existing_data.get("results_dir", "")),
            "note": new_data.get("note", existing_data.get("note", "")),
            "commits": merged_commits_list,
            "commits_sync_status": merged_status_list
        }
        
        # Keep other fields if present
        for key in ["fork_sync_status_statistics"]:
            if key in new_data:
                merged_data[key] = new_data[key]
            elif key in existing_data:
                merged_data[key] = existing_data[key]
        
        return merged_data
    
    def run_stage(
        self,
        stage_num: int,
        candidates: List[CandidateItem],
        resume: bool = True
    ) -> List[CandidateItem]:
        """
        Run single stage
        
        Args:
            stage_num: stage number (0-5)
            candidates
            resume: whether to resume from checkpoint
        
        Returns:
            Candidates passing the stage
        """
        if stage_num not in self.stages:
            raise ValueError(f"Invalid stage number: {stage_num}")
        
        stage = self.stages[stage_num]
        
        # Check for checkpoint
        if resume:
            checkpoint = self.load_checkpoint()
            if checkpoint and f"stage_{stage_num}_completed" in checkpoint:
                # Load stage result
                stage_results_file = self.stage_dirs[stage_num] / "results.json"
                if stage_results_file.exists():
                    with open(stage_results_file, "r", encoding="utf-8") as f:
                        stage_results = json.load(f)
                    # Update candidates
                    for candidate in candidates:
                        if candidate.item_id in stage_results:
                            setattr(candidate, f"stage{stage_num}_result", stage_results[candidate.item_id])
                    return candidates
        
        # Run stage
        try:
            passed_candidates = stage.process(candidates)
            
            # Save stage result (all candidates including rejected)
            stage_results = {}
            # Collect from passed candidates first
            for candidate in passed_candidates:
                result = getattr(candidate, f"stage{stage_num}_result")
                if result:
                    stage_results[candidate.item_id] = result
            
            # Then from all candidates (including rejected)
            # Ensures rejection reasons saved even if all rejected
            for candidate in candidates:
                if candidate.item_id not in stage_results:
                    result = getattr(candidate, f"stage{stage_num}_result")
                    if result:
                        stage_results[candidate.item_id] = result
            
            stage_results_file = self.stage_dirs[stage_num] / "results.json"
            with open(stage_results_file, "w", encoding="utf-8") as f:
                json.dump(stage_results, f, ensure_ascii=False, indent=2)
            
            # Update checkpoint
            checkpoint = self.load_checkpoint() or {}
            checkpoint[f"stage_{stage_num}_completed"] = True
            checkpoint[f"stage_{stage_num}_timestamp"] = datetime.now().isoformat()
            checkpoint[f"stage_{stage_num}_passed"] = len(passed_candidates)
            checkpoint[f"stage_{stage_num}_total"] = len(candidates)
            self.save_checkpoint(checkpoint)
            
            return passed_candidates
        
        except Exception as e:
            self.log(f"Stage {stage_num} failed: {e}\n{traceback.format_exc()}", "ERROR")
            raise
    
    def make_final_decision(self, candidate: CandidateItem) -> Dict[str, Any]:
        """
        Make final decision from all stage results
        
        Returns:
            Decision dict
        """
        # Collect all stage results
        results = {
            "stage0": candidate.stage0_result,
            "stage1": candidate.stage1_result,
            "stage2": candidate.stage2_result,
        }
        
        # Check if any stage rejected
        rejection_reasons = []
        
        # Stage 1: hard filter reject
        if candidate.stage1_result and not candidate.stage1_result.get("passed", True):
            reasons = candidate.stage1_result.get("reasons", [])
            if reasons:
                rejection_reasons.append(f"Stage1: {'; '.join(reasons)}")
            else:
                rejection_reasons.append("Stage1: hard filter reject")
        
        # Stage 2: technical portability check
        stage2_passed = candidate.stage2_result.get("passed", False) if candidate.stage2_result else False
        
        if not stage2_passed:
            rejection_reasons.append("Stage2: technical portability check failed")
        
        # Tier-based decision (no weighted score)
        # All stages must pass -> high-confidence
        # Any stage fails -> not-recommended
        
        stage1_passed = candidate.stage1_result and candidate.stage1_result.get("passed", False)
        
        if rejection_reasons or not stage1_passed or not stage2_passed:
            decision = "not-recommended"
            final_tier = 0
        else:
            # All stages passed
            decision = "high-confidence"
            final_tier = 1
        
        # Keep score for compatibility
        final_score = 1.0 if final_tier == 1 else 0.0
        
        return {
            "decision": decision,
            "tier": final_tier,
            "tier_name": decision,
            "score": final_score,
            "rejection_reasons": rejection_reasons,
            "stage_tiers": {
                "stage1": 1 if stage1_passed else 0,
                "stage2": 1 if stage2_passed else 0,
            },
            "stage_tier_names": {
                "stage1": "passed" if stage1_passed else "failed",
                "stage2": "passed" if stage2_passed else "failed",
            }
        }
    
    def group_by_family(self, input_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group input by family (origin_repo)
        
        Args:
            input_data: may be:
                1. single family dict
                2. families format with families key
                3. list format
        
        Returns:
            Dict grouped by origin_repo
            {
                "owner/origin1": [
                    {"fork_repo": "owner/fork1", "origin_repo": "owner/origin1", "commits": [...]},
                    {"fork_repo": "owner/fork2", "origin_repo": "owner/origin1", "commits": [...]}
                ],
                "owner/origin2": [...]
            }
        """
        families = {}
        
        # Case 1: single family (backward compat)
        if "fork_repo" in input_data and "origin_repo" in input_data:
            origin_repo = input_data["origin_repo"]
            if origin_repo not in families:
                families[origin_repo] = {
                    "forks": [],
                    "metadata": {}
                }
            # Save family metadata
            if "origin_repo_path" in input_data:
                families[origin_repo]["metadata"]["origin_repo_path"] = input_data["origin_repo_path"]
            if "other_forks_paths" in input_data:
                families[origin_repo]["metadata"]["other_forks_paths"] = input_data["other_forks_paths"]
            families[origin_repo]["forks"].append(input_data)
        
        # Case 2: families format
        elif "families" in input_data:
            # Check streaming mode
            streaming_mode = input_data.get("_streaming_mode", False)
            
            for family_item in input_data["families"]:
                origin_repo = family_item.get("origin_repo")
                if not origin_repo:
                    continue
                
                if origin_repo not in families:
                    families[origin_repo] = {
                        "forks": [],
                        "metadata": {}
                    }
                
                # Save family metadata
                if "origin_repo_path" in family_item:
                    families[origin_repo]["metadata"]["origin_repo_path"] = family_item["origin_repo_path"]
                if "other_forks_paths" in family_item:
                    families[origin_repo]["metadata"]["other_forks_paths"] = family_item["other_forks_paths"]
                
                # Process forks list or commit file list
                commit_files_mode = input_data.get("_commit_files_mode", False)
                
                if streaming_mode and commit_files_mode and "commit_files" in family_item:
                    # Streaming + commit file: load on demand
                    commit_files = family_item.get("commit_files", [])
                    # Group commit files by fork
                    fork_commits = {}
                    for commit_file in commit_files:
                        try:
                            with open(commit_file, "r", encoding="utf-8") as f:
                                commit_data = json.load(f)
                            
                            fork_repo = commit_data.get("fork_repo", "")
                            if not fork_repo:
                                continue
                            
                            # Extract commit from commit_data
                            commit_info = {k: v for k, v in commit_data.items() if k not in ["fork_repo", "origin_repo"]}
                            
                            if fork_repo not in fork_commits:
                                fork_commits[fork_repo] = []
                            fork_commits[fork_repo].append(commit_info)
                        except json.JSONDecodeError as e:
                            # Use print (self.log may be unavailable)
                            print(f"Warning: JSON parse failed {commit_file}: {e}")
                            print(f"   Error at line {e.lineno}, col {e.colno} (pos {e.pos})")
                            print(f"   Suggestion: regenerate with generate_syncability_input.py")
                            # Delete incomplete file
                            try:
                                commit_file.unlink()
                                print(f"   Deleted incomplete file: {commit_file.name}")
                            except Exception:
                                pass
                        except Exception as e:
                            print(f"Warning: failed to load commit file {commit_file}: {e}")
                    
                    # Organize commit data as fork format
                    for fork_repo, commits in fork_commits.items():
                        fork_data = {
                            "fork_repo": fork_repo,
                            "origin_repo": origin_repo,
                            "unique_commits": commits
                        }
                        families[origin_repo]["forks"].append(fork_data)
                
                elif streaming_mode and "fork_files" in family_item:
                    # Streaming: load fork data on demand (old format)
                    fork_files = family_item.get("fork_files", [])
                    for fork_file in fork_files:
                        try:
                            with open(fork_file, "r", encoding="utf-8") as f:
                                fork_data = json.load(f)
                            fork_data["origin_repo"] = origin_repo
                            families[origin_repo]["forks"].append(fork_data)
                        except json.JSONDecodeError as e:
                            # Use print (self.log may be unavailable)
                            print(f"Warning: JSON parse failed {fork_file}: {e}")
                            print(f"   Error at line {e.lineno}, col {e.colno} (pos {e.pos})")
                            print(f"   Suggestion: regenerate with generate_syncability_input.py")
                        except Exception as e:
                            print(f"Warning: failed to load fork file {fork_file}: {e}")
                else:
                    # Traditional: use forks list directly
                    forks = family_item.get("forks", [])
                    for fork_data in forks:
                        fork_data["origin_repo"] = origin_repo
                        families[origin_repo]["forks"].append(fork_data)
        
        # Case 3: list format
        elif isinstance(input_data, list):
            for item in input_data:
                origin_repo = item.get("origin_repo")
                if not origin_repo:
                    continue
                
                if origin_repo not in families:
                    families[origin_repo] = {
                        "forks": [],
                        "metadata": {}
                    }
                # Save family metadata
                if "origin_repo_path" in item:
                    families[origin_repo]["metadata"]["origin_repo_path"] = item["origin_repo_path"]
                if "other_forks_paths" in item:
                    families[origin_repo]["metadata"]["other_forks_paths"] = item["other_forks_paths"]
                families[origin_repo]["forks"].append(item)
        
        return families
    
    def sort_candidates_by_file_count(self, candidates: List[CandidateItem]) -> List[CandidateItem]:
        """
        Sort candidates by code file count (exclude test, fewer first)
        Same count: prefer with test files.

        Args:
            candidates
            
        Returns:
            Sorted candidates
        """
        import re
        
        # Get test patterns from stage0
        test_patterns = self.config.get("stage0", {}).get("test_file_patterns", [
            ".*test.*",
            ".*spec\\.",
            ".*_test\\.",
            ".*\\.test\\.",
            "test/.*",
            "tests/.*",
            ".*Test\\.",
            ".*Spec\\."
        ])
        
        # Compile regex
        test_regexes = [re.compile(pattern, re.IGNORECASE) for pattern in test_patterns]
        
        def is_test_file(file_path: str) -> bool:
            """Check if test file"""
            for regex in test_regexes:
                if regex.search(file_path):
                    return True
            return False
        
        def get_sort_key(candidate: CandidateItem) -> tuple:
            """
            Get sort key.
            Returns: (code_file_count, not has_test)
            """
            raw_data = candidate.raw_data
            files = raw_data.get("files", [])
            if not isinstance(files, list):
                return (0, True)
            
            # Count code files and has_test
            code_file_count = 0
            has_test_file = False
            
            for file_path in files:
                if isinstance(file_path, str):
                    if is_test_file(file_path):
                        has_test_file = True
                    else:
                        code_file_count += 1
                elif isinstance(file_path, dict):
                    # If files are dicts: get path
                    path = file_path.get("path") or file_path.get("filename") or str(file_path)
                    if is_test_file(path):
                        has_test_file = True
                    else:
                        code_file_count += 1
            
            # Sort key: fewer first, same count prefer with test
            return (code_file_count, not has_test_file)
        
        # Sort by key
        sorted_candidates = sorted(candidates, key=get_sort_key)
        return sorted_candidates
    
    def run_single_family(
        self,
        origin_repo: str,
        family_data: List[Dict[str, Any]],
        family_metadata: Optional[Dict[str, Any]] = None,
        start_stage: int = 0,
        end_stage: int = 2,
        resume: bool = True,
        sort_by_file_count: bool = False
    ) -> List[CandidateItem]:
        """
        Run pipeline for single family.

        Args:
            origin_repo, family_data, family_metadata
            start_stage, end_stage, resume

        Returns:
            Final candidates for family
        """
        # Update Stage 2 config (read path from family_metadata)
        if family_metadata:
            stage2_config = self.config.get("stage2", {}).copy()
            if "origin_repo_path" in family_metadata:
                stage2_config["origin_repo_path"] = family_metadata["origin_repo_path"]
            if "other_forks_paths" in family_metadata:
                stage2_config["other_forks_paths"] = family_metadata["other_forks_paths"]
            # Update Stage 2 config
            self.stages[2].config.update(stage2_config)
        
        all_candidates = []
        
        # Process each fork in family
        for fork_data in family_data:
            # Extract candidate from fork
            fork_candidates = self.stages[0].extract_candidates(fork_data)
            all_candidates.extend(fork_candidates)
        
        if not all_candidates:
            return []
        
        # If sort by file count enabled, sort candidates
        if sort_by_file_count:
            all_candidates = self.sort_candidates_by_file_count(all_candidates)
        
        # Run each stage
        candidates = all_candidates
        
        # Run each stage
        for stage_num in range(max(1, start_stage), min(3, end_stage + 1)):
            candidates = self.run_stage(stage_num, candidates, resume=resume)
        
        # Make final decision
        final_results = []
        for candidate in candidates:
            decision = self.make_final_decision(candidate)
            candidate.final_decision = decision["decision"]
            candidate.final_score = decision["score"]
            candidate.rejection_reason = "; ".join(decision["rejection_reasons"]) if decision["rejection_reasons"] else None
            final_results.append(candidate)
        
        self.log(f"  Family {origin_repo} done: {len(final_results)} final candidates")
        return final_results
    
    def run_by_commits(
        self,
        input_data: Dict[str, Any],
        start_stage: int = 0,
        end_stage: int = 2,
        resume: bool = True,
        commit_limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        sort_by_file_count: bool = False,
        max_workers: Optional[int] = None
    ) -> List[CandidateItem]:
        """
        Run pipeline by commit (no family grouping)
        
        Args:
            input_data: Input data (single family or multiple families)
            start_stage: Start stage (default 0)
            end_stage: End stage (default 2)
            resume: whether to resume from checkpoint
            commit_limit: limit new commits per run (None=all)
            batch_size: batch save size
            sort_by_file_count: whether to sort by file count
            
        Returns:
            Final candidate list (all stage results)
        """
        # Commit processing mode
        
        # Check if commit file mode: limit files then identify family
        commit_files_mode = input_data.get("_commit_files_mode", False)
        commit_files_to_process = []
        
        if commit_files_mode and input_data.get("_streaming_mode", False):
            # Commit file mode: collect all commit files first
            families = input_data.get("families", [])
            for family_item in families:
                commit_files = family_item.get("commit_files", [])
                commit_files_to_process.extend(commit_files)
            
            # If commit_limit: limit commit files first
            if commit_limit is not None and commit_limit > 0:
                # Filter already processed (if resume enabled)
                if resume:
                    processed_ids = self.load_processed_commits()
                    if processed_ids:
                        # Use multiprocessing to filter processed commit files
                        # Use functools.partial to pass processed_ids
                        check_func = partial(check_if_processed, processed_ids=processed_ids)
                        
                        # Use multiprocessing for fast filter
                        filter_workers = min(len(commit_files_to_process), cpu_count())
                        filtered_files = []
                        
                        with Pool(processes=filter_workers) as pool:
                            results = pool.imap(check_func, commit_files_to_process)
                            for commit_file, not_processed in results:
                                if not_processed:
                                    filtered_files.append(commit_file)
                        
                        commit_files_to_process = filtered_files
                        self.log(f"Filtered processed commits, remaining {len(commit_files_to_process)} to process")
                
                # Take first commit_limit files, validate, fill incomplete from remaining
                target_count = commit_limit
                files_to_validate = commit_files_to_process[:target_count] if len(commit_files_to_process) >= target_count else commit_files_to_process
                remaining_files = commit_files_to_process[target_count:] if len(commit_files_to_process) > target_count else []
                
                self.log(f"Validate {len(files_to_validate)} files, target {target_count} commits...")
                
                # Use multiprocessing to validate (module-level func for pickle)
                valid_files = []
                invalid_files = []
                validate_workers = min(len(files_to_validate), cpu_count())
                
                with Pool(processes=validate_workers) as pool:
                    results = pool.imap(check_file_valid_for_pipeline, files_to_validate)
                    
                    for commit_file, is_valid, error in results:
                        if is_valid:
                            valid_files.append(commit_file)
                        else:
                            invalid_files.append((commit_file, error))
                            # Delete incomplete file
                            try:
                                commit_file.unlink()
                            except Exception:
                                pass
                
                # If valid files insufficient: fill from remaining
                if len(valid_files) < target_count and remaining_files:
                    need_more = target_count - len(valid_files)
                    self.log(f"Found {len(invalid_files)} incomplete files, need {need_more} more from remaining...")
                    
                    # Validate remaining until target count reached
                    for commit_file in remaining_files:
                        if len(valid_files) >= target_count:
                            break
                        
                        commit_file_result, is_valid, error = check_file_valid_for_pipeline(commit_file)
                        if is_valid:
                            valid_files.append(commit_file_result)
                        else:
                            # Delete incomplete file
                            try:
                                commit_file_result.unlink()
                            except Exception:
                                pass
                    
                    self.log(f"After fill: {len(valid_files)} valid files (target: {target_count})")
                
                commit_files_to_process = valid_files[:target_count]
                self.log(f"Will process {len(commit_files_to_process)} commits (no origin_repo grouping)")
            else:
                # No limit but filter processed
                if resume:
                    processed_ids = self.load_processed_commits()
                    if processed_ids:
                        # Use multiprocessing to filter processed commit files
                        # Use functools.partial to pass processed_ids
                        check_func = partial(check_if_processed, processed_ids=processed_ids)
                        
                        # Use multiprocessing for fast filter
                        filter_workers = min(len(commit_files_to_process), cpu_count())
                        filtered_files = []
                        
                        with Pool(processes=filter_workers) as pool:
                            results = pool.imap(check_func, commit_files_to_process)
                            for commit_file, not_processed in results:
                                if not_processed:
                                    filtered_files.append(commit_file)
                        
                        commit_files_to_process = filtered_files
                        self.log(f"After filter: {len(commit_files_to_process)} to process (no origin_repo grouping)")
        
        # If commit file mode with files: use multiprocessing
        if commit_files_mode and commit_files_to_process:
            # No need to build origin_repo -> family_metadata mapping
            # Each process reads origin_repo etc from commit file
            origin_to_metadata = {}
            
            # Use multiprocessing per commit
            # If max_workers: use it; else min(files, cpu_count)
            if max_workers is not None and max_workers > 0:
                workers = min(max_workers, len(commit_files_to_process))
                self.log(f"\nUsing {workers} processes for {len(commit_files_to_process)} commits (user: {max_workers})")
            else:
                workers = min(len(commit_files_to_process), cpu_count())
                self.log(f"\nUsing {workers} processes for {len(commit_files_to_process)} commits (auto: {cpu_count()} CPUs)")
            self.log(f"Each process handles one commit, identifies family, runs pipeline")
            
            # Create process function (fixed args)
            process_func = partial(
                SyncabilityPipeline.process_single_commit_file,
                work_dir=self.work_dir,
                config=self.config,
                start_stage=start_stage,
                end_stage=end_stage,
                origin_to_metadata=origin_to_metadata
            )
            
            # Multiprocess
            final_results = []
            processed_count = 0
            
            with Pool(processes=workers) as pool:
                results = pool.imap(process_func, commit_files_to_process)
                
                for candidate_dict in results:
                    processed_count += 1
                    
                    # Show progress
                    if processed_count % 10 == 0 or processed_count == len(commit_files_to_process):
                        percentage = processed_count * 100 // len(commit_files_to_process) if commit_files_to_process else 0
                        self.log(f"  Processed {processed_count}/{len(commit_files_to_process)} commits ({percentage}%)...")
                    
                    if candidate_dict:
                        # Convert dict back to CandidateItem (incl. failed)
                        try:
                            candidate = CandidateItem.from_dict(candidate_dict)
                            final_results.append(candidate)
                        except Exception as e:
                            self.log(f"  Warning: cannot parse result dict: {e}", "WARNING")
                            # Even if parse failed: try save raw dict
                            try:
                                # Create basic failed record
                                error_candidate = CandidateItem(
                                    item_id=candidate_dict.get("item_id", "unknown"),
                                    fork_repo=candidate_dict.get("fork_repo", "unknown"),
                                    origin_repo=candidate_dict.get("origin_repo", "unknown"),
                                    item_type=candidate_dict.get("item_type", "commit"),
                                    raw_data=candidate_dict.get("raw_data", {})
                                )
                                error_candidate.final_decision = candidate_dict.get("final_decision", "not-recommended")
                                error_candidate.final_score = candidate_dict.get("final_score", 0)
                                error_candidate.rejection_reason = candidate_dict.get("rejection_reason") or f"Parse failed: {str(e)}"
                                final_results.append(error_candidate)
                            except Exception:
                                pass
            
            self.log(f"\nMultiprocess done: {processed_count} commits, {len(final_results)} success")
            
            # get github_projects_json path
            github_projects_json = None
            github_projects_path = self.config.get("github_projects_json")
            if github_projects_path:
                github_projects_json = Path(github_projects_path)
            elif Path("github_projects_filtered_stars_10.json").exists():
                github_projects_json = Path("github_projects_filtered_stars_10.json")
            
            # Save final result (incl. failed, avoid re-analyze)
            # Analyze sync status during save
            self.log(f"Saving result files (incl. failed records)...")
            results_dir = self.work_dir / "results_by_commit"
            saved_count, all_commits_status = self.save_commit_results(
                final_results, 
                github_projects_json=github_projects_json,
                analyze_sync_status=True
            )
            # Stats already output in save_commit_results
            
            # Keep only high-confidence results
            high_confidence_results = [
                c for c in final_results 
                if c.final_decision == "high-confidence"
            ]
            
            self.log(f"Generating summary pipeline_results.json...")
            
            # Save detailed info for all commits
            # Debug info already cleaned in Stage 2
            all_commits_data = []
            total_to_process = len(final_results)
            for idx, candidate in enumerate(final_results, 1):
                if idx % 100 == 0 or idx == total_to_process:
                    self.log(f"  Processing result: {idx}/{total_to_process} ({idx * 100 // total_to_process if total_to_process > 0 else 0}%)")
                all_commits_data.append(candidate.to_dict())
            
            # Prepare basic result data
            results_data = {
                "timestamp": datetime.now().isoformat(),
                "total_candidates": len(final_results),
                "high_confidence": len(high_confidence_results),
                "not_recommended": len([c for c in final_results if c.final_decision == "not-recommended"]),
                "results_dir": str(results_dir),
                "note": f"All commit results. Per-commit in {results_dir}, by file count in file_count_N subdirs",
                "commits": all_commits_data
            }
            
            # Fork sync status stats added later
            # Write basic data first (if no stats)
            # Stats added to results_data later
            
            self.log(f"\n{'=' * 80}")
            self.log("Final result summary")
            self.log(f"{'=' * 80}")
            self.log(f"Total candidates: {len(final_results)}")
            self.log(f"  - High confidence: {results_data['high_confidence']}")
            self.log(f"  - Not recommended: {results_data['not_recommended']}")
            self.log(f"  - Saved {saved_count} result files")
            self.log(f"\nFinal result saved to: {self.results_file}")
            self.log(f"Per-commit results in: {results_dir} (by file count)")
            
            # Stats fork sync status (by commit, then by origin)
            # Sync status already analyzed in save_commit_results
            self.log(f"\n{'=' * 80}")
            self.log("Starting fork sync status stats...")
            self.log(f"{'=' * 80}")
            
            # Use sync status from save_commit_results
            # all_commits_status from save_commit_results
            
            # Aggregate stats by origin
            if all_commits_status:
                origin_stats = self.aggregate_fork_sync_status_by_origin(all_commits_status)
                
                # Save stats to CSV
                self.save_fork_sync_status_statistics(origin_stats)
                
                # Add stats to JSON result
                results_data["commits_sync_status"] = all_commits_status
                
                # Print stats summary
                self.log(f"\n{'=' * 80}")
                self.log("Fork sync status stats summary")
                self.log(f"{'=' * 80}")
                for origin_repo, stats in origin_stats.items():
                    self.log(f"\n{origin_repo}:")
                    self.log(f"  Syncable forks: {stats['syncable_forks_count']}")
                    self.log(f"  Unsyncable forks: {stats['unsyncable_forks_count']}")
                    self.log(f"  Already synced forks: {stats['already_synced_forks_count']}")
                    self.log(f"  Total commits: {stats['total_commits']}")
            
            # Load existing result if present and merge
            existing_data = self.load_existing_results()
            if existing_data:
                self.log(f"Found existing result file, merging new results...")
                existing_count = len(existing_data.get('commits', []))
                new_count = len(all_commits_data)
                results_data = self.merge_results_data(existing_data, results_data)
                merged_count = len(results_data.get('commits', []))
                self.log(f"Merge complete: existing {existing_count} commits, added {new_count}, total {merged_count} commits")
            
            # Write full JSON file (include stats info)
            self.log(f"Writing pipeline_results.json (append mode)...")
            with open(self.results_file, "w", encoding="utf-8") as f:
                json.dump(results_data, f, ensure_ascii=False, indent=2)
            self.log(f"Written pipeline_results.json ({len(results_data.get('commits', []))} commits)")
            
            return final_results
        
        # Legacy mode: collect all candidates (from limited families)
        all_candidates = []
        
        # Group by family to extract candidates (use limited families)
        if commit_files_mode:
            # Extract candidates directly from commit files
            for family_item in families:
                commit_files = family_item.get("commit_files", [])
                origin_repo = family_item.get("origin_repo", "")
                family_metadata = family_item.get("metadata", {})
                
                # Update Stage 2 config
                if family_metadata:
                    stage2_config = self.config.get("stage2", {}).copy()
                    if "origin_repo_path" in family_metadata:
                        stage2_config["origin_repo_path"] = family_metadata["origin_repo_path"]
                    if "other_forks_paths" in family_metadata:
                        stage2_config["other_forks_paths"] = family_metadata["other_forks_paths"]
                    self.stages[2].config.update(stage2_config)
                
                # Extract candidates directly from commit files
                for commit_file in commit_files:
                    try:
                        with open(commit_file, "r", encoding="utf-8") as f:
                            commit_data = json.load(f)
                        
                        fork_repo = commit_data.get("fork_repo", "")
                        if not fork_repo:
                            continue
                        
                        # Convert single commit to format expected by extract_candidates
                        fork_data = {
                            "fork_repo": fork_repo,
                            "origin_repo": origin_repo,
                            "unique_commits": [{k: v for k, v in commit_data.items() if k not in ["fork_repo", "origin_repo"]}]
                        }
                        
                        fork_candidates = self.stages[0].extract_candidates(fork_data)
                        all_candidates.extend(fork_candidates)
                    except Exception as e:
                        self.log(f"⚠️ warning: load commit filefailed {commit_file}: {e}")
        else:
            # Legacy mode: group by family to extract candidates
            families_dict = self.group_by_family(input_data)
            
            for origin_repo, family_info in families_dict.items():
                if isinstance(family_info, dict) and "forks" in family_info:
                    forks = family_info["forks"]
                    family_metadata = family_info.get("metadata", {})
                else:
                    forks = family_info if isinstance(family_info, list) else []
                    family_metadata = {}
                
                # Update Stage 2 config (read path from family_metadata)
                if family_metadata:
                    stage2_config = self.config.get("stage2", {}).copy()
                    if "origin_repo_path" in family_metadata:
                        stage2_config["origin_repo_path"] = family_metadata["origin_repo_path"]
                    if "other_forks_paths" in family_metadata:
                        stage2_config["other_forks_paths"] = family_metadata["other_forks_paths"]
                    self.stages[2].config.update(stage2_config)
                
                # Extract candidates from all forks
                for fork_data in forks:
                    fork_repo = fork_data.get("fork_repo", "")
                    if not fork_repo:
                        continue
                    
                    fork_candidates = self.stages[0].extract_candidates(fork_data)
                    all_candidates.extend(fork_candidates)
        
        if not all_candidates:
            self.log("No candidates found, exiting")
            return []
        
        # If sort by file count enabled, sort candidates
        # If from dir (commit_files_mode): dir already sorted by file count
        if sort_by_file_count and not commit_files_mode:
            all_candidates = self.sort_candidates_by_file_count(all_candidates)
        
        # Run each stage
        candidates = all_candidates
        
        for stage_num in range(max(1, start_stage), min(3, end_stage + 1)):
            candidates = self.run_stage(stage_num, candidates, resume=resume)
        
        # Make final decision
        final_results = []
        for candidate in candidates:
            decision = self.make_final_decision(candidate)
            candidate.final_decision = decision["decision"]
            candidate.final_score = decision["score"]
            candidate.rejection_reason = "; ".join(decision["rejection_reasons"]) if decision["rejection_reasons"] else None
            final_results.append(candidate)
        
        # Save final result (one file per commit, grouped by file count)
        # Analyze sync status when saving to avoid duplicate analysis later
        results_dir = self.work_dir / "results_by_commit"
        saved_count, all_commits_status = self.save_commit_results(
            final_results, 
            github_projects_json=github_projects_json,
            analyze_sync_status=True
        )
        
        # Save summary (incl. per-commit details)
        results_data = {
            "timestamp": datetime.now().isoformat(),
            "total_candidates": len(final_results),
            "high_confidence": len([c for c in final_results if c.final_decision == "high-confidence"]),
            "not_recommended": len([c for c in final_results if c.final_decision == "not-recommended"]),
            "results_dir": str(results_dir),
            "note": f"All commit results. Per-commit in {results_dir}, by file count in file_count_N subdirs",
            "commits": [c.to_dict() for c in final_results]
        }
        
        # Fork sync status stats will be added later (if present)
        # Write basic data first
        
        # Stats fork sync status (per commit, then aggregate by origin)
        # Note: sync status already analyzed in save_commit_results, use result directly here
        self.log(f"\n{'=' * 80}")
        self.log("Starting fork sync status stats...")
        self.log(f"{'=' * 80}")
        
        # Use sync status already analyzed in save_commit_results to avoid duplicate analysis
        # all_commits_status returned from save_commit_results
        
        # Aggregate stats by origin
        if all_commits_status:
            origin_stats = self.aggregate_fork_sync_status_by_origin(all_commits_status)
            
            # Save stats result to CSV
            self.save_fork_sync_status_statistics(origin_stats)
            
            # Add stats info to JSON result
            results_data["fork_sync_status_statistics"] = origin_stats
            results_data["commits_sync_status"] = all_commits_status  # Detailed sync status per commit
            
            # Print stats summary
            self.log(f"\n{'=' * 80}")
            self.log("Fork sync status stats summary")
            self.log(f"{'=' * 80}")
            for origin_repo, stats in origin_stats.items():
                self.log(f"\n{origin_repo}:")
                self.log(f"  Syncable forks: {stats['syncable_forks_count']}")
                self.log(f"  Unsyncable forks: {stats['unsyncable_forks_count']}")
                self.log(f"  Already synced forks: {stats['already_synced_forks_count']}")
                self.log(f"  Total commits: {stats['total_commits']}")
        
        # Load existing result if present and merge
        existing_data = self.load_existing_results()
        if existing_data:
            self.log(f"Found existing result file, merging new results...")
            existing_count = len(existing_data.get('commits', []))
            new_count = len([c.to_dict() for c in final_results])
            results_data = self.merge_results_data(existing_data, results_data)
            merged_count = len(results_data.get('commits', []))
            self.log(f"Merge complete: existing {existing_count} commits, added {new_count}, total {merged_count} commits")
        
        # Write full JSON file (include stats info, append mode)
        self.log(f"Writing pipeline_results.json (append mode)...")
        with open(self.results_file, "w", encoding="utf-8") as f:
            json.dump(results_data, f, ensure_ascii=False, indent=2)
        
        self.log(f"\n{'=' * 80}")
        self.log("Final result summary")
        self.log(f"{'=' * 80}")
        self.log(f"Total candidates: {len(results_data.get('commits', []))}")
        self.log(f"  - High confidence: {results_data.get('high_confidence', 0)}")
        self.log(f"  - Not recommended: {results_data.get('not_recommended', 0)}")
        self.log(f"  - Saved {saved_count} result files")
        self.log(f"\nFinal result saved to: {self.results_file} ({len(results_data.get('commits', []))} commits)")
        self.log(f"Each commit result saved in: {results_dir} (grouped by file count)")
        
        return final_results
    
    def process_single_commit(
        self,
        commit_file: Path,
        start_stage: int,
        end_stage: int,
        resume: bool,
        config: Dict[str, Any]
    ) -> Optional[CandidateItem]:
        """
        Process single commit file (for multiprocessing)
        
        Args:
            commit_file: commit filepath
            start_stage: Start stage
            end_stage: End stage
            resume: whether to resume from checkpoint
            config: config dict
            
        Returns:
            Processed candidate object, or None if failed
        """
        try:
            # Read commit data
            with open(commit_file, "r", encoding="utf-8") as f:
                commit_data = json.load(f)
            
            fork_repo = commit_data.get("fork_repo", "")
            origin_repo = commit_data.get("origin_repo", "")
            
            if not fork_repo or not origin_repo:
                return None
            
            # Create temporary pipeline instance (for processing single commit)
            # Note: use separate work_dir to avoid conflicts
            temp_work_dir = self.work_dir / f"temp_{commit_file.stem}"
            temp_pipeline = SyncabilityPipeline(temp_work_dir, config)
            
            # Identify family info for this commit
            # Convert single commit to format expected by extract_candidates
            fork_data = {
                "fork_repo": fork_repo,
                "origin_repo": origin_repo,
                "unique_commits": [{k: v for k, v in commit_data.items() if k not in ["fork_repo", "origin_repo"]}]
            }
            
            # Extract candidate
            candidates = temp_pipeline.stages[0].extract_candidates(fork_data)
            if not candidates:
                return None
            
            candidate = candidates[0]  # Single commit has only one candidate
            
            # Run each stage
            for stage_num in range(max(1, start_stage), min(3, end_stage + 1)):
                stage_candidates = temp_pipeline.run_stage(stage_num, [candidate], resume=False)
                if not stage_candidates:
                    # Stage rejected, return None
                    return None
                candidate = stage_candidates[0]
            
            # Make final decision
            decision = temp_pipeline.make_final_decision(candidate)
            candidate.final_decision = decision["decision"]
            candidate.final_score = decision["score"]
            candidate.rejection_reason = "; ".join(decision["rejection_reasons"]) if decision["rejection_reasons"] else None
            
            # Clean up temporary dir
            try:
                import shutil
                if temp_work_dir.exists():
                    shutil.rmtree(temp_work_dir)
            except Exception:
                pass
            
            return candidate
            
        except Exception as e:
            self.log(f"Process commit file failed {commit_file}: {e}", "ERROR")
            return None
    
    def run(
        self,
        input_data: Dict[str, Any],
        start_stage: int = 0,
        end_stage: int = 2,
        resume: bool = True,
        family_limit: Optional[int] = None,
        commit_limit: Optional[int] = None,
        batch_size: Optional[int] = None,
        sort_by_file_count: bool = False,
        by_commits: bool = False,
        max_workers: Optional[int] = None
    ) -> List[CandidateItem]:
        """
        Run full pipeline
        
        Args:
            input_data: Input data (single family or multiple families)
            start_stage: Start stage (default 0)
            end_stage: End stage (default 2)
            resume: whether to resume from checkpoint
            family_limit: Limit number of families to process (None = process all)
            batch_size: batch save size
            sort_by_file_count: whether to sort by file count
            by_commits: Process by commit (True) or by family (False)
            max_workers: Max processes when multiprocessing (None = auto-detect CPU cores, only for by_commits=True)
        
        Returns:
            Final candidate list (all stage results)
        """
        # If commit processing mode enabled, use new method
        if by_commits:
            return self.run_by_commits(
                input_data,
                start_stage=start_stage,
                end_stage=end_stage,
                resume=resume,
                commit_limit=commit_limit,
                batch_size=batch_size,
                sort_by_file_count=sort_by_file_count,
                max_workers=max_workers
            )
        
        # Legacy: process by family
        # Group by family
        families = self.group_by_family(input_data)
        family_list = []
        for origin_repo, family_info in families.items():
            if isinstance(family_info, dict) and "forks" in family_info:
                # New format: include metadata
                family_list.append((origin_repo, family_info["forks"], family_info.get("metadata", {})))
            else:
                # Old format: direct list (backward compat)
                family_list.append((origin_repo, family_info, {}))
        
        # Apply limit
        if family_limit is not None and family_limit > 0:
            family_list = family_list[:family_limit]
        
        if not family_list:
            return []
        
        # Process each family (batch to save memory)
        all_final_results = []
        family_results = {}
        total_candidates_count = 0
        total_high_confidence = 0
        total_not_recommended = 0
        
        # If batch_size: save in batches
        batch_results_file = self.work_dir / "pipeline_results_batch.jsonl" if batch_size else None
        batch_count = 0
        
        for idx, (origin_repo, family_data, family_metadata) in enumerate(family_list, 1):
            if idx % 10 == 0 or idx == len(family_list):
                self.log(f"Progress: {idx}/{len(family_list)} families ({idx * 100 // len(family_list) if family_list else 0}%)")
            
            try:
                family_candidates = self.run_single_family(
                    origin_repo,
                    family_data,
                    family_metadata=family_metadata,
                    start_stage=start_stage,
                    end_stage=end_stage,
                    resume=resume,
                    sort_by_file_count=sort_by_file_count
                )
                
                # Stats for this family result
                family_high_confidence = len([c for c in family_candidates if c.final_decision == "high-confidence"])
                family_not_recommended = len([c for c in family_candidates if c.final_decision == "not-recommended"])
                
                family_results[origin_repo] = {
                    "total_candidates": len(family_candidates),
                    "high_confidence": family_high_confidence,
                    "not_recommended": family_not_recommended
                }
                
                total_candidates_count += len(family_candidates)
                total_high_confidence += family_high_confidence
                total_not_recommended += family_not_recommended
                
                # If batch save enabled, save this batch and release memory
                if batch_size and batch_results_file:
                    batch_count += 1
                    if batch_count % batch_size == 0 or idx == len(family_list):
                        # Save this batch of candidates to JSONL (one candidate per line)
                        with open(batch_results_file, "a", encoding="utf-8") as f:
                            for candidate in family_candidates:
                                f.write(json.dumps(candidate.to_dict(), ensure_ascii=False) + "\n")
                        # Release memory: do not keep candidate objects, only stats info
                        del family_candidates
                        self.log(f"  Saved batch {batch_count}, released memory")
                else:
                    # No batch save, keep all candidates in memory
                    all_final_results.extend(family_candidates)
                
                # Release family_data memory
                del family_data
                
            except Exception as e:
                self.log(f"  ❌ Family {origin_repo} process failed: {e}", "ERROR")
                family_results[origin_repo] = {
                    "error": str(e),
                    "total_candidates": 0
                }
        
        # Get github_projects_json path (before saving result)
        github_projects_json = None
        github_projects_path = self.config.get("github_projects_json")
        if github_projects_path:
            github_projects_json = Path(github_projects_path)
        elif Path("github_projects_filtered_stars_10.json").exists():
            github_projects_json = Path("github_projects_filtered_stars_10.json")
        
        # Save final result (one file per commit, grouped by file count)
        # Analyze sync status when saving to avoid duplicate analysis later
        results_dir = self.work_dir / "results_by_commit"
        saved_count, all_commits_status = self.save_commit_results(
            all_final_results, 
            github_projects_json=github_projects_json,
            analyze_sync_status=True
        )
        
        # Save summary info (include all commit results)
            # Debug info already cleaned in Stage 2
        results_data = {
            "timestamp": datetime.now().isoformat(),
            "total_families": len(family_list),
            "total_candidates": total_candidates_count,
            "high_confidence": total_high_confidence,
            "not_recommended": total_not_recommended,
            "family_results": family_results,
            "results_dir": str(results_dir),
                "note": f"All commit results. Per-commit in {results_dir}, by file count in file_count_N subdirs",
            "commits": [c.to_dict() for c in all_final_results]  # Save all commit results
        }
        
        # Fork sync status stats will be added later (if present)
        # Prepare basic data first
        
        self.log(f"\n{'=' * 80}")
        self.log("Final result summary")
        self.log(f"{'=' * 80}")
        self.log(f"Families processed: {len(family_list)}")
        self.log(f"Total candidates: {total_candidates_count}")
        self.log(f"  - High confidence: {total_high_confidence}")
        self.log(f"  - Not recommended: {total_not_recommended}")
        self.log(f"  - Saved {saved_count} result files")
        self.log(f"\nFinal result saved to: {self.results_file}")
        self.log(f"Each commit result saved in: {results_dir} (grouped by file count)")
        
        # Stats fork sync status (per commit, then aggregate by origin)
        # Note: sync status already analyzed in save_commit_results, use result directly here
        self.log(f"\n{'=' * 80}")
        self.log("Starting fork sync status stats...")
        self.log(f"{'=' * 80}")
        
        # Use sync status already analyzed in save_commit_results to avoid duplicate analysis
        # all_commits_status returned from save_commit_results
        
        # Aggregate stats by origin
        if all_commits_status:
            origin_stats = self.aggregate_fork_sync_status_by_origin(all_commits_status)
            
            # Save stats result to CSV
            self.save_fork_sync_status_statistics(origin_stats)
            
            # Add stats info to JSON result
            results_data["fork_sync_status_statistics"] = origin_stats
            results_data["commits_sync_status"] = all_commits_status  # Detailed sync status per commit
            
            # Print stats summary
            self.log(f"\n{'=' * 80}")
            self.log("Fork sync status stats summary")
            self.log(f"{'=' * 80}")
            for origin_repo, stats in origin_stats.items():
                self.log(f"\n{origin_repo}:")
                self.log(f"  Syncable forks: {stats['syncable_forks_count']}")
                self.log(f"  Unsyncable forks: {stats['unsyncable_forks_count']}")
                self.log(f"  Already synced forks: {stats['already_synced_forks_count']}")
                self.log(f"  Total commits: {stats['total_commits']}")
        
        # Load existing result if present and merge
        existing_data = self.load_existing_results()
        if existing_data:
            self.log(f"Found existing result file, merging new results...")
            existing_count = len(existing_data.get('commits', []))
            new_count = len([c.to_dict() for c in all_final_results])
            results_data = self.merge_results_data(existing_data, results_data)
            merged_count = len(results_data.get('commits', []))
            self.log(f"Merge complete: existing {existing_count} commits, added {new_count}, total {merged_count} commits")
        
        # Write full JSON file (include stats info, append mode)
        self.log(f"Writing pipeline_results.json (append mode)...")
        with open(self.results_file, "w", encoding="utf-8") as f:
            json.dump(results_data, f, ensure_ascii=False, indent=2)
        self.log(f"Written pipeline_results.json ({len(results_data.get('commits', []))} commits)")
        
        return all_final_results
    
    @staticmethod
    def process_single_commit_file(
        commit_file: Path,
        work_dir: Path,
        config: Dict[str, Any],
        start_stage: int,
        end_stage: int,
        origin_to_metadata: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Process single commit file (for multiprocessing, static method)
        
        Args:
            commit_file: commit filepath
            work_dir: work dir
            config: config dict
            start_stage: start stage
            end_stage: end stage
            
        Returns:
            Processed candidate dict (to_dict()), or None if failed
        """
        try:
            # Read commit data
            with open(commit_file, "r", encoding="utf-8") as f:
                commit_data = json.load(f)
            
            fork_repo = commit_data.get("fork_repo", "")
            origin_repo = commit_data.get("origin_repo", "")
            
            if not fork_repo or not origin_repo:
                # Create failed record
                commit_hash = commit_data.get("hash") or commit_data.get("sha") or commit_data.get("id") or "unknown"
                error_candidate = CandidateItem(
                    item_id=commit_hash,
                    fork_repo=fork_repo or "unknown",
                    origin_repo=origin_repo or "unknown",
                    item_type="commit",
                    raw_data=commit_data
                )
                error_candidate.final_decision = "not-recommended"
                error_candidate.final_score = 0
                error_candidate.rejection_reason = f"Process failed: fork_repo or origin_repo empty (fork_repo={fork_repo}, origin_repo={origin_repo})"
                return error_candidate.to_dict()
            
            # Create independent pipeline instance (each process runs independently)
            temp_work_dir = work_dir / f"temp_{commit_file.stem}"
            temp_pipeline = SyncabilityPipeline(temp_work_dir, config)
            
            # Identify family info for this commit
            # Convert single commit to format expected by extract_candidates
            fork_data = {
                "fork_repo": fork_repo,
                "origin_repo": origin_repo,
                "unique_commits": [{k: v for k, v in commit_data.items() if k not in ["fork_repo", "origin_repo"]}]
            }
            
            # Extract candidate
            candidates = temp_pipeline.stages[0].extract_candidates(fork_data)
            if not candidates:
                # Create failed record
                commit_hash = commit_data.get("hash") or commit_data.get("sha") or commit_data.get("id") or "unknown"
                error_candidate = CandidateItem(
                    item_id=commit_hash,
                    fork_repo=fork_repo,
                    origin_repo=origin_repo,
                    item_type="commit",
                    raw_data=commit_data
                )
                error_candidate.final_decision = "not-recommended"
                error_candidate.final_score = 0
                error_candidate.rejection_reason = "Process failed: Stage 0 extract candidate failed (cannot extract candidate from commit data)"
                return error_candidate.to_dict()
            
            candidate = candidates[0]  # Single commit has only one candidate
            
            # Update Stage 2 config (read family metadata from commit data or family_metadata)
            stage2_config = temp_pipeline.config.get("stage2", {}).copy()
            
            # Prefer reading from commit data
            if "origin_repo_path" in commit_data:
                stage2_config["origin_repo_path"] = commit_data["origin_repo_path"]
            if "other_forks_paths" in commit_data:
                stage2_config["other_forks_paths"] = commit_data["other_forks_paths"]
            
            # If not in commit data, get from origin_to_metadata mapping
            if origin_to_metadata and origin_repo in origin_to_metadata:
                family_metadata = origin_to_metadata[origin_repo]
                if "origin_repo_path" in family_metadata and "origin_repo_path" not in stage2_config:
                    stage2_config["origin_repo_path"] = family_metadata["origin_repo_path"]
                if "other_forks_paths" in family_metadata and "other_forks_paths" not in stage2_config:
                    stage2_config["other_forks_paths"] = family_metadata["other_forks_paths"]
            
            temp_pipeline.stages[2].config.update(stage2_config)
            
            # Run each stage
            for stage_num in range(max(1, start_stage), min(3, end_stage + 1)):
                stage_candidates = temp_pipeline.run_stage(stage_num, [candidate], resume=False)
                if not stage_candidates:
                    # Stage rejected but need to save result for analysis
                    # Read reject reason from stage result file
                    stage_results_file = temp_pipeline.stage_dirs[stage_num] / "results.json"
                    if stage_results_file.exists():
                        try:
                            with open(stage_results_file, "r", encoding="utf-8") as f:
                                stage_results = json.load(f)
                            if candidate.item_id in stage_results:
                                setattr(candidate, f"stage{stage_num}_result", stage_results[candidate.item_id])
                        except Exception:
                            pass
                    # Continue processing so make_final_decision sets reject reason
                    break
                else:
                    candidate = stage_candidates[0]
            
            # Make final decision (even if rejected, sets final_decision and rejection_reason)
            decision = temp_pipeline.make_final_decision(candidate)
            candidate.final_decision = decision["decision"]
            candidate.final_score = decision["score"]
            candidate.rejection_reason = "; ".join(decision["rejection_reasons"]) if decision["rejection_reasons"] else None
            
            # Save input filepath to raw_data (for later update of input file)
            candidate.raw_data["_input_file"] = str(commit_file)
            
            # Clean up temporary dir
            try:
                import shutil
                if temp_work_dir.exists():
                    shutil.rmtree(temp_work_dir)
            except Exception:
                pass
            
            return candidate.to_dict()
            
        except Exception as e:
            # Even on exception, create failed record for saving
            try:
                # Try to read basic info from commit_file
                with open(commit_file, "r", encoding="utf-8") as f:
                    commit_data = json.load(f)
                fork_repo = commit_data.get("fork_repo", "unknown")
                origin_repo = commit_data.get("origin_repo", "unknown")
                commit_hash = commit_data.get("hash") or commit_data.get("sha") or commit_data.get("id") or "unknown"
            except Exception:
                # If cannot read file, infer from filename
                fork_repo = "unknown"
                origin_repo = "unknown"
                commit_hash = commit_file.stem.split("__")[-1] if "__" in commit_file.stem else "unknown"
            
            # Create failed record
            error_candidate = CandidateItem(
                item_id=commit_hash,
                fork_repo=fork_repo,
                origin_repo=origin_repo,
                item_type="commit",
                raw_data=commit_data if 'commit_data' in locals() else {}
            )
            error_candidate.final_decision = "not-recommended"
            error_candidate.final_score = 0
            error_candidate.rejection_reason = f"Process failed: exception - {str(e)}"
            return error_candidate.to_dict()


def check_if_processed(commit_file: Path, processed_ids: Set[str]) -> Tuple[Path, bool]:
    """
    Check whether commit file has been processed (module-level function, pickleable)
    
    Args:
        commit_file: commit filepath
        processed_ids: set of already processed commit IDs
    
    Returns:
        (commit_file, not_processed) tuple, not_processed=True means not yet processed
    """
    try:
        with open(commit_file, "r", encoding="utf-8") as f:
            # Read first 2KB only, use regex to extract hash
            content = f.read(2048)
            import re
            # Try to extract hash/sha/id
            match = re.search(r'"(?:hash|sha|id)"\s*:\s*"([^"]+)"', content)
            if match:
                item_id = match.group(1)
                return (commit_file, item_id not in processed_ids)
            # If regex failed, full parse (fallback)
            f.seek(0)
            commit_data = json.load(f)
            item_id = commit_data.get("hash") or commit_data.get("sha") or commit_data.get("id")
            return (commit_file, item_id not in processed_ids if item_id else True)
    except Exception:
        return (commit_file, True)  # On error keep it, let later processing decide


def check_file_valid_for_pipeline(commit_file: Path) -> Tuple[Path, bool, Optional[Exception]]:
    """
    Quick check whether file is valid for pipeline (for multiprocessing, module-level function)
    
    Args:
        commit_file: commit filepath
        
    Returns:
        (filepath, is_valid, error_info) tuple
    """
    try:
        with open(commit_file, "r", encoding="utf-8") as f:
            json.load(f)
        return (commit_file, True, None)
    except json.JSONDecodeError as e:
        return (commit_file, False, e)
    except Exception as e:
        return (commit_file, False, e)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Syncability Pipeline - multi-stage workflow")
    parser.add_argument(
        "--work-dir",
        type=str,
        default="syncability_pipeline_work",
        help="Work dir (intermediate results and checkpoint)"
    )
    parser.add_argument(
        "--input",
        type=str,
        required=False,
        help="Input JSON path (fork_repo, origin_repo, commits/PRs)"
    )
    parser.add_argument(
        "--input-dir",
        type=str,
        required=False,
        help="Input dir (multiple owner__repo.json files)"
    )
    parser.add_argument(
        "--start-stage",
        type=int,
        default=0,
        help="Start stage (0-5, default 0)"
    )
    parser.add_argument(
        "--end-stage",
        type=int,
        default=2,
        help="End stage (0-2, default 2)"
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume from checkpoint, start fresh"
    )
    
    # Run mode selection (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--mode-family",
        action="store_true",
        help="Run in family mode (group by origin_repo, default)"
    )
    mode_group.add_argument(
        "--mode-commit",
        action="store_true",
        help="Run in commit mode (no grouping, process all commits)"
    )
    
    # Family mode arguments
    parser.add_argument(
        "--family-limit",
        type=int,
        default=None,
        help="Limit families (--mode-family only, None=all)"
    )
    
    # Commit mode arguments
    parser.add_argument(
        "--commit-limit",
        type=int,
        default=None,
        help="Limit new commits per run (--mode-commit only, None=all)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max processes (--mode-commit only, None=auto)"
    )
    
    # Common arguments
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Batch save size (save every N, None=save once)"
    )
    parser.add_argument(
        "--sort-by-file-count",
        action="store_true",
        help="Sort by file count, fewer first (--mode-commit only)"
    )
    parser.add_argument(
        "--file-count-filter",
        type=int,
        default=None,
        help="Only process commits with N files (e.g. --file-count-filter 1, --input-dir only)"
    )
    parser.add_argument(
        "--full-check",
        action="store_true",
        help="Full check: skip deepseek, run all checks (incl. git apply)"
    )
    
    args = parser.parse_args()
    
    # Adjust config by arguments
    # If full check mode enabled, set enable_git_apply=True
    enable_git_apply = args.full_check
    
    # Default config
    default_config = {
        "stage0": {
            "test_file_patterns": [
                ".*test.*",
                ".*spec\\.",
                ".*_test\\.",
                ".*\\.test\\.",
                "test/.*",
                "tests/.*",
                ".*Test\\.",
                ".*Spec\\."
            ]
        },
        "stage1": {
            "large_refactor_threshold": 50,
            "vendor_dependency_patterns": [
                "vendor/",
                "third_party/",
                "third-party/",
                "dependencies/",
                "deps/",
                "external/",
                "externals/",
                "submodules/",
                "submodule/",
                "libs/",
                "lib/",
                "modules/",
                "contrib/",
                "contribs/",
                "CMakeLists\\.txt$",
                "cmake/.*\\.cmake$",
                "CMakeCache\\.txt$",
                "Makefile$",
                "makefile$",
                "GNUmakefile$",
                "configure\\.ac$",
                "configure\\.in$",
                "aclocal\\.m4$",
                "Makefile\\.am$",
                "Makefile\\.in$",
                "conanfile\\.txt$",
                "conanfile\\.py$",
                "conan\\.lock$",
                "vcpkg\\.json$",
                "vcpkg\\.lock$",
                "meson\\.build$",
                "meson_options\\.txt$",
                "premake5\\.lua$",
                "premake4\\.lua$",
                "BUILD$",
                "BUILD\\.bazel$",
                "WORKSPACE$",
                "WORKSPACE\\.bazel$",
                "\\.bazelrc$",
                "go\\.mod$",
                "go\\.sum$",
                "Gopkg\\.toml$",
                "Gopkg\\.lock$",
                "glide\\.yaml$",
                "glide\\.lock$",
                "package\\.json$",
                "package-lock\\.json$",
                "yarn\\.lock$",
                "pnpm-lock\\.yaml$",
                "\\.npmrc$",
                "\\.yarnrc$",
                "\\.yarnrc\\.yml$",
                "bower\\.json$",
                "bower_components/",
                "node_modules/",
                "requirements\\.txt$",
                "requirements.*\\.txt$",
                "setup\\.py$",
                "setup\\.cfg$",
                "pyproject\\.toml$",
                "Pipfile$",
                "Pipfile\\.lock$",
                "poetry\\.lock$",
                "conda\\.yml$",
                "environment\\.yml$",
                "conda-requirements\\.txt$",
                "\\.python-version$",
                "pom\\.xml$",
                "\\.mvn/",
                "build\\.gradle$",
                "build\\.gradle\\.kts$",
                "ivy\\.xml$",
                "build\\.sbt$",
                "project/.*\\.scala$",
                "project/.*\\.sbt$",
                "Gemfile$",
                "Gemfile\\.lock$",
                "\\.gemspec$",
                "Rakefile$",
                "Cargo\\.toml$",
                "Cargo\\.lock$",
                "composer\\.json$",
                "composer\\.lock$",
                "\\.csproj$",
                "\\.vbproj$",
                "\\.fsproj$",
                "packages\\.config$",
                "project\\.json$",
                "project\\.lock\\.json$",
                "paket\\.dependencies$",
                "paket\\.lock$",
                "\\.sln$",
                "\\.xproj$",
                "Package\\.swift$",
                "Podfile$",
                "Podfile\\.lock$",
                "\\.podspec$",
                "Cartfile$",
                "Cartfile\\.resolved$",
                "build\\.sbt$",
                "project/.*\\.scala$",
                "\\.cabal$",
                "stack\\.yaml$",
                "cabal\\.project$",
                "mix\\.exs$",
                "mix\\.lock$",
                "rebar\\.config$",
                "rebar\\.lock$",
                "\\.rockspec$",
                "DESCRIPTION$",
                "Project\\.toml$",
                "Manifest\\.toml$",
                "\\.nimble$",
                "dub\\.json$",
                "dub\\.sdl$",
                "opam$",
                "dune-project$",
                "dune-workspace$",
                "project\\.clj$",
                "deps\\.edn$",
                "pubspec\\.yaml$",
                "pubspec\\.lock$",
                "build\\.gradle\\.kts$",
                "build\\.gradle$",
                "mvnw$",
                "mvnw\\.cmd$",
                "\\.mvn/wrapper/",
                "gradlew$",
                "gradlew\\.bat$",
                "gradle/wrapper/",
                "\\.travis\\.yml$",
                "\\.circleci/",
                "\\.github/workflows/"
            ]
        },
        "stage2": {
            "origin_repo_path": "",
            "other_forks_paths": [],
            "enable_classification": False,  # Default: cross-server workflow, do not enable online classification
            "enable_git_apply": False,  # Default: do not run git apply check, run by category after classification
            "deepseek_api_key": os.getenv("DEEPSEEK_API_KEY", "sk-ea182b22c22d403f9d093aaf588c6f98")
        }
    }
    
    # Adjust config by arguments
    # If full check mode enabled, set enable_git_apply=True
    if args.full_check:
        default_config["stage2"]["enable_git_apply"] = True
        print("Full check mode: run all checks (incl. git apply), skip deepseek classification")
    
    # Use default config (all config hardcoded in this file)
    # To change config, edit default_config in this file directly
    config = default_config
    
    # Load input data
    # Support reading from file or dir
    input_data = None
    
    if args.input_dir:
        # Read from dir (streaming, do not load all data at once)
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            print(f"Error: input dir not found: {input_dir}")
            sys.exit(1)
        
        print(f"Reading input from dir: {input_dir}")
        
        # Read commit files in file_count dir order (file_count_0, file_count_1, ...)
        commit_files_ordered = []
        
        # Find all file_count_N dirs
        all_file_count_dirs = sorted(
            [d for d in input_dir.iterdir() if d.is_dir() and d.name.startswith("file_count_")],
            key=lambda x: int(x.name.split("_")[-1]) if x.name.split("_")[-1].isdigit() else 999999
        )
        
        # If file count filter specified, only read that dir
        if args.file_count_filter is not None:
            target_dir_name = f"file_count_{args.file_count_filter}"
            file_count_dirs = [d for d in all_file_count_dirs if d.name == target_dir_name]
            if not file_count_dirs:
                print(f"Warning: dir {target_dir_name} not found, will not process any commit")
            else:
                print(f"Only process commits with {args.file_count_filter} files (from {target_dir_name})")
                # Read files in dir order
                for file_count_dir in file_count_dirs:
                    json_files = list(file_count_dir.glob("*.json"))
                    commit_files_ordered.extend(json_files)
                print(f"Read {len(commit_files_ordered)} commit files from dir")
        elif all_file_count_dirs:
            # Smart dir selection: prefer file_count_1, then later dirs if needed
            commit_limit = args.commit_limit if hasattr(args, 'commit_limit') and args.commit_limit else None
            
            if commit_limit and commit_limit > 0:
                # With count limit, read from dirs as needed
                print(f"Smart dir selection: target {commit_limit} commits, prefer file_count_1")
                
                for file_count_dir in all_file_count_dirs:
                    if len(commit_files_ordered) >= commit_limit:
                        break
                    
                    file_count_num = file_count_dir.name.split("_")[-1]
                    json_files = list(file_count_dir.glob("*.json"))
                    
                    if json_files:
                        # Compute how many more commits needed
                        need_more = commit_limit - len(commit_files_ordered)
                        if need_more > 0:
                            # Take only the needed count
                            files_to_add = json_files[:need_more]
                            commit_files_ordered.extend(files_to_add)
                            print(f"  {file_count_dir.name}: add {len(files_to_add)} commits ({len(json_files)} available)")
                            
                            if len(commit_files_ordered) >= commit_limit:
                                print(f"Collected enough from {file_count_dir.name} and earlier, stop reading")
                                break
                
                print(f"Read {len(commit_files_ordered)} commits (target {commit_limit})")
            else:
                # No count limit, read all dirs
                print(f"Process all file counts (read from all file_count_N dirs)")
                for file_count_dir in all_file_count_dirs:
                    json_files = list(file_count_dir.glob("*.json"))
                    commit_files_ordered.extend(json_files)
                    file_count_num = file_count_dir.name.split("_")[-1]
                    print(f"  {file_count_dir.name}: {len(json_files)} commits")
                
                print(f"Read {len(commit_files_ordered)} commits (sorted by file count dir)")
        else:
            # If no file_count_N dirs, try reading from root dir (backward compat)
            json_files = list(input_dir.glob("*.json"))
            commit_files_ordered = [f for f in json_files if f.name.count("__") >= 2 and not f.name.startswith(".")]
            print(f"Scanned dir: {len(commit_files_ordered)} commit files (old format, unclassified)...")
        
        if not commit_files_ordered:
            print(f"Error: no commit files in input dir: {input_dir}")
            sys.exit(1)
        
        # Use commit file list directly, do not group by origin_repo
        # Do not validate all files here, only validate when processing
        # Build input data structure (commit file list, no grouping)
        families = [{
            "origin_repo": "",  # No origin_repo grouping, leave empty
            "commit_files": commit_files_ordered,  # All commit files (in dir order)
            "metadata": {}
        }]
        
        input_data = {"families": families, "_streaming_mode": True, "_commit_files_mode": True}
        print(f"Prepared {len(commit_files_ordered)} commits, process by commit (no origin_repo grouping)")
        print(f"Streaming mode, load on demand to save memory")
    
    elif args.input:
        # Read from file
        input_file = Path(args.input)
        if not input_file.exists():
            print(f"Error: input file not found: {input_file}")
            sys.exit(1)
        
        print(f"Reading input from file: {input_file}")
        with open(input_file, "r", encoding="utf-8") as f:
            input_data = json.load(f)
    else:
        print("Error: must specify --input or --input-dir")
        sys.exit(1)
    
    # Create pipeline and run
    pipeline = SyncabilityPipeline(Path(args.work_dir), config)
    try:
        # Determine run mode
        if args.mode_commit:
            run_mode = "commit"
        elif args.mode_family:
            run_mode = "family"
        else:
            # Default: family mode (backward compat)
            run_mode = "family"
        
        results = pipeline.run(
            input_data,
            start_stage=args.start_stage,
            end_stage=args.end_stage,
            resume=not args.no_resume,
            family_limit=args.family_limit if run_mode == "family" else None,
            commit_limit=args.commit_limit if run_mode == "commit" else None,
            batch_size=args.batch_size,
            sort_by_file_count=args.sort_by_file_count if run_mode == "commit" else False,
            by_commits=(run_mode == "commit"),
            max_workers=args.max_workers if run_mode == "commit" else None
        )
        print(f"\n✅ Pipeline run completed, processed {len(results)} candidates")
    except KeyboardInterrupt:
        print("\n⚠️ Pipeline run interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Pipeline run failed: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

