#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage 0: Candidate extraction (fork-local changes)

Extract candidate unique commits from fork (in fork only, not in origin).
Only unique commits, not PRs.
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

from syncability_common import CandidateItem


class Stage0CandidateExtraction:
    """Stage 0: Candidate extraction"""

    def __init__(self, work_dir: Path, config: Dict[str, Any]):
        """
        Initialize.

        Args:
            work_dir: Work directory
            config: Config dict
        """
        self.work_dir = Path(work_dir)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.config = config
        
        # Test file patterns
        self.test_file_patterns = config.get("test_file_patterns", [
            r".*test.*",
            r".*spec\.",
            r".*_test\.",
            r".*\.test\.",
            r"test/.*",
            r"tests/.*",
            r".*Test\.",
            r".*Spec\.",
        ])
    
    def extract_candidates(self, input_data: Dict[str, Any]) -> List[CandidateItem]:
        """
        Extract candidates from input (unique commits only, no PRs).

        Args:
            input_data: {"fork_repo", "origin_repo", "commits" or "unique_commits"}

        Returns:
            Candidate list (commits only, no PRs)
        """
        candidates = []
        
        fork_repo = input_data.get("fork_repo")
        origin_repo = input_data.get("origin_repo")
        
        if not fork_repo or not origin_repo:
            raise ValueError("Input must contain fork_repo and origin_repo")
        
        # Prefer unique_commits, else commits
        commits = input_data.get("unique_commits", []) or input_data.get("commits", [])
        
        if not commits:
            # If no commits, try loading from other sources (e.g. forks_with_unique_commits.json, commits_trees_shards)
            commits = self._load_unique_commits_from_data_sources(fork_repo, origin_repo)
        
        for commit in commits:
            commit_id = commit.get("sha") or commit.get("hash") or commit.get("id")
            if not commit_id:
                continue
            
            # Verify unique commit (if marked in data)
            is_unique = commit.get("is_unique", True)
            if not is_unique:
                continue

            # If files empty, parse from diff
            files = commit.get("files", [])
            if not files:
                diff_content = commit.get("diff", "") or commit.get("patch", "")
                if diff_content:
                    files = self._parse_files_from_diff(diff_content)
            
            candidate = CandidateItem(
                fork_repo=fork_repo,
                origin_repo=origin_repo,
                item_type="commit",
                item_id=commit_id,
                raw_data=commit,
                stage0_result={
                    "extracted_at": datetime.now().isoformat(),
                    "commit_message": commit.get("message", ""),
                    "author": commit.get("author", {}),
                    "files_changed": files,
                    "is_unique": is_unique,
                }
            )
            candidates.append(candidate)
        
        # Sort by modified file count (exclude test; prefer non-test)
        candidates = self._sort_candidates_by_file_count(candidates)
        
        return candidates
    
    def _load_unique_commits_from_data_sources(
        self, 
        fork_repo: str, 
        origin_repo: str
    ) -> List[Dict[str, Any]]:
        """
        Load unique commits from data sources (if not in input).
        Can try: commits_trees_shards/, forks_with_unique_commits.json, etc.
        Returns:
            list of unique commits
        """
        commits = []
        
        # If data source path configured, try loading
        commits_shards_dir = self.config.get("commits_shards_dir")
        if commits_shards_dir:
            commits_shards_path = Path(commits_shards_dir)
            if commits_shards_path.exists():
                    # Try loading from shard files
                fork_shard = commits_shards_path / f"{fork_repo.replace('/', '__')}.jsonl"
                origin_shard = commits_shards_path / f"{origin_repo.replace('/', '__')}.jsonl"
                
                if fork_shard.exists() and origin_shard.exists():
                    # Load fork and origin commits
                    fork_commits = self._load_commits_from_shard(fork_shard)
                    origin_commits = self._load_commits_from_shard(origin_shard)
                    
                    # Find unique commits (in fork but not origin)
                    origin_commit_hashes = {c.get("hash") or c.get("sha") for c in origin_commits}
                    for commit in fork_commits:
                        commit_hash = commit.get("hash") or commit.get("sha")
                        if commit_hash and commit_hash not in origin_commit_hashes:
                            commit["is_unique"] = True
                            commits.append(commit)
        
        return commits
    
    def _is_test_file(self, file_path: str) -> bool:
        """
        Check if file is test file.
        Args: file_path
        Returns: bool
        """
        file_path_lower = file_path.lower()
        for pattern in self.test_file_patterns:
            if re.search(pattern, file_path_lower, re.IGNORECASE):
                return True
        return False
    
    def _count_code_files(self, files: List[str]) -> Tuple[int, bool]:
        """
        Count code files (exclude test), check if contains test.
        Args: files (file path list)
        Returns: (code_file_count, has_test_files)
        """
        code_files = []
        has_test_files = False
        
        for file_path in files:
            if self._is_test_file(file_path):
                has_test_files = True
            else:
                code_files.append(file_path)
        
        return len(code_files), has_test_files
    
    def _sort_candidates_by_file_count(self, candidates: List[CandidateItem]) -> List[CandidateItem]:
        """
        Sort candidates by file count (exclude test; same count: prefer with test).
        Args: candidates
        Returns: sorted list
        """
        def get_sort_key(candidate: CandidateItem) -> Tuple[int, bool]:
            # Get file list
            files = []
            if candidate.item_type == "commit":
                files = candidate.raw_data.get("files", [])
            elif candidate.item_type == "pr":
                files = candidate.raw_data.get("files", [])
            
            # Count code files and has test
            code_file_count, has_test_files = self._count_code_files(files)
            
            # Sort key: (code file count, has test)
            return (code_file_count, not has_test_files)
        
        # Sort: by file count asc, then same count prefer with test
        sorted_candidates = sorted(candidates, key=get_sort_key)
        
        return sorted_candidates
    
    def _parse_files_from_diff(self, diff_content: str) -> List[str]:
        """
        Parse modified files from diff. Supports diff --git and diff --cc.
        Args: diff_content
        Returns: list of file paths
        """
        if not diff_content:
            return []
        
        files = []
        seen_files = set()
        
        for line in diff_content.split('\n'):
            # Match diff --git format
            match_git = re.match(r'^diff --git\s+a/(.+?)\s+b/(.+?)(?:\s|$)', line)
            if match_git:
                file_path = match_git.group(2)
                if file_path not in seen_files:
                    files.append(file_path)
                    seen_files.add(file_path)
                continue
            
            # Match diff --cc (merge commit)
            match_cc = re.match(r'^diff --cc\s+(.+?)(?:\s|$)', line)
            if match_cc:
                file_path = match_cc.group(1)
                if file_path not in seen_files:
                    files.append(file_path)
                    seen_files.add(file_path)
                continue
        
        return files
    
    def _load_commits_from_shard(self, shard_path: Path) -> List[Dict[str, Any]]:
        """Load commits from shard files"""
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
        except Exception:
            pass
        
        return commits
    
    def process(self, candidates: List[CandidateItem]) -> List[CandidateItem]:
        """
        Process candidates (Stage 0: extract and verify unique commits).
        Returns: candidates passing Stage 0 (unique commits only)
        """
        # Stage 0: extract, verify all are unique commits
        passed_candidates = []
        
        for candidate in candidates:
            # Ensure commit type (not PR)
            if candidate.item_type != "commit":
                continue

            # Verify unique commit
            is_unique = candidate.stage0_result.get("is_unique", True) if candidate.stage0_result else True
            if not is_unique:
                continue

            if not candidate.stage0_result:
                # Create basic stage0_result if missing
                candidate.stage0_result = {
                    "extracted_at": datetime.now().isoformat(),
                    "validated": True,
                    "is_unique": True,
                }
            
            passed_candidates.append(candidate)
        
        # Sort by file count (exclude test; prefer non-test)
        passed_candidates = self._sort_candidates_by_file_count(passed_candidates)
        
        return passed_candidates

