#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage 2: Technical portability (C1)

Assess if changes can be technically ported to origin:
1. Do modified file paths exist (check origin and other forks)
2. Diff + context match in target latest (check origin and other forks)
3. git apply --check --3way to test applicability (check origin and other forks)

Compare against origin and other forks under same origin.
If file or diff context exists in origin or any fork -> consider present/matched.
If git apply succeeds in origin or any fork -> consider applicable.
"""

import os
import re
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from syncability_common import CandidateItem
from syncability_classify import classify_candidate

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
    import os
    # Two path formats
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
    
    # Method 2: cloned_repos dir (default)
    cloned_repos_path = Path("cloned_repos") / repo_path_underscore
    if cloned_repos_path.exists() and (cloned_repos_path / ".git").exists():
        return cloned_repos_path
    # Try standard format
    cloned_repos_path = Path("cloned_repos") / repo_path_standard
    if cloned_repos_path.exists() and (cloned_repos_path / ".git").exists():
        return cloned_repos_path
    
    return None


class Stage2TechnicalPortability:
    """Stage 2: Technical portability"""

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
        
        self.min_score = config.get("min_score", 0.5)
        self.context_lines = config.get("context_lines", 3)
    
    
    def parse_diff(self, diff_content: str) -> List[Dict[str, Any]]:
        """
        Parse diff, extract hunks per file
        
        Returns:
            [{"file": "path/to/file", "hunks": [{"old_start": 10, "old_lines": 5, "context_before": [...], "context_after": [...], "old_lines_content": [...], "new_lines_content": [...]}, ...]}, ...]
        """
        if not diff_content:
            return []
        
        files = []
        current_file = None
        current_hunk = None
        in_hunk = False
        
        lines = diff_content.split('\n')
        i = 0
        
        while i < len(lines):
            line = lines[i]
            
            # File header: diff --git or diff --cc
            if line.startswith('diff --git') or line.startswith('diff --cc'):
                # Save previous file
                if current_file and current_hunk:
                    current_file['hunks'].append(current_hunk)
                    current_hunk = None
                if current_file:
                    files.append(current_file)
                
                # Extract file path
                file_path = None
                if line.startswith('diff --git'):
                    # diff --git a/src/file.c b/src/file.c
                    match = re.match(r'diff --git a/(.+?) b/(.+?)(?:\s|$)', line)
                    if match:
                        file_path = match.group(2)  # Path after b/
                elif line.startswith('diff --cc'):
                    # diff --cc (merge)
                    match = re.match(r'diff --cc\s+(.+?)(?:\s|$)', line)
                    if match:
                        file_path = match.group(1)
                
                if file_path:
                    current_file = {
                        "file": file_path,
                        "hunks": []
                    }
                    in_hunk = False
                i += 1
                continue
            
            # index line
            if line.startswith('index '):
                i += 1
                continue
            
            # File path lines
            if line.startswith('---') or line.startswith('+++'):
                i += 1
                continue
            
            # Hunk header (merge)
            if line.startswith('@@'):
                # Save previous hunk
                if current_hunk and current_file:
                    current_file['hunks'].append(current_hunk)
                
                # Parse hunk header
                # Standard hunk format
                # Merge hunk format
                # Try merge format first
                match_merge = re.match(r'@@@ -(\d+)(?:,(\d+))?\s+-(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@@', line)
                if match_merge:
                    # Merge: use first old/new
                    old_start = int(match_merge.group(1))
                    old_lines = int(match_merge.group(2)) if match_merge.group(2) else 1
                    new_start = int(match_merge.group(5))
                    new_lines = int(match_merge.group(6)) if match_merge.group(6) else 1
                else:
                    # Standard format
                    match = re.match(r'@@ -(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@', line)
                    if match:
                        old_start = int(match.group(1))
                        old_lines = int(match.group(2)) if match.group(2) else 1
                        new_start = int(match.group(3))
                        new_lines = int(match.group(4)) if match.group(4) else 1
                    else:
                        # Skip if no match
                        i += 1
                        continue
                
                # Extract function name if any
                function_name = ""
                if '@@' in line:
                    parts = line.split('@@')
                    if len(parts) > 2:
                        function_name = parts[-1].strip()
                
                current_hunk = {
                    "old_start": old_start,
                    "old_lines": old_lines,
                    "new_start": new_start,
                    "new_lines": new_lines,
                    "function_name": function_name,
                    "context_before": [],
                    "context_after": [],
                    "old_lines_content": [],
                    "new_lines_content": []
                }
                in_hunk = True
                i += 1
                continue
            
            # Hunk content
            if in_hunk and current_hunk:
                # Handle diff --git and diff --cc
                # Context/add/delete lines
                # diff --cc may have conflict markers
                if line.startswith(' ') and not line.startswith(' +'):
                    # Context line (in both old and new)
                    context_line = line[1:]  # Strip leading space
                    
                    # Determine context line position:
                    # 1. Before delete/add: context-before
                    # 2. Between delete and add: context
                    #    Treat as old file for matching
                    #    Add to old_lines_content
                    # 3. After add: check for more
                    #    If no more: context-after; else: between
                    
                    if len(current_hunk['old_lines_content']) == 0 and len(current_hunk['new_lines_content']) == 0:
                        # Case 1: context-before
                        current_hunk['context_before'].append(context_line)
                    elif len(current_hunk['old_lines_content']) > 0 and len(current_hunk['new_lines_content']) == 0:
                        # Case 2: context between
                        # Classify as old file
                        # Add to old_lines_content for matching
                        current_hunk['old_lines_content'].append(context_line)
                    elif len(current_hunk['new_lines_content']) > 0:
                        # Case 3: check for more
                        # Check next 10 lines for more
                        has_more_changes = False
                        for j in range(i + 1, min(i + 10, len(lines))):
                            next_line = lines[j]
                            # New hunk/file: current ended
                            if next_line.startswith('@@') or next_line.startswith('diff --git') or next_line.startswith('diff --cc'):
                                break
                            # Delete/add: more to come
                            if next_line.startswith('-') and not next_line.startswith('---'):
                                has_more_changes = True
                                break
                            elif next_line.startswith('+') and not next_line.startswith('+++'):
                                has_more_changes = True
                                break
                        
                        if has_more_changes:
                            # More edits: context between
                            # Add to old_lines_content
                            current_hunk['old_lines_content'].append(context_line)
                        else:
                            # No more: context-after
                            current_hunk['context_after'].append(context_line)
                    else:
                        # Should not happen
                        current_hunk['context_after'].append(context_line)
                elif line.startswith('-') and not line.startswith('---'):
                    # Deleted lines (old only)
                    # Exclude --- path lines
                    # diff --cc: - means deleted in first parent
                    # Check next for + (conflict)
                    if i + 1 < len(lines) and lines[i + 1].startswith('+') and not lines[i + 1].startswith('+++'):
                        # Conflict: record both - and +
                        # Record - first
                        current_hunk['old_lines_content'].append(line[1:])
                        # Next line handles +
                    else:
                        # Single delete line
                        current_hunk['old_lines_content'].append(line[1:])
                elif line.startswith('+') and not line.startswith('+++'):
                    # Added lines (new only)
                    # Exclude +++ path lines
                    # diff --cc: + means added in second parent
                    # Check prev for - (conflict)
                    if i > 0 and lines[i - 1].startswith('-') and not lines[i - 1].startswith('---'):
                        # Conflict: prev -, this +
                        # Prev recorded -, record +
                        current_hunk['new_lines_content'].append(line[1:])
                    else:
                        # Single add line
                        current_hunk['new_lines_content'].append(line[1:])
                elif not line.strip():
                    # Empty line in diff --cc
                    # Ignore empty in hunk
                    # Empty after add: maybe new file empty
                    # Simplify: ignore empty
                    pass
                elif line.startswith('\\'):
                    # Diff end marker
                    pass
            
            i += 1
        
        # Save last file
        if current_file:
            if current_hunk:
                current_file['hunks'].append(current_hunk)
            files.append(current_file)
        
        return files
    
    def parse_files_from_diff(self, diff_content: str) -> List[str]:
        """
        Parse modified files from diff
        Support diff --git and diff --cc
        
        Args:
            diff_content: diff content
            
        Returns:
            file path list
        """
        if not diff_content:
            return []
        
        files = []
        seen_files = set()
        
        for line in diff_content.split('\n'):
            # Match diff --git format
            match_git = re.match(r'^diff --git\s+a/(.+?)\s+b/(.+?)(?:\s|$)', line)
            if match_git:
                file_path = match_git.group(2)  # Path after b/
                if file_path not in seen_files:
                    files.append(file_path)
                    seen_files.add(file_path)
                continue
            
            # Match diff --cc (merge)
            match_cc = re.match(r'^diff --cc\s+(.+?)(?:\s|$)', line)
            if match_cc:
                file_path = match_cc.group(1)
                if file_path not in seen_files:
                    files.append(file_path)
                    seen_files.add(file_path)
                continue
        
        return files
    
    def _convert_cc_diff_to_git_diff(self, cc_diff: str) -> str:
        """
        Convert diff --cc to diff --git
        
        Args:
            cc_diff: diff --cc content
            
        Returns:
            Converted diff --git or None
        """
        if not cc_diff:
            return None
        
        lines = cc_diff.split('\n')
        output_lines = []
        i = 0
        current_file = None
        current_hunk_start = None
        hunk_content_lines = []
        
        def finalize_hunk():
            """Finish hunk, recalc and write header"""
            nonlocal current_hunk_start, hunk_content_lines, output_lines
            
            if current_hunk_start is None or not hunk_content_lines:
                return
            
            # Parse original hunk header
            hunk_header_line = current_hunk_start
            match = re.match(r'@@@ -(\d+)(?:,(\d+))?\s+-(\d+)(?:,(\d+))?\s+\+(\d+)(?:,(\d+))?\s+@@@', hunk_header_line)
            if not match:
                return
            
            old_start = int(match.group(1))
            old_lines_orig = int(match.group(2)) if match.group(2) else 1
            new_start = int(match.group(5))
            new_lines_orig = int(match.group(6)) if match.group(6) else 1
            
            # Extract function name if any
            function_name = ""
            if '@@@' in hunk_header_line:
                parts = hunk_header_line.split('@@@')
                if len(parts) > 2:
                    function_name = parts[-1].strip()
            
            # Count actual lines
            actual_old_lines = 0
            actual_new_lines = 0
            actual_context = 0
            
            for content_line in hunk_content_lines:
                if content_line.startswith(' '):
                    actual_context += 1
                elif content_line.startswith('-'):
                    actual_old_lines += 1
                elif content_line.startswith('+'):
                    actual_new_lines += 1
            
            # Recalc old_lines and new_lines
            # Conflict: kept + only, adjust
            adjusted_old_lines = actual_old_lines
            adjusted_new_lines = actual_new_lines
            
            # Add only: old_lines=1
            # Delete only: new_lines=1
            if adjusted_old_lines == 0 and adjusted_new_lines > 0:
                # Add only (conflict case)
                adjusted_old_lines = 1  # Min 1 for git apply
            elif adjusted_old_lines > 0 and adjusted_new_lines == 0:
                # Delete only
                adjusted_new_lines = 1  # Min 1 for git apply
            elif adjusted_old_lines == 0 and adjusted_new_lines == 0:
                # Context only (safety)
                adjusted_old_lines = 1
                adjusted_new_lines = 1
            
            # Write adjusted hunk header
            if function_name:
                output_lines.append(f'@@ -{old_start},{adjusted_old_lines} +{new_start},{adjusted_new_lines} @@ {function_name}')
            else:
                output_lines.append(f'@@ -{old_start},{adjusted_old_lines} +{new_start},{adjusted_new_lines} @@')
            
            # Write hunk content
            output_lines.extend(hunk_content_lines)
            
            # Reset
            current_hunk_start = None
            hunk_content_lines = []
        
        while i < len(lines):
            line = lines[i]
            
            # File header conversion
            if line.startswith('diff --cc'):
                # Finish prev hunk if any
                finalize_hunk()
                
                # Extract file path
                match = re.match(r'diff --cc\s+(.+?)(?:\s|$)', line)
                if match:
                    file_path = match.group(1)
                    output_lines.append(f'diff --git a/{file_path} b/{file_path}')
                    current_file = file_path
                else:
                    output_lines.append(line)
                i += 1
                continue
            
            # index line: keep or convert
            if line.startswith('index '):
                # diff --cc index format
                # Convert to standard index
                match = re.match(r'index\s+([a-f0-9]+),([a-f0-9]+)\.\.([a-f0-9]+)', line)
                if match:
                    # Use 2nd as old, 3rd as new
                    output_lines.append(f'index {match.group(2)}..{match.group(3)}')
                else:
                    output_lines.append(line)
                i += 1
                continue
            
            # File path lines
            if line.startswith('---') or line.startswith('+++'):
                # diff --cc may need complement
                if line.startswith('---'):
                    if current_file:
                        output_lines.append(f'--- a/{current_file}')
                    else:
                        output_lines.append(line)
                elif line.startswith('+++'):
                    if current_file:
                        output_lines.append(f'+++ b/{current_file}')
                    else:
                        output_lines.append(line)
                i += 1
                continue
            
            # Merge hunk header
            # Finish prev hunk if any
            if line.startswith('@@@'):
                finalize_hunk()
                
                # Save new hunk header for later
                current_hunk_start = line
                i += 1
                continue
            
            # Content: handle diff --cc markers
            # In diff --cc:
            #   ' ' = in both parents (context)
            #   '-' = deleted in first parent
            #   '+' = added in second parent
            #   ' +' = add line not context
            # Convert to unified diff
            if line.startswith(' ') or line.startswith('-') or line.startswith('+'):
                # Simplify for diff --cc:
                # Space: keep as context
                # ' +' -> '+'
                # '-': convert to -
                # '+': convert to +
                # Conflict: keep +
                
                # Check for ' +' (add)
                if line.startswith(' +') and len(line) > 2:
                    # ' +' -> '+'
                    hunk_content_lines.append('+' + line[2:])
                    i += 1
                    continue
                
                # Check for conflict marker
                if line.startswith('-') and i + 1 < len(lines) and lines[i + 1].startswith('+') and not lines[i + 1].startswith('+++'):
                    # Conflict: keep + only
                    # Skip -, keep +
                    i += 1
                    if i < len(lines) and lines[i].startswith('+'):
                        hunk_content_lines.append(lines[i])  # Keep + line only
                elif line.startswith(' '):
                    # Context: keep as is
                    hunk_content_lines.append(line)
                elif line.startswith('-'):
                    # Delete: keep as is
                    hunk_content_lines.append(line)
                elif line.startswith('+'):
                    # Add: keep as is
                    hunk_content_lines.append(line)
                else:
                    hunk_content_lines.append(line)
                i += 1
                continue
            
            # Other: output or add
            if current_hunk_start is not None:
                # In hunk: maybe empty
                hunk_content_lines.append(line)
            else:
                # Not in hunk: output
                output_lines.append(line)
            i += 1
        
        # Finish last hunk
        finalize_hunk()
        
        # Ensure trailing newline for git apply
        result = '\n'.join(output_lines)
        if result and not result.endswith('\n'):
            result += '\n'
        
        return result
    
    def check_diff_context_in_repos(
        self, 
        diff_content: str, 
        origin_repo_path: Path,
        other_forks_paths: List[Path] = None
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Check if diff context exists in origin/forks latest
        
        Args:
            diff_content: diff content
            origin_repo_path
            other_forks_paths
        
        Returns:
            (score, details)
        """
        if not diff_content:
            return 0.0, {"reason": "Missing diff"}
        
        # Collect repo paths to check
        # Ensure Path objects, normalized
        # Don't check path exists (may be on server)
        # git apply pass means path correct
        repos_to_check = []
        if origin_repo_path:
            # Ensure Path, normalize
            if isinstance(origin_repo_path, str):
                origin_repo_path = Path(origin_repo_path)
            origin_repo_path = Path(str(origin_repo_path))
            repos_to_check.append(("origin", origin_repo_path))
        
        if other_forks_paths:
            for fork_path in other_forks_paths:
                if fork_path:
                    # Ensure Path, normalize
                    if isinstance(fork_path, str):
                        fork_path = Path(fork_path)
                    fork_path = Path(str(fork_path))
                    repos_to_check.append(("fork", fork_path))
        
        if not repos_to_check:
            return 0.0, {"reason": "No repo paths (origin or forks)"}
        
        parsed_diffs = self.parse_diff(diff_content)
        if not parsed_diffs:
            return 0.0, {"reason": "Cannot parse diff"}
        
        total_hunks = 0
        matched_hunks = 0
        file_results = []
        
        for file_diff in parsed_diffs:
            file_path = file_diff["file"]
            file_total = len(file_diff["hunks"])
            
            # Check file exists (in at least one repo)
            file_exists_in = []
            for repo_type, repo_path in repos_to_check:
                full_path = repo_path / file_path
                if full_path.exists():
                    # Record repo type and path
                    repo_name = repo_path.name if repo_path else str(repo_path)
                    file_exists_in.append({
                        "type": repo_type,
                        "path": str(repo_path),
                        "name": repo_name
                    })
            
            # Record match per hunk
            hunk_matches = []  # Match info per hunk
            
            # Check each hunk matches in any repo
            for hunk_idx, hunk in enumerate(file_diff["hunks"], 1):
                total_hunks += 1
                
                # Check context match
                context_before = hunk.get("context_before", [])
                context_after = hunk.get("context_after", [])
                old_lines = hunk.get("old_lines_content", [])
                
                # Build search: context_before + old + context_after
                search_pattern_lines = context_before + old_lines + context_after
                
                # Debug: log hunk details
                if not search_pattern_lines:
                    # No context: cannot match
                    hunk_matches.append({"matched": False, "matched_in": []})
                    continue
                
                # Find match in all repos
                hunk_matched = False
                matched_in = []
                
                for repo_type, repo_path in repos_to_check:
                    full_path = repo_path / file_path
                    
                    if not full_path.exists():
                        continue  # File missing, try next repo
                    
                    # Read file content
                    try:
                        with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                            repo_content = f.read()
                        repo_lines = repo_content.split('\n')
                    except Exception:
                        continue  # Read failed, try next
                    
                    # Find match in repo file
                    found = False
                    
                    # Context only: use precise match
                    if not old_lines and (context_before or context_after):
                        # Match context only (new file)
                        search_pattern = context_before + context_after
                        if search_pattern:
                            for i in range(len(repo_lines) - len(search_pattern) + 1):
                                window = repo_lines[i:i + len(search_pattern)]
                                if all(w.strip() == p.strip() for w, p in zip(window, search_pattern)):
                                    found = True
                                    break
                    else:
                        # With old: match context+old+context
                        search_pattern_lines = context_before + old_lines + context_after
                        
                        for i in range(len(repo_lines) - len(search_pattern_lines) + 1):
                            window = repo_lines[i:i + len(search_pattern_lines)]
                            
                            # Check match (context first, partial old ok)
                            match_score = 0
                            total_checks = 0
                            
                            # Check context_before (exact)
                            context_before_match = True
                            if context_before:
                                for j, ctx_line in enumerate(context_before):
                                    if j < len(window):
                                        if window[j].strip() == ctx_line.strip():
                                            match_score += 1
                                        else:
                                            context_before_match = False
                                    else:
                                        context_before_match = False
                                    total_checks += 1
                            
                            # Check old (partial ok, some match)
                            ctx_before_len = len(context_before)
                            old_match_count = 0
                            if old_lines:
                                for j, old_line in enumerate(old_lines):
                                    idx = ctx_before_len + j
                                    if idx < len(window):
                                        if window[idx].strip() == old_line.strip():
                                            old_match_count += 1
                                    total_checks += 1
                                # Old: at least 50% match
                                if old_match_count >= len(old_lines) * 0.5:
                                    match_score += old_match_count
                                else:
                                    match_score += old_match_count * 0.5  # Partial: half score
                            
                            # Check context_after (exact)
                            ctx_and_old_len = len(context_before) + len(old_lines)
                            context_after_match = True
                            if context_after:
                                for j, ctx_line in enumerate(context_after):
                                    idx = ctx_and_old_len + j
                                    if idx < len(window):
                                        if window[idx].strip() == ctx_line.strip():
                                            match_score += 1
                                        else:
                                            context_after_match = False
                                    else:
                                        context_after_match = False
                                    total_checks += 1
                            
                            # If match >= 70%: consider matched
                            # Context both match: ok even if old partial
                            context_match = (
                                (not context_before or all(w.strip() == p.strip() 
                                    for w, p in zip(window[:len(context_before)], context_before))) and
                                (not context_after or all(w.strip() == p.strip() 
                                    for w, p in zip(window[ctx_and_old_len:ctx_and_old_len+len(context_after)], context_after)))
                            )
                            
                            if context_match and total_checks > 0:
                                found = True
                                break
                            elif total_checks > 0 and match_score / total_checks >= 0.7:
                                found = True
                                break
                    
                    if found:
                        hunk_matched = True
                        # Record repo type and path
                        repo_name = repo_path.name if repo_path else str(repo_path)
                        matched_in.append({
                            "type": repo_type,
                            "path": str(repo_path),
                            "name": repo_name
                        })
                        # Found: stop (avoid duplicate)
                        break
                
                # Record hunk match result
                hunk_matches.append({
                    "matched": hunk_matched,
                    "matched_in": matched_in
                })
                
                if hunk_matched:
                    matched_hunks += 1
            
            # Aggregate file-level match
            file_matched = sum(1 for h in hunk_matches if h["matched"])
            all_matched_in = []
            for h in hunk_matches:
                all_matched_in.extend(h["matched_in"])
            
            # Dedup matched_in
            unique_matched_in = []
            seen_repos = set()
            for repo_info in all_matched_in:
                if isinstance(repo_info, dict):
                    repo_key = (repo_info.get("type"), repo_info.get("path"))
                else:
                    # Compat old format (str)
                    repo_key = (repo_info, None)
                if repo_key not in seen_repos:
                    seen_repos.add(repo_key)
                    unique_matched_in.append(repo_info if isinstance(repo_info, dict) else {"type": repo_info})
            
            file_results.append({
                "file": file_path,
                "exists_in": file_exists_in,
                "exists": len(file_exists_in) > 0,
                "matched_hunks": file_matched,
                "total_hunks": file_total,
                "matched_in_repos": unique_matched_in  # Record which repos matched
            })
        
        # Compute score
        if total_hunks == 0:
            score = 0.0
        else:
            score = matched_hunks / total_hunks
        
        # Score per repo (all hunks match)
        repo_scores = {}
        for repo_type, repo_path in repos_to_check:
            repo_name = repo_path.name if repo_path else str(repo_path)
            repo_key = f"{repo_type}:{repo_name}"
            
            # Count matched hunks in repo
            repo_matched_hunks = 0
            repo_total_hunks = 0
            
            for file_result in file_results:
                file_path = file_result["file"]
                full_path = repo_path / file_path
                
                if not full_path.exists():
                    continue
                
                # Check each hunk matches in repo
                file_total = file_result.get("total_hunks", 0)
                file_matched = file_result.get("matched_hunks", 0)
                matched_in_repos = file_result.get("matched_in_repos", [])
                
                # Check repo in matched_in_repos
                repo_in_matched = False
                for repo_info in matched_in_repos:
                    if isinstance(repo_info, dict):
                        if repo_info.get("path") == str(repo_path) or repo_info.get("name") == repo_name:
                            repo_in_matched = True
                            break
                    elif repo_info == repo_name or repo_info == str(repo_path):
                        repo_in_matched = True
                        break
                
                # If in list: count matched hunks
                # Simplify: file match -> all hunks match
                if repo_in_matched:
                    repo_total_hunks += file_total
                    repo_matched_hunks += file_matched
                else:
                    # File exists but not matched: still count
                    repo_total_hunks += file_total
            
            repo_score = repo_matched_hunks / repo_total_hunks if repo_total_hunks > 0 else 0.0
            repo_scores[repo_key] = {
                "repo_type": repo_type,
                "repo_path": str(repo_path),
                "repo_name": repo_name,
                "score": repo_score,
                "matched_hunks": repo_matched_hunks,
                "total_hunks": repo_total_hunks,
                "all_matched": repo_score >= 1.0 if repo_total_hunks > 0 else False
            }
        
        return score, {
            "total_hunks": total_hunks,
            "matched_hunks": matched_hunks,
            "file_results": file_results,
            "repo_scores": repo_scores  # Detailed score per repo
        }
    
    def check_file_existence(
        self, 
        files: List[str], 
        origin_repo_path: Path,
        other_forks_paths: List[Path] = None
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Check files exist in origin/forks
        
        All code files must exist else 0
        
        Args:
            files: file path list
            origin_repo_path
            other_forks_paths: other fork paths
        
        Returns:
            (score, details)
        """
        if not files:
            return 0.0, {"missing_files": [], "file_status": {}, "all_exist": False, "reason": "No files to check (empty list)"}
        
        # Collect repo paths to check
        # Ensure Path objects, normalized
        # Don't check path exists (may be on server)
        # git apply pass means path correct
        repos_to_check = []
        if origin_repo_path:
            # Ensure Path, normalize
            if isinstance(origin_repo_path, str):
                origin_repo_path = Path(origin_repo_path)
            origin_repo_path = Path(str(origin_repo_path))            
            repos_to_check.append(("origin", origin_repo_path))
        
        if other_forks_paths:
            for fork_path in other_forks_paths:
                if fork_path:
                    # Ensure Path, normalize
                    if isinstance(fork_path, str):
                        fork_path = Path(fork_path)
                    fork_path = Path(str(fork_path))                    
                    repos_to_check.append(("fork", fork_path))
        
        if not repos_to_check:
            return 0.0, {
                "missing_files": files,
                "file_status": {f: {"exists": False, "exists_in": []} for f in files},
                "all_exist": False,
                "reason": "No repo paths"
            }
        
        missing_files = []
        file_status = {}  # Record file status per repo
        
        for file_path in files:
            exists_in = []
            for repo_type, repo_path in repos_to_check:
                full_path = repo_path / file_path
                if full_path.exists():
                    # Record repo type and path
                    repo_name = repo_path.name if repo_path else str(repo_path)
                    exists_in.append({
                        "type": repo_type,
                        "path": str(repo_path),
                        "name": repo_name
                    })
            
            if exists_in:
                file_status[file_path] = {
                    "exists": True,
                    "exists_in": exists_in
                }
            else:
                missing_files.append(file_path)
                file_status[file_path] = {
                    "exists": False,
                    "exists_in": []
                }
        
        # All must exist else 0
        all_exist = len(missing_files) == 0
        score = 1.0 if all_exist else 0.0
        
        # Score per repo (all files exist)
        repo_scores = {}
        for repo_type, repo_path in repos_to_check:
            repo_name = repo_path.name if repo_path else str(repo_path)
            repo_key = f"{repo_type}:{repo_name}"
            
            # Check all files exist in repo
            repo_all_exist = True
            repo_missing_files = []
            for file_path in files:
                full_path = repo_path / file_path
                if not full_path.exists():
                    repo_all_exist = False
                    repo_missing_files.append(file_path)
            
            repo_scores[repo_key] = {
                "repo_type": repo_type,
                "repo_path": str(repo_path),
                "repo_name": repo_name,
                "score": 1.0 if repo_all_exist else 0.0,
                "all_exist": repo_all_exist,
                "missing_files": repo_missing_files,
                "total_files": len(files),
                "existing_files": len(files) - len(repo_missing_files)
            }
        
        return score, {
            "missing_files": missing_files,
            "file_status": file_status,
            "all_exist": all_exist,
            "total_files": len(files),
            "existing_files": len(files) - len(missing_files),
            "repo_scores": repo_scores  # Detailed score per repo
        }
    
    def check_git_apply(
        self,
        diff_content: str,
        origin_repo_path: Path,
        other_forks_paths: List[Path] = None
    ) -> Tuple[float, Dict[str, Any]]:
        """
        Test git apply on origin/forks
        
        Args:
            diff_content: diff content
            origin_repo_path
            other_forks_paths: other fork paths
        
        Returns:
            (score, details)
        """
        if not diff_content or not diff_content.strip():
            return 0.0, {"reason": "Missing or empty diff"}
        
        # Check diff format valid
        diff_lines = diff_content.split('\n')
        has_diff_header = any(
            line.startswith('diff --git') or 
            line.startswith('diff --cc') or
            line.startswith('---') or 
            line.startswith('+++') or
            line.startswith('@@')
            for line in diff_lines[:20]  # Check first 20 lines
        )
        
        if not has_diff_header:
            return 0.0, {"reason": "Invalid diff (no header)"}
        
        # Collect repo paths to check
        # Ensure Path objects, normalized
        # Don't check path exists (may be on server)
        # git apply pass means path correct
        repos_to_check = []
        if origin_repo_path:
            # Ensure Path, normalize
            if isinstance(origin_repo_path, str):
                origin_repo_path = Path(origin_repo_path)
            origin_repo_path = Path(str(origin_repo_path))            
            repos_to_check.append(("origin", origin_repo_path))
        
        if other_forks_paths:
            for fork_path in other_forks_paths:
                if fork_path:
                    # Ensure Path, normalize
                    if isinstance(fork_path, str):
                        fork_path = Path(fork_path)
                    fork_path = Path(str(fork_path))                    
                    repos_to_check.append(("fork", fork_path))
        
        if not repos_to_check:
            return 0.0, {"reason": "No repo paths (origin or forks)"}
        
        # git apply does not support diff --cc
        # If diff --cc: convert to diff --git
        has_cc_format = any(
            line.startswith('diff --cc')
            for line in diff_lines[:50]  # Check first 50 lines
        )
        
        if has_cc_format:
            # Try convert diff --cc to diff --git
            converted_diff = self._convert_cc_diff_to_git_diff(diff_content)
            if converted_diff:
                diff_content = converted_diff
            else:
                # Convert failed
                apply_results = []
                for repo_type, repo_path in repos_to_check:
                    apply_results.append({
                        "repo_type": repo_type,
                        "repo_path": str(repo_path),
                        "applied": False,
                        "reason": "Cannot convert diff --cc"
                    })
                
                return 0.0, {
                    "reason": "Cannot convert diff --cc",
                    "total_repos": len(repos_to_check),
                    "total_attempts": 0,
                    "successful_applies": 0,
                    "apply_results": apply_results,
                    "diff_valid": True,
                    "is_merge_commit": True
                }
        
        # Write diff to temp file
        # Use newline='' to keep line endings
        # Ensure trailing newline for git apply
        # Linux: keep \n
        tmp_patch_path = None
        try:
            # Ensure trailing newline
            normalized_diff = diff_content.rstrip('\r\n') + '\n'  # Strip line endings, add \n
            
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.patch', delete=False) as tmp_file:
                # Write binary to keep line endings
                tmp_file.write(normalized_diff.encode('utf-8'))
                tmp_patch_path = tmp_file.name
            
            apply_results = []
            successful_applies = 0
            total_attempts = 0
            
            for repo_type, repo_path in repos_to_check:
                # Check valid git repo
                if not (repo_path / ".git").exists():
                    apply_results.append({
                        "repo_type": repo_type,
                        "repo_path": str(repo_path),
                        "applied": False,
                        "reason": "Not a valid git repo"
                    })
                    continue
                
                # Check git available
                try:
                    git_check = subprocess.run(
                        ["git", "--version"],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        timeout=5,
                        encoding='utf-8',
                        errors='ignore'
                    )
                    if git_check.returncode != 0:
                        apply_results.append({
                            "repo_type": repo_type,
                            "repo_path": str(repo_path),
                            "applied": False,
                            "reason": "git not available"
                        })
                        continue
                except Exception:
                    apply_results.append({
                        "repo_type": repo_type,
                        "repo_path": str(repo_path),
                        "applied": False,
                        "reason": "Cannot run git"
                    })
                    continue
                
                total_attempts += 1
                applied = False
                apply_reason = ""
                
                # Try multiple git apply strategies
                apply_strategies = [
                    (["git", "apply", "--check"], "standard"),
                    (["git", "apply", "--check", "--ignore-whitespace"], "ignore whitespace"),
                    (["git", "apply", "--check", "--3way"], "3-way merge"),
                ]
                
                for strategy_cmd, strategy_name in apply_strategies:
                    try:
                        result = subprocess.run(
                            strategy_cmd + [tmp_patch_path],
                            cwd=repo_path,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            encoding='utf-8',
                            errors='ignore'
                        )
                        
                        if result.returncode == 0:
                            # Can apply
                            applied = True
                            apply_reason = f"Can apply (using {strategy_name})"
                            successful_applies += 1
                            break
                        else:
                            # Record error (first strategy)
                            if not apply_reason:
                                error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown error"
                                apply_reason = f"Cannot apply ({strategy_name}): {error_msg[:200]}"
                    
                    except Exception as e:
                        if not apply_reason:
                            apply_reason = f"Error running git apply ({strategy_name}): {str(e)[:200]}"
                
                apply_results.append({
                    "repo_type": repo_type,
                    "repo_path": str(repo_path),
                    "applied": applied,
                    "reason": apply_reason or "All strategies failed"
                })
            
            # Score: apply in at least one repo
            # All failed: return 0.0
            score = 1.0 if successful_applies > 0 else 0.0
            
            # Build repo scores from apply_results
            repo_scores = {}
            for apply_result in apply_results:
                repo_type = apply_result.get("repo_type", "unknown")
                repo_path = apply_result.get("repo_path", "")
                repo_name = Path(repo_path).name if repo_path else repo_path
                repo_key = f"{repo_type}:{repo_name}"
                
                repo_scores[repo_key] = {
                    "repo_type": repo_type,
                    "repo_path": repo_path,
                    "repo_name": repo_name,
                    "score": 1.0 if apply_result.get("applied", False) else 0.0,
                    "applied": apply_result.get("applied", False),
                    "reason": apply_result.get("reason", "")
                }
            
            return score, {
                "total_repos": len(repos_to_check),
                "total_attempts": total_attempts,
                "successful_applies": successful_applies,
                "apply_results": apply_results,
                "diff_valid": has_diff_header,
                "repo_scores": repo_scores  # Detailed score per repo
            }
        
        except Exception as e:
            return 0.0, {
                "reason": f"Exception during git apply: {str(e)[:200]}",
                "total_repos": len(repos_to_check) if repos_to_check else 0
            }
        
        finally:
            # Cleanup temp file
            if tmp_patch_path:
                try:
                    os.unlink(tmp_patch_path)
                except Exception:
                    pass
    
    
    def process(self, candidates: List[CandidateItem]) -> List[CandidateItem]:
        """
        Process candidates, compute technical portability
        
        Args:
            candidates
        
        Returns:
            Candidates passing Stage 2
        """
        passed_candidates = []
        
        # Get origin path from config/env
        origin_repo_path_str = self.config.get("origin_repo_path", "")
        origin_repo_path = None
        if origin_repo_path_str:
            # Normalize path
            origin_repo_path_str = str(Path(origin_repo_path_str))
            origin_repo_path = Path(origin_repo_path_str)
            if not origin_repo_path.exists():
                origin_repo_path = None
        
        # If not in config: try env
        if not origin_repo_path:
            origin_repo_path_env = os.getenv("ORIGIN_REPO_PATH", "")
            if origin_repo_path_env:
                origin_repo_path = Path(origin_repo_path_env)
                if not origin_repo_path.exists():
                    origin_repo_path = None
        
        # Get other fork paths
        other_forks_paths = []
        other_forks_config = self.config.get("other_forks_paths", [])
        if other_forks_config:
            for fork_path_str in other_forks_config:
                if fork_path_str:  # Ensure not empty
                    # Normalize path
                    fork_path_str = str(Path(fork_path_str))
                    fork_path = Path(fork_path_str)
                    if fork_path.exists():
                        other_forks_paths.append(fork_path)
        
        # Can also get from env
        if not other_forks_paths:
            other_forks_env = os.getenv("OTHER_FORKS_PATHS", "")
            if other_forks_env:
                for fork_path_str in other_forks_env.split(os.pathsep):
                    fork_path_str = fork_path_str.strip()
                    if fork_path_str:  # Ensure not empty
                        # Normalize path
                        fork_path_str = str(Path(fork_path_str))
                        fork_path = Path(fork_path_str)
                        if fork_path.exists():
                            other_forks_paths.append(fork_path)
        
        for candidate in candidates:
            # Determine commit's repo
            origin_repo_name = candidate.origin_repo
            fork_repo_name = candidate.fork_repo
            # fork==origin: commit in origin
            commit_is_in_origin = (fork_repo_name == origin_repo_name)
            
            # Determine repo to exclude
            commit_source_repo = origin_repo_name if commit_is_in_origin else fork_repo_name
            
            # Get all repos in family
            # 1. Try all_forks_paths from config
            # 2. Else from other_forks_paths
            # 3. Else try JSON files
            
            all_family_forks_paths = []
            
            # Start from other_forks_paths if provided
            if other_forks_paths:
                all_family_forks_paths.extend(other_forks_paths)
            
            # If all_forks_paths in config, add (higher priority)
            all_forks_config = self.config.get("all_forks_paths", [])
            if all_forks_config:
                for fork_path_str in all_forks_config:
                    if fork_path_str:
                        fork_path_str = str(Path(fork_path_str))
                        fork_path = Path(fork_path_str)
                        if fork_path.exists() and fork_path not in all_family_forks_paths:
                            all_family_forks_paths.append(fork_path)
            
            # If not yet: try JSON files
            if not all_family_forks_paths and origin_repo_name:
                # Try forks_with_unique_commits.json for same-origin forks
                forks_json_path = Path("forks_with_unique_commits.json")
                if forks_json_path.exists():
                    try:
                        with open(forks_json_path, "r", encoding="utf-8") as f:
                            forks_data = json.load(f)
                        
                        # Find forks under same origin
                        for fork_item in forks_data:
                            if fork_item.get("origin", "") == origin_repo_name:
                                other_fork_repo = fork_item.get("fork", "")
                                if other_fork_repo:
                                    # Get other fork path
                                    other_fork_path = _get_fork_repo_path(other_fork_repo)
                                    if other_fork_path and other_fork_path not in all_family_forks_paths:
                                        all_family_forks_paths.append(other_fork_path)
                    except Exception as e:
                        # If read fails, ignore
                        pass
            
            # Filter out commit's repo from all_family_forks_paths
            filtered_other_forks_paths = []
            if commit_source_repo:
                # Convert commit's repo name to path format
                commit_repo_normalized_underscore = commit_source_repo.replace("/", "__")
                commit_repo_normalized_slash = commit_source_repo
                
                for fork_path in all_family_forks_paths:
                    fork_path_str = str(fork_path)
                    # Check path contains commit's repo name
                    is_commit_source = False
                    
                    # Check path end matches (common case)
                    if (fork_path_str.endswith(commit_repo_normalized_underscore) or
                        fork_path_str.endswith(commit_repo_normalized_slash)):
                        is_commit_source = True
                    # Check path contains full name
                    elif (f"{os.sep}{commit_repo_normalized_underscore}{os.sep}" in fork_path_str or
                          f"/{commit_repo_normalized_underscore}/" in fork_path_str or
                          f"{os.sep}{commit_repo_normalized_slash}{os.sep}" in fork_path_str or
                          f"/{commit_repo_normalized_slash}/" in fork_path_str):
                        is_commit_source = True
                    # Check path end matches (with sep)
                    elif (fork_path_str.endswith(f"{os.sep}{commit_repo_normalized_underscore}") or
                          fork_path_str.endswith(f"/{commit_repo_normalized_underscore}") or
                          fork_path_str.endswith(f"{os.sep}{commit_repo_normalized_slash}") or
                          fork_path_str.endswith(f"/{commit_repo_normalized_slash}")):
                        is_commit_source = True
                    
                    # If not commit's repo, add to filtered
                    if not is_commit_source:
                        filtered_other_forks_paths.append(fork_path)
            else:
                # If commit's repo unknown, keep all forks
                filtered_other_forks_paths = all_family_forks_paths
            
            # Determine origin path to check
            # If commit in origin: exclude origin; else keep
            final_origin_repo_path = None
            if origin_repo_path and not commit_is_in_origin:
                # Commit not in origin, check origin
                final_origin_repo_path = origin_repo_path
            # If commit in origin: final_origin_repo_path=None
            # Init result structure
            result = {
                "passed": False,
                "repos_passed_all_checks": []
            }
            
            # Get file list and diff
            files = []
            diff_content = ""
            
            if candidate.item_type == "commit":
                files = candidate.raw_data.get("files", [])
                diff_content = candidate.raw_data.get("diff", "") or candidate.raw_data.get("patch", "")
            elif candidate.item_type == "pr":
                files = candidate.raw_data.get("files", [])
                diff_content = candidate.raw_data.get("diff", "") or candidate.raw_data.get("patch", "")
            
            # If files empty, parse from diff
            if not files and diff_content:
                files = self.parse_files_from_diff(diff_content)
            
            # File existence check (origin + forks, exclude commit's repo)
            file_existence_score, file_existence_info = self.check_file_existence(
                files, final_origin_repo_path, filtered_other_forks_paths
            )
            
            # Diff context check (only if file existence passes)
            diff_context_score = 0.0
            diff_context_info = {}
            
            if file_existence_score >= 1.0:
                # File exists: do diff context check
                diff_context_score, diff_context_info = self.check_diff_context_in_repos(
                    diff_content, final_origin_repo_path, filtered_other_forks_paths
                )
            
            # Git apply check (only if diff context passes)
            git_apply_score = 0.0
            git_apply_info = {}
            
            if self.config.get("enable_git_apply", False):
                if diff_context_score >= 1.0:
                    # Diff context passed: do git apply check
                    git_apply_score, git_apply_info = self.check_git_apply(
                        diff_content, final_origin_repo_path, filtered_other_forks_paths
                    )
            
            # Determine if all checks pass
            if self.config.get("enable_git_apply", False):
                all_checks_passed = (
                    file_existence_score >= 1.0 and
                    diff_context_score >= 1.0 and
                    git_apply_score >= 1.0
                )
            else:
                # Without git apply: only file existence and diff context
                all_checks_passed = (
                    file_existence_score >= 1.0 and
                    diff_context_score >= 1.0
                )
            
            # Check if each repo meets all conditions
            repo_scores_file = file_existence_info.get("repo_scores", {})
            repo_scores_diff = diff_context_info.get("repo_scores", {})
            repo_scores_git = git_apply_info.get("repo_scores", {}) if self.config.get("enable_git_apply", False) else {}
            
            # Find all checked repos
            all_repo_keys = set()
            all_repo_keys.update(repo_scores_file.keys())
            all_repo_keys.update(repo_scores_diff.keys())
            if repo_scores_git:
                all_repo_keys.update(repo_scores_git.keys())
            
            # Record all checked repos and status
            # Determine sync status per repo in Stage2
            passed_repos = []
            checked_repos = []
            
            # Import helpers from syncability_pipeline
            # Or reimplement for module independence
            try:
                from syncability_pipeline import (
                    _check_commit_exists_in_repo,
                    _check_file_contains_modified_code,
                    _check_commit_was_reverted,
                    _get_repo_last_updated_time,
                    _get_repo_stars
                )
            except ImportError:
                # If import fails, use local
                # Check functions exist
                import sys
                import syncability_pipeline as sp
                _check_commit_exists_in_repo = getattr(sp, '_check_commit_exists_in_repo', None)
                _check_file_contains_modified_code = getattr(sp, '_check_file_contains_modified_code', None)
                _check_commit_was_reverted = getattr(sp, '_check_commit_was_reverted', None)
                _get_repo_last_updated_time = getattr(sp, '_get_repo_last_updated_time', None)
                _get_repo_stars = getattr(sp, '_get_repo_stars', None)
            
            # Get commit hash
            commit_hash = candidate.item_id
            # Get modified file list
            files_changed = []
            if candidate.item_type == "commit":
                files_changed = candidate.raw_data.get("files", [])
                if isinstance(files_changed, list) and files_changed:
                    if isinstance(files_changed[0], dict):
                        files_changed = [f.get("path") or f.get("filename") or str(f) for f in files_changed]
            
            # If files_changed empty, extract from diff
            if not files_changed and diff_content:
                try:
                    from syncability_pipeline import _extract_files_from_diff
                    files_changed = _extract_files_from_diff(diff_content)
                except ImportError:
                    import syncability_pipeline as sp
                    _extract_files_from_diff = getattr(sp, '_extract_files_from_diff', None)
                    if _extract_files_from_diff:
                        files_changed = _extract_files_from_diff(diff_content)
            
            # Extract modified code (for already_synced check)
            code_after_commit = {}
            if diff_content:
                try:
                    from syncability_pipeline import _extract_code_after_commit
                    code_after_commit = _extract_code_after_commit(diff_content)
                except ImportError:
                    import syncability_pipeline as sp
                    _extract_code_after_commit = getattr(sp, '_extract_code_after_commit', None)
                    if _extract_code_after_commit:
                        code_after_commit = _extract_code_after_commit(diff_content)
            
            for repo_key in all_repo_keys:
                file_score = repo_scores_file.get(repo_key, {}).get("score", 0.0) if repo_key in repo_scores_file else 0.0
                diff_score = repo_scores_diff.get(repo_key, {}).get("score", 0.0) if repo_key in repo_scores_diff else 0.0
                
                if self.config.get("enable_git_apply", False):
                    git_score = repo_scores_git.get(repo_key, {}).get("score", 0.0) if repo_key in repo_scores_git else 0.0
                    repo_all_passed = (file_score >= 1.0 and diff_score >= 1.0 and git_score >= 1.0)
                else:
                    git_score = None
                    repo_all_passed = (file_score >= 1.0 and diff_score >= 1.0)
                
                # Get repo info from repo_scores
                repo_info = repo_scores_file.get(repo_key) or repo_scores_diff.get(repo_key) or repo_scores_git.get(repo_key, {})
                repo_name = repo_info.get("repo_name", "")
                repo_type = repo_info.get("repo_type", "unknown")
                repo_path_str = repo_info.get("repo_path", "")
                repo_path = Path(repo_path_str) if repo_path_str else None
                
                # Init sync status
                sync_status = None  # "syncable" | "unsyncable" | "already_synced"
                is_reverted = False
                last_updated = None
                stars = None
                
                # If repo_path valid, check if already synced
                if repo_path and repo_path.exists():
                    # Get metadata
                    if _get_repo_last_updated_time:
                        try:
                            last_updated = _get_repo_last_updated_time(repo_path)
                        except Exception:
                            pass
                    
                    if _get_repo_stars:
                        try:
                            stars = _get_repo_stars(repo_name)
                        except Exception:
                            pass
                    
                    # Check if reverted
                    if _check_commit_was_reverted and commit_hash:
                        try:
                            is_reverted = _check_commit_was_reverted(repo_path, commit_hash)
                        except Exception:
                            pass
                    
                    # Check if already synced
                    already_synced = False
                    files_missing = False
                    commit_exists = False
                    
                    # Method 1: check commit hash exists (not reverted)
                    # Fix: even if hash exists, verify code files still in current version
                    # If code files missing: may be overwritten, don't mark already_synced
                    if _check_commit_exists_in_repo and commit_hash and not is_reverted:
                        try:
                            # Verify path is valid git repo
                            if not repo_path.exists():
                                commit_exists = False
                            elif not (repo_path / ".git").exists():
                                commit_exists = False
                            else:
                                commit_exists = _check_commit_exists_in_repo(repo_path, commit_hash)
                                if commit_exists:
                                    # Hash exists: verify code files still present
                                    # Filter code files
                                    code_files_in_commit = [
                                        fp for fp in (files_changed or [])
                                        if _is_code_file(fp)
                                    ]
                                    
                                    if code_files_in_commit:
                                        # Check all code files exist
                                        all_code_files_exist = True
                                        for fp in code_files_in_commit:
                                            if not (repo_path / fp).exists():
                                                all_code_files_exist = False
                                                files_missing = True
                                                break
                                        
                                        if all_code_files_exist:
                                            already_synced = True
                                        # If any missing: continue to method 3
                                    # Else: empty files_changed
                                    # Continue to method 3 (code_after_commit from diff)
                        except Exception as e:
                            # If check fails: log, continue
                            commit_exists = False
                            pass
                    
                    # Method 2 (removed): git apply compare caused false positives
                    # Method 3: check file contains modified code (fallback)
                    # Only when commit hash not present
                    # Fix: all code files must exist and match; ignore non-code
                    if not already_synced and not commit_exists and code_after_commit and _check_file_contains_modified_code:
                        try:
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
                                    full_file_path = repo_path / file_path
                                    if not full_file_path.exists():
                                        # Code file missing
                                        any_code_file_missing = True
                                        all_code_files_matched = False
                                        break
                                    
                                    if not _check_file_contains_modified_code(repo_path, file_path, modified_lines):
                                        # File exists but content mismatch
                                        all_code_files_matched = False
                                        break
                                
                                if any_code_file_missing:
                                    files_missing = True
                                
                                if all_code_files_matched and not any_code_file_missing:
                                    already_synced = True
                            # No code files to check: don't mark already_synced
                        except Exception:
                            pass
                    
                    # Determine sync status
                    # If hash exists: already_synced, skip portability check
                    if already_synced:
                        # Hash exists or content match: already_synced
                        sync_status = "already_synced"
                    elif files_missing or not repo_all_passed:
                        # File missing or portability failed: unsyncable
                        sync_status = "unsyncable"
                    elif repo_all_passed:
                        sync_status = "syncable"
                    else:
                        # Other cases: unsyncable
                        sync_status = "unsyncable"
                
                else:
                    # If repo_path invalid or missing: not already_synced
                    # Only syncable or unsyncable from check results
                    if repo_all_passed:
                        sync_status = "syncable"
                    else:
                        sync_status = "unsyncable"
                    # If path missing: still record for compat, but sync_status not already_synced
                
                # Record all checked repos with sync status
                checked_repos.append({
                    "repo_type": repo_type,
                    "repo_name": repo_name,
                    "repo_path": repo_path_str,
                    "passed": repo_all_passed,
                    "file_score": file_score,
                    "diff_score": diff_score,
                    "git_score": git_score,
                    "sync_status": sync_status,
                    "is_reverted": is_reverted,
                    "last_updated": last_updated,
                    "stars": stars
                })
                
                # Record repos that passed all checks (backward compat)
                if repo_all_passed:
                    passed_repos.append({
                        "repo_type": repo_type,
                        "repo_name": repo_name
                    })
            
            # Keep final info including all checked repos
            # passed=False if all already_synced (no sync needed) or all unsyncable
            # passed=True if at least one syncable
            has_syncable_repo = any(
                repo.get("sync_status") == "syncable" 
                for repo in checked_repos
            )
            has_already_synced_repo = any(
                repo.get("sync_status") == "already_synced" 
                for repo in checked_repos
            )
            
            # passed=True if at least one syncable
            # passed=False if all already_synced or unsyncable
            if has_syncable_repo:
                result["passed"] = True
            elif has_already_synced_repo and not has_syncable_repo:
                # All already_synced: no sync needed, passed=False
                result["passed"] = False
            else:
                # All unsyncable: passed=False
                result["passed"] = False
            
            result["repos_passed_all_checks"] = passed_repos
            result["repos_checked"] = checked_repos
            
            # Save result
            candidate.stage2_result = result
            
            # Keep only passed candidates
            if result["passed"]:
                passed_candidates.append(candidate)
        
        return passed_candidates

