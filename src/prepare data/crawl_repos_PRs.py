#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch PR info for all repos from GitHub API.
Supports multi-token rotation and multi-threading.
Fixed token exhaustion deadlock issue.
"""

import json
import os
import time
import threading
from typing import List, Dict, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

try:
    import requests
except ImportError:
    print("Error: requests library required")
    print("Run: pip install requests")
    exit(1)


class GitHubPRCrawler:
    def __init__(
        self,
        tokens: List[str] = None,
        output_dir: str = None,
        max_workers: int = 15,
        checkpoint_file: str = None,
        enrich_details: bool = False,
        enrich_sleep: float = 0.2,
    ):
        """
        Initialize PR crawler.

        Args:
            tokens: List of GitHub Personal Access Tokens (supports multi-token rotation)
            output_dir: Output directory path (uses current dir if None)
            max_workers: Max worker threads in pool (default 15)
            checkpoint_file: Checkpoint file path (for resumable runs)
            enrich_details: Whether to request PR detail API for commits/additions/... (default False)
            enrich_sleep: Sleep seconds between PR detail requests when enriching (default 0.2, avoid rate limit)
        """
        self.tokens = tokens or []
        self.base_url = "https://api.github.com"
        self.max_workers = max_workers
        self.enrich_details = enrich_details
        self.enrich_sleep = enrich_sleep
        
        # Thread locks for shared resources
        self.token_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        self.checkpoint_lock = threading.Lock()

        # Track each token's status
        self.token_status = {}
        for i in range(len(self.tokens)):
            self.token_status[i] = {
                'remaining': 5000,
                'reset_time': 0,
                'exhausted': False,
                'last_checked': 0
            }
        
        # Current token index (rotating)
        self.current_token_index = 0

        # Output directory
        if output_dir is None:
            self.output_dir = os.getcwd()
        else:
            self.output_dir = output_dir
            os.makedirs(self.output_dir, exist_ok=True)
        
        # Checkpoint file path
        self.checkpoint_file = checkpoint_file

        # Track timed-out repos (for retry on next run)
        self.timeout_repos = set()

        # Track 500-error repos (for retry on next run)
        self.server_error_repos = set()

        # Stats
        self.stats = {
            'total_repos': 0,
            'processed_repos': 0,
            'total_prs': 0,
            'api_calls': 0,
            'errors': 0
        }
    
    def _get_available_token_index(self) -> Optional[int]:
        """Get an available token index (thread-safe)"""
        current_time = time.time()
        
        with self.token_lock:
            # First check if current token is available
            current_idx = self.current_token_index
            current_status = self.token_status.get(current_idx, {})
            
            # If current token not exhausted, return it
            if not current_status.get('exhausted', False):
                return current_idx
            
            # If current token exhausted but reset, recover it
            if current_status.get('reset_time', 0) <= current_time:
                current_status['exhausted'] = False
                current_status['remaining'] = 5000
                return current_idx
            
            # From current index, loop through all tokens
            for offset in range(len(self.tokens)):
                idx = (current_idx + offset) % len(self.tokens)
                status = self.token_status.get(idx, {})
                
                # If token not exhausted, use it
                if not status.get('exhausted', False):
                    self.current_token_index = idx
                    return idx
                
                # If token reset, recover and use it
                if status.get('reset_time', 0) <= current_time:
                    status['exhausted'] = False
                    status['remaining'] = 5000
                    self.current_token_index = idx
                    return idx

            # All tokens exhausted
            return None
    
    def _wait_for_token_reset(self) -> Optional[int]:
        """Wait for token reset, return first available token index"""
        # Important: do not hold token_lock during sleep.
        # Compute earliest reset time (brief lock)
        with self.token_lock:
            current_time = time.time()
            reset_times = [
                (idx, status.get("reset_time", 0))
                for idx, status in self.token_status.items()
                if status.get("reset_time", 0) > current_time
            ]
            if not reset_times:
                # All tokens expired or no reset_time: recover directly
                for idx, status in self.token_status.items():
                    status["exhausted"] = False
                    status["remaining"] = max(int(status.get("remaining", 0) or 0), 5000)
                self.current_token_index = 0
                return 0
            earliest_idx, earliest_reset = min(reset_times, key=lambda x: x[1])

        wait_time = earliest_reset - time.time() + 1  # +1 sec buffer
        if wait_time < 0:
            wait_time = 0

        print(f"All tokens exhausted, waiting {wait_time:.0f}s until earliest reset time...")

        # Adjust check interval by wait time: more frequent checks for short waits
        if wait_time <= 60:
            check_interval = 5
        elif wait_time <= 300:
            check_interval = 15
        else:
            check_interval = 30

        deadline = time.time() + wait_time
        last_log_time = 0.0

        while True:
            now = time.time()
            if now >= deadline:
                break

            # Sleep first, then check (without holding lock)
            sleep_time = min(check_interval, max(0.0, deadline - now))
            time.sleep(sleep_time)

            # Check if any token has reset (brief lock)
            current_time = time.time()
            with self.token_lock:
                for idx, status in self.token_status.items():
                    reset_time = status.get("reset_time", 0) or 0
                    if reset_time and reset_time <= current_time:
                        status["exhausted"] = False
                        status["remaining"] = max(int(status.get("remaining", 0) or 0), 5000)
                        self.current_token_index = idx
                        print(f"Token {idx + 1} reset, resuming...")
                        return idx

            # Log remaining wait time
            remaining = deadline - current_time
            if remaining > 0 and ((current_time - last_log_time >= 10) or (remaining <= 30)):
                print(f"Waiting... {remaining:.0f}s remaining")
                last_log_time = current_time

        # After deadline: restore all expired tokens and return first available
        current_time = time.time()
        recovered_tokens = []
        with self.token_lock:
            for idx, status in self.token_status.items():
                reset_time = status.get("reset_time", 0) or 0
                if reset_time and reset_time <= current_time:
                    status["exhausted"] = False
                    status["remaining"] = max(int(status.get("remaining", 0) or 0), 5000)
                    recovered_tokens.append(idx + 1)

            for idx in range(len(self.tokens)):
                if not self.token_status[idx].get("exhausted", False):
                    self.current_token_index = idx
                    if recovered_tokens:
                        print(f"Wait complete. Restored {len(recovered_tokens)} tokens: {recovered_tokens}, switched to token {idx + 1}")
                    else:
                        print(f"Wait complete. Switched to token {idx + 1}")
                    return idx

            # Fallback: force restore first token
            self.current_token_index = 0
            if 0 in self.token_status:
                self.token_status[0]["exhausted"] = False
                self.token_status[0]["remaining"] = max(int(self.token_status[0].get("remaining", 0) or 0), 5000)
            print("Wait complete. Force restored token 1 (fallback)")
            return 0
    
    def _create_session(self, token_index: int) -> requests.Session:
        """Create session with specified token"""
        session = requests.Session()
        token = self.tokens[token_index]
        session.headers.update({
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}"
        })
        return session

    def _get_pr_detail(
        self,
        session: requests.Session,
        token_index: int,
        owner: str,
        repo: str,
        pr_number: int,
    ) -> Optional[Dict]:
        """Fetch single PR detail: GET /repos/{owner}/{repo}/pulls/{number}"""
        url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}"
        try:
            resp = session.get(url, timeout=30)
            with self.stats_lock:
                self.stats["api_calls"] += 1
            if resp.status_code == 200:
                # Update token status from response headers
                try:
                    remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
                    reset_time = int(resp.headers.get("X-RateLimit-Reset", 0))
                    self._update_token_status(token_index, remaining, reset_time)
                except Exception:
                    pass
                return resp.json()
            return None
        except Exception:
            return None
    
    def _update_token_status(self, token_index: int, remaining: int, reset_time: int) -> Optional[int]:
        """
        Update token status.

        Returns:
            Optional[int]: New token index if switch needed; None otherwise
        """
        with self.token_lock:
            self.token_status[token_index] = {
                'remaining': remaining,
                'reset_time': reset_time,
                'exhausted': remaining < 10,
                'last_checked': time.time()
            }
            
            # If current token almost exhausted, try next
            if remaining < 10:
                self.token_status[token_index]['exhausted'] = True
                # Find next available token (excluding current)
                current_time = time.time()
                for offset in range(1, len(self.tokens)):
                    idx = (token_index + offset) % len(self.tokens)
                    status = self.token_status.get(idx, {})
                    
                    # If token not exhausted, use it
                    if not status.get('exhausted', False):
                        self.current_token_index = idx
                        return idx
                    
                    # If token reset, recover and use it
                    if status.get('reset_time', 0) <= current_time:
                        status['exhausted'] = False
                        status['remaining'] = 5000
                        self.current_token_index = idx
                        return idx
                
                # No other available token found
                return None
        
        return None
    
    def check_rate_limit(self, token_index: int) -> bool:
        """Check API rate limit for specified token"""
        session = self._create_session(token_index)
        url = f"{self.base_url}/rate_limit"
        
        try:
            response = session.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                core = data['resources']['core']
                remaining = core['remaining']
                reset_time = core['reset']
                
                self._update_token_status(token_index, remaining, reset_time)
                return True
            else:
                print(f"Check rate limit failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"Check rate limit error: {e}")
            return False
    
    def get_pull_requests(self, owner: str, repo: str, max_prs: Optional[int] = None) -> Optional[List[Dict]]:
        """
        Get all PR info for specified repo.

        Args:
            owner: Repo owner
            repo: Repo name
            max_prs: Max PR count (None = no limit)

        Returns:
            List[Dict]: PR list; None if 500 error and retries failed
        """
        prs = []
        page = 1
        per_page = 100
        server_error_retries = 0
        max_server_error_retries = 3  # Max retries for 500 errors
        
        # Get initial token
        token_index = self._get_available_token_index()
        if token_index is None:
            # All tokens exhausted, wait for reset
            token_index = self._wait_for_token_reset()
            if token_index is None:
                print(f"[{owner}/{repo}] No available token")
                return prs
        
        session = self._create_session(token_index)
        
        while True:
            try:
                url = f"{self.base_url}/repos/{owner}/{repo}/pulls"
                params = {
                    'state': 'all',  # All PR states (open, closed, merged)
                    'per_page': per_page,
                    'page': page
                }
                
                response = session.get(url, params=params, timeout=30)
                
                with self.stats_lock:
                    self.stats['api_calls'] += 1
                
                if response.status_code == 200:
                    pr_list = response.json()
                    
                    if not pr_list:
                        break
                    
                    for pr in pr_list:
                        head_repo_full_name = None
                        try:
                            head_repo = (pr.get("head") or {}).get("repo") or {}
                            if isinstance(head_repo, dict):
                                head_repo_full_name = head_repo.get("full_name")
                        except Exception:
                            head_repo_full_name = None

                        base_repo_full_name = None
                        try:
                            base_repo = (pr.get("base") or {}).get("repo") or {}
                            if isinstance(base_repo, dict):
                                base_repo_full_name = base_repo.get("full_name")
                        except Exception:
                            base_repo_full_name = None
                        pr_item = {
                            'number': pr.get('number'),
                            'title': pr.get('title', ''),
                            'state': pr.get('state', ''),
                            'user': pr.get('user', {}).get('login', ''),
                            'created_at': pr.get('created_at', ''),
                            'updated_at': pr.get('updated_at', ''),
                            'closed_at': pr.get('closed_at'),
                            'merged_at': pr.get('merged_at'),
                            'mergeable': pr.get('mergeable'),
                            'mergeable_state': pr.get('mergeable_state'),
                            'draft': pr.get('draft', False),
                            'url': pr.get('html_url', ''),
                            'body': pr.get('body', '')[:500] if pr.get('body') else '',
                            # refs (branch names)
                            'base_ref': (pr.get('base') or {}).get('ref', ''),
                            'head_ref': (pr.get('head') or {}).get('ref', ''),
                            # repo full name (for external fork source)
                            'base_repo_full_name': base_repo_full_name,
                            'head_repo_full_name': head_repo_full_name,
                            # Note: /pulls list API usually does not return commits/additions/deletions/changed_files
                            # Store None to avoid unknown=0 polluting stats; can enrich later
                            'commits': None,
                            'additions': None,
                            'deletions': None,
                            'changed_files': None,
                            'labels': [label.get('name', '') for label in pr.get('labels', [])],
                            'review_comments': pr.get('review_comments', 0),
                            'comments': pr.get('comments', 0)
                        }
                        prs.append(pr_item)
                    
                    # Update token status from response headers
                    remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                    reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                    next_token_idx = self._update_token_status(token_index, remaining, reset_time)
                    
                    # If token switch needed, update token_index and session
                    if next_token_idx is not None and next_token_idx != token_index:
                        print(f"[{owner}/{repo}] Token {token_index + 1} has {remaining} left, switching to token {next_token_idx + 1}")
                        token_index = next_token_idx
                        session = self._create_session(token_index)
                    
                    if max_prs and len(prs) >= max_prs:
                        prs = prs[:max_prs]
                        break
                    
                    if len(pr_list) < per_page:
                        break
                    
                    page += 1
                    time.sleep(0.5)  # Avoid request spam
                    
                elif response.status_code == 403:
                    # Rate limited - mark current token as exhausted
                    with self.token_lock:
                        self.token_status[token_index]['exhausted'] = True
                        # Get reset time from response headers
                        reset_time = int(response.headers.get('X-RateLimit-Reset', 0))
                        if reset_time > 0:
                            self.token_status[token_index]['reset_time'] = reset_time
                    
                    print(f"[{owner}/{repo}] Token {token_index + 1} rate limited, looking for next available token...")
                    
                    # Try to get next available token
                    next_token_idx = self._get_available_token_index()
                    if next_token_idx is not None:
                        token_index = next_token_idx
                        session = self._create_session(token_index)
                        print(f"[{owner}/{repo}] Switched to token {token_index + 1}/{len(self.tokens)}")
                        time.sleep(2)
                        continue
                    else:
                        # All tokens exhausted, wait for reset
                        print(f"[{owner}/{repo}] All tokens exhausted, waiting for reset...")
                        next_token_idx = self._wait_for_token_reset()
                        if next_token_idx is not None:
                            token_index = next_token_idx
                            session = self._create_session(token_index)
                            print(f"[{owner}/{repo}] Switched to token {token_index + 1}, continuing...")
                            time.sleep(1)  # Brief wait before continuing
                            continue
                        else:
                            print(f"[{owner}/{repo}] No available token, skipping")
                            break
                            
                elif response.status_code == 404:
                    print(f"[{owner}/{repo}] Repo not found or no access")
                    break
                elif response.status_code >= 500:
                    # Server error (500, 502, 503 etc), retry
                    server_error_retries += 1
                    if server_error_retries <= max_server_error_retries:
                        # Exponential backoff: 5s, 10s, 20s
                        wait_time = 5 * (2 ** (server_error_retries - 1))
                        print(f"[{owner}/{repo}] Server error {response.status_code}, retrying in {wait_time}s ({server_error_retries}/{max_server_error_retries})...")
                        time.sleep(wait_time)
                        continue
                    else:
                        # Retries exhausted, record and return None
                        full_name = f"{owner}/{repo}"
                        with self.token_lock:
                            self.server_error_repos.add(full_name)
                        print(f"[{owner}/{repo}] Server error {response.status_code}, failed after {max_server_error_retries} retries, will retry on next run")
                        return None
                else:
                    print(f"[{owner}/{repo}] Get PR failed: {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"[{owner}/{repo}] Get PR error: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
                # Network error, retry once
                time.sleep(5)
                continue
        
        return prs

    def enrich_prs_details(self, owner: str, repo: str, prs: List[Dict]) -> List[Dict]:
        """Enrich PR list with detail fields (increases API calls)."""
        if not self.enrich_details:
            return prs
        if not prs:
            return prs

        # Use current available token/session; token exhaustion handled in get_pull_requests
        # Simplified: use current token; skip enrich for PR if it fails
        token_index = self._get_available_token_index()
        if token_index is None:
            token_index = self._wait_for_token_reset()
        if token_index is None:
            return prs
        session = self._create_session(token_index)

        for pr_item in prs:
            num = pr_item.get("number")
            if not isinstance(num, int):
                continue
            detail = self._get_pr_detail(session, token_index, owner, repo, num)
            if isinstance(detail, dict):
                # Detail API includes commits/additions/deletions/changed_files
                pr_item["commits"] = detail.get("commits")
                pr_item["additions"] = detail.get("additions")
                pr_item["deletions"] = detail.get("deletions")
                pr_item["changed_files"] = detail.get("changed_files")
                # Fill head/base repo full_name (in case list API lacks it)
                try:
                    hr = (detail.get("head") or {}).get("repo") or {}
                    if isinstance(hr, dict) and hr.get("full_name"):
                        pr_item["head_repo_full_name"] = hr.get("full_name")
                except Exception:
                    pass
                try:
                    br = (detail.get("base") or {}).get("repo") or {}
                    if isinstance(br, dict) and br.get("full_name"):
                        pr_item["base_repo_full_name"] = br.get("full_name")
                except Exception:
                    pass

            if self.enrich_sleep and self.enrich_sleep > 0:
                time.sleep(self.enrich_sleep)
        
        return prs
    
    def crawl_repos_prs(self, repos: List[Dict], max_prs_per_repo: Optional[int] = None, resume: bool = True) -> Dict:
        """
        Crawl PR info for multiple repos.

        Args:
            repos: Repo list, each has 'full_name' or 'owner' and 'name'
            max_prs_per_repo: Max PRs per repo (None = no limit)
            resume: Resume from checkpoint (default True)

        Returns:
            Dict: All repos PR data
        """
        # Try to resume from checkpoint
        processed_repos = set()
        all_data = {
            'repositories': [],
            'crawl_time': datetime.now().isoformat(),
            'stats': {}
        }
        
        if resume:
            checkpoint = self.load_checkpoint()
            if checkpoint:
                all_data['repositories'] = checkpoint.get('repositories', [])
                processed_repos = set(checkpoint.get('processed_repos', []))
                # Load timed-out repos but don't skip (allow retry)
                timeout_repos_from_checkpoint = set(checkpoint.get('timeout_repos', []))
                self.timeout_repos.update(timeout_repos_from_checkpoint)
                # Load 500-error repos but don't skip (allow retry)
                server_error_repos_from_checkpoint = set(checkpoint.get('server_error_repos', []))
                self.server_error_repos.update(server_error_repos_from_checkpoint)
                print(f"Resumed from checkpoint: {len(processed_repos)} repos already processed")
                if timeout_repos_from_checkpoint:
                    print(f"Found {len(timeout_repos_from_checkpoint)} timed-out repos, will retry")
                if server_error_repos_from_checkpoint:
                    print(f"Found {len(server_error_repos_from_checkpoint)} 500-error repos, will retry")

        # Filter out already processed repos
        repos_to_process = []
        for repo in repos:
            if 'full_name' in repo:
                full_name = repo['full_name']
            elif 'owner' in repo and 'name' in repo:
                full_name = f"{repo['owner']}/{repo['name']}"
            else:
                continue
            
            if full_name not in processed_repos:
                repos_to_process.append(repo)
        
        if not repos_to_process:
            print("All repos already processed!")
            # Update final stats
            with self.stats_lock:
                all_data['stats'] = {
                    'total_repositories': len(repos),
                    'processed_repositories': len(processed_repos),
                    'total_prs': sum(r.get('pr_count', 0) for r in all_data['repositories']),
                    'total_api_calls': 0,
                    'errors': 0
                }
            return all_data
        
        print(f"Need to process {len(repos_to_process)} new repos (total {len(repos)} repos)")

        # Init stats
        with self.stats_lock:
            self.stats = {
                'total_repos': len(repos),
                'processed_repos': len(processed_repos),
                'total_prs': sum(r.get('pr_count', 0) for r in all_data['repositories']),
                'api_calls': 0,
                'errors': 0
            }
        
        def process_repo(repo_info):
            """Process single repo (for multi-threading)"""
            # Parse repo info
            if 'full_name' in repo_info:
                full_name = repo_info['full_name']
                owner, repo_name = full_name.split('/', 1)
            elif 'owner' in repo_info and 'name' in repo_info:
                owner = repo_info['owner']
                repo_name = repo_info['name']
                full_name = f"{owner}/{repo_name}"
            else:
                return None
            
            try:
                # Check if previously timed-out or 500-error repo
                is_retry_timeout = full_name in self.timeout_repos
                is_retry_server_error = full_name in self.server_error_repos
                is_retry = is_retry_timeout or is_retry_server_error
                
                if is_retry_timeout:
                    print(f"[Retry] Processing previously timed-out repo: {full_name}")
                elif is_retry_server_error:
                    print(f"[Retry] Processing previously 500-error repo: {full_name}")
                
                prs = self.get_pull_requests(owner, repo_name, max_prs=max_prs_per_repo)
                
                # If None, timeout or 500 error, don't add to processed_repos
                if prs is None:
                    if is_retry_server_error:
                        print(f"[Skip] Repo {full_name} still returns 500, will retry on next run")
                    else:
                        print(f"[Skip] Repo {full_name} request timed out, will retry on next run")
                    return None
                
                # Optional: enrich PR detail fields (increases API calls)
                prs = self.enrich_prs_details(owner, repo_name, prs)
                
                repo_data = {
                    'full_name': full_name,
                    'owner': owner,
                    'repo': repo_name,
                    'prs': prs,
                    'pr_count': len(prs)
                }
                
                # If success, remove from timeout and 500-error lists
                if is_retry:
                    with self.token_lock:
                        self.timeout_repos.discard(full_name)
                        self.server_error_repos.discard(full_name)
                    retry_type = "timed-out" if is_retry_timeout else "500-error"
                    print(f"[Success] Previously {retry_type} repo {full_name} processed successfully, got {len(prs)} PRs")
                
                with self.stats_lock:
                    self.stats['processed_repos'] += 1
                    self.stats['total_prs'] += len(prs)
                
                return repo_data
            except Exception as e:
                print(f"Error processing repo {full_name}: {e}")
                with self.stats_lock:
                    self.stats['errors'] += 1
                return None
        
        # Process in parallel with thread pool
        completed_count = 0
        last_checkpoint_save = time.time()
        checkpoint_interval = 60  # Save checkpoint every 60s
        
        try:
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_repo = {executor.submit(process_repo, repo): repo for repo in repos_to_process}
                
                for future in as_completed(future_to_repo):
                    completed_count += 1
                    try:
                        repo_data = future.result()
                        if repo_data:
                            all_data['repositories'].append(repo_data)
                            processed_repos.add(repo_data['full_name'])
                        
                        # Show progress every 10 repos
                        if completed_count % 10 == 0:
                            with self.stats_lock:
                                stats = self.stats.copy()
                            print(f"Progress: {completed_count}/{len(repos_to_process)} new repos | "
                                  f"Total processed: {stats['processed_repos']}/{stats['total_repos']} | "
                                  f"PRs: {stats['total_prs']} | "
                                  f"API calls: {stats['api_calls']} | "
                                  f"Errors: {stats['errors']}")
                        
                        # Periodically save checkpoint
                        current_time = time.time()
                        if current_time - last_checkpoint_save >= checkpoint_interval:
                            with self.stats_lock:
                                all_data['stats'] = {
                                    'total_repositories': self.stats['total_repos'],
                                    'processed_repositories': self.stats['processed_repos'],
                                    'total_prs': self.stats['total_prs'],
                                    'total_api_calls': self.stats['api_calls'],
                                    'errors': self.stats['errors']
                                }
                            self.save_checkpoint(all_data, processed_repos)
                            last_checkpoint_save = current_time
                            print(f"Checkpoint saved ({len(processed_repos)} repos processed)")
                    
                    except Exception as e:
                        print(f"Exception processing repo: {e}")
                        with self.stats_lock:
                            self.stats['errors'] += 1
        except KeyboardInterrupt:
            print("\n\nCrawl interrupted by user, saving checkpoint...")
            with self.stats_lock:
                all_data['stats'] = {
                    'total_repositories': self.stats['total_repos'],
                    'processed_repositories': self.stats['processed_repos'],
                    'total_prs': self.stats['total_prs'],
                    'total_api_calls': self.stats['api_calls'],
                    'errors': self.stats['errors']
                }
            self.save_checkpoint(all_data, processed_repos)
            print("Checkpoint saved, will resume from breakpoint on next run")
            raise
        
        # Update final stats
        with self.stats_lock:
            all_data['stats'] = {
                'total_repositories': self.stats['total_repos'],
                'processed_repositories': self.stats['processed_repos'],
                'total_prs': self.stats['total_prs'],
                'total_api_calls': self.stats['api_calls'],
                'errors': self.stats['errors']
            }
        
        return all_data
    
    def save_checkpoint(self, all_data: Dict, processed_repos: set):
        """Save checkpoint (thread-safe)"""
        if not self.checkpoint_file:
            return
        
        try:
            with self.checkpoint_lock:
                checkpoint_data = {
                    'crawl_time': all_data.get('crawl_time', datetime.now().isoformat()),
                    'repositories': all_data.get('repositories', []),
                    'processed_repos': list(processed_repos),
                    'timeout_repos': list(self.timeout_repos),
                    'server_error_repos': list(self.server_error_repos),
                    'stats': all_data.get('stats', {}),
                    'last_update': datetime.now().isoformat()
                }
                
                # Write to temp file then atomic rename
                temp_file = self.checkpoint_file + '.tmp'
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
                
                # Atomic replace
                if os.path.exists(self.checkpoint_file):
                    os.replace(temp_file, self.checkpoint_file)
                else:
                    os.rename(temp_file, self.checkpoint_file)
        except Exception as e:
            print(f"Failed to save checkpoint: {e}")
    
    def load_checkpoint(self) -> Optional[Dict]:
        """Load checkpoint"""
        if not self.checkpoint_file or not os.path.exists(self.checkpoint_file):
            return None
        
        try:
            with open(self.checkpoint_file, 'r', encoding='utf-8') as f:
                checkpoint_data = json.load(f)
            
            print(f"Found checkpoint file: {self.checkpoint_file}")
            print(f"Last update: {checkpoint_data.get('last_update', 'unknown')}")
            print(f"Processed repos: {len(checkpoint_data.get('repositories', []))}")
            print(f"PRs crawled: {checkpoint_data.get('stats', {}).get('total_prs', 0)}")
            
            return checkpoint_data
        except Exception as e:
            print(f"Failed to load checkpoint: {e}")
            return None
    
    def delete_checkpoint(self):
        """Delete checkpoint file"""
        if self.checkpoint_file and os.path.exists(self.checkpoint_file):
            try:
                os.remove(self.checkpoint_file)
                print(f"Deleted checkpoint file: {self.checkpoint_file}")
            except Exception as e:
                print(f"Failed to delete checkpoint file: {e}")
    
    def save_to_json(self, data: Dict, filename: str):
        """Save data to JSON file"""
        output_path = os.path.join(self.output_dir, filename)
        print(f"\nSaving results to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        print(f"Results saved to: {output_path}")


def load_repos_from_json(json_file: str, only_original: bool = True) -> List[Dict]:
    """
    Load repos from JSON file.

    Args:
        json_file: Path to JSON file
        only_original: Only load original repos (exclude forks), default True

    Returns:
        List[Dict]: Repo list
    """
    print(f"Reading file: {json_file}")
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Compatible with missing_repos input format:
    # {"missing_repos": ["owner/repo", ...]}
    if isinstance(data, dict) and isinstance(data.get("missing_repos"), list):
        repos: List[Dict] = []
        seen_repos = set()
        for full_name in data.get("missing_repos") or []:
            if not isinstance(full_name, str) or "/" not in full_name:
                continue
            full_name = full_name.strip()
            if not full_name or full_name in seen_repos:
                continue
            owner, name = full_name.split("/", 1)
            repos.append({"full_name": full_name, "owner": owner, "name": name, "type": "original"})
            seen_repos.add(full_name)
        print(f"Found {len(repos)} original repos (deduplicated)")
        return repos
    
    repos = []
    seen_repos = set()

    for repo_data in data.get('repositories', []):
        # Add original repo
        original = repo_data.get('original_repo')
        if original and 'full_name' in original:
            full_name = original['full_name']
            if full_name not in seen_repos:
                repos.append({
                    'full_name': full_name,
                    'owner': full_name.split('/')[0],
                    'name': full_name.split('/')[1],
                    'type': 'original'
                })
                seen_repos.add(full_name)
        
        # Add forks only when only_original=False
        if not only_original:
            for fork in repo_data.get('forks', []):
                if 'full_name' in fork:
                    full_name = fork['full_name']
                    if full_name not in seen_repos:
                        repos.append({
                            'full_name': full_name,
                            'owner': full_name.split('/')[0],
                            'name': full_name.split('/')[1],
                            'type': 'fork'
                        })
                        seen_repos.add(full_name)
    
    repo_type = "original repos" if only_original else "repos (including forks)"
    print(f"Found {len(repos)} {repo_type} (deduplicated)")
    return repos


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="GitHub PR crawler (multi-threaded, token rotation)")
    parser.add_argument(
        "--workers",
        type=int,
        default=15,
        help="Number of worker threads (default 15)",
    )
    parser.add_argument(
        "--max-prs",
        type=int,
        default=None,
        help="Max PRs per repo (default no limit)",
    )
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Input JSON file path (default: github_projects_filtered_zero_stars.json)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output JSON file path (default: repos_prs.json)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Do not resume from checkpoint, start fresh",
    )
    parser.add_argument(
        "--enrich-details",
        action="store_true",
        help="Request detail API per PR for commits/additions/deletions/changed_files (significantly increases API calls)",
    )
    parser.add_argument(
        "--enrich-sleep",
        type=float,
        default=0.2,
        help="Sleep seconds between PR detail requests in enrich-details mode (default 0.2)",
    )
    args = parser.parse_args()
    
    # GitHub Personal Access Tokens
    tokens = []
    
    # Filter empty and invalid tokens
    tokens = [t for t in tokens if t and len(t) > 50 and t.startswith('github_pat_')]
    
    # Read token from env var (add to list if present)
    env_token = os.getenv('GITHUB_TOKEN')
    if env_token and env_token not in tokens:
        tokens.append(env_token)
    
    if not tokens:
        print("Error: No GitHub token found")
        return
    
    print(f"Configured {len(tokens)} GitHub tokens, will rotate automatically")
    
    # Set file paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = args.input or os.path.join(current_dir, 'github_projects_filtered_zero_stars.json')
    output_file = args.output or os.path.join(current_dir, 'repos_prs.json')
    
    # Checkpoint file path (based on output filename)
    checkpoint_file = os.path.join(current_dir, os.path.basename(output_file) + '.checkpoint')
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file not found: {input_file}")
        return
    
    # Load repo list
    repos = load_repos_from_json(input_file, only_original=True)
    
    if not repos:
        print("Error: No repos found")
        return
    
    # Create crawler instance
    crawler = GitHubPRCrawler(
        tokens=tokens, 
        output_dir=current_dir, 
        max_workers=args.workers,
        checkpoint_file=checkpoint_file if not args.no_resume else None,
        enrich_details=bool(args.enrich_details),
        enrich_sleep=float(args.enrich_sleep),
    )
    
    print(f"\nStarting crawl of {len(repos)} repos for PR info (using {args.workers} threads)...")
    print(f"Max {args.max_prs or 'all'} PRs per repo")
    if not args.no_resume:
        print(f"Checkpoint file: {checkpoint_file}")
    print()
    
    # Crawl PR info
    try:
        data = crawler.crawl_repos_prs(
            repos,
            max_prs_per_repo=args.max_prs,
            resume=not args.no_resume
        )
        
        # Save results
        crawler.save_to_json(data, os.path.basename(output_file))
        
        # Delete checkpoint (crawl complete)
        crawler.delete_checkpoint()
        
        # Print stats
        stats = data.get('stats', {})
        print(f"\n{'='*80}")
        print("Crawl complete!")
        print(f"Total repos: {stats.get('total_repositories', 0)}")
        print(f"Processed repos: {stats.get('processed_repositories', 0)}")
        print(f"Total PRs: {stats.get('total_prs', 0)}")
        print(f"Total API calls: {stats.get('total_api_calls', 0)}")
        print(f"Errors: {stats.get('errors', 0)}")
        print(f"{'='*80}")
        
    except KeyboardInterrupt:
        print("\n\nCrawl interrupted by user, checkpoint saved")
    except Exception as e:
        print(f"\n\nCrawl error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    main()

