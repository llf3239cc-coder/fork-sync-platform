#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub C/C++ project crawler.
Crawls C/C++ projects on GitHub and their fork lists.
"""

import requests
import time
import json
import csv
import re
import argparse
from typing import List, Dict, Optional
from datetime import datetime
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class GitHubCrawler:
    def __init__(self, tokens: List[str] = None, output_dir: str = None, max_workers: int = 15):
        """
        Initialize crawler.
        
        Args:
            tokens: List of GitHub Personal Access Tokens (supports rotation)
            output_dir: Output directory path (None = current directory)
            max_workers: Max worker threads in pool (default 15)
        """
        self.tokens = tokens or []
        self.current_token_index = 0
        self.base_url = "https://api.github.com"
        self.headers = {
            "Accept": "application/vnd.github.v3+json"
        }
        self.max_workers = max_workers
        
        # Thread locks for shared resources
        self.token_lock = threading.Lock()
        self.stats_lock = threading.Lock()
        
        # Token state: {token_index: {'remaining': int, 'reset_time': float, 'exhausted': bool}}
        self.token_status = {}
        for i in range(len(self.tokens)):
            self.token_status[i] = {
                'remaining': 5000,  # Initial; updated by API check
                'reset_time': 0,
                'exhausted': False
            }
        
        # Output directory
        if output_dir is None:
            self.output_dir = os.getcwd()
        else:
            self.output_dir = output_dir
            os.makedirs(self.output_dir, exist_ok=True)
        
        # Session for main thread
        self.session = requests.Session()
        self._update_headers()
        
        # Exclude keywords (filter books, tutorials, etc.)
        self.exclude_keywords = [
            'book', 'tutorial', 'example', 'sample', 'demo', 
            'learning', 'course', 'lesson', 'guide', 'docs',
            'documentation', 'notes', 'cheatsheet', 'reference',
            'practice', 'exercise', 'homework', 'assignment',
            'textbook', 'manual', 'handbook', 'primer'
        ]
        
        # Strict exclude keywords (if name or description contains these, exclude)
        self.strict_exclude_keywords = [
            'book', 'tutorial', 'example', 'sample', 'demo',
            'learning', 'course', 'lesson', 'practice', 'exercise'
        ]
        
        # Stats
        self.stats = {
            'total_repos': 0,
            'total_forks': 0,
            'api_calls': 0
        }
    
    def _update_headers(self):
        """Update request headers with current token."""
        if self.tokens and self.current_token_index < len(self.tokens):
            self.headers["Authorization"] = f"token {self.tokens[self.current_token_index]}"
        self.session.headers.update(self.headers)
    
    def _switch_token(self):
        """Switch to next token (thread-safe)."""
        with self.token_lock:
            if len(self.tokens) > 1:
                self.current_token_index = (self.current_token_index + 1) % len(self.tokens)
                self._update_headers()
                print(f"Switched to token {self.current_token_index + 1}/{len(self.tokens)}")
    
    def _find_available_token(self) -> Optional[int]:
        """Find next token with remaining quota (thread-safe).
        
        Returns:
            Optional[int]: Index of available token, or None if none available.
        """
        current_time = time.time()
        start_idx = self.current_token_index
        for _ in range(len(self.tokens)):
            idx = (start_idx + _) % len(self.tokens)
            status = self.token_status.get(idx, {})
            
            if not status.get('exhausted', False):
                return idx
            elif status.get('reset_time', 0) <= current_time:
                status['exhausted'] = False
                status['remaining'] = 5000
                return idx
        
        return None
    
    def _get_token_for_thread(self) -> Optional[str]:
        """Get a token for the thread (thread-safe; prefers available token)."""
        with self.token_lock:
            if self.tokens:
                available_idx = self._find_available_token()
                if available_idx is not None:
                    return self.tokens[available_idx]
                token_index = self.current_token_index % len(self.tokens)
                return self.tokens[token_index]
            return None
    
    def _update_session_token(self, session: requests.Session, token_index: int = None) -> requests.Session:
        """Update session token (e.g. when 403 requires switching).
        
        Args:
            session: Session to update
            token_index: Token index to use (None = current)
        """
        with self.token_lock:
            if self.tokens:
                if token_index is None:
                    token_index = self.current_token_index % len(self.tokens)
                else:
                    self.current_token_index = token_index
                token = self.tokens[token_index]
                session.headers.update({"Authorization": f"token {token}"})
                print(f"Session updated to token index {token_index + 1}/{len(self.tokens)}")
        return session
    
    def _create_thread_session(self) -> requests.Session:
        """Create a session for the thread."""
        session = requests.Session()
        token = self._get_token_for_thread()
        headers = {
            "Accept": "application/vnd.github.v3+json"
        }
        if token:
            headers["Authorization"] = f"token {token}"
        session.headers.update(headers)
        return session
    
    def _get_current_token(self) -> Optional[str]:
        """Get current token."""
        if self.tokens and self.current_token_index < len(self.tokens):
            return self.tokens[self.current_token_index]
        return None
    
    def check_rate_limit(self, session: requests.Session = None, start_token_index: int = None):
        """Check API rate limit; switch token if at limit.

        Args:
            session: Request session
            start_token_index: Token index at start (for cycle detection)
        """
        if session is None:
            session = self.session
        
        if start_token_index is None:
            with self.token_lock:
                start_token_index = self.current_token_index
        
        url = f"{self.base_url}/rate_limit"
        response = session.get(url)
        
        with self.stats_lock:
            self.stats['api_calls'] += 1
        
        if response.status_code == 200:
            data = response.json()
            core = data['resources']['core']
            remaining = core['remaining']
            reset_time = core['reset']
            reset_datetime = datetime.fromtimestamp(reset_time)
            
            with self.token_lock:
                current_idx = self.current_token_index
                self.token_status[current_idx] = {
                    'remaining': remaining,
                    'reset_time': reset_time,
                    'exhausted': remaining < 10
                }
                token_idx = current_idx + 1
            
            print(f"Token {token_idx} - API remaining: {remaining}, reset: {reset_datetime}")
            
            if remaining < 10:
                with self.token_lock:
                    self.token_status[current_idx]['exhausted'] = True
                
                if len(self.tokens) > 1:
                    with self.token_lock:
                        next_token_idx = self._find_available_token()
                    
                    if next_token_idx is not None:
                        with self.token_lock:
                            self.current_token_index = next_token_idx
                        
                        if session != self.session:
                            session = self._update_session_token(session, next_token_idx)
                        else:
                            self._update_headers()
                        
                        print(f"Token {token_idx} exhausted, switched to token {next_token_idx + 1}/{len(self.tokens)}")
                        return self.check_rate_limit(session, start_token_index)
                    else:
                        with self.token_lock:
                            max_reset_time = max(
                                status.get('reset_time', 0) 
                                for status in self.token_status.values()
                            )
                        wait_time = max_reset_time - time.time() + 1
                        if wait_time > 0:
                            print(f"All tokens rate-limited, waiting {wait_time:.0f}s...")
                            time.sleep(wait_time)
                        return remaining
                else:
                    wait_time = reset_time - time.time() + 1
                    if wait_time > 0:
                        print(f"Rate limit near, waiting {wait_time:.0f}s...")
                        time.sleep(wait_time)
            
            return remaining
        elif response.status_code == 401:
            with self.token_lock:
                current_idx = self.current_token_index
                self.token_status[current_idx]['exhausted'] = True
                
                if len(self.tokens) > 1:
                    next_token_idx = self._find_available_token()
                else:
                    next_token_idx = None
            
            if len(self.tokens) > 1:
                print(f"Token {current_idx + 1} invalid, finding next...")
                
                if next_token_idx is not None:
                    with self.token_lock:
                        self.current_token_index = next_token_idx
                    if session != self.session:
                        session = self._update_session_token(session, next_token_idx)
                    else:
                        self._update_headers()
                    return self.check_rate_limit(session, start_token_index)
                else:
                    print("All tokens invalid or exhausted")
        return 0
    
    def check_repo_content(self, owner: str, repo: str, session: requests.Session = None) -> Dict:
        """
        Check repo content (file structure, etc.).
        
        Args:
            owner: Repo owner
            repo: Repo name
            session: Request session (None = thread session)
            
        Returns:
            Dict with repo feature info
        """
        if session is None:
            session = self._create_thread_session()
        
        try:
            url = f"{self.base_url}/repos/{owner}/{repo}/contents"
            response = session.get(url, params={'per_page': 100}, timeout=10)
            
            with self.stats_lock:
                self.stats['api_calls'] += 1
            
            if response.status_code == 200:
                contents = response.json()
                if not isinstance(contents, list):
                    return {'has_code': False, 'md_ratio': 1.0, 'file_count': 0}
                
                total_files = len(contents)
                md_files = sum(1 for item in contents if item.get('name', '').endswith('.md'))
                code_files = sum(1 for item in contents 
                               if item.get('name', '').endswith(('.c', '.cpp', '.h', '.hpp', '.cc', '.cxx')))
                
                md_ratio = md_files / total_files if total_files > 0 else 0
                
                return {
                    'has_code': code_files > 0,
                    'md_ratio': md_ratio,
                    'file_count': total_files,
                    'code_files': code_files,
                    'md_files': md_files
                }
        except:
            pass
        
        return {'has_code': True, 'md_ratio': 0, 'file_count': 0}
    
    def is_actual_project(self, repo: Dict, check_content: bool = False, debug: bool = False) -> tuple:
        """
        Whether repo is a real project (exclude books, tutorials, etc.).
        
        Args:
            repo: Repo info dict
            check_content: Whether to check repo content (more API calls)
            debug: Whether to return (kept, filter_reason)
            
        Returns:
            bool or (bool, str): If debug, (kept, reason); else bool
        """
        filter_reason = None
        name_lower = repo['name'].lower()
        description = (repo.get('description') or '').lower()
        full_name_lower = repo['full_name'].lower()
        stars = repo.get('stargazers_count', 0)
        forks = repo.get('forks_count', 0)
        size = repo.get('size', 0)  # KB
        
        text_to_check = f"{name_lower} {description} {full_name_lower}"
        
        strict_name_keywords = ['learn', 'tutorial', 'example', 'demo', 'sample', 
                               'practice', 'exercise', 'homework', 'assignment', 'course']
        for keyword in strict_name_keywords:
            if keyword in name_lower:
                if stars < 1000:
                    filter_reason = f"name contains '{keyword}' and star<1000"
                    if debug:
                        return (False, filter_reason)
                    return False
        
        tutorial_keywords = ['learn', 'tutorial', 'course', 'lesson', 'practice',
                           'exercise', 'homework', 'assignment', 'book', 'guide',
                           'cheatsheet', 'reference', 'notes', 'documentation']
        desc_tutorial_count = sum(1 for kw in tutorial_keywords if kw in description)
        if desc_tutorial_count >= 2:
            if stars < 500:
                filter_reason = f"description has {desc_tutorial_count} tutorial keywords and star<500"
                if debug:
                    return (False, filter_reason)
                return False
        
        if size < 10:
            if stars < 50:
                filter_reason = f"repo size {size}KB<10 and star<50"
                if debug:
                    return (False, filter_reason)
                return False
        
        if forks > 0 and stars > 0:
            fork_star_ratio = forks / stars
            if fork_star_ratio > 2.0 and stars < 100:
                filter_reason = f"fork/star ratio {fork_star_ratio:.2f}>2 and star<100"
                if debug:
                    return (False, filter_reason)
                return False
        
        if check_content and stars < 200:
            owner, repo_name = repo['full_name'].split('/', 1)
            content_info = self.check_repo_content(owner, repo_name)
            
            if content_info.get('md_ratio', 0) > 0.5:
                filter_reason = f"markdown ratio {content_info.get('md_ratio', 0):.2f}>0.5"
                if debug:
                    return (False, filter_reason)
                return False
            
            if not content_info.get('has_code', True):
                filter_reason = "no code files"
                if debug:
                    return (False, filter_reason)
                return False
        
        tutorial_patterns = [
            r'^learn-', r'^tutorial-', r'^example-', r'^demo-', r'^sample-',
            r'^practice-', r'^exercise-', r'^homework-', r'^assignment-',
            r'-tutorial$', r'-example$', r'-demo$', r'-sample$',
            r'^cpp-', r'^c-', r'^cpp_', r'^c_'
        ]
        for pattern in tutorial_patterns:
            if re.search(pattern, name_lower):
                if stars < 200:
                    filter_reason = f"name matches pattern '{pattern}' and star<200"
                    if debug:
                        return (False, filter_reason)
                    return False
        
        if debug:
            return (True, "passed all filters")
        return True
    
    def search_repositories(self, language: str, sort: str = 'stars',
                           order: str = 'desc', per_page: int = 100,
                           max_pages: int = 10) -> List[Dict]:
        """
        Search GitHub repositories.
        
        Args:
            language: Language ('C' or 'C++')
            sort: Sort by ('stars', 'forks', 'updated')
            order: Order ('desc', 'asc')
            per_page: Results per page (max 100)
            max_pages: Max pages
            
        Returns:
            List of repo dicts
        """
        repos = []
        page = 1
        
        while page <= max_pages:
            self.check_rate_limit()
            
            query = f"language:{language} stars:>10"
            url = f"{self.base_url}/search/repositories"
            params = {
                'q': query,
                'sort': sort,
                'order': order,
                'per_page': min(per_page, 100),
                'page': page
            }
            
            try:
                response = self.session.get(url, params=params, timeout=30)
                self.stats['api_calls'] += 1
                
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', [])
                    
                    if not items:
                        break
                    
                    filtered_count = 0
                    filtered_examples = []
                    kept_examples = []
                    check_content = (page <= 2)
                    show_debug = (page == 1)
                    
                    for repo in items:
                        if show_debug:
                            is_actual, reason = self.is_actual_project(repo, check_content=check_content, debug=True)
                        else:
                            is_actual = self.is_actual_project(repo, check_content=check_content, debug=False)
                            reason = None
                        
                        if is_actual:
                            repos.append({
                                'name': repo['name'],
                                'full_name': repo['full_name'],
                                'url': repo['html_url'],
                                'description': repo.get('description', ''),
                                'stars': repo.get('stargazers_count', 0),
                                'forks': repo.get('forks_count', 0),
                                'language': repo.get('language', ''),
                                'created_at': repo.get('created_at', ''),
                                'updated_at': repo.get('updated_at', ''),
                                'is_fork': repo.get('fork', False),
                                'parent_repo': repo.get('parent', {}).get('full_name') if repo.get('fork') else None
                            })
                            if show_debug and len(kept_examples) < 5:
                                kept_examples.append({
                                    'name': repo['name'],
                                    'stars': repo.get('stargazers_count', 0),
                                    'description': repo.get('description', '')[:60]
                                })
                        else:
                            filtered_count += 1
                            if show_debug and len(filtered_examples) < 5 and reason:
                                filtered_examples.append({
                                    'name': repo['name'],
                                    'stars': repo.get('stargazers_count', 0),
                                    'description': repo.get('description', '')[:60],
                                    'reason': reason
                                })
                    
                    print(f"Page {page}: {len(items)} repos, filtered {filtered_count}, kept {len(repos)}")
                    
                    if show_debug:
                        if kept_examples:
                            print(f"  Kept examples (first 5):")
                            for ex in kept_examples:
                                print(f"    - {ex['name']} (⭐{ex['stars']}): {ex['description']}")
                        if filtered_examples:
                            print(f"  Filtered examples (first 5):")
                            for ex in filtered_examples:
                                print(f"    - {ex['name']} (⭐{ex['stars']}): {ex['description']} [reason: {ex['reason']}]")
                        else:
                            print(f"  No repos filtered on this page")
                    
                    if len(items) < per_page:
                        break
                    
                    page += 1
                    time.sleep(1)
                    
                elif response.status_code == 403:
                    with self.token_lock:
                        current_idx = self.current_token_index
                        self.token_status[current_idx]['exhausted'] = True
                        
                        if len(self.tokens) > 1:
                            next_token_idx = self._find_available_token()
                        else:
                            next_token_idx = None
                    
                    if len(self.tokens) > 1:
                        print("API rate limit (403), finding next token...")
                        
                        if next_token_idx is not None:
                            with self.token_lock:
                                self.current_token_index = next_token_idx
                            self._update_headers()
                            print(f"Switched to token {next_token_idx + 1}/{len(self.tokens)}")
                            time.sleep(2)
                        else:
                            print("All tokens rate-limited, waiting...")
                            time.sleep(60)
                    else:
                        print("API rate limit, waiting...")
                        time.sleep(60)
                else:
                    print(f"Request failed: {response.status_code}")
                    print(response.text)
                    break
                    
            except Exception as e:
                print(f"Search error: {e}")
                break
        
        return repos
    
    def get_forks(self, owner: str, repo: str, per_page: int = 100, 
                  max_forks: Optional[int] = None, session: requests.Session = None) -> List[Dict]:
        """
        Get fork list for a repo (only star >= 1).
        
        Args:
            owner: Repo owner
            repo: Repo name
            per_page: Results per page
            max_forks: Max forks (None = no limit)
            session: Request session (None = thread session)
            
        Returns:
            List of fork dicts (filtered, star >= 1)
        """
        if session is None:
            session = self._create_thread_session()
        
        forks = []
        page = 1
        
        effective_max_forks = max_forks if max_forks is not None else float('inf')
        
        while len(forks) < effective_max_forks:
            self.check_rate_limit(session)
            
            url = f"{self.base_url}/repos/{owner}/{repo}/forks"
            params = {
                'per_page': min(per_page, 100),
                'page': page,
                'sort': 'stargazers'
            }
            
            try:
                response = session.get(url, params=params, timeout=30)
                
                with self.stats_lock:
                    self.stats['api_calls'] += 1
                
                if response.status_code == 200:
                    fork_list = response.json()
                    
                    if not fork_list:
                        break
                    
                    valid_forks_before = len(forks)
                    star_stats = {0: 0, 1: 0, 2: 0, '3+': 0}
                    
                    for fork in fork_list:
                        stars = fork.get('stargazers_count', 0)
                        if stars == 0:
                            star_stats[0] += 1
                        elif stars == 1:
                            star_stats[1] += 1
                        elif stars == 2:
                            star_stats[2] += 1
                        else:
                            star_stats['3+'] += 1
                        
                        if stars >= 1:
                            forks.append({
                                'name': fork['name'],
                                'full_name': fork['full_name'],
                                'url': fork['html_url'],
                                'description': fork.get('description', ''),
                                'stars': stars,
                                'forks': fork.get('forks_count', 0),
                                'created_at': fork.get('created_at', ''),
                                'updated_at': fork.get('updated_at', ''),
                                'parent_repo': f"{owner}/{repo}"
                            })
                    
                    if page % 10 == 0 or len(fork_list) < per_page:
                        valid_forks_this_page = len(forks) - valid_forks_before
                        print(f"    Page {page}: {len(fork_list)} forks this page")
                        print(f"      Valid this page: {valid_forks_this_page} (1*={star_stats[1]}, 2*={star_stats[2]}, 3+*={star_stats['3+']})")
                        print(f"      Total valid: {len(forks)} (0*={star_stats[0]}, 1*={star_stats[1]}, 2*={star_stats[2]}, 3+*={star_stats['3+']})")
                    
                    if len(fork_list) < per_page:
                        break
                    
                    page += 1
                    time.sleep(0.5)
                    
                elif response.status_code == 403:
                    with self.token_lock:
                        current_idx = self.current_token_index
                        self.token_status[current_idx]['exhausted'] = True
                        
                        if len(self.tokens) > 1:
                            next_token_idx = self._find_available_token()
                        else:
                            next_token_idx = None
                    
                    if len(self.tokens) > 1:
                        print(f"[{owner}/{repo}] API rate limit (403), finding next token...")
                        
                        if next_token_idx is not None:
                            with self.token_lock:
                                self.current_token_index = next_token_idx
                            session = self._update_session_token(session, next_token_idx)
                            print(f"Switched to token {next_token_idx + 1}/{len(self.tokens)}")
                            time.sleep(2)
                        else:
                            print(f"[{owner}/{repo}] All tokens rate-limited, waiting...")
                            time.sleep(60)
                    else:
                        print(f"[{owner}/{repo}] API rate limit, waiting...")
                        time.sleep(60)
                elif response.status_code == 404:
                    print(f"Repo {owner}/{repo} not found or no access")
                    break
                else:
                    print(f"Get forks failed: {response.status_code}")
                    break
                    
            except Exception as e:
                print(f"Get forks error: {e}")
                break
        
        forks_sorted = sorted(forks, key=lambda x: x.get('stars', 0), reverse=True)
        
        if max_forks is not None:
            return forks_sorted[:max_forks]
        return forks_sorted
    
    def crawl_projects(self, languages: List[str] = ['C', 'C++'], 
                      max_repos_per_lang: Optional[int] = None,
                      max_forks_per_repo: Optional[int] = None) -> Dict:
        """
        Crawl projects and their fork lists.
        
        Args:
            languages: List of languages to crawl
            max_repos_per_lang: Max repos per language (None = no limit; API max 1000)
            max_forks_per_repo: Max forks per repo (None = no limit)
            
        Returns:
            Dict with original repos and fork lists
        """
        all_data = {
            'repositories': [],
            'crawl_time': datetime.now().isoformat(),
            'stats': {}
        }
        
        with self.stats_lock:
            self.stats = {
                'total_repos': 0,
                'total_forks': 0,
                'api_calls': 0
            }
        
        for language in languages:
            print(f"\nCrawling {language} projects...")
            
            if max_repos_per_lang is None:
                max_pages = 10
            else:
                max_pages = min(10, max(1, (max_repos_per_lang + 99) // 100))
            
            repos = self.search_repositories(
                language=language,
                sort='stars',
                order='desc',
                per_page=100,
                max_pages=max_pages
            )
            
            print(f"Found {len(repos)} {language} projects")
            
            original_repos = [r for r in repos if not r.get('is_fork', False)]
            print(f"  {len(original_repos)} are original (non-fork)")
            
            if max_repos_per_lang is not None:
                original_repos = original_repos[:max_repos_per_lang]
                print(f"  Limiting to first {max_repos_per_lang} original repos")
            
            def process_repo(repo_data_tuple):
                idx, repo = repo_data_tuple
                owner, repo_name = repo['full_name'].split('/', 1)
                
                try:
                    print(f"\n[{idx}/{len(original_repos)}] Processing: {repo['full_name']}")
                    print(f"  Getting fork list...")
                    
                    forks = self.get_forks(owner, repo_name, max_forks=max_forks_per_repo)
                    
                    repo_with_lang = repo.copy()
                    repo_with_lang['language_type'] = language
                    
                    repo_data = {
                        'original_repo': repo_with_lang,
                        'forks': forks,
                        'fork_count': len(forks),
                        'language_type': language
                    }
                    
                    with self.stats_lock:
                        self.stats['total_repos'] += 1
                        self.stats['total_forks'] += len(forks)
                    
                    print(f"  [{idx}/{len(original_repos)}] {repo['full_name']} - found {len(forks)} forks")
                    
                    return repo_data
                except Exception as e:
                    print(f"  [{idx}/{len(original_repos)}] Error processing {repo['full_name']}: {e}")
                    repo_with_lang = repo.copy()
                    repo_with_lang['language_type'] = language
                    return {
                        'original_repo': repo_with_lang,
                        'forks': [],
                        'fork_count': 0,
                        'language_type': language
                    }
            
            repos_to_process = list(enumerate(original_repos, 1))
            completed_count = 0
            
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_repo = {executor.submit(process_repo, repo_tuple): repo_tuple 
                                 for repo_tuple in repos_to_process}
                
                for future in as_completed(future_to_repo):
                    completed_count += 1
                    try:
                        repo_data = future.result()
                        all_data['repositories'].append(repo_data)
                        
                        if completed_count % 10 == 0:
                            all_data['stats'] = {
                                'total_repositories': self.stats['total_repos'],
                                'total_forks': self.stats['total_forks'],
                                'total_api_calls': self.stats['api_calls']
                            }
                            backup_filename = 'github_projects_backup.json'
                            self.save_to_json(all_data, backup_filename)
                            print(f"  Backup saved ({completed_count}/{len(repos_to_process)} repos)")
                    except Exception as e:
                        idx, repo = future_to_repo[future]
                        print(f"  Exception processing {repo['full_name']}: {e}")
            
            if completed_count > 0:
                all_data['stats'] = {
                    'total_repositories': self.stats['total_repos'],
                    'total_forks': self.stats['total_forks'],
                    'total_api_calls': self.stats['api_calls']
                }
                backup_filename = 'github_projects_backup.json'
                self.save_to_json(all_data, backup_filename)
                print(f"  Final backup saved ({completed_count} repos)")
        
        all_data['stats'] = {
            'total_repositories': self.stats['total_repos'],
            'total_forks': self.stats['total_forks'],
            'total_api_calls': self.stats['api_calls']
        }
        
        return all_data
    
    def save_to_json(self, data: Dict, filename: str):
        """Save data as JSON."""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"Data saved to {filepath}")
    
    def save_to_csv(self, data: Dict, filename: str):
        """Save data as CSV (original repos and forks separate)."""
        original_filepath = os.path.join(self.output_dir, f'original_repos_{filename}')
        with open(original_filepath, 'w', newline='', encoding='utf-8') as f:
            if not data['repositories']:
                return
            
            writer = csv.DictWriter(f, fieldnames=[
                'name', 'full_name', 'url', 'description', 'stars', 
                'forks', 'language', 'language_type', 'created_at', 'updated_at'
            ])
            writer.writeheader()
            
            for repo_data in data['repositories']:
                original = repo_data['original_repo']
                writer.writerow({
                    'name': original['name'],
                    'full_name': original['full_name'],
                    'url': original['url'],
                    'description': original.get('description', ''),
                    'stars': original.get('stars', 0),
                    'forks': original.get('forks', 0),
                    'language': original.get('language', ''),
                    'language_type': original.get('language_type', ''),
                    'created_at': original.get('created_at', ''),
                    'updated_at': original.get('updated_at', '')
                })
        
        forks_filepath = os.path.join(self.output_dir, f'forks_{filename}')
        with open(forks_filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'name', 'full_name', 'url', 'description', 'stars', 
                'forks', 'parent_repo', 'created_at', 'updated_at'
            ])
            writer.writeheader()
            
            for repo_data in data['repositories']:
                for fork in repo_data['forks']:
                    writer.writerow({
                        'name': fork['name'],
                        'full_name': fork['full_name'],
                        'url': fork['url'],
                        'description': fork.get('description', ''),
                        'stars': fork.get('stars', 0),
                        'forks': fork.get('forks', 0),
                        'parent_repo': fork.get('parent_repo', ''),
                        'created_at': fork.get('created_at', ''),
                        'updated_at': fork.get('updated_at', '')
                    })
        
        print(f"CSV saved to {original_filepath} and {forks_filepath}")
    
    def save_to_txt(self, data: Dict, filename: str):
        """Save as text: original repos + fork lists."""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"GitHub C/C++ Project List\n")
            f.write(f"Crawl time: {data['crawl_time']}\n")
            f.write(f"Stats: {data['stats']}\n")
            f.write("=" * 80 + "\n\n")
            
            for repo_data in data['repositories']:
                original = repo_data['original_repo']
                
                f.write(f"Original repo:\n")
                f.write(f"  Name: {original['name']}\n")
                f.write(f"  Full name: {original['full_name']}\n")
                f.write(f"  URL: {original['url']}\n")
                f.write(f"  Description: {original.get('description', 'N/A')}\n")
                f.write(f"  Stars: {original.get('stars', 0)}\n")
                f.write(f"  Forks: {original.get('forks', 0)}\n")
                f.write(f"  Language: {original.get('language', 'N/A')}\n")
                f.write(f"\n")
                
                if repo_data['forks']:
                    f.write(f"  Fork list ({len(repo_data['forks'])}):\n")
                    for fork in repo_data['forks']:
                        f.write(f"    - {fork['full_name']}: {fork['url']}\n")
                else:
                    f.write(f"  No forks\n")
                
                f.write("\n" + "-" * 80 + "\n\n")
        
        print(f"Text saved to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="GitHub C/C++ project crawler (multi-threaded)")
    parser.add_argument(
        "--workers",
        type=int,
        default=15,
        help="Number of worker threads (default 15)",
    )
    args = parser.parse_args()
    
    tokens = [
    ]
    
    tokens = [t for t in tokens if t and len(t) > 50 and t.startswith('github_pat_')]
    
    env_token = os.getenv('GITHUB_TOKEN')
    if env_token and env_token not in tokens:
        tokens.append(env_token)
    
    if not tokens:
        print("Error: No GitHub token found")
        return
    
    print(f"Using {len(tokens)} GitHub token(s), will rotate")
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Output dir: {current_dir}")
    
    max_workers = args.workers
    
    crawler = GitHubCrawler(tokens=tokens, output_dir=current_dir, max_workers=max_workers)
    
    languages = ['C', 'C++']
    max_repos_per_lang = None
    max_forks_per_repo = None
    
    print(f"\nCrawling GitHub C/C++ projects ({max_workers} workers)...")
    print(f"Config: languages={languages}, max_repos_per_lang={max_repos_per_lang}, max_forks_per_repo={max_forks_per_repo}\n")
    all_languages_data = {
        'repositories': [],
        'crawl_time': datetime.now().isoformat(),
        'stats': {
            'total_repositories': 0,
            'total_forks': 0,
            'total_api_calls': 0
        }
    }
    
    for language in languages:
        print(f"\n{'='*80}")
        print(f"Crawling {language} projects")
        print(f"{'='*80}\n")
        
        try:
            data = crawler.crawl_projects(
                languages=[language],
                max_repos_per_lang=max_repos_per_lang,
                max_forks_per_repo=max_forks_per_repo
            )
        except KeyboardInterrupt:
            print(f"\n\n{language} crawl interrupted by user, saving collected data...")
            backup_file = os.path.join(crawler.output_dir, 'github_projects_backup.json')
            if os.path.exists(backup_file):
                with open(backup_file, 'r', encoding='utf-8') as f:
                    all_languages_data = json.load(f)
                print(f"Restored from backup: {backup_file}")
            else:
                print("Warning: No backup file found")
            break
        except Exception as e:
            print(f"\n\n{language} crawl error: {e}")
            print("Saving collected data...")
            backup_file = os.path.join(crawler.output_dir, 'github_projects_backup.json')
            if os.path.exists(backup_file):
                with open(backup_file, 'r', encoding='utf-8') as f:
                    all_languages_data = json.load(f)
                print(f"Restored from backup: {backup_file}")
            else:
                print("Warning: No backup file found")
            break
        
        if data and data.get('repositories'):
            all_languages_data['repositories'].extend(data['repositories'])
            stats = data.get('stats', {})
            all_languages_data['stats']['total_repositories'] += stats.get('total_repositories', 0)
            all_languages_data['stats']['total_forks'] += stats.get('total_forks', 0)
            all_languages_data['stats']['total_api_calls'] += stats.get('total_api_calls', 0)
            
            print(f"\n{language} crawl done!")
            print(f"  - Repos: {stats.get('total_repositories', 0)}")
            print(f"  - Forks: {stats.get('total_forks', 0)}")
            print(f"  - API calls: {stats.get('total_api_calls', 0)}")
            
            crawler.save_to_json(all_languages_data, 'github_projects_backup.json')
            print("  Merged backup saved")
        else:
            print(f"\nWarning: No data for {language}")
    
    if all_languages_data and all_languages_data.get('repositories'):
        print(f"\n{'='*80}")
        print("Saving merged data for all languages...")
        crawler.save_to_json(all_languages_data, 'github_projects.json')
        crawler.save_to_csv(all_languages_data, 'github_projects.csv')
        crawler.save_to_txt(all_languages_data, 'github_projects.txt')
        
        print(f"\n{'='*80}")
        print("All languages data saved!")
        print(f"Total repos: {all_languages_data.get('stats', {}).get('total_repositories', 0)}")
        print(f"Total forks: {all_languages_data.get('stats', {}).get('total_forks', 0)}")
        print(f"Total API calls: {all_languages_data.get('stats', {}).get('total_api_calls', 0)}")
        print(f"{'='*80}")
    else:
        print("\nWarning: No data to save")


if __name__ == '__main__':
    main()
