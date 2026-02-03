#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Download PR comments via GitHub API for top 5000 fork stars in unmerged_pr_list.

Read first 5000 PRs from unmerged_pr_list_directory.json (sorted by fork_stars),
fetch each PR's comments via GitHub API, and update the corresponding PR files.
"""

import json
import os
import time
import threading
import argparse
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("Error: requests library required")
    print("Run: pip install requests")
    exit(1)


class GitHubAPIClient:
    """GitHub API client with multi-token rotation."""

    def __init__(self, tokens: List[str], max_workers: int = 15):
        self.tokens = tokens or []
        self.base_url = "https://api.github.com"
        self.max_workers = max_workers

        # Thread locks
        self.token_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        # Track each token's status
        self.token_status = {}
        for i in range(len(self.tokens)):
            self.token_status[i] = {
                'remaining': 5000,
                'reset_time': 0,
                'exhausted': False,
                'last_checked': 0
            }

        # Current token index
        self.current_token_index = 0

        # Stats
        self.stats = {
            'api_calls': 0,
            'errors': 0
        }
    
    def _get_available_token_index(self) -> Optional[int]:
        """Get an available token index (thread-safe)."""
        current_time = time.time()

        with self.token_lock:
            current_idx = self.current_token_index
            current_status = self.token_status.get(current_idx, {})

            if not current_status.get('exhausted', False):
                return current_idx

            if current_status.get('reset_time', 0) <= current_time:
                current_status['exhausted'] = False
                current_status['remaining'] = 5000
                return current_idx

            for offset in range(len(self.tokens)):
                idx = (current_idx + offset) % len(self.tokens)
                status = self.token_status.get(idx, {})

                if not status.get('exhausted', False):
                    self.current_token_index = idx
                    return idx

                if status.get('reset_time', 0) <= current_time:
                    status['exhausted'] = False
                    status['remaining'] = 5000
                    self.current_token_index = idx
                    return idx

            return None
    
    def _wait_for_token_reset(self) -> Optional[int]:
        """Wait for token reset, return first available token index."""
        with self.token_lock:
            current_time = time.time()
            reset_times = [
                (idx, status.get("reset_time", 0))
                for idx, status in self.token_status.items()
                if status.get("reset_time", 0) > current_time
            ]
            if not reset_times:
                for idx, status in self.token_status.items():
                    status["exhausted"] = False
                    status["remaining"] = 5000
                self.current_token_index = 0
                return 0

            earliest_idx, earliest_reset = min(reset_times, key=lambda x: x[1])
            wait_seconds = max(0, earliest_reset - current_time) + 1

        if wait_seconds > 0:
            print(f"All tokens exhausted, waiting {wait_seconds:.0f}s to resume...")
            time.sleep(wait_seconds)

        return self._get_available_token_index()
    
    def _update_token_status(self, token_index: int, remaining: int, reset_time: int):
        """Update token status (thread-safe)."""
        with self.token_lock:
            if token_index in self.token_status:
                self.token_status[token_index]['remaining'] = remaining
                self.token_status[token_index]['reset_time'] = reset_time
                if remaining < 10:
                    self.token_status[token_index]['exhausted'] = True
    
    def _create_session(self, token_index: int) -> requests.Session:
        """Create authenticated session."""
        session = requests.Session()
        if token_index < len(self.tokens):
            session.headers.update({
                "Authorization": f"Bearer {self.tokens[token_index]}",
                "Accept": "application/vnd.github.v3+json"
            })
        return session
    
    def get_pr_comments(self, owner: str, repo: str, pr_number: int) -> Optional[Dict[str, List[Dict]]]:
        """Get PR comments (review comments and issue comments).

        API endpoints:
        - GET /repos/{owner}/{repo}/pulls/{pull_number}/comments (review comments)
        - GET /repos/{owner}/{repo}/issues/{issue_number}/comments (issue comments)

        Args:
            owner: Repo owner
            repo: Repo name
            pr_number: PR number

        Returns:
            Dict[str, List[Dict]]: {"review_comments": [...], "issue_comments": [...]}, or None on failure
        """
        token_index = self._get_available_token_index()
        if token_index is None:
            token_index = self._wait_for_token_reset()
        if token_index is None:
            return None
        
        session = self._create_session(token_index)
        
        def fetch_comments(url: str) -> List[Dict]:
            """Fetch comments of the given type."""
            all_comments = []
            page = 1
            per_page = 100
            current_token_idx = token_index
            current_session = session
            
            while True:
                try:
                    params = {
                        'page': page,
                        'per_page': per_page
                    }
                    resp = current_session.get(url, params=params, timeout=30)
                    
                    with self.stats_lock:
                        self.stats["api_calls"] += 1

                    try:
                        remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
                        reset_time = int(resp.headers.get("X-RateLimit-Reset", 0))
                        if remaining > 0 or reset_time > 0:
                            self._update_token_status(current_token_idx, remaining, reset_time)
                    except Exception:
                        pass
                    
                    if resp.status_code == 200:
                        comments = resp.json()
                        if not comments:
                            break
                        all_comments.extend(comments)

                        if len(comments) < per_page:
                            break

                        page += 1
                        time.sleep(0.1)

                    elif resp.status_code == 404:
                        break
                    elif resp.status_code == 403:
                        with self.stats_lock:
                            self.stats["errors"] += 1
                        with self.token_lock:
                            self.token_status[current_token_idx]['exhausted'] = True
                        current_token_idx = self._get_available_token_index()
                        if current_token_idx is None:
                            current_token_idx = self._wait_for_token_reset()
                        if current_token_idx is None:
                            return []
                        current_session = self._create_session(current_token_idx)
                        continue
                    else:
                        with self.stats_lock:
                            self.stats["errors"] += 1
                        break
                        
                except Exception as e:
                    with self.stats_lock:
                        self.stats["errors"] += 1
                    break
            
            return all_comments

        review_comments_url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}/comments"
        review_comments = fetch_comments(review_comments_url)

        issue_comments_url = f"{self.base_url}/repos/{owner}/{repo}/issues/{pr_number}/comments"
        issue_comments = fetch_comments(issue_comments_url)
        
        return {
            "review_comments": review_comments,
            "issue_comments": issue_comments
        }


def load_pr_list_from_directory_json(directory_json_path: Path, max_count: int = 5000) -> List[Dict]:
    """Load PR list from directory JSON (first max_count entries).

    Returns:
        List[Dict]: PR info list with filename, repo_full_name, pr_number, etc.
    """
    if not directory_json_path.exists():
        print(f"Directory JSON not found: {directory_json_path}")
        return []

    print(f"Loading PR list: {directory_json_path}")
    try:
        with directory_json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        prs_list = data.get("prs", [])
        selected_prs = prs_list[:max_count]

        print(f"Loaded {len(selected_prs):,} PRs (top {max_count} by fork_stars)")
        return selected_prs
    except Exception as e:
        print(f"Failed to load directory JSON: {e}")
        return []


def load_pr_data(pr_file_path: Path) -> Optional[Dict]:
    """Load PR data file."""
    if not pr_file_path.exists():
        return None

    try:
        with pr_file_path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Failed to read PR file {pr_file_path}: {e}")
        return None


def save_pr_data(pr_file_path: Path, pr_data: Dict):
    """Save PR data file."""
    try:
        with pr_file_path.open("w", encoding="utf-8") as f:
            json.dump(pr_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Failed to save PR file {pr_file_path}: {e}")


def download_pr_comments(
    pr_info: Dict,
    pr_list_dir: Path,
    github_client: GitHubAPIClient
) -> Tuple[str, bool, Optional[str]]:
    """Download comments for a single PR.

    Returns:
        Tuple[filename, success, error_message]
    """
    filename = pr_info.get("filename", "")
    repo_full_name = pr_info.get("repo_full_name", "")
    pr_number = pr_info.get("pr_number", 0)

    if not filename or not repo_full_name or not pr_number:
        return filename, False, "Missing required info"

    parts = repo_full_name.split("/", 1)
    if len(parts) != 2:
        return filename, False, f"Invalid repo name: {repo_full_name}"

    owner, repo = parts

    pr_file_path = pr_list_dir / filename
    pr_data = load_pr_data(pr_file_path)
    if not pr_data:
        return filename, False, "Failed to load PR data"

    if "pr_comments" in pr_data and pr_data["pr_comments"] is not None:
        return filename, True, None

    comments_data = github_client.get_pr_comments(owner, repo, pr_number)

    if comments_data is None:
        pr_data["pr_comments"] = {
            "review_comments": [],
            "issue_comments": []
        }
        pr_data["comments_downloaded_at"] = datetime.now().isoformat()
        save_pr_data(pr_file_path, pr_data)
        return filename, True, None
    else:
        pr_data["pr_comments"] = comments_data
        pr_data["comments_downloaded_at"] = datetime.now().isoformat()
        save_pr_data(pr_file_path, pr_data)
        return filename, True, None


def main():
    parser = argparse.ArgumentParser(description="Download PR comments for top 5000 fork stars in unmerged_pr_list")
    parser.add_argument(
        "--pr-list-dir",
        type=str,
        default="unmerged_pr_list",
        help="PR list directory path (default: unmerged_pr_list)"
    )
    parser.add_argument(
        "--directory-json",
        type=str,
        default="unmerged_pr_list_directory.json",
        help="Directory JSON path (default: unmerged_pr_list_directory.json)"
    )
    parser.add_argument(
        "--max-count",
        type=int,
        default=5000,
        help="Max PRs to process (default: 5000)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=15,
        help="Max concurrency (default: 15)"
    )
    parser.add_argument(
        "--tokens",
        type=str,
        nargs="+",
        default=None,
        help="GitHub Personal Access Token list (space-separated)"
    )

    args = parser.parse_args()
    default_tokens = [
    ]
    
    tokens = args.tokens or []
    if not tokens:
        token_env = os.getenv("GITHUB_TOKENS", "")
        if token_env:
            tokens = [t.strip() for t in token_env.split(",") if t.strip()]

    if not tokens:
        tokens = default_tokens
        print(f"Using default tokens ({len(tokens)} total)")

    current_dir = Path(__file__).parent

    directory_json_path = current_dir / args.directory_json
    pr_list = load_pr_list_from_directory_json(directory_json_path, max_count=args.max_count)

    if not pr_list:
        print("No PR list found, exiting")
        return

    github_client = GitHubAPIClient(tokens, max_workers=args.max_workers)

    pr_list_dir = current_dir / args.pr_list_dir
    if not pr_list_dir.exists():
        print(f"PR list directory not found: {pr_list_dir}")
        return

    print(f"\nDownloading PR comments...")
    print(f"  - PR count: {len(pr_list):,}")
    print(f"  - Workers: {args.max_workers}")
    print(f"  - Tokens: {len(tokens)}")
    print()

    completed = 0
    failed = 0
    skipped = 0

    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_to_pr = {
            executor.submit(download_pr_comments, pr_info, pr_list_dir, github_client): pr_info
            for pr_info in pr_list
        }

        for future in as_completed(future_to_pr):
            pr_info = future_to_pr[future]
            try:
                filename, success, error_msg = future.result()
                if success:
                    completed += 1
                    if completed % 100 == 0:
                        print(f"  Processed {completed:,}/{len(pr_list):,} PRs (ok: {completed}, failed: {failed})...")
                else:
                    failed += 1
                    if error_msg:
                        print(f"Failed {filename}: {error_msg}")
            except Exception as e:
                failed += 1
                print(f"Error processing {pr_info.get('filename', 'unknown')}: {e}")

    print(f"\nDone.")
    print(f"  - Success: {completed:,}")
    print(f"  - Failed: {failed:,}")
    print(f"  - API calls: {github_client.stats['api_calls']:,}")
    print(f"  - API errors: {github_client.stats['errors']:,}")


if __name__ == "__main__":
    main()

