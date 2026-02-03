#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extract PRs from repos_prs_v2.json that come from downloaded forks; fetch commits via GitHub API.

Uses clone_report.json (forks with commit_count > 0) to filter PRs by head_repo_full_name,
then fetches commits for each PR via GitHub API.
"""

import argparse
import json
import os
import time
import threading
import signal
from pathlib import Path
from typing import Set, Dict, List, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import ijson  # type: ignore
except Exception:
    ijson = None

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

        self.token_lock = threading.Lock()
        self.stats_lock = threading.Lock()

        self.token_status = {}
        for i in range(len(self.tokens)):
            self.token_status[i] = {
                'remaining': 5000,
                'reset_time': 0,
                'exhausted': False,
                'last_checked': 0
            }

        self.current_token_index = 0
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
        """Wait for token reset; return first available token index."""
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
            print(f"All tokens exhausted; waiting {wait_seconds:.0f}s to recover...")
            start_time = time.time()
            last_print_time = start_time
            print_interval = 10

            while True:
                elapsed = time.time() - start_time
                remaining = wait_seconds - elapsed

                if remaining <= 0:
                    break

                current_time = time.time()
                if (current_time - last_print_time >= print_interval) or (remaining <= 10):
                    mins = int(remaining // 60)
                    secs = int(remaining % 60)
                    if mins > 0:
                        print(f"Remaining wait: {mins}m {secs}s")
                    else:
                        print(f"Remaining wait: {secs}s")
                    last_print_time = current_time
                
                sleep_time = min(1.0, remaining)
                if sleep_time > 0:
                    time.sleep(sleep_time)

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
                        print(f"Wait done: recovered {len(recovered_tokens)} token(s): {recovered_tokens}, switched to token {idx + 1}")
                    else:
                        print(f"Wait done: switched to token {idx + 1}")
                    return idx
            
            self.current_token_index = 0
            if 0 in self.token_status:
                self.token_status[0]["exhausted"] = False
                self.token_status[0]["remaining"] = max(int(self.token_status[0].get("remaining", 0) or 0), 5000)
            print("Wait done: fallback recovery of token 1")
            return 0
    
    def _create_session(self, token_index: int) -> requests.Session:
        """Create session using the given token index."""
        session = requests.Session()
        token = self.tokens[token_index]
        session.headers.update({
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"token {token}"
        })
        return session
    
    def _update_token_status(self, token_index: int, remaining: int, reset_time: int):
        """Update token state from response headers."""
        with self.token_lock:
            self.token_status[token_index] = {
                'remaining': remaining,
                'reset_time': reset_time,
                'exhausted': remaining < 10,
                'last_checked': time.time()
            }
            
            # If current token is low, try switching to next
            if remaining < 10:
                self.token_status[token_index]['exhausted'] = True
                current_time = time.time()
                for offset in range(1, len(self.tokens)):
                    idx = (token_index + offset) % len(self.tokens)
                    status = self.token_status.get(idx, {})
                    
                    if not status.get('exhausted', False):
                        self.current_token_index = idx
                        return
                    
                    if status.get('reset_time', 0) <= current_time:
                        status['exhausted'] = False
                        status['remaining'] = 5000
                        self.current_token_index = idx
                        return
    
    def get_pr_commits(self, owner: str, repo: str, pr_number: int) -> Optional[List[Dict]]:
        """Get commits for a PR. Returns list of commit dicts or None on failure."""
        # Get available token
        token_index = self._get_available_token_index()
        if token_index is None:
            token_index = self._wait_for_token_reset()
        if token_index is None:
            return None
        
        session = self._create_session(token_index)
        url = f"{self.base_url}/repos/{owner}/{repo}/pulls/{pr_number}/commits"
        
        try:
            resp = session.get(url, timeout=30)
            with self.stats_lock:
                self.stats["api_calls"] += 1
            
            try:
                remaining = int(resp.headers.get("X-RateLimit-Remaining", 0))
                reset_time = int(resp.headers.get("X-RateLimit-Reset", 0))
                if remaining > 0 or reset_time > 0:
                    self._update_token_status(token_index, remaining, reset_time)
            except Exception:
                pass
            
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                return None
            elif resp.status_code == 403:
                with self.stats_lock:
                    self.stats["errors"] += 1
                return None
            else:
                with self.stats_lock:
                    self.stats["errors"] += 1
                return None
        except Exception as e:
            with self.stats_lock:
                self.stats["errors"] += 1
            return None
    
    def enrich_pr_with_commits(self, pr: Dict, base_repo_full_name: str) -> Dict:
        """Add commits to PR data. Returns updated PR dict."""
        pr_number = pr.get("number")
        if not isinstance(pr_number, int):
            return pr

        parts = base_repo_full_name.split("/", 1)
        if len(parts) != 2:
            return pr

        owner, repo = parts
        commits = self.get_pr_commits(owner, repo, pr_number)
        if commits is not None:
            pr["commits"] = commits
            pr["commit_count"] = len(commits) if isinstance(commits, list) else None
        else:
            pr["commits"] = None
            pr["commit_count"] = None
        
        return pr


def load_downloaded_forks(clone_report_path: Path) -> Set[str]:
    """Load successfully downloaded repos (commit_count > 0) from clone_report.json. Returns set of repo names."""
    if not clone_report_path.exists():
        print(f"Warning: clone_report.json not found: {clone_report_path}")
        return set()

    downloaded_repos = set()
    try:
        with clone_report_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        for item in data.get("success_repos", []):
            repo_name = item.get("repo_name")
            commit_count = item.get("commit_count", None)

            if repo_name and commit_count is not None and commit_count > 0:
                downloaded_repos.add(repo_name)

        print(f"Loaded {len(downloaded_repos)} downloaded repos (commit_count > 0) from clone_report.json")
    except Exception as e:
        print(f"Warning: failed to read clone_report.json: {e}")

    return downloaded_repos


def enrich_existing_file_with_commits(
    existing_file_path: Path,
    github_client: GitHubAPIClient
) -> None:
    """Enrich existing JSON file with commits for each PR."""
    if not existing_file_path.exists():
        print(f"File not found: {existing_file_path}")
        return

    print(f"Reading existing file: {existing_file_path}")

    try:
        with existing_file_path.open("r", encoding="utf-8") as f:
            output_data = json.load(f)
    except Exception as e:
        print(f"Failed to read file: {e}")
        return
    
    repositories = output_data.get("repositories", [])
    if not repositories:
        print("Warning: no repository data in file")
        return

    total_prs = 0
    prs_need_commits = 0
    prs_with_commits = 0
    
    for repo_data in repositories:
        prs = repo_data.get("prs", [])
        total_prs += len(prs)
        for pr in prs:
            if pr.get("commits") is None or pr.get("commits") == []:
                prs_need_commits += 1
            else:
                prs_with_commits += 1
    
    print(f"Stats: total PRs {total_prs}, with commits {prs_with_commits}, need enrich {prs_need_commits}")

    if prs_need_commits == 0:
        print("All PRs already have commits; nothing to do")
        return

    print(f"\nEnriching commits for {prs_need_commits} PRs...")

    stats_lock = threading.Lock()
    prs_with_commits_after = prs_with_commits
    prs_without_commits_after = 0

    def enrich_repo_worker(repo_data: Dict) -> tuple:
        repo_full_name = repo_data["full_name"]
        prs = repo_data["prs"]
        
        enriched_prs = []
        local_with_commits = 0
        local_without_commits = 0
        
        for pr in prs:
            if pr.get("commits") is not None:
                enriched_prs.append(pr)
                local_with_commits += 1
            else:
                enriched_pr = github_client.enrich_pr_with_commits(pr.copy(), repo_full_name)
                if enriched_pr.get("commits") is not None:
                    local_with_commits += 1
                else:
                    local_without_commits += 1
                enriched_prs.append(enriched_pr)
        
        return {
            "full_name": repo_full_name,
            "prs": enriched_prs
        }, local_with_commits, local_without_commits
    
    with ThreadPoolExecutor(max_workers=github_client.max_workers) as executor:
        futures = {
            executor.submit(enrich_repo_worker, repo_data): repo_data
            for repo_data in repositories
        }
        
        completed = 0
        enriched_repos = []
        for future in as_completed(futures):
            completed += 1
            try:
                enriched_repo, local_with_commits, local_without_commits = future.result()
                enriched_repos.append(enriched_repo)
                
                with stats_lock:
                    prs_with_commits_after += local_with_commits
                    prs_without_commits_after += local_without_commits
                
                if completed % 10 == 0:
                    print(f"Progress: {completed}/{len(repositories)} repos processed")
            except Exception as e:
                repo_data = futures[future]
                print(f"Warning: error processing repo {repo_data['full_name']}: {e}")
                enriched_repos.append(repo_data)

    output_data["repositories"] = enriched_repos
    output_data["stats"]["prs_with_commits"] = prs_with_commits_after
    output_data["stats"]["prs_without_commits"] = prs_without_commits_after

    temp_file = existing_file_path.with_suffix('.tmp')
    with temp_file.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    temp_file.replace(existing_file_path)

    print(f"\nCommits enrichment done.")
    print(f"  - PRs with commits: {prs_with_commits_after}")
    print(f"  - PRs without commits: {prs_without_commits_after}")
    print(f"  - API calls: {github_client.stats['api_calls']}")
    print(f"  - API errors: {github_client.stats['errors']}")
    print(f"  - File updated: {existing_file_path}")


def extract_prs_from_forks(
    prs_json_path: Path,
    downloaded_forks: Set[str],
    output_path: Path,
    github_client: Optional[GitHubAPIClient] = None,
    fetch_commits: bool = False,
    repos_prs_shards_dir: Optional[Path] = None
) -> None:
    """Extract PRs from repos_prs_v2.json that come from downloaded forks; optionally fetch commits via API."""
    processed_repos_from_checkpoint = set()
    if output_path.exists():
        print(f"Existing output file found: {output_path}")
        try:
            with output_path.open("r", encoding="utf-8") as f:
                existing_data = json.load(f)
            
            for repo_data in existing_data.get("repositories", []):
                repo_name = repo_data.get("full_name")
                if repo_name:
                    processed_repos_from_checkpoint.add(repo_name)
            
            if fetch_commits and github_client:
                print("Checking if commits need to be enriched...")
                has_missing_commits = False
                for repo_data in existing_data.get("repositories", []):
                    for pr in repo_data.get("prs", []):
                        if pr.get("commits") is None:
                            has_missing_commits = True
                            break
                    if has_missing_commits:
                        break
                
                if has_missing_commits:
                    print("Existing file missing commits; enriching...")
                    enrich_existing_file_with_commits(output_path, github_client)
                    return
                else:
                    print(f"Existing file has full data for {len(processed_repos_from_checkpoint)} repos")
        except Exception as e:
            print(f"Warning: error checking existing file: {e}; will re-extract")
    
    if not prs_json_path.exists():
        print(f"❌ PR data file not found: {prs_json_path}")
        return
    
    if not downloaded_forks:
        print("⚠️ No downloaded forks; cannot extract PRs")
        return
    
    if fetch_commits and github_client is None:
        print("❌ fetch_commits=True but github_client not provided")
        return
    
    repos_in_shards = set()
    if repos_prs_shards_dir and repos_prs_shards_dir.exists():
        for p in repos_prs_shards_dir.glob("*.jsonl"):
            name = p.stem.replace("__", "/")
            repos_in_shards.add(name)
        print(f"Loaded {len(repos_in_shards)} repos from REPOS_PRS_SHARDS_DIR")
        print(f"   Will keep only PRs for these repos (head_repo in downloaded forks)")
    else:
        if repos_prs_shards_dir:
            print(f"⚠️ REPOS_PRS_SHARDS_DIR does not exist: {repos_prs_shards_dir}")
        print(f"❌ Valid REPOS_PRS_SHARDS_DIR required; exiting")
        return
    
    print(f"Extracting PRs from {prs_json_path} (source forks: {len(downloaded_forks)})...")
    if fetch_commits:
        print("Will fetch commits via API for each PR; this may take a while")
    
    output_data = {
        "crawl_time": None,
        "repositories": [],
        "stats": {
            "total_repositories": 0,
            "total_prs": 0,
            "filtered_repos": 0,
            "filtered_prs": 0,
            "prs_with_commits": 0,
            "prs_without_commits": 0
        }
    }
    
    filtered_repos_count = 0
    filtered_prs_count = 0
    total_repos_count = 0
    total_prs_count = 0
    prs_with_commits = 0
    prs_without_commits = 0
    
    filtered_repos = []

    if ijson:
        print("Using streaming parse (ijson)...")
        try:
            with prs_json_path.open("rb") as f:
                output_data["crawl_time"] = next(ijson.items(f, "crawl_time"), None)
        except Exception:
            pass

        with prs_json_path.open("rb") as f:
            for repo_data in ijson.items(f, "repositories.item"):
                total_repos_count += 1
                repo_full_name = repo_data.get("full_name")
                prs = repo_data.get("prs", [])
                
                if not repo_full_name or not prs:
                    continue

                filtered_prs = []
                for pr in prs:
                    total_prs_count += 1
                    head_repo = pr.get("head_repo_full_name")

                    if not head_repo or head_repo not in downloaded_forks:
                        continue

                    if repo_full_name not in repos_in_shards:
                        continue
                    
                    filtered_prs.append(pr)
                    filtered_prs_count += 1
                
                # Only save repos that have matching PRs
                if filtered_prs:
                    filtered_repos.append({
                        "full_name": repo_full_name,
                        "prs": filtered_prs
                    })
                    filtered_repos_count += 1
    else:
        print("Using json.load (may use a lot of memory)...")
        with prs_json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        
        output_data["crawl_time"] = data.get("crawl_time")
        
        for repo_data in data.get("repositories", []):
            total_repos_count += 1
            repo_full_name = repo_data.get("full_name")
            prs = repo_data.get("prs", [])
            
            if not repo_full_name or not prs:
                continue
            
                filtered_prs = []
                for pr in prs:
                    total_prs_count += 1
                    head_repo = pr.get("head_repo_full_name")

                    if not head_repo or head_repo not in downloaded_forks:
                        continue

                    if repo_full_name not in repos_in_shards:
                        continue
                
                filtered_prs.append(pr)
                filtered_prs_count += 1
            
            # Only save repos that have matching PRs
            if filtered_prs:
                filtered_repos.append({
                    "full_name": repo_full_name,
                    "prs": filtered_prs
                })
                filtered_repos_count += 1
    
    print(f"\nPR extraction done.")
    print(f"  - Total repos: {total_repos_count}")
    print(f"  - Total PRs: {total_prs_count}")
    print(f"  - Filtered repos: {filtered_repos_count}")
    print(f"  - Filtered PRs: {filtered_prs_count}")
    
    if fetch_commits and github_client:
        if processed_repos_from_checkpoint and output_path.exists():
            try:
                with output_path.open("r", encoding="utf-8") as f:
                    checkpoint_data = json.load(f)
                output_data["repositories"] = checkpoint_data.get("repositories", [])
                output_data["crawl_time"] = checkpoint_data.get("crawl_time")
                print(f"Resumed from checkpoint: {len(output_data['repositories'])} repos processed")
            except Exception as e:
                print(f"Warning: failed to load checkpoint: {e}; will start fresh")
                output_data["repositories"] = []
        
        repos_to_process = [
            repo_data for repo_data in filtered_repos
            if repo_data.get("full_name") not in processed_repos_from_checkpoint
        ]
        
        if not repos_to_process:
            print("All repos already processed.")
        else:
            print(f"\nFetching PR commits via GitHub API...")
            print(f"  - Repos to process: {len(repos_to_process)}")
            print(f"  - Already processed: {len(processed_repos_from_checkpoint)}")
        
        stats_lock = threading.Lock()
        
        def enrich_pr_worker(repo_data: Dict) -> tuple:
            repo_full_name = repo_data["full_name"]
            prs = repo_data["prs"]
            
            enriched_prs = []
            local_with_commits = 0
            local_without_commits = 0
            
            for pr in prs:
                enriched_pr = github_client.enrich_pr_with_commits(pr.copy(), repo_full_name)
                if enriched_pr.get("commits") is not None:
                    local_with_commits += 1
                else:
                    local_without_commits += 1
                enriched_prs.append(enriched_pr)
            
            return {
                "full_name": repo_full_name,
                "prs": enriched_prs
            }, local_with_commits, local_without_commits
        
        last_save_time = time.time()
        save_interval = 60

        def save_checkpoint():
            """Save checkpoint (processed data)."""
            try:
                temp_file = output_path.with_suffix('.tmp')
                checkpoint_data = {
                    "crawl_time": output_data.get("crawl_time"),
                    "repositories": output_data["repositories"],
                    "stats": {
                        "total_repositories": total_repos_count,
                        "total_prs": total_prs_count,
                        "filtered_repos": filtered_repos_count,
                        "filtered_prs": filtered_prs_count,
                        "prs_with_commits": prs_with_commits,
                        "prs_without_commits": prs_without_commits,
                        "processed_repos": len(output_data["repositories"]),
                        "last_update": datetime.now().isoformat()
                    }
                }
                with temp_file.open("w", encoding="utf-8") as f:
                    json.dump(checkpoint_data, f, ensure_ascii=False, indent=2)
                temp_file.replace(output_path)
                print(f"Checkpoint saved: {len(output_data['repositories'])} repos processed")
            except Exception as e:
                print(f"Warning: failed to save checkpoint: {e}")
        
        interrupted = False
        
        def signal_handler(signum, frame):
            """Handle interrupt signal."""
            nonlocal interrupted
            interrupted = True
            print("\n\nInterrupt detected; saving processed data...")
            save_checkpoint()
            print("Checkpoint saved; you can resume later")
        
        # Register signal handler
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        
        if repos_to_process:
            try:
                with ThreadPoolExecutor(max_workers=github_client.max_workers) as executor:
                    futures = {
                        executor.submit(enrich_pr_worker, repo_data): repo_data
                        for repo_data in repos_to_process
                    }
                    
                    completed = 0
                    for future in as_completed(futures):
                        if interrupted:
                            break
                        
                        completed += 1
                        try:
                            enriched_repo, local_with_commits, local_without_commits = future.result()
                            output_data["repositories"].append(enriched_repo)
                            
                            with stats_lock:
                                prs_with_commits += local_with_commits
                                prs_without_commits += local_without_commits
                            
                            current_time = time.time()
                            if current_time - last_save_time >= save_interval:
                                save_checkpoint()
                                last_save_time = current_time
                            
                            if completed % 10 == 0:
                                print(f"Progress: {completed}/{len(repos_to_process)} repos ({len(output_data['repositories'])} total)")
                        except Exception as e:
                            repo_data = futures[future]
                            print(f"Warning: error processing repo {repo_data['full_name']}: {e}")
                            output_data["repositories"].append(repo_data)
            except KeyboardInterrupt:
                interrupted = True
                print("\n\nKeyboard interrupt; saving processed data...")
                save_checkpoint()
                print("Checkpoint saved; you can resume later")
                raise
            finally:
                # Final save
                if interrupted or len(output_data["repositories"]) > 0:
                    save_checkpoint()
        
        print(f"\nCommits fetch done.")
        print(f"  - PRs with commits: {prs_with_commits}")
        print(f"  - PRs without commits: {prs_without_commits}")
        print(f"  - API calls: {github_client.stats['api_calls']}")
        print(f"  - API errors: {github_client.stats['errors']}")
    else:
        # No commits fetch; use filtered data as-is
        output_data["repositories"] = filtered_repos
    
    # Update stats
    output_data["stats"] = {
        "total_repositories": total_repos_count,
        "total_prs": total_prs_count,
        "filtered_repos": filtered_repos_count,
        "filtered_prs": filtered_prs_count,
        "prs_with_commits": prs_with_commits,
        "prs_without_commits": prs_without_commits
    }
    
    # Save output
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(output_data, f, ensure_ascii=False, indent=2)
    
    print(f"\nDone. Output: {output_path}")


def load_tokens_from_crawl_script(crawl_script_path: Path = None) -> List[str]:
    """Read tokens from crawl_repos_PRs.py. Returns list of token strings."""
    if crawl_script_path is None:
        crawl_script_path = Path(__file__).parent / "crawl_repos_PRs.py"
    
    if not crawl_script_path.exists():
        return []
    
    try:
        with crawl_script_path.open("r", encoding="utf-8") as f:
            content = f.read()
        
        import re
        pattern = r'tokens\s*=\s*\[(.*?)\]'
        match = re.search(pattern, content, re.DOTALL)
        if match:
            tokens_str = match.group(1)
            token_pattern = r"['\"]([^'\"]+)['\"]"
            tokens = re.findall(token_pattern, tokens_str)
            tokens = [t for t in tokens if t and len(t) > 50 and t.startswith('github_pat_')]
            return tokens
    except Exception as e:
        print(f"Warning: failed to read tokens from crawl_repos_PRs.py: {e}")
    
    return []


def main():
    parser = argparse.ArgumentParser(
        description="Extract PRs from repos_prs_v2.json (from downloaded forks); optionally fetch commits."
    )
    parser.add_argument(
        "--clone-report",
        type=Path,
        default=Path("cloned_repos/clone_report.json"),
        help="Path to clone_report.json (default: cloned_repos/clone_report.json)"
    )
    parser.add_argument(
        "--prs-json",
        type=Path,
        default=Path("repos_prs_v2.json"),
        help="Path to PR data JSON (default: repos_prs_v2.json)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("repos_prs_from_downloaded_forks.json"),
        help="Output JSON path (default: repos_prs_from_downloaded_forks.json)"
    )
    parser.add_argument(
        "--fetch-commits",
        action="store_true",
        default=True,
        help="Fetch commits via GitHub API for each PR (default: on)"
    )
    parser.add_argument(
        "--no-fetch-commits",
        dest="fetch_commits",
        action="store_false",
        help="Do not fetch PR commits (disable default)"
    )
    parser.add_argument(
        "--tokens",
        type=str,
        default=None,
        help="GitHub tokens (comma-separated). Or GITHUB_TOKEN/GITHUB_TOKENS, or read from crawl_repos_PRs.py"
    )
    parser.add_argument(
        "--crawl-script",
        type=Path,
        default=None,
        help="Path to crawl_repos_PRs.py for reading tokens (default: current dir)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=15,
        help="Concurrent threads for fetching commits (default: 15)"
    )
    parser.add_argument(
        "--repos-prs-shards-dir",
        type=Path,
        default=Path("repos_prs_shards"),
        help="REPOS_PRS_SHARDS_DIR path; only PRs for repos in this dir (default: repos_prs_shards)"
    )
    
    args = parser.parse_args()
    
    downloaded_forks = load_downloaded_forks(args.clone_report)
    
    if not downloaded_forks:
        print("No downloaded repos (success_repos); exiting")
        return
    
    github_client = None
    if args.fetch_commits:
        tokens = []
        if args.tokens:
            tokens = [t.strip() for t in args.tokens.split(",") if t.strip()]
        # Default tokens (same as crawl_repos_PRs.py)
        else:
            tokens = [
            ]
            tokens = [t for t in tokens if t and len(t) > 50 and t.startswith('github_pat_')]
            print(f"Using default tokens ({len(tokens)})")
        
        if not tokens:
            print("No valid GitHub token found")
            return
        
        print(f"Using {len(tokens)} GitHub token(s)")
        github_client = GitHubAPIClient(tokens, max_workers=args.workers)
    
    # Extract PRs and optionally fetch commits
    extract_prs_from_forks(
        args.prs_json,
        downloaded_forks,
        args.output,
        github_client=github_client,
        fetch_commits=args.fetch_commits,
        repos_prs_shards_dir=args.repos_prs_shards_dir
    )
    
    print("\nDone.")


if __name__ == "__main__":
    main()
