#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Reproduce old/final_analyzer(1).py stats logic from local JSON files (replacing MongoDB).

Input:
- github_projects_filtered_stars_10.json   (origin + forks + stars)
- repos_prs.json                          (PR data; try to extract External Repo)
- commits_trees.json                      (from generate_commits_tree.py; for developers set)
- cloned_repos/clone_report.json          (optional: filter successfully cloned repos)

Output:
- analysis_results.csv

Field mapping (vs old version):
- fork_set: set of repo_full_name for forks with stars>=10 for this origin
- developers: repos that appear in "fork-only commits" from commits_trees.json (repos with unique commits in this family)
- contributions_*: External Repo extracted from PR data (prefer head.repo.full_name)

Note: repos_prs.json may not include fork repo (External Repo) info; then contributions_* may be low or 0.
"""

from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    import ijson  # type: ignore
except Exception:
    ijson = None


CURRENT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
PROJECTS_JSON = CURRENT_DIR / "github_projects_filtered_stars_10.json"
PROJECTS_JSON_ALL = CURRENT_DIR / "github_projects.json"  # full set (Fork Count = total forks)
REPOS_PRS_JSON = CURRENT_DIR / "repos_prs.json"
COMMITS_SHARDS_DIR = CURRENT_DIR / "commits_trees_shards"  # from shard_commits_trees_to_jsonl.py
PR_COMMIT_METRICS_SHARDS_DIR = CURRENT_DIR / "pr_commit_metrics_shards"  # from build_pr_commit_metrics_from_local_git.py
CLONE_REPORT_JSON = CURRENT_DIR / "cloned_repos" / "clone_report.json"

OUTPUT_CSV = CURRENT_DIR / "analysis_results.csv"
FORK_STARS_THRESHOLD = 10


CSV_HEADER = [
    "Original Repo ID",
    "Average Fork Lifespan (days)",
    "Fork Count",
    "Forks >= 10",
    "Contributions >= 10",
    "Contributions < 10",
    "Developers",
    "Common Contributions",
    "Forks - Contributions >= 10 - Developers",
    "PR Count",
    "PR Commits Count",
    "Stars >= 10 PRs Count",
    "Stars >= 10 and Unique Commits PRs Count",
    "Stars >= 10 PRs Commits Count",
    "Stars >= 10 and Unique Commits PRs Commits Count",
    "Accepted PR avg lifetime per origin",
    "Stars >= 10 and Unique Commits PR Accepted PR avg lifetime per origin",
    "Forks Unique Commits Count + PR Commits Count",
]


def load_successful_repos(report_path: Path) -> Set[str]:
    """Load set of successfully cloned repos from clone_report.json (optional)."""
    if not report_path.exists():
        return set()
    try:
        data = json.load(report_path.open("r", encoding="utf-8"))
        out = set()
        for item in data.get("success_repos", []) or []:
            name = item.get("repo_name")
            if not name:
                continue
            cc = item.get("commit_count", None)
            if cc == 0:
                continue
            out.add(name)
        return out
    except Exception:
        return set()


def _parse_iso_date(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    ss = str(s).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(ss)
    except Exception:
        return None


def load_projects(projects_json: Path) -> Tuple[List[str], Dict[str, Set[str]], Dict[str, int], Dict[str, float]]:
    """Load projects JSON; return:
    - list of origins
    - origin_to_forkset (forks with stars>=10)
    - origin_to_fork_count (total GitHub forks from original_repo["forks"])
    - origin_to_avg_fork_lifespan_days (avg lifespan of stars>=10 forks = updated_at - created_at)
    """
    data = json.load(projects_json.open("r", encoding="utf-8"))
    origins: List[str] = []
    origin_to_forkset: Dict[str, Set[str]] = {}
    origin_to_fork_count: Dict[str, int] = {}
    origin_to_avg_fork_lifespan_days: Dict[str, float] = {}

    for repo_data in data.get("repositories", []) or []:
        original = repo_data.get("original_repo") or {}
        origin = original.get("full_name")
        if not origin:
            continue
        origins.append(origin)

        try:
            origin_to_fork_count[origin] = int(original.get("forks", 0) or 0)
        except Exception:
            origin_to_fork_count[origin] = 0

        forkset: Set[str] = set()
        lifespans: List[float] = []
        for fork in repo_data.get("forks", []) or []:
            fn = fork.get("full_name")
            stars = fork.get("stars", 0) or 0
            if fn and stars >= FORK_STARS_THRESHOLD:
                forkset.add(fn)
                created = _parse_iso_date(fork.get("created_at"))
                updated = _parse_iso_date(fork.get("updated_at"))
                if created and updated and updated >= created:
                    lifespans.append((updated - created).total_seconds() / 86400)
        origin_to_forkset[origin] = forkset
        origin_to_avg_fork_lifespan_days[origin] = round(sum(lifespans) / len(lifespans), 2) if lifespans else 0.0

    return origins, origin_to_forkset, origin_to_fork_count, origin_to_avg_fork_lifespan_days


def load_all_fork_counts(projects_json_all: Path) -> Dict[str, int]:
    """Get total fork count per origin from github_projects.json (prefer forks list length)."""
    out: Dict[str, int] = {}
    if not projects_json_all.exists():
        return out

    def iter_repos():
        if ijson is None:
            data = json.load(projects_json_all.open("r", encoding="utf-8"))
            for repo_data in data.get("repositories", []) or []:
                yield repo_data
        else:
            with projects_json_all.open("rb") as f:
                for repo_data in ijson.items(f, "repositories.item"):
                    yield repo_data

    for repo_data in iter_repos():
        original = repo_data.get("original_repo") or {}
        origin = original.get("full_name")
        if not origin:
            continue
        forks_list = repo_data.get("forks")
        if isinstance(forks_list, list):
            out[origin] = len(forks_list)
        else:
            try:
                out[origin] = int(original.get("forks", 0) or 0)
            except Exception:
                out[origin] = 0
    return out


def repo_to_shard_path(shards_dir: Path, repo_full_name: str) -> Path:
    return shards_dir / (repo_full_name.replace("/", "__") + ".jsonl")


def iter_commit_hashes_from_shard(shards_dir: Path, repo_full_name: str):
    """Read repo shard line by line, yield commit hash. Supports minimal/full line formats."""
    p = repo_to_shard_path(shards_dir, repo_full_name)
    if not p.exists():
        return
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            h = obj.get("hash")
            if isinstance(h, str) and h:
                yield h


def load_commit_set_from_shard(shards_dir: Path, repo_full_name: str) -> Set[str]:
    """Load all commit hashes for a repo into a set (for origin)."""
    s: Set[str] = set()
    for h in iter_commit_hashes_from_shard(shards_dir, repo_full_name) or []:
        s.add(h)
    return s


def extract_external_repo_from_pr(pr: dict) -> Optional[str]:
    """Try to extract External Repo (fork repo) from PR structure.

    Common (GitHub API): pr["head"]["repo"]["full_name"] == "owner/repo"
    """
    head = pr.get("head")
    if isinstance(head, dict):
        repo = head.get("repo")
        if isinstance(repo, dict):
            full_name = repo.get("full_name")
            if isinstance(full_name, str) and full_name:
                return full_name
    v2 = pr.get("head_repo_full_name")
    if isinstance(v2, str) and v2:
        return v2
    for k in ("external_repo", "external_repo_full_name", "from_repo", "fork_full_name"):
        v = pr.get(k)
        if isinstance(v, str) and v:
            return v
    return None


def get_pr_commit_count(pr: dict) -> Optional[int]:
    """Extract commit count from PR data. Prefer commit_count field, else len(commits).

    Returns:
        int: commit count, or None if unavailable
    """
    commit_count = pr.get("commit_count")
    if isinstance(commit_count, int) and commit_count >= 0:
        return commit_count
    commits = pr.get("commits")
    if isinstance(commits, list):
        return len(commits)
    if commits is None:
        return 0
    return None


def extract_pr_commit_shas(pr: dict) -> Set[str]:
    """Extract all commit SHAs from PR data (deduplicated). Supports GitHub API, simplified, and commit_analyses structures.

    Returns:
        Set[str]: deduplicated commit SHAs
    """
    commit_shas: Set[str] = set()
    commits = pr.get("commits")
    if isinstance(commits, list):
        for commit in commits:
            if isinstance(commit, dict):
                commit_sha = commit.get("sha")
                if not commit_sha:
                    commit_obj = commit.get("commit")
                    if isinstance(commit_obj, dict):
                        commit_sha = commit_obj.get("sha")
                if isinstance(commit_sha, str) and commit_sha:
                    commit_shas.add(commit_sha)
            elif isinstance(commit, str):
                if commit:
                    commit_shas.add(commit)
    commit_analyses = pr.get("commit_analyses")
    if isinstance(commit_analyses, list):
        for commit_analysis in commit_analyses:
            if isinstance(commit_analysis, dict):
                commit_hash = commit_analysis.get("commit_hash") or commit_analysis.get("sha")
                if isinstance(commit_hash, str) and commit_hash:
                    commit_shas.add(commit_hash)
    
    return commit_shas


def load_prs(repos_prs_json: Path) -> Dict[str, List[dict]]:
    """Return {origin_repo_full_name: [pr, ...]}. Uses streaming parse to avoid OOM on large files."""
    if not repos_prs_json.exists():
        return {}
    out: Dict[str, List[dict]] = {}

    def iter_repos():
        if ijson is None:
            try:
                data = json.load(repos_prs_json.open("r", encoding="utf-8"))
                for repo in data.get("repositories", []) or []:
                    yield repo
            except MemoryError:
                print(f"Out of memory loading {repos_prs_json}; install ijson for streaming: pip install ijson")
                return
        else:
            with repos_prs_json.open("rb") as f:
                try:
                    for repo in ijson.items(f, "repositories.item"):
                        yield repo
                except Exception as e:
                    print(f"ijson parse failed: {e}, falling back to standard JSON")
                    try:
                        data = json.load(repos_prs_json.open("r", encoding="utf-8"))
                        for repo in data.get("repositories", []) or []:
                            yield repo
                    except MemoryError:
                        print(f"Out of memory loading {repos_prs_json}")
                        return

    repo_count = 0
    for repo in iter_repos():
        repo_count += 1
        full_name = repo.get("full_name")
        prs = repo.get("prs", []) or []
        if full_name and prs:
            out[full_name] = prs
        if repo_count % 100 == 0:
            print(f"  Processed {repo_count:,} repos...")
    print(f"Loaded PR data for {len(out):,} repos from {repos_prs_json} (processed {repo_count:,} repos)")
    return out


def load_pr_commit_metrics_for_repo(repo_full_name: str) -> Dict[int, int]:
    """Load PR commit_count map for one origin: {pr_number: commit_count}"""
    p = PR_COMMIT_METRICS_SHARDS_DIR / (repo_full_name.replace("/", "__") + ".jsonl")
    if not p.exists():
        return {}
    out: Dict[int, int] = {}
    try:
        with p.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                n = obj.get("number")
                c = obj.get("commit_count")
                if isinstance(n, int) and isinstance(c, int) and c >= 0:
                    out[n] = c
    except Exception:
        return {}
    return out


def compute_family_developers_from_shards(
    origin: str,
    forks: Set[str],
    shards_dir: Path,
) -> Set[str]:
    """Use shards to determine developers (same as old "fork-only commits"): a fork is a developer if it has at least one commit hash not in origin's commit set."""
    origin_commits = load_commit_set_from_shard(shards_dir, origin)
    if not origin_commits:
        return set()

    developers: Set[str] = set()
    for fk in forks:
        found = False
        p = repo_to_shard_path(shards_dir, fk)
        if not p.exists():
            continue
        try:
            with p.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except Exception:
                        continue
                    h = obj.get("hash")
                    if isinstance(h, str) and h and h not in origin_commits:
                        found = True
                        break
        except Exception:
            continue

        if found:
            developers.add(fk)

    return developers


def compute_forks_unique_commits_count(
    origin: str,
    forks: Set[str],
    shards_dir: Path,
) -> int:
    """Count unique commits: commits in forks but not in origin (deduplicated).

    Args:
        origin: origin repo full_name
        forks: fork set (stars >= 10)
        shards_dir: commits shards directory

    Returns:
        len(forks commits union - origin commits)
    """
    all_forks_commits: Set[str] = set()
    for fk in forks:
        fork_commits = load_commit_set_from_shard(shards_dir, fk)
        if fork_commits:
            all_forks_commits.update(fork_commits)
    
    if not all_forks_commits:
        return 0
    origin_commits = load_commit_set_from_shard(shards_dir, origin)
    if not origin_commits:
        return len(all_forks_commits)
    unique_commits = all_forks_commits - origin_commits
    return len(unique_commits)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Final analyzer based on JSON files")
    parser.add_argument(
        "--prs-json",
        type=str,
        default=None,
        help=f"PR data JSON path (default: prefer repos_prs_from_downloaded_forks.json, then repos_prs_v2.json, then {REPOS_PRS_JSON})",
    )
    args = parser.parse_args()

    if not PROJECTS_JSON.exists():
        print(f"Not found: {PROJECTS_JSON}")
        return

    if args.prs_json:
        repos_prs_json = Path(args.prs_json)
    else:
        repos_prs_from_downloaded_forks = CURRENT_DIR / "repos_prs_from_downloaded_forks.json"
        repos_prs_v2 = CURRENT_DIR / "repos_prs_v2.json"
        if repos_prs_from_downloaded_forks.exists():
            repos_prs_json = repos_prs_from_downloaded_forks
            print("Using repos_prs_from_downloaded_forks.json as PR source (includes commits)")
        elif repos_prs_v2.exists():
            repos_prs_json = repos_prs_v2
            print("Using repos_prs_v2.json as PR source (includes head_repo_full_name)")
        else:
            repos_prs_json = REPOS_PRS_JSON

    if not repos_prs_json.exists():
        print(f"PR data file not found: {repos_prs_json}")
        return
    print(f"Using PR data: {repos_prs_json}")

    successful_repos = load_successful_repos(CLONE_REPORT_JSON)
    origins, origin_to_forkset, origin_to_fork_count, origin_to_avg_lifespan = load_projects(PROJECTS_JSON)
    all_fork_counts = load_all_fork_counts(PROJECTS_JSON_ALL)
    if all_fork_counts:
        for k, v in all_fork_counts.items():
            origin_to_fork_count[k] = v
    prs_by_repo = load_prs(repos_prs_json)
    if not COMMITS_SHARDS_DIR.exists():
        print(f"Shards dir not found: {COMMITS_SHARDS_DIR} (run shard_commits_trees_to_jsonl.py first)")

    if successful_repos:
        origins = [o for o in origins if o in successful_repos]

    rows: List[List] = []
    for origin in origins:
        fork_set = origin_to_forkset.get(origin, set())
        if successful_repos:
            fork_set = {f for f in fork_set if f in successful_repos}
        developers = compute_family_developers_from_shards(origin, fork_set, COMMITS_SHARDS_DIR)
        forks_unique_commits_count = compute_forks_unique_commits_count(
            origin, fork_set, COMMITS_SHARDS_DIR
        )
        contributions_gte_10: Set[str] = set()
        contributions_lt_10: Set[str] = set()
        pr_count: Set[int] = set()
        pr_commits_set: Set[str] = set()
        stars_gte_10_prs_count: Set[int] = set()
        stars_gte_10_unique_commits_prs_count: Set[int] = set()
        stars_gte_10_prs_commits_set: Set[str] = set()
        stars_gte_10_unique_commits_prs_commits_set: Set[str] = set()
        accepted_pr_lifetimes: List[float] = []
        stars_gte_10_unique_commits_accepted_pr_lifetimes: List[float] = []
        prs_with_ext = 0
        prs_ext_in_fork_set = 0
        prs_ext_not_in_fork_set = 0
        prs_no_ext = 0

        for pr in prs_by_repo.get(origin, []) or []:
            pr_num = pr.get("number")
            if isinstance(pr_num, int):
                pr_count.add(pr_num)
            pr_commit_shas = extract_pr_commit_shas(pr)
            if pr_commit_shas:
                pr_commits_set.update(pr_commit_shas)
            ext = extract_external_repo_from_pr(pr)
            if not ext:
                prs_no_ext += 1
                continue
            prs_with_ext += 1
            if ext == origin:
                prs_ext_not_in_fork_set += 1
                continue
            state = pr.get("state", "").lower()
            merged = pr.get("merged", False)
            merged_at = pr.get("merged_at")
            pr_lifetime_days = None
            if state == "closed" and (merged or merged_at):
                created_at = _parse_iso_date(pr.get("created_at"))
                merged_at_date = _parse_iso_date(merged_at)
                if created_at and merged_at_date and merged_at_date >= created_at:
                    pr_lifetime_days = (merged_at_date - created_at).total_seconds() / 86400
                    accepted_pr_lifetimes.append(pr_lifetime_days)

            if ext in fork_set:
                contributions_gte_10.add(ext)
                if isinstance(pr_num, int):
                    stars_gte_10_prs_count.add(pr_num)
                    if ext in developers:
                        stars_gte_10_unique_commits_prs_count.add(pr_num)
                if pr_commit_shas:
                    stars_gte_10_prs_commits_set.update(pr_commit_shas)
                    if ext in developers:
                        stars_gte_10_unique_commits_prs_commits_set.update(pr_commit_shas)
                if ext in developers and pr_lifetime_days is not None:
                    stars_gte_10_unique_commits_accepted_pr_lifetimes.append(pr_lifetime_days)
                prs_ext_in_fork_set += 1
            else:
                contributions_lt_10.add(ext)
                prs_ext_not_in_fork_set += 1

        if len(pr_count) > 0 and (prs_with_ext == 0 or (prs_ext_in_fork_set == 0 and prs_with_ext > 0)):
            print(f"[diagnostic] {origin}: PRs={len(pr_count)}, with_ext={prs_with_ext}, ext_in_fork_set={prs_ext_in_fork_set}, ext_not_in_fork_set={prs_ext_not_in_fork_set}, no_ext={prs_no_ext}, fork_set_size={len(fork_set)}")
            if prs_no_ext > 0 and len(prs_by_repo.get(origin, [])) > 0:
                sample_pr = prs_by_repo.get(origin, [])[0]
                has_head_repo = "head_repo_full_name" in sample_pr
                has_head_dict = "head" in sample_pr and isinstance(sample_pr.get("head"), dict)
                print(f"  [sample PR] number={sample_pr.get('number')}, has_head_repo_full_name={has_head_repo}, has_head_dict={has_head_dict}")
                if has_head_dict:
                    head = sample_pr.get("head", {})
                    has_repo = "repo" in head and isinstance(head.get("repo"), dict)
                    print(f"  [head] has_repo={has_repo}, head_repo_full_name={sample_pr.get('head_repo_full_name')}")
            if prs_ext_not_in_fork_set > 0:
                sample_exts = []
                for pr in prs_by_repo.get(origin, [])[:10]:
                    ext = extract_external_repo_from_pr(pr)
                    if ext and ext != origin and ext not in fork_set:
                        sample_exts.append(ext)
                        if len(sample_exts) >= 3:
                            break
                if sample_exts:
                    print(f"  [sample ext not in fork_set] {sample_exts}")

        common_contributions = len((contributions_gte_10 | contributions_lt_10) & developers)
        fork_diff = len(fork_set - contributions_gte_10 - developers)
        accepted_pr_avg_lifetime = (
            round(sum(accepted_pr_lifetimes) / len(accepted_pr_lifetimes), 2)
            if accepted_pr_lifetimes
            else ""
        )
        stars_gte_10_unique_commits_accepted_pr_avg_lifetime = (
            round(sum(stars_gte_10_unique_commits_accepted_pr_lifetimes) / len(stars_gte_10_unique_commits_accepted_pr_lifetimes), 2)
            if stars_gte_10_unique_commits_accepted_pr_lifetimes
            else ""
        )
        rows.append(
            [
                origin,
                origin_to_avg_lifespan.get(origin, 0.0),
                origin_to_fork_count.get(origin, 0),
                len(fork_set),
                len(contributions_gte_10),
                len(contributions_lt_10),
                len(developers),
                common_contributions,
                fork_diff,
                len(pr_count),
                len(pr_commits_set),
                len(stars_gte_10_prs_count),
                len(stars_gte_10_unique_commits_prs_count),
                len(stars_gte_10_prs_commits_set),
                len(stars_gte_10_unique_commits_prs_commits_set),
                accepted_pr_avg_lifetime,
                stars_gte_10_unique_commits_accepted_pr_avg_lifetime,
                forks_unique_commits_count + len(pr_commits_set),
            ]
        )

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        for r in rows:
            w.writerow(r)

    print("=" * 80)
    print("Done.")
    print(f"Output: {OUTPUT_CSV}")
    print(f"Origins: {len(rows)}")
    print(f"Generated: {datetime.now().isoformat()}")
    print("=" * 80)


if __name__ == "__main__":
    main()


