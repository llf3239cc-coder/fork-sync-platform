#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate CSV from JSON files in the output folder.

Based on extract_passed_commits_to_csv.py CSV format, appends classification fields.

Each JSON file contains:
- commit info (same format as pipeline_results_passed_commits.json)
- classification info (from classify_results)
"""

import json
import csv
import argparse
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime


def extract_commit_message(commit_data: Dict[str, Any]) -> str:
    """Extract commit message."""
    # Prefer meta
    meta = commit_data.get("meta", {})
    if meta.get("commit_message"):
        return meta["commit_message"]
    
    # Then commit_message field
    if commit_data.get("commit_message"):
        return commit_data["commit_message"]
    
    # Fallback to other fields
    return commit_data.get("message", "")


def extract_diff(commit_data: Dict[str, Any]) -> str:
    """Extract diff info."""
    # Prefer meta
    meta = commit_data.get("meta", {})
    if meta.get("diff"):
        return meta["diff"]
    
    return commit_data.get("diff", "")


def build_github_commit_url(fork_repo: str, commit_hash: str) -> str:
    """
    Build GitHub commit URL.

    Args:
        fork_repo: fork repo name (format: owner/repo)
        commit_hash: commit hash

    Returns:
        GitHub commit URL
    """
    if not fork_repo or not commit_hash:
        return ""
    
    # Normalize fork_repo (replace __ with /)
    fork_repo = fork_repo.replace("__", "/")
    
    return f"github.com/{fork_repo}/commit/{commit_hash}"


def build_single_repo_file_urls(repo_name: str, files_changed: List[str], branch: str = "main") -> str:
    """
    Build GitHub file URLs for a single passed_repo (separated by ;).
    Uses latest branch (main or master), not a specific commit_hash.

    Args:
        repo_name: repo name (format: owner/repo)
        files_changed: list of changed files
        branch: branch name (default: main)

    Returns:
        All file GitHub URLs for that repo, separated by ;
    """
    if not repo_name or not files_changed:
        return ""
    
    # Normalize repo_name (replace __ with /)
    repo_name = repo_name.replace("__", "/")
    
    urls = []
    for file_path in files_changed:
        if file_path:
            # GitHub blob URL: github.com/{repo}/blob/{branch}/{file_path}
            url = f"github.com/{repo_name}/blob/{branch}/{file_path}"
            urls.append(url)
    
    return "; ".join(urls)


def format_single_passed_repo(repo: Dict[str, Any]) -> str:
    """
    Format a single passed repo.

    Args:
        repo: single repo info

    Returns:
        Formatted string: repo_type:repo_name
    """
    if not repo:
        return ""
    
    repo_type = repo.get("repo_type", "unknown")
    repo_name = repo.get("repo_name", "").replace("__", "/")
    return f"{repo_type}:{repo_name}"


def build_passed_repos_github_urls(passed_repos: List[Dict[str, Any]], branch: str = "main") -> str:
    """
    Build GitHub repo URLs for each passed repo (latest, separated by ;).
    Uses latest branch (main or master), not a specific commit_hash.

    Args:
        passed_repos: list of passed repos
        branch: branch name (default: main)

    Returns:
        Multiple repo GitHub URLs, separated by ;
    """
    if not passed_repos:
        return ""
    
    urls = []
    for repo in passed_repos:
        repo_name = repo.get("repo_name", "").replace("__", "/")
        if repo_name:
            # Use repo latest URL (can be /tree/{branch} or repo URL)
            url = f"github.com/{repo_name}"
            urls.append(url)
    
    return "; ".join(urls)


def extract_author_date(commit_data: Dict[str, Any]) -> str:
    """Extract author date."""
    # Prefer meta
    meta = commit_data.get("meta", {})
    author = meta.get("author", {}) or commit_data.get("author", {})
    if isinstance(author, dict):
        return author.get("date", "")
    return ""


def extract_committer_date(commit_data: Dict[str, Any]) -> str:
    """Extract committer date."""
    # Prefer meta
    meta = commit_data.get("meta", {})
    committer = meta.get("committer", {}) or commit_data.get("committer", {})
    if isinstance(committer, dict):
        return committer.get("date", "")
    return ""


def has_cve_info(commit_data: Dict[str, Any]) -> bool:
    """
    Check whether commit data contains CVE info.

    Args:
        commit_data: commit data dict

    Returns:
        True if CVE info present, else False
    """
    # CVE format: CVE-YYYY-NNNNN (NNNNN may be 4-7 digits)
    cve_pattern = re.compile(r'CVE-\d{4}-\d{4,7}', re.IGNORECASE)
    
    # Check commit_message
    commit_message = extract_commit_message(commit_data)
    if commit_message and cve_pattern.search(commit_message):
        return True
    
    # Check classification reasoning fields
    classification = commit_data.get("classification", {})
    if classification:
        reasoning = classification.get("reasoning", "")
        if reasoning and cve_pattern.search(reasoning):
            return True
        
        promotion_reasoning = classification.get("promotion_reasoning", "")
        if promotion_reasoning and cve_pattern.search(promotion_reasoning):
            return True
        
        independent_reasoning = classification.get("independent_reasoning", "")
        if independent_reasoning and cve_pattern.search(independent_reasoning):
            return True
        
        sync_importance_reasoning = classification.get("sync_importance_reasoning", "")
        if sync_importance_reasoning and cve_pattern.search(sync_importance_reasoning):
            return True
    
    return False


def _value_for_csv(value: Any) -> str:
    """Convert any value to CSV-safe string (dict/list serialized as JSON)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except Exception:
            return str(value)
    return str(value)


def extract_classification_fields(commit_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract all classification fields (no keys dropped), prefix keys with classification_.
    """
    classification = commit_data.get("classification", {})
    if not classification:
        return {}
    out = {}
    for k, v in classification.items():
        key = f"classification_{k}"
        out[key] = _value_for_csv(v)
    return out


def extract_source_aggregated_fields(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Extract pipeline-related fields from meta.source_aggregated, keep key info."""
    out = {}
    source = meta.get("source_aggregated") if isinstance(meta, dict) else {}
    if not source:
        return out
    for key in ("item_type", "item_id", "final_decision", "final_score", "rejection_reason"):
        if key in source:
            out[f"source_{key}"] = _value_for_csv(source[key])
    stage2 = source.get("stage2_result")
    if isinstance(stage2, dict):
        out["source_stage2_passed"] = _value_for_csv(stage2.get("passed"))
    if "sync_status" in source and source["sync_status"] is not None:
        out["source_sync_status"] = _value_for_csv(source["sync_status"])
    return out


def extract_author_committer_fields(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Extract author/committer name and email from meta (complement author_date/committer_date)."""
    out = {}
    if not isinstance(meta, dict):
        return out
    for role in ("author", "committer"):
        obj = meta.get(role)
        if not isinstance(obj, dict):
            continue
        prefix = f"{role}_"
        out[prefix + "name"] = _value_for_csv(obj.get("name"))
        out[prefix + "email"] = _value_for_csv(obj.get("email"))
    return out


def process_output_directory(
    input_dir: Path,
    output_file: Path,
    allowed_categories: Optional[List[str]] = None
) -> int:
    """
    Process JSON files in output directory and generate CSV.

    Args:
        input_dir: input output directory path
        output_file: output CSV path
        allowed_categories: allowed classification_category list (default: Bug Fix, Security Enhancement, Safety Enhancement)

    Returns:
        Number of records processed
    """
    if allowed_categories is None:
        allowed_categories = ["Bug Fix", "Security Enhancement", "Safety Enhancement"]
    
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    
    # Collect all JSON files
    json_files = sorted([f for f in input_dir.glob("*.json") if f.is_file()])
    
    if not json_files:
        print(f"No JSON files found in {input_dir}")
        return 0
    
    print(f"Found {len(json_files)} JSON files")
    print(f"Allowed categories: {', '.join(allowed_categories)}")
    
    # Prepare CSV data
    csv_rows = []
    skipped_count = 0
    
    for idx, json_file in enumerate(json_files, 1):
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                commit_data = json.load(f)
        except Exception as e:
            print(f"  Warning: failed to load {json_file.name}: {e}")
            continue
        
        # Check if skipped (based on syncability_classify.py result)
        classification = commit_data.get("classification", {})
        if classification.get("skipped") is True:
            skipped_count += 1
            continue
        
        # Check for successful classification
        if classification.get("success") is not True and not classification.get("category"):
            skipped_count += 1
            continue
        
        # Extract commit info (prefer meta)
        meta = commit_data.get("meta", {})
        fork_repo = meta.get("fork_repo", "") or commit_data.get("fork_repo", "")
        commit_hash = meta.get("commit_hash", "") or commit_data.get("commit_hash", "")
        files_changed = meta.get("files_changed", []) or commit_data.get("files_changed", [])
        passed_repos = meta.get("passed_repos", []) or commit_data.get("passed_repos", [])
        
        # Skip if no passed_repos
        if not passed_repos:
            continue
        
        # Extract classification fields
        classification_fields = extract_classification_fields(commit_data)
        
        # Check if classification_category is in allowed list
        classification_category = classification_fields.get("classification_category", "")
        if classification_category not in allowed_categories:
            skipped_count += 1
            continue
        
        # Do not skip records with test files; mark them for sorting
        # (previously skipped, now kept but marked)
        
        # Check for CVE info
        has_cve = has_cve_info(commit_data)
        
        # Check for test files
        if isinstance(files_changed, list):
            has_test_file = any("test" in str(file_path).lower() for file_path in files_changed if file_path)
        else:
            has_test_file = "test" in str(files_changed).lower()
        
        # Build base info (shared by all passed_repo)
        github_commit_url = build_github_commit_url(fork_repo, commit_hash)
        
        # files_changed may be list or comma-separated string
        if isinstance(files_changed, list):
            files_changed_str = ", ".join(files_changed)
            files_count = len(files_changed)
        else:
            files_changed_str = str(files_changed)
            # If comma-separated string, split then count
            if files_changed_str and "," in files_changed_str:
                files_count = len([f.strip() for f in files_changed_str.split(",") if f.strip()])
            elif files_changed_str:
                files_count = 1
            else:
                files_count = 0
        
        base_info = {
            "commit_id": meta.get("commit_id", "") or commit_data.get("commit_id", ""),
            "fork_repo": fork_repo,
            "origin_repo": meta.get("origin_repo", "") or commit_data.get("origin_repo", ""),
            "commit_hash": commit_hash,
            "commit_message": extract_commit_message(commit_data),
            "diff": extract_diff(commit_data),
            "github_commit_url": github_commit_url,
            "files_changed": files_changed_str,
            "files_count": files_count,
            "author_date": extract_author_date(commit_data),
            "committer_date": extract_committer_date(commit_data),
            "passed_repos_count": len(passed_repos),
            "has_cve": has_cve,
            "has_test_file": has_test_file,
        }
        base_info.update(extract_author_committer_fields(meta))
        base_info.update(extract_source_aggregated_fields(meta))
        
        # One row per passed_repo
        for passed_repo in passed_repos:
            repo_name = passed_repo.get("repo_name", "").replace("__", "/")
            passed_repos_github_file_urls = build_single_repo_file_urls(repo_name, files_changed)
            passed_repo_github_url = build_passed_repos_github_urls([passed_repo])
            passed_repo_str = format_single_passed_repo(passed_repo)
            
            row = {
                **base_info,
                "passed_repo": passed_repo_str,
                "passed_repo_github_url": passed_repo_github_url,
                "passed_repos_github_file_urls": passed_repos_github_file_urls,
                **classification_fields,
            }
            csv_rows.append(row)
        
        if idx % 100 == 0:
            print(f"  Processed {idx}/{len(json_files)} files (kept: {len(csv_rows)}, skipped: {skipped_count})")
    
    print(f"\nFilter stats: processed {len(json_files)} files, kept {len(csv_rows)} records, skipped {skipped_count} (category mismatch)")
    
    if not csv_rows:
        print("No valid data found, not writing CSV")
        return 0
    
    # Sort: CVE first, then records with test files, then author_date descending
    def get_sort_key(row: Dict[str, str]) -> tuple:
        """Sort key: CVE first, then test files, then author_date descending."""
        has_cve = row.get("has_cve", False)
        cve_priority = 1 if has_cve else 0
        
        has_test_file = row.get("has_test_file", False)
        test_priority = 1 if has_test_file else 0
        
        author_date = row.get("author_date", "")
        if not author_date or not author_date.strip():
            return (cve_priority, test_priority, "0000-01-01 00:00:00")
        
        try:
            parts = author_date.strip().split()
            if len(parts) >= 2:
                date_time = f"{parts[0]} {parts[1]}"
                return (cve_priority, test_priority, date_time)
            elif len(parts) == 1:
                return (cve_priority, test_priority, parts[0])
            else:
                return (cve_priority, test_priority, "0000-01-01 00:00:00")
        except Exception:
            return (cve_priority, test_priority, "0000-01-01 00:00:00")
    
    csv_rows.sort(key=get_sort_key, reverse=True)
    print("Sorted: CVE first, then records with test files, then author_date descending (newest first)")
    
    # Collect all column names (no fields dropped)
    all_keys_set = set()
    for row in csv_rows:
        all_keys_set.update(row.keys())
    fieldnames_sorted = sorted(all_keys_set)
    
    # Drop columns that are identical to another (keep one)
    def column_values(rows: List[Dict], col: str) -> tuple:
        return tuple(rows[i].get(col, "") for i in range(len(rows)))
    
    seen_content: Dict[tuple, str] = {}
    fieldnames_filtered = []
    for col in fieldnames_sorted:
        content = column_values(csv_rows, col)
        if content not in seen_content:
            seen_content[content] = col
            fieldnames_filtered.append(col)
        # else: column identical to seen_content[content], skip
    
    if len(fieldnames_filtered) < len(fieldnames_sorted):
        print(f"Dropped {len(fieldnames_sorted) - len(fieldnames_filtered)} duplicate columns (identical content)")
    
    fieldnames = fieldnames_filtered
    
    # Write CSV
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    print(f"\nSaving to: {output_file} ({len(fieldnames)} columns)")
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(csv_rows)
    
    print("Results saved")
    print(f"Total {len(csv_rows)} records")
    
    return len(csv_rows)


def main():
    parser = argparse.ArgumentParser(
        description="Generate CSV from JSON files in output folder (with classification info)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate CSV
  python extract_classified_commits_to_csv.py \\
      --input-dir output \\
      --output classified_commits.csv
        """
    )
    
    parser.add_argument(
        "--input-dir",
        type=str,
        default="output",
        help="Input output directory path (default: output)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        default="classified_commits.csv",
        help="Output CSV path (default: classified_commits.csv)"
    )
    
    parser.add_argument(
        "--categories",
        type=str,
        nargs="+",
        default=["Bug Fix", "Security Enhancement", "Safety Enhancement"],
        help="classification_category list to filter (default: Bug Fix Security Enhancement Safety Enhancement)"
    )
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_file = Path(args.output)
    allowed_categories = args.categories
    
    print(f"Input dir: {input_dir}")
    print(f"Output file: {output_file}")
    print(f"Allowed categories: {', '.join(allowed_categories)}")
    print()
    
    try:
        count = process_output_directory(
            input_dir=input_dir,
            output_file=output_file,
            allowed_categories=allowed_categories
        )
        print(f"\nSuccess: processed {count} records")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
