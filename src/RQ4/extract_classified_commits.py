#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Process result files from syncability_classify.py.

Filter commits by category (Bug Fix, Security Fix, Code Quality) that satisfy:
- is_independent: true (can stand alone)
- recommend_for_origin_and_forks: true (worth promoting)

Output CSV.
"""

import json
import csv
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional


def load_classification_result(file_path: Path) -> Optional[Dict[str, Any]]:
    """
    Load a single classification result file.

    Args:
        file_path: path to classification result file

    Returns:
        Parsed data, or None if file is corrupt or invalid
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except Exception as e:
        print(f"Warning: failed to read {file_path}: {e}")
        return None


def is_target_commit(data: Dict[str, Any], target_categories: List[str]) -> bool:
    """
    Check whether a commit meets the filter criteria.

    Args:
        data: classification result data
        target_categories: target category list

    Returns:
        True if criteria are met
    """
    classification = data.get("classification", {})
    
    if not classification.get("success", False):
        return False
    
    category = classification.get("category", "")
    if category not in target_categories:
        return False
    
    is_independent = classification.get("is_independent", False)
    if not is_independent:
        return False
    
    recommend = classification.get("recommend_for_origin_and_forks", False)
    if not recommend:
        return False
    
    return True


def extract_commit_info(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract needed info from classification result.

    Args:
        data: classification result data

    Returns:
        Extracted info dict
    """
    meta = data.get("meta", {})
    classification = data.get("classification", {})
    
    # Extract author and committer info
    author = meta.get("author", {})
    committer = meta.get("committer", {})
    author_date = author.get("date", "") if isinstance(author, dict) else ""
    committer_date = committer.get("date", "") if isinstance(committer, dict) else ""
    
    # Extract base info
    commit_info = {
        "commit_id": meta.get("commit_id", ""),
        "fork_repo": meta.get("fork_repo", ""),
        "origin_repo": meta.get("origin_repo", ""),
        "commit_hash": meta.get("commit_hash", ""),
        "category": classification.get("category", ""),
        "confidence": classification.get("confidence", 0.0),
        "reasoning": classification.get("reasoning", ""),
        "is_independent": classification.get("is_independent", False),
        "independent_confidence": classification.get("independent_confidence", 0.0),
        "independent_reasoning": classification.get("independent_reasoning", ""),
        "recommend_for_origin_and_forks": classification.get("recommend_for_origin_and_forks", False),
        "promotion_confidence": classification.get("promotion_confidence", 0.0),
        "promotion_reasoning": classification.get("promotion_reasoning", ""),
        "author_date": author_date,
        "committer_date": committer_date,
        "files_changed": ", ".join(meta.get("files_changed", [])),
        "files_count": len(meta.get("files_changed", [])),
        "passed_repos": ", ".join([
            f"{repo.get('repo_type', '')}:{repo.get('repo_name', '')}"
            for repo in meta.get("passed_repos", [])
        ]),
        "passed_repos_count": len(meta.get("passed_repos", []))
    }
    
    return commit_info


def process_classification_results(
    input_dir: Path,
    output_file: Path,
    target_categories: List[str]
) -> int:
    """
    Process classification result directory, filter commits and write CSV.

    Args:
        input_dir: classification result directory
        output_file: output CSV path
        target_categories: target category list

    Returns:
        Number of commits that match
    """
    if not input_dir.exists():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    
    # Collect all classification result files
    result_files = sorted([f for f in input_dir.glob("*.json") if f.is_file()])
    
    if not result_files:
        print(f"Warning: no classification result files in {input_dir}")
        return 0
    
    print(f"Processing {len(result_files)} classification result files...")
    
    # Filter matching commits
    matched_commits = []
    processed = 0
    
    for file_path in result_files:
        processed += 1
        if processed % 100 == 0:
            print(f"  Processed {processed}/{len(result_files)} files, matched {len(matched_commits)} commits")
        
        data = load_classification_result(file_path)
        if not data:
            continue
        
        if is_target_commit(data, target_categories):
            commit_info = extract_commit_info(data)
            matched_commits.append(commit_info)
    
    print(f"Done: found {len(matched_commits)} matching commits")
    
    if not matched_commits:
        print("No matching commits, not writing CSV")
        return 0
    
    def get_sort_key(commit_info: Dict[str, Any]) -> tuple:
        """Sort key for author_date descending."""
        author_date = commit_info.get("author_date", "")
        if not author_date or not author_date.strip():
            return (1, "")
        
        try:
            parts = author_date.strip().split()
            if len(parts) >= 2:
                date_time = f"{parts[0]} {parts[1]}"
                return (0, date_time)
            elif len(parts) == 1:
                return (0, parts[0])
            else:
                return (1, author_date)
        except Exception:
            return (1, author_date)
    
    matched_commits.sort(key=get_sort_key, reverse=True)
    print("Sorted by author_date descending (newest first, empty last)")
    
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # CSV columns
    fieldnames = [
        "commit_id",
        "fork_repo",
        "origin_repo",
        "commit_hash",
        "category",
        "confidence",
        "reasoning",
        "is_independent",
        "independent_confidence",
        "independent_reasoning",
        "recommend_for_origin_and_forks",
        "promotion_confidence",
        "promotion_reasoning",
        "author_date",
        "committer_date",
        "files_changed",
        "files_count",
        "passed_repos",
        "passed_repos_count"
    ]
    
    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(matched_commits)
    
    print(f"Results saved to: {output_file}")
    print(f"Total {len(matched_commits)} records")
    
    return len(matched_commits)


def main():
    parser = argparse.ArgumentParser(
        description="Process syncability_classify.py results, filter commits and output CSV",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Filter Bug Fix and Security Fix
  python extract_classified_commits.py \\
      --input-dir classify_commits_results \\
      --output classified_commits_filtered.csv \\
      --categories "Bug Fix" "Security Fix" "Code Quality"
  
  # Bug Fix only
  python extract_classified_commits.py \\
      --input-dir classify_commits_results \\
      --output bug_fixes.csv \\
      --categories "Bug Fix"
        """
    )
    
    parser.add_argument(
        "--input-dir",
        type=str,
        required=True,
        help="Classification result directory (JSON from syncability_classify.py)"
    )
    
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output CSV path"
    )
    
    parser.add_argument(
        "--categories",
        type=str,
        nargs="+",
        default=["Bug Fix", "Security Fix", "Code Quality"],
        help="Target categories (default: Bug Fix, Security Fix, Code Quality)"
    )
    
    args = parser.parse_args()
    
    input_dir = Path(args.input_dir)
    output_file = Path(args.output)
    
    print(f"Input dir: {input_dir}")
    print(f"Output file: {output_file}")
    print(f"Target categories: {', '.join(args.categories)}")
    print("Filter criteria:")
    print(f"  - is_independent: true")
    print(f"  - recommend_for_origin_and_forks: true")
    print()
    
    try:
        count = process_classification_results(
            input_dir=input_dir,
            output_file=output_file,
            target_categories=args.categories
        )
        print(f"\nSuccess: filtered {count} matching commits")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        exit(1)


if __name__ == "__main__":
    main()
