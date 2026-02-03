#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Update commit-tree and sharded commit-tree.

Flow:
1. Run git pull for all repos (optional)
2. Regenerate commits_trees.json
3. Re-shard into JSONL files

"""

import subprocess
import argparse
import sys
from pathlib import Path


def run_command(cmd: list, description: str, check: bool = True) -> bool:
    """
    Run command and show progress.

    Args:
        cmd: Command list
        description: Command description
        check: Whether to exit on failure

    Returns:
        Whether the command succeeded
    """
    print(f"\n{'=' * 80}")
    print(f"Step: {description}")
    print(f"{'=' * 80}")
    print(f"Executing: {' '.join(cmd)}")
    print()

    result = subprocess.run(cmd)

    if result.returncode != 0:
        if check:
            print(f"\nError: {description} failed")
            sys.exit(1)
        else:
            print(f"\nWarning: {description} failed (continuing)")
            return False

    print(f"\nSuccess: {description}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Update commit-tree and sharded commit-tree"
    )

    parser.add_argument(
        "--skip-pull",
        action="store_true",
        help="Skip git pull step (if repos are already up to date)"
    )

    parser.add_argument(
        "--pull-max-workers",
        type=int,
        default=None,
        help="Max parallel workers for git pull (default: auto-detect)"
    )

    parser.add_argument(
        "--generate-commits-tree",
        type=str,
        default="generate_commits_tree.py",
        help="Path to generate_commits_tree.py script (default: generate_commits_tree.py)"
    )

    parser.add_argument(
        "--shard-jsonl",
        type=str,
        default="shard_commits_trees_to_jsonl.py",
        help="Path to shard_commits_trees_to_jsonl.py script (default: shard_commits_trees_to_jsonl.py)"
    )

    parser.add_argument(
        "--pull-base-dir",
        type=str,
        default=None,
        help="Base directory for git pull repos (optional, default uses generate_commits_tree.py config)"
    )

    parser.add_argument(
        "--commits-trees-file",
        type=str,
        default="commits_trees.json",
        help="Path to commits_trees.json file (default: commits_trees.json)"
    )

    parser.add_argument(
        "--shards-dir",
        type=str,
        default="commits_trees_shards",
        help="Output directory for JSONL shard files (default: commits_trees_shards)"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Max workers for generate_commits_tree.py (optional)"
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("Update Commit-Tree and Sharded Commit-Tree")
    print("=" * 80)

    # Step 1: Git Pull (optional)
    if not args.skip_pull:
        pull_cmd = ["python", "pull_all_repos.py"]
        if args.pull_base_dir:
            pull_cmd.extend(["--base-dir", args.pull_base_dir])
        if args.pull_max_workers:
            pull_cmd.extend(["--max-workers", str(args.pull_max_workers)])

        if not run_command(pull_cmd, "Update all repos (git pull)", check=False):
            print("\nWarning: git pull partially failed, continuing with next steps")
    else:
        print("\nSkipping git pull step")

    # Step 2: Generate commits_trees.json
    generate_cmd = ["python", args.generate_commits_tree]

    # Add optional arguments
    if args.max_workers:
        generate_cmd.extend(["--max-workers", str(args.max_workers)])

    # Check for output file args (different script versions may have different params)
    # Using defaults here; pass via args if needed

    if not run_command(generate_cmd, "Generate commits_trees.json", check=True):
        print("\nError: Generate commits_trees.json failed, stopping")
        sys.exit(1)

    # Check that the generated file exists
    commits_trees_file = Path(args.commits_trees_file)
    if not commits_trees_file.exists():
        print(f"\nError: Generated commits_trees.json file not found: {commits_trees_file}")
        sys.exit(1)

    # Step 3: Shard into JSONL files
    shard_cmd = ["python", args.shard_jsonl]

    # shard_commits_trees_to_jsonl.py uses --input and --output-dir
    shard_cmd.extend(["--input", str(commits_trees_file)])
    shard_cmd.extend(["--output-dir", args.shards_dir])

    if not run_command(shard_cmd, "Shard into JSONL files", check=True):
        print("\nError: Sharding JSONL files failed")
        sys.exit(1)

    # Check output directory exists and has files
    shards_dir = Path(args.shards_dir)
    if shards_dir.exists():
        jsonl_files = list(shards_dir.glob("*.jsonl"))
        print(f"\nSharding done: generated {len(jsonl_files)} JSONL files")

    print("\n" + "=" * 80)
    print("All steps completed.")
    print("=" * 80)
    print(f"  - commits_trees.json: {commits_trees_file}")
    print(f"  - JSONL shards dir: {shards_dir}")
    print()


if __name__ == "__main__":
    main()
