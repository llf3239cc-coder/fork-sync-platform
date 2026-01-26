#!/usr/bin/env python3
"""
Convert the CSV data to JSON for the dashboard.
Optimizes file size by only including necessary columns.
"""

import csv
import json
import sys
from pathlib import Path

# Columns needed for the dashboard
KEEP_COLUMNS = [
    'commit_id',
    'fork_repo',
    'origin_repo',
    'commit_hash',
    'github_commit_url',
    'files_count',
    'author_date',
    'has_cve',
    'has_test_file',
    'classification_category',
    'classification_confidence',
    'classification_is_independent',
    'classification_independent_confidence',
    'classification_recommend_for_origin_and_forks',
    'classification_promotion_confidence',
    'classification_success'
]

def convert_csv_to_json(csv_path: str, json_path: str):
    """Convert CSV to optimized JSON."""
    data = []

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Only keep necessary columns
            filtered_row = {k: row.get(k, '') for k in KEEP_COLUMNS}
            data.append(filtered_row)

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, separators=(',', ':'))  # Compact JSON

    print(f"Converted {len(data)} records")
    print(f"Output: {json_path}")
    print(f"Size: {Path(json_path).stat().st_size / 1024 / 1024:.2f} MB")

if __name__ == '__main__':
    # Default paths
    script_dir = Path(__file__).parent
    csv_path = script_dir.parent / 'data' / 'Large data extension RQ4 - commit recommend new.csv'
    json_path = script_dir / 'data.json'

    # Allow override from command line
    if len(sys.argv) > 1:
        csv_path = Path(sys.argv[1])
    if len(sys.argv) > 2:
        json_path = Path(sys.argv[2])

    convert_csv_to_json(str(csv_path), str(json_path))
