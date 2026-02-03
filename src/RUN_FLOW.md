# Website Scripts Directory – Run Flow

This document describes the **website scripts** directory structure: purpose of each script, main inputs/outputs, and recommended execution order. Paths are relative to the `scripts` directory or its parent working directory.

---

## 1. Directory Structure

```
网站脚本/
├── prepare data/   # Data prep: crawler, clone, PR, commit tree, Syncability input
├── RQ1/            # Final stats (from local JSON)
├── RQ2/            # PR Lag and timing analysis
├── RQ3/            # PR classification (DeepSeek)
├── RQ4/            # Syncability and security
└── RUN_FLOW.md     # This document
```

---

## 2. Prerequisite Data (External or Upstream)

These files/directories are used by multiple scripts and must exist or be produced by other scripts:

| File/Directory | Description | Source |
|----------------|-------------|--------|
| `github_projects_filtered_stars_10.json` | Filtered projects (stars≥10, origin + forks) | Upstream crawler/filter |
| `github_projects_filtered_zero_stars.json` | Filtered projects (zero stars, for PR crawl) | Upstream |
| `github_projects.json` | Full project list (Fork Count etc.) | Upstream (e.g. `prepare data/github_crawler.py`) |
| `forks_with_unique_commits.json` | Forks with unique commits | e.g. `list_forks_with_unique_commits.py` |
| `filtered_prs_split/` | Filtered PR split (one file per PR) | e.g. `filter_prs_from_forks_with_unique_commits.py` |
| `unmerged_pr_list_directory.json` / `unmerged_pr_list/` | Unmerged PR list and directory | e.g. `generate_unmerged_pr_list.py` |
| `sync_prs_fork_analysis_filtered.json` | Filtered PR-fork analysis | Other analysis scripts |

---

## 3. Scripts by Directory (Input/Output)

### 3.1 prepare data/

Crawl projects → clone/pull repos → build commit tree and shards → crawl/shard PRs → extract PRs from downloaded forks → download PR comments (optional) → generate Syncability input.

| Script | Purpose | Main Input | Main Output (JSON/dir) | Main Output (CSV) |
|--------|---------|------------|------------------------|-------------------|
| `github_crawler.py` | Crawl GitHub C/C++ projects and fork lists (token rotation) | Tokens, language config | `github_projects.json`, `github_projects_backup.json`, `original_repos_*`, `forks_*`, `github_projects.txt` | See JSON row |
| `clone_repos.py` | Batch clone repos from project list | `github_projects_filtered_stars_10.json` | `cloned_repos/`, `cloned_repos/clone_report.json` | — |
| `pull_all_repos.py` | `git pull` for cloned repos | `cloned_repos/`, optional `--json-file` | `.pull_progress.json` | — |
| `generate_commits_tree.py` | Build unified commit tree per origin+forks | Project JSON, `clone_report.json`, `cloned_repos/` | `commits_trees.json` | — |
| `shard_commits_trees_to_jsonl.py` | Shard commit tree by repo to JSONL | `commits_trees.json` | `commits_trees_shards/*.jsonl`, `index.json` | — |
| `update_commits_trees.py` | One-shot: optional pull → commit tree → shard | Same as pull + generate + shard | `.pull_progress.json`, `commits_trees.json`, `commits_trees_shards/` | — |
| `crawl_repos_PRs.py` | Crawl PRs per project (token rotation) | `github_projects_filtered_zero_stars.json`, tokens | `repos_prs.json` (and optional checkpoint) | — |
| `shard_repos_prs_to_jsonl.py` | Shard PR table by repo to JSONL | `repos_prs.json` | `repos_prs_shards/*.jsonl`, index | — |
| `extract_prs_from_downloaded_forks.py` | Keep PRs whose head is in downloaded forks; optionally fetch commits | `clone_report.json`, `repos_prs_v2.json`, `repos_prs_shards/`, tokens | `repos_prs_from_downloaded_forks.json`, `repos_prs_commits_shards/` etc. | — |
| `download_pr_comments.py` | Download comments for unmerged PR list and write back into PR files | `unmerged_pr_list_directory.json`, `unmerged_pr_list/`, tokens | Updates fields in `unmerged_pr_list/*.json` | — |
| `filter_prs_from_forks_with_unique_commits.py` | Filter PRs from forks with unique commits; optional commit analysis | `repos_prs_from_downloaded_forks.json` or PR shard dir, `forks_with_unique_commits.json` | Output dir (one file per PR) | — |
| `generate_syncability_input.py` | Build Syncability input from forks_with_unique_commits + commit shards | `forks_with_unique_commits.json`, `commits_trees_shards/` | `syncability_input/` or `file_count_N/*.json` (see script args) | — |

To only refresh the commit tree, run `update_commits_trees.py` (use `--skip-pull` to skip pull).

---

### 3.2 RQ1/ (Final stats)

Reproduce final analyzer stats from local JSON/shard data (no MongoDB).

| Script | Purpose | Main Input | Main Output (JSON) | Main Output (CSV) |
|--------|---------|------------|--------------------|-------------------|
| `final_analyzer_from_json.py` | Compute Fork Count, Contributions, Developers, PR metrics from project JSON, PR, commit shards, clone_report | `github_projects_filtered_stars_10.json`, `repos_prs.json` (or `repos_prs_from_downloaded_forks.json`), `commits_trees_shards/`, optional `clone_report.json` | — | `analysis_results.csv` |

---

### 3.3 RQ2/ (PR Lag and timing)

Compute PR Lag1/Lag2/Lag3 and commit timing from sharded PRs, commit tree shards, and project info.

| Script | Purpose | Main Input | Main Output (JSON) | Main Output (CSV) |
|--------|---------|------------|--------------------|-------------------|
| `pr_lag_from_json.py` | Read PR shards, compute Lag1/Lag2/Lag3 with commit tree and project info | `repos_prs_commits_shards/`, `commits_trees_shards/`, `github_projects_*.json`, `forks_with_unique_commits.json`, optional `clone_report.json` | — | `pr_lag_classification_from_json.csv`, `pr_lag_from_json_log.txt` |
| `calculate_lag3_all_forks.py` | Compute Lag3 for all downloaded forks (sync delay fork–origin) from commit tree shards | `commits_trees_shards/`, project/clone info | — | Lag3 stats CSV (see script) |
| `analyze_pr_commit_timing.py` | Analyze timing of “PR commits” in other forks vs origin, per commit | `sync_prs_fork_analysis_filtered.json`, `commits_trees_shards/`, `pr_classification_results/` etc. | `commit_timing_analysis.json` | `commit_timing_analysis.csv` |

---

### 3.4 RQ3/ (PR classification)

DeepSeek classification of filtered/split PRs (e.g. Bug Fix, Performance).

| Script | Purpose | Main Input | Main Output (JSON/dir) | Main Output (CSV) |
|--------|---------|------------|------------------------|-------------------|
| `classify_prs_from_downloaded_forks.py` | Read `filtered_prs_split`, classify merged PRs via DeepSeek | `filtered_prs_split/`, `pr_classification_prompt.txt`, API key | `pr_classification_results/*.json`, `pr_classification_status.json`, `pr_classification_categories.json`, logs | — |

Requires `filtered_prs_split/` (often produced by scripts like `filter_prs_from_forks_with_unique_commits.py` outside this dir).

---

### 3.5 RQ4/ (Syncability and security)

Multi-stage Syncability pipeline (candidate extraction → hard filters → technical portability) and security classification/unit analysis. Some scripts are modules called by the pipeline, not run as standalone steps.

| Script | Type | Purpose | Main Input | Main Output |
|--------|------|---------|------------|-------------|
| `syncability_common.py` | Shared module | Syncability data structures and helpers (e.g. `CandidateItem`) | — | Imported by others |
| `syncability_stage0_candidate_extraction.py` | Shared module | Stage0: candidate commit extraction | — | Called by pipeline |
| `syncability_stage1_hard_filters.py` | Shared module | Stage1: hard filters | — | Called by pipeline |
| `syncability_stage2_technical_portability.py` | Shared module | Stage2: technical portability (file existence, diff context) | — | Called by pipeline |
| `syncability_pipeline.py` | Main flow | Pipeline Stage0 → Stage1 → Stage2; output syncable/synced commits | prepare data output (e.g. `syncability_input/`), local clone path | Stage result dirs/JSON (see args) |
| `extract_classified_commits.py` | Post-process | Filter commits by category (Bug Fix, Security Fix, Code Quality) and flags; output CSV | Classify result dir (syncability_classify output) | CSV (e.g. `classified_commits_filtered.csv`) |
| `extract_classified_commits_to_csv.py` | Post-process | Build CSV from output-dir JSON (extract_passed_commits format + classification) | Output dir JSON | CSV (e.g. `classified_commits.csv`) |
| `keyword_then_deepseek_security.py` | Optional | Security keyword filter + DeepSeek security check on Syncability-style input | Syncability input dir (e.g. `syncability_input/`) | `keyword_deepseek_out/` per-commit JSON, summary CSV |
| `security_syncability_units.py` | Optional | Syncability-unit analysis for security commits (using local clones) | Security commit dir, `github_projects_filtered_stars_10.json`, clone dir | Unit stats dir/JSON/CSV (see args) |

---

## 4. Recommended Overall Order and Script → JSON/CSV

Execute in the order below; each step states **which .py produces which .json / .csv (or directory)**. Script paths are **directory/script** (e.g. `prepare data/clone_repos.py`).

1. **Prerequisite data**  
   Project JSON (`github_projects_*.json`), and as needed `forks_with_unique_commits.json`, `filtered_prs_split/`, `unmerged_pr_list*`.  
   → Prepared outside this dir; **prepare data/github_crawler.py** can produce `github_projects.json`, `github_projects_backup.json`, and CSV/text backups.

2. **Repos and commit tree**  
   - **prepare data/clone_repos.py** → `cloned_repos/`, `cloned_repos/clone_report.json`.  
   - (Optional) **prepare data/pull_all_repos.py** → `.pull_progress.json`.  
   - **prepare data/generate_commits_tree.py** → `commits_trees.json`.  
   - **prepare data/shard_commits_trees_to_jsonl.py** → `commits_trees_shards/*.jsonl`, `commits_trees_shards/index.json`.  
   - Or run **prepare data/update_commits_trees.py** once to do pull (optional) → `commits_trees.json` → `commits_trees_shards/`.

3. **PR data**  
   - **prepare data/crawl_repos_PRs.py** → `repos_prs.json` (and optional checkpoint).  
   - **prepare data/shard_repos_prs_to_jsonl.py** → `repos_prs_shards/*.jsonl`, index.  
   - For “PRs from downloaded forks + commits”: **prepare data/extract_prs_from_downloaded_forks.py** → `repos_prs_from_downloaded_forks.json`, `repos_prs_commits_shards/` etc.  
   - For filtered PRs (from forks with unique commits): **prepare data/filter_prs_from_forks_with_unique_commits.py** → output dir (one file per PR).

4. **PR comments** (if analyzing unmerged PRs)  
   - **prepare data/download_pr_comments.py** → updates `unmerged_pr_list/*.json` (no new files).  
   - Requires `unmerged_pr_list_directory.json` and `unmerged_pr_list/`.

5. **PR classification** (if needed)  
   - **RQ3/classify_prs_from_downloaded_forks.py** → `pr_classification_results/*.json`, `pr_classification_status.json`, `pr_classification_categories.json` etc.  
   - Requires `filtered_prs_split/`.

6. **PR Lag and timing**  
   - **RQ2/pr_lag_from_json.py** → `pr_lag_classification_from_json.csv`, `pr_lag_from_json_log.txt`.  
   - **RQ2/calculate_lag3_all_forks.py** → Lag3 stats CSV (see script).  
   - **RQ2/analyze_pr_commit_timing.py** → `commit_timing_analysis.json`, `commit_timing_analysis.csv`.

7. **Syncability**  
   - **prepare data/generate_syncability_input.py** → `syncability_input/` or `file_count_N/*.json` etc.  
   - **RQ4/syncability_pipeline.py** → stage result dirs/JSON.  
   - **RQ4/extract_classified_commits.py** / **RQ4/extract_classified_commits_to_csv.py** → CSV from classify/output JSON.  
   - Optional: **RQ4/keyword_then_deepseek_security.py** → `keyword_deepseek_out/` JSON, summary CSV; **RQ4/security_syncability_units.py** → unit stats dir/CSV (see args).

8. **Final stats**  
   - **RQ1/final_analyzer_from_json.py** → `analysis_results.csv`.

You can skip or reorder steps depending on available inputs and goals; default paths are per-script (use `--help`).

---

## 5. Script → JSON/CSV Quick Reference

| Script (.py) | Directory | JSON output | CSV output |
|--------------|-----------|-------------|------------|
| `github_crawler.py` | prepare data | `github_projects.json`, `github_projects_backup.json`, `original_repos_*`, `forks_*` | `original_repos_*.csv`, `forks_*.csv` |
| `clone_repos.py` | prepare data | `cloned_repos/`, `cloned_repos/clone_report.json` | — |
| `pull_all_repos.py` | prepare data | `.pull_progress.json` | — |
| `generate_commits_tree.py` | prepare data | `commits_trees.json` | — |
| `shard_commits_trees_to_jsonl.py` | prepare data | `commits_trees_shards/*.jsonl`, `index.json` | — |
| `update_commits_trees.py` | prepare data | Same as A2–A4 | — |
| `crawl_repos_PRs.py` | prepare data | `repos_prs.json` | — |
| `shard_repos_prs_to_jsonl.py` | prepare data | `repos_prs_shards/*.jsonl`, index | — |
| `extract_prs_from_downloaded_forks.py` | prepare data | `repos_prs_from_downloaded_forks.json`, `repos_prs_commits_shards/` etc. | — |
| `filter_prs_from_forks_with_unique_commits.py` | prepare data | Output dir (one JSON per PR) | — |
| `download_pr_comments.py` | prepare data | Updates `unmerged_pr_list/*.json` | — |
| `generate_syncability_input.py` | prepare data | `syncability_input/` or `file_count_N/*.json` etc. | — |
| `final_analyzer_from_json.py` | RQ1 | — | `analysis_results.csv` |
| `pr_lag_from_json.py` | RQ2 | — | `pr_lag_classification_from_json.csv` |
| `calculate_lag3_all_forks.py` | RQ2 | — | Lag3 stats CSV |
| `analyze_pr_commit_timing.py` | RQ2 | `commit_timing_analysis.json` | `commit_timing_analysis.csv` |
| `classify_prs_from_downloaded_forks.py` | RQ3 | `pr_classification_results/*.json`, status/categories JSON | — |
| `syncability_pipeline.py` | RQ4 | Stage result dirs/JSON | — |
| `extract_classified_commits.py` | RQ4 | — | e.g. `classified_commits_filtered.csv` |
| `extract_classified_commits_to_csv.py` | RQ4 | — | e.g. `classified_commits.csv` |
| `keyword_then_deepseek_security.py` | RQ4 | `keyword_deepseek_out/commits/*.json` etc. | `keyword_deepseek_security_summary.csv` etc. |
| `security_syncability_units.py` | RQ4 | Unit result dir/JSON | May include CSV (see args) |

---

## 6. Running Scripts

- Scripts resolve relative paths from the **current working directory** or script directory; if data lives in a parent dir, `cd` there or pass absolute paths.  
- Use `python script.py --help` for each script’s arguments.  
- In RQ4, `syncability_common.py` and `syncability_stage0/1/2_*.py` are modules used by `syncability_pipeline.py` and are not normally run alone.
