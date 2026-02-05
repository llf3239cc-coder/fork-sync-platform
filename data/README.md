# Dataset for Fork Synchronization Study

This directory contains the datasets used in our ISSTA 2026 paper:
*"Mind the Gap: An Empirical Study of Synchronization Gaps, Delays, and Missed Opportunities in Software Forks"*

## Overview

| Directory | Research Question | Description |
|-----------|------------------|-------------|
| `RQ1_synchronization_status/` | RQ1: Synchronization Prevalence | Fork distribution, commit exposure ratios, PR coverage |
| `RQ2_synchronization_lag/` | RQ2: Synchronization Lag | Developer batching, maintainer review, and fork pulling delays |
| `RQ3_pr_categorization/` | RQ3: What Syncs and What Fails | PR intent classification, rejection reasons, synchronized commits |
| `RQ4_syncability_mining/` | RQ4: Missed Opportunities | Sync-ready commits, security validation, manual review |
| `repositories.csv` | All RQs | Master list of 977 origin repositories and their fork families |

**Note:** Files marked with `_sample` contain a random sample of 5,000 rows from the full dataset. The complete datasets are available upon request.

---

## `repositories.csv`

Master repository list with metadata for all 977 origin repositories studied.

| Column | Description |
|--------|-------------|
| Original Repo Name | GitHub `owner/repo` identifier |
| Stars | Star count at time of collection |
| Family Count (All Forks) | Total number of forks |
| Family Count (Stars >= 10) | Number of actively maintained forks (stars >= 10) |
| Family Repo Names (Stars >= 10) | Names of active forks |
| Forks with Unique Commits Count | Forks containing native (fork-local) commits |
| PR Coverage Ratio | Proportion of fork commits that appear in PRs |
| PR Merge Rate | Proportion of submitted PRs that were merged |

---

## RQ1: Synchronization Prevalence (`RQ1_synchronization_status/`)

### `fork_distribution.csv`
Per-origin repository statistics including fork counts, PR activity, and commit exposure metrics.
- **Rows:** 977 origins
- **Key columns:** Fork Count, PR Count, Fork's unique commits, PR coverage proportion

### `per_fork_statistics.csv`
Per-fork synchronization statistics for the top forks by PR activity.
- **Rows:** 326 forks
- **Key columns:** fork repo, prs_submitted, prs_merged, merge_rate

### `summary_statistics.csv`
Aggregate statistics across all analyzed forks (merge rates, PR counts).

### `cer_distribution.csv`
Distribution of Commit Exposure Ratio (CER) across origins, bucketed into ranges (0-5%, 5-10%, etc.).

### `commit_coverage.csv`
Per-origin commit coverage analysis showing how many commits are in PRs vs. fork-only.
- **Rows:** 977 origins
- **Key columns:** Origin, Fork_Count, Total_Commits, PR_Commits, Fork_Only_Commits, PR_Coverage_Pct

### `commit_coverage_summary.csv`
Aggregate commit coverage metrics (median 6.92% CER, 66.17% commits not in PRs).

---

## RQ2: Synchronization Lag (`RQ2_synchronization_lag/`)

### `fork_to_origin_lag_sample.csv` *(sampled: 5,000 of 158,485 rows)*
PR-level lag measurements for fork-to-origin synchronization.
- **Key columns:** Developer Batching Lag, Maintainer Review Lag, has Developer Batching Lag, has Maintainer Review Lag

### `origin_to_fork_lag.csv`
Fork pulling lag for origin-to-fork synchronization direction.
- **Rows:** 3,820 forks
- **Key columns:** Fork name, has Fork Pulling Lag, Fork Pulling Lag (day)

### `lag_statistics.csv`
Summary statistics for all three lag types (Lag1: developer batching, Lag2: maintainer review, Lag3: fork pulling).

### `lag_overview.csv`
Detailed lag overview with count, prevalence, median, mean, percentiles, and threshold exceedance rates.

### `lag_combinations.csv`
Co-occurrence patterns of multiple lag types (e.g., how often Lag1+Lag2+Lag3 occur together).

### `commit_lifecycle_summary.csv`
End-to-end commit lifecycle timing metrics.

### `commit_lifecycle_phases.csv`
Per-phase breakdown of synchronization delay (Developer Batching: 9.1%, Maintainer Review: 13.7%, Fork Propagation: 38.8%).

---

## RQ3: PR Categorization (`RQ3_pr_categorization/`)

### `pr_categories_sample.csv` *(sampled: 5,000 of 359,645 rows)*
LLM-classified PR intent categories for merged fork-to-origin PRs.
- **Key columns:** classification.category, classification.confidence, classification.sync_confidence

### `pr_closed_reasons_sample.csv` *(sampled: 5,000 of 291,937 rows)*
Rejection reason analysis for closed (not merged) fork-to-origin PRs.
- **Key columns:** closed_reason, confidence, was_technical_issue, was_project_fit_issue, could_be_merged_later

### `sync_fork_commits_sample.csv` *(sampled: 5,000 of 33,767 rows)*
Commits that were successfully synchronized from forks to origins via PRs, with timing data.
- **Key columns:** commit_sha, origin_repo, fork_repo, time_diff_days

### `rejection_summary.csv`
Aggregate rejection statistics (5,000 rejected PRs: 17.3% technical, 32.2% potentially mergeable later).

### `rejection_category_distribution.csv`
Distribution of rejection reasons (Superseded: 24.6%, Process Issues: 24.1%, Maintainer Policy: 16.7%, etc.).

### `per_origin_rejection_statistics.csv`
Per-origin breakdown of rejection patterns.

### `sankey_data.json`, `sankey_flows.csv`, `sankey_nodes.csv`
Data for the PR flow Sankey diagram visualization (PR submission to merge/rejection pathways).

---

## RQ4: Syncability Mining (`RQ4_syncability_mining/`)

### `sync_ready_commits_sample.csv` *(sampled: 5,000 of 12,284 rows)*
Final sync-ready commit-repository pairs identified by our three-stage pipeline.
- **Key columns:** fork_repo, origin_repo, commit_hash, classification_category, classification_recommend_for_origin_and_forks, classification_promotion_confidence, has_cve

### `security_syncable_commits.json`
144 security-related commits with 153 syncable units identified for security validation.
- **Structure:** `{total_syncable_commits, total_syncable_units, commits: [{commit_hash, origin_repo, fork_repo, syncable_repos, ...}]}`

### `security_validation_results.csv`
Manual validation results for 153 security commit-repository pairs.
- **Rows:** 153 validated pairs
- **Key columns:** commit_hash, origin_repo, target_syncable_repo, security_category, target_has_security_risk, validated_syncable, reasoning
- **Results:** 83 confirmed as potential 1-day vulnerabilities (YES + LIKELY)

### `validation_sample_300.csv`
Random sample of 300 sync-ready candidates used for manual validation of pipeline accuracy.

### `validation_review_sheet.csv`
Human reviewer assessments of the 300-sample validation (86% confirmed sync-worthy, 85% inter-rater agreement).

---

## Reproducibility

- **Sampled files** (`*_sample.csv`): Contain 5,000 randomly selected rows (seed=42) from the full dataset. Full datasets are available upon request.
- **Scripts**: See `src/` directory for all data collection, processing, and analysis scripts organized by stage (prepare data, RQ1-RQ4).
- **Dashboard**: The monitoring platform (`index.html`) reads from `data.json` in the repository root.
