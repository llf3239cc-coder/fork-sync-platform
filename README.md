# Fork Synchronization Monitor

Interactive dashboard for viewing fork synchronization recommendations.

The source files are located in `src` with guideline to reproduce at `src/RUN_FLOW.md`. 

## Setup Instructions

### 1. Generate the data file

```bash
python convert_csv_to_json.py
```

This creates `data.json` from the CSV file.

### 2. Deploy to GitHub Pages

1. Create a new GitHub account (for anonymity)
2. Create a new repository (must be public for free GitHub Pages)
3. Copy these files to the repo:
   - `index.html`
   - `data.json`
4. Go to Settings > Pages > Source: Deploy from branch (main)
5. Your site will be available at `https://[username].github.io/[repo-name]`

### Anonymous Git Config

Before committing, set anonymous git config locally:

```bash
git config user.name "Anonymous"
git config user.email "anonymous@example.com"
```

## Features

- Summary statistics (total commits, CVE fixes, recommendations)
- Category distribution chart
- Confidence distribution chart
- Searchable, sortable, filterable data table
- Direct links to GitHub commits

## Files

- `index.html` - Main dashboard (single-page app)
- `data.json` - Data file (generated from CSV)
- `convert_csv_to_json.py` - CSV to JSON converter
