# Retrieval Flow

## Current module layout

- `scripts/pubmed.py`: thin CLI entry point
- `scripts/pubmed_pipeline/cli.py`: argument parsing and run orchestration
- `scripts/pubmed_pipeline/config.py`: topic query loading
- `scripts/pubmed_pipeline/client.py`: PubMed HTTP requests and XML/JSON parsing
- `scripts/pubmed_pipeline/records.py`: normalized record construction and cross-topic merge
- `scripts/pubmed_pipeline/filters.py`: non-research type exclusion and quality flags
- `scripts/pubmed_pipeline/cache.py`: cache-key and cache-path helpers
- `scripts/pubmed_pipeline/outputs.py`: CSV/JSON writers and quality summary generation
- `scripts/pubmed_pipeline/pipeline.py`: topic-level retrieval pipeline with progress reporting
- `scripts/pubmed_pipeline/constants.py`: shared constants
- `scripts/pubmed_pipeline/env.py`: environment variable and API key resolution
- `scripts/pubmed_pipeline/utils.py`: utility functions

## Data flow

1. Load topic definitions from `config/queries.json`.
2. For each topic, choose `broad_query` or `filtered_query`.
3. Run `esearch` to collect all matching PMIDs (with progress reporting).
4. Run `efetch` in batches and normalize PubMed metadata into raw records.
5. Apply record filters: non-research type exclusion and quality flags.
6. Split records into kept rows and manual-review rows.
7. Merge records across topics by PMID while preserving `matched_topics`.
8. Write merged outputs, topic outputs, summaries, and cache files.

## Version 3 changes

- Rewrote all five topic queries to fix severe recall failures:
  - car_dc: 9 -> ~346 PMIDs
  - car_mono: 77 -> ~186 PMIDs
  - car_mac: reduced noise by removing `"CAR"[tiab]` AND clauses
  - car_t / car_nk: removed `"CAR"[tiab]` noise, cleaner results
- Removed topic-boundary filtering heuristics (deferred to downstream classification subagent).
- Removed title-based noise word filtering (high false-positive risk).
- Removed attribution config from `queries.json` and related code from `config.py` / `filters.py`.
- Eliminated redundant `apply_record_filters()` call in `build_quality_summary_rows()`.
- Added batch-level progress reporting during efetch.
- Filtering now has two layers only:
  - non-research publication type exclusion (from PubMed metadata)
  - metadata quality flags (missing/short abstract)
