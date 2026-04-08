# Retrieval Flow

## Current module layout

- `scripts/pubmed.py`: thin CLI entry point
- `scripts/pubmed_pipeline/cli.py`: argument parsing and run orchestration
- `scripts/pubmed_pipeline/config.py`: topic query loading and attribution config helpers
- `scripts/pubmed_pipeline/client.py`: PubMed HTTP requests and XML/JSON parsing
- `scripts/pubmed_pipeline/records.py`: normalized record construction and cross-topic merge
- `scripts/pubmed_pipeline/filters.py`: research filtering, quality flags, and topic-boundary checks
- `scripts/pubmed_pipeline/cache.py`: cache-key and cache-path helpers
- `scripts/pubmed_pipeline/outputs.py`: CSV/JSON writers and quality summary generation
- `scripts/pubmed_pipeline/pipeline.py`: topic-level retrieval pipeline

## Data flow

1. Load topic definitions from `config/queries.json`.
2. For each topic, choose `broad_query` or `filtered_query`.
3. Run `esearch` to collect all matching PMIDs.
4. Run `efetch` in batches and normalize PubMed metadata into raw records.
5. Apply record filters as a separate step.
6. Split records into kept rows and manual-review rows.
7. Merge records across topics by PMID while preserving `matched_topics`.
8. Write merged outputs, topic outputs, summaries, and cache files.

## Design changes in this refactor

- Raw records are now truly pre-filter records. They no longer embed filter decisions.
- Filtering logic is split into three smaller layers:
  - non-research detection
  - metadata quality flags
  - topic-boundary checks
- Unused `esummary` code and cache paths were removed.
- The CLI behavior stays in one command, but the implementation is split into smaller files.
