# Query Config

This directory stores the canonical PubMed query definitions used by the CAR literature retrieval pipeline.

## Files

- `queries.json`: topic-level query definitions and shared filtering blocks.

## Current design

Each topic entry contains:

- `id`: stable machine-readable identifier
- `label`: human-readable topic label
- `description`: short scope statement
- `query_notes`: rationale for the query design
- `must_hit_examples`: manual validation reminders for sentinel papers
- `broad_query`: default high-recall retrieval query
- `filtered_query`: optional narrower query that bakes in first-pass exclusions
- `attribution.primary_title_abstract_phrases`: strong evidence phrases for post-retrieval topic attribution
- `attribution.secondary_cell_terms`: weaker lineage terms used for boundary checks
- `attribution.mesh_support_terms`: MeSH support terms used as secondary evidence
- `attribution.conflict_terms`: other topic phrases used to detect cross-topic dominance

## Notes

- `broad_query` is the default query the retrieval script should use.
- `filtered_query` remains available for exploratory runs, but should not replace downstream record-level filtering.
- Query-time publication-type exclusions are intentionally conservative and should be treated as a first-pass narrowing tool rather than the primary research filter.
- Topic attribution rules now live in this config file instead of being hardcoded in `scripts/pubmed.py`.
- Some non-research records may still slip through due to PubMed metadata inconsistency, so downstream validation should remain enabled.
- Some true research articles may still require query refinement after sampling early retrieval results.

## Output counts

- Topic-level summaries may expose `pmid_count`, `raw_record_count`, `filtered_record_count`, and `review_record_count`.
- `pmid_count` is the number of PubMed IDs retrieved before runtime filtering.
- `raw_record_count` is the number of normalized rows before filtering.
- `filtered_record_count` is the number of rows kept after runtime filtering.
- `review_record_count` is the number of kept rows additionally flagged for manual review.

## Expected next step

The retrieval script should load `queries.json`, iterate over `topics`, run `broad_query` by default, and preserve raw records before applying runtime filtering.

## Runtime behavior

- The script supports `--query-mode broad|filtered` for topic-based runs.
- `broad` is the default and is recommended for corpus-building.
- `filtered` is useful for narrower validation runs when comparing query precision.
