# Query Config

This directory stores the canonical PubMed query definitions used by the CAR literature retrieval pipeline.

## Files

- `queries.json`: topic-level query definitions and shared filtering blocks.

## Current design (version 3)

Each topic entry contains:

- `id`: stable machine-readable identifier
- `label`: human-readable topic label
- `description`: short scope statement
- `query_notes`: rationale for the query design
- `must_hit_examples`: manual validation reminders for sentinel papers
- `broad_query`: default high-recall retrieval query
- `filtered_query`: optional narrower query that bakes in publication-type exclusions

## Query strategy

Each topic's `broad_query` is built from two parts combined with OR:

1. **Exact compound phrases**: direct matches like `"CAR-DC"`, `"CAR macrophage"`, etc.
2. **AND clause**: `"chimeric antigen receptor"[Title/Abstract] AND <cell-type terms>[Title/Abstract]`

The AND clause uses the full phrase "chimeric antigen receptor" rather than the abbreviation "CAR" to avoid noise from the 50,000+ unrelated PubMed records that match "CAR" alone.

## Notes

- `broad_query` is the default query the retrieval script should use.
- `filtered_query` remains available for exploratory runs, but should not replace downstream record-level filtering.
- Query-time publication-type exclusions are intentionally conservative and should be treated as a first-pass narrowing tool rather than the primary research filter.
- Topic attribution and boundary classification are deferred to a downstream classification subagent. They are not performed during retrieval.
- Some non-research records may still slip through due to PubMed metadata inconsistency, so downstream validation should remain enabled.

## Output counts

- Topic-level summaries expose `pmid_count`, `raw_record_count`, `filtered_record_count`, and `review_record_count`.
- `pmid_count` is the number of PubMed IDs retrieved before runtime filtering.
- `raw_record_count` is the number of normalized rows before filtering.
- `filtered_record_count` is the number of rows kept after runtime filtering.
- `review_record_count` is the number of kept rows additionally flagged for manual review.

## Runtime behavior

- The script supports `--query-mode broad|filtered` for topic-based runs.
- `broad` is the default and is recommended for corpus-building.
- `filtered` is useful for narrower validation runs when comparing query precision.
