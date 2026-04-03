# Query Config

This directory stores the canonical PubMed query definitions used by the CAR literature retrieval pipeline.

## Files

- `queries.json`: topic-level query definitions and shared filtering blocks.

## Current design

Each topic entry contains:

- `id`: stable machine-readable identifier
- `label`: human-readable topic label
- `description`: short scope statement
- `topic_query`: the core recall-oriented topic query
- `full_query`: the topic query combined with first-pass research-only exclusions

## Notes

- `full_query` is the query that retrieval code should use by default.
- The publication-type exclusions are intentionally conservative and include `Case Reports`; they should be treated as a first-pass filter only.
- Some non-research records may still slip through due to PubMed metadata inconsistency, so downstream validation should remain enabled.
- Some true research articles may still require query refinement after sampling early retrieval results.

## Output counts

- Topic-level summaries may expose both `pmid_count` and `record_count`.
- `pmid_count` is the number of PubMed IDs retrieved before runtime filtering.
- `record_count` is the number of rows actually written after runtime filtering.

## Expected next step

The retrieval script should load `queries.json`, iterate over `topics`, and run the `full_query` value for each topic.
