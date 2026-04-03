# Retrieval Cache

The PubMed retrieval pipeline now writes a reusable cache under each run output directory.

## Location

For a run like:

`python scripts/pubmed.py --output-dir output/full_run`

cache files are stored under:

`output/full_run/cache/`

## Layout

- `cache/README.json`: brief cache metadata
- `cache/<topic_id>/<cache_key>/pmids.json`: cached PMID list for that topic/query
- `cache/<topic_id>/<cache_key>/detail/*.json`: cached `efetch` batch payloads
- `cache/<topic_id>/<cache_key>/records.json`: cached filtered topic records
- `cache/<topic_id>/<cache_key>/meta.json`: cache metadata for the topic run

## Resume behavior

- If `records.json` and `pmids.json` already exist for the same topic/query/batch parameters and the same `max-records-per-topic` setting, the script reuses them and skips network retrieval for that topic.
- If only part of the cache exists, the script reuses cached PMID lists and `efetch` batch payloads, then fetches only missing pieces.
- Changing the topic query, batch sizes, or `max-records-per-topic` creates a new cache key automatically.

## Notes

- Cache is scoped to the chosen `--output-dir`.
- This design keeps runs self-contained and reproducible.
- Old output directories created before cache support will not contain cache files until the topic is re-run.
