# CAR Literature Search Pipeline

PubMed retrieval and LLM classification pipeline for CAR literature across five topic groups:

- `CAR-DC`
- `CAR-Mac`
- `CAR-Mono`
- `CAR-T`
- `CAR-NK`

The project retrieves PubMed records, normalizes metadata, merges cross-topic hits, and then uses an LLM-based classification pipeline to assign:

- a single `primary_topic`
- `relevance` (`relevant` / `peripheral` / `irrelevant`)
- `primary_mechanism`
- `secondary_mechanism`
- `disease_label`
- `disease_detail`
- `confidence`
- `reason`

## Goals

This repository is designed to support a reproducible CAR literature workflow that can:

1. Retrieve all matching PubMed research articles without date restriction.
2. Preserve core bibliographic metadata and abstract text.
3. Classify each paper by mechanism of action.
4. Label each paper by primary disease area.
5. Export a final CSV for downstream analysis.

Journal impact factor is intentionally out of scope for the current implementation. Journal names are normalized now so external metrics can be joined later.

## Current Corpus Snapshot

Using the current query set and retrieval pipeline, the merged filtered corpus contains:

- 10,772 merged research records after de-duplication
- 896 records matched by more than one topic query
- LLM-based `primary_topic` assignment to collapse multi-topic matches into one main topic
- `relevance` tagging to identify peripheral and clearly irrelevant false-positive matches

## Repository Layout

- `scripts/pubmed.py`: retrieval CLI entry point
- `scripts/pubmed_pipeline/`: PubMed retrieval, filtering, merge, and output pipeline
- `scripts/classify.py`: classification CLI entry point
- `scripts/classify_pipeline/`: LLM client, prompts, validation, cache, and classification pipeline
- `config/queries.json`: canonical PubMed topic queries
- `config/README.md`: notes about query design
- `docs/retrieval_flow.md`: retrieval architecture notes
- `EXECUTION_PLAN.md`: end-to-end project execution plan
- `CACHE.md`: cache behavior notes
- `output/`: generated runtime artifacts (ignored by Git)
- `.env.example`: environment variable template

## Requirements

- Python 3
- Network access to PubMed (`eutils.ncbi.nlm.nih.gov`)
- Optional LLM API access for semantic classification

This repository intentionally uses the Python standard library only.

## Setup

Create a local `.env` from `.env.example` and fill in your own values:

```powershell
copy .env.example .env
```

Example `.env` fields:

```env
NCBI_API_KEY="your_ncbi_api_key"

# Optional proxy settings
HTTP_PROXY=http://127.0.0.1:7897/
HTTPS_PROXY=http://127.0.0.1:7897/

# LLM API for classification subagent
LLM_API_BASE=http://localhost:9310/v1
LLM_API_KEY=your_llm_api_key
LLM_MODEL=your_model_name
```

Important:

- Do not commit `.env`.
- Do not commit `output/`.
- The retrieval and classification pipelines both support resume through on-disk cache files.

## Retrieval Pipeline

Run the full PubMed retrieval pipeline:

```powershell
python scripts/pubmed.py --output-dir output/full_v3
```

Run a subset of topics:

```powershell
python scripts/pubmed.py --topic car_t --topic car_nk --output-dir output/subset_run
```

Run a direct one-off query:

```powershell
python scripts/pubmed.py --query "chimeric antigen receptor macrophage" --output-dir output/direct_query
```

### Retrieval behavior

- PubMed `esearch` is used to collect PMIDs.
- Large result sets are automatically split by publication year to work around the PubMed `esearch` 9999-record limit.
- PubMed `efetch` is used in batches to retrieve article metadata.
- Record-level filtering excludes clearly non-research publication types using PubMed metadata.
- Missing or short abstracts are flagged for manual review rather than dropped.
- Records are deduplicated by `pmid` and merged across matched topics.

### Main retrieval outputs

The retrieval phase writes into the selected output directory, for example `output/full_v3/`:

- `merged_raw_records.json`
- `merged_raw_records.csv`
- `merged_filtered_records.json`
- `merged_filtered_records.csv`
- `manual_review_records.json`
- `manual_review_records.csv`
- `topic_summary.csv`
- `retrieval_quality_summary.csv`
- `cache/`

The classification phase uses `merged_filtered_records.json` as input.

## Classification Pipeline

Run semantic classification on retrieval output:

```powershell
python scripts/classify.py --input output/full_v3 --output-dir output/classified_v1
```

Run a small smoke test:

```powershell
python scripts/classify.py --input output/full_v3 --output-dir output/classify_test --limit 10
```

Run with concurrency:

```powershell
python scripts/classify.py --input output/full_v3 --output-dir output/classified_v1 --workers 4
```

### Classification behavior

For each record, the LLM is asked to assign:

- `primary_topic`
- `relevance`
- `primary_mechanism`
- `secondary_mechanism`
- `disease_label`
- `disease_detail`
- `confidence`
- `reason`

The classification layer includes:

- per-PMID cache files for resume
- LLM network retries
- JSON parse retries when the model returns malformed output
- optional multi-worker concurrency via `--workers`
- rules-based validation and normalization
- review routing for low-confidence, peripheral, irrelevant, missing-abstract, and inconsistent outputs
- automatic cache invalidation when the classification schema changes

### Why `primary_topic` exists

Many records match more than one topic query, especially combinations such as:

- `car_t` + `car_nk`
- `car_t` + `car_mac`
- `car_t` + `car_dc`

The retrieval layer intentionally preserves all matched topic memberships in `matched_topics`. The classification layer then assigns a single `primary_topic` based on the actual experimental focus of the paper.

Examples:

- a CAR-T paper that discusses tumor-associated macrophages still gets `primary_topic=car_t`
- a CAR-NK engineering paper that also mentions CAR-T comparisons gets `primary_topic=car_nk`
- a general review that only briefly mentions CAR platforms may be marked `relevance=peripheral`
- a keyword false positive can be marked `relevance=irrelevant`

### Main classification outputs

The classification phase writes into the selected output directory, for example `output/classified_v1/`:

- `classified_records.json`
- `classified_records.csv`
- `manual_review_records.csv`
- `classification_summary.json`
- `cache/classifications/`

The final CSV includes both retrieval metadata and semantic labels, including:

- `primary_topic`
- `relevance`
- `primary_mechanism`
- `secondary_mechanism`
- `disease_label`
- `disease_detail`
- `confidence`
- `reason`
- `needs_manual_review`
- `review_reasons`

## Label Schemas

### Topic labels

- `car_dc`
- `car_mac`
- `car_mono`
- `car_t`
- `car_nk`

### Relevance labels

- `relevant`
- `peripheral`
- `irrelevant`

### Mechanism labels

- `cytotoxic_killing`
- `phagocytosis`
- `immune_regulation`
- `microenvironment_remodeling`
- `antigen_presentation`
- `fibrosis_modulation`
- `drug_delivery_or_platform`
- `manufacturing_or_engineering`
- `safety_or_toxicity`
- `diagnostic_or_monitoring`
- `other`

### Disease labels

- `cancer`
- `autoimmune_disease`
- `organ_fibrosis`
- `infectious_disease`
- `hematologic_disorder_nonmalignant`
- `transplantation`
- `inflammatory_disease`
- `neurologic_disease`
- `other`

## Validation

Syntax check:

```powershell
python -m compileall scripts
```

Recommended end-to-end smoke sequence:

```powershell
python scripts/pubmed.py --max-records-per-topic 3 --output-dir output/smoke_retrieval
python scripts/classify.py --input output/smoke_retrieval --output-dir output/smoke_classify --limit 5
```

## Notes on Outputs and Redistribution

- `output/` contains generated datasets and caches and is intentionally ignored by Git.
- PubMed metadata and abstract redistribution policies may differ from code licensing and should be reviewed before public redistribution of generated output files.
- This repository is intended to open-source the pipeline code and configuration, not the generated dataset artifacts.

## Current Status

The repository currently supports:

- full-history PubMed retrieval with resume support
- merged topic-level corpus generation
- LLM-based semantic classification with cache, retry, review routing, and concurrency support
- single-label `primary_topic` assignment for multi-topic records
- `relevance` classification to isolate peripheral and irrelevant matches
- CSV/JSON export for downstream analysis

## Next Useful Improvements

- add a compact sample dataset or fixture set that does not include large generated outputs
- add optional notebook/reporting layer for downstream analysis
- add a post-processing step that separates `relevant` records from `peripheral` / `irrelevant` records for final release tables
