# CAR Literature Search Execution Plan

## Objective

Build a reproducible PubMed-based pipeline to retrieve and organize research articles for the following CAR cell therapy topics:

- CAR-DC
- CAR-Mac
- CAR-Mono
- CAR-T
- CAR-NK

The pipeline should:

1. Retrieve all matching PubMed research articles with no date restriction.
2. Preserve core bibliographic metadata and abstract text.
3. Classify each paper by mechanism of action.
4. Label each paper by primary disease area.
5. Export a final CSV for downstream analysis.

Journal impact factor is explicitly out of scope for the first implementation phase. Journal names should still be normalized so impact factor can be joined later.

## Confirmed Scope

### Included literature

- Include research articles only.
- Keep all research modalities, including:
- clinical trials
- observational studies
- preclinical animal studies
- in vitro studies
- Exclude clearly non-research publications where possible, such as reviews, editorials, comments, letters, and similar items.

### Time range

- No publication date restriction.
- CAR-T and CAR-NK are intentionally full-history retrieval targets.

### Disease labeling

- If an article is primarily about a platform, engineering approach, or method and has no clear disease context, assign `disease_label=other` and leave `disease_detail` empty.

## Key Challenges

### 1. Query completeness

PubMed terminology is inconsistent across these domains. Each topic requires a high-recall query that covers:

- abbreviations
- full names
- hyphen and spacing variants
- singular and plural forms
- common alternate phrasings

The queries must balance recall and precision. Broader queries reduce misses but increase noise.

### 2. Scale

CAR-T and CAR-NK have large publication volumes. The implementation must support:

- paginated retrieval
- batching
- deduplication
- caching
- retries
- resumability

### 3. Metadata limitations

PubMed metadata alone does not directly provide:

- mechanism classification
- primary disease category
- journal impact factor

Mechanism and disease labels must be inferred from title, abstract, MeSH terms, and related metadata.

### 4. Classification ambiguity

Some papers describe more than one mechanism or more than one disease area. The labeling system should prefer a single primary classification, but support a secondary mechanism when both mechanisms are genuinely co-primary.

## Overall Architecture

The work will be implemented in four layers:

1. Query definition
2. PubMed retrieval and normalization
3. Semantic classification and validation
4. CSV generation and summary reporting

## Phase 1: Query Definition

### Goal

Create robust PubMed queries for the five target topic groups.

### Deliverables

- A query configuration file with one canonical query per topic.
- Documentation of included synonyms and exclusion logic.

### Requirements

Each topic query should attempt to cover:

- CAR-DC
- CAR-Mac
- CAR-Mono
- CAR-T
- CAR-NK

Each query should include combinations of:

- short-form abbreviations
- full written names
- alternative spacing and hyphenation
- cell-type-specific variations

### Research article filtering

Research-only filtering should be implemented conservatively using both:

- PubMed publication type metadata
- fallback filtering logic where publication type metadata is missing or inconsistent

The goal is to preserve original research while excluding clearly secondary or non-research publications.

## Phase 2: PubMed Retrieval Pipeline

### Goal

Upgrade the current `pubmed/scripts/pubmed.py` workflow from a small top-5 lookup utility into a full retrieval pipeline.

### Required capabilities

1. Run `esearch` to obtain total result count.
2. Retrieve all matching PMIDs via pagination.
3. Batch metadata retrieval for all PMIDs.
4. Retrieve abstract and MeSH-related detail when needed.
5. Cache intermediate results to avoid re-fetching completed batches.
6. Support resume after interruption.
7. Log failures and retry transient network/API issues.

### Recommended retrieval fields

For each article, preserve at least:

- `pmid`
- `title`
- `doi`
- `journal_raw`
- `journal_normalized`
- `publication_date_raw`
- `publication_year`
- `publication_month`
- `abstract`
- `mesh_terms`
- `publication_types`
- `pubmed_url`
- `matched_topics`
- `source_query`

### Deduplication rules

- Deduplicate primarily by `pmid`.
- Keep all matching topic memberships in `matched_topics`.
- If a paper is matched by multiple topic queries, do not drop the multi-topic information.

### Intermediate outputs

At minimum, persist:

- raw PMID lists per topic
- normalized article-level metadata cache
- one merged raw CSV before semantic labeling

## Phase 3: Journal Name Normalization

### Goal

Normalize journal names now so journal metrics can be joined later.

### Required behavior

Preserve both:

- `journal_raw`
- `journal_normalized`

Normalization should handle common surface differences such as:

- case
- punctuation
- whitespace
- common abbreviation variations where safe

This phase should not attempt to calculate or infer impact factor.

## Phase 4: Semantic Classification Strategy

### Goal

Classify each article based on title and abstract after retrieval is complete.

### Recommended approach

Use a dedicated classification subagent after the retrieval phase, rather than trying to fully classify at fetch time.

This subagent should read:

- title
- abstract
- MeSH terms when available
- matched topic group(s)

and return structured labels.

### Why a subagent is preferred

- Mechanism and disease labels are not reliable PubMed metadata fields.
- Title and abstract semantics matter more than keyword presence alone.
- A subagent can make better judgment calls on ambiguous papers.

### Why a rules layer is still needed

The classification system should not rely on the subagent alone. A lightweight rules layer is still useful for:

- detecting likely non-research records
- flagging empty or low-information abstracts
- identifying likely disease mentions
- validating obviously inconsistent outputs
- marking uncertain cases for manual review

The intended design is:

- subagent for primary semantic judgment
- rules for normalization, validation, and review routing

## Phase 5: Mechanism Classification Schema

### Core output fields

- `primary_mechanism`
- `secondary_mechanism`

### Classification policy

- Prefer a single primary mechanism whenever reasonably possible.
- Only assign `secondary_mechanism` when the paper truly presents two mechanisms with comparable importance.
- If the second mechanism is only incidental, leave `secondary_mechanism` empty.

### Initial mechanism label set

The mechanism taxonomy should remain open, but the following labels are a practical starting set:

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

This set may be expanded if the literature clearly requires additional mechanism labels.

## Phase 6: Disease Labeling Schema

### Core output fields

- `disease_label`
- `disease_detail`

### Classification policy

- Assign one primary disease label whenever possible.
- Use `disease_detail` for the specific disease entity when identifiable.
- If no clear disease context exists, set `disease_label=other` and leave `disease_detail` empty.

### Initial disease label set

The disease taxonomy should remain open, but the following labels are a practical starting set:

- `cancer`
- `autoimmune_disease`
- `organ_fibrosis`
- `infectious_disease`
- `hematologic_disorder_nonmalignant`
- `transplantation`
- `inflammatory_disease`
- `neurologic_disease`
- `other`

This set may be expanded if the literature clearly requires additional disease labels.

## Phase 7: Classification Subagent Contract

### Input

Per article, provide the subagent with at least:

- `pmid`
- `title`
- `abstract`
- `mesh_terms`
- `matched_topics`

### Output

The subagent should return structured fields only:

- `primary_mechanism`
- `secondary_mechanism`
- `disease_label`
- `disease_detail`
- `confidence`
- `reason`

### Output rules

- `confidence` should be a compact categorical value such as `high`, `medium`, or `low`.
- `reason` should be short and evidence-based, summarizing why the assigned labels were chosen.
- The subagent should avoid freeform essays.
- The subagent should favor stable, repeatable outputs.

### Review routing

The downstream pipeline should set `needs_manual_review=true` when:

- confidence is low
- abstract is missing or too short
- disease context is unclear
- mechanism assignment is highly ambiguous
- the rules layer detects a conflict between metadata and subagent output

## Phase 8: Final Output Schema

### Final CSV fields

The final CSV should contain at least:

- `pmid`
- `title`
- `doi`
- `journal_raw`
- `journal_normalized`
- `publication_year`
- `publication_month`
- `publication_date_raw`
- `matched_topics`
- `primary_mechanism`
- `secondary_mechanism`
- `disease_label`
- `disease_detail`
- `confidence`
- `needs_manual_review`
- `reason`
- `abstract`
- `mesh_terms`
- `publication_types`
- `pubmed_url`

### Optional additional outputs

It is recommended to also generate:

- a raw merged CSV before labeling
- a review-only CSV containing rows with `needs_manual_review=true`
- a compact summary report with counts by topic, mechanism, and disease label

## Phase 9: Validation and Quality Control

### Query validation

- Sample and inspect early retrievals for each topic.
- Confirm that expected canonical papers are captured.
- Refine overly broad or overly narrow query branches.

### Classification validation

- Manually inspect a sample from each mechanism class.
- Manually inspect a sample from each disease label.
- Review low-confidence and multi-topic records first.

### Consistency checks

Check for:

- duplicate PMIDs after merge
- empty titles or missing dates
- malformed DOI values
- unexpected journal normalization collisions
- labels outside the approved schema

## Proposed Implementation Order

1. Create and review the five PubMed query definitions.
2. Extend the retrieval script to support full-history paginated retrieval.
3. Add metadata enrichment for abstract, date, journal, and publication type.
4. Add deduplication, caching, and resume support.
5. Generate the merged raw CSV.
6. Design the classification schema and subagent prompt.
7. Run semantic labeling in batches.
8. Run rules-based validation and mark records for manual review.
9. Export the final labeled CSV and summary outputs.

## Immediate Next Deliverables

The next implementation step should produce:

1. A query configuration file covering the five CAR topic groups.
2. An upgraded retrieval script capable of full PubMed extraction.
3. A raw merged dataset with abstracts and core metadata.

After that, the classification subagent workflow can be implemented on top of the retrieved corpus.
