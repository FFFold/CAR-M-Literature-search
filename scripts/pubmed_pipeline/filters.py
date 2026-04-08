import re
from typing import Dict, Tuple

from .config import attribution_conflict_terms, attribution_terms
from .constants import (
    NON_RESEARCH_PUBLICATION_TYPES,
    NON_RESEARCH_TITLE_TERMS,
)
from .utils import normalize_whitespace


def has_car_core_signal(text: str) -> bool:
    return (
        "chimeric antigen receptor" in text or re.search(r"\bcar\b", text) is not None
    )


def has_engineering_signal(text: str) -> bool:
    engineering_terms = (
        "engineered",
        "modified",
        "transduced",
        "expressing",
        "express",
        "constructed",
        "construct",
    )
    return any(term in text for term in engineering_terms)


def topic_boundary_reason(
    topic_id: str, conflict_terms: Dict[str, Tuple[str, ...]]
) -> str:
    if topic_id in {"car_dc", "car_mono"} and conflict_terms:
        return "topic_boundary:other_car_context_dominant"
    return "topic_boundary:car_t_context_dominant"


def detect_non_research_reasons(record: Dict[str, str]) -> Tuple[str, ...]:
    normalized_types = {
        normalize_whitespace(item).lower()
        for item in record.get("publication_types", "").split(";")
        if normalize_whitespace(item)
    }
    title_lower = normalize_whitespace(record.get("title", "")).lower()

    reasons = []
    excluded_types = sorted(normalized_types & NON_RESEARCH_PUBLICATION_TYPES)
    if excluded_types:
        reasons.append(f"publication_type:{','.join(excluded_types)}")

    hit_title_terms = sorted(
        term for term in NON_RESEARCH_TITLE_TERMS if term in title_lower
    )
    if hit_title_terms:
        reasons.append(f"title_noise:{','.join(hit_title_terms)}")

    return tuple(reasons)


def detect_quality_flags(record: Dict[str, str]) -> Tuple[str, ...]:
    abstract_text = normalize_whitespace(record.get("abstract", ""))
    flags = []
    if not abstract_text:
        flags.append("missing_abstract")
    elif len(abstract_text) < 200:
        flags.append("short_abstract")
    return tuple(flags)


def detect_topic_boundary_reasons(
    topic: Dict[str, object], record: Dict[str, str]
) -> Tuple[str, ...]:
    topic_id = str(topic.get("id", ""))
    title = normalize_whitespace(record.get("title", ""))
    abstract = normalize_whitespace(record.get("abstract", ""))
    combined_text = f"{title} {abstract}".lower().strip()

    if not combined_text:
        return ()

    primary_hints = attribution_terms(topic, "primary_title_abstract_phrases")
    cell_terms = attribution_terms(topic, "secondary_cell_terms")
    conflict_terms = attribution_conflict_terms(topic)
    has_primary_hint = any(hint in combined_text for hint in primary_hints)
    has_cell_term = any(term in combined_text for term in cell_terms)
    has_broad_topic_signal = has_car_core_signal(combined_text) and has_cell_term
    has_engineering_context = has_engineering_signal(combined_text)

    reasons = []
    if (
        topic_id in {"car_dc", "car_mac", "car_mono"}
        and has_cell_term
        and not has_primary_hint
        and not (has_broad_topic_signal and has_engineering_context)
    ):
        reasons.append("topic_boundary:cell_context_without_primary_car_platform")

    conflict_hits = any(
        term in combined_text for terms in conflict_terms.values() for term in terms
    )
    if conflict_hits and not has_primary_hint and not has_engineering_context:
        reasons.append(topic_boundary_reason(topic_id, conflict_terms))

    return tuple(reasons)


def apply_record_filters(
    topic: Dict[str, object], record: Dict[str, str]
) -> Dict[str, str]:
    reasons = list(detect_non_research_reasons(record))
    reasons.extend(detect_topic_boundary_reasons(topic, record))
    quality_flags = list(detect_quality_flags(record))

    status = "keep"
    if reasons:
        status = "exclude"
    elif quality_flags:
        status = "review"

    return {
        "filter_status": status,
        "filter_reason": "; ".join(reasons),
        "record_quality_flags": "; ".join(quality_flags),
        "needs_manual_review": "true" if status == "review" else "false",
    }


def annotate_record_for_topic(
    topic: Dict[str, object], raw_record: Dict[str, str]
) -> Dict[str, str]:
    record = dict(raw_record)
    filter_payload = apply_record_filters(topic, record)
    record.update(filter_payload)
    record["topic_filter_statuses"] = f"{topic['id']}:{record['filter_status']}"
    if record["filter_reason"]:
        record["topic_filter_reasons"] = f"{topic['id']}:{record['filter_reason']}"
    else:
        record["topic_filter_reasons"] = ""
    return record


def split_records_by_filter(
    records: Dict[str, Dict[str, str]],
) -> Tuple[Dict[str, Dict[str, str]], Dict[str, Dict[str, str]]]:
    filtered_records: Dict[str, Dict[str, str]] = {}
    review_records: Dict[str, Dict[str, str]] = {}
    for pmid, record in records.items():
        if record.get("filter_status") == "exclude":
            continue
        filtered_records[pmid] = record
        if record.get("needs_manual_review") == "true":
            review_records[pmid] = record
    return filtered_records, review_records
