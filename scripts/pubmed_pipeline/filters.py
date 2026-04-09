from typing import Dict, Tuple

from .constants import NON_RESEARCH_PUBLICATION_TYPES
from .utils import normalize_whitespace


def detect_non_research_reasons(record: Dict[str, str]) -> Tuple[str, ...]:
    """Detect non-research publication types from PubMed metadata.

    Only uses the publication_types field, which is PubMed's own annotation.
    Title-based heuristics were removed due to high false-positive risk.
    """
    normalized_types = {
        normalize_whitespace(item).lower()
        for item in record.get("publication_types", "").split(";")
        if normalize_whitespace(item)
    }

    excluded_types = sorted(normalized_types & NON_RESEARCH_PUBLICATION_TYPES)
    if excluded_types:
        return (f"publication_type:{','.join(excluded_types)}",)

    return ()


def detect_quality_flags(record: Dict[str, str]) -> Tuple[str, ...]:
    abstract_text = normalize_whitespace(record.get("abstract", ""))
    flags = []
    if not abstract_text:
        flags.append("missing_abstract")
    elif len(abstract_text) < 200:
        flags.append("short_abstract")
    return tuple(flags)


def apply_record_filters(record: Dict[str, str]) -> Dict[str, str]:
    reasons = list(detect_non_research_reasons(record))
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
    filter_payload = apply_record_filters(record)
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
