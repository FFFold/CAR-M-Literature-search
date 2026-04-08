from typing import Dict, List, Sequence, Tuple

from .utils import normalize_journal_name, normalize_whitespace


def base_record_fieldnames() -> List[str]:
    return [
        "pmid",
        "title",
        "doi",
        "journal_raw",
        "journal_normalized",
        "publication_date_raw",
        "publication_year",
        "publication_month",
        "abstract",
        "mesh_terms",
        "publication_types",
        "pubmed_url",
        "matched_topics",
        "matched_topic_labels",
        "source_query",
        "source_query_id",
        "source_query_label",
        "filter_status",
        "filter_reason",
        "topic_filter_statuses",
        "topic_filter_reasons",
        "record_quality_flags",
        "needs_manual_review",
    ]


def build_raw_record(
    topic: Dict[str, object], query: str, pmid: str, detail: Dict[str, str]
) -> Dict[str, str]:
    title = normalize_whitespace(detail.get("title", "") or "")
    journal_raw = normalize_whitespace(detail.get("journal_raw", ""))
    return {
        "pmid": pmid,
        "title": title,
        "doi": detail.get("doi", ""),
        "journal_raw": journal_raw,
        "journal_normalized": detail.get("journal_normalized")
        or normalize_journal_name(journal_raw),
        "publication_date_raw": detail.get("publication_date_raw", ""),
        "publication_year": detail.get("publication_year", ""),
        "publication_month": detail.get("publication_month", ""),
        "abstract": detail.get("abstract", ""),
        "mesh_terms": detail.get("mesh_terms", ""),
        "publication_types": detail.get("publication_types", ""),
        "pubmed_url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
        "matched_topics": str(topic["id"]),
        "matched_topic_labels": str(topic["label"]),
        "source_query": query,
        "source_query_id": str(topic["id"]),
        "source_query_label": str(topic["label"]),
        "filter_status": "",
        "filter_reason": "",
        "topic_filter_statuses": "",
        "topic_filter_reasons": "",
        "record_quality_flags": "",
        "needs_manual_review": "false",
    }


def merge_topic_records(
    topic_results: Sequence[
        Tuple[Dict[str, object], List[str], Dict[str, Dict[str, str]]]
    ],
) -> List[Dict[str, str]]:
    merged: Dict[str, Dict[str, str]] = {}
    for topic, _, records in topic_results:
        for pmid, record in records.items():
            existing = merged.get(pmid)
            if not existing:
                merged[pmid] = dict(record)
                continue

            matched_topics = set(filter(None, existing["matched_topics"].split(";")))
            matched_topics.add(str(topic["id"]))
            existing["matched_topics"] = ";".join(sorted(matched_topics))

            matched_labels = set(
                filter(None, existing["matched_topic_labels"].split(";"))
            )
            matched_labels.add(str(topic["label"]))
            existing["matched_topic_labels"] = ";".join(sorted(matched_labels))

            if not existing.get("abstract") and record.get("abstract"):
                existing["abstract"] = record["abstract"]
            if not existing.get("mesh_terms") and record.get("mesh_terms"):
                existing["mesh_terms"] = record["mesh_terms"]
            if not existing.get("publication_types") and record.get(
                "publication_types"
            ):
                existing["publication_types"] = record["publication_types"]
            if not existing.get("source_query") and record.get("source_query"):
                existing["source_query"] = record["source_query"]

            topic_filter_statuses = set(
                filter(None, existing.get("topic_filter_statuses", "").split(";"))
            )
            topic_filter_statuses.update(
                filter(None, record.get("topic_filter_statuses", "").split(";"))
            )
            existing["topic_filter_statuses"] = ";".join(sorted(topic_filter_statuses))

            topic_filter_reasons = set(
                filter(None, existing.get("topic_filter_reasons", "").split(";"))
            )
            topic_filter_reasons.update(
                filter(None, record.get("topic_filter_reasons", "").split(";"))
            )
            existing["topic_filter_reasons"] = ";".join(sorted(topic_filter_reasons))

            if not existing.get("record_quality_flags") and record.get(
                "record_quality_flags"
            ):
                existing["record_quality_flags"] = record["record_quality_flags"]
            if (
                existing.get("needs_manual_review") != "true"
                and record.get("needs_manual_review") == "true"
            ):
                existing["needs_manual_review"] = "true"
            if record.get("filter_status") == "exclude":
                existing["filter_status"] = "exclude"
            elif not existing.get("filter_status") and record.get("filter_status"):
                existing["filter_status"] = record["filter_status"]
            if not existing.get("filter_reason") and record.get("filter_reason"):
                existing["filter_reason"] = record["filter_reason"]

    return sorted(
        merged.values(),
        key=lambda item: (
            item.get("publication_year", ""),
            item.get("publication_month", ""),
            item["pmid"],
        ),
    )
