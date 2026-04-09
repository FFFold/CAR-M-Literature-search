import csv
import json
from pathlib import Path
from typing import Dict, List, Sequence

from .records import base_record_fieldnames
from .utils import normalize_whitespace


def output_fieldnames() -> List[str]:
    return base_record_fieldnames()


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_csv(
    path: Path, rows: Sequence[Dict[str, str]], fieldnames: Sequence[str]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def build_quality_summary_rows(
    screened_topic_results: Sequence[
        tuple[Dict[str, object], List[str], Dict[str, Dict[str, str]]]
    ],
    summary_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, str]]:
    """Build quality summary from already-screened records.

    Uses the filter_status/filter_reason/record_quality_flags that were already
    computed during the pipeline, instead of re-running apply_record_filters().
    """
    counts_by_topic = {
        row["topic_id"]: {
            "pmid_count": int(row.get("pmid_count", "0") or "0"),
            "raw_record_count": int(row.get("raw_record_count", "0") or "0"),
            "filtered_record_count": int(row.get("filtered_record_count", "0") or "0"),
            "review_record_count": int(row.get("review_record_count", "0") or "0"),
            "status": row.get("status", ""),
        }
        for row in summary_rows
    }
    screened_records_by_topic = {
        str(topic["id"]): records for topic, _, records in screened_topic_results
    }
    quality_rows: List[Dict[str, str]] = []

    for row in summary_rows:
        topic_id = row["topic_id"]
        stats = counts_by_topic.get(topic_id, {})
        topic_label = row.get("topic_label", "")
        records = screened_records_by_topic.get(topic_id, {})

        doi_missing = 0
        year_missing = 0
        missing_abstract = 0
        short_abstract = 0
        exclusion_reasons: Dict[str, int] = {}

        for record in records.values():
            if not record.get("doi"):
                doi_missing += 1
            if not record.get("publication_year"):
                year_missing += 1

            flags = set(
                filter(
                    None,
                    normalize_whitespace(record.get("record_quality_flags", "")).split(
                        "; "
                    ),
                )
            )
            if "missing_abstract" in flags:
                missing_abstract += 1
            if "short_abstract" in flags:
                short_abstract += 1

            for reason in filter(
                None,
                normalize_whitespace(record.get("filter_reason", "")).split("; "),
            ):
                exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1

        quality_rows.append(
            {
                "topic_id": topic_id,
                "topic_label": topic_label,
                "status": stats.get("status", ""),
                "pmid_count": str(stats.get("pmid_count", 0)),
                "raw_record_count": str(stats.get("raw_record_count", 0)),
                "filtered_record_count": str(stats.get("filtered_record_count", 0)),
                "review_record_count": str(stats.get("review_record_count", 0)),
                "doi_missing_count": str(doi_missing),
                "publication_year_missing_count": str(year_missing),
                "missing_abstract_count": str(missing_abstract),
                "short_abstract_count": str(short_abstract),
                "filter_reason_counts": json.dumps(
                    exclusion_reasons, ensure_ascii=False, sort_keys=True
                ),
            }
        )

    return quality_rows
