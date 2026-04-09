import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .config import load_topic_queries
from .constants import (
    DEFAULT_CACHE_DIRNAME,
    DEFAULT_CONFIG_PATH,
    DEFAULT_OUTPUT_DIR,
    DEFAULT_QUERY_MODE,
    FETCH_BATCH_SIZE,
    SEARCH_BATCH_SIZE,
)
from .env import apply_proxy_environment, resolve_api_key
from .outputs import (
    build_quality_summary_rows,
    output_fieldnames,
    write_csv,
    write_json,
)
from .pipeline import collect_topic_records
from .records import merge_topic_records
from .utils import utc_timestamp


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Retrieve PubMed records for CAR literature queries and export raw outputs."
    )
    parser.add_argument(
        "--query",
        help="Run one direct PubMed query instead of loading topic queries from config.",
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to the query configuration JSON file.",
    )
    parser.add_argument(
        "--topic",
        action="append",
        dest="topics",
        help="Topic id to run from config. Repeat to run multiple topics. Defaults to all topics.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where raw JSON and CSV outputs will be written.",
    )
    parser.add_argument(
        "--search-batch-size",
        type=int,
        default=SEARCH_BATCH_SIZE,
        help="PMIDs requested per esearch page.",
    )
    parser.add_argument(
        "--fetch-batch-size",
        type=int,
        default=FETCH_BATCH_SIZE,
        help="PMIDs requested per efetch batch.",
    )
    parser.add_argument(
        "--max-records-per-topic",
        type=int,
        help="Optional cap per topic for dry runs and debugging.",
    )
    parser.add_argument(
        "--api-key",
        dest="api_key",
        help="NCBI API key. If omitted, resolve from environment variables or .env.",
    )
    parser.add_argument(
        "--query-mode",
        choices=("broad", "filtered"),
        default=DEFAULT_QUERY_MODE,
        help="Which configured query variant to run for topic-based retrieval.",
    )
    return parser.parse_args()


def run_direct_query(args: argparse.Namespace, api_key: Optional[str]) -> int:
    topic = {
        "id": "direct_query",
        "label": "Direct Query",
        "broad_query": args.query,
        "filtered_query": args.query,
    }
    pmids, raw_records, filtered_records, review_records, query = collect_topic_records(
        topic=topic,
        api_key=api_key,
        search_batch_size=args.search_batch_size,
        fetch_batch_size=args.fetch_batch_size,
        max_records=args.max_records_per_topic,
        output_dir=args.output_dir,
        query_mode=args.query_mode,
    )
    raw_rows = list(raw_records.values())
    filtered_rows = list(filtered_records.values())
    review_rows = list(review_records.values())
    fieldnames = output_fieldnames()

    write_json(
        args.output_dir / "direct_query_meta.json",
        {
            "topic_id": topic["id"],
            "topic_label": topic["label"],
            "query_mode": args.query_mode,
            "source_query": query,
            "pmid_count": len(pmids),
            "raw_record_count": len(raw_rows),
            "filtered_record_count": len(filtered_rows),
            "review_record_count": len(review_rows),
            "retrieved_at": utc_timestamp(),
            "status": "success",
        },
    )
    write_json(args.output_dir / "direct_query_pmids.json", pmids)
    write_json(args.output_dir / "direct_query_raw_records.json", raw_rows)
    write_json(args.output_dir / "direct_query_filtered_records.json", filtered_rows)
    write_json(args.output_dir / "direct_query_review_records.json", review_rows)
    write_csv(args.output_dir / "direct_query_raw_records.csv", raw_rows, fieldnames)
    write_csv(
        args.output_dir / "direct_query_filtered_records.csv",
        filtered_rows,
        fieldnames,
    )
    write_csv(
        args.output_dir / "direct_query_review_records.csv",
        review_rows,
        fieldnames,
    )
    print(f"Retrieved {len(filtered_rows)} filtered PubMed records for direct query.")
    return 0


def run_topic_config(args: argparse.Namespace, api_key: Optional[str]) -> int:
    topics = load_topic_queries(args.config, args.topics)
    topic_results: List[
        Tuple[Dict[str, object], List[str], Dict[str, Dict[str, str]]]
    ] = []
    raw_topic_results: List[
        Tuple[Dict[str, object], List[str], Dict[str, Dict[str, str]]]
    ] = []
    review_topic_results: List[
        Tuple[Dict[str, object], List[str], Dict[str, Dict[str, str]]]
    ] = []
    summary_rows = []
    failed_topics = []
    run_started_at = utc_timestamp()

    for topic in topics:
        print(f"Running topic {topic['id']}...", file=sys.stderr)
        try:
            pmids, raw_records, filtered_records, review_records, query = (
                collect_topic_records(
                    topic=topic,
                    api_key=api_key,
                    search_batch_size=args.search_batch_size,
                    fetch_batch_size=args.fetch_batch_size,
                    max_records=args.max_records_per_topic,
                    output_dir=args.output_dir,
                    query_mode=args.query_mode,
                )
            )
            raw_topic_results.append((topic, pmids, raw_records))
            topic_results.append((topic, pmids, filtered_records))
            review_topic_results.append((topic, pmids, review_records))
            summary_rows.append(
                {
                    "topic_id": topic["id"],
                    "topic_label": topic["label"],
                    "pmid_count": str(len(pmids)),
                    "raw_record_count": str(len(raw_records)),
                    "filtered_record_count": str(len(filtered_records)),
                    "review_record_count": str(len(review_records)),
                    "status": "success",
                    "error": "",
                }
            )
            write_json(
                args.output_dir / "topics" / f"{topic['id']}_meta.json",
                {
                    "topic_id": topic["id"],
                    "topic_label": topic["label"],
                    "description": topic.get("description", ""),
                    "query_mode": args.query_mode,
                    "broad_query": topic.get("broad_query", ""),
                    "filtered_query": topic.get("filtered_query", ""),
                    "source_query": query,
                    "pmid_count": len(pmids),
                    "raw_record_count": len(raw_records),
                    "filtered_record_count": len(filtered_records),
                    "review_record_count": len(review_records),
                    "retrieved_at": utc_timestamp(),
                    "status": "success",
                },
            )
            write_json(args.output_dir / "topics" / f"{topic['id']}_pmids.json", pmids)
            write_json(
                args.output_dir / "topics" / f"{topic['id']}_raw_records.json",
                list(raw_records.values()),
            )
            write_json(
                args.output_dir / "topics" / f"{topic['id']}_filtered_records.json",
                list(filtered_records.values()),
            )
            write_json(
                args.output_dir / "topics" / f"{topic['id']}_review_records.json",
                list(review_records.values()),
            )
            write_csv(
                args.output_dir / "topics" / f"{topic['id']}_raw_records.csv",
                list(raw_records.values()),
                output_fieldnames(),
            )
            write_csv(
                args.output_dir / "topics" / f"{topic['id']}_filtered_records.csv",
                list(filtered_records.values()),
                output_fieldnames(),
            )
            write_csv(
                args.output_dir / "topics" / f"{topic['id']}_review_records.csv",
                list(review_records.values()),
                output_fieldnames(),
            )
        except Exception as exc:
            message = str(exc)
            failed_topics.append({"topic_id": topic["id"], "error": message})
            summary_rows.append(
                {
                    "topic_id": topic["id"],
                    "topic_label": topic["label"],
                    "pmid_count": "0",
                    "raw_record_count": "0",
                    "filtered_record_count": "0",
                    "review_record_count": "0",
                    "status": "failed",
                    "error": message,
                }
            )
            write_json(
                args.output_dir / "topics" / f"{topic['id']}_meta.json",
                {
                    "topic_id": topic["id"],
                    "topic_label": topic["label"],
                    "description": topic.get("description", ""),
                    "query_mode": args.query_mode,
                    "broad_query": topic.get("broad_query", ""),
                    "filtered_query": topic.get("filtered_query", ""),
                    "pmid_count": 0,
                    "raw_record_count": 0,
                    "filtered_record_count": 0,
                    "review_record_count": 0,
                    "retrieved_at": utc_timestamp(),
                    "status": "failed",
                    "error": message,
                },
            )
            print(f"Topic {topic['id']} failed: {message}", file=sys.stderr)

    merged_raw_rows = (
        merge_topic_records(raw_topic_results) if raw_topic_results else []
    )
    merged_filtered_rows = merge_topic_records(topic_results) if topic_results else []
    merged_review_rows = (
        merge_topic_records(review_topic_results) if review_topic_results else []
    )
    write_json(args.output_dir / "merged_raw_records.json", merged_raw_rows)
    write_json(args.output_dir / "merged_filtered_records.json", merged_filtered_rows)
    write_json(args.output_dir / "manual_review_records.json", merged_review_rows)
    write_csv(
        args.output_dir / "merged_raw_records.csv",
        merged_raw_rows,
        output_fieldnames(),
    )
    write_csv(
        args.output_dir / "merged_filtered_records.csv",
        merged_filtered_rows,
        output_fieldnames(),
    )
    write_csv(
        args.output_dir / "manual_review_records.csv",
        merged_review_rows,
        output_fieldnames(),
    )
    write_csv(
        args.output_dir / "topic_summary.csv",
        summary_rows,
        [
            "topic_id",
            "topic_label",
            "pmid_count",
            "raw_record_count",
            "filtered_record_count",
            "review_record_count",
            "status",
            "error",
        ],
    )
    quality_summary_rows = build_quality_summary_rows(raw_topic_results, summary_rows)
    write_csv(
        args.output_dir / "retrieval_quality_summary.csv",
        quality_summary_rows,
        [
            "topic_id",
            "topic_label",
            "status",
            "pmid_count",
            "raw_record_count",
            "filtered_record_count",
            "review_record_count",
            "doi_missing_count",
            "publication_year_missing_count",
            "missing_abstract_count",
            "short_abstract_count",
            "filter_reason_counts",
        ],
    )
    write_json(
        args.output_dir / DEFAULT_CACHE_DIRNAME / "README.json",
        {
            "description": "Topic-level cache for PubMed retrieval. Reused across repeated runs with identical topic/query and batch parameters.",
            "contents": {
                "pmids": "Cached PMID lists per topic/query.",
                "detail": "Cached efetch batch responses.",
                "raw_records": "Cached normalized topic records with filter annotations.",
                "filtered_records": "Cached topic records kept after filtering.",
                "review_records": "Cached kept topic records flagged for manual review.",
            },
        },
    )
    write_json(args.output_dir / "failed_topics.json", failed_topics)
    write_json(
        args.output_dir / "run_manifest.json",
        {
            "started_at": run_started_at,
            "completed_at": utc_timestamp(),
            "config_path": str(args.config),
            "query_mode": args.query_mode,
            "selected_topics": args.topics or [],
            "search_batch_size": args.search_batch_size,
            "fetch_batch_size": args.fetch_batch_size,
            "max_records_per_topic": args.max_records_per_topic,
            "successful_topic_count": len(topic_results),
            "failed_topic_count": len(failed_topics),
            "merged_raw_record_count": len(merged_raw_rows),
            "merged_filtered_record_count": len(merged_filtered_rows),
            "manual_review_record_count": len(merged_review_rows),
        },
    )

    print(
        f"Retrieved {len(merged_filtered_rows)} filtered unique PubMed records across {len(topic_results)} successful topics."
    )
    return 0 if not failed_topics else 2


def main() -> int:
    args = parse_args()
    apply_proxy_environment()
    api_key = resolve_api_key(args.api_key)

    if args.search_batch_size <= 0 or args.fetch_batch_size <= 0:
        print("Batch sizes must be positive integers.", file=sys.stderr)
        return 1

    try:
        if args.query:
            return run_direct_query(args, api_key)
        return run_topic_config(args, api_key)
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
