#!/usr/bin/env python3
"""CLI entry point for the classification subagent pipeline.

Usage:
    python scripts/classify.py --input output/full_v3 --output-dir output/classified_v1
    python scripts/classify.py --input output/full_v3 --output-dir output/classified_v1 --limit 10
"""

import argparse
import csv
import json
import sys
from pathlib import Path

# Ensure the scripts directory is importable
sys.path.insert(0, str(Path(__file__).resolve().parent))

from classify_pipeline.pipeline import classify_records
from classify_pipeline.schema import CLASSIFICATION_FIELDS
from pubmed_pipeline.utils import load_env_file


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify CAR literature records using an LLM subagent.",
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Input directory from the retrieval pipeline (e.g., output/full_v3).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/classified"),
        help="Output directory for classified results.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of records to classify (for testing).",
    )
    parser.add_argument(
        "--api-base",
        type=str,
        default=None,
        help="LLM API base URL (overrides .env LLM_API_BASE).",
    )
    parser.add_argument(
        "--api-key",
        type=str,
        default=None,
        help="LLM API key (overrides .env LLM_API_KEY).",
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="LLM model name (overrides .env LLM_MODEL).",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="LLM temperature (default: 0.1).",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of concurrent LLM calls (default: 1, serial).",
    )
    return parser.parse_args()


def resolve_env(cli_value: str | None, env_key: str) -> str:
    """Resolve a config value from CLI arg, env var, or .env file."""
    if cli_value:
        return cli_value

    import os

    value = os.environ.get(env_key)
    if value:
        return value

    # Try .env files
    for env_path in (Path.cwd() / ".env", Path(__file__).resolve().parents[1] / ".env"):
        env_values = load_env_file(env_path)
        value = env_values.get(env_key)
        if value:
            return value

    return ""


def load_filtered_records(input_dir: Path) -> dict:
    """Load filtered records from the retrieval pipeline output."""
    # Try merged JSON first
    merged_json = input_dir / "merged_filtered_records.json"
    if merged_json.exists():
        print(f"Loading records from {merged_json}", file=sys.stderr)
        data = json.loads(merged_json.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
        if isinstance(data, list):
            return {r["pmid"]: r for r in data if "pmid" in r}

    print(
        f"Error: cannot find merged_filtered_records.json in {input_dir}",
        file=sys.stderr,
    )
    sys.exit(1)


# Output CSV fieldnames: retrieval metadata + classification fields
RETRIEVAL_FIELDS = [
    "pmid",
    "title",
    "doi",
    "journal_raw",
    "journal_normalized",
    "publication_year",
    "publication_month",
    "publication_date_raw",
    "matched_topics",
    "matched_topic_labels",
    "abstract",
    "mesh_terms",
    "publication_types",
    "pubmed_url",
    "source_query",
    "filter_status",
    "filter_reason",
]

OUTPUT_FIELDS = (
    RETRIEVAL_FIELDS
    + list(CLASSIFICATION_FIELDS)
    + [
        "needs_manual_review",
        "review_reasons",
    ]
)


def main() -> None:
    args = parse_args()

    # Resolve LLM config
    api_base = resolve_env(args.api_base, "LLM_API_BASE")
    api_key = resolve_env(args.api_key, "LLM_API_KEY")
    model = resolve_env(args.model, "LLM_MODEL")

    if not api_base:
        print("Error: LLM_API_BASE not configured.", file=sys.stderr)
        sys.exit(1)
    if not model:
        print("Error: LLM_MODEL not configured.", file=sys.stderr)
        sys.exit(1)
    if not api_key:
        print(
            "Warning: LLM_API_KEY is empty. Requests may fail if the API requires authentication.",
            file=sys.stderr,
        )

    print(f"LLM API: {api_base}", file=sys.stderr)
    print(f"Model:   {model}", file=sys.stderr)

    # Load records
    records = load_filtered_records(args.input)
    print(f"Loaded {len(records)} filtered records.", file=sys.stderr)

    if args.limit:
        pmids = sorted(records.keys())[: args.limit]
        records = {p: records[p] for p in pmids}
        print(f"Limited to {len(records)} records.", file=sys.stderr)

    # Run classification
    classifications = classify_records(
        records=records,
        output_dir=args.output_dir,
        api_base=api_base,
        api_key=api_key,
        model=model,
        temperature=args.temperature,
        workers=args.workers,
    )

    # Merge retrieval metadata with classification results
    merged: list[dict] = []
    for pmid in sorted(records.keys()):
        row = dict(records[pmid])
        cls = classifications.get(pmid, {})
        row.update(cls)
        merged.append(row)

    # Write outputs
    args.output_dir.mkdir(parents=True, exist_ok=True)

    # Full classified JSON
    json_path = args.output_dir / "classified_records.json"
    json_path.write_text(
        json.dumps(merged, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Full classified CSV
    csv_path = args.output_dir / "classified_records.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in merged:
            writer.writerow(row)

    # Review-only CSV
    review_rows = [r for r in merged if r.get("needs_manual_review") == "true"]
    review_path = args.output_dir / "manual_review_records.csv"
    with review_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for row in review_rows:
            writer.writerow(row)

    # Summary statistics
    summary = build_summary(merged)
    summary_path = args.output_dir / "classification_summary.json"
    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    irrelevant = sum(1 for r in merged if r.get("relevance") == "irrelevant")
    peripheral = sum(1 for r in merged if r.get("relevance") == "peripheral")

    print(f"\nClassification complete.", file=sys.stderr)
    print(f"  Total records:   {len(merged)}", file=sys.stderr)
    print(
        f"  Relevant:        {len(merged) - irrelevant - peripheral}", file=sys.stderr
    )
    print(f"  Peripheral:      {peripheral}", file=sys.stderr)
    print(f"  Irrelevant:      {irrelevant}", file=sys.stderr)
    print(f"  Needs review:    {len(review_rows)}", file=sys.stderr)
    print(f"  Output:          {args.output_dir}", file=sys.stderr)


def build_summary(records: list[dict]) -> dict:
    """Build summary statistics from classified records."""
    relevance_counts: dict[str, int] = {}
    mechanism_counts: dict[str, int] = {}
    disease_counts: dict[str, int] = {}
    confidence_counts: dict[str, int] = {}
    topic_counts: dict[str, int] = {}

    for r in records:
        rel = r.get("relevance", "relevant")
        relevance_counts[rel] = relevance_counts.get(rel, 0) + 1

        pm = r.get("primary_mechanism", "other")
        mechanism_counts[pm] = mechanism_counts.get(pm, 0) + 1

        dl = r.get("disease_label", "other")
        disease_counts[dl] = disease_counts.get(dl, 0) + 1

        conf = r.get("confidence", "low")
        confidence_counts[conf] = confidence_counts.get(conf, 0) + 1

        topics = r.get("matched_topics", "")
        for t in topics.split(";"):
            t = t.strip()
            if t:
                topic_counts[t] = topic_counts.get(t, 0) + 1

    return {
        "total_records": len(records),
        "needs_review_count": sum(
            1 for r in records if r.get("needs_manual_review") == "true"
        ),
        "relevance_counts": dict(sorted(relevance_counts.items())),
        "mechanism_counts": dict(sorted(mechanism_counts.items())),
        "disease_counts": dict(sorted(disease_counts.items())),
        "confidence_counts": dict(sorted(confidence_counts.items())),
        "topic_counts": dict(sorted(topic_counts.items())),
    }


if __name__ == "__main__":
    main()
