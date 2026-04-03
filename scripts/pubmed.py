#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
ENV_KEY_NAMES = ("NCBI_API_KEY", "EUTILS_API_KEY", "API_KEY")
TIMEOUT_SECONDS = 30
SEARCH_BATCH_SIZE = 500
FETCH_BATCH_SIZE = 200
REQUEST_RETRIES = 3
REQUEST_SLEEP_SECONDS = 0.34
DEFAULT_OUTPUT_DIR = Path("output") / "pubmed_raw"
DEFAULT_CONFIG_PATH = Path("config") / "queries.json"


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
        help="PMIDs requested per esummary/efetch batch.",
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
    return parser.parse_args()


def load_env_file(path: Path) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not path.is_file():
        return values

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if (
            value
            and len(value) >= 2
            and value[0] == value[-1]
            and value[0] in {'"', "'"}
        ):
            value = value[1:-1]

        if key:
            values[key] = value

    return values


def resolve_api_key(cli_api_key: Optional[str]) -> Optional[str]:
    if cli_api_key:
        return cli_api_key

    for key_name in ENV_KEY_NAMES:
        value = os.environ.get(key_name)
        if value:
            return value

    cwd_env = Path.cwd() / ".env"
    project_root_env = Path(__file__).resolve().parent.parent / ".env"

    for env_path in (cwd_env, project_root_env):
        env_values = load_env_file(env_path)
        for key_name in ENV_KEY_NAMES:
            value = env_values.get(key_name)
            if value:
                return value

    return None


def chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_journal_name(value: str) -> str:
    cleaned = normalize_whitespace(value)
    cleaned = re.sub(r"[\.,:;()\[\]]", "", cleaned)
    return cleaned.lower()


def fetch_url(url: str) -> bytes:
    last_error: Optional[Exception] = None
    for attempt in range(REQUEST_RETRIES):
        try:
            with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
                payload = response.read()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return payload
        except (HTTPError, URLError) as exc:
            last_error = exc
            if attempt == REQUEST_RETRIES - 1:
                break
            time.sleep((attempt + 1) * REQUEST_SLEEP_SECONDS)

    if isinstance(last_error, HTTPError):
        raise RuntimeError(f"PubMed API HTTP error: {last_error.code}") from last_error
    if isinstance(last_error, URLError):
        raise RuntimeError(
            f"PubMed API connection error: {last_error.reason}"
        ) from last_error
    raise RuntimeError("PubMed API request failed")


def fetch_json(endpoint: str, params: Dict[str, str]) -> Dict:
    url = f"{EUTILS_BASE}/{endpoint}?{urlencode(params)}"
    payload = fetch_url(url)
    try:
        return json.loads(payload.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError("PubMed API returned invalid JSON") from exc


def fetch_xml(endpoint: str, params: Dict[str, str]) -> ET.Element:
    url = f"{EUTILS_BASE}/{endpoint}?{urlencode(params)}"
    payload = fetch_url(url)
    try:
        return ET.fromstring(payload)
    except ET.ParseError as exc:
        raise RuntimeError("PubMed API returned invalid XML") from exc


def load_topic_queries(
    config_path: Path, selected_topics: Optional[List[str]]
) -> List[Dict[str, str]]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    topics = payload.get("topics", [])
    if not selected_topics:
        return topics

    selected = set(selected_topics)
    filtered = [topic for topic in topics if topic.get("id") in selected]
    missing = sorted(selected - {topic.get("id") for topic in filtered})
    if missing:
        raise RuntimeError(f"Unknown topic ids: {', '.join(missing)}")
    return filtered


def esearch_page(
    query: str,
    api_key: Optional[str],
    retstart: int,
    retmax: int,
) -> Tuple[int, List[str]]:
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retstart": str(retstart),
        "retmax": str(retmax),
        "sort": "relevance",
    }
    if api_key:
        params["api_key"] = api_key

    payload = fetch_json("esearch.fcgi", params)
    search_result = payload.get("esearchresult", {})
    count = int(search_result.get("count", "0") or "0")
    return count, search_result.get("idlist", [])


def esearch_all(
    query: str,
    api_key: Optional[str],
    batch_size: int,
    max_records: Optional[int],
) -> List[str]:
    count, first_page = esearch_page(query, api_key, retstart=0, retmax=batch_size)
    pmids = list(first_page)

    target_count = count
    if max_records is not None:
        target_count = min(target_count, max_records)
        pmids = pmids[:target_count]

    retstart = len(first_page)
    while len(pmids) < target_count:
        remaining = target_count - len(pmids)
        _, page = esearch_page(
            query,
            api_key,
            retstart=retstart,
            retmax=min(batch_size, remaining),
        )
        if not page:
            break
        pmids.extend(page)
        retstart += len(page)

    return pmids[:target_count]


def esummary_batch(pmids: Sequence[str], api_key: Optional[str]) -> Dict[str, Dict]:
    if not pmids:
        return {}

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
    }
    if api_key:
        params["api_key"] = api_key

    payload = fetch_json("esummary.fcgi", params)
    result = payload.get("result", {})
    return {pmid: result.get(pmid, {}) for pmid in pmids if pmid in result}


def extract_text_list(parent: Optional[ET.Element], tag_name: str) -> List[str]:
    if parent is None:
        return []
    values = []
    for node in parent.findall(tag_name):
        text = normalize_whitespace("".join(node.itertext()))
        if text:
            values.append(text)
    return values


def parse_pubdate(article: ET.Element) -> Tuple[str, str, str]:
    journal_issue = article.find(
        "./MedlineCitation/Article/Journal/JournalIssue/PubDate"
    )
    article_date = article.find(
        "./PubmedData/History/PubMedPubDate[@PubStatus='pubmed']"
    )

    year = ""
    month = ""
    raw = ""

    if journal_issue is not None:
        raw = normalize_whitespace(
            " ".join(text for text in journal_issue.itertext() if text)
        )
        year = normalize_whitespace(journal_issue.findtext("Year", default=""))
        month = normalize_whitespace(journal_issue.findtext("Month", default=""))
        medline_date = normalize_whitespace(
            journal_issue.findtext("MedlineDate", default="")
        )
        if not year and medline_date:
            match = re.search(r"\b(19|20)\d{2}\b", medline_date)
            if match:
                year = match.group(0)
            raw = medline_date

    if article_date is not None and not year:
        year = normalize_whitespace(article_date.findtext("Year", default=""))
        month = normalize_whitespace(article_date.findtext("Month", default=""))
        raw = normalize_whitespace(
            " ".join(text for text in article_date.itertext() if text)
        )

    return raw, year, month


def efetch_batch(
    pmids: Sequence[str], api_key: Optional[str]
) -> Dict[str, Dict[str, str]]:
    if not pmids:
        return {}

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "xml",
    }
    if api_key:
        params["api_key"] = api_key

    root = fetch_xml("efetch.fcgi", params)
    items: Dict[str, Dict[str, str]] = {}

    for article in root.findall(".//PubmedArticle"):
        pmid = normalize_whitespace(
            article.findtext("./MedlineCitation/PMID", default="")
        )
        if not pmid:
            continue

        abstract_sections = extract_text_list(
            article.find("./MedlineCitation/Article/Abstract"),
            "AbstractText",
        )
        mesh_terms = extract_text_list(
            article.find("./MedlineCitation/MeshHeadingList"),
            "MeshHeading/DescriptorName",
        )
        publication_types = extract_text_list(
            article.find("./MedlineCitation/Article/PublicationTypeList"),
            "PublicationType",
        )
        raw_date, year, month = parse_pubdate(article)
        journal_raw = normalize_whitespace(
            article.findtext("./MedlineCitation/Article/Journal/Title", default="")
        )

        items[pmid] = {
            "abstract": "\n".join(section for section in abstract_sections if section),
            "mesh_terms": "; ".join(mesh_terms),
            "publication_types": "; ".join(publication_types),
            "publication_date_raw": raw_date,
            "publication_year": year,
            "publication_month": month,
            "journal_raw": journal_raw,
            "journal_normalized": normalize_journal_name(journal_raw),
        }

    return items


def extract_doi(entry: Dict) -> str:
    for article_id in entry.get("articleids", []):
        if article_id.get("idtype") == "doi":
            return article_id.get("value", "") or ""
    return ""


def collect_topic_records(
    topic: Dict[str, str],
    api_key: Optional[str],
    search_batch_size: int,
    fetch_batch_size: int,
    max_records: Optional[int],
) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    query = topic["full_query"]
    pmids = esearch_all(query, api_key, search_batch_size, max_records)
    records: Dict[str, Dict[str, str]] = {}

    for batch in chunked(pmids, fetch_batch_size):
        summary_map = esummary_batch(batch, api_key)
        detail_map = efetch_batch(batch, api_key)
        for pmid in batch:
            summary = summary_map.get(pmid, {})
            detail = detail_map.get(pmid, {})
            title = normalize_whitespace(summary.get("title", "") or "")
            journal_raw = detail.get("journal_raw") or normalize_whitespace(
                summary.get("fulljournalname", "") or summary.get("source", "") or ""
            )
            record = {
                "pmid": pmid,
                "title": title,
                "doi": extract_doi(summary),
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
                "source_query": query,
                "matched_topics": topic["id"],
                "matched_topic_labels": topic["label"],
            }
            records[pmid] = record

    return pmids, records


def merge_topic_records(
    topic_results: Sequence[
        Tuple[Dict[str, str], List[str], Dict[str, Dict[str, str]]]
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
            matched_topics.add(topic["id"])
            existing["matched_topics"] = ";".join(sorted(matched_topics))

            matched_labels = set(
                filter(None, existing["matched_topic_labels"].split(";"))
            )
            matched_labels.add(topic["label"])
            existing["matched_topic_labels"] = ";".join(sorted(matched_labels))

            if not existing.get("abstract") and record.get("abstract"):
                existing["abstract"] = record["abstract"]
            if not existing.get("mesh_terms") and record.get("mesh_terms"):
                existing["mesh_terms"] = record["mesh_terms"]
            if not existing.get("publication_types") and record.get(
                "publication_types"
            ):
                existing["publication_types"] = record["publication_types"]
            if not existing.get("source_query"):
                existing["source_query"] = record.get("source_query", "")

    return sorted(
        merged.values(),
        key=lambda item: (item.get("publication_year", ""), item["pmid"]),
    )


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


def run_direct_query(args: argparse.Namespace, api_key: Optional[str]) -> int:
    topic = {
        "id": "direct_query",
        "label": "Direct Query",
        "full_query": args.query,
    }
    pmids, records = collect_topic_records(
        topic=topic,
        api_key=api_key,
        search_batch_size=args.search_batch_size,
        fetch_batch_size=args.fetch_batch_size,
        max_records=args.max_records_per_topic,
    )
    rows = list(records.values())
    fieldnames = output_fieldnames()
    write_json(args.output_dir / "direct_query_pmids.json", pmids)
    write_json(args.output_dir / "direct_query_records.json", rows)
    write_csv(args.output_dir / "direct_query_records.csv", rows, fieldnames)
    print(f"Retrieved {len(rows)} PubMed records for direct query.")
    return 0


def output_fieldnames() -> List[str]:
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
    ]


def run_topic_config(args: argparse.Namespace, api_key: Optional[str]) -> int:
    topics = load_topic_queries(args.config, args.topics)
    topic_results: List[
        Tuple[Dict[str, str], List[str], Dict[str, Dict[str, str]]]
    ] = []
    summary_rows = []

    for topic in topics:
        print(f"Running topic {topic['id']}...", file=sys.stderr)
        pmids, records = collect_topic_records(
            topic=topic,
            api_key=api_key,
            search_batch_size=args.search_batch_size,
            fetch_batch_size=args.fetch_batch_size,
            max_records=args.max_records_per_topic,
        )
        topic_results.append((topic, pmids, records))
        summary_rows.append(
            {
                "topic_id": topic["id"],
                "topic_label": topic["label"],
                "record_count": str(len(pmids)),
            }
        )
        write_json(args.output_dir / "topics" / f"{topic['id']}_pmids.json", pmids)
        write_json(
            args.output_dir / "topics" / f"{topic['id']}_records.json",
            list(records.values()),
        )
        write_csv(
            args.output_dir / "topics" / f"{topic['id']}_records.csv",
            list(records.values()),
            output_fieldnames(),
        )

    merged_rows = merge_topic_records(topic_results)
    write_json(args.output_dir / "merged_records.json", merged_rows)
    write_csv(args.output_dir / "merged_records.csv", merged_rows, output_fieldnames())
    write_csv(
        args.output_dir / "topic_summary.csv",
        summary_rows,
        ["topic_id", "topic_label", "record_count"],
    )

    print(
        f"Retrieved {len(merged_rows)} unique PubMed records across {len(topics)} topics."
    )
    return 0


def main() -> int:
    args = parse_args()
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


if __name__ == "__main__":
    raise SystemExit(main())
