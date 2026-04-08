#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
ENV_KEY_NAMES = ("NCBI_API_KEY", "EUTILS_API_KEY", "API_KEY")
PROXY_KEY_NAMES = (
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "http_proxy",
    "https_proxy",
    "no_proxy",
)
TIMEOUT_SECONDS = 90
SEARCH_BATCH_SIZE = 500
FETCH_BATCH_SIZE = 200
REQUEST_RETRIES = 3
REQUEST_SLEEP_SECONDS = 0.34
DEFAULT_OUTPUT_DIR = Path("output") / "pubmed_raw"
DEFAULT_CONFIG_PATH = Path("config") / "queries.json"
DEFAULT_CACHE_DIRNAME = "cache"
DEFAULT_QUERY_MODE = "broad"
NON_RESEARCH_PUBLICATION_TYPES = {
    "review",
    "systematic review",
    "meta-analysis",
    "editorial",
    "comment",
    "letter",
    "news",
    "biography",
    "interview",
    "lecture",
    "guideline",
    "practice guideline",
    "consensus development conference",
    "consensus development conference, nih",
    "historical article",
    "published erratum",
    "retraction of publication",
    "duplicate publication",
    "case reports",
}
NON_RESEARCH_TITLE_TERMS = (
    "review",
    "editorial",
    "commentary",
    "comment",
    "perspective",
    "perspectives",
    "guideline",
)


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
    parser.add_argument(
        "--query-mode",
        choices=("broad", "filtered"),
        default=DEFAULT_QUERY_MODE,
        help="Which configured query variant to run for topic-based retrieval.",
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


def load_json_file(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


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


def apply_proxy_environment() -> None:
    cwd_env = Path.cwd() / ".env"
    project_root_env = Path(__file__).resolve().parent.parent / ".env"

    for env_path in (cwd_env, project_root_env):
        env_values = load_env_file(env_path)
        for key_name in PROXY_KEY_NAMES:
            value = env_values.get(key_name)
            if value and not os.environ.get(key_name):
                os.environ[key_name] = value


def chunked(values: Sequence[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def normalize_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_journal_name(value: str) -> str:
    cleaned = normalize_whitespace(value)
    cleaned = re.sub(r"[\.,:;()\[\]]", "", cleaned)
    return cleaned.lower()


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def topic_attribution(topic: Dict[str, object]) -> Dict[str, object]:
    payload = topic.get("attribution", {})
    if isinstance(payload, dict):
        return payload
    return {}


def attribution_terms(topic: Dict[str, object], key: str) -> Tuple[str, ...]:
    payload = topic_attribution(topic).get(key, [])
    if not isinstance(payload, list):
        return ()
    return tuple(
        normalize_whitespace(str(item)).lower()
        for item in payload
        if normalize_whitespace(str(item))
    )


def attribution_conflict_terms(topic: Dict[str, object]) -> Dict[str, Tuple[str, ...]]:
    payload = topic_attribution(topic).get("conflict_terms", {})
    if not isinstance(payload, dict):
        return {}
    conflicts: Dict[str, Tuple[str, ...]] = {}
    for topic_id, values in payload.items():
        if not isinstance(values, list):
            continue
        conflicts[str(topic_id)] = tuple(
            normalize_whitespace(str(item)).lower()
            for item in values
            if normalize_whitespace(str(item))
        )
    return conflicts


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


def classify_record_filters(
    topic: Dict[str, object], title: str, abstract: str, publication_types: str
) -> Dict[str, str]:
    topic_id = str(topic.get("id", ""))
    normalized_types = {
        normalize_whitespace(item).lower()
        for item in publication_types.split(";")
        if normalize_whitespace(item)
    }

    title_lower = normalize_whitespace(title).lower()
    reasons = []

    excluded_types = sorted(normalized_types & NON_RESEARCH_PUBLICATION_TYPES)
    if excluded_types:
        reasons.append(f"publication_type:{','.join(excluded_types)}")

    hit_title_terms = sorted(
        term for term in NON_RESEARCH_TITLE_TERMS if term in title_lower
    )
    if hit_title_terms:
        reasons.append(f"title_noise:{','.join(hit_title_terms)}")

    abstract_text = normalize_whitespace(abstract)
    quality_flags = []
    if not abstract_text:
        quality_flags.append("missing_abstract")
    elif len(abstract_text) < 200:
        quality_flags.append("short_abstract")

    combined_text = f"{title_lower} {abstract_text.lower()}"
    primary_hints = attribution_terms(topic, "primary_title_abstract_phrases")
    cell_terms = attribution_terms(topic, "secondary_cell_terms")
    conflict_terms = attribution_conflict_terms(topic)
    has_primary_hint = any(hint in combined_text for hint in primary_hints)
    has_cell_term = any(term in combined_text for term in cell_terms)
    has_broad_topic_signal = has_car_core_signal(combined_text) and has_cell_term
    has_engineering_context = has_engineering_signal(combined_text)

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


def fetch_url(url: str) -> bytes:
    last_error: Optional[Exception] = None
    for attempt in range(REQUEST_RETRIES):
        try:
            with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
                payload = response.read()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return payload
        except (HTTPError, URLError, TimeoutError) as exc:
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
    if isinstance(last_error, TimeoutError):
        raise RuntimeError("PubMed API read timed out") from last_error
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
) -> List[Dict[str, object]]:
    payload = load_json_file(config_path)
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
        "sort": "pub date",
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


def make_cache_key(
    topic_id: str,
    query: str,
    search_batch_size: int,
    fetch_batch_size: int,
    max_records: Optional[int],
) -> str:
    raw = json.dumps(
        {
            "topic_id": topic_id,
            "query": query,
            "search_batch_size": search_batch_size,
            "fetch_batch_size": fetch_batch_size,
            "max_records": max_records,
        },
        sort_keys=True,
        ensure_ascii=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def cache_paths(output_dir: Path, topic_id: str, cache_key: str) -> Dict[str, Path]:
    topic_cache_dir = output_dir / DEFAULT_CACHE_DIRNAME / topic_id / cache_key
    return {
        "root": topic_cache_dir,
        "pmids": topic_cache_dir / "pmids.json",
        "summary": topic_cache_dir / "summary",
        "detail": topic_cache_dir / "detail",
        "raw_records": topic_cache_dir / "raw_records.json",
        "filtered_records": topic_cache_dir / "filtered_records.json",
        "review_records": topic_cache_dir / "review_records.json",
        "meta": topic_cache_dir / "meta.json",
    }


def load_cached_pmids(path: Path, max_records: Optional[int]) -> Optional[List[str]]:
    if not path.is_file():
        return None
    cached = load_json_file(path)
    if not isinstance(cached, list):
        return None
    pmids = [str(item) for item in cached]
    if max_records is not None:
        return pmids[:max_records]
    return pmids


def load_cached_mapping(path: Path) -> Optional[Dict[str, Dict]]:
    if not path.is_file():
        return None
    cached = load_json_file(path)
    if not isinstance(cached, dict):
        return None
    return cached


def load_cached_records(path: Path) -> Optional[Dict[str, Dict[str, str]]]:
    cached = load_cached_mapping(path)
    if cached is None:
        return None
    return {str(key): value for key, value in cached.items()}


def ensure_cache_dirs(paths: Dict[str, Path]) -> None:
    paths["summary"].mkdir(parents=True, exist_ok=True)
    paths["detail"].mkdir(parents=True, exist_ok=True)


def batch_cache_path(base_dir: Path, batch: Sequence[str]) -> Path:
    label = f"{batch[0]}_{batch[-1]}_{len(batch)}.json"
    return base_dir / label


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
        title = normalize_whitespace(
            article.findtext("./MedlineCitation/Article/ArticleTitle", default="")
        )
        doi = ""
        for article_id in article.findall("./PubmedData/ArticleIdList/ArticleId"):
            if article_id.attrib.get("IdType") == "doi":
                doi = normalize_whitespace("".join(article_id.itertext()))
                if doi:
                    break

        items[pmid] = {
            "title": title,
            "doi": doi,
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


def choose_topic_query(topic: Dict[str, object], query_mode: str) -> str:
    if query_mode == "filtered":
        return topic.get("filtered_query") or topic.get("broad_query") or ""
    return topic.get("broad_query") or topic.get("filtered_query") or ""


def build_raw_record(
    topic: Dict[str, object], query: str, pmid: str, detail: Dict[str, str]
) -> Dict[str, str]:
    title = normalize_whitespace(detail.get("title", "") or "")
    journal_raw = normalize_whitespace(detail.get("journal_raw", ""))
    record = {
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
        "matched_topics": topic["id"],
        "matched_topic_labels": topic["label"],
        "source_query": query,
        "source_query_id": topic["id"],
        "source_query_label": topic["label"],
        "filter_status": "",
        "filter_reason": "",
        "topic_filter_statuses": "",
        "topic_filter_reasons": "",
        "record_quality_flags": "",
        "needs_manual_review": "false",
    }
    record.update(
        classify_record_filters(
            topic,
            record["title"],
            record["abstract"],
            record["publication_types"],
        )
    )
    record["topic_filter_statuses"] = (
        f"{topic['id']}:{record['filter_status']}" if record["filter_status"] else ""
    )
    record["topic_filter_reasons"] = (
        f"{topic['id']}:{record['filter_reason']}" if record["filter_reason"] else ""
    )
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


def collect_topic_records(
    topic: Dict[str, object],
    api_key: Optional[str],
    search_batch_size: int,
    fetch_batch_size: int,
    max_records: Optional[int],
    output_dir: Path,
    query_mode: str,
) -> Tuple[
    List[str],
    Dict[str, Dict[str, str]],
    Dict[str, Dict[str, str]],
    Dict[str, Dict[str, str]],
    str,
]:
    query = choose_topic_query(topic, query_mode)
    if not query:
        raise RuntimeError(f"Topic {topic['id']} does not define a usable query")

    cache_key = make_cache_key(
        topic["id"],
        query,
        search_batch_size,
        fetch_batch_size,
        max_records,
    )
    paths = cache_paths(output_dir, topic["id"], cache_key)
    ensure_cache_dirs(paths)

    cached_raw_records = load_cached_records(paths["raw_records"])
    cached_filtered_records = load_cached_records(paths["filtered_records"])
    cached_review_records = load_cached_records(paths["review_records"])
    cached_pmids = load_cached_pmids(paths["pmids"], max_records)
    if (
        cached_raw_records is not None
        and cached_filtered_records is not None
        and cached_review_records is not None
        and cached_pmids is not None
    ):
        return (
            cached_pmids,
            cached_raw_records,
            cached_filtered_records,
            cached_review_records,
            query,
        )

    pmids = cached_pmids
    if pmids is None:
        pmids = esearch_all(query, api_key, search_batch_size, max_records)
        write_json(paths["pmids"], pmids)

    raw_records: Dict[str, Dict[str, str]] = {}

    for batch in chunked(pmids, fetch_batch_size):
        detail_cache = batch_cache_path(paths["detail"], batch)

        detail_map = load_cached_mapping(detail_cache)
        if detail_map is None:
            detail_map = efetch_batch(batch, api_key)
            write_json(detail_cache, detail_map)

        for pmid in batch:
            detail = detail_map.get(pmid, {})
            raw_records[pmid] = build_raw_record(topic, query, pmid, detail)

    filtered_records, review_records = split_records_by_filter(raw_records)

    write_json(paths["raw_records"], raw_records)
    write_json(paths["filtered_records"], filtered_records)
    write_json(paths["review_records"], review_records)
    write_json(
        paths["meta"],
        {
            "topic_id": topic["id"],
            "topic_label": topic["label"],
            "cache_key": cache_key,
            "query_mode": query_mode,
            "source_query": query,
            "pmid_count": len(pmids),
            "raw_record_count": len(raw_records),
            "filtered_record_count": len(filtered_records),
            "review_record_count": len(review_records),
            "fetch_batch_size": fetch_batch_size,
            "search_batch_size": search_batch_size,
            "max_records_per_topic": max_records,
            "retrieved_at": utc_timestamp(),
        },
    )

    return pmids, raw_records, filtered_records, review_records, query


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

    return sorted(
        merged.values(),
        key=lambda item: (
            item.get("publication_year", ""),
            item.get("publication_month", ""),
            item["pmid"],
        ),
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


def build_quality_summary_rows(
    topic_results: Sequence[
        Tuple[Dict[str, object], List[str], Dict[str, Dict[str, str]]]
    ],
    summary_rows: Sequence[Dict[str, str]],
) -> List[Dict[str, str]]:
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
    raw_records_by_topic = {
        topic["id"]: (topic, raw_records) for topic, _, raw_records in topic_results
    }
    quality_rows: List[Dict[str, str]] = []

    for row in summary_rows:
        topic_id = row["topic_id"]
        stats = counts_by_topic.get(topic_id, {})
        topic_payload = raw_records_by_topic.get(topic_id)
        topic_label = row.get("topic_label", "")
        raw_records = topic_payload[1] if topic_payload else {}
        doi_missing = 0
        year_missing = 0
        missing_abstract = 0
        short_abstract = 0
        exclusion_reasons: Dict[str, int] = {}
        for record in raw_records.values():
            if not record.get("doi"):
                doi_missing += 1
            if not record.get("publication_year"):
                year_missing += 1
            flags = set(
                filter(None, record.get("record_quality_flags", "").split("; "))
            )
            if "missing_abstract" in flags:
                missing_abstract += 1
            if "short_abstract" in flags:
                short_abstract += 1
            for reason in filter(None, record.get("filter_reason", "").split("; ")):
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
        "source_query_id",
        "source_query_label",
        "filter_status",
        "filter_reason",
        "topic_filter_statuses",
        "topic_filter_reasons",
        "record_quality_flags",
        "needs_manual_review",
    ]


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
            print(
                f"Topic {topic['id']} failed: {message}",
                file=sys.stderr,
            )

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
                "summary": "Cached esummary batch responses.",
                "detail": "Cached efetch batch responses.",
                "raw_records": "Cached normalized topic records before filtering.",
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


if __name__ == "__main__":
    raise SystemExit(main())
