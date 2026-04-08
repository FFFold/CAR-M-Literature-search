import json
import re
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from .constants import (
    EUTILS_BASE,
    REQUEST_RETRIES,
    REQUEST_SLEEP_SECONDS,
    TIMEOUT_SECONDS,
)
from .utils import normalize_journal_name, normalize_whitespace


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
