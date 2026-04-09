import calendar
import datetime
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .constants import (
    EUTILS_BASE,
    REQUEST_RETRIES,
    REQUEST_SLEEP_SECONDS,
    TIMEOUT_SECONDS,
)
from .utils import normalize_journal_name, normalize_whitespace


def fetch_url(url: str, post_data: Optional[bytes] = None) -> bytes:
    """Fetch a URL with retries and exponential backoff.

    Supports both GET and POST. Uses POST when post_data is provided,
    which is useful for long queries that may exceed URL length limits.
    """
    last_error: Optional[Exception] = None
    for attempt in range(REQUEST_RETRIES):
        try:
            if post_data is not None:
                req = Request(url, data=post_data)
                req.add_header("Content-Type", "application/x-www-form-urlencoded")
            else:
                req = Request(url)
            with urlopen(req, timeout=TIMEOUT_SECONDS) as response:
                payload = response.read()
            time.sleep(REQUEST_SLEEP_SECONDS)
            return payload
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            last_error = exc
            if attempt == REQUEST_RETRIES - 1:
                break
            wait = REQUEST_SLEEP_SECONDS * (2 ** (attempt + 1))
            print(
                f"    network retry {attempt + 1}/{REQUEST_RETRIES - 1} "
                f"({type(exc).__name__}), waiting {wait:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(wait)

    if isinstance(last_error, HTTPError):
        raise RuntimeError(f"PubMed API HTTP error: {last_error.code}") from last_error
    if isinstance(last_error, URLError):
        raise RuntimeError(
            f"PubMed API connection error: {last_error.reason}"
        ) from last_error
    if isinstance(last_error, TimeoutError):
        raise RuntimeError("PubMed API read timed out") from last_error
    raise RuntimeError(f"PubMed API request failed: {last_error}") from last_error


def _eutils_request(
    endpoint: str, params: Dict[str, str], use_post: bool = False
) -> bytes:
    """Send a request to the NCBI E-utilities API.

    Uses POST when use_post is True or URL would exceed 2000 chars,
    which avoids truncation for long queries.
    """
    url = f"{EUTILS_BASE}/{endpoint}"
    encoded = urlencode(params)
    if use_post or len(url) + 1 + len(encoded) > 2000:
        return fetch_url(url, post_data=encoded.encode("utf-8"))
    return fetch_url(f"{url}?{encoded}")


PARSE_RETRIES = 3


def fetch_json(endpoint: str, params: Dict[str, str]) -> Dict:
    """Fetch JSON from E-utilities with parse-level retries."""
    last_error: Optional[Exception] = None
    for attempt in range(PARSE_RETRIES):
        payload = _eutils_request(endpoint, params)
        try:
            return json.loads(payload.decode("utf-8"))
        except json.JSONDecodeError as exc:
            last_error = exc
            if attempt < PARSE_RETRIES - 1:
                wait = REQUEST_SLEEP_SECONDS * (2 ** (attempt + 1))
                print(
                    f"    parse retry {attempt + 1}/{PARSE_RETRIES - 1} "
                    f"(invalid JSON, got {len(payload)} bytes), waiting {wait:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait)
    raise RuntimeError(
        f"PubMed API returned invalid JSON after {PARSE_RETRIES} attempts"
    ) from last_error


def fetch_xml(endpoint: str, params: Dict[str, str]) -> ET.Element:
    """Fetch XML from E-utilities with parse-level retries."""
    last_error: Optional[Exception] = None
    for attempt in range(PARSE_RETRIES):
        payload = _eutils_request(endpoint, params)
        try:
            return ET.fromstring(payload)
        except ET.ParseError as exc:
            last_error = exc
            if attempt < PARSE_RETRIES - 1:
                wait = REQUEST_SLEEP_SECONDS * (2 ** (attempt + 1))
                print(
                    f"    parse retry {attempt + 1}/{PARSE_RETRIES - 1} "
                    f"(invalid XML, got {len(payload)} bytes), waiting {wait:.1f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait)
    raise RuntimeError(
        f"PubMed API returned invalid XML after {PARSE_RETRIES} attempts"
    ) from last_error


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


ESEARCH_MAX_RECORDS = 9999


def esearch_all(
    query: str,
    api_key: Optional[str],
    batch_size: int,
    max_records: Optional[int],
) -> List[str]:
    """Retrieve all PMIDs matching a query.

    PubMed esearch has a hard limit of 9999 records per search.  When the
    total count exceeds this, the query is automatically split into yearly
    date-range slices to work around the restriction.
    """
    # First, get the total count
    total_count, _ = esearch_page(query, api_key, retstart=0, retmax=0)

    target_count = total_count
    if max_records is not None:
        target_count = min(target_count, max_records)

    if target_count <= ESEARCH_MAX_RECORDS:
        return _esearch_simple(query, api_key, batch_size, target_count)

    # Need to split by year to work around the 9999 limit
    print(
        f"    {total_count} results exceed esearch limit ({ESEARCH_MAX_RECORDS}), "
        f"splitting by year...",
        file=sys.stderr,
    )
    return _esearch_by_year(query, api_key, batch_size, max_records)


def _esearch_simple(
    query: str,
    api_key: Optional[str],
    batch_size: int,
    target_count: int,
) -> List[str]:
    """Paginate through esearch results for queries within the 9999 limit."""
    _, first_page = esearch_page(query, api_key, retstart=0, retmax=batch_size)
    pmids = list(first_page)[:target_count]

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


def _esearch_by_year(
    query: str,
    api_key: Optional[str],
    batch_size: int,
    max_records: Optional[int],
) -> List[str]:
    """Split a large query into yearly date-range slices.

    Uses PubMed date range filter to keep each slice under 9999.
    Scans from the current year back to 1900.
    """
    current_year = datetime.date.today().year
    all_pmids: List[str] = []
    seen: set = set()

    for year in range(current_year, 1899, -1):
        year_query = f'({query}) AND ("{year}/01/01"[Date - Publication] : "{year}/12/31"[Date - Publication])'
        year_count, _ = esearch_page(year_query, api_key, retstart=0, retmax=0)
        if year_count == 0:
            continue

        print(
            f"    {year}: {year_count} records...",
            file=sys.stderr,
        )

        if year_count > ESEARCH_MAX_RECORDS:
            # Extremely rare: split further by month
            for month in range(1, 13):
                last_day = calendar.monthrange(year, month)[1]
                month_query = (
                    f"({query}) AND "
                    f'("{year}/{month:02d}/01"[Date - Publication] : '
                    f'"{year}/{month:02d}/{last_day}"[Date - Publication])'
                )
                month_pmids = _esearch_simple(
                    month_query,
                    api_key,
                    batch_size,
                    min(ESEARCH_MAX_RECORDS, year_count),
                )
                for pmid in month_pmids:
                    if pmid not in seen:
                        seen.add(pmid)
                        all_pmids.append(pmid)
        else:
            year_pmids = _esearch_simple(
                year_query,
                api_key,
                batch_size,
                year_count,
            )
            for pmid in year_pmids:
                if pmid not in seen:
                    seen.add(pmid)
                    all_pmids.append(pmid)

        if max_records is not None and len(all_pmids) >= max_records:
            all_pmids = all_pmids[:max_records]
            break

    return all_pmids


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
