#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen


EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
ENV_KEY_NAMES = ("NCBI_API_KEY", "EUTILS_API_KEY", "API_KEY")
MAX_RESULTS = 5
TIMEOUT_SECONDS = 30


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Search PubMed and return the top 5 most relevant results."
    )
    parser.add_argument(
        "--query",
        required=True,
        help='PubMed-formatted English query, e.g. ""long covid"[MeSH] AND 2025[DP]"',
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
    skill_root_env = Path(__file__).resolve().parent.parent / ".env"

    for env_path in (cwd_env, skill_root_env):
        env_values = load_env_file(env_path)
        for key_name in ENV_KEY_NAMES:
            value = env_values.get(key_name)
            if value:
                return value

    return None


def fetch_json(endpoint: str, params: Dict[str, str]) -> Dict:
    url = f"{EUTILS_BASE}/{endpoint}?{urlencode(params)}"
    try:
        with urlopen(url, timeout=TIMEOUT_SECONDS) as response:
            return json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"PubMed API HTTP error: {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"PubMed API connection error: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError("PubMed API returned invalid JSON") from exc


def esearch(query: str, api_key: Optional[str]) -> List[str]:
    params = {
        "db": "pubmed",
        "term": query,
        "retmode": "json",
        "retmax": str(MAX_RESULTS),
        "sort": "relevance",
    }
    if api_key:
        params["api_key"] = api_key

    payload = fetch_json("esearch.fcgi", params)
    return payload.get("esearchresult", {}).get("idlist", [])[:MAX_RESULTS]


def esummary(pmids: List[str], api_key: Optional[str]) -> List[Dict[str, str]]:
    if not pmids:
        return []

    params = {
        "db": "pubmed",
        "id": ",".join(pmids),
        "retmode": "json",
    }
    if api_key:
        params["api_key"] = api_key

    payload = fetch_json("esummary.fcgi", params)
    result = payload.get("result", {})

    items: List[Dict[str, str]] = []
    for pmid in pmids:
        entry = result.get(pmid, {})
        doi = ""
        for article_id in entry.get("articleids", []):
            if article_id.get("idtype") == "doi":
                doi = article_id.get("value", "")
                break

        items.append(
            {
                "title": entry.get("title", "") or "",
                "pmid": pmid,
                "doi": doi,
                "link": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
            }
        )

    return items


def format_markdown(results: List[Dict[str, str]]) -> str:
    if not results:
        return "无相关文献"

    lines = []
    for item in results:
        lines.append(
            "- Title: {title}\n"
            "  PMID: {pmid}\n"
            "  DOI: {doi}\n"
            "  PubMed link: {link}".format(
                title=item["title"] or "N/A",
                pmid=item["pmid"] or "N/A",
                doi=item["doi"] or "N/A",
                link=item["link"],
            )
        )
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    api_key = resolve_api_key(args.api_key)

    try:
        pmids = esearch(args.query, api_key)
        if not pmids:
            print("无相关文献")
            return 0

        results = esummary(pmids, api_key)
        print(format_markdown(results))
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
