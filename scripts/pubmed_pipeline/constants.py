from pathlib import Path


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
