"""Cache layer for per-PMID classification results."""

import json
from pathlib import Path
from typing import Dict, Optional


def classification_cache_dir(output_dir: Path) -> Path:
    return output_dir / "cache" / "classifications"


REQUIRED_CACHE_FIELDS = ("primary_mechanism", "relevance")


def load_cached_classification(cache_dir: Path, pmid: str) -> Optional[Dict[str, str]]:
    """Load a cached classification result for a single PMID.

    Returns None if the cache file is missing, corrupt, or was written
    by an older schema version that lacks required fields (e.g. relevance).
    This forces re-classification with the current schema.
    """
    path = cache_dir / f"{pmid}.json"
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
        if isinstance(data, dict) and all(f in data for f in REQUIRED_CACHE_FIELDS):
            return data
    except (json.JSONDecodeError, OSError):
        pass
    return None


def save_classification(
    cache_dir: Path, pmid: str, classification: Dict[str, str]
) -> None:
    """Save a classification result for a single PMID."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{pmid}.json"
    path.write_text(
        json.dumps(classification, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def load_all_cached(cache_dir: Path) -> Dict[str, Dict[str, str]]:
    """Load all cached classifications from the cache directory."""
    results: Dict[str, Dict[str, str]] = {}
    if not cache_dir.exists():
        return results
    for path in cache_dir.glob("*.json"):
        pmid = path.stem
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(data, dict) and "primary_mechanism" in data:
                results[pmid] = data
        except (json.JSONDecodeError, OSError):
            continue
    return results
