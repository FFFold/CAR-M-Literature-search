import hashlib
import json
from pathlib import Path
from typing import Dict, Optional, Sequence

from .constants import DEFAULT_CACHE_DIRNAME
from .utils import load_json_file


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
        "detail": topic_cache_dir / "detail",
        "raw_records": topic_cache_dir / "raw_records.json",
        "filtered_records": topic_cache_dir / "filtered_records.json",
        "review_records": topic_cache_dir / "review_records.json",
        "meta": topic_cache_dir / "meta.json",
    }


def load_cached_pmids(path: Path, max_records: Optional[int]) -> Optional[list[str]]:
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
    paths["detail"].mkdir(parents=True, exist_ok=True)


def batch_cache_path(base_dir: Path, batch: Sequence[str]) -> Path:
    label = f"{batch[0]}_{batch[-1]}_{len(batch)}.json"
    return base_dir / label
