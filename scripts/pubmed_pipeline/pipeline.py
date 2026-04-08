from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .cache import (
    batch_cache_path,
    cache_paths,
    ensure_cache_dirs,
    load_cached_mapping,
    load_cached_pmids,
    load_cached_records,
    make_cache_key,
)
from .client import efetch_batch, esearch_all
from .config import choose_topic_query
from .filters import annotate_record_for_topic, split_records_by_filter
from .outputs import write_json
from .records import build_raw_record
from .utils import chunked, utc_timestamp


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
        str(topic["id"]),
        query,
        search_batch_size,
        fetch_batch_size,
        max_records,
    )
    paths = cache_paths(output_dir, str(topic["id"]), cache_key)
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

    screened_records = {
        pmid: annotate_record_for_topic(topic, record)
        for pmid, record in raw_records.items()
    }
    filtered_records, review_records = split_records_by_filter(screened_records)

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
