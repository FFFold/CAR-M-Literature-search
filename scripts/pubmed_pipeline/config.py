from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .utils import load_json_file, normalize_whitespace


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


def choose_topic_query(topic: Dict[str, object], query_mode: str) -> str:
    if query_mode == "filtered":
        return topic.get("filtered_query") or topic.get("broad_query") or ""
    return topic.get("broad_query") or topic.get("filtered_query") or ""


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
