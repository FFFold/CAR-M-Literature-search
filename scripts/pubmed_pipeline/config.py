from pathlib import Path
from typing import Dict, List, Optional

from .utils import load_json_file


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
