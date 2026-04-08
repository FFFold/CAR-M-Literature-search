import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


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
