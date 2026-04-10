"""Batch classification pipeline: coordinates LLM calls, caching, and validation."""

import sys
from pathlib import Path
from typing import Dict, Optional

from .cache import (
    classification_cache_dir,
    load_cached_classification,
    save_classification,
)
from .llm_client import chat_completion
from .prompt import SYSTEM_PROMPT, build_user_message, parse_llm_response
from .validator import make_fallback_classification, validate_classification


def classify_records(
    records: Dict[str, Dict[str, str]],
    output_dir: Path,
    api_base: str,
    api_key: str,
    model: str,
    temperature: float = 0.1,
    max_tokens: int = 1024,
) -> Dict[str, Dict[str, str]]:
    """Classify all records, using cache for already-classified ones.

    Returns a dict mapping PMID -> classification result (with
    needs_manual_review and review_reasons fields added).
    """
    cache_dir = classification_cache_dir(output_dir)
    total = len(records)
    results: Dict[str, Dict[str, str]] = {}
    cached_count = 0
    classified_count = 0
    failed_count = 0

    pmids = sorted(records.keys())

    for i, pmid in enumerate(pmids, start=1):
        record = records[pmid]

        # Check cache first
        cached = load_cached_classification(cache_dir, pmid)
        if cached is not None:
            results[pmid] = cached
            cached_count += 1
        else:
            # Call LLM
            classification = _classify_single(
                record,
                api_base,
                api_key,
                model,
                temperature,
                max_tokens,
            )

            if classification is not None:
                validated, review_reasons = validate_classification(
                    record,
                    classification,
                )
                classified_count += 1
            else:
                validated, review_reasons = make_fallback_classification(record)
                failed_count += 1

            # Add review metadata
            validated["needs_manual_review"] = "true" if review_reasons else "false"
            validated["review_reasons"] = "; ".join(review_reasons)

            # Cache the result
            save_classification(cache_dir, pmid, validated)
            results[pmid] = validated

        # Progress reporting (includes cache hits)
        if i % 100 == 0 or i == total:
            print(
                f"  Progress: {i}/{total} "
                f"(cached={cached_count}, classified={classified_count}, "
                f"failed={failed_count})",
                file=sys.stderr,
            )

    print(
        f"  Classification complete: {total} records "
        f"(cached={cached_count}, classified={classified_count}, "
        f"failed={failed_count})",
        file=sys.stderr,
    )
    return results


def _classify_single(
    record: Dict[str, str],
    api_base: str,
    api_key: str,
    model: str,
    temperature: float,
    max_tokens: int,
) -> Optional[Dict[str, str]]:
    """Classify a single record via the LLM API."""
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_message(record)},
    ]

    raw_response = chat_completion(
        api_base=api_base,
        api_key=api_key,
        model=model,
        messages=messages,
        temperature=temperature,
        max_tokens=max_tokens,
    )

    if raw_response is None:
        return None

    parsed = parse_llm_response(raw_response)
    return parsed
