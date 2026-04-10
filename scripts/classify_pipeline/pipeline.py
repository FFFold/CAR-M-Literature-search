"""Batch classification pipeline: coordinates LLM calls, caching, and validation.

Supports concurrent execution via ThreadPoolExecutor and parse-level
retries when the LLM returns unparseable output.
"""

import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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

PARSE_RETRIES = 2  # extra LLM calls when response is not valid JSON


def classify_records(
    records: Dict[str, Dict[str, str]],
    output_dir: Path,
    api_base: str,
    api_key: str,
    model: str,
    temperature: float = 0.1,
    max_tokens: int = 1024,
    workers: int = 1,
) -> Dict[str, Dict[str, str]]:
    """Classify all records, using cache for already-classified ones.

    Args:
        workers: number of concurrent LLM calls (1 = serial).

    Returns a dict mapping PMID -> classification result (with
    needs_manual_review and review_reasons fields added).
    """
    cache_dir = classification_cache_dir(output_dir)
    total = len(records)
    results: Dict[str, Dict[str, str]] = {}

    # Split into cached and uncached
    uncached_pmids = []
    for pmid in sorted(records.keys()):
        cached = load_cached_classification(cache_dir, pmid)
        if cached is not None:
            results[pmid] = cached
        else:
            uncached_pmids.append(pmid)

    cached_count = len(results)
    if cached_count > 0:
        print(
            f"  Loaded {cached_count} cached classifications, "
            f"{len(uncached_pmids)} remaining.",
            file=sys.stderr,
        )

    if not uncached_pmids:
        print(
            f"  Classification complete: {total} records (all cached)",
            file=sys.stderr,
        )
        return results

    classified_count = 0
    failed_count = 0
    lock = threading.Lock()

    def _process_one(pmid: str) -> None:
        nonlocal classified_count, failed_count
        record = records[pmid]

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
            with lock:
                classified_count += 1
        else:
            validated, review_reasons = make_fallback_classification(record)
            with lock:
                failed_count += 1

        validated["needs_manual_review"] = "true" if review_reasons else "false"
        validated["review_reasons"] = "; ".join(review_reasons)

        save_classification(cache_dir, pmid, validated)

        with lock:
            results[pmid] = validated
            done = cached_count + classified_count + failed_count
            if done % 50 == 0 or done == total:
                print(
                    f"  Progress: {done}/{total} "
                    f"(cached={cached_count}, classified={classified_count}, "
                    f"failed={failed_count})",
                    file=sys.stderr,
                )

    effective_workers = max(1, min(workers, len(uncached_pmids)))

    if effective_workers == 1:
        for pmid in uncached_pmids:
            _process_one(pmid)
    else:
        print(
            f"  Using {effective_workers} concurrent workers.",
            file=sys.stderr,
        )
        with ThreadPoolExecutor(max_workers=effective_workers) as pool:
            futures = {pool.submit(_process_one, pmid): pmid for pmid in uncached_pmids}
            for future in as_completed(futures):
                exc = future.exception()
                if exc is not None:
                    pmid = futures[future]
                    print(
                        f"    Unexpected error for PMID {pmid}: {exc}",
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
    """Classify a single record via the LLM API with parse-level retries.

    If the LLM returns text that cannot be parsed as JSON, retries
    up to PARSE_RETRIES additional times before giving up.
    """
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_message(record)},
    ]

    for attempt in range(1 + PARSE_RETRIES):
        raw_response = chat_completion(
            api_base=api_base,
            api_key=api_key,
            model=model,
            messages=messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        if raw_response is None:
            # Network-level failure — already retried inside chat_completion
            return None

        parsed = parse_llm_response(raw_response)
        if parsed is not None:
            return parsed

        # Parse failed — retry with a nudge
        if attempt < PARSE_RETRIES:
            pmid = record.get("pmid", "?")
            print(
                f"    PMID {pmid}: parse retry {attempt + 1}/{PARSE_RETRIES} "
                f"(response was not valid JSON)",
                file=sys.stderr,
            )
            # Append the failed response and a correction hint
            messages = messages + [
                {"role": "assistant", "content": raw_response},
                {
                    "role": "user",
                    "content": (
                        "Your response could not be parsed as JSON. "
                        "Please return ONLY the JSON object, nothing else."
                    ),
                },
            ]

    return None
