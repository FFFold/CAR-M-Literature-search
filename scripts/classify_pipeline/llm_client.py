"""OpenAI-compatible LLM API client using only the standard library."""

import json
import sys
import time
from typing import Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

DEFAULT_TIMEOUT = 120
DEFAULT_MAX_TOKENS = 1024
DEFAULT_TEMPERATURE = 0.1
REQUEST_RETRIES = 4
RETRY_BASE_WAIT = 2.0


def chat_completion(
    api_base: str,
    api_key: str,
    model: str,
    messages: List[Dict[str, str]],
    temperature: float = DEFAULT_TEMPERATURE,
    max_tokens: int = DEFAULT_MAX_TOKENS,
    timeout: int = DEFAULT_TIMEOUT,
) -> Optional[str]:
    """Call an OpenAI-compatible /v1/chat/completions endpoint.

    Returns the assistant message content string, or None on failure
    after all retries are exhausted.
    """
    url = f"{api_base.rstrip('/')}/chat/completions"
    payload = {
        "model": model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    last_error: Optional[Exception] = None
    for attempt in range(REQUEST_RETRIES):
        try:
            req = Request(url, data=body, headers=headers, method="POST")
            with urlopen(req, timeout=timeout) as resp:
                resp_data = json.loads(resp.read().decode("utf-8"))

            choices = resp_data.get("choices", [])
            if not choices:
                return None

            message = choices[0].get("message", {})
            content = message.get("content", "")
            return content if content else None

        except (
            HTTPError,
            URLError,
            TimeoutError,
            OSError,
            json.JSONDecodeError,
        ) as exc:
            last_error = exc
            if attempt < REQUEST_RETRIES - 1:
                wait = RETRY_BASE_WAIT * (2**attempt)
                error_detail = ""
                if isinstance(exc, HTTPError):
                    error_detail = f" HTTP {exc.code}"
                    # Don't retry on 4xx client errors (except 429 rate limit)
                    if 400 <= exc.code < 500 and exc.code != 429:
                        break
                print(
                    f"    LLM retry {attempt + 1}/{REQUEST_RETRIES - 1}"
                    f"{error_detail} ({type(exc).__name__}), "
                    f"waiting {wait:.0f}s...",
                    file=sys.stderr,
                )
                time.sleep(wait)

    if last_error:
        print(
            f"    LLM call failed after {REQUEST_RETRIES} attempts: {last_error}",
            file=sys.stderr,
        )
    return None
