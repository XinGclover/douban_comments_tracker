from __future__ import annotations

import logging
import random
import time
from typing import Any

import requests
from requests.exceptions import ConnectionError, ReadTimeout


def call_ollama(
    prompt: str,
    model: str,
    url: str,
    timeout: int = 120,
    format_json: bool = False,
    options: dict[str, Any] | None = None,
    fallback_thinking: bool = True,
) -> str:
    payload: dict[str, Any] = {
        "model": model,
        "prompt": prompt,
        "stream": False,
        "options": options or {},
    }

    if format_json:
        payload["format"] = "json"

    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()

    resp = (data.get("response") or "").strip()

    if not resp and fallback_thinking:
        for key in ("response", "thinking", "reasoning", "reasoning_content"):
            val = data.get(key)
            if isinstance(val, str) and val.strip():
                resp = val.strip()
                logging.warning("response empty, fallback to %s", key)
                break

    logging.info(
        "ollama meta: model=%s done=%r done_reason=%r error=%r resp_len=%s",
        model,
        data.get("done"),
        data.get("done_reason"),
        data.get("error"),
        len(resp),
    )
    logging.debug("ollama raw keys=%s", list(data.keys()))
    logging.debug("ollama resp_head=%r", resp[:300])

    if not resp:
        raise ValueError(
            f"ollama response empty. done_reason={data.get('done_reason')} keys={list(data.keys())}"
        )

    return resp


def call_ollama_with_retry(
    prompt: str,
    model: str,
    url: str,
    timeout: int = 120,
    format_json: bool = False,
    options: dict[str, Any] | None = None,
    fallback_thinking: bool = True,
    max_retries: int = 3,
) -> str:
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            return call_ollama(
                prompt=prompt,
                model=model,
                url=url,
                timeout=timeout,
                format_json=format_json,
                options=options,
                fallback_thinking=fallback_thinking,
            )
        except (ReadTimeout, ConnectionError, requests.HTTPError, ValueError) as e:
            last_err = e
            sleep_s = min(2 ** attempt, 8) + random.random()
            logging.warning(
                "Ollama failed, retry %s/%s after %.1fs: %r",
                attempt,
                max_retries,
                sleep_s,
                e,
            )
            time.sleep(sleep_s)

    raise last_err