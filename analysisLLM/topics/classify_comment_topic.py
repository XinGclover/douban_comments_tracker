from __future__ import annotations

import json
import logging
import os
import random
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from requests.exceptions import ConnectionError, ReadTimeout

from db import get_db_conn
from utils.logger import setup_logger
import re
from analysisLLM.llm.ollama_client import call_ollama_with_retry

# =========================
# config
# =========================

BASE_DIR = Path(__file__).resolve().parent.parent
PROMPT_DIR = BASE_DIR / "prompts"

TOPIC_PROMPT_PATH = PROMPT_DIR / "topic_prompt.txt"
TOPIC_TAXONOMY_PATH = PROMPT_DIR / "topic_taxonomy_short.txt"

LOG_PATH = BASE_DIR.parent / "logs" / "classify_comment_topic.log"
setup_logger(str(LOG_PATH), logging.INFO)

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL_NAME = os.getenv("OLLAMA_MODEL", "qwen3:4b")
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "v2_short_taxonomy_thinking_fallback").strip()

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
SLEEP_BETWEEN_ITEMS = 0.3

VALID_TOPICS = {
    "PLOT",
    "ACTING",
    "CAST",
    "PRODUCTION",
    "RATING",
    "PLATFORM_CAPITAL",
    "INFO",
    "META_FANDOM",
    "OTHER",
}


FETCH_SQL = """
SELECT
    comment_id,
    user_id,
    drama_id,
    comment,
    rating,
    comment_time,
    ip_location,
    comment_length
FROM public_marts.fact_comments fc
WHERE fc.comment IS NOT NULL
  AND btrim(fc.comment) <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM ai_raw.comment_topic_labels_raw r
      WHERE r.comment_id = fc.comment_id
        AND r.prompt_version = %s
        AND r.model_name = %s
  )
ORDER BY fc.comment_id
LIMIT %s;
"""

INSERT_SQL = """
INSERT INTO ai_raw.comment_topic_labels_raw (
    comment_id,
    primary_topic,
    secondary_topics,
    confidence,
    model_name,
    prompt_version,
    raw_response
)
VALUES (%s, %s, %s, %s, %s, %s, %s);
"""


# =========================
# helpers
# =========================

def load_text(path: Path) -> str:
    return path.read_text(encoding="utf-8").strip()


def fetch_pending_comments(limit: int) -> list[dict[str, Any]]:
    conn = get_db_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(FETCH_SQL, (PROMPT_VERSION, MODEL_NAME, limit))
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    finally:
        conn.close()


def build_prompt(
    comment: str,
    base_prompt: str,
    taxonomy_text: str,
) :

    return f"""{base_prompt}

        [TAXONOMY]
        {taxonomy_text}

        [COMMENT]
        {comment}
        """


def extract_json_object(text: str) -> str | None:
    text = text.strip()

    # 1) try full text first
    try:
        json.loads(text)
        return text
    except Exception:
        pass

    # 2) extract first {...}
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        return None

    candidate = match.group(0)

    try:
        json.loads(candidate)
        return candidate
    except Exception:
        return None


def parse_llm_response(resp_text: str) -> dict[str, Any]:
    candidate = extract_json_object(resp_text)

    if not candidate:
        logging.warning("No valid JSON found, fallback to OTHER. resp=%r", resp_text[:300])
        return {
            "primary_topic": "OTHER",
            "secondary_topics": [],
            "confidence": None,
            "raw_json": None,
        }

    try:
        data = json.loads(candidate)
        clean_json = json.dumps(data, ensure_ascii=False)
    except json.JSONDecodeError:
        logging.warning("Invalid JSON response, fallback to OTHER. resp=%r", resp_text[:300])
        return {
            "primary_topic": "OTHER",
            "secondary_topics": [],
            "confidence": None,
        }

    primary_topic = normalize_topic(data.get("primary_topic")) or "OTHER"
    secondary_topics = normalize_secondary_topics(data.get("secondary_topics"), primary_topic)
    confidence = normalize_confidence(data.get("confidence"))

    return {
        "primary_topic": primary_topic,
        "secondary_topics": secondary_topics,
        "confidence": confidence,
        "raw_json": clean_json,
    }

def strip_code_fence(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if len(lines) >= 2:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text


def normalize_topic(value: Any) -> str | None:
    if value is None:
        return None
    topic = str(value).strip().upper()
    return topic if topic in VALID_TOPICS else None


def normalize_secondary_topics(value: Any, primary_topic: str) -> list[str]:
    if not value:
        return []

    if isinstance(value, str):
        # Compatibility with occasionally returning strings instead of arrays
        value = [value]

    if not isinstance(value, list):
        return []

    cleaned: list[str] = []
    seen = set()

    for item in value:
        topic = normalize_topic(item)
        if not topic:
            continue
        if topic == primary_topic:
            continue
        if topic in seen:
            continue
        seen.add(topic)
        cleaned.append(topic)

    return cleaned


def normalize_confidence(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None

    if num < 0:
        num = 0.0
    elif num > 1:
        num = 1.0

    return Decimal(f"{num:.4f}")


def insert_result(
    comment_id: int,
    primary_topic: str,
    secondary_topics: list[str],
    confidence: Decimal | None,
    raw_response: str,
) -> None:
    conn = get_db_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                INSERT_SQL,
                (
                    comment_id,
                    primary_topic,
                    json.dumps(secondary_topics, ensure_ascii=False) if secondary_topics else None,
                    confidence,
                    MODEL_NAME,
                    PROMPT_VERSION,
                    raw_response,
                ),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def process_one_comment(row: dict[str, Any], base_prompt: str, taxonomy_text: str) -> None:
    comment_id = row["comment_id"]
    comment = row["comment"]
    prompt = build_prompt(
        comment=comment,
        base_prompt=base_prompt,
        taxonomy_text=taxonomy_text
    )

    resp_text = call_ollama_with_retry(
        prompt=prompt,
        model=MODEL_NAME,
        url=OLLAMA_URL,
        timeout=REQUEST_TIMEOUT,
        format_json=True,
        options={
            "temperature": 0.0,
            "num_predict": 80,
            "num_ctx": 4096,
            "think": False,
        },
        fallback_thinking=True,
        max_retries=3,
    )
    parsed = parse_llm_response(resp_text)

    insert_result(
        comment_id=comment_id,
        primary_topic=parsed["primary_topic"],
        secondary_topics=parsed["secondary_topics"],
        confidence=parsed["confidence"],
        raw_response=parsed["raw_json"],
    )

    logging.info(
        "Saved topic label: comment_id=%s primary_topic=%s confidence=%s",
        comment_id,
        parsed["primary_topic"],
        parsed["confidence"],
    )


def main() -> None:
    base_prompt = load_text(TOPIC_PROMPT_PATH)
    taxonomy_text = load_text(TOPIC_TAXONOMY_PATH)

    rows = fetch_pending_comments(BATCH_SIZE)

    if not rows:
        logging.info("No pending comments to classify.")
        return

    logging.info("🚀 Start classifying %s comments...", len(rows))

    success_count = 0
    error_count = 0

    for idx, row in enumerate(rows, start=1):
        comment_id = row["comment_id"]
        try:
            logging.info("📄 Classifying %s/%s comment_id=%s", idx, len(rows), comment_id)
            process_one_comment(row, base_prompt, taxonomy_text)
            success_count += 1
            time.sleep(SLEEP_BETWEEN_ITEMS)
        except KeyboardInterrupt:
            logging.warning("⚠️ Interrupted by user. Stopping early.")
            break
        except Exception as e:
            error_count += 1
            logging.exception("❌ Failed comment_id=%s error=%r", comment_id, e)

    logging.info(
        "✅ Done. total=%s success=%s error=%s",
        len(rows),
        success_count,
        error_count,
    )


if __name__ == "__main__":
    main()
    logging.shutdown()