import json
import logging
import os
import random
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from requests.exceptions import ConnectionError, ReadTimeout

from db import get_db_conn
from utils.logger import setup_logger

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "label_posts_with_llm.log"
setup_logger(log_file=str(LOG_PATH))

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL = os.getenv("OLLAMA_MODEL", "qwen3:4b")
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "v1_qwen3_4b_json").strip()
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))


PROMPT_TEMPLATE = """
You are a Chinese entertainment opinion classifier.

IMPORTANT RULES:
- Output ONE line of JSON only.
- Do NOT explain.
- Do NOT think aloud.
- Do NOT include analysis, markdown, or extra text.

JSON schema:
{
  "label":"hater|fan|neutral",
  "final_attitude_to_landy":"support|attack|neutral",
  "ai_is_rebuttal": true|false,
  "ai_mention_negative_claim": true|false,
  "confidence":0.00,
  "sentiment":"positive|neutral|negative",
  "is_sarcasm":false,
  "reason":"一句话解释（<=20字）"
}

Comment:
<<COMMENT>>
""".strip()


LABEL_MAP = {"hater": "hater", "fan": "fan", "neutral": "neutral"}
ZH_MAP = {"黑子": "hater", "粉丝": "fan", "路人": "neutral"}

POST_SQL = """
SELECT
  r.user_id,
  r.topic_id,
  r.post_type,
  r.floor_no,
  r.content_text AS content
FROM public.douban_topic_post_raw r
WHERE r.content_text IS NOT NULL
  AND length(btrim(r.content_text)) > 0
  AND NOT EXISTS (
    SELECT 1
    FROM public.douban_topic_post_ai a
    WHERE a.topic_id = r.topic_id
      AND a.post_type = r.post_type
      AND a.floor_no  = r.floor_no
      AND btrim(a.prompt_version) = btrim(%(prompt_version)s)
  )
ORDER BY r.topic_id, r.post_type, r.floor_no
LIMIT %(batch_size)s;
"""

INSERT_SQL = """
INSERT INTO public.douban_topic_post_ai (
  user_id,
  topic_id,
  post_type,
  floor_no,
  ai_label,
  ai_confidence,
  ai_sentiment,
  ai_is_sarcasm,
  ai_reason,
  ai_result,
  ai_model,
  prompt_version,
  labeled_at,
  final_attitude_to_landy,
  ai_is_rebuttal,
  ai_mention_negative_claim
)
VALUES (
  %(user_id)s,
  %(topic_id)s,
  %(post_type)s,
  %(floor_no)s,
  %(ai_label)s,
  %(ai_confidence)s,
  %(ai_sentiment)s,
  %(ai_is_sarcasm)s,
  %(ai_reason)s,
  %(ai_result)s::jsonb,
  %(ai_model)s,
  %(prompt_version)s,
  now(),
  %(final_attitude_to_landy)s,
  %(ai_is_rebuttal)s,
  %(ai_mention_negative_claim)s
)
ON CONFLICT (topic_id, post_type, floor_no, prompt_version)
DO NOTHING;
"""


def build_prompt(comment: str) -> str:
    return PROMPT_TEMPLATE.replace("<<COMMENT>>", comment.strip())


def call_ollama(prompt: str) -> str:
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "format": "json",     # JSON only, no extra text
        "options": {
            "temperature": 0.0,
            "num_predict": 256,
            "num_ctx": 4096,
            "think": False,     # key：close thinking
        },
    }

    r = requests.post(OLLAMA_URL, json=payload, timeout=120)
    r.raise_for_status()
    data = r.json()
    resp = (data.get("response") or "").strip()

    # if response is empty, try to read from reasoning / thinking fields (some versions of ollama may put output there when "think" is enabled)
    if not resp:
        for key in ("thinking", "reasoning", "reasoning_content"):
            val = data.get(key)
            if isinstance(val, str) and val.strip().startswith("{"):
                resp = val.strip()
                logging.debug("using %s field as response", key)
                break

    logging.debug("🧾 resp_head=%r", resp[:200])

    if not resp:
        raise ValueError(
            f"ollama response empty. done_reason={data.get('done_reason')} keys={list(data.keys())}"
        )

    if not resp.lstrip().startswith("{"):
        raise ValueError(f"ollama non-JSON response head={resp[:200]!r}")

    return resp



def call_ollama_with_retry(prompt: str, max_retries: int = 3) -> str:
    last_err = None

    for attempt in range(1, max_retries + 1):
        try:
            return call_ollama(prompt)

        # network error → retry
        except (ReadTimeout, ConnectionError) as e:
            last_err = e
            sleep_s = min(2 ** attempt, 8) + random.random()
            logging.warning(
                "Ollama network error, retry %s/%s after %.1fs: %r",
                attempt, max_retries, sleep_s, e
            )
            time.sleep(sleep_s)

        # empty response / abnormal JSON → retry once
        except ValueError as e:
            last_err = e
            logging.warning(
                "Ollama invalid response, retry %s/%s: %r",
                attempt, max_retries, e
            )
            time.sleep(0.5)

    raise last_err


def extract_json(text: str) -> Dict[str, Any]:
    """
    Strict parse first. If model accidentally adds extra text,
    try to locate the first {...} block.
    """
    if text is None:
        raise ValueError("ollama response is None")
    raw = text.strip()
    if not raw:
        raise ValueError("ollama response is empty")
    # extract only first JSON block
    m = re.search(r"```json\s*(\{.*?\})\s*```", raw, flags=re.S)
    if m:
        return json.loads(m.group(1))
    # then try strict parse
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        # fallback: try to slice JSON object
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = raw[start:end + 1]
            return json.loads(candidate)

        raise


def normalize_result(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize fields and enforce allowed values.
    """
    label = str(obj.get("label", "")).strip().lower()

    if label not in LABEL_MAP:
        # sometimes model outputs Chinese label; map if needed

        label = ZH_MAP.get(label, label)
    if label not in LABEL_MAP:
        raise ValueError(f"Invalid label: {label}")

    sentiment = obj.get("sentiment")
    if sentiment is not None:
        sentiment = str(sentiment).strip().lower()
        if sentiment not in ("positive", "neutral", "negative"):
            sentiment = None

    confidence = obj.get("confidence")
    if confidence is not None:
        try:
            confidence = float(confidence)
            if confidence < 0 or confidence > 1:
                confidence = None
        except Exception:
            confidence = None

    is_sarcasm = obj.get("is_sarcasm")
    if isinstance(is_sarcasm, str):
        is_sarcasm = is_sarcasm.strip().lower() in ("true", "1", "yes")
    elif is_sarcasm is not None:
        is_sarcasm = bool(is_sarcasm)

    reason = obj.get("reason")
    if reason is not None:
        reason = str(reason).strip()

    # keep full JSON for auditing/extensibility
    normalized = {
        "label": label,
        "confidence": confidence,
        "sentiment": sentiment,
        "is_sarcasm": is_sarcasm,
        "reason": reason,
    }
    # merge back into original to preserve extra keys (e.g., cues)
    merged = dict(obj)
    merged.update({k: v for k, v in normalized.items() if v is not None})
    merged["label"] = label  # enforce
    return merged


def fetch_batch(conn) -> List[Tuple[int, str, int, str]]:
    """
    Returns rows: (user_id,topic_id, post_type, floor_no, content)
    IMPORTANT: change r.post_content_col to your real column name.
    """
    with conn.cursor() as cur:
        cur.execute(POST_SQL, {"prompt_version": PROMPT_VERSION, "batch_size": BATCH_SIZE})
        return cur.fetchall()


def upsert_ai(
    conn,
    row_key: Tuple[int, str, int],
    user_id: str,
    result: Dict[str, Any],
) -> None:
    topic_id, post_type, floor_no = row_key

    # ---- old fields ----
    ai_label = result.get("label", "ERROR")
    ai_confidence = result.get("confidence", 0.0)
    ai_sentiment = result.get("sentiment", "neutral")
    ai_is_sarcasm = result.get("is_sarcasm", False)
    ai_reason = result.get("reason", "")

    # ---- new fields ----
    final_attitude_to_landy = result.get(
        "final_attitude_to_landy",
        {"fan": "support", "hater": "attack"}.get(ai_label, "neutral")
    )
    ai_is_rebuttal = result.get("ai_is_rebuttal")
    ai_mention_negative_claim = result.get("ai_mention_negative_claim")

    with conn.cursor() as cur:
        cur.execute(
            INSERT_SQL,
            {
                "user_id": user_id,
                "topic_id": topic_id,
                "post_type": post_type,
                "floor_no": floor_no,
                "ai_label": ai_label,
                "ai_confidence": ai_confidence,
                "ai_sentiment": ai_sentiment,
                "ai_is_sarcasm": ai_is_sarcasm,
                "ai_reason": ai_reason,
                "ai_result": json.dumps(result, ensure_ascii=False),
                "ai_model": MODEL,
                "prompt_version": PROMPT_VERSION,
                "final_attitude_to_landy": final_attitude_to_landy,
                "ai_is_rebuttal": ai_is_rebuttal,
                "ai_mention_negative_claim": ai_mention_negative_claim,
            },
        )


def build_error_obj(error_code: str, exception: Exception, raw_text: str) -> Dict[str, Any]:
    return {
        "label": "neutral",
        "confidence": 0.0,
        "sentiment": "neutral",
        "is_sarcasm": False,
        "reason": f"{error_code}: {type(exception).__name__}",
        "raw_head": (raw_text or "")[:500],
    }



def main():
    conn = get_db_conn()
    conn.autocommit = False
    try:
        rows = fetch_batch(conn)
        logging.info("🚀 Start labeling batch of %s rows", len(rows))
        ok, fail = 0, 0

        for user_id, topic_id, post_type, floor_no, content in rows:
            key = (topic_id, post_type, floor_no)
            resp_text = None   # prevent UnboundLocalError in except
            text = (content or "").strip()

            # ---- Short circuit: Empty / Blank only ----
            if not text:
                logging.warning(
                    "Skip empty content: topic_id=%s post_type=%s floor_no=%s",
                    topic_id, post_type, floor_no
                )

                # Simply write a neutral statement (without calling LLM).
                upsert_ai(
                    conn,
                    key,
                    user_id,
                    result={
                        "label": "neutral",
                        "final_attitude_to_landy": "neutral",
                        "confidence": 0.0,
                        "sentiment": "neutral",
                        "is_sarcasm": False,
                        "ai_is_rebuttal": False,
                        "ai_mention_negative_claim": False,
                        "reason": "empty_content"
                    }
                )
                continue

            try:
                prompt = build_prompt(content)
                logging.info(
                    "Calling ollama: topic_id=%s %s-%s",
                    topic_id, post_type, floor_no
                )
                resp_text = call_ollama_with_retry(prompt, max_retries=3)
                logging.debug("Raw LLM output: %s", resp_text)

                # --- handle parse/normalize errors ---
                try:
                    obj = extract_json(resp_text)
                    obj = normalize_result(obj)
                except Exception as e:
                    conn.rollback()
                    fail += 1
                    logging.error(
                        "JSON parse/normalize failed: user_id=%s topic_id=%s %s-%s error=%s raw_head=%r",
                        user_id, topic_id, post_type, floor_no, repr(e), (resp_text or "")[:200]
                    )
                    logging.debug("Raw LLM output: %s", resp_text)

                    # ✅ Key point: Construct ERROR results, write them as placeholders to avoid repeated analysis in the future.
                    obj = build_error_obj(
                        error_code="ollama_empty_response" if not (resp_text and resp_text.strip()) else "json_parse_failed",
                        exception=e,
                        raw_text=resp_text
                    )

                # --- handle DB errors ---
                try:
                    upsert_ai(conn, key, user_id, obj)
                    conn.commit()
                    ok += 1
                except Exception as e:
                    conn.rollback()
                    fail += 1
                    logging.error(
                        "DB upsert failed: user_id=%s topic_id=%s %s-%s error=%s",
                        user_id, topic_id, post_type, floor_no, repr(e)
                    )
                    # helpful to debug constraint issues:
                    logging.debug("Normalized obj: %s", obj)
                    time.sleep(0.3)
                    continue  # go next row


            except Exception as e:
                conn.rollback()
                fail += 1
                logging.error(
                    "LLM call/build prompt failed: user_id=%s topic_id=%s %s-%s error=%s",
                    user_id, topic_id, post_type, floor_no, repr(e)
                )
                logging.debug("Last resp_text: %s", resp_text)

                # ✅ Write a placeholder to prevent the same record from being captured and failing repeatedly in the next round.
                try:
                    obj = build_error_obj(
                        error_code="llm_call_failed",
                        exception=e,
                        raw_text=resp_text or ""
                    )
                    obj["final_attitude_to_landy"] = "neutral"
                    obj["ai_is_rebuttal"] = False
                    obj["ai_mention_negative_claim"] = False

                    upsert_ai(conn, key, user_id, obj)
                    conn.commit()
                except Exception as db_e:
                    conn.rollback()
                    logging.error(
                        "DB upsert failed (after LLM failure): user_id=%s topic_id=%s %s-%s error=%s",
                        user_id, topic_id, post_type, floor_no, repr(db_e)
                    )

                time.sleep(0.3)
                continue

        logging.info("✅ Done. ok=%s fail=%s", ok, fail)

    except KeyboardInterrupt:
        logging.warning("Interrupted by user. Already committed ok=%s fail=%s", ok, fail)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
    logging.shutdown()
