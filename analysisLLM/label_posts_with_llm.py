import json
import os
import time
from typing import Any, Dict, Optional, Tuple, List

import requests
from db import get_db_conn


OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
MODEL = os.getenv("OLLAMA_MODEL", "qwen3:4b")
PROMPT_VERSION = os.getenv("PROMPT_VERSION", "v1_qwen3_4b_json")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))


PROMPT_TEMPLATE = """
You are a Chinese entertainment opinion analysis assistant.

Return STRICT JSON ONLY. Any non-JSON output is forbidden.

Schema:
{{
  "label": "hater | fan | neutral",
  "confidence": number between 0 and 1,
  "sentiment": "positive | neutral | negative",
  "is_sarcasm": true | false,
  "reason": "short explanation in Chinese"
}}

Comment:
{comment}
""".strip()


def build_prompt(comment: str) -> str:
    return PROMPT_TEMPLATE.format(comment=comment.strip())


def call_ollama(prompt: str) -> str:
    """
    Calls Ollama /api/generate with stream=false.
    Returns the 'response' text (should be JSON if prompt is followed).
    """
    payload = {
        "model": MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.2,
        },
    }
    r = requests.post(OLLAMA_URL, json=payload, timeout=120)
    r.raise_for_status()
    data = r.json()
    return data.get("response", "").strip()


def extract_json(text: str) -> Dict[str, Any]:
    """
    Strict parse first. If model accidentally adds extra text,
    try to locate the first {...} block.
    """
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # fallback: try to slice JSON object
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(text[start : end + 1])
        raise


def normalize_result(obj: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize fields and enforce allowed values.
    """
    label = str(obj.get("label", "")).strip().lower()
    label_map = {"hater": "hater", "fan": "fan", "neutral": "neutral"}
    if label not in label_map:
        # sometimes model outputs Chinese label; map if needed
        zh_map = {"黑子": "hater", "粉丝": "fan", "路人": "neutral"}
        label = zh_map.get(label, label)
    if label not in label_map:
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
    sql = """
    SELECT
        r.user_id,
        r.topic_id,
        r.post_type,
        r.floor_no,
        r.content_text AS content
    FROM public.douban_topic_post_raw r
    LEFT JOIN public.douban_topic_post_ai a
        ON a.topic_id = r.topic_id
        AND a.post_type = r.post_type
        AND a.floor_no = r.floor_no
        AND a.prompt_version = %(prompt_version)s
    WHERE a.topic_id IS NULL
        AND r.content_text IS NOT NULL
        AND length(trim(r.content_text)) > 0
    ORDER BY r.topic_id, r.post_type, r.floor_no
    LIMIT %(batch_size)s;
    """
    with conn.cursor() as cur:
        cur.execute(sql, {"prompt_version": PROMPT_VERSION, "batch_size": BATCH_SIZE})
        return cur.fetchall()


def upsert_ai(conn, row_key: Tuple[int, str, int], result: Dict[str, Any]) -> None:
    user_id,topic_id, post_type, floor_no = row_key

    ai_label = result.get("label")
    ai_confidence = result.get("confidence")
    ai_sentiment = result.get("sentiment")
    ai_is_sarcasm = result.get("is_sarcasm")
    ai_reason = result.get("reason")

    sql = """
    INSERT INTO public.douban_topic_post_ai (
      user_id, topic_id, post_type, floor_no,
      ai_label, ai_confidence, ai_sentiment, ai_is_sarcasm, ai_reason,
      ai_result, ai_model, prompt_version, labeled_at
    )
    VALUES (
      %(user_id)s, %(topic_id)s, %(post_type)s, %(floor_no)s,
      %(ai_label)s, %(ai_confidence)s, %(ai_sentiment)s, %(ai_is_sarcasm)s, %(ai_reason)s,
      %(ai_result)s::jsonb, %(ai_model)s, %(prompt_version)s, now()
    )
    ON CONFLICT (topic_id, post_type, floor_no, prompt_version)
    DO UPDATE SET
      ai_label = EXCLUDED.ai_label,
      ai_confidence = EXCLUDED.ai_confidence,
      ai_sentiment = EXCLUDED.ai_sentiment,
      ai_is_sarcasm = EXCLUDED.ai_is_sarcasm,
      ai_reason = EXCLUDED.ai_reason,
      ai_result = EXCLUDED.ai_result,
      ai_model = EXCLUDED.ai_model,
      labeled_at = EXCLUDED.labeled_at;
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
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
            },
        )


def main():
    conn = get_db_conn()
    conn.autocommit = False
    try:
        rows = fetch_batch(conn)
        ok, fail = 0, 0
        for user_id, topic_id, post_type, floor_no, content in rows:
            key = (user_id, topic_id, post_type, floor_no)
            try:
                prompt = build_prompt(content)
                resp_text = call_ollama(prompt)
                obj = extract_json(resp_text)
                obj = normalize_result(obj)

                upsert_ai(conn, key, obj)
                conn.commit()
                ok += 1

            except Exception as e:
                conn.rollback()
                fail += 1
                # keep it simple: print error + key
                print(f"[FAIL] {key} error={e}")
                # small backoff to avoid hammering if something goes wrong
                time.sleep(0.3)

        print(f"Done. ok={ok}, fail={fail}, model={MODEL}, prompt_version={PROMPT_VERSION}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
