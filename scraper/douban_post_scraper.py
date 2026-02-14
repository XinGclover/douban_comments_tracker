import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_sleep
from utils.config_loader import get_headers
from utils.logger import setup_logger
from pathlib import Path
from utils.html_tools import extract_href_info, extract_count


LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "douban_post_scraper.log"
setup_logger(log_file=str(LOG_PATH))


BASE_URL_PAGE = "https://www.douban.com/group/topic/{}/?start={}"

GROUP_PREFIX = "other"
TABLE_TOPICS = f"{GROUP_PREFIX}_group_topics"

SINCE_TIMESTAMP = "2025-07-13 12:00:00+01"  # Sweden winter time

TITLE_KEYWORDS = ["%兰迪%", "%landy%"]
EXCLUDE_KEYWORDS = ["%抽奖%", "%开奖%","%庆祝%","%祝贺%","%恭喜%"]  # Exclude topics with these keywords in the title
LIMIT_TOPICS = 10

SQL_GET_TOPICS = f"""
    SELECT topic_id
    FROM {TABLE_TOPICS}
    WHERE
        full_time >= %s::timestamptz
        AND title ILIKE ANY(%s)
        AND crawled_at IS NULL
        AND NOT (title ILIKE ANY(%s))
    ORDER BY full_time ASC
    LIMIT %s;
"""


INSERT_SQL = """
    INSERT INTO public.douban_topic_post_raw (
        topic_id, topic_title, topic_url,
        post_type, floor_no,
        user_id, user_name, is_op_author,
        content_text, like_count,
        pubtime, ip_location,
        crawled_at, crawler_version
    )
    VALUES (%s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            now(), %s)
    ON CONFLICT (topic_id, post_type, floor_no) DO UPDATE SET
        topic_title   = EXCLUDED.topic_title,
        topic_url     = EXCLUDED.topic_url,
        user_id       = EXCLUDED.user_id,
        user_name     = EXCLUDED.user_name,
        is_op_author  = EXCLUDED.is_op_author,
        content_text  = EXCLUDED.content_text,
        like_count    = EXCLUDED.like_count,
        pubtime       = EXCLUDED.pubtime,
        ip_location   = EXCLUDED.ip_location,
        crawler_version = EXCLUDED.crawler_version
"""

SQL_MARK_CRAWLED = f"""
    UPDATE {TABLE_TOPICS}
    SET crawl_status = %s,
        crawled_at = NOW()
    WHERE topic_id = %s;
"""

# ===============================
# Custom Exceptions
# ===============================

class NotFoundError(Exception):
    """Raised when topic returns 404"""
    pass
class ForbiddenError(Exception):
    """Raised when topic returns 403"""
    pass


def parse_max_page(block: BeautifulSoup) -> int:
    pager = block.select_one("div.paginator")
    if not pager:
        return 1  # no pagination, only 1 page

    pages = []
    for a in pager.select("a"):
        txt = a.get_text(strip=True)
        if txt.isdigit():
            pages.append(int(txt))

    return max(pages) if pages else 1

def parse_op(block: BeautifulSoup) -> dict:
    # topic_title
    h1 = block.select_one("div.article h1")
    topic_title = ""
    if h1:
        topic_title = h1.get_text(" ", strip=True).replace('"', "").replace("“", "").replace("”", "").strip()

    # Topic URL (can be uploaded from outside)
    # OP author (multiple selectors as backup)
    op_user_tag = (
        block.select_one('#topic-content .user-face a') or
        block.select_one('#topic-content .topic-doc h3 a') or
        block.select_one('div.topic-doc h3 a')
    )
    op_user_id = extract_href_info(r'/people/([^/]+)/', op_user_tag) if op_user_tag else ""
    op_user_name = op_user_tag.get_text(strip=True) if op_user_tag else ""

    # OP content
    main_container = block.select_one("#link-report .topic-content .rich-content") or block.select_one("#link-report .topic-content")
    op_text = ""
    if main_container:
        ps = main_container.find_all("p")
        op_text = "\n".join(p.get_text(" ", strip=True) for p in ps if p.get_text(strip=True)).strip()

    return {
        "topic_title": topic_title,
        "op_user_id": op_user_id,
        "op_user_name": op_user_name,
        "op_text": op_text,
    }


def parse_reply_row(li, floor_no: int, op_user_id: str):
    result = {}

    user_tag = li.find("h4").find("a") if li.find("h4") else None
    result["user_id"] = extract_href_info(r'/people/([^/]+)/', user_tag)
    result["user_name"] = user_tag.get_text(strip=True) if user_tag else ""

    pubtime_tag = li.find("span", class_="pubtime")
    pubtime_text = pubtime_tag.get_text(strip=True) if pubtime_tag else ""
    if pubtime_text:
        parts = pubtime_text.split()
        result["pubtime"] = " ".join(parts[:2])
        result["ip_location"] = parts[2] if len(parts) > 2 else ""
    else:
        result["pubtime"], result["ip_location"] = None, ""

    reply_content = li.find("div", class_="reply-content")
    result["content_text"] = " ".join(p.get_text(strip=True) for p in reply_content.find_all("p")) if reply_content else ""

    like_count = extract_count(li, r"(\d+)", 'a[class*="comment-vote"]')
    result["like_count"] = like_count if like_count is not None else 0

    result["floor_no"] = floor_no
    result["is_op_author"] = bool(result["user_id"]) and (result["user_id"] == op_user_id)

    return result

def get_topic_list(conn):
    with conn.cursor() as cur:
        cur.execute(SQL_GET_TOPICS, (SINCE_TIMESTAMP, TITLE_KEYWORDS, EXCLUDE_KEYWORDS, LIMIT_TOPICS))
        rows = cur.fetchall()
    return [row[0] for row in rows]


def fetch_topic_page(topic_id, start_offset=0, headers=None):
    url = BASE_URL_PAGE.format(topic_id, start_offset)

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        status = resp.status_code
        if status != 200:
            if status == 404:
                raise NotFoundError(f"404 topic_id={topic_id}")
            if status == 403:
                raise ForbiddenError(f"403 topic_id={topic_id}")
            raise Exception(f"Unexpected HTTP status: {status}")

        block = BeautifulSoup(resp.text, "html.parser")
        rows = block.find_all("li", class_="comment-item")

        max_page = parse_max_page(block)
        if start_offset == 0:
            max_page = parse_max_page(block)

        return rows, block, max_page, url

    except requests.exceptions.RequestException as e:
        logging.error("Error: %s", e)
        return [], None, None, url

def mark_topic_status(cursor, topic_id, status):
    cursor.execute(SQL_MARK_CRAWLED, (status, topic_id))

CRAWLER_VERSION = "topic_post_raw_v1"

def insert_op(cursor, topic_id: int, topic_title: str, topic_url: str, op_user_id: str, op_user_name: str, op_text: str):
    params = (
        topic_id, topic_title, topic_url,
        "op", 0,
        op_user_id, op_user_name, True,
        op_text, None,
        None, None,
        CRAWLER_VERSION
    )
    cursor.execute(INSERT_SQL, params)

def insert_reply(cursor, topic_id: int, topic_title: str, topic_url: str, reply: dict):
    params = (
        topic_id, topic_title, topic_url,
        "reply", reply["floor_no"],
        reply["user_id"], reply["user_name"], reply["is_op_author"],
        reply["content_text"], reply["like_count"],
        reply["pubtime"], reply["ip_location"],
        CRAWLER_VERSION
    )
    cursor.execute(INSERT_SQL, params)

def main_loop(topic_id: int):
    conn = get_db_conn()
    headers = get_headers()

    STEP = 100

    topic_title = ""
    topic_url = ""
    op_user_id = ""

    try:
        # 1) Fetch the first page: get rows0 + block0 + max_page
        rows0, block0, max_page, url0 = fetch_topic_page(topic_id, start_offset=0, headers=headers)
        topic_url = url0

        if not block0:
            raise Exception("empty_html_first_page")

        # 2) Parse the OP and perform upsert (only once).
        op_meta = parse_op(block0)
        if op_meta:
            topic_title = op_meta.get("topic_title", "")
            op_user_id = op_meta.get("op_user_id", "")

            with conn.cursor() as cursor:
                insert_op(
                    cursor,
                    topic_id=topic_id,
                    topic_title=topic_title,
                    topic_url=topic_url,
                    op_user_id=op_user_id,
                    op_user_name=op_meta.get("op_user_name", ""),
                    op_text=op_meta.get("op_text", ""),
                )
                conn.commit()

            logging.info("🧾 OP upserted. title=%s", topic_title)
        else:
            logging.warning("⚠️ OP meta not parsed. topic_id=%s", topic_id)

        # 3) Calculate the maximum start_offset
        #    Without a paging mechanism, max_page could be 1 or None.
        if not max_page:
            max_page = 1

        max_start = (max_page - 1) * STEP
        logging.info("📌 topic_id=%s max_page=%s => max_start=%s", topic_id, max_page, max_start)

        topic_success = True
        # 4) Loop through all page replies
        for start_offset in range(0, max_start + STEP, STEP):
            try:
                logging.info("📄 Fetching replies topic_id=%s start=%s", topic_id, start_offset)

                if start_offset == 0:
                    rows = rows0
                    url = url0
                else:
                    rows, _block, _max_page_unused, url = fetch_topic_page(
                        topic_id, start_offset=start_offset, headers=headers
                    )

                topic_url = url
                logging.info("📄 start=%s got %d replies", start_offset, len(rows))

                # If rows are not found on a certain page: This doesn't necessarily mean a break is needed (it can happen occasionally), skip it for now.
                if not rows:
                    logging.warning("⚠️ start=%s returned 0 rows; skip", start_offset)
                    continue

                with conn.cursor() as cursor:
                    for idx, li in enumerate(rows):
                        floor_no = start_offset + idx + 1  # 1..N continuous floor number across pages
                        reply = parse_reply_row(li, floor_no=floor_no, op_user_id=op_user_id)
                        insert_reply(cursor, topic_id, topic_title, topic_url, reply)

                conn.commit()
                safe_sleep(20, 30)

            except KeyboardInterrupt:
                raise
            except psycopg2.Error as e:
                topic_success = False
                conn.rollback()
                logging.error("❌ DB error: %s (start=%s)", e, start_offset)
                safe_sleep(10, 20)

        with conn.cursor() as cursor:
            if topic_success:
                mark_topic_status(cursor, topic_id, "success")
            else:
                mark_topic_status(cursor, topic_id, "partial_error")
        conn.commit()
        logging.info("✅ Marked topic_id=%s as crawled", topic_id)

    except NotFoundError as e:
        with conn.cursor() as cursor:
            mark_topic_status(cursor, topic_id, "not_found")
        conn.commit()
        logging.warning("🟡 Marked not_found: topic_id=%s", topic_id)

    except ForbiddenError as e:
        with conn.cursor() as cursor:
            mark_topic_status(cursor, topic_id, "forbidden")
        conn.commit()
        logging.warning("🟠 Marked forbidden: topic_id=%s", topic_id)

    except KeyboardInterrupt:
        raise
    except Exception as e:
        topic_success = False
        with conn.cursor() as cursor:
            mark_topic_status(cursor, topic_id, "error")
        conn.commit()
        logging.error("❌ Failed to crawl whole topic_id=%s: %s", topic_id, e)

    finally:
        conn.close()


if __name__ == "__main__":
    logging.info("params since=%s keyword=%s limit=%s", SINCE_TIMESTAMP, TITLE_KEYWORDS, LIMIT_TOPICS)

    conn = get_db_conn()
    try:
        topic_ids = get_topic_list(conn)
        logging.info("🧮 topic_ids count=%d", len(topic_ids))

    finally:
        conn.close()

    for topic_id in topic_ids:
        logging.info("🚀 Starting Douban topics scraper topic_id=%s", topic_id)
        main_loop(topic_id)
    logging.shutdown()

