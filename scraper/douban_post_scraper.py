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


post_list = [
    # {
    #     "topic_id": "334175701",
    #     "start": 50,
    #     "end": 51,
    #     "title": "午夜钟声⏳｜八月公主🧚‍♀️茶话会2.0"
    # },
     {
        "topic_id": "334501225",
        "start": 5,
        "end": 6,
        "title": "午夜钟声⏳｜八月公主🧚‍♀️茶话会3.0 "
    },
    # {
    #     "topic_id": "333355524",
    #     "start": 0,
    #     "end": 14,
    #     "title": "真爱之舞💃｜有人考古过这个cut吗？"
    # },
    {
        "topic_id": "334455247",
        "start": 5,
        "end": 10,
        "title": "午夜钟声⏳｜永夜森林🌳"
    }
]

INSERT_SQL = """
    INSERT INTO douban_post (
        topic_id, user_id, user_name, pubtime, ip, comment_text, like_count
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (topic_id, user_id, pubtime) DO NOTHING
    """



def parse_row(li):
    result = {}

    user_tag = li.find("h4").find("a") if li.find("h4") else None
    result['user_id'] = extract_href_info(r'/people/([^/]+)/', user_tag)
    result['user_name'] = user_tag.get_text(strip=True) if user_tag else ""

    pubtime_tag = li.find("span", class_="pubtime")

    pubtime_text = pubtime_tag.get_text(strip=True) if pubtime_tag else ""
    if pubtime_text:
        parts = pubtime_text.split()
        result['pubtime'] = " ".join(parts[:2])
        result['ip'] = parts[2] if len(parts) > 2 else ""
    else:
        result['pubtime'], result['ip'] = "", ""

    reply_content = li.find("div", class_="reply-content")
    result['comment_text'] = " ".join(p.get_text(strip=True) for p in reply_content.find_all("p")) if reply_content else ""

    like_count = extract_count(li, r'(\d+)', 'a[class*="comment-vote"]')
    result['like_count'] = like_count if like_count is not None else 0

    return result


def fetch_topic_page(post_id, page_num=0, headers=None):
    url = BASE_URL_PAGE.format(post_id, page_num * 100)

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed: %s", resp.status_code)
            return []

        block = BeautifulSoup(resp.text, "html.parser")
        rows = block.find_all("li", class_="comment-item")

        return [parse_row(r) for r in rows]

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error: %s", e)
        return []



def insert_single_topic(cursor, post_dict, post_meta):

    try:
        params = (
            post_meta['topic_id'],
            post_dict['user_id'],
            post_dict['user_name'],
            post_dict['pubtime'],
            post_dict['ip'],
            post_dict['comment_text'],
            post_dict['like_count']
        )
        cursor.execute(INSERT_SQL, params)
        return cursor.rowcount == 1
    except (ValueError, TypeError, psycopg2.Error) as e:
        logging.error("❌ Insert failed: %s", e)
        logging.info("🔧 Wrong data: %s", post_dict)
        return False


def main_loop(post):
    conn = get_db_conn()
    request_headers = get_headers()

    for page in range(post['start'], post['end']):  # Adjust range as needed
        try:
            logging.info("\n📄 Fetching comments on page %s...", page)
            topics = fetch_topic_page(post['topic_id'], page, headers=request_headers)
            logging.info("📄 Fetched %d comments on page %d", len(topics), page)
            if not topics:
                logging.warning("⚠️ No more comments, may be limited or reached the end")
                break

            with conn.cursor() as cursor:
                for c in topics:
                    success = insert_single_topic(cursor, c, post)
                    if success:
                        logging.info("✅ Insert user_name=%s", c['user_name'])
                    else:
                        logging.warning("⚠️ Failed to insert user_name=%s",c['user_name'])

                conn.commit()

                safe_sleep(20, 30)     # Sleep between requests

        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            conn.rollback()
            logging.error("❌ Page crawl failed: %s, rollback", e)
            safe_sleep(10, 20)  # Sleep before retrying

    conn.close()


if __name__ == "__main__":
    for post in post_list:
        logging.info("🚀 Starting Douban topics scraper for %s", post['title'])
        main_loop(post)  # Adjust start_page and max_pages as needed
    logging.shutdown()

