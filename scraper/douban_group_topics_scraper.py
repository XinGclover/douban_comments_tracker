import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_sleep
from utils.config import TABLE_PREFIX
from utils.config_loader import get_headers
from utils.logger import setup_logger
from pathlib import Path
import re
from utils.html_tools import extract_href_info


LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "douban_group_topics_scraper.log"
setup_logger(log_file=str(LOG_PATH))


BASE_URL_PAGE = "https://www.douban.com/group/search?start={}&cat=1013&sort=time&q=%E6%9C%9D%E9%9B%AA%E5%BD%95"

INSERT_SQL = f"""
    INSERT INTO {TABLE_PREFIX}_group_topics (
        topic_id, title, full_time, reply_count, group_id, group_name
    )
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (topic_id) DO NOTHING
    """
KEY_WORD = "朝雪录"


def parse_row(row):
    result = {}

    title_tag = row.select_one('td.td-subject a')
    if title_tag:
        result['topic_id'] = extract_href_info(r'/group/topic/(\d+)/', title_tag)
        result['title'] = title_tag.get_text(strip=True)
    else:
        result['topic_id'] = None
        result['title'] = None

    time_tag = row.select_one('td.td-time')
    result['full_time'] = time_tag['title'] if time_tag and 'title' in time_tag.attrs else None

    reply_tag = row.select_one('td.td-reply span')
    if reply_tag:
        reply_text = reply_tag.get_text(strip=True)
        result['reply_count'] = int(''.join(filter(str.isdigit, reply_text)))
    else:
        result['reply_count'] = None

    tds = row.select('td')
    if len(tds) >= 4:
        group_link_tag = tds[-1].select_one('a')
        if group_link_tag:
            group_href = group_link_tag.get('href', '')
            match = re.search(r'/group/(\d+)/', group_href)
            result['group_id'] = match.group(1) if match else None
            result['group_name'] = group_link_tag.get_text(strip=True)
        else:
            result['group_id'] = None
            result['group_name'] = None
    else:
        result['group_id'] = None
        result['group_name'] = None

    return result


def fetch_topic_page(page_num=0, headers=None):
    url = BASE_URL_PAGE.format(page_num * 50)

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed: %s", resp.status_code)
            return []

        block = BeautifulSoup(resp.text, "html.parser")
        rows = block.find_all("tr", class_="pl")
        return [parse_row(r) for r in rows]

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error: %s", e)
        return []



def insert_single_topic(cursor, topic_dict):

    try:
        params = (
            topic_dict['topic_id'],
            topic_dict['title'],
            topic_dict['full_time'],
            topic_dict['reply_count'],
            topic_dict['group_id'],
            topic_dict['group_name'],
        )
        cursor.execute(INSERT_SQL, params)
        return cursor.rowcount == 1
    except (ValueError, TypeError, psycopg2.Error) as e:
        logging.error("❌ Insert failed: %s", e)
        logging.info("🔧 Wrong data: %s", topic_dict)
        return False


def main_loop():
    conn = get_db_conn()
    request_headers = get_headers()

    for page in range(0,2):  # Adjust range as needed
        try:
            logging.info("\n📄 Fetching topics on page %s...", page)
            topics = fetch_topic_page(page, headers=request_headers)
            logging.info("📄 Fetched %d topics on page %d", len(topics), page)
            if not topics:
                logging.warning("⚠️ No more topics, may be limited or reached the end")
                break

            with conn.cursor() as cursor:
                for c in topics:
                    success = insert_single_topic(cursor, c)
                    if success:
                        logging.info("✅ Insert topic_id=%s", c['topic_id'])
                    else:
                        logging.warning("⚠️ Failed to insert topic_id=%s", c['topic_id'])

                conn.commit()

                safe_sleep(20, 30)     # Sleep between requests

        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            conn.rollback()
            logging.error("❌ Page crawl failed: %s, rollback", e)
            safe_sleep(10, 20)  # Sleep before retrying

    conn.close()


if __name__ == "__main__":
    logging.info("🚀 Starting Douban topics scraper for %s",KEY_WORD )
    main_loop()  # Adjust start_page and max_pages as needed
    logging.shutdown()

