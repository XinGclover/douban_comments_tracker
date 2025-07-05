from datetime import datetime
import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_sleep
from utils.config import BASE_URL, TABLE_PREFIX, DRAMA_TITLE
from utils.config_loader import get_headers
from utils.logger import setup_logger
from utils.html_tools import extract_href_info

setup_logger("logs/douban_comments_scraper.log", logging.INFO)

BASE_URL_FIRST_PAGE = "{}/comments?limit=20&status=P&sort=time"  # URL for the first page of comments
BASE_URL_OTHER_PAGES = "{}/comments?start={}&limit=20&status=P&sort=time"   # URL for subsequent pages of comments

def extract_rating(block):
    """ Extract rating from the comment block.
    :param block: BeautifulSoup object representing a comment block
    :return: int, rating out of 10 or None if not found """
    rating_tag = block.select_one('.rating')
    if rating_tag:
        rating_class = rating_tag.get('class', [])
        for cls in rating_class:
            if cls.startswith('allstar'):
                try:
                    return int(cls.replace('allstar', '')) // 10
                except ValueError:
                    return None
    return None



def parse_comment_block(block):
    """ Parse a single comment block and extract relevant data.
    :param block: BeautifulSoup object representing a comment block
    :return: dict, contains user_id, user_name, votes, status, rating, location, time, comment """
    if not block:
        return {}
    votes = block.select_one('.vote-count').text.strip()
    a_tag = block.select_one('.comment-info a')
    user_name = a_tag.text.strip()
    user_id = extract_href_info(r'/people/([^/]+)/', a_tag)
    status = block.select_one('.comment-info span:nth-of-type(1)').text.strip()
    rating = extract_rating(block)
    location = block.select_one('.comment-location').text.strip()
    time_str = block.select_one('.comment-time').text.strip()
    comment = block.select_one('.comment-content .short').text.strip()

    return {
        "user_id": user_id,
        "user_name": user_name,
        "votes": int(votes) if votes.isdigit() else 0,
        "status": status,
        "rating": rating,
        "location": location,
        "time": time_str,
        "comment": comment,
    }

def get_url(page_num, drama_url):
    """ Generate the URL for fetching comments based on the page number.
    :param page_num: int, page number to fetch, 0 for first page
    :param drama_url: str, the drama URL to fetch comments from
    :return: str, the complete URL for fetching comments
    """
    if page_num == 0:
        return BASE_URL_FIRST_PAGE.format(drama_url)
    else:
        start = page_num * 20
        return BASE_URL_OTHER_PAGES.format(drama_url, start)


def fetch_comments_page(page_num=0, headers=None, drama_url=BASE_URL):
    """ Fetch comments from a specific page of the drama.
    :param page_num: int, page number to fetch, 0 for first page
    :param headers: dict, HTTP headers to use
    :param drama_url: str, the drama URL to fetch comments from
    :return: list of dicts, each dict contains comment data
    """
    url = get_url(page_num, drama_url)
    logging.info("[%s] Fetching: %s", datetime.now(), url)

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed: %s", resp.status_code)
            return []

        block = BeautifulSoup(resp.text, "html.parser")
        blocks = block.find_all("div", class_="comment")
        return [parse_comment_block(b) for b in blocks]

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error: %s", e)
        return []


def insert_single_comment(cursor, comment_dict):
    """ Insert a single comment into the database.
    :param cursor: psycopg2 cursor object
    :param comment_dict: dict, contains comment data to insert
    :return: bool, True if insert was successful, False otherwise
    """
    sql = f"""
    INSERT INTO {TABLE_PREFIX}_comments (
        user_id, user_name, votes, status, rating, user_location, create_time, user_comment
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id, create_time) DO NOTHING
    """

    try:
        rating_raw = comment_dict.get('rating')
        rating = None
        if rating_raw is not None:
            try:
                rating = int(rating_raw)
            except ValueError:
                logging.warning("⚠️ Invalid rating value: %s", rating_raw)

        params = (
            str(comment_dict.get('user_id')).strip(),
            str(comment_dict.get('user_name', '')).strip(),
            int(comment_dict.get('votes', 0)),
            str(comment_dict.get('status', '')).strip(),
            rating,
            str(comment_dict.get('location', '')).strip(),
            comment_dict.get('time'),
            str(comment_dict.get('comment', '')).strip()
        )
        cursor.execute(sql, params)
        return cursor.rowcount == 1
    except (ValueError, TypeError, psycopg2.Error) as e:
        logging.error("❌ Insert failed: %s", e)
        logging.info("🔧 Wrong data: %s", comment_dict)
        return False


def main_loop():
    """ Main loop to fetch and insert comments into the database.
    :param start_page: int, page number to start fetching from
    :param max_pages: int, maximum number of pages to fetch
    :param sleep_range: tuple, range of seconds to sleep between requests
    """

    conn = get_db_conn()
    request_headers = get_headers()
    inserted = 0
    skipped = 0
    for page in range(0, 4):
        try:
            logging.info("\n📄 Fetching comments on page %s...", page)
            comments = fetch_comments_page(page, headers=request_headers, drama_url=BASE_URL)
            logging.info("📄 Fetched %d comments on page %d", len(comments), page)
            if not comments:
                logging.warning("⚠️ No more comments, may be limited or reached the end")
                break

            with conn.cursor() as cursor:
                for c in comments:
                    success = insert_single_comment(cursor, c)
                    if success:
                        inserted += 1
                        logging.info("✅ Insert user_id=%s", c['user_id'])
                    else:
                        skipped += 1

                conn.commit()

                logging.info("✅ Inserted: %d, Skip: %d", inserted, skipped)
                safe_sleep(20, 30)     # Sleep between requests

        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            conn.rollback()
            logging.error("❌ Page crawl failed: %s, rollback", e)
            safe_sleep(10, 20)  # Sleep before retrying

    conn.close()
    logging.info("\n✨ %d total inserted, %d skipped", inserted, skipped)

if __name__ == "__main__":
    logging.info("🚀 Starting Douban comments scraper for %s", DRAMA_TITLE)
    main_loop()  # Adjust start_page and max_pages as needed


