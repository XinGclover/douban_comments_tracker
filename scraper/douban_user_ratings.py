from datetime import datetime
import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_sleep
from utils.config import DOUBAN_DRAMA_ID, COLLECT_HEADERS, DRAMA_TITLE
from utils.logger import setup_logger
from utils.html_tools import extract_count
import re


setup_logger("logs/douban_user_ratings.log", logging.INFO)

BASE_URL_PAGES = "https://movie.douban.com/people/{}/collect?start={}&sort=time&rating=all&mode=grid&type=all&filter=all"   # URL for subsequent pages of comments

AMOUNT_LIMIT = 10
AMOUNT_MOST_RATING = 500


SELECT_QUERY = """
    SELECT user_id
    FROM low_rating_users lru
    WHERE
        drama_id = %s
        AND fetched = false
        AND NOT EXISTS (
            SELECT 1
            FROM fetched_users fu
            WHERE fu.user_id = lru.user_id
        )
    ORDER BY comment_time ASC
    LIMIT %s
    """

INSERT_QUERY = """
    INSERT INTO drama_collection (
        user_id, drama_id, rating, rating_time, comment, vote_useful
    ) VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id, drama_id) DO NOTHING
    """

UPDATE_QUERY ="""
    UPDATE low_rating_users
    SET fetched = TRUE, fetched_time = NOW()
    WHERE user_id = %s AND drama_id = %s
    """

INSERT_FETCHED_QUERY = """
    INSERT INTO fetched_users (user_id, fetched_time, total_dramas, inserted_dramas)
    VALUES (%s, now(), %s, %s)
    ON CONFLICT (user_id) DO NOTHING;
    """

def parse_block(block):
    """Parse one drama review"""
    if not block:
        return {}

    href = block.select_one('li.title a')['href']
    drama_id = href.split('/')[4] if href else None

    rating_tag = block.select_one('span[class*="rating"]')
    rating = rating_tag['class'][0][6] if rating_tag else None

    rating_time = None
    rating_time_tag = block.select_one('span.date')
    if rating_time_tag:
        try:
            rating_time = datetime.strptime(rating_time_tag.text.strip(), '%Y-%m-%d')
        except ValueError:
            pass

    comment_tag = block.select_one('span.comment')
    comment = comment_tag.text.strip() if comment_tag and comment_tag.text.strip() not in ('', 'None') else None

    vote_useful = extract_count(block, r'\((\d+)\s.*?\)', 'span.pl')

    li_tag = block.select_one('div.info > ul > li.intro')
    date_str = None
    region = None
    if li_tag:
        li_text = li_tag.get_text(strip=True)
        match = re.search(r'(\d{4}-\d{2}-\d{2})\((.*?)\)', li_text)
        if match:
            date_str = match.group(1)
            region = match.group(2)

    return {
       "drama_id": drama_id,
       "rating" : rating,
       "rating_time" : rating_time,
       "comment" : comment,
       "vote_useful": vote_useful,
       "date_str": date_str,
       "region": region
    }


def should_save_drama(date_str, region):
    """Only includes mainland China after 2019"""
    try:
        release_date = datetime.strptime(date_str, '%Y-%m-%d')
        cutoff_date = datetime(2019, 1, 1)
        return release_date >= cutoff_date and region == "中国大陆"
    except (ValueError, TypeError) :
        return False

def insert_single_drama(cursor, drama_dict, user_id):
    """Return (isInsert, stop_reason)"""
    if drama_dict.get('rating') is None or drama_dict.get('rating_time') is None:
        return False, None

    if not should_save_drama(drama_dict.get('date_str'), drama_dict.get('region')):
        return False, None

    if drama_dict.get('rating_time') < datetime(2019, 1, 1):
        return False, "early_drama"

    try:
        params = (
            str(user_id).strip(),
            str(drama_dict.get('drama_id')).strip(),
            int(drama_dict.get('rating')),
            drama_dict.get('rating_time'),
            drama_dict.get('comment'),
            int(drama_dict.get('vote_useful') or 0)
        )

        cursor.execute(INSERT_QUERY, params)
        return cursor.rowcount == 1, None
    except (ValueError, TypeError):
        return False, False

def insert_dramas_page(dramas, user_id, cursor):
    """Handle the counting and stop reason"""
    inserted = 0
    skipped = 0

    for drama in dramas:
        success, stop_reason = insert_single_drama(cursor, drama, user_id)
        if success:
            inserted += 1
        else:
            skipped += 1

        if stop_reason:
            return inserted, skipped, stop_reason

    return inserted, skipped, None


def fetch_one_page(page_num, headers, user_id):
    """Parse one page to get review blocks and total page and total dramas amount"""
    start = page_num * 15
    collect_url = BASE_URL_PAGES.format(user_id, start)

    try:
        resp = requests.get(collect_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None, None, None

        soup = BeautifulSoup(resp.text, "html.parser")
        span_tag = soup.select_one('span.thispage')
        total_page = int(span_tag['data-total-page']) if span_tag and span_tag.has_attr('data-total-page') else 0

        total_dramas = extract_count(soup, r'\((\d+)\)','h1')

        blocks = soup.find_all("div", class_="comment-item")
        parsed_blocks = [parse_block(b) for b in blocks]

        if page_num == 0:
            return total_page, parsed_blocks, total_dramas
        return None, parsed_blocks, None

    except (requests.exceptions.RequestException, psycopg2.Error):
        return None, None, None


def fetch_user_collect(user_id, headers, cursor, conn):
    """Fetch one user's drama collection"""
    inserted = 0
    skipped = 0
    total_page, first_page_drama, total_dramas =  fetch_one_page(0, headers, user_id)
    logging.info("🧮 There is total %s pages, %s dramas for user %s", total_page, total_dramas, user_id)

    if total_dramas and total_dramas > AMOUNT_MOST_RATING:
        return 0, 0, total_dramas, "too_many_dramas"

    if first_page_drama:
        ins, ski, stop_reason = insert_dramas_page(first_page_drama, user_id, cursor)
        inserted += ins
        skipped += ski
        conn.commit()
        safe_sleep(8,12)
        if stop_reason:
            return inserted, skipped, total_dramas, stop_reason

    for page in range(1, total_page):
        total_page, other_dramas, _ = fetch_one_page(page, headers, user_id)
        if not other_dramas:
            continue
        ins, ski, stop_reason = insert_dramas_page(other_dramas, user_id, cursor)
        inserted += ins
        skipped += ski
        conn.commit()
        safe_sleep(8,12)
        if stop_reason:
            return inserted, skipped, total_dramas, stop_reason

    return inserted, skipped, total_dramas, "normal"




def process_db():
    """Get the low rating users then fetch their drama collections"""
    conn = get_db_conn()
    request_headers = COLLECT_HEADERS
    total_users = 0
    try:
        with conn.cursor() as cursor:
            cursor.execute(SELECT_QUERY,(DOUBAN_DRAMA_ID, AMOUNT_LIMIT))
            results = cursor.fetchall()
            logging.info("📝 Start fetching %s:", results)

            user_ids = [row[0] for row in results]

            for user_id in user_ids:
                try:
                    logging.info("🚀 Start fetching for user %s", user_id)
                    inserted, skipped, total_dramas, stop_reason = fetch_user_collect(user_id, request_headers,cursor,conn)
                    logging.info("🎬 User %s totally insert %s dramas, skip %s", user_id, inserted, skipped)

                    if stop_reason == "early_drama":
                        logging.info("🛑 User %s stopped early due to rating_time < 2019-01-01", user_id)
                    elif stop_reason == "too_many_dramas":
                        logging.info("⛔ User %s skipped entirely due to too many dramas (%d)", user_id, total_dramas)
                    else:
                        logging.info("✅ User %s fetched normally", user_id)

                    cursor.execute(UPDATE_QUERY, (user_id, DOUBAN_DRAMA_ID))
                    cursor.execute(INSERT_FETCHED_QUERY, (user_id, total_dramas, inserted))
                    conn.commit()
                    logging.info("✅ Fetched flag updated for user %s",user_id)

                    safe_sleep(20, 30)
                    total_users += 1

                except (psycopg2.Error, ValueError, KeyError) as e:
                    conn.rollback()
                    logging.error("❌ Error processing user %s: %s", user_id, e)

    except psycopg2.Error as e:
        conn.rollback()
        logging.error("❌ Top-level DB error: %s", e)
    finally:
        conn.close()
    return total_users

if __name__ == "__main__":
    logging.info("🚀 Start fetching low rating users of %s", DRAMA_TITLE)
    total = process_db()
    logging.info("📦 A total of %s users of Drama %s were processed ", total, DOUBAN_DRAMA_ID )