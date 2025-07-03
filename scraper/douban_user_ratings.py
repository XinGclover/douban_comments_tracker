from datetime import datetime
import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_sleep
from utils.config import DOUBAN_DRAMA_ID, COLLECT_HEADERS
from utils.logger import setup_logger 
from utils.html_tools import extract_count


setup_logger("logs/douban_user_ratings.log", logging.INFO)  

BASE_URL_PAGES = "https://movie.douban.com/people/{}/collect?start={}&sort=time&rating=all&mode=grid&type=all&filter=all"   # URL for subsequent pages of comments

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
        source_drama_id, user_id, drama_id, rating, rating_time, comment, vote_useful
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id, drama_id) DO NOTHING
    """

UPDATE_QUERY ="""
    UPDATE low_rating_users
    SET fetched = TRUE, fetched_time = NOW()
    WHERE user_id = %s
    """

INSERT_FETCHED_QUERY = """
    INSERT INTO fetched_users (user_id, fetched_time)
    VALUES (%s, now())
    ON CONFLICT (user_id) DO NOTHING;
    """


def insert_single_drama(cursor, drama_dict, user_id):
    if drama_dict.get('rating') is None:
        logging.info("🧹 Skipping drama_id=%s for user %s due to missing rating", drama_dict.get('drama_id'), user_id)
        return False
    
    try:       
        params = (
            DOUBAN_DRAMA_ID,
            str(user_id).strip(),
            str(drama_dict.get('drama_id')).strip(),
            int(drama_dict.get('rating')),
            drama_dict.get('rating_time'), 
            drama_dict.get('comment'),
            int(drama_dict.get('vote_useful') or 0)                    
        )
   
        cursor.execute(INSERT_QUERY, params) 
        return cursor.rowcount == 1
    except (ValueError, TypeError) as e:
        logging.error("❌ Data formatting error for user %s, drama_dict: %s, error: %s", user_id, drama_dict, e)
        return False

def insert_dramas_page(dramas, user_id, cursor):
    inserted = 0
    skipped = 0
    for drama in dramas:
        success = insert_single_drama(cursor, drama, user_id)
        if success:
            inserted += 1
            logging.info("ℹ️ Insert drama_id=%s", drama['drama_id'])
        else:
            skipped += 1
    logging.info("📊 Inserted: %d, Skip: %d", inserted, skipped)
    safe_sleep(10 , 20)
    return inserted,skipped


def parse_block(block):
    if not block:
        return {}

    href = block.select_one('li.title a')['href']
    drama_id = href.split('/')[4] if href else None

    rating_tag = block.select_one('span[class*="rating"]')
    rating = rating_tag['class'][0][6] if rating_tag else None
    rating_time = block.select_one('span.date').text.strip()
    
    comment_tag = block.select_one('span.comment')
    comment = comment_tag.text.strip() if comment_tag else None
    if comment in (None, '', 'None'):
        comment = None

    vote_useful = extract_count(block, r'\((\d+)\s.*?\)', 'span.pl')

    return {
       "drama_id": drama_id,
       "rating" : rating,
       "rating_time" : rating_time,
       "comment" : comment,
       "vote_useful": vote_useful
    }

def fetch_one_page(page_num, headers, user_id):
    start = page_num * 15
    collect_url = BASE_URL_PAGES.format(user_id, start) 

    try:
        resp = requests.get(collect_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.error("🚫 Failed: %s", resp.status_code)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        span_tag = soup.select_one('span.thispage')
        total_page = int(span_tag['data-total-page']) if span_tag and span_tag.has_attr('data-total-page') else 0


        blocks = soup.find_all("div", class_="comment-item")
        parsed_blocks = [parse_block(b) for b in blocks]
        if page_num == 0:
            return total_page, parsed_blocks
        return None, parsed_blocks

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("❌ Error: %s", e)
        return None, None


def fetch_user_collect(user_id, headers, cursor, conn): 
    inserted = 0
    skipped = 0
    total_page, first_page_drama =  fetch_one_page(0, headers, user_id)
    if first_page_drama:
        ins, ski = insert_dramas_page(first_page_drama, user_id, cursor)
        inserted += ins
        skipped += ski
        conn.commit()
        logging.info("📊 Page 0 committed for user %s", user_id)

    for page in range(1, total_page):
        try:
            total_page, other_dramas = fetch_one_page(page, headers, user_id)
            if not other_dramas:
                continue
            ins, ski = insert_dramas_page(other_dramas, user_id, cursor)
            inserted += ins
            skipped += ski
            conn.commit()
            logging.info("📊 Page %s committed for user %s",page, user_id)

        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("❌ Error in main_loop for user %s, page %d: %s", user_id, page, e)
            conn.rollback()
            safe_sleep(10, 20)
    return inserted, skipped

    


def process_db():
    conn = get_db_conn()
    request_headers = COLLECT_HEADERS
    total_users = 0
    try:
        with conn.cursor() as cursor:
            limit = 5
            cursor.execute(SELECT_QUERY,(DOUBAN_DRAMA_ID,limit)) 
            results = cursor.fetchall()
            logging.info("📝 Start fetching %s:", results)
            
            user_ids = [row[0] for row in results]

            for user_id in user_ids:
                try:
                    inserted, skipped = fetch_user_collect(user_id, request_headers,cursor,conn)
                    logging.info("🎬 User %s totallly insert %s dramas, skip %s", user_id, inserted, skipped) 
                    cursor.execute(UPDATE_QUERY, (user_id,))
                    cursor.execute(INSERT_FETCHED_QUERY, (user_id,))
                    conn.commit()  
                    logging.info("🚀 Fetched flag updated for user %s",user_id)         
                    safe_sleep(30, 50)
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
    total = process_db()
    logging.info("📦 A total of %s users of Drama %s were processed ", total, DOUBAN_DRAMA_ID )