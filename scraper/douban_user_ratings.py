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
from utils.html_tools import extract_count


setup_logger("logs/douban_user_ratings.log", logging.INFO)  

BASE_URL_PAGES = "https://movie.douban.com/people/{}/collect?start={}&sort=time&rating=all&mode=grid&type=all&filter=all"   # URL for subsequent pages of comments

SLEEP_RANGE = (60, 90)  # Sleep time between requests
N_ROUNDS = 5  # Number of rounds to run the scraper
RUN_TIME_MINUTES = 10  # Total runtime in minutes for the scraper

def parse_collect_block(block):
    if not block:
        return {}

    drama_id = block.select_one('li.title a')['href'].split('/')[4]
    rating_tag = block.select_one('span[class*="rating"]')
    rating = rating_tag['class'][0][6] if rating_tag else None
    rating_time = block.select_one('span.date').text.strip()
    comment_tag = block.select_one('span.comment')
    comment = comment_tag.text.strip() if comment_tag else None
    vote_useful = extract_count(block, r'\((\d+)\s.*?\)', 'span.pl')

    return {
       "drama_id": drama_id,
       "rating" : rating,
       "rating_time" : rating_time,
       "comment" : comment,
       "vote_useful": vote_useful
    }

def fetch_total_page(soup):
    span_tag = soup.select_one('span.thispage')
    if span_tag and 'data-total-page' in span_tag.attrs:
        return int(span_tag['data-total-page']) 
    return 0



def fetch_collect_page(page_num=0, headers=None, user_id=278582560):
    start = page_num * 15
    collect_url = BASE_URL_PAGES.format(user_id, start) 
    print("[%s] Fetching: %s", datetime.now(), collect_url)

    try:
        resp = requests.get(collect_url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed: %s", resp.status_code)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        total_page = fetch_total_page(soup)

        blocks = soup.find_all("div", class_="comment-item")
        parsed_blocks = [parse_collect_block(b) for b in blocks]
        if page_num == 0:
            return total_page, parsed_blocks
        return None, parsed_blocks

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error: %s", e)
        return []

def get_unfetched_user_ids(conn):
    try:
        cursor = conn.cursor()
        limit = 2
        query = """
        SELECT user_id 
        FROM low_rating_users
        WHERE fetched = false
        ORDER BY comment_time ASC
        LIMIT %s
        """
        cursor.execute(query,(limit,))     
        results = cursor.fetchall()
        user_ids = [row[0] for row in results]

        cursor.close()
        conn.close()
        return user_ids

    except (psycopg2.Error, psycopg2.DatabaseError) as e:
        logging.error(f"Database error: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return []




def main_loop():
    conn = get_db_conn()
    request_headers = get_headers()
    user_ids = get_unfetched_user_ids(conn) 
    for user_id in user_ids:
        try:
            total_page, first_page_drama =  fetch_collect_page(0, headers=request_headers, user_id=user_id)
            all_drama = first_page_drama.copy()

            safe_sleep(20,30)
            for page in range(1, total_page):
                try:
                    _, drama = fetch_collect_page(page, headers=request_headers, user_id=user_id)
                    all_drama.extend(drama)
                
                    safe_sleep(20, 30)
                except (requests.exceptions.RequestException, psycopg2.Error) as e:
                    logging.error("Error in main_loop: %s", e)
                    safe_sleep(10, 20)
            safe_sleep(30, 50)
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            logging.error("Error fetching first page: %s", e)




if __name__ == "__main__":
    main_loop()