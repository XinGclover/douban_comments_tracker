import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import psycopg2
import random
from config import TABLE_NAME, BASE_URL, get_headers
from db import get_db_conn

BASE_URL_FIRST_PAGE = "{}/comments?limit=20&status=P&sort=time"
BASE_URL_OTHER_PAGES = "{}/comments?start={}&limit=20&status=P&sort=time"


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
    user_id = block.select_one('a[data-id]')['data-id']
    user_name = block.select_one('.comment-info a').text.strip()
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
    print(f"[{datetime.now()}] Fetching: {url}")

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"Failed: {resp.status_code}")
            return []

        block = BeautifulSoup(resp.text, "html.parser")
        blocks = block.find_all("div", class_="comment")
        return [parse_comment_block(b) for b in blocks]

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        print("Error:", e)
        return []


def insert_single_comment(conn, comment_dict):
    """ Insert a single comment into the database.
    :param conn: psycopg2 connection object
    :param comment_dict: dict, contains comment data to insert
    :return: bool, True if insert was successful, False otherwise
    """
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        user_id, user_name, votes, status, rating, user_location, create_time, user_comment
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id, create_time) DO NOTHING   
    """

    try:
        rating_raw = comment_dict.get('rating')
        rating = int(rating_raw) if rating_raw is not None else None
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

        with conn.cursor() as cursor:
            cursor.execute(sql, params)
        conn.commit()
        return cursor.rowcount == 1
    except (psycopg2.Error, ValueError, KeyError) as e:
        conn.rollback()
        print(f"Insert failed: {e}")
        print("Wrong data:", comment_dict)
        return False


def main_loop(start_page=0, max_pages=10000, sleep_range=(50, 70)):
    """ Main loop to fetch and insert comments into the database.
    :param start_page: int, page number to start fetching from
    :param max_pages: int, maximum number of pages to fetch
    :param sleep_range: tuple, range of seconds to sleep between requests
    """
    
    conn = get_db_conn()
    request_headers = get_headers()   
    inserted = 0
    skipped = 0
    for page in range(start_page, max_pages):
        try:
            print(f"\n📄 Fetching comments on page {page}...")
            comments = fetch_comments_page(page, headers=request_headers, drama_url=BASE_URL)
            print(f"📄 Fetched {len(comments)} comments on page {page}")
            if not comments:
                print("⚠️ No more comments, may be limited or reached the end")
                break
            
            for c in comments:
                success = insert_single_comment(conn, c)
                if success:
                    inserted += 1
                    print(f"✅ Insert user_id={c['user_id']}")
                else:
                    skipped += 1

            print(f"✅ Inserted: {inserted},Skip: {skipped}")
            time.sleep(random.uniform(*sleep_range))
        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            print("❌ Page crawl failed:", e)
            time.sleep(10)

    conn.close()
    print(f"\n✨ {inserted} total inserted, {skipped} skipped")

if __name__ == "__main__":
    main_loop()



