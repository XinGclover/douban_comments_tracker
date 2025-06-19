import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import psycopg2
import os
from dotenv import load_dotenv
import random

load_dotenv() 


FILTER = 36553916
LIZHI = 35651341
ZHAOXUELU = 36317401

table_name = "filter_comments"
# table_name = "lizhi_comments"
# table_name = "zhaoxuelu_comments"

BASE_URL_FIRST_PAGE = "https://movie.douban.com/subject/{}/comments?limit=20&status=F&sort=new_score"
BASE_URL_OTHER_PAGES = "https://movie.douban.com/subject/{}/comments?start={}&limit=20&status=P&sort=new_score"

def get_db_conn():
    """
    Get PostgreSQL database connection
    :return: psycopg2 connection object
    """
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )

def extract_rating(block):
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

def get_url(page_num, drama_id):
    if page_num == 0:
        return BASE_URL_FIRST_PAGE.format(drama_id)
    else:
        start = page_num * 20
        return BASE_URL_OTHER_PAGES.format(drama_id, start) 


def fetch_comments_page(page_num=0, headers=None, drama_id=FILTER):
    url = get_url(page_num, drama_id)
    print(f"[{datetime.now()}] Fetching: {url}")

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            print(f"Failed: {resp.status_code}")
            return []

        block = BeautifulSoup(resp.text, "html.parser")
        blocks = block.find_all("div", class_="comment")
        return [parse_comment_block(b) for b in blocks]

    except Exception as e:
        print("Error:", e)
        return []
    
def insert_single_comment(conn, comment_dict):
    """
    Insert a single comment into the PostgreSQL database, automatically skipping abnormal data.
    :param conn: psycopg2 database connection
    :param comment_dict: dict, single comment
    """
    sql = """
    INSERT INTO {} (
        user_id, user_name, votes, status, rating, user_location, create_time, user_comment
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id, create_time) DO NOTHING   
    """.format(table_name)

    try:
        rating_raw = comment_dict.get('rating')
        rating = int(rating_raw) if rating_raw is not None else None
        params = (
            int(comment_dict.get('user_id')),
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
    except Exception as e:
        conn.rollback()
        print(f"Insert failed: {e}")
        print("Wrong data:", comment_dict)
        return False


def main_loop(start_page=0, max_pages=10000, sleep_range=(50, 70)):
    conn = get_db_conn()
    cookies = os.getenv("DOUBAN_COOKIE")
    headers = { "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    "Cookie": cookies
}
    
    inserted = 0
    skipped = 0
    for page in range(start_page, max_pages):
        try:
            print(f"\n📄 Fetching comments on page {page}...")
            comments = fetch_comments_page(page, headers=headers, drama_id=FILTER)
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
        except Exception as e:
            print("❌ Page crawl failed:", e)
            time.sleep(10)

    conn.close()
    print(f"\n✨ {inserted} total inserted, {skipped} skipped")

if __name__ == "__main__":
    main_loop()



