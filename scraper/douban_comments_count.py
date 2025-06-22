from datetime import datetime

import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_float_percent, safe_number
from utils.config import BASE_URL, COUNT_TABLE_NAME
from utils.config_loader import get_headers
from utils.html_tools import extract_count


def extract_movie_stats(drama_url, headers=None):
    """ Extracts movie statistics from the given Douban drama URL.
    :param drama_url: URL of the Douban drama page
    :return: dict containing movie statistics such as rating people, total comments, reviews, discussions, and rating percentages
    """ 
    response = requests.get(drama_url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    

    rating = extract_count(soup, r'(\d+\.\d+)', 'strong[property="v:average"]')
    rating_people = extract_count(soup, r'(\d+)', 'span[property="v:votes"]')
    total_comments = extract_count(soup, r'全部\s*(\d+)\s*条', 'a[href*="comments?status=P"]')
    total_reviews = extract_count(soup, r'全部\s*(\d+)\s*条', 'a[href="reviews"]')
    total_discussions = extract_count(soup, r'全部\s*(\d+)\s*条', 'p.pl[align="right"]')

    rating_percents = []
    percent_tags = soup.select('.ratings-on-weight .item .rating_per')
    for tag in percent_tags:
        rating_percents.append(tag.get_text().strip())

    return {
        "insert_time": datetime.now(),
        "rating": rating,
        "rating_people": rating_people,
        "total_comments": total_comments,
        "total_reviews": total_reviews,
        "total_discussions": total_discussions,
        "rating_percents": rating_percents,       
    }




def insert_movie_stats(movie_stats, db_conn):
    """ Inserts movie statistics into the database.
    :param movie_stats: dict containing movie statistics
    :param conn: psycopg2 connection object
    :return: None
    """
    rating = safe_number(movie_stats.get('rating')) 
    total_comments = safe_number(movie_stats.get('total_comments'))
    total_discussions = safe_number(movie_stats.get('total_discussions'))
    total_reviews = safe_number(movie_stats.get('total_reviews'))
    rating_people = safe_number(movie_stats.get('rating_people'))

    percents = movie_stats.get('rating_percents', [])
    percents = [safe_float_percent(p) for p in percents] + [None] * 5
    rating_1, rating_2, rating_3, rating_4, rating_5 = percents[:5]

    insert_time = movie_stats.get('insert_time', datetime.now())

    sql = f"""
    INSERT INTO {COUNT_TABLE_NAME} (
        insert_time,
        rating,
        rating_people,
        rating_1_star, rating_2_star, rating_3_star, rating_4_star, rating_5_star,    
        total_comments, total_reviews, total_discussions
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (insert_time) DO NOTHING;
    """

    params = (    
        insert_time,
        rating,
        rating_people,
        rating_1,
        rating_2,
        rating_3,
        rating_4,
        rating_5,
        total_comments,     
        total_reviews,
        total_discussions      
    )

    with db_conn.cursor() as cursor:
        cursor.execute(sql, params)
    db_conn.commit()
    print("✅ Inserted movie stats at", insert_time)


if __name__ == "__main__":
    request_headers = get_headers() 
    stats = extract_movie_stats(BASE_URL,request_headers)
    conn = get_db_conn()
    insert_movie_stats(stats, conn)
    conn.close()

