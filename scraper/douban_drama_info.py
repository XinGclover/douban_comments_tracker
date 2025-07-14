from datetime import datetime
import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_float_percent, safe_number, safe_sleep
from utils.config import DRAMA_TITLE, COLLECT_HEADERS
from utils.html_tools import extract_count
from utils.logger import setup_logger

setup_logger("logs/douban_drama_info.log", logging.INFO)

DRAMA_URL = "https://movie.douban.com/subject/{}/"

LIMIT = 40


SELECT_QUERY = """
    SELECT drama_id, COUNT(DISTINCT user_id) AS user_count
    FROM drama_collection
    WHERE rating = 5
    AND drama_id IN (SELECT drama_id FROM high_rating_dramas_from_low_rating_users)
    AND NOT EXISTS (
        SELECT 1 FROM douban_drama_info d
        WHERE d.drama_id = drama_collection.drama_id
    )
    GROUP BY drama_id
    ORDER BY user_count DESC
    LIMIT %s;
    """

INSERT_SQL = """
    INSERT INTO douban_drama_info (
        drama_id,
        drama_name,
        release_year,
        director,
        actors,
        release_date,
        rating,
        rating_people,
        rating_1_star, rating_2_star, rating_3_star, rating_4_star, rating_5_star,
        total_comments,
        total_reviews,
        total_discussions,
        insert_time
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (drama_id) DO NOTHING;
    """



def extract_movie_stats(drama_id, url, headers=None):
    """ Extracts movie statistics from the given Douban drama URL.
    :param url: URL of the Douban drama page
    :return: dict containing movie statistics such as rating people, total comments, reviews, discussions, and rating percentages
    """
    response = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    name_tag = soup.select_one('span[property="v:itemreviewed"]')
    drama_name = name_tag.text.strip() if name_tag else None

    year_tag = soup.select_one('span.year')
    year = year_tag.text.strip('()') if year_tag else None

    director_tag = soup.select_one('a[rel="v:directedBy"]')
    director = director_tag.text.strip() if director_tag else None

    actors = []
    for a in soup.select('a[rel="v:starring"]'):
        parent_span = a.find_parent('span')
        if parent_span and 'display:none' in parent_span.get('style', '').replace(' ', '').lower():
            continue
        actors.append(a.get_text(strip=True))
        if len(actors) >= 4:
            break

    tag = soup.select_one('span[property="v:initialReleaseDate"]')
    date_str = tag['content'] if tag and tag.has_attr('content') else None
    release_date = date_str.split('(')[0] if date_str else None

    rating = extract_count(soup, r'(\d+\.\d+)', 'strong[property="v:average"]')
    rating_people = extract_count(soup, r'(\d+)', 'span[property="v:votes"]')
    total_comments = extract_count(soup, r'全部\s*(\d+)\s*条', 'a[href*="comments?status=P"]')
    total_reviews = extract_count(soup, r'全部\s*(\d+)\s*条', 'a[href="reviews"]')
    total_discussions = extract_count(soup, r'全部\s*(\d+)\s*条', 'p.pl[align="right"]')

    percent_tags = soup.select('.ratings-on-weight .item .rating_per')
    percents = [safe_float_percent(tag.get_text().strip()) for tag in percent_tags] + [None] * 5
    rating_1, rating_2, rating_3, rating_4, rating_5 = percents[:5]

    return (
        drama_id,
        drama_name,
        safe_number(year),
        director,
        actors,
        release_date,
        safe_number(rating),
        safe_number(rating_people),
        rating_1,
        rating_2,
        rating_3,
        rating_4,
        rating_5,
        safe_number(total_comments),
        safe_number(total_reviews),
        safe_number(total_discussions),
        datetime.now()
    )



def main_loop():
    """ Main loop to fetch and insert movie statistics into the database.
    :return: None
    """
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(SELECT_QUERY,(LIMIT,))
            results = cursor.fetchall()
            drama_ids = [row[0] for row in results]

            for drama_id in drama_ids:
                try:
                    drama_url = DRAMA_URL.format(drama_id)
                    params = extract_movie_stats(drama_id, drama_url, COLLECT_HEADERS)

                    if not params:
                        logging.error("❌ Failed to extract movie stats from %s", drama_id)
                        return

                    cursor.execute(INSERT_SQL, params)
                    conn.commit()
                    logging.info("✅ Movie %s stats inserted successfully.", drama_id )
                    safe_sleep(5, 10)
                except (psycopg2.Error, ValueError, KeyError) as e:
                    conn.rollback()
                    logging.error("❌ Error processing drama %s: %s", drama_id, e)

    except psycopg2.Error as e:
        conn.rollback()
        logging.error("❌ Top-level DB error: %s", e)

    finally:
        conn.close()
        logging.info("✅ Top %s hight rating dramas have been inserted successfully.", LIMIT )


if __name__ == "__main__":
    logging.info("🚀 Starting the high_rating dramas info fetcher from low_rating %s", DRAMA_TITLE)
    main_loop()

