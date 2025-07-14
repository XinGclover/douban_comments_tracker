import logging
from datetime import datetime

import pytz
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_float_percent, safe_number
from utils.config import (DRAMA_TITLE, IQIYI_BASE_URL, IQIYI_HEADERS, TABLE_PREFIX)
from utils.html_tools import extract_count
from utils.logger import setup_logger
from pathlib import Path

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "iqiyi_heat.log"
setup_logger(log_file=str(LOG_PATH))


def extract_movie_stats(drama_url, headers=None):
    """ Extracts movie statistics from the given Douban drama URL.
    :param drama_url: URL of the iqiyi drama page
    :return: dict containing movie statistics such as rating people, total comments, reviews, discussions, and rating percentages
    """
    response = requests.get(drama_url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    heat_info = extract_count(soup, r'热度\s*(\d+)', 'span.heat-info')
    effect_score = extract_count(soup, r'\s*(\d+(?:\.\d+)?)\s*', 'i.effect-score')
    hot_link = extract_count(soup, r'第([0-9０-９]+)名', 'a.hot-link')

    return {
        "heat_info": heat_info,
        "effect_score": effect_score,
        "hot_link": hot_link
    }

def insert_movie_stats(movie_stats, db_conn):
    """ Inserts movie statistics into the database.
    :param movie_stats: dict containing movie statistics
    :param conn: psycopg2 connection object
    """
    insert_query = f"""
        INSERT INTO {TABLE_PREFIX}_heat_iqiyi (insert_time, heat_info, effect_score, hot_link)
        VALUES (%s, %s, %s, %s)
        """

    heat_info = safe_number(movie_stats.get("heat_info"))
    effect_score = safe_float_percent(movie_stats.get("effect_score"))
    hot_link = safe_number(movie_stats.get("hot_link"))

    params = (
        datetime.now(pytz.utc),
        heat_info,
        effect_score,
        hot_link
    )

    with db_conn.cursor() as cursor:
        cursor.execute(insert_query, params)
        db_conn.commit()
        logging.info("Inserted movie stats: %s", movie_stats)

def main_loop():
    """ Main loop to fetch and insert movie statistics into the database.
    :return: None
    """
    stats = extract_movie_stats(IQIYI_BASE_URL,IQIYI_HEADERS)

    if not stats:
        logging.error("Failed to extract movie stats from %s", IQIYI_BASE_URL)
        return

    conn = get_db_conn()
    insert_movie_stats(stats, conn)
    conn.close()
    logging.info("Movie stats inserted successfully.")


if __name__ == "__main__":
    logging.info("Starting iQIYI heat scraper for %s", DRAMA_TITLE)
    main_loop()
    logging.shutdown()

