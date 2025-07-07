import logging

from db import get_db_conn
from utils.config import DOUBAN_DRAMA_ID, TABLE_PREFIX
from utils.logger import setup_logger

setup_logger("logs/insert_low_rating_users.log", logging.INFO)

LOW_SCORE = 1

def insert_low_rating_users(drama_id):
    """SQL insert value filtered from comments table into statistic table"""
    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        sql = f"""
        INSERT INTO low_rating_users (user_id, drama_id, rating, comment_time)
        SELECT DISTINCT user_id, %s, rating, create_time
        FROM {TABLE_PREFIX}_comments
        WHERE rating = %s
        ON CONFLICT (user_id, drama_id) DO NOTHING;
        """

        cursor.execute(sql, (drama_id, LOW_SCORE))
        conn.commit()

        logging.info("✅ The low-scoring user data is inserted, drama_id=%s", drama_id)

    except Exception as e:
        logging.error("❌ Error inserting low-scoring user data: %s", str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    insert_low_rating_users(DOUBAN_DRAMA_ID)
