import logging

from db import get_db_conn
from utils.logger import setup_logger

setup_logger("logs/insert_high_rating_dramas.log", logging.INFO)


def insert_high_rating_dramas():
    """SQL insert value filtered from comments table into statistic table"""
    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        sql = """
        INSERT INTO high_rating_dramas_from_low_rating_users (drama_id)
        SELECT c2.drama_id
        FROM low_rating_users l
        JOIN drama_collection c2 ON l.user_id = c2.user_id
        WHERE c2.rating = 5
          AND c2.drama_id != l.drama_id
        GROUP BY c2.drama_id
        ORDER BY COUNT(DISTINCT c2.user_id) DESC
        ON CONFLICT (drama_id) DO NOTHING;
        """

        cursor.execute(sql)
        conn.commit()

        logging.info("✅ The high-rating drama is inserted")

    except Exception as e:
        logging.error("❌ Error inserting high-rating drama: %s", str(e))
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    insert_high_rating_dramas()
