import logging
from pathlib import Path

from db import get_db_conn
from utils.logger import setup_logger

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "insert_high_rating_dramas.log"
setup_logger(log_file=str(LOG_PATH))

HIGH_SCORE = 5

def insert_high_rating_dramas():
    """SQL insert value filtered from comments table into statistic table"""
    try:
        conn = get_db_conn()
        cursor = conn.cursor()

        sql = """
        TRUNCATE high_rating_dramas_from_low_rating_users;

        INSERT INTO high_rating_dramas_from_low_rating_users (drama_id)
        SELECT drama_id
        FROM drama_collection c
        WHERE rating = %s
        AND NOT EXISTS (
            SELECT 1 FROM low_rating_users l
            WHERE l.user_id = c.user_id AND l.drama_id = c.drama_id
        )
        ON CONFLICT (drama_id) DO NOTHING;
        """

        cursor.execute(sql,(HIGH_SCORE,))
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
    logging.shutdown()