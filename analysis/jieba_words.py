import glob
import logging
import os

import jieba
import psycopg2

from db import get_db_conn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
USER_DICT_FILE = os.path.join(BASE_DIR, 'user_dict.txt')

STOPWORDS_DIR = os.path.join(BASE_DIR, '../stopwords')
STOPWORDS = set()


jieba.load_userdict(USER_DICT_FILE)


for fname in glob.glob(os.path.join(STOPWORDS_DIR, '*.txt')):
    with open(fname, 'r', encoding='utf-8') as f:
        STOPWORDS.update(line.strip() for line in f if line.strip())


SELECT_SQL= """
    SELECT user_id, create_time, user_comment
    FROM zhaoxuelu_comments
    WHERE user_comment IS NOT NULL
    AND (user_id, create_time) NOT IN (
        SELECT DISTINCT user_id, create_time FROM zhaoxuelu_comment_words
    );
    """

INSERT_SQL = """
    INSERT INTO zhaoxuelu_comment_words (user_id, create_time, word)
    VALUES (%s, %s, %s);
    """


def clean_word(word):
    return word not in STOPWORDS and len(word) > 1


def process_comment(cursor, user_id, create_time, comment):
    words = [w for w in jieba.lcut(comment) if clean_word(w)]
    for word in words:
        cursor.execute(INSERT_SQL, (user_id, create_time, word))

def main():
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute(SELECT_SQL)
            rows = cursor.fetchall()

            logging.info("🚀 Total number of comments requiring segmentation: %d", len(rows))

            count = 0
            for user_id, create_time, comment in rows:
                process_comment(cursor, user_id, create_time, comment)
                count += 1
                if count % 500 == 0:
                    conn.commit()
                    logging.info("%d comments processed", count)

            conn.commit()
            cursor.close()
            conn.close()
            logging.info("📦 All completed, %d comments processed", count)

    except psycopg2.Error as e:
        conn.rollback()
        logging.error("❌ Top-level DB error: %s", e)
    finally:
        conn.close()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    main()
