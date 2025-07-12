from datetime import datetime
import json
import logging
import os

from kafka import KafkaProducer

from db import get_db_conn
from utils.config import TABLE_PREFIX
from utils.logger import setup_logger

setup_logger("logs/kafka_producer_send_comments.log", logging.INFO)

KAFKA_TOPIC = "short_reviews_topic"
KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

SELECT_BATCHID_SQL = f"""
    SELECT batch_id
    FROM {TABLE_PREFIX}_comments
    WHERE batch_id IS NOT NULL
    ORDER BY batch_id DESC LIMIT 1
    """

SELECT_SQL = f"""
    SELECT user_id, rating, user_comment, create_time, batch_id
    FROM {TABLE_PREFIX}_comments
    WHERE batch_id = %s
    ORDER BY create_time DESC
    """

def get_latest_short_comments():
    conn = get_db_conn()
    cursor = conn.cursor()
    cursor.execute(SELECT_BATCHID_SQL)
    result = cursor.fetchone()
    if not result:
        return None, []

    latest_batch_id = result[0]

    cursor.execute(SELECT_SQL, (latest_batch_id,))
    rows = cursor.fetchall()
    conn.close()
    return latest_batch_id, [dict(zip(['user_id', 'rating', 'user_comment', 'create_time', 'batch_id'], row)) for row in rows]

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

def send_to_kafka(messages, kafka_batch_id):
    if not messages:
        logging.warning("⚠️ No messages found.")
        return

    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_SERVER],
        value_serializer=lambda m: json.dumps(m, default=json_serializer).encode('utf-8')
    )


    for msg in messages:
        producer.send(KAFKA_TOPIC, value=msg)

    producer.flush()
    producer.close()
    print(f"✅ Sent {len(messages)} short reviews to Kafka, batch_id={kafka_batch_id}")

if __name__ == "__main__":
    batch_id, reviews = get_latest_short_comments()

    if reviews:
        send_to_kafka(reviews, batch_id)
    else:
        print("No reviews found to send.")
