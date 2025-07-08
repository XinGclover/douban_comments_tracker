import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer

from db import get_db_conn
from utils.logger import setup_logger

setup_logger("logs/kafka_consumer_low_rating.log", logging.INFO)

KAFKA_TOPIC = "short_reviews_topic"
LOW_RATING_THRESHOLD = 2
RATIO_ALERT_THRESHOLD = 0.5
BATCH_MIN_COUNT = 10


INSERT_SQL = """
    INSERT INTO low_rating_batches (batch_id, insert_time, total_count, low_rating_count, low_rating_ratio)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (batch_id) DO NOTHING;
    """
batch_buffer = defaultdict(list)

def process_batch(batch_id, comments):
    total = len(comments)
    low_rating = sum(1 for c in comments if c.get('rating') is not None and c['rating'] <= LOW_RATING_THRESHOLD)

    ratio = low_rating / total if total > 0 else 0

    logging.info(f"[{batch_id}] total={total}, low_rating={low_rating}, ratio={ratio:.2f}")

    if ratio >= RATIO_ALERT_THRESHOLD:
        logging.warning(f"❗️ Low rating ratio exceeds threshold: {ratio:.2f}")

        conn = get_db_conn()
        cursor = conn.cursor()

        cursor.execute(INSERT_SQL, (
            batch_id,
            datetime.now(timezone.utc),
            total,
            low_rating,
            ratio
        ))
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"✅ Inserted alert batch {batch_id} into low_rating_batches")
    return {
        'batch_id': batch_id,
        'total': total,
        'low_rating': low_rating,
        'ratio': ratio
    }

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='low_rating_checker'
    )

    logging.info("👂 Consumer started, waiting for messages...")

    for msg in consumer:
        data = msg.value
        batch_id = data["batch_id"]
        batch_buffer[batch_id].append(data)

        if len(batch_buffer[batch_id]) >= BATCH_MIN_COUNT:
            result = process_batch(batch_id, batch_buffer[batch_id])
            del batch_buffer[batch_id]

    logging.info(
                f"[{result['batch_id']}] 🎯 Total comments processed: {result['total']}, "
                f"total low ratings: {result['low_rating']}, "
                f"ratio: {result['ratio']:.2f}"
            )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    main()
