from db import get_db_conn
from utils.config import TABLE_PREFIX


SELECT_SQL =f"""
        SELECT user_id, insert_time
        FROM {TABLE_PREFIX}_comments
        WHERE batch_id IS NULL
        AND insert_time >= NOW() - INTERVAL '3 days'
        ORDER BY insert_time
    """

INSERT_SQL = f"""
        UPDATE {TABLE_PREFIX}_comments
        SET batch_id = %s
        WHERE user_id = %s
    """

def patch_old_batch_ids():
    conn = get_db_conn()
    cursor = conn.cursor()

    cursor.execute(SELECT_SQL)
    rows = cursor.fetchall()

    for row in rows:
        user_id, insert_time = row
        # round down to 10-minute bucket
        bucket_time = insert_time.replace(minute=(insert_time.minute // 10) * 10, second=0, microsecond=0)
        batch_id = bucket_time.strftime('%Y%m%d%H%M')

        cursor.execute(INSERT_SQL, (batch_id, user_id))

    conn.commit()
    conn.close()
    print(f"✅ Patched {len(rows)} records with batch_id")

if __name__ == "__main__":
    patch_old_batch_ids()
