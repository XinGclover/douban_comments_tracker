from datetime import datetime, timezone
import json

import requests

from db import get_db_conn
from utils.config import IQIYI_HEADERS
from uuid import uuid4

# 热播榜
URL = "https://mesh.if.iqiyi.com/portal/pcw/rankList/comSecRankList"


def fetch_tv_rank():
    params = {
        "v": "1",
        "page_st": "0",
        "tag": "0",
        "category_id": "2",
        "date": "",
        "pg_num": "1",
    }

    response = requests.get(URL, params=params, headers=IQIYI_HEADERS)
    response.raise_for_status()

    items = response.json().get("data", {}).get("items", [])
    if not items:
        print("❌ No items found in the response.")
        return []
    contents = items[0].get("contents", [])
    if not contents:
        print("❌ No contents found in the first item.")
        return []
    results = []

    batch_id = str(uuid4())
    collected_at = datetime.now(timezone.utc)

    for item in contents:
        title = item.get("title")
        if not title:
            continue

        results.append({
            "title": title,
            "bullet_index": item.get("bulletIndex"),
            "rec_index": item.get("recIndex"),
            "main_index": item.get("mainIndex"),
            "tvid": item.get("tvid"),
            "order_index": item.get("order"),
            "collected_at": collected_at,
            "batch_id": batch_id,
        })
    return results

def insert_into_postgres(data_list):
    conn = get_db_conn()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO iqiyi_rank_dramas (title, bullet_index, rec_index, main_index, tvid, "order_index", collected_at, batch_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (batch_id, tvid) DO NOTHING
    """
    for item in data_list:
        cur.execute(insert_sql, (item["title"], item["bullet_index"], item["rec_index"], item["main_index"], item["tvid"], item["order_index"], item["collected_at"], item["batch_id"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(data_list)} records successfully!")


if __name__ == "__main__":
    print("🔄 Fetching TV rank from iQIYI...")
    data_list = fetch_tv_rank()
    insert_into_postgres(data_list)
