from datetime import datetime, timezone

import requests

from db import get_db_conn
from utils.config import IQIYI_HEADERS
from uuid import uuid4

# 热搜榜
URL = "https://mesh.if.iqiyi.com/portal/pcw/rankList/comSecRankList"

CATEGORY_MAPPING = {
    "热搜": "Hot Searches",
    "电视剧": "TV Dramas"
}

def fetch_iqiyi_hot_and_drama():
    params = {
        "v": "1",
        "page_st": "-10",
        "tag": "-10",
        "category_id": "-10",
        "date": "",
        "pg_num": "1",
    }

    response = requests.get(URL, params=params, headers=IQIYI_HEADERS)
    response.raise_for_status()

    data = response.json().get("data", {}).get("items", [])
    results = []

    batch_id = str(uuid4())
    collected_at = datetime.now(timezone.utc)

    for item in data:
        cn_category = item.get("name")
        if cn_category not in CATEGORY_MAPPING:
            continue

        en_category = CATEGORY_MAPPING[cn_category]
        contents = item.get("contents", [])
        for rank, content in enumerate(contents, start=1):
            title = content.get("title")
            if title:
                results.append({
                    "title": title,
                    "category": en_category,
                    "ranking": rank,
                    "batch_id": batch_id,
                    "collected_at": collected_at
                })
    return results

def insert_into_postgres(data_list):
    conn = get_db_conn()
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO iqiyi_rank_titles (title, category, ranking, batch_id, collected_at)
        VALUES (%s, %s, %s, %s, %s)
    """

    for item in data_list:
        cur.execute(insert_sql, (item["title"], item["category"], item["ranking"], item["batch_id"], item["collected_at"]))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Inserted {len(data_list)} records successfully!")



if __name__ == "__main__":
    print("🔄 Fetching hot searches and dramas from iQIYI...")
    data_list = fetch_iqiyi_hot_and_drama()
    insert_into_postgres(data_list)
