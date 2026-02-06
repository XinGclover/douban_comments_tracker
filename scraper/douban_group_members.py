from datetime import datetime
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.config_loader import get_headers
from utils.logger import setup_logger
from urllib.parse import urlparse

LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "douban_group_members.log"
setup_logger(log_file=str(LOG_PATH))

MEMBERS_URL = "https://www.douban.com/group/{}/members?start={}"  # start: 0, 36, 72, ...
PEOPLE_ID_RE = re.compile(r"/people/(\d+)/")
PAGE_SIZE = 36

INSERT_MEMBER_SQL = """
INSERT INTO douban_group_members (
  group_id,
  member_id,
  member_name,
  img_url
) VALUES (%s, %s, %s, %s)
ON CONFLICT (group_id, member_id) DO NOTHING
RETURNING member_id;
"""

def load_groups_from_db(conn):
    sql = """
    SELECT group_id, group_name, group_who, max_page
    FROM douban_groups
    WHERE is_active = TRUE
    ORDER BY group_id;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql)
        rows = cursor.fetchall()

    return [
        {
            "group_id": str(r[0]),
            "group_name": r[1],
            "group_who": r[2],
            "max_page": r[3] or 0,
        }
        for r in rows
    ]


def extract_member_id(href: str) -> str | None:
    if not href:
        return None
    path = urlparse(href).path  # /people/244176393/
    parts = [p for p in path.split("/") if p]  # ['people','244176393']
    if len(parts) >= 2 and parts[0] == "people":
        return parts[1]   # regardless of whether it's numeric or not
    return None


def parse_member_li(li):
    a = li.select_one('a[href*="/people/"]')
    if not a or not a.has_attr("href"):
        return None

    member_id = extract_member_id(a["href"])
    if not member_id:
        return None

    img = li.select_one("div.pic img")
    avatar_src = img.get("src") if img else None
    avatar_alt = img.get("alt") if img else None

    name_a = li.select_one("div.name a")
    display_name = name_a.get_text(strip=True) if name_a else None

    return {
        "member_id": member_id,
        "profile_url": a["href"],
        "img_url": avatar_src,
        "member_name": display_name or avatar_alt,
    }


def fetch_members_page(group_id: str, start: int, headers: dict, timeout: int = 10):
    url = MEMBERS_URL.format(group_id, start)
    resp = requests.get(url, headers=headers, timeout=timeout)
    if resp.status_code != 200:
        logging.warning("GET %s failed: %s", url, resp.status_code)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select("div.member-list ul > li")

    parsed = []
    for li in items:
        d = parse_member_li(li)
        if d:
            parsed.append(d)
    return parsed


def insert_member(cursor, group_id: int, member: dict) -> bool:
    """"
    True = newly inserted
    False = already exists
    """
    cursor.execute(
        INSERT_MEMBER_SQL,
        (
            group_id,
            member.get("member_id"),
            member.get("member_name"),
            member.get("img_url"),
        )
    )
    return cursor.fetchone() is not None


def main():
    conn = get_db_conn()
    headers = get_headers()

    try:
        group_list = load_groups_from_db(conn)
        logging.info("📦 Loaded %d groups from DB", len(group_list))
        with conn.cursor() as cursor:
            for g in group_list:
                group_id = int(g["group_id"])
                group_name = g.get("group_name")
                max_page = int(g.get("max_page", 0))

                logging.info("🚀 Start group %s (%s), max_page=%s", group_id, group_name, max_page)

                # page: 0..max_page-1  -> start: 0,36,...,(max_page-1)*36
                for page in range(max_page):
                    start = page * PAGE_SIZE
                    try:
                        logging.info("📄 Group %s page %s/%s, start=%s", group_id, page + 1, max_page, start)

                        members = fetch_members_page(
                            group_id=str(group_id),
                            start=start,
                            headers=headers
                        )

                        if not members:
                            logging.warning("⚠️ No members parsed. group=%s start=%s", group_id, start)
                            # can choose to break here if you think no members means we have reached the end, but in practice it's safer to continue to next page in case of temporary issues
                            continue

                        inserted = 0
                        for m in members:
                            if insert_member(cursor, group_id, m):
                                inserted += 1

                        conn.commit()
                        logging.info("✅ Group %s start=%s: inserted %s / %s", group_id, start, inserted, len(members))

                    except (requests.exceptions.RequestException, psycopg2.Error) as e:
                        conn.rollback()
                        logging.error("❌ Group %s start=%s failed: %s (rollback)", group_id, start, e)
                        # if failed, it can continue to next page, or you can choose to break to stop crawling this group
                        continue

                    time.sleep(10)

    finally:
        conn.close()
        logging.info("🏁 All groups finished")

if __name__ == "__main__":
    start_ts = datetime.now()
    logging.info("🕒 Job start at %s", start_ts.strftime("%Y-%m-%d %H:%M:%S"))
    try:
        main()
    finally:
        logging.shutdown()