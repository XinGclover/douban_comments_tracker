import logging

import psycopg2
import requests
from bs4 import BeautifulSoup

from db import get_db_conn
from utils.common import safe_sleep
from utils.config_loader import get_headers
from utils.logger import setup_logger
from pathlib import Path
from utils.html_tools import extract_href_info, extract_count


LOG_PATH = Path(__file__).resolve().parent.parent / "logs" / "douban_post_scraper.log"
setup_logger(log_file=str(LOG_PATH))


BASE_URL_PAGE = "https://www.douban.com/group/topic/{}/?start={}"


post_list = [
    {
        "topic_id": "341265141",
        "start": 0,
        "end": 6,
        "title": "午夜钟声⏳｜11🈷️永夜森林🌳 "
    },
    {
        "topic_id": "341265185",
        "start": 0,
        "end": 6,
        "title": "午夜钟声⏳｜11🈷️公主🧚‍♀️茶话会"
    }
    # {
    #     "topic_id": "338469865",
    #     "start": 20,
    #     "end": 25,
    #     "title": "午夜钟声⏳｜10🈷️公主🧚‍♀️茶话会"
    # },
    # {
    #     "topic_id": "338736980",
    #     "start": 13,
    #     "end": 22,
    #     "title": "午夜钟声⏳｜🔟🈷️永夜森林🌳 "
    # },
    # {
    #     "topic_id": "334175701",
    #     "start": 50,
    #     "end": 51,
    #     "title": "午夜钟声⏳｜八月公主🧚‍♀️茶话会2.0"
    # },
    # {
    #     "topic_id": "334501225",
    #     "start": 54,
    #     "end": 56,
    #     "title": "午夜钟声⏳｜八月公主🧚‍♀️茶话会3.0 "
    # },
    # {
    #     "topic_id": "334984342",
    #     "start": 56,
    #     "end": 58,
    #     "title": "午夜钟声⏳｜八月公主🧚‍♀️茶话会4.0 "
    # },
    # {
    #     "topic_id": "335670877",
    #     "start": 51,
    #     "end": 52,
    #     "title": "午夜钟声⏳｜九月公主🧚‍♀️茶话会1.0 "
    # },
    # {
    #     "topic_id": "335673488",
    #     "start": 59,
    #     "end": 61,
    #     "title": "午夜钟声⏳｜9️⃣🈷️永夜森林🌳 "
    # },
    # {
    #     "topic_id": "336517564",
    #     "start": 25,
    #     "end": 27,
    #     "title": "午夜钟声⏳｜9🈷️公主🧚‍♀️茶话会2.0 "
    # },
    # {
    #     "topic_id": "333355524",
    #     "start": 0,
    #     "end": 14,
    #     "title": "真爱之舞💃｜有人考古过这个cut吗？"
    # },
    # {
    #     "topic_id": "334455247",
    #     "start": 80,
    #     "end": 81,
    #     "title": "午夜钟声⏳｜永夜森林🌳"
    # },
    # {
    #     "topic_id": "335667914",
    #     "start": 7,
    #     "end": 8,
    #     "title": "真爱之舞💃｜突然发现一个点，可能是🐢🍬且CPN严重，想🍑一下 "
    # },
    # {
    #     "topic_id": "333177149",
    #     "start": 0,
    #     "end": 13,
    #     "title": "午夜钟声⏳｜关于二搭🍉（四编"
    # },
    # {
    #     "topic_id": "333639193",
    #     "start": 0,
    #     "end": 3,
    #     "title": "破除朝雪录之前arp线下不到200人的洗脑包"  #兰迪黑帖
    # },
    # {
    #     "topic_id": "334216331",
    #     "start": 0,
    #     "end": 3,
    #     "title": "才发现李兰迪有两部cvb破1的一番剧，一部cvb破1的女主剧"
    # },
    # {
    #     "topic_id": "333696018",
    #     "start": 0,
    #     "end": 4,
    #     "title": "其实剧播前… "  #兰迪黑帖
    # },
    # {
    #     "topic_id": "333758638",
    #     "start": 0,
    #     "end": 9,
    #     "title": "八月闲聊楼2.0 "  #敖后花园
    # }
    # {
    #     "topic_id": "334970297",
    #     "start": 0,
    #     "end": 1,
    #     "title": "午夜钟声⏳｜投票 豚哈何时开始谈的 "
    # },
    # {
    #     "topic_id": "335537081",
    #     "start": 0,
    #     "end": 3,
    #     "title": "之前我repo过💧的直播，团队深夜很快就⛰️了，再发一次"
    # },
    # {
    #     "topic_id": "335530018",
    #     "start": 0,
    #     "end": 6,
    #     "title": "算官方辟谣吗？那不是瓜主给💧炒🫓实锤了。。"
    # },
    # {
    #     "topic_id": "335556249",
    #     "start": 0,
    #     "end": 2,
    #     "title": "💧这个事的前因是什么？"
    # },
    # {
    #     "topic_id": "335543754",
    #     "start": 0,
    #     "end": 1,
    #     "title": "组里打的，感觉💧很有流量的苗子了呀 "
    # },
    # {
    #     "topic_id": "335555904",
    #     "start": 0,
    #     "end": 1,
    #     "title": "谁还记得"
    # },
    # {
    #     "topic_id": "335535985",
    #     "start": 0,
    #     "end": 6,
    #     "title": "看过亲爱的客栈真的很难好感💧"
    # },
    # {
    #     "topic_id": "335531020",
    #     "start": 0,
    #     "end": 2,
    #     "title": "💧最近已经被敖丁刘檀四家辟谣了…… "
    # },
    # {
    #     "topic_id": "336215378",
    #     "start": 0,
    #     "end": 1,
    #     "title": "午夜钟声⏳｜九月公主🧚‍♀️茶话会1.0 "
    # },
    # {
    #     "topic_id": "328818302",
    #     "start": 0,
    #     "end": 1,
    #     "title": "🌊毛为什么要骂朝雪录"
    # }
    # {
    #     "topic_id": "321957009",
    #     "start": 0,
    #     "end": 2,
    #     "title": "流水迢迢jrjj问心无愧"
    # }
    # {
    #     "topic_id": "328673166",
    #     "start": 0,
    #     "end": 3,
    #     "title": "我是怎么对白鹿粉转厌恶的？"
    # }
    # {
    #     "topic_id": "340440212",
    #     "start": 0,
    #     "end": 4,
    #     "title": "菌菌维稳了，男频需要发酵，留存率特别高，大家别急 "
    # }
]

INSERT_SQL = """
    INSERT INTO douban_post (
        topic_id, user_id, user_name, pubtime, ip, comment_text, like_count
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (topic_id, user_id, pubtime) DO NOTHING
    """



def parse_row(li):
    result = {}

    user_tag = li.find("h4").find("a") if li.find("h4") else None
    result['user_id'] = extract_href_info(r'/people/([^/]+)/', user_tag)
    result['user_name'] = user_tag.get_text(strip=True) if user_tag else ""

    pubtime_tag = li.find("span", class_="pubtime")

    pubtime_text = pubtime_tag.get_text(strip=True) if pubtime_tag else ""
    if pubtime_text:
        parts = pubtime_text.split()
        result['pubtime'] = " ".join(parts[:2])
        result['ip'] = parts[2] if len(parts) > 2 else ""
    else:
        result['pubtime'], result['ip'] = "", ""

    reply_content = li.find("div", class_="reply-content")
    result['comment_text'] = " ".join(p.get_text(strip=True) for p in reply_content.find_all("p")) if reply_content else ""

    like_count = extract_count(li, r'(\d+)', 'a[class*="comment-vote"]')
    result['like_count'] = like_count if like_count is not None else 0

    return result


def fetch_topic_page(post_id, page_num=0, headers=None):
    url = BASE_URL_PAGE.format(post_id, page_num * 100)

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            logging.error("Failed: %s", resp.status_code)
            return []

        block = BeautifulSoup(resp.text, "html.parser")
        rows = block.find_all("li", class_="comment-item")

        return [parse_row(r) for r in rows]

    except (requests.exceptions.RequestException, psycopg2.Error) as e:
        logging.error("Error: %s", e)
        return []



def insert_single_topic(cursor, post_dict, post_meta):

    try:
        params = (
            post_meta['topic_id'],
            post_dict['user_id'],
            post_dict['user_name'],
            post_dict['pubtime'],
            post_dict['ip'],
            post_dict['comment_text'],
            post_dict['like_count']
        )
        cursor.execute(INSERT_SQL, params)
        return cursor.rowcount == 1
    except (ValueError, TypeError, psycopg2.Error) as e:
        logging.error("❌ Insert failed: %s", e)
        logging.info("🔧 Wrong data: %s", post_dict)
        return False


def main_loop(post):
    conn = get_db_conn()
    request_headers = get_headers()

    for page in range(post['start'], post['end']):  # Adjust range as needed
        try:
            logging.info("\n📄 Fetching comments on page %s...", page)
            topics = fetch_topic_page(post['topic_id'], page, headers=request_headers)
            logging.info("📄 Fetched %d comments on page %d", len(topics), page)
            if not topics:
                logging.warning("⚠️ No more comments, may be limited or reached the end")
                break

            with conn.cursor() as cursor:
                for c in topics:
                    success = insert_single_topic(cursor, c, post)
                    if success:
                        logging.info("✅ Insert user_name=%s", c['user_name'])
                    else:
                        logging.warning("⚠️ Failed to insert user_name=%s",c['user_name'])

                conn.commit()

                safe_sleep(20, 30)     # Sleep between requests

        except (requests.exceptions.RequestException, psycopg2.Error) as e:
            conn.rollback()
            logging.error("❌ Page crawl failed: %s, rollback", e)
            safe_sleep(10, 20)  # Sleep before retrying

    conn.close()


if __name__ == "__main__":
    for post in post_list:
        logging.info("🚀 Starting Douban topics scraper for %s", post['title'])
        main_loop(post)  # Adjust start_page and max_pages as needed
    logging.shutdown()

