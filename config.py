import os
from dotenv import load_dotenv

load_dotenv()

DOUBAN_DRAMA_IDS = {
    "FILTER" : "36553916",
    "LIZHI" : "35651341",
    "ZHAOXUELU" : "36317401"
}

DOUBAN_DRAMAS = {
    "ZHAOXUELU": {
        "id": "36317401",
        "table": "zhaoxuelu_comments",
        "title": "朝雪录"
    },
    "LIZHI": {
        "id": "35651341",
        "table": "lizhi_comments",
        "title": "长安的荔枝"
    },
    "FILTER": {
        "id": "36553916",
        "table": "filter_comments",
        "title": "滤镜"
    }
}

TARGET_DRAMA = os.getenv("TARGET_DRAMA")

if not TARGET_DRAMA:
    raise ValueError("Environment variable TARGET_DRAMA is not set. Please set it in .env file or system environment.")
drama_info = DOUBAN_DRAMAS.get(TARGET_DRAMA)

if not drama_info:
    raise ValueError(f"Drama '{TARGET_DRAMA}' not found in config. Please check config.py or .env.")

DOUBAN_DRAMA_ID = drama_info["id"]
TABLE_NAME = drama_info["table"]
DRAMA_TITLE = drama_info["title"]

BASE_URL = f"https://movie.douban.com/subject/{DOUBAN_DRAMA_ID}/"
