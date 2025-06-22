import os
from dotenv import load_dotenv

load_dotenv()

DOUBAN_DRAMAS = {
    "ZHAOXUELU": {
        "title": "朝雪录", 
        "id": "36317401",
        "table": "zhaoxuelu_comments",
        "count_table": "zhaoxuelu_comments_count"  
    },
    "LIZHI": {
        "title": "长安的荔枝", 
        "id": "35651341",
        "table": "lizhi_comments",
        "count_table": "lizhi_comments_count",
        
    },
    "FILTER": {
        "title": "滤镜",
        "id": "36553916",
        "table": "filter_comments",
        "count_table": "filter_comments_count",        
    },
    "HUANYU": {
        "title": "焕羽", 
        "id": "36455107",
        "table": "huanyu_comments",
        "count_table": "huanyu_comments_count",
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
COUNT_TABLE_NAME = drama_info["count_table"]

BASE_URL = f"https://movie.douban.com/subject/{DOUBAN_DRAMA_ID}/"


