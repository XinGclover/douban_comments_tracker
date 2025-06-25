import os
from dotenv import load_dotenv

load_dotenv()

DOUBAN_DRAMAS = {
    "ZHAOXUELU": {
        "title": "朝雪录", 
        "id": "36317401",
        "name": "zhaoxuelu",
        "iqiyi_id": "a_j7cnht5c2t",
    },
    "LIZHI": {
        "title": "长安的荔枝", 
        "id": "35651341",
        "name": "lizhi"        
    },
    "FILTER": {
        "title": "滤镜",
        "id": "36553916",
        "name": "filter"        
    },
    "HUANYU": {
        "title": "焕羽", 
        "id": "36455107",
        "name": "huanyu",
    },
    "LINJIANGXIAN": {
        "title": "临江仙", 
        "id": "36686675",
        "name": "linjiangxian", 
        "iqiyi_id": "a_kn7105i0c5",
    },
      "BAIYUEFANXING": {
        "title": "白月梵星", 
        "id": "36094754",
        "name": "baiyuefanxing",
        "iqiyi_id": "a_ee9igdu3k9",
    },
    "SHUJUANYIMENG": {
        "title": "书卷一梦",  
        "id": "36744438",
        "name": "shujuanyimeng",
        "iqiyi_id": "a_16xvy4vfx8h",
    }
}

TARGET_DRAMA = os.getenv("TARGET_DRAMA")

if not TARGET_DRAMA:
    raise ValueError("Environment variable TARGET_DRAMA is not set. Please set it in .env file or system environment.")
drama_info = DOUBAN_DRAMAS.get(TARGET_DRAMA)

if not drama_info:
    raise ValueError(f"Drama '{TARGET_DRAMA}' not found in config. Please check config.py or .env.")

# Constants for the selected drama of Douban 
DOUBAN_DRAMA_ID = drama_info["id"]
TABLE_PREFIX = drama_info["name"]
DRAMA_TITLE = drama_info["title"]

BASE_URL = f"https://movie.douban.com/subject/{DOUBAN_DRAMA_ID}" if DOUBAN_DRAMA_ID else None

# Constants for the selected drama of iQIYI 
IQIYI_DRAMA_ID = drama_info.get("iqiyi_id")

IQIYI_BASE_URL = f"https://www.iqiyi.com/{IQIYI_DRAMA_ID}.html" if IQIYI_DRAMA_ID else None

IQIYI_HEADERS = {  
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}

DB_TABLES = [
    f"{TABLE_PREFIX}_comments_count",
    f"{TABLE_PREFIX}_comments",
    f"{TABLE_PREFIX}_heat_iqiyi",
]