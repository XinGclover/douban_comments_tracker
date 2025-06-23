import os
from dotenv import load_dotenv

load_dotenv()

DOUBAN_DRAMAS = {
    "ZHAOXUELU": {
        "title": "朝雪录", 
        "id": "36317401",
        "table": "zhaoxuelu_comments",
        "count_table": "zhaoxuelu_comments_count",
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
    },
    "LINJIANGXIAN": {
        "title": "临江仙", 
        "id": "36686675",
        "table": "linjiangxian_comments",
        "count_table": "linjiangxian_comments_count",
        "iqiyi_id": "a_kn7105i0c5",
        "iqiyi_heat_table": "linjiangxian_heat_iqiyi",
    },
      "BAIYUEFANXING": {
        "title": "白月梵星", 
        "id": "36094754",
        "table": "baiyuefanxing_comments",
        "count_table": "baiyuefanxing_comments_count",
        "iqiyi_id": "a_ee9igdu3k9",
        "iqiyi_heat_table": "baiyuefanxing_heat_iqiyi",
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
TABLE_NAME = drama_info["table"]
DRAMA_TITLE = drama_info["title"]
COUNT_TABLE_NAME = drama_info["count_table"]
BASE_URL = f"https://movie.douban.com/subject/{DOUBAN_DRAMA_ID}/"

# Constants for the selected drama of iQIYI 
IQIYI_DRAMA_ID = drama_info.get("iqiyi_id")
IQIYI_HEAT_TABLE_NAME = drama_info.get("iqiyi_heat_table")
IQIYI_BASE_URL = f"https://www.iqiyi.com/{IQIYI_DRAMA_ID}.html" if IQIYI_DRAMA_ID else None

IQIYI_HEADERS = {  
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8"
}