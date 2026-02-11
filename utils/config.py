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


COLLECT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}



VIEW_WHITELIST = [
    ("public", f"view_{TABLE_PREFIX}_comments_count_with_shanghai_time"),
    ("public", f"view_{TABLE_PREFIX}_comments_rating_percentage"),
    ("public", f"view_{TABLE_PREFIX}_comments_distribution_long"),
    ("public", f"view_{TABLE_PREFIX}_heat_iqiyi_with_shanghai_time",),
    ("public", f"view_{TABLE_PREFIX}_iqiyiheat_timeline"),
    ("public", "view_high_rating_dramas_source"),
    ("public", f"view_{TABLE_PREFIX}_top_words"),
    ("public", f"view_{TABLE_PREFIX}_daily_top_words"),
    ("public", "view_weibo_stats"),
    ("public", f"view_{TABLE_PREFIX}_comments_rating_percentage_with_time"),
]

POST_LIST = [
    # {
    #     "topic_id": "475514385"
    # }
    {
        "topic_id": "475249400"
    }
    # {
    #     "topic_id": "341265141",
    #     "start": 0,
    #     "end": 6,
    #     "title": "午夜钟声⏳｜11🈷️永夜森林🌳 "
    # },
    # {
    #     "topic_id": "341265185",
    #     "start": 0,
    #     "end": 6,
    #     "title": "午夜钟声⏳｜11🈷️公主🧚‍♀️茶话会"
    # }
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
