# streamlit_console/core/tasks.py
from __future__ import annotations

CATEGORIES = [
    "Douban Members",
    "Douban Topics",
    "Douban Posts",
    "Douban Comments",
    "LLM Analysis",
]

VIEWS = [
    {
        "name": "📈 未分析的回复数量",
        "view": "v_reply_amount_unlabeled",
        "category": "LLM Analysis",
        "desc": "剩余 LLM 标注的回复数量",
        "sortable": [],
        "default_order": None,
    },
    {
        "name": "🚫 最活跃的黑子",
        "view": "v_active_haters",
        "category": "Douban Posts",
        "desc": "LLM 标注hater的回复量排行（仍有误判的粉丝）",
        "sortable": ["reply_count", "user_id", "pubtime"],
        "default_order": "reply_count DESC",
    },
    {
        "name": "🕵️ 萌物组卧底分布",
        "view": "v_members_distribution",
        "category": "Douban Members",
        "desc": "其他小组成员在萌物组的分布情况",
        "sortable": ["member_cnt", "group_who"],
        "default_order": "member_cnt DESC",
    },
    {
        "name": "📊 积极评论者分布",
        "view": "v_reply_users_distribution",
        "category": "Douban Members",
        "desc": "回复数量排名顺序的用户在各小组内的分布情况",
        "sortable": ["reply_count"],
        "default_order": "reply_count DESC",
    },
    {
        "name": "🕵️ 给朝雪录打1星者分布",
        "view": "v_lowrating_users_distribution",
        "category": "Douban Members",
        "desc": "各后花园给朝雪录打1星的人数统计",
        "sortable": ["user_cnt"],
        "default_order": "user_cnt DESC",
    },
    {
        "name": "📈 未爬取话题数（兰迪，landy）",
        "view": "v_posts_amount_uncrawled",
        "category": "Douban Topics",
        "desc": "统计截止目前，话题表中未爬取的兰迪,landy相关话题数量",
        "sortable": [],
        "default_order": None,
    },
    {
        "name": "🔍 Watchlist里人的分组 ",
        "view": "v_groups_watchlist",
        "category": "Douban Members",
        "desc": "最近搜过的一些人的成分",
        "sortable": [],
        "default_order": None,
    },
    {
        "name": "📊 各后花园发帖统计",
        "view": "v_groups_focus",
        "category": "Douban Posts",
        "desc": "统计各个后花园里发帖数量和回复数量，看看哪些后花园对兰迪比较关注",
        "sortable": [],
        "default_order": None,
    },
    {
        "name": "📊 明星粉丝发帖统计",
        "view": "v_fans_focus",
        "category": "Douban Posts",
        "desc": "统计明星粉丝发帖数量和回复数量，看看哪些明星粉丝对兰迪比较关注",
        "sortable": [],
        "default_order": None,
    }
]
