# streamlit_console/core/tasks.py
from __future__ import annotations

CATEGORIES = [
    "Douban Scraper",
    "Iqiyi Scraper",
    "Airflow Tasks",
    "LLM Analysis",
    "Common Analysis",
    "Tools"
]

TASKS = [
    {
        "name": "🦎 爬取整篇 post + replies",
        "cmd": ["python3", "-m", "scraper.douban_post_scraper"],
        "category": "Douban Scraper",
        "desc": "从 Douban 抓取主贴与回复",
    },
    {
        "name": "🦎 根据关键词抓取相关讨论 topics ",
        "cmd": ["python3", "-m", "scraper.douban_topics_statistic_scraper"],
        "category": "Douban Scraper",
        "desc": "从 Douban 抓取 post 列表",
    },
    {
        "name": "🦎 搜集小组成员名单",
        "cmd": ["python3", "-m", "scraper.douban_group_members"],
        "category": "Douban Scraper",
        "desc": "从 Douban 抓取小组成员名单",
    },
    {
        "name": "🦎 爬取剧集短评",
        "cmd": ["python3", "-m", "scraper.douban_comments_scraper"],
        "category": "Douban Scraper",
        "desc": "从 Douban 抓取剧集短评",
    },
    {
        "name": "🤖 LLM 标注 posts",
        "cmd": ["python3", "-m", "analysisLLM.label_posts_with_llm"],
        "category": "LLM Analysis",
        "desc": "读取 raw 表，调用本地 Ollama",
    },
    {
        "name": "📊 结巴分词",
        "cmd": ["python3", "-m", "analysis.jieba_words"],
        "category": "Common Analysis",
        "desc": "读取 comment 表，进行结巴分词",
    },
    {
        "name": "🥝 爬取爱奇艺实时热度",
        "cmd": ["python3", "-m", "iqiyi.heat_scraper"],
        "category": "Iqiyi Scraper",
        "desc": "从爱奇艺抓取实时热度数据",
    },
    {
        "name": "🔧 更新cookies",
        "cmd": ["python3", "-m", "update_cookie"],
        "category": "Tools",
        "desc": "更新 cookies",
    },
    {
        "name": "🌀 Start Airflow Web Server UI",
        "cmd": ["airflow", "webserver", "--port", "8080"],
        "category": "Airflow Tasks",
        "desc": "启动 Airflow Web UI (8080)",
    },
    {
        "name": "🌀 Start Airflow Scheduler to trigger DAGs",
        "cmd": ["airflow", "scheduler"],
        "category": "Airflow Tasks",
        "desc": "启动 Airflow Scheduler 以触发 DAGs",
    }
]
