# streamlit_console/core/tasks.py
from __future__ import annotations

CATEGORIES = [
    "Douban Scraper",
    "Iqiyi Scraper",
    "LLM Analysis",
    "Common Analysis",
]

TASKS = [
    {
        "name": "爬取整篇 post + replies",
        "cmd": ["python3", "-m", "scraper.douban_post_scraper"],
        "category": "Douban Scraper",
        "desc": "从 Douban 抓取主贴与回复，写入 raw 表。",
    },
    {
        "name": "LLM 标注 posts",
        "cmd": ["python3", "-m", "analysisLLM.label_posts_with_llm"],
        "category": "LLM Analysis",
        "desc": "读取 raw 表，调用本地 Ollama，写入 ai 表。",
    },
    {
        "name": "生成分析报告",
        "cmd": ["python3", "-m", "report.report_generator"],
        "category": "Common Analysis",
        "desc": "读取 ai 表，生成分析报告（Markdown 文件）。",
    },
    {
        "name": "清理日志",
        "cmd": ["python3", "-m", "core.clean_logs"],
        "category": "Common Analysis",
        "desc": "清理日志文件，释放磁盘空间。",
    }
]
