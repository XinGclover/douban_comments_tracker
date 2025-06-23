import logging
import os
from pathlib import Path

from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

PROJECT_PATH = os.environ["PROJECT_PATH"]
cookie_path = os.path.join(PROJECT_PATH, "cookie.txt")

def load_cookie(cookie_file=cookie_path):
    """ Load cookie from a file.
    :param cookie_file: str, path to the cookie file 
    :return: str, cookie string """ 
   
    try:
        with open(cookie_file, "r", encoding="utf-8") as f:
            return f.read().strip()
    except FileNotFoundError:
        logging.error("⚠️ Cookie file not found: %s", cookie_file)
        return ""

def get_headers():
    """ Get headers for requests with loaded cookie.
    :return: dict, headers for requests """ 
   
    cookie_str = load_cookie()
    return {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
        "Cookie": cookie_str,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }