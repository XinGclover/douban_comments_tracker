import logging

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from db import get_db_conn
from utils.logger import setup_logger

setup_logger("logs/weibo_scraper.log", logging.INFO)


USER_URL = "https://weibo.com/u/{}"

INSERT_SQL = """
    INSERT INTO weibo_user_stats (user_id, followers_count, followings_count, likes_count)
    VALUES (%s, %s, %s, %s)
"""


def parse_count(count_str):
    """
    Parse Weibo's fans, likes, reposts and other counting strings and convert them into integers
    """
    count_str = count_str.replace(',', '').strip()
    if '万' in count_str:
        return int(float(count_str.replace('万', '')) * 10_000)
    elif '亿' in count_str:
        return int(float(count_str.replace('亿', '')) * 100_000_000)
    else:
        try:
            return int(count_str)
        except ValueError:
            return None  # If it cannot be parsed, it returns None.


def crawl_weibo(user_id):
    """Use Selenium to start a headless browser and visit the Weibo user homepage.
    Wait for the page to load completely and use driver.page_source to get the rendered HTML.
    Use your previous robust XPath to extract the required data from the HTML.
    """
    url = USER_URL.format(user_id)
    options = Options()
    options.binary_location = "/Applications/Chrome137.app/Contents/MacOS/Google Chrome for Testing"
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')

    service = Service(executable_path="/usr/local/bin/chromedriver")

    driver = webdriver.Chrome(service=service, options=options)
    driver.get(url)

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//div[contains(@class, 'ProfileHeader_h4')]"))
        )

        followers_text = driver.find_element(By.XPATH, '//div[contains(@class, "ProfileHeader_h4")]//span[contains(text(), "粉丝")]/span').text
        followings_text = driver.find_element(By.XPATH, '//div[contains(@class, "ProfileHeader_h4")]//span[contains(text(), "关注")]/span').text
        likes_text = driver.find_element(By.XPATH, '//div[contains(@class, "ProfileHeader_h4")]//span[contains(text(), "转评赞")]/span').text

        followers_count = parse_count(followers_text)
        followings_count = parse_count(followings_text)
        likes_count = parse_count(likes_text)

        try:
            conn = get_db_conn()
            cursor = conn.cursor()
            cursor.execute(INSERT_SQL, (user_id, followers_count,followings_count,likes_count))
            conn.commit()

            logging.info("✅ The weibo user data is inserted, user_id=%s", user_id)

        except Exception as e:
            logging.error("❌ Error inserting low-scoring user data: %s", str(e))
            raise
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    finally:
        driver.quit()


if __name__ == "__main__":
    test_id = 1798539915
    crawl_weibo(test_id)
