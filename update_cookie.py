import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def wait_for_login(driver, timeout=300):
    """
    Wait until user logs in successfully.
    Check login status by detecting specific cookie (e.g. dbcl2).
    """
    start_time = time.time()

    while time.time() - start_time < timeout:
        cookies = driver.get_cookies()

        # After successfully logging into Douban, you will usually see dbcl2.
        if any(c["name"] == "dbcl2" for c in cookies):
            print("Login detected ✅")
            return True

        print("Waiting for login...")
        time.sleep(2)

    return False


def save_cookie_to_file(cookie_file="cookie.txt"):
    chrome_options = Options()
    chrome_options.binary_location = "/Applications/Chrome137.app/Contents/MacOS/Google Chrome for Testing"

    driver = webdriver.Chrome(options=chrome_options)

    try:
        driver.get("https://accounts.douban.com/passport/login")
        print("Please complete the login in the browser...")

        success = wait_for_login(driver)

        if not success:
            print("Login timeout ❌")
            return

        cookies = driver.get_cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])

        with open(cookie_file, "w") as f:
            f.write(cookie_str)

        print(f"Cookie saved to {cookie_file} ✅")

    finally:
        driver.quit()


if __name__ == "__main__":
    save_cookie_to_file()