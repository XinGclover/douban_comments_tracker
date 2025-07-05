from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def save_cookie_to_file(cookie_file="cookie.txt"):
    """ Save cookies from a logged-in session to a file.
    :param cookie_file: str, path to the cookie file
    :return: None
    """
    chrome_options = Options()
    chrome_options.binary_location = "/Applications/Chrome137.app/Contents/MacOS/Google Chrome for Testing"

    driver = webdriver.Chrome(options=chrome_options)
    try:
        driver.get("https://accounts.douban.com/passport/login")
        print("Please complete the login in your browser, and press Enter to continue...")
        input()

        cookies = driver.get_cookies()
        cookie_str = ""
        for c in cookies:
            cookie_str += f"{c['name']}={c['value']}; "

        with open(cookie_file, "w") as f:
            f.write(cookie_str.strip())
        print(f"Cookie has been saved to {cookie_file}")
    finally:
        driver.quit()

if __name__ == "__main__":
    save_cookie_to_file()
