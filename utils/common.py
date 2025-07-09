import random
import time
from datetime import datetime
import pytz

def safe_number(value):
    """
    Convert value to int if it's integer.
    Otherwise convert to float rounded to 1 decimal place.
    Return None if conversion fails.
    """
    if value is None:
        return None
    try:
        num = float(value)
        if num.is_integer():
            return int(num)
        else:
            return round(num, 1)
    except (ValueError, TypeError):
        return None


def safe_float_percent(value):
    """ Safely converts a percentage string to a float.
    :param value: The percentage string to convert
    :return: float if conversion is successful, None otherwise
    """
    if value is None:
        return None
    try:
        if isinstance(value, str) and '%' in value:
            return float(value.strip('%'))
        return float(value)
    except ValueError:
        return None



def safe_sleep(min_sec=10, max_sec=20):
    """ Sleep for a random duration between min_sec and max_sec. """
    s = random.uniform(min_sec, max_sec)
    print(f"🕒 Sleeping {round(s, 1)} seconds...")
    time.sleep(s)


TIMEZONE = pytz.timezone('Europe/Stockholm')


def wait_until_next_even_hour(task_interval_hours=2, check_interval_seconds=1200):
    """ Wait until the next even hour (e.g., 00:00, 02:00, etc.) """
    while True:
        now = datetime.now(TIMEZONE)
        if now.minute == 0 and now.second < 5 and now.hour % task_interval_hours == 0:
            return
        time.sleep(check_interval_seconds)