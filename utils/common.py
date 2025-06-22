import random
import time 

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
    try:
        if not value or '%' not in value:
            return None
        return float(value.strip('%'))
    except ValueError:
        return None
    
    

def safe_sleep(min_sec=10, max_sec=20):
    """ Sleep for a random duration between min_sec and max_sec. """
    s = random.uniform(min_sec, max_sec)
    print(f"🕒 Sleeping {round(s, 1)} seconds...")
    time.sleep(s)