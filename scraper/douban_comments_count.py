import requests
from datetime import datetime
from bs4 import BeautifulSoup
from config import COUNT_TABLE_NAME, BASE_URL, HEADERS
from db import get_db_conn
import re


def extract_movie_stats(drama_url, headers=None):
    """ Extracts movie statistics from the given Douban drama URL.
    :param drama_url: URL of the Douban drama page
    :return: dict containing movie statistics such as rating people, total comments, reviews, discussions, and rating percentages
    """ 
    response = requests.get(drama_url, headers=headers, timeout=10)
    soup = BeautifulSoup(response.text, 'html.parser')

    def extract_count(soup, pattern, selector=None):
        try:
            if selector:
                # If the selector is for comments, handle it separately 
                if selector == 'a[href*="comments?status=P"]':
                    tag = soup.find('a', href=re.compile(r'comments\?status=[PF]'))
                else:
                    tag = soup.select_one(selector)
                # If the tag is not found, return None
                if tag is None:
                    print(f"⚠️ Selector not found: {selector}")
                    return None
                # Extract text from the tag 
                text = tag.get_text(strip=True)
            else:
                text = soup.get_text()
            # Use regex to find the count in the text 
            match = re.search(pattern, text)
            if match:
                raw_value = match.group(1).replace(',', '').replace(' ', '')
                return safe_number(raw_value) 
            else:
                print(f"⚠️ Pattern not matched: {pattern}")
                return None
            
        except (AttributeError, TypeError, re.error) as e:
            print(f"❌ Error extracting count: {e}")
            return None

    rating = extract_count(soup, r'(\d+\.\d+)', 'strong[property="v:average"]')
    rating_people = extract_count(soup, r'(\d+)', 'span[property="v:votes"]')
    total_comments = extract_count(soup, r'全部\s*(\d+)\s*条', 'a[href*="comments?status=P"]')
    total_reviews = extract_count(soup, r'全部\s*(\d+)\s*条', 'a[href="reviews"]')
    total_discussions = extract_count(soup, r'全部\s*(\d+)\s*条', 'p.pl[align="right"]')

    rating_percents = []
    percent_tags = soup.select('.ratings-on-weight .item .rating_per')
    for tag in percent_tags:
        rating_percents.append(tag.get_text().strip())

    return {
        "insert_time": datetime.now(),
        "rating": rating,
        "rating_people": rating_people,
        "total_comments": total_comments,
        "total_reviews": total_reviews,
        "total_discussions": total_discussions,
        "rating_percents": rating_percents,       
    }


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


def insert_movie_stats(movie_stats, db_conn):
    """ Inserts movie statistics into the database.
    :param movie_stats: dict containing movie statistics
    :param conn: psycopg2 connection object
    :return: None
    """
    rating = safe_number(movie_stats.get('rating')) 
    total_comments = safe_number(movie_stats.get('total_comments'))
    total_discussions = safe_number(movie_stats.get('total_discussions'))
    total_reviews = safe_number(movie_stats.get('total_reviews'))
    rating_people = safe_number(movie_stats.get('rating_people'))

    percents = movie_stats.get('rating_percents', [])
    percents = [safe_float_percent(p) for p in percents] + [None] * 5
    rating_1, rating_2, rating_3, rating_4, rating_5 = percents[:5]

    insert_time = movie_stats.get('insert_time', datetime.now())

    sql = f"""
    INSERT INTO {COUNT_TABLE_NAME} (
        insert_time,
        rating,
        rating_people,
        rating_1_star, rating_2_star, rating_3_star, rating_4_star, rating_5_star,    
        total_comments, total_reviews, total_discussions
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (insert_time) DO NOTHING;
    """

    params = (    
        insert_time,
        rating,
        rating_people,
        rating_1,
        rating_2,
        rating_3,
        rating_4,
        rating_5,
        total_comments,     
        total_reviews,
        total_discussions      
    )

    with db_conn.cursor() as cursor:
        cursor.execute(sql, params)
    db_conn.commit()
    print("✅ Inserted movie stats at", insert_time)


if __name__ == "__main__":
    stats = extract_movie_stats(BASE_URL,HEADERS)
    conn = get_db_conn()
    insert_movie_stats(stats, conn)
    conn.close()

