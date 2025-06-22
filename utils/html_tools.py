import re
from utils.common import safe_number

def extract_count(soup, pattern, selector=None):
    """ Extracts a count from a BeautifulSoup object using a regex pattern.
    :param soup: BeautifulSoup object containing the HTML content
    :param pattern: Regex pattern to match the count
    :param selector: Optional CSS selector to find the specific tag
    :return: Extracted count as an integer or float, or None if not found
    """ 
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