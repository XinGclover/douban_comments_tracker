import re
from utils.common import safe_number

def extract_count(source, pattern, selector=None):
    """ Extracts a count from a BeautifulSoup object using a regex pattern.
    :param soup: BeautifulSoup object containing the HTML content
    :param pattern: Regex pattern to match the count
    :param selector: Optional CSS selector to find the specific tag
    :return: Extracted count as an integer or float, or None if not found
    """
    try:
        if isinstance(source, str):
            text = source.strip()
        else:
            if selector:
                # If the selector is for comments, handle it separately
                if selector == 'a[href*="comments?status=P"]':
                    tag = source.find('a', href=re.compile(r'comments\?status=[PF]'))
                else:
                    tag = source.select_one(selector)
                # If the tag is not found, return None
                if tag is None:
                    print(f"⚠️ Selector not found: {selector}")
                    return None
                # Extract text from the tag
                text = tag.get_text(strip=True)
            else:
                text = source.get_text()
        # Use regex to find the count in the text
        match = re.search(pattern, text)
        if match:
            raw_value = match.group(1)
            raw_value = raw_value.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
            raw_value = raw_value.replace(',', '').replace(' ', '')
            return safe_number(raw_value)
        else:
            print(f"⚠️ Pattern not matched: {pattern}")
            return None

    except (AttributeError, TypeError, re.error) as e:
        print(f"❌ Error extracting count: {e}")
        return None


def extract_href_info(pattern, tag):
    """ Extracts user_id from a tag containing a link.
    :param tag: BeautifulSoup tag containing the link
    :return: Extracted user_id as a string or None if not found
    """
    try:
        href = tag['href']
        match = re.search(pattern, href)
        if match:
            return match.group(1)
        else:
            print(f"⚠️ Unable to extract user_id from link: {href}")
            return None
    except (AttributeError, TypeError, KeyError) as e:
        print(f"❌ Failed to extract user_id: {e}")
        return None
