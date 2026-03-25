
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def to_number(text):
    if text is None:
        return None
    if isinstance(text, (int, float)):
        return text
    s = str(text).strip()
    if s == "":
        return None
    negative = False
    if s.startswith("(") and s.endswith(")"):
        negative = True
        s = s[1:-1]
    s = re.sub(r"[^\d.\\-]", "", s)
    if s == "":
        return None
    try:
        num = float(s)
        return -num if negative else num
    except Exception as e:
        logger.error(f"Failed to convert text to number: {text}", exc_info=True)
        return None

def to_datetime(text_list):
    if not text_list:
        return []
    results = []
    for t in text_list:
        text = str(t).strip()
        try:
            date_part = text.split(' ')[0]
            parts = date_part.split('/')
            if len(parts) == 3:
                year = int(parts[0]) + 1911
                month = parts[1]
                day = parts[2]
                clean_text = f"{year}-{month}-{day}"
                dt = pd.to_datetime(clean_text)
                results.append(dt)
        except Exception as e:
            logger.error(f"Failed to convert text to datetime: {t}", exc_info=True)
            pass
    return results