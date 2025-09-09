import re
from datetime import datetime
from dateutil import parser

def norm_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def parse_date(dt_str: str):
    if not dt_str:
        return None
    try:
        return parser.parse(dt_str)
    except Exception:
        return None

def looks_like_article_link(href: str) -> bool:
    if not href:
        return False
    bad = ["#", "javascript:", "mailto:"]
    return not any(href.lower().startswith(b) for b in bad)

def contains_keywords(text: str, any_words) -> bool:
    t = (text or "").lower()
    return any(w.lower() in t for w in any_words)

def contains_borough(text: str, boroughs) -> bool:
    t = (text or "").lower()
    return any(b.lower() in t for b in boroughs)

def to_iso(dt):
    if isinstance(dt, datetime):
        return dt.isoformat()
    return None
