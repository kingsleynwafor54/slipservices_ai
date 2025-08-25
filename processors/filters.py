
import hashlib
import re
from dateutil import parser

def make_fingerprint(link: str, title: str) -> str:
    key = (link or "") + "|" + (title or "")
    return hashlib.sha256(key.encode("utf-8")).hexdigest()

def matches_any(text: str, keywords):
    if not text:
        return False
    t = text.lower()
    return any(k.lower() in t for k in keywords)

def parse_date_safe(dt_str: str):
    try:
        return parser.parse(dt_str).isoformat()
    except Exception:
        return None
