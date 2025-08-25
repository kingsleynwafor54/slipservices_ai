
import feedparser
from urllib.parse import quote_plus

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-GB&gl=GB&ceid=GB:en"

def search_google_news(query: str):
    url = GOOGLE_NEWS_RSS.format(query=quote_plus(query))
    feed = feedparser.parse(url)
    for e in feed.entries:
        yield {
            "title": getattr(e, "title", ""),
            "link": getattr(e, "link", ""),
            "published": getattr(e, "published", ""),
            "summary": getattr(e, "summary", ""),
        }
