
import feedparser

# HSE press office / news RSS (example; adjust if needed)
HSE_NEWS_RSS = "https://press.hse.gov.uk/feed/"
HSE_SAFETY_ALERTS_RSS = "https://www.hse.gov.uk/syndication/alerts.xml"

def hse_news():
    for url in [HSE_NEWS_RSS, HSE_SAFETY_ALERTS_RSS]:
        feed = feedparser.parse(url)
        for e in feed.entries:
            yield {
                "title": getattr(e, "title", ""),
                "link": getattr(e, "link", ""),
                "published": getattr(e, "published", ""),
                "summary": getattr(e, "summary", ""),
            }
