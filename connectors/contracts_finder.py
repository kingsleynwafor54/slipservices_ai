
import requests

# UK Contracts Finder search API (public). See: https://www.contractsfinder.service.gov.uk/
# NOTE: This is a lightweight example using the web search endpoint, which returns HTML/JSON depending on params.
# For robust usage, consult official API docs and consider pagination/backoff.

BASE = "https://www.contractsfinder.service.gov.uk/Published/Notices/Search"

def search_tenders(keyword: str, page: int = 1):
    params = {
        "includeClosed": "false",
        "awarded": "false",
        "showExpiring": "false",
        "page": page,
        "q": keyword
    }
    r = requests.get(BASE, params=params, timeout=30)
    r.raise_for_status()
    # The endpoint returns HTML; parsing fully requires BeautifulSoup. We keep this as a placeholder return.
    return {"url": r.url, "status_code": r.status_code}
