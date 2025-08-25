
import yaml
import os
from datetime import datetime
from connectors.news_rss import search_google_news
from connectors.gov_uk_hse import hse_news
from connectors.tripadvisor_stub import fetch_reviews_stub
from processors.filters import matches_any, make_fingerprint, parse_date_safe
from storage import insert_if_new, init_db

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(DATA_DIR, exist_ok=True)

def load_config():
    cfg_path = os.path.join(os.path.dirname(__file__), "..", "config.yaml")
    with open(cfg_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def normalize(item, source, tags):
    fp = make_fingerprint(item.get("link"), item.get("title"))
    return {
        "source": source,
        "title": item.get("title"),
        "link": item.get("link"),
        "published": parse_date_safe(item.get("published")) or datetime.utcnow().isoformat(),
        "summary": item.get("summary"),
        "tags": tags,
        "fingerprint": fp,
    }

def agent_news_slip_accidents(cfg):
    q = " OR ".join(cfg["keywords"]["slips_any"]) + " site:uk"
    for e in search_google_news(q):
        text = (e.get("title","") + " " + e.get("summary",""))
        if matches_any(text, cfg["keywords"]["slips_any"]):
            yield normalize(e, "news_google", ["slip_incident","news"])

def agent_cleanliness_mentions(cfg):
    q = " OR ".join(cfg["keywords"]["cleanliness_any"]) + " AND (" + " OR ".join(cfg["keywords"]["venues_any"]) + ")"
    for e in search_google_news(q):
        text = (e.get("title","") + " " + e.get("summary",""))
        if matches_any(text, cfg["keywords"]["cleanliness_any"]):
            yield normalize(e, "news_google", ["cleanliness","venue"])

def agent_company_contracts(cfg):
    queries = []
    for c in cfg["companies"]:
        queries += [f"{c} wins contract UK", f"{c} retains contract UK", f"{c} contract renewal UK"]
    for q in queries:
        for e in search_google_news(q):
            yield normalize(e, "news_google", ["company_contracts"])

def agent_leisure_openings(cfg):
    q = "(" + " OR ".join(cfg["keywords"]["venues_any"]) + ") opening OR opens OR new management OR reopens"
    for e in search_google_news(q):
        yield normalize(e, "news_google", ["venue_update"])

def agent_hse_prosecutions(cfg):
    for e in hse_news():
        text = (e.get("title","") + " " + e.get("summary",""))
        if any(k in text.lower() for k in ["prosecut", "fined", "sentenced", "convicted", "safety breach", "notice"]):
            yield normalize(e, "hse_rss", ["hse","enforcement"])
        if any(k in text.lower() for k in ["slip", "fall", "trip"]):
            yield normalize(e, "hse_rss", ["hse","slip_related"])

def agent_tripadvisor_stub(cfg):
    if not cfg["agents"].get("tripadvisor_reviews_stub"):
        return
    for e in fetch_reviews_stub("slip OR slippery OR slippy OR slipped"):
        yield normalize(e, "tripadvisor_stub", ["reviews"])

def run_once():
    cfg = load_config()
    out_db = os.path.join(os.path.dirname(__file__), "..", cfg["outputs"]["sqlite_path"])
    init_db(out_db)

    agents = []
    if cfg["agents"].get("news_slip_accidents"): agents.append(agent_news_slip_accidents)
    if cfg["agents"].get("hse_prosecutions"): agents.append(agent_hse_prosecutions)
    if cfg["agents"].get("company_contracts"): agents.append(agent_company_contracts)
    if cfg["agents"].get("leisure_openings"): agents.append(agent_leisure_openings)
    if cfg["agents"].get("cleanliness_mentions"): agents.append(agent_cleanliness_mentions)
    if cfg["agents"].get("tripadvisor_reviews_stub"): agents.append(agent_tripadvisor_stub)

    all_rows = []
    for fn in agents:
        try:
            for row in fn(cfg):
                all_rows.append(row)
        except Exception as ex:
            print(f"[WARN] Agent {fn.__name__} error: {ex}")

    inserted = insert_if_new(out_db, all_rows)
    print(f"[OK] Fetched: {len(all_rows)} | Inserted new: {inserted} | DB: {out_db}")

    # Write CSV snapshot
    csv_path = os.path.join(os.path.dirname(__file__), "..", cfg["outputs"]["csv_path"])
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    import csv
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source","title","link","published","summary","tags","fingerprint"])
        w.writeheader()
        for r in all_rows:
            r2 = dict(r)
            r2["tags"] = ",".join(r.get("tags", []))
            w.writerow(r2)
    print(f"[OK] Wrote CSV snapshot: {csv_path}")

if __name__ == "__main__":
    run_once()
