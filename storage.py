
import sqlite3
from contextlib import contextmanager
import os

SCHEMA = """
CREATE TABLE IF NOT EXISTS findings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    title TEXT,
    link TEXT,
    published TEXT,
    summary TEXT,
    tags TEXT,
    fingerprint TEXT UNIQUE
);
"""

@contextmanager
def db(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        yield conn
    finally:
        conn.commit()
        conn.close()

def init_db(path: str):
    with db(path) as conn:
        conn.execute(SCHEMA)

def insert_if_new(path: str, rows):
    if not rows:
        return 0
    init_db(path)
    inserted = 0
    with db(path) as conn:
        for r in rows:
            try:
                conn.execute(
                    "INSERT INTO findings (source,title,link,published,summary,tags,fingerprint) VALUES (?,?,?,?,?,?,?)",
                    (r.get("source"), r.get("title"), r.get("link"), r.get("published"), r.get("summary"),
                     ",".join(r.get("tags", [])), r.get("fingerprint"))
                )
                inserted += 1
            except sqlite3.IntegrityError:
                pass
    return inserted
