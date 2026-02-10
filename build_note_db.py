#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_note_db.py
- Creates/updates a local SQLite DB for note.com article sources (only /n/ articles).
- Given a list of URLs, fetches HTML, extracts best-effort meta, stores HTML + extracted text + text_hash.

Usage:
  python3 build_note_db.py --db note.db --urls-file urls.txt --limit 20000
  python3 build_note_db.py --db note.db --only-schema
"""

import argparse
import hashlib
import json
import re
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


SCHEMA_SQL = r"""
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS articles (
  url TEXT PRIMARY KEY,
  canonical_url TEXT,
  source TEXT NOT NULL DEFAULT 'note',
  title_raw TEXT,
  author_name_raw TEXT,
  published_at TEXT,
  fetched_at TEXT NOT NULL,
  lang TEXT,
  l1_decision TEXT,
  l1_reason TEXT,
  l2_score REAL,
  l2_label TEXT,
  l2_confidence REAL,
  l2_evidence_json TEXT,
  l2_model TEXT,
  l2_ran_at TEXT
);

CREATE TABLE IF NOT EXISTS contents (
  url TEXT PRIMARY KEY,
  html TEXT NOT NULL,
  text TEXT NOT NULL,
  text_hash TEXT NOT NULL,
  extract_ver TEXT NOT NULL DEFAULT 'v0',
  FOREIGN KEY(url) REFERENCES articles(url) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  status TEXT NOT NULL,
  count_total INTEGER NOT NULL DEFAULT 0,
  count_inserted INTEGER NOT NULL DEFAULT 0,
  count_updated INTEGER NOT NULL DEFAULT 0,
  count_unchanged INTEGER NOT NULL DEFAULT 0,
  count_failed INTEGER NOT NULL DEFAULT 0,
  meta_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_articles_fetched_at ON articles(fetched_at);
CREATE INDEX IF NOT EXISTS idx_articles_published_at ON articles(published_at);
CREATE INDEX IF NOT EXISTS idx_articles_l2_score ON articles(l2_score);
"""


NOTE_HOST_RE = re.compile(r"(^|\.)note\.com$", re.IGNORECASE)

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

def is_note_article(url: str) -> bool:
    try:
        p = urlparse(url)
    except Exception:
        return False
    if p.scheme not in ("http", "https"):
        return False
    if not NOTE_HOST_RE.search(p.hostname or ""):
        return False
    return "/n/" in (p.path or "")

def normalize_url(url: str) -> str:
    url = url.strip()
    p = urlparse(url)
    if not p.scheme:
        return url
    clean = p._replace(fragment="")
    return clean.geturl()

def fetch_html(url: str, timeout: float, user_agent: str) -> str:
    req = Request(url, headers={"User-Agent": user_agent, "Accept-Language": "ja,en;q=0.8"})
    with urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        try:
            return raw.decode("utf-8", errors="replace")
        except Exception:
            return raw.decode(errors="replace")

def extract_meta(html: str) -> dict:
    def meta_content(prop_or_name: str):
        pat = re.compile(
            rf'<meta[^>]+(?:property|name)\s*=\s*[\"\']{re.escape(prop_or_name)}[\"\'][^>]*content\s*=\s*[\"\']([^\"\']+)[\"\']',
            re.IGNORECASE
        )
        m = pat.search(html)
        return m.group(1).strip() if m else None

    title = meta_content("og:title") or meta_content("twitter:title")
    author = meta_content("author") or meta_content("og:site_name")

    canonical = None
    m = re.search(r'<link[^>]+rel\s*=\s*[\"\']canonical[\"\'][^>]+href\s*=\s*[\"\']([^\"\']+)[\"\']', html, re.IGNORECASE)
    if m:
        canonical = m.group(1).strip()

    published = meta_content("article:published_time") or meta_content("og:updated_time")

    lang = None
    m = re.search(r'<html[^>]+lang\s*=\s*[\"\']([^\"\']+)[\"\']', html, re.IGNORECASE)
    if m:
        lang = m.group(1).strip()

    return {
        "title_raw": title,
        "author_name_raw": author,
        "canonical_url": canonical,
        "published_at": published,
        "lang": lang,
    }

def html_to_text_simple(html: str) -> str:
    html = re.sub(r"(?is)<(script|style)\b.*?>.*?</\\1>", " ", html)
    html = re.sub(r"(?is)<!--.*?-->", " ", html)
    text = re.sub(r"(?is)<br\s*/?>", "\n", html)
    text = re.sub(r"(?is)</p\s*>", "\n", text)
    text = re.sub(r"(?is)<[^>]+>", " ", text)

    text = (text.replace("&nbsp;", " ")
                .replace("&lt;", "<")
                .replace("&gt;", ">")
                .replace("&amp;", "&")
                .replace("&quot;", '"')
                .replace("&#39;", "'"))

    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()

def sha256_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()

def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()

def upsert_article(conn: sqlite3.Connection, url: str, meta: dict, fetched_at: str) -> None:
    conn.execute(
        """
        INSERT INTO articles(url, canonical_url, source, title_raw, author_name_raw, published_at, fetched_at, lang)
        VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
          canonical_url=excluded.canonical_url,
          title_raw=COALESCE(excluded.title_raw, articles.title_raw),
          author_name_raw=COALESCE(excluded.author_name_raw, articles.author_name_raw),
          published_at=COALESCE(excluded.published_at, articles.published_at),
          fetched_at=excluded.fetched_at,
          lang=COALESCE(excluded.lang, articles.lang)
        """,
        (
            url,
            meta.get("canonical_url"),
            "note",
            meta.get("title_raw"),
            meta.get("author_name_raw"),
            meta.get("published_at"),
            fetched_at,
            meta.get("lang"),
        )
    )

def upsert_content(conn: sqlite3.Connection, url: str, html: str, text: str, text_hash: str, extract_ver: str="v0") -> str:
    cur = conn.execute("SELECT text_hash FROM contents WHERE url=?", (url,))
    row = cur.fetchone()
    if row and row[0] == text_hash:
        return "unchanged"

    conn.execute(
        """
        INSERT INTO contents(url, html, text, text_hash, extract_ver)
        VALUES(?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
          html=excluded.html,
          text=excluded.text,
          text_hash=excluded.text_hash,
          extract_ver=excluded.extract_ver
        """,
        (url, html, text, text_hash, extract_ver)
    )
    return "inserted" if row is None else "updated"

def new_run_id() -> str:
    return "run_" + now_iso().replace(":", "").replace("-", "")

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default="note.db")
    ap.add_argument("--urls-file", required=False, help="Text file with one URL per line.")
    ap.add_argument("--limit", type=int, default=20000)
    ap.add_argument("--sleep", type=float, default=0.2, help="Seconds between requests.")
    ap.add_argument("--timeout", type=float, default=20.0)
    ap.add_argument("--user-agent", default="AIsmellCollector/0.1 (+https://example.invalid)")
    ap.add_argument("--only-schema", action="store_true")
    args = ap.parse_args()

    db_path = Path(args.db)
    conn = sqlite3.connect(str(db_path))
    ensure_schema(conn)

    if args.only_schema:
        print(f"OK: schema ensured at {db_path}")
        return 0

    if not args.urls_file:
        print("ERROR: --urls-file is required unless --only-schema is used.", file=sys.stderr)
        return 2

    urls_path = Path(args.urls_file)
    if not urls_path.exists():
        print(f"ERROR: urls file not found: {urls_path}", file=sys.stderr)
        return 2

    urls = []
    for line in urls_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        u = normalize_url(line)
        if is_note_article(u):
            urls.append(u)

    urls = urls[: args.limit]

    run_id = new_run_id()
    started_at = now_iso()
    conn.execute(
        "INSERT INTO runs(run_id, started_at, status, meta_json) VALUES(?,?,?,?)",
        (run_id, started_at, "running", json.dumps({"limit": args.limit, "urls_file": str(urls_path)}, ensure_ascii=False))
    )
    conn.commit()

    stats = {"total": 0, "inserted": 0, "updated": 0, "unchanged": 0, "failed": 0}

    for i, url in enumerate(urls, start=1):
        stats["total"] += 1
        fetched_at = now_iso()
        try:
            html = fetch_html(url, timeout=args.timeout, user_agent=args.user_agent)
            meta = extract_meta(html)
            text = html_to_text_simple(html)
            th = sha256_text(text)

            upsert_article(conn, url, meta, fetched_at)
            status = upsert_content(conn, url, html, text, th, extract_ver="v0")
            stats[status] += 1
            conn.commit()
        except (HTTPError, URLError, TimeoutError) as e:
            stats["failed"] += 1
            conn.rollback()
            print(f"WARN: fetch failed ({i}/{len(urls)}): {url} :: {e}", file=sys.stderr)
        except Exception as e:
            stats["failed"] += 1
            conn.rollback()
            print(f"WARN: error ({i}/{len(urls)}): {url} :: {e}", file=sys.stderr)

        if args.sleep > 0:
            time.sleep(args.sleep)

    ended_at = now_iso()
    conn.execute(
        """
        UPDATE runs
        SET ended_at=?, status=?, count_total=?, count_inserted=?, count_updated=?, count_unchanged=?, count_failed=?
        WHERE run_id=?
        """,
        (ended_at, "done", stats["total"], stats["inserted"], stats["updated"], stats["unchanged"], stats["failed"], run_id)
    )
    conn.commit()
    conn.close()

    print("OK:", json.dumps({"run_id": run_id, **stats}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
