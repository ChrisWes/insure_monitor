"""
intelligence_monitor.py

Unified daily intelligence monitor. For each target firm, checks:
  1. Officer changes (Companies House API)
  2. Relevant job postings (Adzuna API)

Writes two dated digest CSVs and sends one consolidated email structured
per company, with a signal strength indicator for prioritisation.
"""

import csv
import logging
import os
import re
import smtplib
import sqlite3
import sys
import time
from datetime import date, datetime, timedelta
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import anthropic
import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

from director_intelligence import (
    build_director_intelligence,
    extract_officer_id,
    flatten_for_csv,
    get_director_profile,
    init_director_db,
    is_profile_stale,
    load_clients,
    store_director_profile,
)

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
FIRMS_NAME_COLUMN   = "Company Name"
FIRMS_NUMBER_COLUMN = "Company Number"
FIRMS_AGENT_COLUMN  = "lloyds_managing_agent"
FIRMS_STATUS_COLUMN = "monitoring_status"

# Companies House
CH_API_BASE        = "https://api.company-information.service.gov.uk"
CH_WINDOW_SECONDS  = 300
CH_WINDOW_REQUESTS = 500
CH_MAX_RETRIES     = 3

SENIOR_ROLES = {
    "director", "secretary", "managing director", "chief executive",
    "chairman", "chief financial officer", "chief operating officer",
    "chief technology officer",
}

# Adzuna
ADZUNA_BASE_URL         = "https://api.adzuna.com/v1/api/jobs/gb/search"
ADZUNA_RESULTS_PER_PAGE = 50
ADZUNA_MAX_PER_MINUTE   = 100
ADZUNA_MAX_PAGES        = 10
COMPANY_MATCH_THRESHOLD = 80
SHORT_LIVED_DAYS        = 30
MULTI_POSTING_THRESHOLD = 3

ROLE_KEYWORDS = [
    "transformation", "programme manager", "project manager",
    "business analyst", "PMO", "change manager", "data manager",
    "digital", "technology lead", "IT director",
    "chief information", "chief technology", "chief data",
]

# NewsAPI
NEWS_API_BASE       = "https://newsapi.org/v2/everything"
NEWS_DAILY_BUDGET   = 95        # hard cap; leaves 5 of 100 free-tier calls as headroom
NEWS_PAGE_SIZE      = 100       # max results per page
NEWS_LOOKBACK_DAYS  = 30
NEWS_SLEEP_SECONDS  = 1.0       # polite delay between calls

NEWS_KEYWORDS = [
    "technology", "digital", "transformation", "system", "platform",
    "software", "data", "cyber", "acquisition", "merger", "partnership",
    "investment", "regulatory", "compliance", "appointed", "restructure",
]

# Signal scoring thresholds
SIGNAL_HIGH   = 5
SIGNAL_MEDIUM = 2

LLM_MODEL = "claude-haiku-4-5-20251001"

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
LOGS_DIR   = SCRIPT_DIR / "logs"

FIRMS_CSV           = INPUT_DIR / "firms.csv"
CLIENTS_CSV         = INPUT_DIR / "clients.csv"
OFFICER_DB_PATH     = DATA_DIR / "officer_baseline.db"
JOBS_DB_PATH        = DATA_DIR / "jobs_baseline.db"
DIRECTOR_DB_PATH    = DATA_DIR / "director_intelligence.db"

TODAY             = date.today().isoformat()
OFFICER_DIGEST    = OUTPUT_DIR / f"officer_changes_{TODAY}.csv"
JOBS_DIGEST       = OUTPUT_DIR / f"job_changes_{TODAY}.csv"
NEWS_DIGEST       = OUTPUT_DIR / f"news_changes_{TODAY}.csv"
LOG_FILE          = LOGS_DIR / f"intelligence_monitor_{TODAY}.log"

NEWS_DB_PATH      = DATA_DIR / "news_baseline.db"

OFFICER_COLUMNS = [
    "change_type", "company_number", "company_name",
    "officer_name", "officer_role", "appointed_on", "resigned_on", "date_detected",
    "digital_background", "digital_roles", "client_connections",
    "watchlist_connections", "concurrent_watchlist",
    "llm_commentary",
]
JOBS_COLUMNS = [
    "change_type", "company_number", "company_name",
    "job_title", "job_location", "salary_min", "salary_max",
    "posted_date", "date_detected", "lloyds_managing_agent", "monitoring_status",
]
NEWS_COLUMNS = [
    "change_type", "company_number", "company_name",
    "article_title", "article_source", "article_published",
    "article_url", "date_detected",
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("intelligence_monitor")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch_handler = logging.StreamHandler(sys.stdout)
    ch_handler.setLevel(logging.INFO)
    ch_handler.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch_handler)
    return logger


# ---------------------------------------------------------------------------
# OFFICER DATABASE
# ---------------------------------------------------------------------------
def init_officer_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS firms (
            company_number TEXT PRIMARY KEY,
            company_name   TEXT,
            last_checked   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS officers (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT    NOT NULL,
            officer_name   TEXT    NOT NULL,
            officer_role   TEXT    NOT NULL,
            appointed_on   DATE,
            resigned_on    DATE,
            is_active      INTEGER NOT NULL DEFAULT 1,
            last_seen      TIMESTAMP,
            UNIQUE(company_number, officer_name, officer_role)
        );
        CREATE TABLE IF NOT EXISTS officer_enrichment (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            officer_name   TEXT NOT NULL,
            company_number TEXT NOT NULL,
            career_summary TEXT,
            fetched_at     TIMESTAMP,
            UNIQUE(officer_name, company_number)
        );
    """)
    conn.commit()


def officer_is_first_run(conn: sqlite3.Connection, company_number: str) -> bool:
    return conn.execute(
        "SELECT last_checked FROM firms WHERE company_number = ?", (company_number,)
    ).fetchone() is None


def get_baseline_officers(
    conn: sqlite3.Connection, company_number: str
) -> Dict[Tuple[str, str], Dict]:
    rows = conn.execute(
        "SELECT officer_name, officer_role, appointed_on FROM officers "
        "WHERE company_number = ? AND is_active = 1",
        (company_number,),
    ).fetchall()
    return {(r["officer_name"].lower(), r["officer_role"].lower()): dict(r) for r in rows}


def upsert_officer_firm(conn, company_number, name, ts):
    conn.execute(
        """INSERT INTO firms (company_number, company_name, last_checked) VALUES (?, ?, ?)
           ON CONFLICT(company_number) DO UPDATE SET
               company_name = excluded.company_name, last_checked = excluded.last_checked""",
        (company_number, name, ts),
    )


def upsert_officers(conn, company_number, officers, ts):
    for o in officers:
        conn.execute(
            """INSERT INTO officers
               (company_number, officer_name, officer_role, appointed_on, resigned_on, is_active, last_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(company_number, officer_name, officer_role) DO UPDATE SET
                   appointed_on = excluded.appointed_on, resigned_on = excluded.resigned_on,
                   is_active = excluded.is_active, last_seen = excluded.last_seen""",
            (
                company_number, o["officer_name"], o["officer_role"],
                o.get("appointed_on") or None, o.get("resigned_on") or None,
                0 if o.get("resigned_on") else 1, ts,
            ),
        )


def get_active_director_count(conn: sqlite3.Connection, company_number: str) -> int:
    row = conn.execute(
        "SELECT COUNT(*) FROM officers WHERE company_number = ? "
        "AND is_active = 1 AND LOWER(officer_role) LIKE '%director%'",
        (company_number,),
    ).fetchone()
    return row[0] if row else 0


def get_officer_enrichment(conn: sqlite3.Connection, officer_name: str, company_number: str) -> Optional[str]:
    row = conn.execute(
        "SELECT career_summary FROM officer_enrichment WHERE officer_name=? AND company_number=?",
        (officer_name, company_number),
    ).fetchone()
    return row["career_summary"] if row else None


def upsert_officer_enrichment(conn, officer_name, company_number, career_summary, ts):
    conn.execute(
        """INSERT INTO officer_enrichment (officer_name, company_number, career_summary, fetched_at)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(officer_name, company_number) DO UPDATE SET
               career_summary=excluded.career_summary, fetched_at=excluded.fetched_at""",
        (officer_name, company_number, career_summary, ts),
    )


# ---------------------------------------------------------------------------
# JOBS DATABASE
# ---------------------------------------------------------------------------
def init_jobs_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS processed_firms (
            company_number TEXT PRIMARY KEY,
            company_name   TEXT,
            last_checked   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS job_postings (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number TEXT NOT NULL,
            company_name   TEXT,
            job_id         TEXT NOT NULL,
            job_title      TEXT,
            job_location   TEXT,
            salary_min     REAL,
            salary_max     REAL,
            posted_date    DATE,
            first_seen     TIMESTAMP,
            last_seen      TIMESTAMP,
            is_active      INTEGER NOT NULL DEFAULT 1,
            UNIQUE(company_number, job_id)
        );
    """)
    conn.commit()


def jobs_is_first_run(conn: sqlite3.Connection, company_number: str) -> bool:
    return conn.execute(
        "SELECT last_checked FROM processed_firms WHERE company_number = ?", (company_number,)
    ).fetchone() is None


def get_active_postings(conn, company_number) -> Dict[str, Dict]:
    rows = conn.execute(
        "SELECT job_id, job_title, job_location, salary_min, salary_max, posted_date, first_seen "
        "FROM job_postings WHERE company_number = ? AND is_active = 1",
        (company_number,),
    ).fetchall()
    return {r["job_id"]: dict(r) for r in rows}


def upsert_posting(conn, company_number, company_name, job, ts):
    conn.execute(
        """INSERT INTO job_postings
           (company_number, company_name, job_id, job_title, job_location,
            salary_min, salary_max, posted_date, first_seen, last_seen, is_active)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
           ON CONFLICT(company_number, job_id) DO UPDATE SET
               last_seen = excluded.last_seen, is_active = 1""",
        (
            company_number, company_name, job["job_id"], job["job_title"],
            job["job_location"], job.get("salary_min"), job.get("salary_max"),
            job.get("posted_date"), ts, ts,
        ),
    )


def mark_posting_disappeared(conn, company_number, job_id):
    conn.execute(
        "UPDATE job_postings SET is_active = 0 WHERE company_number = ? AND job_id = ?",
        (company_number, job_id),
    )


def upsert_jobs_firm(conn, company_number, name, ts):
    conn.execute(
        """INSERT INTO processed_firms (company_number, company_name, last_checked)
           VALUES (?, ?, ?)
           ON CONFLICT(company_number) DO UPDATE SET
               company_name = excluded.company_name, last_checked = excluded.last_checked""",
        (company_number, name, ts),
    )


# ---------------------------------------------------------------------------
# NEWS DATABASE
# ---------------------------------------------------------------------------
def init_news_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS news_processed_firms (
            company_number TEXT PRIMARY KEY,
            company_name   TEXT,
            last_checked   TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS news_articles (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            company_number    TEXT NOT NULL,
            company_name      TEXT,
            article_url       TEXT NOT NULL UNIQUE,
            article_title     TEXT,
            article_source    TEXT,
            article_published DATE,
            first_seen        TIMESTAMP,
            is_active         INTEGER NOT NULL DEFAULT 1
        );
    """)
    conn.commit()


def news_is_first_run(conn: sqlite3.Connection, company_number: str) -> bool:
    return conn.execute(
        "SELECT last_checked FROM news_processed_firms WHERE company_number = ?",
        (company_number,),
    ).fetchone() is None


def get_baseline_articles(conn: sqlite3.Connection, company_number: str) -> Dict[str, Dict]:
    rows = conn.execute(
        "SELECT article_url, article_title, article_source, article_published, first_seen "
        "FROM news_articles WHERE company_number = ? AND is_active = 1",
        (company_number,),
    ).fetchall()
    return {r["article_url"]: dict(r) for r in rows}


def upsert_article(conn, company_number, company_name, article, ts):
    conn.execute(
        """INSERT INTO news_articles
           (company_number, company_name, article_url, article_title, article_source,
            article_published, first_seen, is_active)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1)
           ON CONFLICT(article_url) DO UPDATE SET is_active = 1""",
        (
            company_number, company_name, article["article_url"], article["article_title"],
            article["article_source"], article.get("article_published"), ts,
        ),
    )


def upsert_news_firm(conn, company_number, name, ts):
    conn.execute(
        """INSERT INTO news_processed_firms (company_number, company_name, last_checked)
           VALUES (?, ?, ?)
           ON CONFLICT(company_number) DO UPDATE SET
               company_name = excluded.company_name, last_checked = excluded.last_checked""",
        (company_number, name, ts),
    )


# ---------------------------------------------------------------------------
# RATE LIMITERS
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, window_seconds: int, window_requests: int) -> None:
        self._min_interval = window_seconds / window_requests
        self._last_call: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        gap = now - self._last_call
        if gap < self._min_interval:
            time.sleep(self._min_interval - gap)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# COMPANIES HOUSE API
# ---------------------------------------------------------------------------
def build_ch_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.auth = (api_key, "")
    s.headers.update({"Accept": "application/json"})
    return s


def _ch_get(session, url, params, limiter, logger) -> Optional[requests.Response]:
    for attempt in range(1, CH_MAX_RETRIES + 1):
        limiter.wait()
        try:
            resp = session.get(url, params=params or {}, timeout=(5, 20))
        except requests.RequestException as exc:
            logger.error("  CH network error (attempt %d/%d): %s", attempt, CH_MAX_RETRIES, exc)
            if attempt < CH_MAX_RETRIES:
                time.sleep(2 ** attempt)
                continue
            return None
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", CH_WINDOW_SECONDS))
            logger.warning("  CH 429 — sleeping %ds", wait)
            time.sleep(wait)
            continue
        return resp
    return None


def fetch_ch_officers(session, company_number, limiter, logger) -> Optional[List[Dict]]:
    url = f"{CH_API_BASE}/company/{company_number}/officers"
    all_officers, start = [], 0
    while True:
        resp = _ch_get(session, url, {"items_per_page": 100, "start_index": start}, limiter, logger)
        if resp is None:
            return None
        if resp.status_code == 404:
            logger.warning("  CH: %s not found (404)", company_number)
            return None
        if resp.status_code != 200:
            logger.error("  CH officers HTTP %d for %s", resp.status_code, company_number)
            return None
        data = resp.json()
        items = data.get("items", [])
        for item in items:
            all_officers.append({
                "officer_name":     str(item.get("name", "")).strip(),
                "officer_role":     str(item.get("officer_role", "")).strip(),
                "appointed_on":     item.get("appointed_on", "") or "",
                "resigned_on":      item.get("resigned_on", "") or "",
                "appointments_url": (item.get("links") or {}).get("officer", {}).get("appointments", ""),
            })
        total = data.get("total_results", len(items))
        start += 100
        if start >= total:
            break
    return all_officers


def fetch_officer_appointments(session, appointments_url, limiter, logger) -> List[Dict]:
    """Fetch all appointments for one officer via their CH appointments URL."""
    if not appointments_url:
        return []
    url = f"{CH_API_BASE}{appointments_url}"
    resp = _ch_get(session, url, {"items_per_page": 50}, limiter, logger)
    if resp is None or resp.status_code != 200:
        return []
    items = resp.json().get("items", [])
    return [
        {
            "company_name":   (item.get("appointed_to") or {}).get("company_name", ""),
            "company_number": (item.get("appointed_to") or {}).get("company_number", ""),
            "role":           item.get("officer_role", ""),
            "appointed_on":   item.get("appointed_on", ""),
            "resigned_on":    item.get("resigned_on", ""),
        }
        for item in items
        if (item.get("appointed_to") or {}).get("company_name")
    ]


def build_career_summary(appointments: List[Dict], exclude_company_number: str) -> str:
    prior = [a for a in appointments if a["company_number"] != exclude_company_number]
    if not prior:
        return "No other UK registered directorships found."
    parts = []
    for a in prior[:5]:
        tenure = a["appointed_on"][:4] if a["appointed_on"] else "?"
        end    = a["resigned_on"][:4]  if a["resigned_on"]  else "present"
        parts.append(f"{a['role'].title()} at {a['company_name']} ({tenure}–{end})")
    return "Previously: " + "; ".join(parts)


# ---------------------------------------------------------------------------
# ADZUNA API
# ---------------------------------------------------------------------------
_STRIP_FOR_ADZUNA = re.compile(
    r"\b(limited|ltd|llp|plc|lp|inc|the|insurance|reinsurance|underwriters|underwriting|"
    r"syndicate|syndicates|managing|agency|agent|group|holdings|uk|services|"
    r"financial|life|general|mutual|assurance|society|association|of|and)\b",
    re.IGNORECASE,
)
# For company-match validation: strip only legal suffixes, keep industry words so
# "HIVE UNDERWRITERS" stays distinct from "HIVE RECRUITMENT".
_STRIP_LEGAL_ONLY = re.compile(
    r"\b(limited|ltd|llp|plc|lp|inc)\b",
    re.IGNORECASE,
)
_WS = re.compile(r"\s+")


def make_search_name(registered_name: str) -> str:
    """Short trading-style name for the Adzuna 'what=' query parameter."""
    cleaned = _WS.sub(" ", _STRIP_FOR_ADZUNA.sub(" ", registered_name)).strip()
    words = [w for w in cleaned.split() if len(w) > 1]
    return " ".join(words[:2]) if words else registered_name


def make_validation_name(registered_name: str) -> str:
    """Fuller name (legal suffixes stripped, industry words kept) for company match validation."""
    cleaned = _WS.sub(" ", _STRIP_LEGAL_ONLY.sub(" ", registered_name)).strip()
    return cleaned.lower()


def build_adzuna_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"Accept": "application/json"})
    return s


def matches_keywords(title: str, description: str) -> bool:
    text = f"{title} {description}".lower()
    return any(kw.lower() in text for kw in ROLE_KEYWORDS)


def fetch_adzuna_jobs(session, app_id, app_key, firm_name, limiter, logger) -> Optional[List[Dict]]:
    search_name     = make_search_name(firm_name)
    validation_name = make_validation_name(firm_name)  # fuller name used for company match
    relevant, page = [], 1
    retry_counts: Dict[Tuple, int] = {}

    while True:
        url = f"{ADZUNA_BASE_URL}/{page}"
        limiter.wait()
        try:
            resp = session.get(
                url,
                params={"app_id": app_id, "app_key": app_key,
                        "what": search_name, "results_per_page": ADZUNA_RESULTS_PER_PAGE},
                timeout=(5, 20),
            )
        except requests.RequestException as exc:
            logger.error("  Adzuna request failed (page %d): %s", page, exc)
            return None

        if resp.status_code == 400:
            if page == 1:
                logger.error("  Adzuna 400 for '%s'", search_name)
                return None
            break

        if resp.status_code in (500, 502, 503, 504):
            key = (firm_name, page)
            retries = retry_counts.get(key, 0)
            if retries < 3:
                wait = 10 * (retries + 1)
                logger.warning("  Adzuna %d (attempt %d/3) — retrying in %ds",
                               resp.status_code, retries + 1, wait)
                retry_counts[key] = retries + 1
                time.sleep(wait)
                continue
            logger.error("  Adzuna %d for %s — giving up", resp.status_code, firm_name)
            return None

        if resp.status_code != 200:
            logger.error("  Adzuna HTTP %d for %s", resp.status_code, firm_name)
            return None

        data = resp.json()
        for job in data.get("results", []):
            company_display = job.get("company", {}).get("display_name", "")
            # token_sort_ratio: handles word-order differences without treating subsets
            # as perfect matches (unlike token_set_ratio). Compared against the fuller
            # validation_name so "hive underwriters" stays distinct from "hive recruitment".
            if fuzz.token_sort_ratio(validation_name, company_display.lower()) < COMPANY_MATCH_THRESHOLD:
                continue
            title = job.get("title", "")
            if not matches_keywords(title, job.get("description", "")):
                continue
            created = job.get("created", "")
            relevant.append({
                "job_id":       str(job.get("id", "")),
                "job_title":    title,
                "job_location": job.get("location", {}).get("display_name", ""),
                "salary_min":   job.get("salary_min"),
                "salary_max":   job.get("salary_max"),
                "posted_date":  created[:10] if created else "",
            })

        total = data.get("count", 0)
        fetched = (page - 1) * ADZUNA_RESULTS_PER_PAGE + len(data.get("results", []))
        if not data.get("results") or fetched >= total or page >= ADZUNA_MAX_PAGES:
            break
        page += 1

    return relevant


# ---------------------------------------------------------------------------
# NEWSAPI
# ---------------------------------------------------------------------------
def article_matches_keywords(title: str, description: str) -> bool:
    text = f"{title} {description}".lower()
    return any(kw.lower() in text for kw in NEWS_KEYWORDS)


def fetch_newsapi(
    session: requests.Session, api_key: str, firm_name: str, logger: logging.Logger
) -> Tuple[Optional[List[Dict]], int]:
    """Returns (relevant_articles, pages_fetched). On unrecoverable error returns (None, pages_fetched)."""
    from_date = (date.today() - timedelta(days=NEWS_LOOKBACK_DAYS)).isoformat()
    query = f'"{firm_name}"'
    relevant: List[Dict] = []
    page = 1
    pages_fetched = 0

    while True:
        time.sleep(NEWS_SLEEP_SECONDS)
        try:
            resp = session.get(
                NEWS_API_BASE,
                params={
                    "apiKey":       api_key,
                    "q":            query,
                    "language":     "en",
                    "from":         from_date,
                    "sortBy":       "publishedAt",
                    "pageSize":     NEWS_PAGE_SIZE,
                    "page":         page,
                },
                timeout=(5, 20),
            )
        except requests.RequestException as exc:
            logger.error("  NewsAPI request failed (page %d): %s", page, exc)
            return None, pages_fetched

        pages_fetched += 1

        if resp.status_code == 426:
            logger.error("  NewsAPI: free-tier upgrade required (426)")
            return None, pages_fetched
        if resp.status_code == 429:
            logger.warning("  NewsAPI 429 — sleeping 60s")
            time.sleep(60)
            pages_fetched -= 1  # don't count the failed attempt
            continue
        if resp.status_code != 200:
            logger.error("  NewsAPI HTTP %d for '%s'", resp.status_code, firm_name)
            return None, pages_fetched

        data = resp.json()
        if data.get("status") != "ok":
            logger.error("  NewsAPI error: %s", data.get("message", "unknown"))
            return None, pages_fetched

        for art in data.get("articles", []):
            title = art.get("title") or ""
            desc  = art.get("description") or ""
            if not article_matches_keywords(title, desc):
                continue
            pub = (art.get("publishedAt") or "")[:10]
            url = art.get("url", "")
            if not url:
                continue
            relevant.append({
                "article_url":       url,
                "article_title":     title,
                "article_source":    (art.get("source") or {}).get("name", ""),
                "article_published": pub,
            })

        total_results  = data.get("totalResults", 0)
        fetched_so_far = page * NEWS_PAGE_SIZE
        if not data.get("articles") or fetched_so_far >= total_results:
            break
        page += 1

    return relevant, pages_fetched


# ---------------------------------------------------------------------------
# CHANGE DETECTION
# ---------------------------------------------------------------------------
def is_senior(role: str) -> bool:
    return any(sr in role.lower() for sr in SENIOR_ROLES)


def detect_officer_changes(
    company_number, company_name, baseline, api_officers, logger
) -> List[Dict]:
    changes = []
    api_lookup = {
        (o["officer_name"].lower(), o["officer_role"].lower()): o for o in api_officers
    }
    for key, o in api_lookup.items():
        if o["resigned_on"] or key in baseline:
            continue
        senior = is_senior(o["officer_role"])
        logger.log(
            logging.WARNING if senior else logging.INFO,
            "  OFFICER NEW: %s | %s%s",
            o["officer_name"], o["officer_role"], "  [SENIOR]" if senior else "",
        )
        changes.append({
            "change_type": "New Appointment", "company_number": company_number,
            "company_name": company_name, "officer_name": o["officer_name"],
            "officer_role": o["officer_role"], "appointed_on": o["appointed_on"],
            "resigned_on": "", "date_detected": TODAY,
        })
    for key, b in baseline.items():
        api_o = api_lookup.get(key)
        if not api_o or not api_o["resigned_on"]:
            continue
        senior = is_senior(b["officer_role"])
        logger.log(
            logging.WARNING if senior else logging.INFO,
            "  OFFICER RESIGNED: %s | %s%s",
            b["officer_name"], b["officer_role"], "  [SENIOR]" if senior else "",
        )
        changes.append({
            "change_type": "Resignation", "company_number": company_number,
            "company_name": company_name, "officer_name": b["officer_name"],
            "officer_role": b["officer_role"], "appointed_on": b.get("appointed_on", ""),
            "resigned_on": api_o["resigned_on"], "date_detected": TODAY,
        })
    return changes


def detect_job_changes(
    company_number, company_name, baseline, current_jobs, firm_meta, logger
) -> List[Dict]:
    changes = []
    current_ids = {job["job_id"] for job in current_jobs}
    agent  = firm_meta.get(FIRMS_AGENT_COLUMN, "")
    status = firm_meta.get(FIRMS_STATUS_COLUMN, "")

    new_count = 0
    for job in current_jobs:
        if job["job_id"] not in baseline:
            new_count += 1
            logger.info("  JOB NEW: %s | %s", job["job_title"], job["job_location"])
            changes.append({
                "change_type": "New Posting", "company_number": company_number,
                "company_name": company_name, "job_title": job["job_title"],
                "job_location": job["job_location"],
                "salary_min": job.get("salary_min") or "",
                "salary_max": job.get("salary_max") or "",
                "posted_date": job["posted_date"], "date_detected": TODAY,
                "lloyds_managing_agent": agent, "monitoring_status": status,
            })

    if new_count >= MULTI_POSTING_THRESHOLD:
        logger.warning("  ** %d new relevant postings — strong signal **", new_count)

    for job_id, b in baseline.items():
        if job_id in current_ids:
            continue
        days_active = (datetime.utcnow() - datetime.fromisoformat(b["first_seen"])).days
        change_type = "Disappeared - Short-lived" if days_active < SHORT_LIVED_DAYS else "Disappeared"
        logger.info("  JOB %s: %s (%d days)", change_type.upper(), b["job_title"], days_active)
        changes.append({
            "change_type": change_type, "company_number": company_number,
            "company_name": company_name, "job_title": b["job_title"],
            "job_location": b.get("job_location", ""),
            "salary_min": b.get("salary_min") or "", "salary_max": b.get("salary_max") or "",
            "posted_date": b.get("posted_date", ""), "date_detected": TODAY,
            "lloyds_managing_agent": agent, "monitoring_status": status,
        })

    return changes


def detect_news_changes(
    company_number: str, company_name: str,
    baseline_urls: Dict[str, Dict], current_articles: List[Dict],
    first_run: bool, logger: logging.Logger,
) -> List[Dict]:
    if first_run:
        return []
    changes = []
    for art in current_articles:
        if art["article_url"] in baseline_urls:
            continue
        logger.info("  NEWS NEW: %s | %s", art["article_source"], art["article_title"][:60])
        changes.append({
            "change_type":       "New Article",
            "company_number":    company_number,
            "company_name":      company_name,
            "article_title":     art["article_title"],
            "article_source":    art["article_source"],
            "article_published": art["article_published"],
            "article_url":       art["article_url"],
            "date_detected":     TODAY,
        })
    return changes


def signal_score(officer_changes: List[Dict], job_changes: List[Dict], news_changes: Optional[List[Dict]] = None) -> Tuple[str, int]:
    score = 0
    for c in officer_changes:
        if c["change_type"] in ("New Appointment", "Resignation"):
            score += 3 if is_senior(c["officer_role"]) else 1
    new_jobs = [j for j in job_changes if j["change_type"] == "New Posting"]
    score += len(new_jobs)
    if len(new_jobs) >= MULTI_POSTING_THRESHOLD:
        score += 2
    new_articles = [n for n in (news_changes or []) if n["change_type"] == "New Article"]
    score += min(2, len(new_articles))
    label = "HIGH" if score >= SIGNAL_HIGH else ("MEDIUM" if score >= SIGNAL_MEDIUM else "LOW")
    return label, score


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------
def write_digest(path: Path, columns: List[str], rows: List[Dict],
                 empty_label: str, logger: logging.Logger) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        if rows:
            writer.writerows(rows)
        else:
            writer.writerow({col: "" for col in columns} | {
                "change_type": empty_label, "date_detected": TODAY,
            })
    logger.info("Digest written -> %s", path.name)


# ---------------------------------------------------------------------------
# LLM COMMENTARY
# ---------------------------------------------------------------------------
def generate_llm_commentary(name: str, agent: str, activity: Dict, logger: logging.Logger) -> str:
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return ""

    lines = [
        f"Firm: {name}" + (f" [Lloyd's managing agent: {agent}]" if agent else ""),
        f"Active directors: {activity['director_count']}  |  "
        f"Relevant live job postings: {activity['active_job_count']}",
        "",
    ]

    o_changes = activity["officer_changes"]
    if o_changes:
        lines.append("OFFICER CHANGES:")
        for c in o_changes:
            senior = " [SENIOR]" if is_senior(c["officer_role"]) else ""
            lines.append(f"  {c['change_type']}: {c['officer_name']} as {c['officer_role']}{senior}")
            intel = c.get("director_intel")
            if intel:
                if intel.get("career_summary"):
                    lines.append(f"    Career: {intel['career_summary']}")
                if intel.get("digital_background"):
                    dig = "; ".join(intel["digital_roles"][:3])
                    lines.append(f"    Digital transformation background: {dig}")
                for cc in intel.get("client_connections", [])[:3]:
                    status = "current" if cc["is_current"] else f"until {cc['resigned_on'][:7]}" if cc["resigned_on"] else "former"
                    lines.append(f"    CLIENT CONNECTION: {cc['officer_role'].title()} at {cc['company_name']} ({status})")
                for cc in intel.get("concurrent_watchlist", [])[:3]:
                    lines.append(f"    CONCURRENT POSITION: Also active as {cc['officer_role'].title()} at {cc['company_name']}")
            elif c.get("career_summary"):
                lines.append(f"    Background: {c['career_summary']}")

    j_changes = [j for j in activity["job_changes"] if j["change_type"] == "New Posting"]
    if j_changes:
        lines.append("NEW JOB POSTINGS:")
        for j in j_changes:
            lines.append(f"  {j['job_title']} | {j['job_location']}")

    n_changes = [n for n in activity["news_changes"] if n["change_type"] == "New Article"]
    if n_changes:
        lines.append("RECENT NEWS:")
        for n in n_changes:
            lines.append(
                f"  \"{n['article_title']}\" ({n['article_source']}, {n['article_published']})"
            )

    system = (
        "You are an insurance market intelligence analyst helping a Lloyd's broker identify "
        "business development opportunities. Given signals about a UK insurance sector firm, "
        "provide a brief assessment in 2-3 sentences. Focus on what the signals suggest about "
        "the firm's direction and any BD opportunity. If a newly appointed director has a "
        "digital transformation background or connections to existing client firms, treat these "
        "as high-priority signals and explain specifically why. Be specific and concise."
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=LLM_MODEL,
            max_tokens=200,
            system=system,
            messages=[{"role": "user", "content": "\n".join(lines)}],
        )
        return msg.content[0].text.strip()
    except Exception as exc:
        logger.warning("LLM commentary failed for %s: %s", name, exc)
        return ""


# ---------------------------------------------------------------------------
# EMAIL
# ---------------------------------------------------------------------------
def send_email(
    officer_changes: List[Dict],
    job_changes: List[Dict],
    news_changes: List[Dict],
    company_activity: Dict[str, Dict],
    stats: Dict,
    logger: logging.Logger,
) -> None:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    if not smtp_host:
        logger.info("SMTP_HOST not set — skipping email")
        return

    smtp_port     = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    email_from    = os.getenv("EMAIL_FROM", smtp_username).strip()
    email_to_raw  = os.getenv("EMAIL_TO", "").strip()
    if not email_to_raw:
        logger.warning("EMAIL_TO not set — skipping email")
        return

    recipients = [a.strip() for a in email_to_raw.split(",") if a.strip()]
    total_changes = (
        stats["officer_new"] + stats["officer_resigned"]
        + stats["job_new"] + stats["job_disappeared"]
        + stats["news_new"]
    )

    if total_changes:
        subject = (
            f"Intelligence Monitor: {total_changes} signal(s) — {TODAY}  "
            f"({stats['officer_new']} appointments, {stats['officer_resigned']} resignations, "
            f"{stats['job_new']} new postings, {stats['news_new']} news articles)"
        )
    else:
        subject = f"Intelligence Monitor: No changes — {TODAY}"

    lines = [
        f"Intelligence Monitor — {TODAY}",
        "=" * 60,
        f"Firms checked        : {stats['firms_checked']}",
        f"Officer changes      : {stats['officer_new']} new appointments, "
        f"{stats['officer_resigned']} resignations",
        f"Job posting changes  : {stats['job_new']} new, {stats['job_disappeared']} disappeared",
        f"News articles        : {stats['news_new']} new",
        f"API errors (CH)      : {stats['ch_errors']}",
        f"API errors (Adzuna)  : {stats['adzuna_errors']}",
        "",
    ]

    if total_changes and company_activity:
        lines.append("COMPANIES WITH ACTIVITY")
        lines.append("=" * 60)

        # Sort by signal score descending
        sorted_companies = sorted(
            company_activity.items(),
            key=lambda x: x[1]["score"],
            reverse=True,
        )

        for ch_num, activity in sorted_companies:
            o_changes = activity["officer_changes"]
            j_changes = activity["job_changes"]
            signal, score = activity["signal"], activity["score"]
            director_count = activity["director_count"]
            name = activity["name"]
            agent = activity.get("agent", "")

            header = f"[ {signal} ]  {name}"
            if agent:
                header += f"  [Lloyd's: {agent}]"
            lines.append("")
            lines.append(header)
            lines.append("-" * 60)

            if o_changes:
                lines.append(f"  Officers  ({director_count} active director(s)):")
                for c in o_changes:
                    ct = c["change_type"].upper()
                    senior = "  [SENIOR]" if is_senior(c["officer_role"]) else ""
                    lines.append(f"    {ct:<20}  {c['officer_name']}  |  {c['officer_role']}{senior}")
                    intel = c.get("director_intel")
                    if intel:
                        if intel.get("career_summary"):
                            lines.append(f"                          {intel['career_summary']}")
                        if intel.get("digital_background"):
                            dig = "; ".join(intel["digital_roles"][:2])
                            lines.append(f"                          *** DIGITAL BACKGROUND: {dig}")
                        for cc in intel.get("client_connections", [])[:3]:
                            status = "current" if cc["is_current"] else f"until {cc['resigned_on'][:7]}" if cc["resigned_on"] else "former"
                            lines.append(f"                          *** CLIENT: {cc['officer_role'].title()} at {cc['company_name']} ({status})")
                        for cc in intel.get("concurrent_watchlist", [])[:3]:
                            lines.append(f"                          *** CONCURRENT: Also active at {cc['company_name']} ({cc['officer_role'].title()})")
                    elif c.get("career_summary"):
                        lines.append(f"                          {c['career_summary']}")

            active_jobs = activity.get("active_job_count", 0)
            new_jobs    = [j for j in j_changes if j["change_type"] == "New Posting"]
            gone_jobs   = [j for j in j_changes if j["change_type"] != "New Posting"]

            if j_changes:
                lines.append(f"  Jobs  ({active_jobs} relevant posting(s) currently live):")
                for j in new_jobs:
                    salary = ""
                    if j.get("salary_min") or j.get("salary_max"):
                        salary = f"  £{j.get('salary_min','')}–{j.get('salary_max','')}"
                    lines.append(f"    NEW  {j['job_title']}  |  {j['job_location']}{salary}")
                for j in gone_jobs:
                    lines.append(f"    {j['change_type'].upper()}  {j['job_title']}")

            new_articles = [
                n for n in activity.get("news_changes", [])
                if n["change_type"] == "New Article"
            ]
            if new_articles:
                lines.append(f"  News  ({len(new_articles)} new article(s)):")
                for n in new_articles:
                    lines.append(
                        f'    - "{n["article_title"]}"'
                        f'  --  {n["article_source"]}, {n["article_published"]}'
                    )

            commentary = activity.get("llm_commentary", "")
            if commentary:
                lines.append(f"  Assessment: {commentary}")

        lines.append("")

    lines.append("Full digests attached.")
    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["From"]    = email_from
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    for digest_path in (OFFICER_DIGEST, JOBS_DIGEST, NEWS_DIGEST):
        if digest_path.exists():
            with digest_path.open("rb") as fh:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(fh.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={digest_path.name}")
            msg.attach(part)

    try:
        if smtp_port == 465:
            with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=30) as server:
                if smtp_username:
                    server.login(smtp_username, smtp_password)
                server.sendmail(email_from, recipients, msg.as_string())
        else:
            with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                if smtp_username:
                    server.login(smtp_username, smtp_password)
                server.sendmail(email_from, recipients, msg.as_string())
        logger.info("Email sent to %s", ", ".join(recipients))
    except smtplib.SMTPException as exc:
        logger.error("Failed to send email: %s", exc)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Intelligence Monitor")
    logger.info("=" * 60)

    load_dotenv()
    ch_api_key = os.getenv("CH_API_KEY", "").strip()
    app_id     = os.getenv("ADZUNA_APP_ID", "").strip()
    app_key    = os.getenv("ADZUNA_APP_KEY", "").strip()

    if not ch_api_key:
        logger.error("CH_API_KEY not set. Exiting.")
        sys.exit(1)
    if not app_id or not app_key:
        logger.error("ADZUNA_APP_ID / ADZUNA_APP_KEY not set. Exiting.")
        sys.exit(1)

    if not FIRMS_CSV.exists():
        logger.error("Firms CSV not found: %s", FIRMS_CSV)
        sys.exit(1)

    with FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        firms = list(csv.DictReader(fh))

    logger.info("Loaded %d firms from %s", len(firms), FIRMS_CSV.name)

    # Build watchlist lookup sets from firms.csv
    watchlist_numbers: Set[str] = set()
    watchlist_names:   Dict[str, str] = {}
    for row in firms:
        num = row.get(FIRMS_NUMBER_COLUMN, "").strip()
        if num:
            num = num.zfill(8) if num.isdigit() else num
            watchlist_numbers.add(num)
            watchlist_names[num] = row.get(FIRMS_NAME_COLUMN, "")

    # Load client list (exported from HubSpot into input/clients.csv)
    client_names    = load_clients(CLIENTS_CSV)
    client_numbers  = set(client_names.keys())
    if client_names:
        logger.info("Loaded %d clients from %s", len(client_names), CLIENTS_CSV.name)
    else:
        logger.info("No clients.csv found — client connection matching disabled")

    DATA_DIR.mkdir(exist_ok=True)
    officer_conn = sqlite3.connect(OFFICER_DB_PATH)
    officer_conn.row_factory = sqlite3.Row
    init_officer_db(officer_conn)

    jobs_conn = sqlite3.connect(JOBS_DB_PATH)
    jobs_conn.row_factory = sqlite3.Row
    init_jobs_db(jobs_conn)

    director_conn = sqlite3.connect(DIRECTOR_DB_PATH)
    director_conn.row_factory = sqlite3.Row
    init_director_db(director_conn)

    ch_session     = build_ch_session(ch_api_key)
    adzuna_session = build_adzuna_session()
    ch_limiter     = RateLimiter(CH_WINDOW_SECONDS, CH_WINDOW_REQUESTS)
    adzuna_limiter = RateLimiter(60, ADZUNA_MAX_PER_MINUTE)

    all_officer_changes: List[Dict] = []
    all_job_changes:     List[Dict] = []
    company_activity:    Dict[str, Dict] = {}
    firm_buffer:         Dict[str, Dict] = {}

    total_firms      = len(firms)
    ch_errors        = 0
    adzuna_errors    = 0
    officer_new      = 0
    officer_resigned = 0
    job_new          = 0
    job_disappeared  = 0

    for idx, row in enumerate(firms, start=1):
        ch_num = row.get(FIRMS_NUMBER_COLUMN, "").strip()
        name   = (row.get(FIRMS_NAME_COLUMN, "") or "").strip() or ch_num
        if not ch_num:
            continue
        if ch_num.isdigit():
            ch_num = ch_num.zfill(8)

        logger.info("[%d/%d] %s  (%s)", idx, total_firms, name, ch_num)
        ts = datetime.utcnow().isoformat()

        # ---- Officers (Companies House) ----
        o_first = officer_is_first_run(officer_conn, ch_num)
        api_officers = fetch_ch_officers(ch_session, ch_num, ch_limiter, logger)

        if api_officers is None:
            ch_errors += 1
            o_changes = []
        elif o_first:
            logger.info("  Officers: first run (%d officers)", len(api_officers))
            upsert_officer_firm(officer_conn, ch_num, name, ts)
            upsert_officers(officer_conn, ch_num, api_officers, ts)
            officer_conn.commit()
            o_changes = []
        else:
            baseline = get_baseline_officers(officer_conn, ch_num)
            o_changes = detect_officer_changes(ch_num, name, baseline, api_officers, logger)
            upsert_officer_firm(officer_conn, ch_num, name, ts)
            upsert_officers(officer_conn, ch_num, api_officers, ts)
            officer_conn.commit()
            if not o_changes:
                logger.info("  Officers: no changes")
            # Enrich new appointments with director intelligence
            for change in o_changes:
                if change["change_type"] != "New Appointment":
                    continue
                appt_url = next(
                    (o["appointments_url"] for o in api_officers
                     if o["officer_name"] == change["officer_name"]),
                    "",
                )
                officer_id = extract_officer_id(appt_url)

                # Try director DB first (avoids repeat CH calls; shares data across firms)
                profile = get_director_profile(director_conn, officer_id) if officer_id else None

                if profile is None or is_profile_stale(profile):
                    appts = fetch_officer_appointments(ch_session, appt_url, ch_limiter, logger)
                    if officer_id and appts is not None:
                        profile = store_director_profile(
                            director_conn, officer_id, change["officer_name"],
                            appts, watchlist_numbers, client_numbers, ts,
                        )
                    elif appts:
                        # No officer_id — fall back to simple text summary cached in officer_enrichment
                        summary = build_career_summary(appts, ch_num)
                        change["career_summary"] = summary
                        upsert_officer_enrichment(
                            officer_conn, change["officer_name"], ch_num, summary, ts
                        )
                        officer_conn.commit()
                        logger.info("  Enrichment (no ID): %s — %s",
                                    change["officer_name"], summary[:80])
                        continue

                if profile:
                    intel = build_director_intelligence(
                        profile, ch_num, watchlist_names, client_names
                    )
                    change["director_intel"] = intel
                    change["career_summary"] = intel["career_summary"]

                    # Flatten to CSV-safe fields
                    change.update(flatten_for_csv(intel))

                    # Log notable findings
                    if intel["digital_background"]:
                        logger.info(
                            "  Director Intel [DIGITAL]: %s — %s",
                            change["officer_name"],
                            "; ".join(intel["digital_roles"][:2]),
                        )
                    if intel["client_connections"]:
                        logger.warning(
                            "  Director Intel [CLIENT CONNECTION]: %s previously at %s",
                            change["officer_name"],
                            ", ".join(c["company_name"] for c in intel["client_connections"]),
                        )
                    if intel["concurrent_watchlist"]:
                        logger.warning(
                            "  Director Intel [CONCURRENT]: %s also active at %s",
                            change["officer_name"],
                            ", ".join(c["company_name"] for c in intel["concurrent_watchlist"]),
                        )
                    logger.info("  Director Intel: %s — %s",
                                change["officer_name"], intel["career_summary"][:80])
                else:
                    # Absolute fallback: check legacy cache, otherwise skip
                    cached = get_officer_enrichment(officer_conn, change["officer_name"], ch_num)
                    if cached:
                        change["career_summary"] = cached

        # ---- Jobs (Adzuna) ----
        j_first = jobs_is_first_run(jobs_conn, ch_num)
        current_jobs = fetch_adzuna_jobs(adzuna_session, app_id, app_key, name, adzuna_limiter, logger)

        if current_jobs is None:
            adzuna_errors += 1
            j_changes = []
        elif j_first:
            logger.info("  Jobs: first run (%d relevant postings)", len(current_jobs))
            for job in current_jobs:
                upsert_posting(jobs_conn, ch_num, name, job, ts)
            upsert_jobs_firm(jobs_conn, ch_num, name, ts)
            jobs_conn.commit()
            j_changes = []
        else:
            j_baseline = get_active_postings(jobs_conn, ch_num)
            j_changes = detect_job_changes(ch_num, name, j_baseline, current_jobs, row, logger)
            current_ids = {j["job_id"] for j in current_jobs}
            for job in current_jobs:
                upsert_posting(jobs_conn, ch_num, name, job, ts)
            for job_id in j_baseline:
                if job_id not in current_ids:
                    mark_posting_disappeared(jobs_conn, ch_num, job_id)
            upsert_jobs_firm(jobs_conn, ch_num, name, ts)
            jobs_conn.commit()
            if not j_changes:
                logger.info("  Jobs: no changes (%d relevant live)", len(current_jobs))

        # ---- Aggregate ----
        all_officer_changes.extend(o_changes)
        all_job_changes.extend(j_changes)

        for c in o_changes:
            if c["change_type"] == "New Appointment":
                officer_new += 1
            else:
                officer_resigned += 1
        for c in j_changes:
            if c["change_type"] == "New Posting":
                job_new += 1
            else:
                job_disappeared += 1

        firm_buffer[ch_num] = {
            "row":             row,
            "name":            name,
            "o_changes":       o_changes,
            "j_changes":       j_changes,
            "director_count":  get_active_director_count(officer_conn, ch_num),
            "active_job_count": len(current_jobs) if current_jobs is not None else 0,
        }

    officer_conn.close()
    jobs_conn.close()
    director_conn.close()

    # -------------------------------------------------------------------------
    # PASS 2 — NEWS (NewsAPI, priority-sorted, daily budget capped)
    # -------------------------------------------------------------------------
    news_api_key    = os.getenv("NEWS_API_KEY", "").strip()
    all_news_changes: List[Dict] = []
    news_calls_used = 0
    news_new        = 0

    if not news_api_key:
        logger.warning("NEWS_API_KEY not set — skipping news monitoring")
    else:
        news_conn = sqlite3.connect(NEWS_DB_PATH)
        news_conn.row_factory = sqlite3.Row
        init_news_db(news_conn)
        news_session = requests.Session()
        news_session.headers.update({"Accept": "application/json"})

        def _news_priority(ch_num: str) -> int:
            buf = firm_buffer.get(ch_num, {})
            has_agent   = bool((buf.get("row") or {}).get(FIRMS_AGENT_COLUMN, ""))
            has_signals = bool(buf.get("o_changes") or buf.get("j_changes"))
            if has_agent and has_signals:
                return 0
            if has_agent:
                return 1
            if has_signals:
                return 2
            return 3

        sorted_ch_nums = sorted(firm_buffer.keys(), key=_news_priority)
        total_to_check = len(sorted_ch_nums)

        for pos, ch_num in enumerate(sorted_ch_nums):
            if news_calls_used >= NEWS_DAILY_BUDGET:
                logger.warning(
                    "News API daily budget reached (%d calls used) — %d firm(s) skipped",
                    news_calls_used, total_to_check - pos,
                )
                break
            if news_calls_used >= int(NEWS_DAILY_BUDGET * 0.9):
                logger.warning(
                    "News API budget at 90%% (%d/%d calls used)", news_calls_used, NEWS_DAILY_BUDGET
                )

            buf  = firm_buffer[ch_num]
            name = buf["name"]
            ts   = datetime.utcnow().isoformat()

            logger.info("[NEWS %d/%d] %s  (%s)", pos + 1, total_to_check, name, ch_num)
            n_first  = news_is_first_run(news_conn, ch_num)
            articles, pages = fetch_newsapi(news_session, news_api_key, name, logger)
            news_calls_used += pages

            if articles is None:
                continue

            for art in articles:
                upsert_article(news_conn, ch_num, name, art, ts)
            upsert_news_firm(news_conn, ch_num, name, ts)
            news_conn.commit()

            if n_first:
                logger.info("  News: first run (%d relevant article(s) baselined)", len(articles))
                n_changes: List[Dict] = []
            else:
                baseline_urls = get_baseline_articles(news_conn, ch_num)
                n_changes = detect_news_changes(ch_num, name, baseline_urls, articles, n_first, logger)
                if not n_changes:
                    logger.info("  News: no new articles (%d relevant found)", len(articles))

            firm_buffer[ch_num]["n_changes"] = n_changes
            all_news_changes.extend(n_changes)
            news_new += len(n_changes)

        news_conn.close()

    # -------------------------------------------------------------------------
    # BUILD company_activity from buffered per-firm results
    # -------------------------------------------------------------------------
    for ch_num, buf in firm_buffer.items():
        o_changes = buf["o_changes"]
        j_changes = buf["j_changes"]
        n_changes = buf.get("n_changes", [])
        if not (o_changes or j_changes or n_changes):
            continue
        signal, score = signal_score(o_changes, j_changes, n_changes)
        company_activity[ch_num] = {
            "name":             buf["name"],
            "agent":            (buf["row"] or {}).get(FIRMS_AGENT_COLUMN, ""),
            "officer_changes":  o_changes,
            "job_changes":      j_changes,
            "news_changes":     n_changes,
            "director_count":   buf["director_count"],
            "active_job_count": buf["active_job_count"],
            "signal":           signal,
            "score":            score,
        }

    # -------------------------------------------------------------------------
    # LLM COMMENTARY — generate "so what" assessment per firm with activity
    # -------------------------------------------------------------------------
    if os.getenv("ANTHROPIC_API_KEY", "").strip():
        logger.info("Generating LLM commentary for %d firm(s)...", len(company_activity))
        for ch_num, activity in company_activity.items():
            commentary = generate_llm_commentary(
                activity["name"], activity["agent"], activity, logger
            )
            activity["llm_commentary"] = commentary
    else:
        logger.info("ANTHROPIC_API_KEY not set — skipping LLM commentary")

    # Annotate officer change rows with per-firm LLM commentary for CSV output
    for change in all_officer_changes:
        ch_num = change["company_number"]
        change["llm_commentary"] = company_activity.get(ch_num, {}).get("llm_commentary", "")

    write_digest(OFFICER_DIGEST, OFFICER_COLUMNS, all_officer_changes,
                 "No officer changes detected", logger)
    write_digest(JOBS_DIGEST, JOBS_COLUMNS, all_job_changes,
                 "No job changes detected", logger)
    write_digest(NEWS_DIGEST, NEWS_COLUMNS, all_news_changes,
                 "No news articles detected", logger)

    stats = {
        "firms_checked":    total_firms,
        "officer_new":      officer_new,
        "officer_resigned": officer_resigned,
        "job_new":          job_new,
        "job_disappeared":  job_disappeared,
        "news_new":         news_new,
        "ch_errors":        ch_errors,
        "adzuna_errors":    adzuna_errors,
    }

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info("Firms checked        : %d", total_firms)
    logger.info("New appointments     : %d", officer_new)
    logger.info("Resignations         : %d", officer_resigned)
    logger.info("New job postings     : %d", job_new)
    logger.info("Disappeared postings : %d", job_disappeared)
    logger.info("New news articles    : %d", news_new)
    logger.info("News API calls used  : %d / %d", news_calls_used, NEWS_DAILY_BUDGET)
    logger.info("CH API errors        : %d", ch_errors)
    logger.info("Adzuna API errors    : %d", adzuna_errors)
    logger.info("=" * 60)

    send_email(all_officer_changes, all_job_changes, all_news_changes, company_activity, stats, logger)


if __name__ == "__main__":
    main()
