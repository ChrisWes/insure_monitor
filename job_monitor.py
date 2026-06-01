"""
job_monitor.py

Monitors job postings for target firms via the Adzuna API.
Detects new and disappeared postings and writes a dated digest.
"""

import csv
import logging
import os
import smtplib
import sqlite3
import sys
import time
from datetime import date, datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Dict, List, Optional

import re

import requests
from dotenv import load_dotenv
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
FIRMS_NAME_COLUMN   = "Company Name"
FIRMS_NUMBER_COLUMN = "Company Number"
FIRMS_AGENT_COLUMN  = "lloyds_managing_agent"
FIRMS_STATUS_COLUMN = "monitoring_status"

ADZUNA_BASE_URL         = "https://api.adzuna.com/v1/api/jobs/gb/search"
ADZUNA_RESULTS_PER_PAGE = 50
ADZUNA_MAX_PER_MINUTE   = 100
ADZUNA_MAX_PAGES        = 10   # Cap pagination — prevents runaway calls for large firms

COMPANY_MATCH_THRESHOLD = 80   # Minimum rapidfuzz score to accept a result as from the target firm
SHORT_LIVED_DAYS        = 30   # Postings that disappear within this many days flagged as short-lived
MULTI_POSTING_THRESHOLD = 3    # New posting count at or above this triggers a prominent log warning

ROLE_KEYWORDS = [
    "transformation",
    "programme manager",
    "project manager",
    "business analyst",
    "PMO",
    "change manager",
    "data manager",
    "digital",
    "technology lead",
    "IT director",
    "chief information",
    "chief technology",
    "chief data",
]

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
LOGS_DIR   = SCRIPT_DIR / "logs"

FIRMS_CSV = INPUT_DIR / "firms.csv"
DB_PATH   = DATA_DIR / "jobs_baseline.db"

TODAY      = date.today().isoformat()
DIGEST_CSV = OUTPUT_DIR / f"job_changes_{TODAY}.csv"
LOG_FILE   = LOGS_DIR / f"jobs_monitor_{TODAY}.log"

DIGEST_COLUMNS = [
    "change_type",
    "company_number",
    "company_name",
    "job_title",
    "job_location",
    "salary_min",
    "salary_max",
    "posted_date",
    "date_detected",
    "lloyds_managing_agent",
    "monitoring_status",
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("job_monitor")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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
# DATABASE
# ---------------------------------------------------------------------------
def init_db(conn: sqlite3.Connection) -> None:
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


def is_first_run(conn: sqlite3.Connection, company_number: str) -> bool:
    row = conn.execute(
        "SELECT last_checked FROM processed_firms WHERE company_number = ?",
        (company_number,),
    ).fetchone()
    return row is None


def get_active_postings(conn: sqlite3.Connection, company_number: str) -> Dict[str, Dict]:
    """Return active baseline postings keyed by job_id."""
    rows = conn.execute(
        "SELECT job_id, job_title, job_location, salary_min, salary_max, "
        "posted_date, first_seen FROM job_postings "
        "WHERE company_number = ? AND is_active = 1",
        (company_number,),
    ).fetchall()
    return {r["job_id"]: dict(r) for r in rows}


def upsert_posting(
    conn: sqlite3.Connection,
    company_number: str,
    company_name: str,
    job: Dict,
    ts: str,
) -> None:
    conn.execute(
        """INSERT INTO job_postings
           (company_number, company_name, job_id, job_title, job_location,
            salary_min, salary_max, posted_date, first_seen, last_seen, is_active)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
           ON CONFLICT(company_number, job_id) DO UPDATE SET
               last_seen = excluded.last_seen,
               is_active = 1""",
        (
            company_number,
            company_name,
            job["job_id"],
            job["job_title"],
            job["job_location"],
            job.get("salary_min"),
            job.get("salary_max"),
            job.get("posted_date"),
            ts,
            ts,
        ),
    )


def mark_disappeared(conn: sqlite3.Connection, company_number: str, job_id: str) -> None:
    conn.execute(
        "UPDATE job_postings SET is_active = 0 WHERE company_number = ? AND job_id = ?",
        (company_number, job_id),
    )


def upsert_processed_firm(
    conn: sqlite3.Connection, company_number: str, name: str, ts: str
) -> None:
    conn.execute(
        """INSERT INTO processed_firms (company_number, company_name, last_checked)
           VALUES (?, ?, ?)
           ON CONFLICT(company_number) DO UPDATE SET
               company_name = excluded.company_name,
               last_checked = excluded.last_checked""",
        (company_number, name, ts),
    )


# ---------------------------------------------------------------------------
# RATE LIMITER
# ---------------------------------------------------------------------------
class RateLimiter:
    def __init__(self, max_per_minute: int) -> None:
        self._min_interval = 60.0 / max_per_minute
        self._last_call: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        gap = now - self._last_call
        if gap < self._min_interval:
            time.sleep(self._min_interval - gap)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# ADZUNA API
# ---------------------------------------------------------------------------
# Words stripped before querying Adzuna — Adzuna uses trading names, not registered names
_STRIP_FOR_ADZUNA = re.compile(
    r"\b(limited|ltd|llp|plc|lp|inc|the|"
    r"insurance|reinsurance|underwriters|underwriting|"
    r"syndicate|syndicates|managing agency|managing agent|"
    r"group|holdings|uk|services|financial|life|general|"
    r"mutual|assurance|society|association|"
    r"of|and)\b",
    re.IGNORECASE,
)
_WHITESPACE = re.compile(r"\s+")


def make_search_name(registered_name: str) -> str:
    """Reduce a CH registered name to a short trading-style name Adzuna can resolve."""
    cleaned = _STRIP_FOR_ADZUNA.sub(" ", registered_name)
    cleaned = _WHITESPACE.sub(" ", cleaned).strip()
    # Take the first two meaningful words (avoids over-broad queries)
    words = [w for w in cleaned.split() if len(w) > 1]
    result = " ".join(words[:2]) if words else registered_name
    return result or registered_name


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})
    return session


def matches_keywords(title: str, description: str) -> bool:
    text = f"{title} {description}".lower()
    return any(kw.lower() in text for kw in ROLE_KEYWORDS)


def fetch_jobs_for_firm(
    session: requests.Session,
    app_id: str,
    app_key: str,
    firm_name: str,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[List[Dict]]:
    """Fetch all relevant job postings for a firm, paginated. Returns None on API error."""
    search_name = make_search_name(firm_name)
    if search_name != firm_name:
        logger.debug("  Search name: %s -> %s", firm_name, search_name)

    relevant: List[Dict] = []
    page = 1

    while True:
        url = f"{ADZUNA_BASE_URL}/{page}"
        limiter.wait()

        try:
            resp = session.get(
                url,
                params={
                    "app_id":           app_id,
                    "app_key":          app_key,
                    "what":             search_name,
                    "results_per_page": ADZUNA_RESULTS_PER_PAGE,
                },
                timeout=(5, 20),
            )
        except requests.RequestException as exc:
            logger.error("  Adzuna request failed (page %d): %s", page, exc)
            return None

        if resp.status_code == 400:
            if page == 1:
                logger.error("  Adzuna 400 on page 1 for '%s'", search_name)
                return None
            # 400 on subsequent pages means we've gone past the last page
            break

        if resp.status_code in (500, 502, 503, 504):
            # Transient server error — wait and retry up to 3 times
            if not hasattr(fetch_jobs_for_firm, "_retries"):
                fetch_jobs_for_firm._retries = {}
            key = (firm_name, page)
            retries = fetch_jobs_for_firm._retries.get(key, 0)
            if retries < 3:
                wait = 10 * (retries + 1)
                logger.warning(
                    "  Adzuna %d for %s (attempt %d/3) — retrying in %ds",
                    resp.status_code, firm_name, retries + 1, wait,
                )
                fetch_jobs_for_firm._retries[key] = retries + 1
                time.sleep(wait)
                continue
            logger.error("  Adzuna %d for %s — giving up after 3 retries", resp.status_code, firm_name)
            return None

        if resp.status_code != 200:
            logger.error(
                "  Adzuna returned HTTP %d for %s: %s",
                resp.status_code, firm_name, resp.text[:200],
            )
            return None

        data = resp.json()
        results = data.get("results", [])
        total   = data.get("count", 0)

        for job in results:
            # Filter by company name similarity
            company_display = job.get("company", {}).get("display_name", "")
            if fuzz.token_set_ratio(firm_name.lower(), company_display.lower()) < COMPANY_MATCH_THRESHOLD:
                continue

            title       = job.get("title", "")
            description = job.get("description", "")

            if not matches_keywords(title, description):
                continue

            created     = job.get("created", "")
            relevant.append({
                "job_id":       str(job.get("id", "")),
                "job_title":    title,
                "job_location": job.get("location", {}).get("display_name", ""),
                "salary_min":   job.get("salary_min"),
                "salary_max":   job.get("salary_max"),
                "posted_date":  created[:10] if created else "",
            })

        fetched = (page - 1) * ADZUNA_RESULTS_PER_PAGE + len(results)
        if not results or fetched >= total or page >= ADZUNA_MAX_PAGES:
            break

        page += 1

    return relevant


# ---------------------------------------------------------------------------
# CHANGE DETECTION
# ---------------------------------------------------------------------------
def detect_changes(
    company_number: str,
    company_name: str,
    baseline: Dict[str, Dict],
    current_jobs: List[Dict],
    firm_meta: Dict,
    logger: logging.Logger,
) -> List[Dict]:
    changes: List[Dict] = []
    current_ids = {job["job_id"] for job in current_jobs}

    agent  = firm_meta.get(FIRMS_AGENT_COLUMN, "")
    status = firm_meta.get(FIRMS_STATUS_COLUMN, "")

    # New postings: in current API response but not in baseline
    new_count = 0
    for job in current_jobs:
        if job["job_id"] not in baseline:
            new_count += 1
            logger.info("  NEW: %s  |  %s", job["job_title"], job["job_location"])
            changes.append({
                "change_type":          "New Posting",
                "company_number":       company_number,
                "company_name":         company_name,
                "job_title":            job["job_title"],
                "job_location":         job["job_location"],
                "salary_min":           job.get("salary_min") or "",
                "salary_max":           job.get("salary_max") or "",
                "posted_date":          job["posted_date"],
                "date_detected":        TODAY,
                "lloyds_managing_agent": agent,
                "monitoring_status":    status,
            })

    if new_count >= MULTI_POSTING_THRESHOLD:
        logger.warning(
            "  ** %d new relevant postings — strong signal of change activity **",
            new_count,
        )

    # Disappeared: active in baseline but absent from current response
    for job_id, b in baseline.items():
        if job_id in current_ids:
            continue

        first_seen  = datetime.fromisoformat(b["first_seen"])
        days_active = (datetime.utcnow() - first_seen).days
        change_type = (
            "Disappeared - Short-lived" if days_active < SHORT_LIVED_DAYS
            else "Disappeared"
        )

        logger.info(
            "  %s: %s  (active %d days)",
            change_type.upper(), b["job_title"], days_active,
        )
        changes.append({
            "change_type":          change_type,
            "company_number":       company_number,
            "company_name":         company_name,
            "job_title":            b["job_title"],
            "job_location":         b.get("job_location", ""),
            "salary_min":           b.get("salary_min") or "",
            "salary_max":           b.get("salary_max") or "",
            "posted_date":          b.get("posted_date", ""),
            "date_detected":        TODAY,
            "lloyds_managing_agent": agent,
            "monitoring_status":    status,
        })

    return changes


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------
def write_digest(digest_rows: List[Dict], logger: logging.Logger) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    with DIGEST_CSV.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=DIGEST_COLUMNS)
        writer.writeheader()
        if digest_rows:
            writer.writerows(digest_rows)
        else:
            writer.writerow({col: "" for col in DIGEST_COLUMNS} | {
                "change_type":  "No changes detected",
                "date_detected": TODAY,
            })
    logger.info("Digest written -> %s", DIGEST_CSV.name)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Job Monitor")
    logger.info("=" * 60)

    load_dotenv()
    app_id  = os.getenv("ADZUNA_APP_ID", "").strip()
    app_key = os.getenv("ADZUNA_APP_KEY", "").strip()
    if not app_id or not app_key:
        logger.error("ADZUNA_APP_ID and ADZUNA_APP_KEY must be set in .env. Exiting.")
        sys.exit(1)

    if not FIRMS_CSV.exists():
        logger.error("Firms CSV not found: %s", FIRMS_CSV)
        sys.exit(1)

    with FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        firms = list(csv.DictReader(fh))

    if not firms:
        logger.error("No firms found in %s", FIRMS_CSV)
        sys.exit(1)

    logger.info("Loaded %d firms from %s", len(firms), FIRMS_CSV.name)

    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    session = build_session()
    limiter = RateLimiter(ADZUNA_MAX_PER_MINUTE)

    digest_rows:    List[Dict] = []
    total_firms     = len(firms)
    new_postings    = 0
    disappeared     = 0
    api_errors      = 0
    baseline_count  = 0

    for idx, row in enumerate(firms, start=1):
        ch_num = row.get(FIRMS_NUMBER_COLUMN, "").strip()
        name   = (row.get(FIRMS_NAME_COLUMN, "") or "").strip() or ch_num

        if not ch_num:
            logger.warning("[%d/%d] Blank CH number — skipping", idx, total_firms)
            continue

        logger.info("[%d/%d] %s  (%s)", idx, total_firms, name, ch_num)

        first_run    = is_first_run(conn, ch_num)
        current_jobs = fetch_jobs_for_firm(session, app_id, app_key, name, limiter, logger)

        if current_jobs is None:
            api_errors += 1
            continue

        ts = datetime.utcnow().isoformat()

        if first_run:
            logger.info("  First run — establishing baseline (%d relevant postings)", len(current_jobs))
            for job in current_jobs:
                upsert_posting(conn, ch_num, name, job, ts)
            upsert_processed_firm(conn, ch_num, name, ts)
            conn.commit()
            baseline_count += 1
            digest_rows.append({
                "change_type":          "Initial Baseline",
                "company_number":       ch_num,
                "company_name":         name,
                "job_title":            f"{len(current_jobs)} relevant postings loaded",
                "job_location":         "",
                "salary_min":           "",
                "salary_max":           "",
                "posted_date":          "",
                "date_detected":        TODAY,
                "lloyds_managing_agent": row.get(FIRMS_AGENT_COLUMN, ""),
                "monitoring_status":    row.get(FIRMS_STATUS_COLUMN, ""),
            })
            continue

        baseline = get_active_postings(conn, ch_num)
        changes  = detect_changes(ch_num, name, baseline, current_jobs, row, logger)

        if not changes:
            logger.info("  No changes  (%d relevant postings live)", len(current_jobs))

        # Update baseline: upsert all current jobs, mark missing ones as disappeared
        current_ids = {job["job_id"] for job in current_jobs}
        for job in current_jobs:
            upsert_posting(conn, ch_num, name, job, ts)
        for job_id in baseline:
            if job_id not in current_ids:
                mark_disappeared(conn, ch_num, job_id)

        upsert_processed_firm(conn, ch_num, name, ts)
        conn.commit()

        for c in changes:
            if c["change_type"] == "New Posting":
                new_postings += 1
            else:
                disappeared += 1

        digest_rows.extend(changes)

    conn.close()
    write_digest(digest_rows, logger)

    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info("Firms checked       : %d", total_firms - api_errors)
    logger.info("Initial baselines   : %d", baseline_count)
    logger.info("New postings        : %d", new_postings)
    logger.info("Disappeared         : %d", disappeared)
    logger.info("API errors          : %d", api_errors)
    logger.info("Digest              : %s", DIGEST_CSV.name)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
