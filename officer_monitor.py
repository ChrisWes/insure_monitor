"""
officer_monitor.py

Monitors a list of companies for officer changes (new appointments and resignations)
using the Companies House API. Stores a baseline in SQLite and writes a dated digest.
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
from typing import Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
CH_NUMBER_COLUMN = "Company Number"   # Column in firms.csv containing the CH number
CH_NAME_COLUMN   = "Company Name"     # Column in firms.csv containing the firm name

CH_API_BASE        = "https://api.company-information.service.gov.uk"
CH_WINDOW_SECONDS  = 300              # CH rate-limit window: 5 minutes
CH_WINDOW_REQUESTS = 500              # Target requests per window (hard ceiling is 600)
CH_MAX_RETRIES     = 3

# Roles logged at WARNING level for prominence in console output
SENIOR_ROLES = {
    "director",
    "secretary",
    "managing director",
    "chief executive",
    "chairman",
    "chief financial officer",
    "chief operating officer",
    "chief technology officer",
}

# ---------------------------------------------------------------------------
# EMAIL  — all read from .env; delivery is skipped if SMTP_HOST is not set
# ---------------------------------------------------------------------------
# SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, EMAIL_FROM, EMAIL_TO

# ---------------------------------------------------------------------------
# PATHS  (all relative to this script)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
DATA_DIR   = SCRIPT_DIR / "data"
OUTPUT_DIR = SCRIPT_DIR / "output"
LOGS_DIR   = SCRIPT_DIR / "logs"

FIRMS_CSV = INPUT_DIR / "firms.csv"
DB_PATH   = DATA_DIR / "officer_baseline.db"

TODAY      = date.today().isoformat()
DIGEST_CSV = OUTPUT_DIR / f"officer_changes_{TODAY}.csv"
LOG_FILE   = LOGS_DIR / f"monitor_{TODAY}.log"

DIGEST_COLUMNS = [
    "change_type",
    "company_number",
    "company_name",
    "officer_name",
    "officer_role",
    "appointed_on",
    "resigned_on",
    "date_detected",
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    logger = logging.getLogger("officer_monitor")
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
    """)
    conn.commit()


def is_first_run(conn: sqlite3.Connection, company_number: str) -> bool:
    row = conn.execute(
        "SELECT last_checked FROM firms WHERE company_number = ?",
        (company_number,),
    ).fetchone()
    return row is None


def get_baseline_officers(
    conn: sqlite3.Connection, company_number: str
) -> Dict[Tuple[str, str], Dict]:
    """Return active baseline officers keyed by (name_lower, role_lower)."""
    rows = conn.execute(
        "SELECT officer_name, officer_role, appointed_on FROM officers "
        "WHERE company_number = ? AND is_active = 1",
        (company_number,),
    ).fetchall()
    return {
        (r["officer_name"].lower(), r["officer_role"].lower()): dict(r)
        for r in rows
    }


def upsert_firm(
    conn: sqlite3.Connection, company_number: str, name: str, ts: str
) -> None:
    conn.execute(
        """INSERT INTO firms (company_number, company_name, last_checked)
           VALUES (?, ?, ?)
           ON CONFLICT(company_number) DO UPDATE SET
               company_name = excluded.company_name,
               last_checked = excluded.last_checked""",
        (company_number, name, ts),
    )


def upsert_officers(
    conn: sqlite3.Connection,
    company_number: str,
    officers: List[Dict],
    ts: str,
) -> None:
    for o in officers:
        is_active = 0 if o.get("resigned_on") else 1
        conn.execute(
            """INSERT INTO officers
               (company_number, officer_name, officer_role,
                appointed_on, resigned_on, is_active, last_seen)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(company_number, officer_name, officer_role) DO UPDATE SET
                   appointed_on = excluded.appointed_on,
                   resigned_on  = excluded.resigned_on,
                   is_active    = excluded.is_active,
                   last_seen    = excluded.last_seen""",
            (
                company_number,
                o["officer_name"],
                o["officer_role"],
                o.get("appointed_on") or None,
                o.get("resigned_on") or None,
                is_active,
                ts,
            ),
        )


# ---------------------------------------------------------------------------
# RATE LIMITER
# ---------------------------------------------------------------------------
class RateLimiter:
    """Enforces a maximum call rate against a rolling window."""

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
def build_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.auth = (api_key, "")
    session.headers.update({"Accept": "application/json"})
    return session


def _api_get(
    session: requests.Session,
    url: str,
    params: Optional[Dict],
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[requests.Response]:
    """GET with rate-limiting and automatic retry on 429."""
    for attempt in range(1, CH_MAX_RETRIES + 1):
        limiter.wait()
        try:
            resp = session.get(url, params=params or {}, timeout=(5, 20))
        except requests.RequestException as exc:
            logger.error("  Network error (attempt %d/%d): %s", attempt, CH_MAX_RETRIES, exc)
            if attempt < CH_MAX_RETRIES:
                time.sleep(2 ** attempt)
                continue
            return None

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", CH_WINDOW_SECONDS))
            logger.warning(
                "  HTTP 429 rate-limited (attempt %d/%d) — sleeping %ds for window reset",
                attempt, CH_MAX_RETRIES, retry_after,
            )
            time.sleep(retry_after)
            continue

        return resp

    return None


def fetch_officers(
    session: requests.Session,
    company_number: str,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[List[Dict]]:
    """Fetch all officers for a company (paginated). Returns None on API error."""
    url = f"{CH_API_BASE}/company/{company_number}/officers"
    PAGE_SIZE = 100
    all_officers: List[Dict] = []
    start = 0

    while True:
        resp = _api_get(
            session, url,
            {"items_per_page": PAGE_SIZE, "start_index": start},
            limiter, logger,
        )
        if resp is None:
            return None
        if resp.status_code == 404:
            logger.warning("  %s not found in Companies House (404) — skipping", company_number)
            return None
        if resp.status_code != 200:
            logger.error(
                "  Officers API returned HTTP %d for %s: %s",
                resp.status_code, company_number, resp.text[:200],
            )
            return None

        data = resp.json()
        for item in data.get("items", []):
            all_officers.append({
                "officer_name": str(item.get("name", "")).strip(),
                "officer_role": str(item.get("officer_role", "")).strip(),
                "appointed_on": item.get("appointed_on", "") or "",
                "resigned_on":  item.get("resigned_on", "") or "",
            })

        total = data.get("total_results", len(all_officers))
        start += PAGE_SIZE
        if start >= total:
            break

    return all_officers


# ---------------------------------------------------------------------------
# CHANGE DETECTION
# ---------------------------------------------------------------------------
def is_senior(role: str) -> bool:
    return any(sr in role.lower() for sr in SENIOR_ROLES)


def detect_changes(
    company_number: str,
    company_name: str,
    baseline: Dict[Tuple[str, str], Dict],
    api_officers: List[Dict],
    logger: logging.Logger,
) -> List[Dict]:
    """
    Compare API response against baseline.

    New appointment : officer is active in the API response and absent from the baseline.
    Resignation     : officer is active in the baseline but has resigned_on in the API response.

    Matching key is (officer_name, officer_role) — the same person can hold multiple roles.
    """
    changes: List[Dict] = []

    # Index current API response by (name_lower, role_lower)
    api_lookup: Dict[Tuple[str, str], Dict] = {
        (o["officer_name"].lower(), o["officer_role"].lower()): o
        for o in api_officers
    }

    # New appointments: active in API, not in baseline
    for key, o in api_lookup.items():
        if o["resigned_on"]:
            continue
        if key in baseline:
            continue
        senior = is_senior(o["officer_role"])
        logger.log(
            logging.WARNING if senior else logging.INFO,
            "  NEW APPOINTMENT: %s  |  %s%s",
            o["officer_name"], o["officer_role"],
            "  [SENIOR]" if senior else "",
        )
        changes.append({
            "change_type":    "New Appointment",
            "company_number": company_number,
            "company_name":   company_name,
            "officer_name":   o["officer_name"],
            "officer_role":   o["officer_role"],
            "appointed_on":   o["appointed_on"],
            "resigned_on":    "",
            "date_detected":  TODAY,
        })

    # Resignations: active in baseline, now carries resigned_on in API
    for key, b in baseline.items():
        api_o = api_lookup.get(key)
        if api_o is None or not api_o["resigned_on"]:
            continue
        senior = is_senior(b["officer_role"])
        logger.log(
            logging.WARNING if senior else logging.INFO,
            "  RESIGNATION:     %s  |  %s%s",
            b["officer_name"], b["officer_role"],
            "  [SENIOR]" if senior else "",
        )
        changes.append({
            "change_type":    "Resignation",
            "company_number": company_number,
            "company_name":   company_name,
            "officer_name":   b["officer_name"],
            "officer_role":   b["officer_role"],
            "appointed_on":   b.get("appointed_on", ""),
            "resigned_on":    api_o["resigned_on"],
            "date_detected":  TODAY,
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
            writer.writerow({
                "change_type":    "No changes detected",
                "company_number": "",
                "company_name":   "",
                "officer_name":   "",
                "officer_role":   "",
                "appointed_on":   "",
                "resigned_on":    "",
                "date_detected":  TODAY,
            })
    logger.info("Digest written -> %s", DIGEST_CSV.name)


# ---------------------------------------------------------------------------
# EMAIL DELIVERY
# ---------------------------------------------------------------------------
def send_digest_email(
    digest_rows: List[Dict],
    new_appointments: int,
    resignations: int,
    api_errors: int,
    firms_checked: int,
    logger: logging.Logger,
) -> None:
    smtp_host = os.getenv("SMTP_HOST", "").strip()
    if not smtp_host:
        logger.info("SMTP_HOST not set — skipping email delivery")
        return

    smtp_port     = int(os.getenv("SMTP_PORT", "587"))
    smtp_username = os.getenv("SMTP_USERNAME", "").strip()
    smtp_password = os.getenv("SMTP_PASSWORD", "").strip()
    email_from    = os.getenv("EMAIL_FROM", smtp_username).strip()
    email_to_raw  = os.getenv("EMAIL_TO", "").strip()

    if not email_to_raw:
        logger.warning("EMAIL_TO not set — skipping email delivery")
        return

    recipients = [addr.strip() for addr in email_to_raw.split(",") if addr.strip()]

    # Subject reflects whether anything was found
    total_changes = new_appointments + resignations
    if total_changes:
        subject = (
            f"Officer Monitor: {total_changes} change(s) detected — {TODAY} "
            f"({new_appointments} appointment(s), {resignations} resignation(s))"
        )
    else:
        subject = f"Officer Monitor: No changes — {TODAY}"

    # Plain-text body
    lines = [
        f"Officer Monitor — {TODAY}",
        "=" * 50,
        f"Firms checked      : {firms_checked}",
        f"New appointments   : {new_appointments}",
        f"Resignations       : {resignations}",
        f"API errors         : {api_errors}",
        "",
    ]

    if total_changes:
        lines.append("Changes detected:")
        lines.append("-" * 50)
        for r in digest_rows:
            ct = r.get("change_type", "")
            if ct in ("New Appointment", "Resignation"):
                lines.append(
                    f"  {ct:<20}  {r['company_name']}  |  "
                    f"{r['officer_name']}  |  {r['officer_role']}"
                )
        lines.append("")

    lines.append("Full digest attached.")
    body = "\n".join(lines)

    msg = MIMEMultipart()
    msg["From"]    = email_from
    msg["To"]      = ", ".join(recipients)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    # Attach the digest CSV
    if DIGEST_CSV.exists():
        with DIGEST_CSV.open("rb") as fh:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(fh.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition",
            f"attachment; filename={DIGEST_CSV.name}",
        )
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

        logger.info("Digest emailed to %s", ", ".join(recipients))

    except smtplib.SMTPException as exc:
        logger.error("Failed to send email: %s", exc)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Officer Monitor")
    logger.info("=" * 60)

    load_dotenv()
    api_key = os.getenv("CH_API_KEY", "").strip()
    if not api_key:
        logger.error("CH_API_KEY not set in .env file. Exiting.")
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

    session = build_session(api_key)
    limiter  = RateLimiter(CH_WINDOW_SECONDS, CH_WINDOW_REQUESTS)

    digest_rows:     List[Dict] = []
    total_firms      = len(firms)
    new_appointments = 0
    resignations     = 0
    api_errors       = 0
    baseline_count   = 0

    for idx, row in enumerate(firms, start=1):
        ch_num = row.get(CH_NUMBER_COLUMN, "").strip()
        name   = (row.get(CH_NAME_COLUMN, "") or "").strip() or ch_num

        if not ch_num:
            logger.warning("[%d/%d] Blank CH number — skipping row", idx, total_firms)
            continue

        if ch_num.isdigit():
            ch_num = ch_num.zfill(8)

        logger.info("[%d/%d] %s  (%s)", idx, total_firms, name, ch_num)

        first_run    = is_first_run(conn, ch_num)
        api_officers = fetch_officers(session, ch_num, limiter, logger)

        if api_officers is None:
            api_errors += 1
            continue

        ts = datetime.utcnow().isoformat()

        if first_run:
            logger.info("  First run — establishing baseline (%d officers)", len(api_officers))
            upsert_firm(conn, ch_num, name, ts)
            upsert_officers(conn, ch_num, api_officers, ts)
            conn.commit()
            baseline_count += 1
            # Record in digest so the first run produces a meaningful file
            digest_rows.append({
                "change_type":    "Initial Baseline",
                "company_number": ch_num,
                "company_name":   name,
                "officer_name":   f"{len(api_officers)} officers loaded",
                "officer_role":   "",
                "appointed_on":   "",
                "resigned_on":    "",
                "date_detected":  TODAY,
            })
            continue

        baseline = get_baseline_officers(conn, ch_num)
        changes  = detect_changes(ch_num, name, baseline, api_officers, logger)

        if not changes:
            logger.info("  No changes")

        upsert_firm(conn, ch_num, name, ts)
        upsert_officers(conn, ch_num, api_officers, ts)
        conn.commit()

        for c in changes:
            if c["change_type"] == "New Appointment":
                new_appointments += 1
            else:
                resignations += 1

        digest_rows.extend(changes)

    conn.close()
    write_digest(digest_rows, logger)

    firms_checked = total_firms - api_errors
    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info("Firms checked       : %d", firms_checked)
    logger.info("Initial baselines   : %d", baseline_count)
    logger.info("New appointments    : %d", new_appointments)
    logger.info("Resignations        : %d", resignations)
    logger.info("API errors          : %d", api_errors)
    logger.info("Digest              : %s", DIGEST_CSV.name)
    logger.info("=" * 60)

    send_digest_email(
        digest_rows, new_appointments, resignations, api_errors, firms_checked, logger
    )


if __name__ == "__main__":
    main()
