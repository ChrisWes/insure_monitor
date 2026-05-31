"""
seed_list_builder.py

Reads CSV files from input/, merges and deduplicates by Companies House number,
enriches each firm via the Companies House API, and writes output CSVs to output/.
"""

import csv
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# CONFIGURATION  — edit CH_NUMBER_COLUMN if your CSV uses a different heading
# ---------------------------------------------------------------------------
CH_NUMBER_COLUMN = "company_number"      # Column containing the CH registration number
CH_API_BASE = "https://api.company-information.service.gov.uk"
CH_WINDOW_SECONDS  = 300                 # CH rate-limit window: 5 minutes
CH_WINDOW_REQUESTS = 500                 # Target requests per window (ceiling is 600 — keep headroom)
CH_MAX_RETRIES     = 3                   # Max attempts per request before giving up

DEFAULT_MONITORING_STATUS = "Pending Review"

# ---------------------------------------------------------------------------
# PATHS  (all relative to this script — no hardcoded absolute paths)
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
OUTPUT_DIR = SCRIPT_DIR / "output"
LOGS_DIR   = SCRIPT_DIR / "logs"

OUTPUT_CSV  = OUTPUT_DIR / "seed_list_output.csv"
NO_DATA_CSV = OUTPUT_DIR / "seed_list_no_ch_data.csv"

# ---------------------------------------------------------------------------
# EXTRA COLUMNS APPENDED TO EVERY OUTPUT ROW
# ---------------------------------------------------------------------------
EXTRA_COLUMNS = [
    "ch_status",
    "ch_address",
    "ch_sic_codes",
    "ch_latest_accounts",
    "ch_active_officers",
    "monitoring_status",
    "notes",
    "lloyds_managing_agent",
    "employee_count",
]


# ---------------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------------
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = LOGS_DIR / f"seed_list_{ts}.log"

    logger = logging.getLogger("seed_list_builder")
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    ch_handler = logging.StreamHandler(sys.stdout)
    ch_handler.setLevel(logging.INFO)
    ch_handler.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch_handler)
    return logger


# ---------------------------------------------------------------------------
# RATE LIMITER
# ---------------------------------------------------------------------------
class RateLimiter:
    """Enforces a maximum call rate by sleeping between requests as needed."""

    def __init__(self, window_seconds: int, window_requests: int) -> None:
        # e.g. 300s / 500 requests = 0.6s minimum between calls
        self._min_interval = window_seconds / window_requests
        self._last_call: float = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        gap = now - self._last_call
        if gap < self._min_interval:
            time.sleep(self._min_interval - gap)
        self._last_call = time.monotonic()


# ---------------------------------------------------------------------------
# CSV HELPERS
# ---------------------------------------------------------------------------
def read_input_csvs(logger: logging.Logger) -> List[Dict]:
    if not INPUT_DIR.exists():
        logger.error("Input directory does not exist: %s", INPUT_DIR)
        sys.exit(1)

    csv_files = sorted(INPUT_DIR.glob("*.csv"))
    if not csv_files:
        logger.error("No CSV files found in %s", INPUT_DIR)
        sys.exit(1)

    rows: List[Dict] = []
    for path in csv_files:
        logger.info("Reading %s", path.name)
        with path.open(encoding="utf-8-sig") as fh:
            file_rows = list(csv.DictReader(fh))
        logger.info("  %d rows loaded", len(file_rows))
        rows.extend(file_rows)

    logger.info("Total rows across all input files: %d", len(rows))
    return rows


def deduplicate(rows: List[Dict], logger: logging.Logger) -> List[Dict]:
    """Keep the first occurrence of each unique normalised CH number."""
    seen: Dict[str, Dict] = {}
    skipped = 0

    for row in rows:
        raw = row.get(CH_NUMBER_COLUMN, "").strip()
        if not raw:
            logger.warning("Row with blank CH number skipped: %s", dict(list(row.items())[:3]))
            skipped += 1
            continue
        key = normalise_ch_number(raw)
        if key not in seen:
            seen[key] = row

    if skipped:
        logger.warning("%d row(s) skipped due to missing CH number", skipped)
    logger.info("Unique firms after deduplication: %d", len(seen))
    return list(seen.values())


def normalise_ch_number(raw: str) -> str:
    """Zero-pad numeric CH numbers to 8 digits; leave prefixed ones (SC, NI…) uppercased."""
    s = raw.strip()
    if s.isdigit():
        return s.zfill(8)
    return s.upper()


# ---------------------------------------------------------------------------
# COMPANIES HOUSE API
# ---------------------------------------------------------------------------
def build_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.auth = (api_key, "")          # CH uses HTTP Basic Auth: key as username, blank password
    session.headers.update({"Accept": "application/json"})
    return session


def _api_get(
    session: requests.Session,
    url: str,
    params: Optional[Dict],
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[requests.Response]:
    """GET with rate-limiting and automatic retry on 429. Returns None on unrecoverable failure."""
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
            # CH rate limit is 600 requests per 5-minute window.
            # Sleep the full window so it resets; Retry-After header overrides if present.
            retry_after = int(resp.headers.get("Retry-After", CH_WINDOW_SECONDS))
            logger.warning(
                "  HTTP 429 — rate limit window exhausted (attempt %d/%d). "
                "Sleeping %ds for window reset...",
                attempt, CH_MAX_RETRIES, retry_after,
            )
            time.sleep(retry_after)
            continue

        return resp

    logger.error("  Exhausted %d retries for %s", CH_MAX_RETRIES, url)
    return None


def fetch_company_profile(
    session: requests.Session,
    ch_number: str,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[Dict]:
    url = f"{CH_API_BASE}/company/{ch_number}"
    resp = _api_get(session, url, None, limiter, logger)
    if resp is None:
        return None
    if resp.status_code == 200:
        return resp.json()
    if resp.status_code == 404:
        logger.warning("  Company %s not found in Companies House (404)", ch_number)
    else:
        logger.error("  Profile API returned HTTP %d for %s: %s", resp.status_code, ch_number, resp.text[:200])
    return None


def fetch_active_officer_count(
    session: requests.Session,
    ch_number: str,
    limiter: RateLimiter,
    logger: logging.Logger,
) -> Optional[int]:
    url = f"{CH_API_BASE}/company/{ch_number}/officers"
    PAGE_SIZE = 100
    active_count = 0
    start = 0

    while True:
        resp = _api_get(session, url, {"items_per_page": PAGE_SIZE, "start_index": start}, limiter, logger)
        if resp is None:
            return None
        if resp.status_code == 404:
            logger.warning("  Officers endpoint 404 for %s", ch_number)
            return None
        if resp.status_code != 200:
            logger.error("  Officers API returned HTTP %d for %s: %s", resp.status_code, ch_number, resp.text[:200])
            return None

        data = resp.json()
        items = data.get("items", [])
        active_count += sum(1 for officer in items if not officer.get("resigned_on"))

        total_results = data.get("total_results", len(items))
        start += PAGE_SIZE
        if start >= total_results:
            break

    return active_count


def enrich_from_profile(profile: Dict, officer_count: Optional[int]) -> Dict:
    addr = profile.get("registered_office_address", {})
    addr_parts = [
        addr.get("address_line_1", ""),
        addr.get("address_line_2", ""),
        addr.get("locality", ""),
        addr.get("region", ""),
        addr.get("postal_code", ""),
        addr.get("country", ""),
    ]
    address_str = ", ".join(p for p in addr_parts if p)

    sic_codes = profile.get("sic_codes", [])

    last_accounts = profile.get("accounts", {}).get("last_accounts", {})
    latest_accounts_date = last_accounts.get("made_up_to", "")

    return {
        "ch_status":          profile.get("company_status", ""),
        "ch_address":         address_str,
        "ch_sic_codes":       ", ".join(sic_codes),
        "ch_latest_accounts": latest_accounts_date,
        "ch_active_officers": "" if officer_count is None else str(officer_count),
    }


def empty_enrichment() -> Dict:
    return {
        "ch_status":          "",
        "ch_address":         "",
        "ch_sic_codes":       "",
        "ch_latest_accounts": "",
        "ch_active_officers": "",
    }


# ---------------------------------------------------------------------------
# OUTPUT
# ---------------------------------------------------------------------------
def write_csv(
    path: Path,
    fieldnames: List[str],
    rows: List[Dict],
    logger: logging.Logger,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %d row(s) -> %s", len(rows), path.name)


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main() -> None:
    logger = setup_logging()
    logger.info("=" * 60)
    logger.info("Seed List Builder")
    logger.info("=" * 60)

    load_dotenv()
    api_key = os.getenv("CH_API_KEY", "").strip()
    if not api_key:
        logger.error("CH_API_KEY not set in .env file. Exiting.")
        sys.exit(1)

    # Read and deduplicate
    rows = read_input_csvs(logger)
    unique_rows = deduplicate(rows, logger)
    if not unique_rows:
        logger.error("No rows to process after deduplication. Exiting.")
        sys.exit(1)

    # Fieldnames: preserve all original columns then append extra ones
    original_columns = list(unique_rows[0].keys())
    fieldnames = original_columns + EXTRA_COLUMNS

    session = build_session(api_key)
    limiter  = RateLimiter(CH_WINDOW_SECONDS, CH_WINDOW_REQUESTS)

    enriched: List[Dict] = []
    no_data:  List[Dict] = []
    total = len(unique_rows)
    success_count = 0
    fail_count    = 0

    for idx, row in enumerate(unique_rows, start=1):
        ch_num = normalise_ch_number(row.get(CH_NUMBER_COLUMN, "").strip())
        name   = row.get("company_name", ch_num) or ch_num
        logger.info("[%d/%d] %s  (%s)", idx, total, name, ch_num)

        profile = fetch_company_profile(session, ch_num, limiter, logger)

        if profile is None:
            enrichment = empty_enrichment()
            fail_count += 1
            out_row = {
                **row,
                **enrichment,
                "monitoring_status":    DEFAULT_MONITORING_STATUS,
                "notes":                "",
                "lloyds_managing_agent": "",
                "employee_count":       "",
            }
            no_data.append(out_row)
        else:
            officer_count = fetch_active_officer_count(session, ch_num, limiter, logger)
            enrichment = enrich_from_profile(profile, officer_count)
            success_count += 1
            out_row = {
                **row,
                **enrichment,
                "monitoring_status":    DEFAULT_MONITORING_STATUS,
                "notes":                "",
                "lloyds_managing_agent": "",
                "employee_count":       "",
            }

        enriched.append(out_row)

    # Write both output files
    write_csv(OUTPUT_CSV,  fieldnames, enriched, logger)
    write_csv(NO_DATA_CSV, fieldnames, no_data,  logger)

    # Final summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("Summary")
    logger.info("=" * 60)
    logger.info("Total firms processed : %d", total)
    logger.info("Successful enrichments: %d", success_count)
    logger.info("Failed lookups        : %d", fail_count)
    logger.info("Full output           : %s", OUTPUT_CSV)
    logger.info("No-data output        : %s", NO_DATA_CSV)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
