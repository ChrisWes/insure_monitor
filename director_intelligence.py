"""
director_intelligence.py

Persistent director profile database for cross-firm intelligence.

Tracks each director across all monitored firms using their Companies House
officer ID as a stable key, building a cumulative picture of:
  - Digital transformation background (from role title analysis)
  - Connections to watchlist firms (firms.csv)
  - Connections to client firms (clients.csv, exported from HubSpot)
  - Concurrent positions at other watched firms

This means when the same person appears at multiple firms on different days,
each report has the full cross-firm context rather than seeing them in isolation.
"""

import csv
import json
import re
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Role title keywords that signal digital transformation leadership.
# Matched case-insensitively against the full role string.
DIGITAL_ROLE_KEYWORDS = [
    "digital",
    "technology",
    "innovation",
    "transformation",
    "chief information",
    "chief digital",
    "chief technology",
    "chief data",
    "cto",
    "cdo",
    "cio",
    "head of it",
    "head of digital",
    "head of technology",
    "it director",
    "programme director",
    "program director",
    "data director",
    "technical director",
    "platform",
    "cyber",
    "infrastructure",
    "e-commerce",
    "ecommerce",
]

# Refresh a director profile if it's older than this many days.
PROFILE_STALE_DAYS = 30

# Build the pattern after the keyword list is defined (below).
# Done at module level to compile once rather than per call.
_DIGITAL_RE = re.compile(
    r"\b(?:" + "|".join(re.escape(kw) for kw in DIGITAL_ROLE_KEYWORDS) + r")\b",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# OFFICER ID EXTRACTION
# ---------------------------------------------------------------------------

def extract_officer_id(appointments_url: str) -> str:
    """Pull the persistent CH officer ID out of an appointments URL.

    CH appointments URLs look like: /officers/abc123def456/appointments
    This ID is stable across all companies the person is associated with.
    """
    match = re.match(r"/officers/([^/]+)/appointments", appointments_url or "")
    return match.group(1) if match else ""


# ---------------------------------------------------------------------------
# DATABASE INITIALISATION
# ---------------------------------------------------------------------------

def init_director_db(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS directors (
            ch_officer_id      TEXT PRIMARY KEY,
            officer_name       TEXT NOT NULL,
            digital_background INTEGER NOT NULL DEFAULT 0,
            digital_roles      TEXT    NOT NULL DEFAULT '[]',
            client_alum        INTEGER NOT NULL DEFAULT 0,
            profile_updated_at TIMESTAMP
        );

        -- One row per appointment in this person's CH history.
        -- company_number and officer_role are NOT NULL (defaulting to '')
        -- so the UNIQUE constraint works even when CH returns empty values.
        CREATE TABLE IF NOT EXISTS director_appointments (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ch_officer_id   TEXT    NOT NULL REFERENCES directors(ch_officer_id),
            company_number  TEXT    NOT NULL DEFAULT '',
            company_name    TEXT,
            officer_role    TEXT    NOT NULL DEFAULT '',
            appointed_on    TEXT,
            resigned_on     TEXT,
            is_current      INTEGER NOT NULL DEFAULT 0,
            is_watchlist    INTEGER NOT NULL DEFAULT 0,
            is_client       INTEGER NOT NULL DEFAULT 0,
            is_digital_role INTEGER NOT NULL DEFAULT 0,
            UNIQUE(ch_officer_id, company_number, officer_role)
        );
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# CLIENT LIST
# ---------------------------------------------------------------------------

def load_clients(clients_csv_path: Path) -> Dict[str, str]:
    """Load company_number → company_name from clients.csv.

    Accepts either 'Company Number' or 'company_number' as the column header.
    Returns an empty dict silently if the file doesn't exist yet — the tool
    degrades gracefully until the user exports their HubSpot client list.
    """
    if not clients_csv_path.exists():
        return {}
    clients: Dict[str, str] = {}
    with clients_csv_path.open(encoding="utf-8-sig") as fh:
        for row in csv.DictReader(fh):
            num  = (row.get("Company Number") or row.get("company_number") or "").strip()
            name = (row.get("Company Name")   or row.get("company_name")   or "").strip()
            if num:
                num = num.zfill(8) if num.isdigit() else num
                clients[num] = name
    return clients


# ---------------------------------------------------------------------------
# DIGITAL ROLE ANALYSIS
# ---------------------------------------------------------------------------

def _is_digital_role(role: str) -> bool:
    return bool(_DIGITAL_RE.search(role))


def analyze_digital_roles(appointments: List[Dict]) -> Tuple[bool, List[str]]:
    """Scan a list of appointments for digital/tech role titles.

    Returns (has_digital_background, list_of_triggering_role_descriptions).
    """
    seen: Set[str] = set()
    triggering: List[str] = []
    for appt in appointments:
        role = (appt.get("role") or appt.get("officer_role") or "").strip()
        if role and _is_digital_role(role) and role.lower() not in seen:
            seen.add(role.lower())
            company = appt.get("company_name") or ""
            label = f"{role.title()} at {company}" if company else role.title()
            triggering.append(label)
    return bool(triggering), triggering


# ---------------------------------------------------------------------------
# DATABASE READ / WRITE
# ---------------------------------------------------------------------------

def get_director_profile(conn: sqlite3.Connection, ch_officer_id: str) -> Optional[Dict]:
    """Return the full director profile dict, or None if not yet in DB."""
    row = conn.execute(
        "SELECT * FROM directors WHERE ch_officer_id = ?", (ch_officer_id,)
    ).fetchone()
    if not row:
        return None
    appts = conn.execute(
        "SELECT * FROM director_appointments "
        "WHERE ch_officer_id = ? ORDER BY appointed_on DESC NULLS LAST",
        (ch_officer_id,),
    ).fetchall()
    return {
        "ch_officer_id":      row["ch_officer_id"],
        "officer_name":       row["officer_name"],
        "digital_background": bool(row["digital_background"]),
        "digital_roles":      json.loads(row["digital_roles"] or "[]"),
        "client_alum":        bool(row["client_alum"]),
        "profile_updated_at": row["profile_updated_at"],
        "appointments":       [dict(a) for a in appts],
    }


def is_profile_stale(profile: Dict) -> bool:
    updated = profile.get("profile_updated_at")
    if not updated:
        return True
    return (datetime.utcnow() - datetime.fromisoformat(updated)).days >= PROFILE_STALE_DAYS


def store_director_profile(
    conn: sqlite3.Connection,
    ch_officer_id: str,
    officer_name: str,
    appointments: List[Dict],
    watchlist_numbers: Set[str],
    client_numbers: Set[str],
    ts: str,
) -> Optional[Dict]:
    """Persist (or refresh) a director's full profile from their CH appointments list.

    appointments: raw list from fetch_officer_appointments() in intelligence_monitor.py
    watchlist_numbers: set of CH company numbers from firms.csv
    client_numbers: set of CH company numbers from clients.csv
    """
    has_digital, digital_roles = analyze_digital_roles(appointments)
    has_client = any(
        (a.get("company_number") or "").strip() in client_numbers
        for a in appointments
    )

    conn.execute(
        """INSERT INTO directors
               (ch_officer_id, officer_name, digital_background,
                digital_roles, client_alum, profile_updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(ch_officer_id) DO UPDATE SET
               officer_name       = excluded.officer_name,
               digital_background = excluded.digital_background,
               digital_roles      = excluded.digital_roles,
               client_alum        = excluded.client_alum,
               profile_updated_at = excluded.profile_updated_at""",
        (
            ch_officer_id, officer_name, int(has_digital),
            json.dumps(digital_roles), int(has_client), ts,
        ),
    )

    for appt in appointments:
        comp_num = (appt.get("company_number") or "").strip()
        # CH appointments API returns company numbers without zero-padding; normalise
        # to match the zero-padded format used in firms.csv and watchlist_numbers.
        if comp_num.isdigit():
            comp_num = comp_num.zfill(8)
        role     = (appt.get("role") or appt.get("officer_role") or "").strip()
        is_cur   = 1 if not appt.get("resigned_on") else 0
        is_watch = 1 if comp_num in watchlist_numbers else 0
        is_cli   = 1 if comp_num in client_numbers else 0
        is_dig   = 1 if _is_digital_role(role) else 0

        conn.execute(
            """INSERT INTO director_appointments
                   (ch_officer_id, company_number, company_name, officer_role,
                    appointed_on, resigned_on, is_current,
                    is_watchlist, is_client, is_digital_role)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(ch_officer_id, company_number, officer_role) DO UPDATE SET
                   appointed_on    = excluded.appointed_on,
                   resigned_on     = excluded.resigned_on,
                   is_current      = excluded.is_current,
                   is_watchlist    = excluded.is_watchlist,
                   is_client       = excluded.is_client,
                   is_digital_role = excluded.is_digital_role""",
            (
                ch_officer_id, comp_num, appt.get("company_name"),
                role,
                appt.get("appointed_on") or None,
                appt.get("resigned_on")  or None,
                is_cur, is_watch, is_cli, is_dig,
            ),
        )

    conn.commit()
    return get_director_profile(conn, ch_officer_id)


# ---------------------------------------------------------------------------
# INTELLIGENCE SUMMARY (for LLM prompts and email output)
# ---------------------------------------------------------------------------

def build_director_intelligence(
    profile: Dict,
    exclude_company_number: str,
    watchlist_names: Dict[str, str],
    client_names: Dict[str, str],
) -> Dict:
    """Build a structured intelligence summary for one director appointment.

    exclude_company_number: the firm they've just been appointed to (excluded
        from "prior" and "connections" lists to avoid self-reference).

    Returns a dict with:
        career_summary        — plain-text career history (5 roles max)
        digital_background    — bool
        digital_roles         — list of triggering role descriptions
        watchlist_connections — list of dicts for any watched-firm history
        client_connections    — list of dicts for any client-firm history
        concurrent_watchlist  — subset of watchlist_connections that are still active
    """
    appointments = profile.get("appointments", [])

    # Plain-text career summary for the basic career history line
    prior = [
        a for a in appointments
        if (a.get("company_number") or "") != exclude_company_number
    ]
    career_parts: List[str] = []
    for a in prior[:5]:
        tenure = (a.get("appointed_on") or "?")[:4]
        end    = (a.get("resigned_on")  or "")[:4] or "present"
        career_parts.append(
            f"{(a.get('officer_role') or '').title()} at {a.get('company_name', '')} "
            f"({tenure}–{end})"
        )
    career_summary = (
        ("Previously: " + "; ".join(career_parts))
        if career_parts
        else "No other UK registered directorships found."
    )

    def _connection(a: Dict, name_lookup: Dict[str, str]) -> Dict:
        comp_num = a.get("company_number") or ""
        return {
            "company_number": comp_num,
            "company_name":   name_lookup.get(comp_num) or a.get("company_name") or "",
            "officer_role":   a.get("officer_role") or "",
            "appointed_on":   a.get("appointed_on") or "",
            "resigned_on":    a.get("resigned_on")  or "",
            "is_current":     bool(a.get("is_current")),
        }

    watchlist_connections = [
        _connection(a, watchlist_names)
        for a in appointments
        if a.get("is_watchlist") and (a.get("company_number") or "") != exclude_company_number
    ]

    client_connections = [
        _connection(a, client_names)
        for a in appointments
        if a.get("is_client") and (a.get("company_number") or "") != exclude_company_number
    ]

    concurrent_watchlist = [c for c in watchlist_connections if c["is_current"]]

    return {
        "career_summary":        career_summary,
        "digital_background":    profile.get("digital_background", False),
        "digital_roles":         profile.get("digital_roles", []),
        "watchlist_connections": watchlist_connections,
        "client_connections":    client_connections,
        "concurrent_watchlist":  concurrent_watchlist,
    }


# ---------------------------------------------------------------------------
# FLAT STRINGS FOR CSV OUTPUT
# ---------------------------------------------------------------------------

def flatten_for_csv(intel: Dict) -> Dict[str, str]:
    """Convert a build_director_intelligence() result to flat CSV-safe strings."""

    def _conn_label(c: Dict, show_status: bool = True) -> str:
        status = ""
        if show_status:
            if c["is_current"]:
                status = ", current"
            elif c["resigned_on"]:
                status = f", until {c['resigned_on'][:7]}"
        return f"{c['company_name']} ({c['officer_role'].title()}{status})"

    client_str = " | ".join(
        _conn_label(c) for c in intel.get("client_connections", [])[:5]
    )
    watchlist_str = " | ".join(
        _conn_label(c) for c in intel.get("watchlist_connections", [])[:5]
    )
    concurrent_str = " | ".join(
        _conn_label(c, show_status=False) for c in intel.get("concurrent_watchlist", [])[:5]
    )

    return {
        "digital_background":    "Yes" if intel.get("digital_background") else "",
        "digital_roles":         "; ".join(intel.get("digital_roles", [])[:5]),
        "client_connections":    client_str,
        "watchlist_connections": watchlist_str,
        "concurrent_watchlist":  concurrent_str,
    }
