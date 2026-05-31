"""
fuzzy_match_agents.py

Matches Lloyd's managing agents against the master firm list using fuzzy name
matching. Outputs candidates for manual review — does not modify any source file.
"""

import csv
import re
from pathlib import Path

from rapidfuzz import fuzz, process

# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------
LLOYDS_NAME_COLUMN = "company_name"    # Column in lloyds_agents.csv
LLOYDS_URL_COLUMN  = "url"             # Column in lloyds_agents.csv

FIRMS_NAME_COLUMN  = "Company Name"    # Column in firms.csv
FIRMS_CH_COLUMN    = "Company Number"  # Column in firms.csv

MATCH_THRESHOLD    = 90                # Scores below this are written as NO MATCH

# ---------------------------------------------------------------------------
# PATHS
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_DIR  = SCRIPT_DIR / "input"
OUTPUT_DIR = SCRIPT_DIR / "output"

LLOYDS_CSV = INPUT_DIR / "lloyds_agents.csv"
FIRMS_CSV  = INPUT_DIR / "firms.csv"
OUTPUT_CSV = OUTPUT_DIR / "lloyds_match_candidates.csv"

OUTPUT_COLUMNS = [
    "lloyds_name",
    "lloyds_url",
    "matched_firm_name",
    "matched_ch_number",
    "match_score",
    "confidence",
]


# Legal suffixes and Lloyd's-specific terms that add noise to matching
_STRIP = re.compile(
    r"\b(limited|ltd|llp|plc|lp|inc|managing agency|managing agent|"
    r"underwriters|underwriting|syndicate|syndicates|"
    r"insurance|reinsurance|group|holdings|uk)\b",
    re.IGNORECASE,
)
_WHITESPACE = re.compile(r"\s+")


def normalise(name: str) -> str:
    name = _STRIP.sub(" ", name)
    name = _WHITESPACE.sub(" ", name).strip().lower()
    return name


def confidence_band(score: float) -> str:
    if score >= 90:
        return "High"
    if score >= 70:
        return "Medium"
    return "Low"


def main() -> None:
    # Read inputs
    with LLOYDS_CSV.open(encoding="utf-8-sig") as fh:
        lloyds = list(csv.DictReader(fh))

    with FIRMS_CSV.open(encoding="utf-8-sig") as fh:
        firms = list(csv.DictReader(fh))

    print(f"Lloyd's agents : {len(lloyds)}")
    print(f"Master firms   : {len(firms)}")
    print()

    firm_names      = [r[FIRMS_NAME_COLUMN] for r in firms]
    firm_names_norm = [normalise(n) for n in firm_names]
    firm_by_name    = {r[FIRMS_NAME_COLUMN]: r[FIRMS_CH_COLUMN] for r in firms}

    results = []
    for row in lloyds:
        name      = row[LLOYDS_NAME_COLUMN].strip()
        url       = row.get(LLOYDS_URL_COLUMN, "").strip()
        name_norm = normalise(name)

        _, score, idx = process.extractOne(
            name_norm, firm_names_norm, scorer=fuzz.token_set_ratio
        )
        if score >= MATCH_THRESHOLD:
            match    = firm_names[idx]
            ch_num   = firm_by_name[match]
            band     = confidence_band(score)
        else:
            match  = "NO MATCH"
            ch_num = ""
            band   = "None"

        print(f"  [{band:<6}  {score:5.1f}]  {name}  ->  {match}")

        results.append({
            "lloyds_name":       name,
            "lloyds_url":        url,
            "matched_firm_name": match,
            "matched_ch_number": ch_num,
            "match_score":       round(score, 1),
            "confidence":        band,
        })

    # Sort: High first, then by score descending
    results.sort(key=lambda r: r["match_score"], reverse=True)

    OUTPUT_DIR.mkdir(exist_ok=True)
    with OUTPUT_CSV.open("w", newline="", encoding="utf-8-sig") as fh:
        writer = csv.DictWriter(fh, fieldnames=OUTPUT_COLUMNS)
        writer.writeheader()
        writer.writerows(results)

    matched   = sum(1 for r in results if r["confidence"] != "None")
    no_match  = sum(1 for r in results if r["confidence"] == "None")

    print()
    print(f"Matched      : {matched}")
    print(f"No match     : {no_match}")
    print(f"\nWrote {len(results)} rows -> {OUTPUT_CSV.name}")


if __name__ == "__main__":
    main()
