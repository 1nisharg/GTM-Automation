"""
scripts/migrate_excel_to_db.py
-------------------------------
One-time migration: load both Excel sheets into the PostgreSQL `partners` table.

Usage
-----
# Locally (with .env configured):
    python scripts/migrate_excel_to_db.py

# Via Docker (recommended — DB_HOST is set to 'postgres' automatically):
    docker compose run --rm pipeline python scripts/migrate_excel_to_db.py

# Options:
    --excel   Path to Excel file  (default: "GTM UAE_ Track 1 & 2 Db.xlsx" at repo root)
    --clear   Drop and reload all rows before inserting (default: skip duplicates)

Excel → PostgreSQL column mapping
----------------------------------
Both sheets share the same output columns but have different source column names:

  Sheet             | Source col name     | DB column
  Track 1 Db        | Partner Name        | partner_name
  Track 2 Db        | Partner name        | partner_name
  Both              | Digitisation        | digitisation
  Track 1 Db        | Category            | category
  Track 2 Db        | Categories          | category
  Track 1 Db        | Subcategories       | subcategories
  Track 2 Db        | Sub Categories      | subcategories
  Both              | Website             | website
  Track 1 Db        | Product Content     | product_count
  Track 2 Db        | Product Count       | product_count
  Both              | Status              | status
  Both              | Integrated          | integrated
  Both              | Region              | region
  Both              | Phone number        | phone_number
  Both              | Email ID            | email_id
  Both              | Linkedin profile    | linkedin_profile
  (derived)         | —                   | sheet_source  ('track1' | 'track2')
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ── Make repo root importable when run from scripts/ ─────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ── Default Excel path ────────────────────────────────────────────────────────
REPO_ROOT = Path(__file__).parent.parent
DEFAULT_EXCEL_PATH = REPO_ROOT / "GTM UAE_ Track 1 & 2 Db.xlsx"

# ── Column rename maps ────────────────────────────────────────────────────────
# Maps each sheet's source column names → normalised DB column names.
# Columns not in this map are dropped.

TRACK1_COLUMN_MAP = {
    "Partner Name":    "partner_name",
    "Digitisation":    "digitisation",
    "Category":        "category",
    "Subcategories":   "subcategories",
    "Website":         "website",
    "Product Content": "product_count",
    "Status":          "status",
    "Integrated":      "integrated",
    "Region":          "region",
    "Phone number":    "phone_number",
    "Email ID":        "email_id",
    "Linkedin profile":"linkedin_profile",
}

TRACK2_COLUMN_MAP = {
    "Partner name":    "partner_name",
    "Digitisation":    "digitisation",
    "Categories":      "category",
    "Sub Categories":  "subcategories",
    "Website":         "website",
    "Product Count":   "product_count",
    "Status":          "status",
    "Integrated":      "integrated",
    "Region":          "region",
    "Phone number":    "phone_number",
    "Email ID":        "email_id",
    "Linkedin profile":"linkedin_profile",
}

# ── SQL statements ─────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO partners (
    partner_name, digitisation, category, subcategories, website,
    product_count, status, integrated, region,
    phone_number, email_id, linkedin_profile, sheet_source
)
VALUES (
    %(partner_name)s, %(digitisation)s, %(category)s, %(subcategories)s, %(website)s,
    %(product_count)s, %(status)s, %(integrated)s, %(region)s,
    %(phone_number)s, %(email_id)s, %(linkedin_profile)s, %(sheet_source)s
)
ON CONFLICT DO NOTHING;
"""

TRUNCATE_SQL = "TRUNCATE TABLE partners RESTART IDENTITY;"

# DDL — run idempotently before every migration so the script works even if
# Docker's init.sql was never executed (e.g. volume already existed).
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS partners (
    id               SERIAL PRIMARY KEY,
    partner_name     TEXT,
    digitisation     TEXT,
    category         TEXT,
    subcategories    TEXT,
    website          TEXT,
    product_count    INTEGER,
    status           TEXT,
    integrated       TEXT,
    region           TEXT,
    phone_number     TEXT,
    email_id         TEXT,
    linkedin_profile TEXT,
    sheet_source     TEXT
);

CREATE INDEX IF NOT EXISTS idx_partners_status
    ON partners (status);

CREATE INDEX IF NOT EXISTS idx_partners_name
    ON partners (partner_name);
"""


# ── Helpers ────────────────────────────────────────────────────────────────────

def _ensure_schema(conn) -> None:
    """
    Create the partners table and indexes if they don't already exist.
    Safe to call on every run — all statements use IF NOT EXISTS.

    This makes the migration self-contained: it works whether or not Docker's
    init.sql was executed (i.e., regardless of the volume's initialisation state).
    """
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    logger.info("Schema verified / created (partners table ready).")

def _clean_value(val):
    """Normalise a cell value: strip whitespace, convert empty/NaN → None."""
    if val is None:
        return None
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s else None


def _load_sheet(excel_path: Path, sheet_name: str, col_map: dict, source_tag: str) -> list[dict]:
    """
    Read one Excel sheet, rename + filter columns, and return a list of row dicts
    ready for insertion.
    """
    logger.info("Reading sheet %r from %s …", sheet_name, excel_path.name)
    df = pd.read_excel(excel_path, sheet_name=sheet_name)

    # Keep only columns we care about (ignore unmapped ones silently)
    available_cols = {c: col_map[c] for c in col_map if c in df.columns}
    missing = set(col_map) - set(available_cols)
    if missing:
        logger.warning("Sheet %r: expected columns not found — %s", sheet_name, missing)

    df = df[list(available_cols.keys())].rename(columns=available_cols)
    df["sheet_source"] = source_tag

    # Ensure all target columns exist (fill with None if a source col was missing)
    for col in ["partner_name", "digitisation", "category", "subcategories", "website",
                "product_count", "status", "integrated", "region",
                "phone_number", "email_id", "linkedin_profile"]:
        if col not in df.columns:
            df[col] = None

    # Coerce product_count to int (Excel stores as float due to NaN)
    df["product_count"] = pd.to_numeric(df["product_count"], errors="coerce").astype("Int64")

    # Clean all values
    rows = []
    for _, row in df.iterrows():
        cleaned = {col: _clean_value(row[col]) for col in df.columns}
        # Convert pandas NA integer back to plain Python None / int
        if cleaned["product_count"] is not None:
            try:
                cleaned["product_count"] = int(cleaned["product_count"])
            except (ValueError, TypeError):
                cleaned["product_count"] = None
        rows.append(cleaned)

    logger.info("Sheet %r: %d rows loaded.", sheet_name, len(rows))
    return rows


def _get_db_conn():
    """Create a psycopg2 connection using DB_* env vars."""
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "gtm_uae"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def _insert_rows(conn, rows: list[dict], batch_size: int = 500) -> int:
    """Insert rows in batches. Returns total rows attempted."""
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cur.executemany(INSERT_SQL, batch)
            # psycopg2 returns -1 for executemany rowcount; use batch length instead
            total += len(batch)
            logger.info("  … processed batch %d–%d (%d rows so far)", i + 1, i + len(batch), total)
    conn.commit()
    return total


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Migrate Excel → PostgreSQL partners table")
    parser.add_argument(
        "--excel",
        type=Path,
        default=DEFAULT_EXCEL_PATH,
        help=f"Path to the Excel file (default: {DEFAULT_EXCEL_PATH})",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Truncate the partners table before inserting (full reload). "
             "Default: skip duplicate rows (ON CONFLICT DO NOTHING).",
    )
    args = parser.parse_args()

    if not args.excel.exists():
        logger.error("Excel file not found: %s", args.excel)
        logger.error(
            "Place the file at the repo root or pass --excel /path/to/file.xlsx"
        )
        sys.exit(1)

    # Load both sheets
    track1_rows = _load_sheet(args.excel, "Track 1 Db", TRACK1_COLUMN_MAP, "track1")
    track2_rows = _load_sheet(args.excel, "Track 2 Db", TRACK2_COLUMN_MAP, "track2")
    all_rows = track1_rows + track2_rows
    logger.info("Total rows to insert: %d", len(all_rows))

    # Connect and insert
    logger.info("Connecting to PostgreSQL at %s:%s/%s …",
                os.getenv("DB_HOST", "localhost"),
                os.getenv("DB_PORT", "5432"),
                os.getenv("DB_NAME", "gtm_uae"))

    conn = _get_db_conn()
    try:
        # Always ensure the schema exists before touching data
        _ensure_schema(conn)

        if args.clear:
            with conn.cursor() as cur:
                cur.execute(TRUNCATE_SQL)
            conn.commit()
            logger.info("Table truncated — performing full reload.")

        total = _insert_rows(conn, all_rows)
        logger.info("✅ Migration complete. %d rows processed into `partners`.", total)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
