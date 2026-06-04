"""
db/models.py
------------
Defines the PostgreSQL schema for the `partners` table, which is the
database representation of the GTM UAE Excel workbook.

Sheet mapping
-------------
  Excel sheet "Track 1 Db"  →  rows with sheet_source = 'track1'
  Excel sheet "Track 2 Db"  →  rows with sheet_source = 'track2'

Column mapping (Excel → PostgreSQL)
------------------------------------
  Sr. No / (row #)      → id               INTEGER
  Partner Name          → partner_name     TEXT
  Digitisation          → digitisation     TEXT
  Category / Categories → category         TEXT
  Subcategories / Sub Categories → subcategories TEXT
  Website               → website          TEXT
  Product Content / Product Count → product_count INTEGER
  Status                → status           TEXT
  Integrated            → integrated       TEXT
  Region                → region           TEXT
  Phone number          → phone_number     TEXT
  Email ID              → email_id         TEXT
  Linkedin profile      → linkedin_profile TEXT
  (derived)             → sheet_source     TEXT  ('track1' | 'track2')

DDL
---
Run the SQL below against your PostgreSQL database to create the table.
This file does NOT execute the DDL automatically — the connection pool in
connection.py is used for queries only.  Apply the DDL manually or via a
migration tool of your choice.
"""

# ---------------------------------------------------------------------------
# DDL — apply once to your PostgreSQL database
# ---------------------------------------------------------------------------

CREATE_PARTNERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS partners (
    id              SERIAL PRIMARY KEY,
    partner_name    TEXT,
    digitisation    TEXT,
    category        TEXT,
    subcategories   TEXT,       -- may be comma-separated, e.g. "Adventure & Extreme Sports, Hiking"
    website         TEXT,
    product_count   INTEGER,
    status          TEXT,       -- e.g. "Yet to Start", "Partner Outreach", "Fully Onboarded"
    integrated      TEXT,       -- "Yes" | "No"
    region          TEXT,       -- e.g. "Local", "International"
    phone_number    TEXT,
    email_id        TEXT,
    linkedin_profile TEXT,
    sheet_source    TEXT        -- 'track1' | 'track2'
);
"""

# ---------------------------------------------------------------------------
# Index to speed up the most common query pattern (discovery_node.py):
#   WHERE status = 'Yet to Start'
#     AND subcategories ILIKE '%<input_category>%'
# ---------------------------------------------------------------------------

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_partners_status
    ON partners (status);

CREATE INDEX IF NOT EXISTS idx_partners_subcategories
    ON partners USING gin (to_tsvector('english', subcategories));
"""

# ---------------------------------------------------------------------------
# Normalised column name → Python dict key mapping used across the codebase
# ---------------------------------------------------------------------------

COLUMN_KEYS = [
    "partner_name",
    "digitisation",
    "category",
    "subcategories",
    "website",
    "product_count",
    "status",
    "integrated",
    "region",
    "phone_number",
    "email_id",
    "linkedin_profile",
    "sheet_source",
]

# Status value used as the discovery filter
STATUS_TO_ENRICH = "Yet to Start"
