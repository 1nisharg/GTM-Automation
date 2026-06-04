"""
db/init.sql
-----------
DDL executed automatically by the PostgreSQL Docker container on first startup.
Mounted via: ./db/init.sql → /docker-entrypoint-initdb.d/01_init.sql

This file is only run once — when the named Docker volume is empty (fresh DB).
To re-run it, destroy the volume: docker compose down -v
"""

-- ── partners table ──────────────────────────────────────────────────────────
-- Stores all rows from both Excel sheets (Track 1 Db + Track 2 Db).
-- sheet_source distinguishes rows: 'track1' | 'track2'

CREATE TABLE IF NOT EXISTS partners (
    id               SERIAL PRIMARY KEY,
    partner_name     TEXT,
    digitisation     TEXT,
    category         TEXT,
    subcategories    TEXT,        -- may be comma-separated, e.g. "Adventure & Extreme Sports, Hiking"
    website          TEXT,
    product_count    INTEGER,
    status           TEXT,        -- e.g. "Yet to Start", "Partner Outreach", "Fully Onboarded"
    integrated       TEXT,        -- "Yes" | "No"
    region           TEXT,        -- e.g. "Local", "International"
    phone_number     TEXT,
    email_id         TEXT,
    linkedin_profile TEXT,
    sheet_source     TEXT         -- 'track1' | 'track2'
);

-- ── Indexes for the most common query patterns ───────────────────────────────

-- discovery_node: WHERE status = 'Yet to Start'
CREATE INDEX IF NOT EXISTS idx_partners_status
    ON partners (status);

-- discovery_node: WHERE subcategories ILIKE '%<category>%'
CREATE INDEX IF NOT EXISTS idx_partners_subcategories_gin
    ON partners USING gin (to_tsvector('english', COALESCE(subcategories, '')));

-- General lookup by name (enrichment sources, deduplication)
CREATE INDEX IF NOT EXISTS idx_partners_name
    ON partners (partner_name);
