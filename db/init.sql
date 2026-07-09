-- partners table
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

CREATE INDEX IF NOT EXISTS idx_partners_subcategories_gin
    ON partners USING gin (to_tsvector('english', COALESCE(subcategories, '')));

CREATE INDEX IF NOT EXISTS idx_partners_name
    ON partners (partner_name);

-- ── Outreach sequence tracker ─────────────────────────────────────────────────
-- Tracks which touch (Day 1 / Day 3 / Day 12) was sent to each partner,
-- on which channel, and when. The scheduler reads this table daily to decide
-- which partners are due for their next touch.
CREATE TABLE IF NOT EXISTS outreach_sequence (
    id             SERIAL PRIMARY KEY,
    partner_id     INTEGER REFERENCES partners(id) ON DELETE CASCADE,
    partner_name   TEXT,
    touch_number   INTEGER NOT NULL,          -- 1 = Day 1, 2 = Day 3, 3 = Day 12
    channel        TEXT NOT NULL,             -- email | whatsapp | linkedin | voice
    digitisation   TEXT,                      -- Fully digitised | Semi-digitised | Un-digitised
    status         TEXT DEFAULT 'sent',       -- sent | failed | skipped
    note           TEXT,
    sent_at        TIMESTAMP DEFAULT now(),
    next_touch_due TIMESTAMP,                 -- pre-calculated when next touch is due
    created_at     TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outreach_seq_partner_id
    ON outreach_sequence (partner_id);

CREATE INDEX IF NOT EXISTS idx_outreach_seq_next_due
    ON outreach_sequence (next_touch_due)
    WHERE next_touch_due IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_outreach_seq_touch
    ON outreach_sequence (partner_id, touch_number);