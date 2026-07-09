"""
nodes/discovery_node.py
-----------------------
Discovery Node — Stage 1 of the pipeline.

Strategy
--------
AUTO-DETECT the input type, then run the appropriate query:

  MODE: "name"
    Input looks like a specific company name (e.g. "Al Rakha Tourism",
    "Mondee", "Desert Safari Adventures LLC").
    → Fuzzy ILIKE match on partner_name.
    → No Apollo prospecting (we're looking for one specific company).
    → Returns ALL matching partners regardless of status.

  MODE: "category"
    Input looks like a category/subcategory theme (e.g. "food activities",
    "Adventure & Extreme Sports", "desert safari").
    → Tag match + ILIKE on subcategories column (original behaviour).
    → Apollo prospecting gap-fill if DB results < threshold.
    → Only "Yet to Start" / "Partner Outreach" status partners.

Auto-detection heuristic
-------------------------
1. Try a fast name-match query against partner_name (ILIKE).
2. If it returns ≥ 1 result → mode = "name", return those results.
3. If it returns 0 results → mode = "category", run the subcategory query.
   This means:
   - "al rakha tourism"  → name match found → returns that specific company
   - "food activities"   → no name match → falls through to category search
   - "Mondee"            → name match found → returns Mondee specifically
   - "desert safari"     → likely no exact name match → category search
"""

import logging
import os

from db.connection import get_pool
from enrichment_sources.apollo import prospect_apollo
from state import GraphState

logger = logging.getLogger(__name__)

STATUSES_TO_ENRICH = ["Yet to Start", "Partner Outreach"]

# Minimum DB results before Apollo prospecting kicks in (category mode only)
_APOLLO_MIN_THRESHOLD: int = int(os.getenv("APOLLO_PROSPECTING_MIN", "10"))
# Max Apollo partners to add per run
_APOLLO_MAX_PROSPECT:  int = int(os.getenv("APOLLO_PROSPECTING_MAX", "30"))

# ── Query: name search ────────────────────────────────────────────────────────
# Searches partner_name with ILIKE — case-insensitive partial match.
# Returns ALL statuses (not restricted to "Yet to Start") because if you
# search for a specific company you want to see it regardless of its pipeline
# status — even if already enriched or onboarded.
_NAME_SEARCH_QUERY = """
SELECT
    id,
    partner_name,
    digitisation,
    category,
    subcategories,
    subcategory_tags,
    website,
    product_count,
    status,
    integrated,
    region,
    phone_number,
    email_id,
    linkedin_profile,
    sheet_source
FROM partners
WHERE partner_name ILIKE $1
ORDER BY
    -- Exact prefix match scores highest (e.g. "Al Rakha" → "Al Rakha Tourism" first)
    CASE WHEN lower(partner_name) = lower($2) THEN 0
         WHEN lower(partner_name) LIKE lower($2) || '%' THEN 1
         ELSE 2
    END,
    partner_name;
"""

# ── Query: category search (original behaviour) ───────────────────────────────
_CATEGORY_SEARCH_QUERY = """
SELECT
    id,
    partner_name,
    digitisation,
    category,
    subcategories,
    subcategory_tags,
    website,
    product_count,
    status,
    integrated,
    region,
    phone_number,
    email_id,
    linkedin_profile,
    sheet_source
FROM partners
WHERE status = ANY($1)
  AND (
      $2 = ANY(subcategory_tags)          -- exact tag match (preferred)
      OR subcategories ILIKE $3           -- fallback for untagged rows
      OR category ILIKE $3               -- also search the category column
  )
ORDER BY
    ($2 = ANY(subcategory_tags)) DESC,   -- exact matches first
    sheet_source,
    partner_name;
"""

_UPSERT_APOLLO_PARTNER = """
INSERT INTO partners
    (partner_name, category, subcategories, website, status,
     digitisation, region, phone_number, email_id, linkedin_profile, sheet_source)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (partner_name, sheet_source) DO UPDATE SET
    category         = EXCLUDED.category,
    subcategories    = EXCLUDED.subcategories,
    website          = COALESCE(EXCLUDED.website, partners.website),
    phone_number     = COALESCE(NULLIF(EXCLUDED.phone_number, ''), partners.phone_number),
    email_id         = COALESCE(NULLIF(EXCLUDED.email_id,    ''), partners.email_id),
    linkedin_profile = COALESCE(NULLIF(EXCLUDED.linkedin_profile,''), partners.linkedin_profile)
RETURNING id, partner_name;
"""


async def discovery_node(state: GraphState) -> dict:
    """
    LangGraph node: discover partners using auto-detected search mode.

    Flow:
      1. Try name match first (fast ILIKE on partner_name)
      2. If name match returns results → "name" mode, return them
      3. If name match returns nothing → "category" mode, run subcategory query
      4. In category mode: if results < threshold → Apollo prospecting gap-fill
      5. Merge + deduplicate and return

    Returns
    -------
    dict: {"discovered_partners": list[dict], "search_mode": str}
    """
    raw_input   = state["input_category"].strip()
    run_id      = state.get("run_id", "")
    prefix      = f"[{run_id}] " if run_id else ""
    like_pattern = f"%{raw_input}%"

    pool = await get_pool()

    # ── Step 1: Try name match first ──────────────────────────────────────
    logger.info(
        "%sDiscovery: input=%r — trying name match first…",
        prefix, raw_input,
    )

    async with pool.acquire() as conn:
        name_rows = await conn.fetch(
            _NAME_SEARCH_QUERY,
            like_pattern,   # $1 — ILIKE pattern
            raw_input,      # $2 — for exact/prefix scoring
        )

    # ── Step 2: Auto-detect mode based on name match results ──────────────
    if name_rows:
        # Found partners matching this as a name — use name mode
        db_partners  = [dict(row) for row in name_rows]
        search_mode  = "name"
        logger.info(
            "%sDiscovery: NAME MODE — %r matched %d partner(s) by name.",
            prefix, raw_input, len(db_partners),
        )
        # In name mode: no Apollo prospecting, return immediately
        return {
            "discovered_partners": db_partners,
            "search_mode": search_mode,
        }

    # ── Step 3: No name match — fall through to category search ───────────
    search_mode = "category"
    logger.info(
        "%sDiscovery: No name match for %r — falling through to CATEGORY MODE.",
        prefix, raw_input,
    )

    async with pool.acquire() as conn:
        cat_rows = await conn.fetch(
            _CATEGORY_SEARCH_QUERY,
            STATUSES_TO_ENRICH,  # $1
            raw_input,           # $2 — exact tag match
            like_pattern,        # $3 — ILIKE pattern
        )

    db_partners = [dict(row) for row in cat_rows]
    logger.info(
        "%sDiscovery: CATEGORY MODE — %r matched %d partner(s).",
        prefix, raw_input, len(db_partners),
    )

    # ── Step 4: Apollo prospecting gap-fill (category mode only) ──────────
    apollo_partners: list[dict] = []

    if len(db_partners) < _APOLLO_MIN_THRESHOLD:
        logger.info(
            "%sDiscovery: DB results (%d) below threshold (%d) — running Apollo prospecting for %r.",
            prefix, len(db_partners), _APOLLO_MIN_THRESHOLD, raw_input,
        )
        try:
            apollo_partners = await prospect_apollo(
                category=raw_input,
                region="UAE",
                max_companies=_APOLLO_MAX_PROSPECT,
                run_id=run_id,
            )
            logger.info(
                "%sDiscovery: Apollo prospecting found %d partners.",
                prefix, len(apollo_partners),
            )
        except Exception as exc:
            logger.warning("%sDiscovery: Apollo prospecting failed: %s", prefix, exc)
            apollo_partners = []

        # ── Step 5: Upsert Apollo partners to DB ──────────────────────────
        if apollo_partners:
            try:
                async with pool.acquire() as conn:
                    upserted = 0
                    for p in apollo_partners:
                        await conn.fetchrow(
                            _UPSERT_APOLLO_PARTNER,
                            p.get("partner_name", ""),
                            p.get("category", raw_input),
                            p.get("subcategories", raw_input),
                            p.get("website", ""),
                            "Yet to Start",
                            p.get("digitisation", "Semi-digitised"),
                            p.get("region", "Local"),
                            p.get("phone_number", "") or "",
                            p.get("email_id", "") or "",
                            p.get("linkedin_profile", "") or "",
                            "apollo_prospecting",
                        )
                        upserted += 1
                logger.info(
                    "%sDiscovery: upserted %d Apollo partners to DB.",
                    prefix, upserted,
                )
            except Exception as exc:
                logger.error("%sDiscovery: Apollo upsert failed: %s", prefix, exc)

    # ── Step 6: Merge + deduplicate ───────────────────────────────────────
    seen_names: set[str] = {p.get("partner_name", "").lower() for p in db_partners}
    new_from_apollo = []

    for p in apollo_partners:
        name_lower = p.get("partner_name", "").lower()
        if name_lower and name_lower not in seen_names:
            seen_names.add(name_lower)
            new_from_apollo.append(p)

    discovered = db_partners + new_from_apollo

    logger.info(
        "%sDiscovery: total %d partners (%d DB + %d new from Apollo). mode=%s",
        prefix, len(discovered), len(db_partners), len(new_from_apollo), search_mode,
    )

    return {
        "discovered_partners": discovered,
        "search_mode": search_mode,
    }