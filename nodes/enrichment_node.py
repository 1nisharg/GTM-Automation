"""
nodes/enrichment_node.py
------------------------
Enrichment Node — Stage 2 of the pipeline.

Purpose
-------
For each partner discovered in Stage 1, attempt to fill in any missing contact
fields (phone_number, email_id, linkedin_profile) using a strict prioritised
fallback chain:

    Priority 1   — Partners DB data (already in discovered_partners dict)
                   If the field is non-null and non-empty, use it directly.
    Priority 2   — Internal Database API  (enrichment_sources/database_query.py)
    Priority 2.3 — LinkedIn URL Finder    (enrichment_sources/linkedin_url_finder.py)
                   Tavily web search: ``"<name>" <category> <region> site:linkedin.com/company``
                   Resolves the company's LinkedIn page URL so Priority 2.5 can work
                   even when the DB has no ``company_linkedin_url`` column value.
    Priority 2.5 — LinkedIn Employee Scrape (enrichment_sources/linkedin_company_employees.py)
                   Scrapes the company LinkedIn page, filters for senior staff
                   (VP, Director, C-suite, etc.) and returns a real person's
                   profile URL — far better than a support@ inbox.
    Priority 3   — Hunter.io              (enrichment_sources/hunter.py)
    Priority 4   — Apollo.io              (enrichment_sources/apollo.py)
    Priority 5   — LinkedIn Sales Nav     (enrichment_sources/linkedin_sales_nav.py)

Rules
-----
- Per-field resolution: each contact field is resolved independently.
  e.g. email may come from Hunter while phone comes from Apollo.
- Stop early per field: as soon as a non-null, non-empty value is found,
  do not call lower-priority sources for that field.
- Failure tolerance: if all sources return null for a field, set it to None.
  Never raise an exception — return partial data and continue.
- Async + concurrent: all external source calls for a single partner are run
  concurrently via asyncio.gather for speed.

Extensibility
-------------
To add a new enrichment source:
  1. Create enrichment_sources/<new_source>.py with an async query function.
  2. Add it to enrichment_sources/__init__.py.
  3. Append it to the FALLBACK_CHAIN list below. That's it.
"""

import asyncio
import logging
import os
import time
from typing import Any

from enrichment_sources.apollo import query_apollo
from enrichment_sources.database_query import query_database
from enrichment_sources.hunter import query_hunter
from enrichment_sources.linkedin_company_employees import query_linkedin_employees
from enrichment_sources.linkedin_sales_nav import query_linkedin
from enrichment_sources.linkedin_url_finder import find_company_linkedin_url
from enrichment_sources.website_scraper import scrape_website, _is_generic_email
from state import GraphState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Concurrency cap — limits how many partners are enriched simultaneously.
# Set via env var ENRICH_CONCURRENCY (default 10).
# Raising this speeds things up but uses more API quota & DB connections.
# ---------------------------------------------------------------------------
_ENRICH_CONCURRENCY: int = int(os.getenv("ENRICH_CONCURRENCY", "10"))
_enrich_semaphore: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    """Return (or lazily create) the module-level enrichment semaphore."""
    global _enrich_semaphore
    if _enrich_semaphore is None:
        _enrich_semaphore = asyncio.Semaphore(_ENRICH_CONCURRENCY)
        logger.info(
            "Enrichment semaphore initialised: max %d concurrent partners.",
            _ENRICH_CONCURRENCY,
        )
    return _enrich_semaphore

# ---------------------------------------------------------------------------
# Fields we attempt to enrich.  Order matters for logging clarity only;
# the fallback chain logic below is field-agnostic.
# ---------------------------------------------------------------------------
ENRICHABLE_FIELDS = ("phone_number", "email_id", "linkedin_profile")

# Extra fields written by the LinkedIn employee scraper (not standard enrichment
# targets, but stored on the partner record for outreach personalisation).
# NOTE: the LinkedIn scraper's own field is named "contact_headline", but the
# DB column (and every other source's field) is "contact_title" — normalised
# to "contact_title" wherever it's read below, so it actually reaches the DB
# write-back instead of silently landing in an "contact_headline" key that
# _write_enriched_to_db() never looks at.
_LINKEDIN_BONUS_FIELDS = ("contact_name", "contact_headline")


def _is_present(value: Any) -> bool:
    """Return True if `value` is a non-null, non-whitespace-only string."""
    if value is None:
        return False
    return str(value).strip() != ""


async def _enrich_one_partner(partner: dict, run_id: str = "") -> dict:
    """
    Run the fallback chain for a single partner and return the enriched dict.
    Acquires the module-level semaphore so at most ENRICH_CONCURRENCY partners
    are processed simultaneously — essential for large partner lists.

    Parameters
    ----------
    partner : dict
        A single record from discovered_partners.
    run_id : str
        Optional pipeline run ID for log correlation.

    Returns
    -------
    dict
        Same dict with phone_number / email_id / linkedin_profile filled in
        where possible.  Unresolved fields are set to None.
    """
    prefix = f"[{run_id}] " if run_id else ""
    enriched = dict(partner)

    business_name: str = partner.get("partner_name", "") or ""
    detail: str = " ".join(
        filter(None, [partner.get("category", ""), partner.get("subcategories", "")])
    )

    # ------------------------------------------------------------------
    # Priority 1: check what's already in the DB record
    # ------------------------------------------------------------------
    fields_needed = [f for f in ENRICHABLE_FIELDS if not _is_present(partner.get(f))]

    if not fields_needed:
        logger.info("%sPartner %r: all fields already present — skipping enrichment.", prefix, business_name)
        return enriched

    logger.info(
        "%sPartner %r: needs enrichment for fields %s.",
        prefix, business_name, fields_needed,
    )

    async with _get_semaphore():
        await _enrich_one_partner_inner(enriched, partner, business_name, detail, fields_needed, prefix, run_id)
    return enriched


async def _enrich_one_partner_inner(
    enriched: dict,
    partner: dict,
    business_name: str,
    detail: str,
    fields_needed: list,
    prefix: str,
    run_id: str = "",
) -> None:
    """Inner enrichment logic — runs inside the semaphore."""
    t0 = time.monotonic()

    # ------------------------------------------------------------------
    # Priority 2–5: resolve company_linkedin_url first (sequential pre-step),
    # then fire all remaining sources concurrently.
    # ------------------------------------------------------------------

    # Step A — Resolve company LinkedIn page URL (Priority 2.3)
    company_linkedin_url: str = partner.get("company_linkedin_url") or ""

    if not company_linkedin_url:
        try:
            logger.info("%s  → [%s] Searching LinkedIn URL via Tavily…", prefix, business_name)
            url_result = await find_company_linkedin_url(
                partner_name=business_name,
                category=partner.get("category", "") or "",
                region=partner.get("region", "") or "",
                website=partner.get("website", "") or "",
            )
            company_linkedin_url = url_result.get("company_linkedin_url", "")
            if company_linkedin_url:
                enriched["company_linkedin_url"] = company_linkedin_url
                logger.info(
                    "%s  → [%s] LinkedIn URL found: %s",
                    prefix, business_name, company_linkedin_url,
                )
            else:
                logger.info("%s  → [%s] LinkedIn URL not found via Tavily.", prefix, business_name)
        except Exception as exc:
            logger.warning(
                "%s  → [%s] LinkedIn URL finder failed: %s",
                prefix, business_name, exc,
            )

    # Step B — Fire all remaining sources concurrently (Priority 2, 2.5, 3, 4, 5)
    logger.info(
        "%s  → [%s] Querying sources: apollo first (cross-feed to hunter), then database, linkedin, sales nav…",
        prefix, business_name,
    )
    # Defensive defaults — these are only otherwise assigned INSIDE the try
    # block below, after the gather() call succeeds. If gather itself raises
    # (caught by the except below), these would stay undefined and later
    # code referencing them (the contact_name/contact_title carry-through
    # further down) would raise NameError instead of degrading gracefully.
    _apollo_name = ""
    _contact_name_for_hunter = ""
    try:
        # Step 1: Run Apollo + LinkedIn + DB + Sales Nav concurrently
        # Apollo result is cross-fed into Hunter so Pass 2 can use the contact name
        (
            db_result,
            linkedin_emp_result,
            apollo_result,
            linkedin_result,
        ) = await asyncio.gather(
            query_database(business_name, detail),
            query_linkedin_employees(business_name, company_linkedin_url),
            query_apollo(business_name, run_id=run_id),
            query_linkedin(business_name),
            return_exceptions=True,
        )

        # Step 2: Extract contact name from Apollo for Hunter Pass 2
        _apollo_name = ""
        if isinstance(apollo_result, dict):
            _apollo_name = apollo_result.get("contact_name") or ""
        elif isinstance(apollo_result, Exception):
            apollo_result = {}

        _linkedin_emp_name = ""
        if isinstance(linkedin_emp_result, dict):
            _linkedin_emp_name = linkedin_emp_result.get("contact_name") or ""

        _contact_name_for_hunter = _apollo_name or _linkedin_emp_name

        # Step 3: Run Hunter with contact name AND known domain.
        # Domain is sourced from (in priority order):
        #   a. Partner's website field already in our DB
        #   b. Apollo's org_domain (discovered via org search)
        # Passing the domain makes Hunter search the EXACT right company
        # instead of guessing from the company name — this was the root
        # cause of Hunter returning emails from wrong companies (e.g. a
        # French "Café 26" instead of the UAE partner in our DB).
        _partner_website = partner.get("website") or ""
        _apollo_domain   = ""
        if isinstance(apollo_result, dict):
            _apollo_domain = apollo_result.get("org_domain") or ""

        # Clean to bare domain (strip https://www. etc)
        # Also rejects common spreadsheet placeholder values ("-", "n/a",
        # "none", "null", "tbd") that are non-empty strings but not real
        # domains. Without this, a website field of "-" would be passed
        # straight to Hunter/website_scraper as if it were a real domain,
        # wasting an API call and ~20s trying to fetch https://-.
        _PLACEHOLDER_VALUES = {
            "-", "--", "n/a", "na", "none", "null", "nil", "tbd", "unknown",
            "nan", "#n/a", "n\\a", ".", "..", "0", "x", "xx", "xxx",
        }

        def _to_bare_domain(raw: str) -> str:
            if not raw:
                return ""
            raw = raw.strip()
            if raw.lower() in _PLACEHOLDER_VALUES:
                return ""
            # Use startswith+slice NOT lstrip — lstrip strips individual chars
            # from a charset, so lstrip("https://") strips 't' from "triphabibi.com"
            for prefix in ("https://www.", "http://www.", "https://", "http://", "www."):
                if raw.startswith(prefix):
                    raw = raw[len(prefix):]
                    break
            bare = raw.rstrip("/").split("/")[0]
            # Final sanity check — a real domain must contain a dot and
            # at least 2 chars after the last dot (TLD), and no spaces.
            if "." not in bare or " " in bare or len(bare) < 4:
                return ""
            return bare

        _hunter_domain = _to_bare_domain(_partner_website) or _to_bare_domain(_apollo_domain) or ""

        if _hunter_domain:
            logger.info(
                "%s  → [%s] Passing domain=%r to Hunter (prevents wrong-company match)",
                prefix, business_name, _hunter_domain,
            )
            hunter_result = await query_hunter(
                business_name,
                contact_name=_contact_name_for_hunter or None,
                domain=_hunter_domain,
                run_id=run_id,
            )
        else:
            # No domain known — skip Hunter entirely.
            # Without a domain, Hunter searches by company name globally and
            # regularly returns emails from wrong companies in other countries
            # (e.g. "Pacific Adventures" → pacificadventures.mx in Mexico).
            # Better to have no email than a wrong-company email.
            logger.info(
                "%s  → [%s] Skipping Hunter — no domain available (prevents wrong-country match)",
                prefix, business_name,
            )
            hunter_result = {}

    except Exception as exc:
        logger.error("Unexpected error during gather for %r: %s", business_name, exc)
        db_result = linkedin_emp_result = hunter_result = apollo_result = linkedin_result = {}

    # Convert any exceptions returned by gather into empty dicts
    source_results = []
    for name, result in [
        ("database",         db_result),
        ("linkedin_emp",     linkedin_emp_result),
        ("hunter",           hunter_result),
        ("apollo",           apollo_result),
        ("linkedin",         linkedin_result),
    ]:
        if isinstance(result, Exception):
            logger.warning(
                "Partner %r: source %r raised %s — treating as empty.",
                business_name,
                name,
                result,
            )
            source_results.append({})
        else:
            source_results.append(result or {})

    db_res, linkedin_emp_res, hunter_res, apollo_res, linkedin_res = source_results

    # ------------------------------------------------------------------
    # Bonus: store contact_name / contact_title from LinkedIn scrape.
    # LinkedIn's own field is "contact_headline" — normalised to
    # "contact_title" here so it actually reaches the DB write-back
    # (which reads enriched["contact_title"], not "contact_headline").
    # These are metadata fields, not standard ENRICHABLE_FIELDS, so we
    # write them directly without going through the fallback loop.
    # ------------------------------------------------------------------
    if _is_present(linkedin_emp_res.get("contact_name")) and not _is_present(enriched.get("contact_name")):
        enriched["contact_name"] = linkedin_emp_res["contact_name"]
    if _is_present(linkedin_emp_res.get("contact_headline")) and not _is_present(enriched.get("contact_title")):
        enriched["contact_title"] = linkedin_emp_res["contact_headline"]

    # Walk fallback chain per field — with field-specific priority ordering.
    #
    # KEY DESIGN DECISION: email_id uses a DIFFERENT priority order than
    # phone/linkedin. Apollo is Priority 1 for email because it's the only
    # source that reveals real named-person emails (e.g. michael.burke@co.com).
    # Hunter is a strong second because it can construct personal emails from
    # a known name+domain. Generic sources (website scraper, linkedin) come last.
    #
    # For phone and linkedin: original order is fine — no source has a
    # particular advantage over others for those fields.
    #
    # Email quality gate: reject generic/shared inboxes and low-confidence
    # Hunter results regardless of source order.

    _EMAIL_SOURCE_ORDER = [
        ("apollo",       apollo_res),       # Priority 1 — verified revealed personal email
        ("linkedin_emp", linkedin_emp_res),  # Priority 2 — named contact from LinkedIn
        ("hunter",       hunter_res),        # Priority 3 — constructed from name+domain
        ("database",     db_res),            # Priority 4 — previously stored
        ("linkedin",     linkedin_res),      # Priority 5 — LinkedIn profile scrape
    ]

    _OTHER_SOURCE_ORDER = [
        ("database",     db_res),
        ("linkedin_emp", linkedin_emp_res),
        ("hunter",       hunter_res),
        ("apollo",       apollo_res),
        ("linkedin",     linkedin_res),
    ]

    resolved_fields = {}
    for field in fields_needed:
        resolved_value = None
        source_order = _EMAIL_SOURCE_ORDER if field == "email_id" else _OTHER_SOURCE_ORDER

        for source_name, source_data in source_order:
            candidate = source_data.get(field)
            if not _is_present(candidate):
                continue

            # ── Email quality gate ─────────────────────────────────────────
            if field == "email_id":
                # Block any generic/shared inbox address regardless of source
                if _is_generic_email(candidate):
                    logger.info(
                        "%s  → [%s] Rejecting generic email %r from %s — routing to WhatsApp",
                        prefix, business_name, candidate, source_name,
                    )
                    continue
                # For Apollo: log whether this was a revealed personal email
                if source_name == "apollo":
                    revealed = source_data.get("email_revealed", False)
                    logger.info(
                        "%s  → [%s] Apollo email %r (revealed=%s) — accepting as Priority 1",
                        prefix, business_name, candidate, revealed,
                    )
                # Block low-confidence or explicitly generic Hunter results
                if source_name == "hunter":
                    if source_data.get("email_type") == "generic":
                        logger.info(
                            "%s  → [%s] Rejecting Hunter generic-type email %r",
                            prefix, business_name, candidate,
                        )
                        continue
                    confidence = source_data.get("confidence", 100)
                    if confidence < 70 and source_data.get("email_type") != "constructed":
                        logger.info(
                            "%s  → [%s] Rejecting low-confidence Hunter email %r (score=%d)",
                            prefix, business_name, candidate, confidence,
                        )
                        continue

            enriched[field] = candidate
            resolved_value = candidate
            resolved_fields[field] = source_name
            break

        if resolved_value is None:
            enriched[field] = None  # explicit None — all sources exhausted
            if field == "email_id":
                logger.info(
                    "%s  → [%s] No quality email found — will route to WhatsApp/LinkedIn only",
                    prefix, business_name,
                )

    # ------------------------------------------------------------------
    # Carry through the NAME and TITLE of whoever the resolved email_id
    # actually belongs to. Previously, Apollo computed contact_name and
    # contact_title internally (used only to feed Hunter's Pass 2 email
    # construction, see _apollo_name above) and then discarded them —
    # the email address made it into the enriched record, but nobody
    # could tell who it belonged to. Fixed here: whichever source won
    # email_id, pull its matching name/title through too.
    # ------------------------------------------------------------------
    email_source = resolved_fields.get("email_id")

    if email_source == "apollo":
        if _is_present(apollo_res.get("contact_name")) and not _is_present(enriched.get("contact_name")):
            enriched["contact_name"] = apollo_res["contact_name"]
        if _is_present(apollo_res.get("contact_title")) and not _is_present(enriched.get("contact_title")):
            enriched["contact_title"] = apollo_res["contact_title"]

    elif email_source == "hunter":
        # Hunter doesn't return a name of its own — but if the winning
        # email was CONSTRUCTED using a name cross-fed from Apollo or the
        # LinkedIn scraper (_contact_name_for_hunter, built earlier in
        # this function), that name belongs to this exact email address.
        if _contact_name_for_hunter and not _is_present(enriched.get("contact_name")):
            enriched["contact_name"] = _contact_name_for_hunter
            # Attach whichever title came with that name, if any.
            if _apollo_name and _contact_name_for_hunter == _apollo_name:
                if _is_present(apollo_res.get("contact_title")) and not _is_present(enriched.get("contact_title")):
                    enriched["contact_title"] = apollo_res["contact_title"]
            elif _is_present(linkedin_emp_res.get("contact_headline")) and not _is_present(enriched.get("contact_title")):
                enriched["contact_title"] = linkedin_emp_res["contact_headline"]

    if enriched.get("contact_name") or enriched.get("contact_title"):
        logger.info(
            "%s  → [%s] Contact identified: name=%r title=%r (email source=%s)",
            prefix, business_name, enriched.get("contact_name"),
            enriched.get("contact_title"), email_source,
        )

    # ------------------------------------------------------------------
    # Carry through company_synopsis from Apollo's org data — independent
    # of which source won the email match, since the synopsis comes from
    # org resolution (organization/search), not from whoever's email
    # ultimately got used. Feeds voice_agent/engine.py's personalized
    # opening/pitch question; left empty if Apollo had no real
    # description on file (never fabricated — see apollo.py's
    # _build_synopsis docstring).
    # ------------------------------------------------------------------
    if _is_present(apollo_res.get("company_synopsis")) and not _is_present(enriched.get("company_synopsis")):
        enriched["company_synopsis"] = apollo_res["company_synopsis"]

    # ------------------------------------------------------------------
    # Priority 6: Website Scraper fallback
    # Runs only for fields still missing after the full chain.
    # Domain comes from Hunter's result — no CRM website URL needed.
    # ------------------------------------------------------------------
    still_missing = [f for f in fields_needed if not _is_present(enriched.get(f))]

    if still_missing:
        domain = (
            hunter_res.get("domain")
            or apollo_res.get("org_domain")
            or partner.get("website", "")
        )
        if domain:
            logger.info(
                "%s  → [%s] Website scraper fallback for fields=%s domain=%r",
                prefix, business_name, still_missing, domain,
            )
            try:
                scraped = await scrape_website(domain, still_missing)
                for field in still_missing:
                    if _is_present(scraped.get(field)):
                        candidate = scraped[field]
                        # Apply the same email quality gate to website scraper
                        # results — the scraper pulls whatever email appears on
                        # the contact page, which is almost always info@ or
                        # contact@. Reject those here, same as we do for Hunter.
                        if field == "email_id" and _is_generic_email(candidate):
                            logger.info(
                                "%s  → [%s] website_scraper found generic email %r — rejecting, routing to WhatsApp",
                                prefix, business_name, candidate,
                            )
                            continue
                        enriched[field] = candidate
                        resolved_fields[field] = "website_scraper"
                        logger.info(
                            "%s  → [%s] website_scraper filled %r from %r",
                            prefix, business_name, field, scraped.get("scraped_from"),
                        )
            except Exception as exc:
                logger.warning(
                    "%s  → [%s] Website scraper failed: %s", prefix, business_name, exc
                )
        else:
            logger.info(
                "%s  → [%s] Website scraper skipped — no domain available.",
                prefix, business_name,
            )

    elapsed = time.monotonic() - t0
    if resolved_fields:
        logger.info(
            "%s  ✓ [%s] Enriched in %.1fs — %s",
            prefix, business_name, elapsed,
            ", ".join(f"{k} via {v}" for k, v in resolved_fields.items()),
        )
    else:
        logger.info(
            "%s  ✗ [%s] No new data found in %.1fs (all sources empty).",
            prefix, business_name, elapsed,
        )


async def _clear_stale_generic_emails(partner_names: list[str]) -> int:
    """
    Null out any generic/shared inbox emails (info@, contact@, dubai@, etc.)
    already stored in the DB for the partners we're about to enrich.

    These emails may have been written by earlier pipeline runs before the
    quality gate was introduced. Clearing them here ensures the enrichment
    pipeline finds them missing and re-enriches with a real personal email
    rather than treating them as "already have email, skip".

    Returns count of emails cleared.
    """
    if not partner_names:
        return 0
    try:
        from db.connection import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE partners
                SET email_id = NULL
                WHERE partner_name = ANY($1)
                  AND email_id IS NOT NULL
                  AND (
                    split_part(email_id, '@', 1) ILIKE ANY(ARRAY[
                      'info','contact','hello','support','admin','sales',
                      'enquiries','enquiry','booking','bookings','reservations',
                      'reception','office','team','mail','general','service',
                      'services','help','noreply','no-reply','donotreply',
                      'marketing','pr','media','press','events','promotions',
                      'deals','offers','partnerships','partner','operations',
                      'ops','billing','finance','accounts','procurement',
                      'purchasing','contracts','legal','hr','recruitment',
                      'careers','jobs','concierge','guestrelations',
                      'guestservices','guest','experiences','activities','tours',
                      'checkin','checkout','frontdesk','complaints','feedback',
                      'reviews','dubai','uae','abu','abudhabi','sharjah',
                      'ajman','rak','fujairah','alain','ae','gcc','me',
                      'middleeast','gulf','webmaster','web','digital','online',
                      'website','tech','it','helpdesk','newsletter','subscribe',
                      'unsubscribe','postmaster','abuse','spam',
                      'mail','email','contactus','getintouch','reach',
                      'connect','query','queries'
                    ])
                    OR split_part(email_id, '@', 1) ~ '^[0-9]+$'
                    OR length(split_part(email_id, '@', 1)) <= 2
                  )
                """,
                partner_names,
            )
            # asyncpg returns "UPDATE N" — extract the count
            count = int(result.split()[-1]) if result else 0
            if count > 0:
                logger.info(
                    "Enrichment: cleared %d stale generic email(s) from DB before re-enriching.",
                    count,
                )
            return count
    except Exception as exc:
        logger.warning("Enrichment: stale email cleanup failed (non-critical): %s", exc)
        return 0


async def enrichment_node(state: GraphState) -> dict:
    """
    LangGraph node: enrich all discovered partners concurrently.
    At most ENRICH_CONCURRENCY (default 10) partners are processed
    simultaneously — controlled by the module-level semaphore.

    Parameters
    ----------
    state : GraphState
        Must contain `discovered_partners` (list[dict]).

    Returns
    -------
    dict
        Partial state update: {"enriched_partners": list[dict]}
    """
    discovered = state.get("discovered_partners", [])
    run_id: str = state.get("run_id", "")

    if not discovered:
        logger.warning("[%s] Enrichment node: no discovered partners to enrich.", run_id)
        return {"enriched_partners": []}

    # Clear any stale generic emails (info@, contact@, etc.) already in DB
    # for this batch before enrichment runs. This ensures partners that
    # previously got a generic email stored will be re-enriched to find
    # a real personal email rather than being skipped as "already complete".
    partner_names = [p.get("partner_name", "") for p in discovered if p.get("partner_name")]
    await _clear_stale_generic_emails(partner_names)

    total = len(discovered)
    logger.info(
        "[%s] Enrichment node: starting %d partners (concurrency cap: %d).",
        run_id, total, _ENRICH_CONCURRENCY,
    )
    t_start = time.monotonic()

    # Schedule all partners; semaphore inside _enrich_one_partner limits
    # actual concurrency to _ENRICH_CONCURRENCY at any moment.
    enriched_partners = await asyncio.gather(
        *[_enrich_one_partner(partner, run_id=run_id) for partner in discovered],
        return_exceptions=False,
    )

    elapsed = time.monotonic() - t_start
    filled = sum(
        1 for p in enriched_partners
        if any(p.get(f) for f in ("phone_number", "email_id", "linkedin_profile"))
    )
    logger.info(
        "[%s] Enrichment node: finished %d partners in %.1fs — %d/%d had contact data filled.",
        run_id, total, elapsed, filled, total,
    )

    # Write enriched contact fields back to Supabase
    await _write_enriched_to_db(list(enriched_partners), run_id=run_id)

    return {"enriched_partners": list(enriched_partners)}

# ---------------------------------------------------------------------------
# DB write-back helper
# ---------------------------------------------------------------------------

async def _write_enriched_to_db(enriched_partners: list, run_id: str = "") -> None:
    """
    Write enriched contact fields back to the partners table in Supabase.
    Updates phone_number, email_id, linkedin_profile, contact_name,
    contact_title, and company_synopsis — never overwrites non-contact
    fields. REQUIRES a company_synopsis column on partners (see the
    ALTER TABLE note flagged alongside this change).
    """
    from db.connection import get_pool

    prefix = f"[{run_id}] " if run_id else ""
    if not enriched_partners:
        return

    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            for partner in enriched_partners:
                name = partner.get("partner_name", "")
                if not name:
                    continue
                # Email quality gate at DB write — never store generic emails.
                # Also clear from the in-memory partner dict so the SSE
                # response shown to the frontend also reflects no email,
                # rather than showing a generic email in the UI even though
                # it was never saved to the DB.
                email_to_write = partner.get("email_id") or ""
                if email_to_write and _is_generic_email(email_to_write):
                    logger.warning(
                        "DB write-back: blocking generic email %r for %r from DB",
                        email_to_write, name,
                    )
                    email_to_write = ""
                    partner["email_id"] = None   # clear from in-memory dict too

                await conn.execute(
                    """
                    UPDATE partners SET
                        phone_number      = COALESCE(NULLIF($1, ''), phone_number),
                        email_id          = COALESCE(NULLIF($2, ''), email_id),
                        linkedin_profile  = COALESCE(NULLIF($3, ''), linkedin_profile),
                        contact_name      = COALESCE(NULLIF($5, ''), contact_name),
                        contact_title     = COALESCE(NULLIF($6, ''), contact_title),
                        company_synopsis  = COALESCE(NULLIF($7, ''), company_synopsis)
                    WHERE partner_name = $4
                    """,
                    partner.get("phone_number") or "",
                    email_to_write,
                    partner.get("linkedin_profile") or "",
                    name,
                    partner.get("contact_name") or "",
                    partner.get("contact_title") or "",
                    partner.get("company_synopsis") or "",
                )
        logger.info(
            "%sDB write-back: updated %d partners in Supabase.",
            prefix, len(enriched_partners),
        )
    except Exception as exc:
        logger.error("%sDB write-back failed: %s", prefix, exc)