"""
enrichment_sources/hunter.py
-----------------------------
Hunter.io enrichment source — Priority 3 in the fallback chain.

Strategy (Two-Pass)
-------------------
Pass 1 — Domain Search:
    Query Hunter's domain-search by company name.
    Filter out generic/catch-all emails (info@, contact@, etc.).
    Pick the best personal email by department + confidence score.

Pass 2 — Email Finder (runs only if Pass 1 yields no personal email):
    If a contact name is available (from CRM or Apollo), use Hunter's
    email-finder endpoint to construct + verify a personal email
    using the domain's known email pattern.

Return schema
-------------
{
    "email_id":    str,           # the selected email address
    "email_type":  "personal"     # verified personal email
                 | "constructed"  # pattern-matched via email-finder
                 | "generic",     # info@/contact@ fallback
    "confidence":  int,           # Hunter confidence score (0–100)
    "domain":      str,           # company domain found by Hunter
    "source":      "hunter_domain_search" | "hunter_email_finder"
}
Returns {} on any error or if nothing usable is found.

Environment variable required: HUNTER_API_KEY
Docs: https://hunter.io/api-documentation
"""

import logging
import os

import httpx

# Use the SAME comprehensive generic-email detector as website_scraper.py
# and enrichment_node.py — single source of truth. Hunter previously had
# its own smaller, stale ~20-entry list here that missed location prefixes
# (dubai@, uae@) and department inboxes (marketing@, pr@), which could
# cause Hunter to log "Pass 1 success" on an email that the outer gate in
# enrichment_node.py then silently rejected two log lines later.
from enrichment_sources.website_scraper import _is_generic_email as _is_generic

logger = logging.getLogger(__name__)

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")

_DOMAIN_SEARCH_URL = "https://api.hunter.io/v2/domain-search"
_EMAIL_FINDER_URL  = "https://api.hunter.io/v2/email-finder"

_MIN_CONFIDENCE = 70  # Below this, Hunter's email is a guess — not usable

# ── Daily rate limit (GTM Head requirement, 2026-06-29) ─────────────────────
# Global cap on Hunter API calls per day, across all partners and pipeline
# runs. Independent of Hunter's own plan quota — this is an operational
# guardrail we control. Reads from api_usage table so it survives restarts.
_DAILY_LIMIT: int = int(os.getenv("HUNTER_DAILY_LIMIT", "10"))


async def _check_daily_limit() -> bool:
    """
    Return True if Hunter has made fewer than _DAILY_LIMIT calls today,
    globally. Queries the api_usage table (not in-memory) so the limit
    holds across server restarts.
    """
    try:
        from db.connection import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM api_usage
                WHERE api_name = 'hunter'
                  AND called_at >= CURRENT_DATE
            """)
        if count >= _DAILY_LIMIT:
            logger.warning(
                "Hunter DAILY LIMIT REACHED: %d/%d calls used today. "
                "Skipping further Hunter calls until tomorrow.",
                count, _DAILY_LIMIT,
            )
            return False
        return True
    except Exception as exc:
        # api_usage table may not exist yet, or DB unreachable — fail safe
        # by allowing the call rather than blocking the whole pipeline.
        logger.debug("Hunter daily limit check failed (allowing call): %s", exc)
        return True


async def _log_hunter_usage(
    operation: str,
    partner_name: str = "",
    run_id: str = "",
    success: bool = True,
    result: str = "",
) -> None:
    """Log a Hunter API call to api_usage for daily-limit tracking and audit."""
    try:
        from db.connection import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO api_usage
                    (run_id, partner_name, api_name, operation,
                     success, result, request_cost)
                VALUES ($1, $2, 'hunter', $3, $4, $5, 1)
                """,
                run_id or "", partner_name or "",
                operation, success, result or "",
            )
    except Exception as exc:
        logger.debug("Hunter usage log failed (non-critical): %s", exc)

# Departments to prioritise for decision-maker contacts
_PREFERRED_DEPARTMENTS = [
    "executive",
    "management",
    "sales",
    "business development",
]

# _GENERIC_PREFIXES and local _is_generic() removed — now imported from
# website_scraper.py as _is_generic (see top of file) for a single,
# comprehensive, consistently-maintained list.


def _pick_best_personal(emails: list) -> tuple[str | None, int]:
    """
    Select the best personal (non-generic) email from a domain-search result.

    Priority:
      1. Verified (confidence ≥ 70), non-generic, preferred department
      2. Verified, non-generic, highest confidence
      3. Any non-generic email (unverified)

    Returns (email, confidence) or (None, 0).
    """
    if not emails:
        return None, 0

    verified     = [e for e in emails if e.get("confidence", 0) >= _MIN_CONFIDENCE]
    personal_v   = [e for e in verified if not _is_generic(e.get("value", ""))]
    personal_any = [e for e in emails   if not _is_generic(e.get("value", ""))]

    # Priority 1 — verified personal from a preferred department
    for dept in _PREFERRED_DEPARTMENTS:
        for e in personal_v:
            if dept in (e.get("department") or "").lower():
                return e["value"], e.get("confidence", 0)

    # Priority 2 — any verified personal, highest confidence
    if personal_v:
        best = max(personal_v, key=lambda e: e.get("confidence", 0))
        return best["value"], best.get("confidence", 0)

    # Priority 3 — non-generic but unverified
    if personal_any:
        best = max(personal_any, key=lambda e: e.get("confidence", 0))
        return best["value"], best.get("confidence", 0)

    return None, 0


# _pick_generic_fallback removed — generic emails (info@, contact@, dubai@)
# cause bounces and damage sender reputation. Never use them for outreach.


# ── Main Query ─────────────────────────────────────────────────────────────────

async def query_hunter(
    business_name: str,
    first_name: str | None = None,
    last_name: str | None = None,
    contact_name: str | None = None,  # full name from Apollo — auto-split for Pass 2
    domain: str | None = None,        # partner's known domain — searched directly
                                       # instead of letting Hunter guess from company name
    run_id: str = "",                 # pipeline run ID — for daily-limit tracking/audit
) -> dict:
    """
    Query Hunter.io for the best contact email for a given business.

    Parameters
    ----------
    business_name : str
        The company name — used ONLY as fallback if domain is not provided.
    first_name : str, optional
        Contact first name for Pass 2 Email Finder.
    last_name : str, optional
        Contact last name for Pass 2 Email Finder.
    contact_name : str, optional
        Full name from Apollo — auto-split into first/last for Pass 2.
    domain : str, optional
        The partner's known website domain (e.g. "secretfoodtours.com").
        When provided, Hunter searches this domain DIRECTLY instead of
        looking up the company by name — this prevents Hunter from matching
        a wrong company (e.g. "Café 26" matching cdg26.fr in France instead
        of the UAE partner we actually want to reach).

    Returns
    -------
    dict — see module docstring for schema. Returns {} on failure.
    """
    # Auto-split contact_name into first/last if not explicitly provided
    if contact_name and not (first_name and last_name):
        parts = contact_name.strip().split()
        if len(parts) >= 2:
            first_name = parts[0]
            last_name  = parts[-1]
    if not HUNTER_API_KEY:
        logger.warning("HUNTER_API_KEY not set — skipping Hunter for %r.", business_name)
        return {}

    if not business_name:
        return {}

    if not await _check_daily_limit():
        logger.warning("Hunter daily limit (%d/day) reached — skipping %r.", _DAILY_LIMIT, business_name)
        return {}

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:

            # ── Pass 1: Domain Search ──────────────────────────────────────
            # PRIORITY: if we know the partner's domain, search by domain
            # directly. This bypasses Hunter's company-name lookup which
            # can match the wrong company (e.g. "Café 26" matching a French
            # company cdg26.fr instead of our UAE partner). Domain search is
            # also more reliable and returns more emails per query.
            #
            # If no domain known, fall back to company name search as before.
            # Fix: use startswith+slice NOT lstrip — lstrip strips individual
            # chars from a charset, corrupting domains like "triphabibi.com"
            # → "riphabibi.com" because 't' is in the "https://" charset.
            _raw_domain = (domain or "").strip()
            for _pfx in ("https://www.", "http://www.", "https://", "http://", "www."):
                if _raw_domain.startswith(_pfx):
                    _raw_domain = _raw_domain[len(_pfx):]
                    break
            _clean_domain = _raw_domain.rstrip("/").split("/")[0]

            if _clean_domain:
                logger.info(
                    "Hunter Pass 1: searching by DOMAIN %r for %r (bypasses name-based company lookup).",
                    _clean_domain, business_name,
                )
                resp = await client.get(
                    _DOMAIN_SEARCH_URL,
                    params={
                        "domain":  _clean_domain,
                        "api_key": HUNTER_API_KEY,
                        "limit":   10,
                        "type":    "personal",
                    },
                )
            else:
                logger.info(
                    "Hunter Pass 1: no domain known — searching by company name %r.",
                    business_name,
                )
                resp = await client.get(
                    _DOMAIN_SEARCH_URL,
                    params={
                        "company": business_name,
                        "api_key": HUNTER_API_KEY,
                        "limit":   10,
                        "type":    "personal",
                    },
                )

            resp.raise_for_status()
            data = resp.json()

            errors = data.get("errors")
            if errors:
                logger.warning("Hunter domain-search errors for %r: %s", business_name, errors)
                return {}

            domain_data  = data.get("data", {})
            found_domain = domain_data.get("domain") or _clean_domain or ""
            emails       = domain_data.get("emails", [])

            # Normalise: use the domain Hunter confirmed, or the one we passed in
            domain = found_domain

            logger.info(
                "Hunter Pass 1: domain=%r, %d emails for %r (searched_by=%s).",
                domain, len(emails), business_name,
                "domain" if _clean_domain else "company_name",
            )

            personal_email, confidence = _pick_best_personal(emails)

            if personal_email:
                logger.info(
                    "Hunter Pass 1 success: %r (confidence=%d) for %r.",
                    personal_email, confidence, business_name,
                )
                await _log_hunter_usage(
                    "domain_search", business_name, run_id,
                    success=True, result=personal_email,
                )
                return {
                    "email_id":   personal_email,
                    "email_type": "personal",
                    "confidence": confidence,
                    "domain":     domain or "",
                    "source":     "hunter_domain_search",
                }

            logger.info(
                "Hunter Pass 1: no personal email found for %r — trying Email Finder.",
                business_name,
            )

            # ── Pass 2: Email Finder (requires name + domain) ──────────────
            if domain and first_name and last_name:
                finder_resp = await client.get(
                    _EMAIL_FINDER_URL,
                    params={
                        "domain":      domain,
                        "first_name":  first_name,
                        "last_name":   last_name,
                        "api_key":     HUNTER_API_KEY,
                    },
                )
                finder_resp.raise_for_status()
                finder_data = finder_resp.json().get("data", {})

                constructed = finder_data.get("email")
                score       = finder_data.get("score", 0)

                logger.info(
                    "Hunter Pass 2: email=%r score=%d for %r.",
                    constructed, score, business_name,
                )

                if constructed and score >= _MIN_CONFIDENCE:
                    logger.info(
                        "Hunter Pass 2 success: %r (score=%d) for %r.",
                        constructed, score, business_name,
                    )
                    await _log_hunter_usage(
                        "email_finder", business_name, run_id,
                        success=True, result=constructed,
                    )
                    return {
                        "email_id":   constructed,
                        "email_type": "constructed",
                        "confidence": score,
                        "domain":     domain or "",
                        "source":     "hunter_email_finder",
                    }

                logger.info(
                    "Hunter Pass 2: score %d below threshold for %r — skipping.",
                    score, business_name,
                )
            else:
                logger.info(
                    "Hunter Pass 2 skipped for %r — missing domain=%r or name (%r %r).",
                    business_name, domain, first_name, last_name,
                )

            # Generic fallback REMOVED intentionally.
            # Sending outreach to info@, contact@, dubai@ etc. causes bounces
            # and damages sender reputation. If no personal email found,
            # return domain only so Apollo/website_scraper can try.
            # The outreach channel decision engine will route to WhatsApp instead.
            logger.info(
                "Hunter: no personal email found for %r — returning domain only.",
                business_name,
            )
            await _log_hunter_usage(
                "domain_search", business_name, run_id,
                success=False, result="no personal email found",
            )
            return {"domain": domain or ""} if domain else {}

    except httpx.HTTPStatusError as exc:
        logger.error("Hunter HTTP %d for %r: %s", exc.response.status_code, business_name, exc)
        return {}
    except Exception as exc:
        logger.error("Hunter unexpected error for %r: %s", business_name, exc)
        return {}