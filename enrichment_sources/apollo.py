"""
enrichment_sources/apollo.py
-----------------------------
Apollo.io enrichment source — Priority 4 in the fallback chain.

Key constraints:
- Max 5 contacts per company (APOLLO_MAX_CONTACTS_PER_COMPANY env var)
- Title filter: only the 14 decision-maker roles defined by GTM team
- Credit logging: every API call logged to apollo_usage table in Supabase
- Rate limiter: 45 req/min (Basic plan: 50/min hard limit)
- Retry with exponential backoff on 429 / 5xx
- Phone reveal + email reveal via /people/match
- Returns contact_name + contact_title for Hunter Pass 2 cross-feed
- Returns org_domain for website scraper fallback

Credit cost per operation (Apollo Basic):
  org_search    → 1 credit
  people_search → 1 credit per page
  email_reveal  → 1 credit per reveal
  phone_reveal  → 1 credit per reveal

Environment variable required: APOLLO_API_KEY
Optional:
  APOLLO_RPM                    — requests/min cap (default 45)
  APOLLO_MAX_CONTACTS_PER_COMPANY — max people to fetch per org (default 5)
  APOLLO_MONTHLY_CREDIT_CAP     — hard stop threshold (default 2520)
  APOLLO_CREDIT_WARNING_PCT     — warn at this % of cap (default 80)
"""

import asyncio
import logging
import os
import re
import time
from urllib.parse import urlparse

import httpx

logger = logging.getLogger(__name__)

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")

_ORG_SEARCH_URL    = "https://api.apollo.io/api/v1/organizations/search"
_PEOPLE_SEARCH_URL = "https://api.apollo.io/api/v1/mixed_people/api_search"
_PEOPLE_MATCH_URL  = "https://api.apollo.io/api/v1/people/match"

# ── Credit config ──────────────────────────────────────────────────────────
_MONTHLY_CAP:     int = int(os.getenv("APOLLO_MONTHLY_CREDIT_CAP",     "2520"))
_WARNING_PCT:     int = int(os.getenv("APOLLO_CREDIT_WARNING_PCT",     "80"))
_MAX_PER_COMPANY: int = int(os.getenv("APOLLO_MAX_CONTACTS_PER_COMPANY", "5"))

# ── Daily rate limit (GTM Head requirement, 2026-06-29) ─────────────────────
# 10 Apollo API calls per day, GLOBAL across all partners and all pipeline
# runs. This is separate from the monthly credit cap above — the daily
# limit is a hard operational guardrail, not a billing concern.
_DAILY_LIMIT: int = int(os.getenv("APOLLO_DAILY_LIMIT", "10"))

# ═══════════════════════════════════════════════════════════════════════════
# TITLE MATCHING — designation-wise AND vertical-wise, word-boundary aware.
# ═══════════════════════════════════════════════════════════════════════════
# Replaces naive substring matching, which had two confirmed failure modes
# against real Apollo data:
#   1. FALSE NEGATIVE — "Director of Sales" was REJECTED (no exact phrase
#      "sales manager" present), despite being a genuine senior sales
#      decision-maker.
#   2. FALSE POSITIVE — "Assistant General Manager" and "Sales Manager
#      Assistant" INCORRECTLY PASSED, because the substring "general
#      manager" / "sales manager" happens to appear inside a junior title.
#
# New approach — three independent checks, in order:
#   (a) JUNIOR EXCLUSION — reject outright if any junior/support word is
#       present as a whole word.
#   (b) OWNERSHIP TIER — accept on seniority alone (Founder/CEO/MD/GM),
#       regardless of department.
#   (c) VERTICAL MATCH — accept only if BOTH a business-function word AND
#       a seniority word are present as whole words — this is what
#       correctly catches "Director of Sales", "Head of Marketing", "VP
#       Partnerships", "Sales Manager" in any word order, while still
#       rejecting bare "Manager", "Operations Manager", "Finance Manager",
#       "HR Manager", "Project Manager" (none of our vertical words appear
#       in those).
# ═══════════════════════════════════════════════════════════════════════════

_OWNERSHIP_TITLES = [
    "founder", "co-founder", "cofounder",
    "owner", "co-owner", "proprietor",
    "ceo", "chief executive officer", "chief executive",
    "managing director",
    "managing partner",
    "general manager",   # acceptable — Apollo queries are scoped to
                          # 1-200 employee orgs, so a GM at that size IS
                          # the senior decision-maker.
]

_TARGET_VERTICALS = [
    "sales", "marketing", "business development", "partnership", "partnerships",
    "commercial", "contracting", "channel", "distribution", "reservations",
    "product",
]

_SENIORITY_WORDS = [
    "director", "vp", "vice president", "head", "lead", "manager", "executive",
]

# "Executive" is deliberately NOT a generic seniority word usable with any
# vertical — per explicit decision, we do not broaden "X Executive" across
# the board (e.g. "Sales Executive" must NOT pass). The ONLY place the
# original GTM-approved 14-role list uses an "Executive" suffix is
# "Contracting Manager / Executive" — so that one exact compound phrase is
# special-cased here, checked before the generic vertical+seniority path,
# rather than making "executive" a broadly reusable seniority word.
_EXPLICIT_APPROVED_PHRASES = [
    "contracting executive",
]

_JUNIOR_EXCLUSIONS = [
    "intern", "trainee", "apprentice", "assistant", "student",
    "junior", "deputy", "associate", "coordinator",
]

# "Assistant Director" is a real, senior hospitality/hotel management title
# (Director of Sales -> Assistant Director of Sales -> Sales Manager is a
# standard hotel org structure) -- NOT the same as "Sales Assistant" or
# "Assistant Manager", which genuinely are junior/support roles. Without
# this carve-out, the generic "assistant" exclusion above incorrectly
# rejected "Assistant Director of Sales" -- confirmed via real Apollo data
# at a 360-employee resort where this exact title appeared twice among
# just 7 visible people, clearly the standard title, not an anomaly.
_SENIOR_ASSISTANT_PHRASES = [
    "assistant director",
]

# Broader keyword set sent to Apollo's server-side person_titles filter.
# Deliberately wider than a fixed exact-phrase list — server-side filtering
# is itself fuzzy, so sending vertical words alone pulls in more real
# candidates (Directors, VPs, Heads), not just literal "Sales Manager"
# matches. _is_target_title() is still applied client-side afterward as
# the final, precise authority.
_APOLLO_TITLE_KEYWORDS = [
    "founder", "owner", "ceo", "managing director", "general manager",
    "managing partner",
    "sales", "marketing", "business development", "partnership",
    "commercial", "contracting", "channel", "distribution",
    "reservations", "product",
]


def _is_target_title(title: str) -> bool:
    """
    Return True only if this title is a real GTM-approved decision-maker —
    designation-wise (seniority) AND vertical-wise (business function) —
    with word-boundary matching so embedded substrings never cause a false
    match (e.g. "Manager" inside "Management Assistant").
    """
    t = (title or "").lower().strip()
    if not t:
        return False

    def _has_word(word: str) -> bool:
        return re.search(rf"\b{re.escape(word)}\b", t) is not None

    is_senior_assistant = any(phrase in t for phrase in _SENIOR_ASSISTANT_PHRASES)

    for j in _JUNIOR_EXCLUSIONS:
        if _has_word(j):
            # "assistant" alone would normally reject this title -- but if
            # it's specifically "assistant director" (a real senior hotel/
            # hospitality management title), don't reject on that word.
            # Any OTHER junior word (intern, trainee, deputy, coordinator,
            # etc.) still rejects immediately regardless.
            if j == "assistant" and is_senior_assistant:
                continue
            return False

    if any(_has_word(k) for k in _OWNERSHIP_TITLES):
        return True

    # Explicit exact-phrase exceptions (currently: "Contracting Executive")
    # — checked before the generic vertical+seniority rule so this specific
    # approved compound phrase passes without making "executive" a
    # generally reusable seniority word for other verticals.
    if any(phrase in t for phrase in _EXPLICIT_APPROVED_PHRASES):
        return True

    has_vertical  = any(_has_word(v) for v in _TARGET_VERTICALS)
    has_seniority = any(_has_word(s) for s in _SENIORITY_WORDS)
    return has_vertical and has_seniority


# Seniority score for ranking multiple qualifying contacts at one company.
# Pattern-based (seniority word x vertical bonus) rather than a fixed
# phrase-lookup dict, so real-world phrasings like "Director of Sales" or
# "VP Partnerships" still rank sensibly against "Sales Manager" etc.
_OWNERSHIP_SCORES = {
    "founder": 100, "co-founder": 100, "cofounder": 100,
    "owner": 95, "co-owner": 92, "proprietor": 90,
    "ceo": 90, "chief executive officer": 90, "chief executive": 90,
    "managing director": 88,
    "managing partner": 82,
    "general manager": 85,
}

# ── Vertical priority tiers ─────────────────────────────────────────────
# Directly encodes the GTM team's originally specified priority order:
#   Business Development / Partnerships  >  Sales / Commercial  >
#   Contracting  >  Channel / Distribution  >  Reservations  >  Product  >
#   Marketing (lowest — explicitly last in the original list).
#
# DESIGN NOTE: tier is the DOMINANT signal (multiplied by 8), seniority
# word is only a small tie-breaker (max +5) within the SAME tier. This
# guarantees the vertical priority order can never be inverted just
# because two titles use different seniority words (e.g. "Marketing
# Head" scoring above "Product Manager" purely because "Head" > "Manager"
# — that was a real bug in an earlier version of this scoring, confirmed
# by test: tier-based separation makes it structurally impossible.
_VERTICAL_TIER = {
    "business development": 9, "partnership": 9, "partnerships": 9,
    "sales": 8, "commercial": 8,
    "contracting": 7,
    "channel": 6, "distribution": 6,
    "reservations": 5,
    "product": 4,
    "marketing": 3,
}

_SENIORITY_BONUS = {
    "director": 5, "vp": 5, "vice president": 5,
    "head": 4, "lead": 3,
    "manager": 2, "executive": 1,
}

# Fixed score for the one explicit "Executive"-suffixed exception (see
# _EXPLICIT_APPROVED_PHRASES above). Placed between Contracting Manager
# (7*8+2=58) and Channel/Distribution Manager (6*8+2=50), preserving both
# "executive < manager" within Contracting itself, and Contracting's
# overall priority above Channel/Distribution.
_EXPLICIT_PHRASE_SCORES = {
    "contracting executive": 55,
}


def _score_title(title: str) -> int:
    """
    Score a title for ranking multiple qualifying contacts at one company.
    Ownership titles score highest (fixed values). Vertical roles score as
    (tier * 8) + small seniority tie-breaker — tier is dominant so the
    GTM team's priority order between departments can never be inverted
    by which seniority word (Manager/Head/Director) happens to be used.
    """
    t = (title or "").lower().strip()
    if not t:
        return 0

    def _has_word(word: str) -> bool:
        return re.search(rf"\b{re.escape(word)}\b", t) is not None

    for phrase, score in _OWNERSHIP_SCORES.items():
        if _has_word(phrase):
            return score

    for phrase, score in _EXPLICIT_PHRASE_SCORES.items():
        if phrase in t:
            return score

    tier = 0
    for vertical, v_tier in _VERTICAL_TIER.items():
        if _has_word(vertical) and v_tier > tier:
            tier = v_tier

    if tier == 0:
        return 0  # no recognised vertical — shouldn't reach here if
                  # _is_target_title() already gated it, but safe.

    seniority_bonus = 0
    for word, bonus in _SENIORITY_BONUS.items():
        if _has_word(word) and bonus > seniority_bonus:
            seniority_bonus = bonus

    return tier * 8 + seniority_bonus


# ── Rate limiter ───────────────────────────────────────────────────────────
_APOLLO_RPM: int    = int(os.getenv("APOLLO_RPM", "45"))
_apollo_lock        = asyncio.Lock()
_apollo_calls: list = []


async def _rate_gate() -> None:
    async with _apollo_lock:
        now = time.monotonic()
        while _apollo_calls and now - _apollo_calls[0] > 60:
            _apollo_calls.pop(0)
        if len(_apollo_calls) >= _APOLLO_RPM:
            sleep_for = 60 - (now - _apollo_calls[0]) + 0.1
            logger.info("Apollo rate gate: sleeping %.1fs", sleep_for)
            await asyncio.sleep(sleep_for)
        _apollo_calls.append(time.monotonic())


# ── Credit tracker ─────────────────────────────────────────────────────────

_session_credits: dict = {
    "org_search":    0,
    "people_search": 0,
    "email_reveal":  0,
    "phone_reveal":  0,
    "prospecting":   0,
    "total":         0,
}


async def _log_credit(
    operation: str,
    credits_used: int = 1,
    partner_name: str = "",
    run_id: str = "",
    result_fields: list | None = None,
    success: bool = True,
    error_msg: str = "",
) -> None:
    """Log an Apollo credit usage event to Supabase. Non-blocking."""
    _session_credits[operation] = _session_credits.get(operation, 0) + credits_used
    _session_credits["total"]   = _session_credits.get("total", 0) + credits_used

    total = _session_credits["total"]
    warning_threshold = int(_MONTHLY_CAP * _WARNING_PCT / 100)

    if total >= _MONTHLY_CAP:
        logger.error(
            "Apollo CREDIT CAP REACHED: %d/%d credits used this session. "
            "Stopping further API calls.",
            total, _MONTHLY_CAP,
        )
    elif total >= warning_threshold:
        logger.warning(
            "Apollo credit warning: %d/%d credits used (%d%% of monthly cap).",
            total, _MONTHLY_CAP, int(total / _MONTHLY_CAP * 100),
        )

    try:
        from db.connection import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO apollo_usage
                    (run_id, partner_name, operation, credits_used,
                     result_fields, success, error_msg)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                """,
                run_id or "",
                partner_name or "",
                operation,
                credits_used,
                result_fields or [],
                success,
                error_msg or "",
            )
    except Exception as exc:
        logger.debug("Apollo credit log failed (non-critical): %s", exc)


def _check_credit_cap() -> bool:
    return _session_credits.get("total", 0) < _MONTHLY_CAP


async def _check_daily_limit() -> bool:
    """Return True if Apollo has made fewer than _DAILY_LIMIT calls today."""
    try:
        from db.connection import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM apollo_usage
                WHERE created_at >= CURRENT_DATE
            """)
        if count >= _DAILY_LIMIT:
            logger.warning(
                "Apollo DAILY LIMIT REACHED: %d/%d calls used today. "
                "Skipping further Apollo calls until tomorrow.",
                count, _DAILY_LIMIT,
            )
            return False
        return True
    except Exception as exc:
        logger.debug("Apollo daily limit check failed (allowing call): %s", exc)
        return True


# ── Retry helper ───────────────────────────────────────────────────────────

async def _post_with_retry(
    client: httpx.AsyncClient,
    url: str,
    headers: dict,
    payload: dict,
    retries: int = 3,
) -> dict | None:
    for attempt in range(retries):
        await _rate_gate()
        try:
            resp = await client.post(url, headers=headers, json=payload)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = 2 ** attempt * 15
                logger.warning("Apollo 429 — backing off %.0fs (attempt %d)", wait, attempt + 1)
                await asyncio.sleep(wait)
                continue
            if resp.status_code >= 500:
                wait = 2 ** attempt * 5
                logger.warning("Apollo %d — backing off %.0fs", resp.status_code, wait)
                await asyncio.sleep(wait)
                continue
            if 400 <= resp.status_code < 500:
                # Log the full error body for ANY 4xx client error — not just
                # 422. This exact gap (400 had no body logging, only 422 did)
                # is why Path 4c's rejection reason was invisible on the
                # first failure and had to be diagnosed blind. Never again —
                # any payload Apollo rejects now shows its actual reason.
                try:
                    body = resp.json()
                    logger.warning(
                        "Apollo %d for %s — payload rejected: %s | full body: %s",
                        resp.status_code, url,
                        body.get("error") or body.get("message") or "no error field",
                        str(body)[:500],
                    )
                except Exception:
                    logger.warning(
                        "Apollo %d for %s — could not parse error body. Raw: %s",
                        resp.status_code, url, resp.text[:500],
                    )
                return None
            logger.warning("Apollo HTTP %d for %s", resp.status_code, url)
            return None
        except Exception as exc:
            logger.error("Apollo request error: %s", exc)
            if attempt < retries - 1:
                await asyncio.sleep(2 ** attempt * 3)
    return None


# ── Helpers ────────────────────────────────────────────────────────────────

def _is_masked(email: str | None) -> bool:
    if not email:
        return False
    local = email.split("@")[0]
    return "***" in local or local.strip("*") == ""


def _clean_domain(raw: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    parsed = urlparse(raw)
    domain = parsed.netloc or parsed.path.split("/")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def _extract_phone(person: dict) -> str | None:
    phones = person.get("phone_numbers") or []
    for phone in phones:
        number = phone.get("sanitized_number") or phone.get("raw_number")
        if number:
            return number
    return None


# Apollo's organization/search response can carry a real description under
# any of these field names depending on plan/version — try them in order.
_ORG_DESCRIPTION_FIELDS = ["short_description", "description", "seo_description"]
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")


def _build_synopsis(org: dict, max_sentences: int = 3, max_chars: int = 320) -> str:
    """
    Build a short, factual company synopsis from Apollo's OWN organization
    data — never invented. Used to personalize outreach calls (see
    voice_agent/engine.py's company_synopsis param) instead of asking
    generic questions.

    Deliberately returns "" (not a fallback guess) when Apollo has no real
    description on file — the voice agent degrades gracefully to
    category-only personalization in that case rather than receiving a
    fabricated synopsis that could misrepresent the business.
    """
    if not org:
        return ""

    raw = ""
    for field in _ORG_DESCRIPTION_FIELDS:
        val = (org.get(field) or "").strip()
        if val:
            raw = val
            break

    if not raw:
        return ""

    sentences = _SENTENCE_SPLIT_RE.split(raw)
    trimmed = " ".join(sentences[:max_sentences]).strip()

    if len(trimmed) > max_chars:
        trimmed = trimmed[:max_chars].rsplit(" ", 1)[0].rstrip(",.;: ") + "."

    return trimmed


# ── Main enrichment query ──────────────────────────────────────────────────

async def query_apollo(
    business_name: str,
    run_id: str = "",
) -> dict:
    """
    Query Apollo.io for the best decision-maker contact at a given business.
    See module header for the full designation + vertical matching design.
    """
    if not APOLLO_API_KEY:
        logger.warning("APOLLO_API_KEY not set — skipping Apollo for %r.", business_name)
        return {}
    if not business_name:
        return {}
    if not _check_credit_cap():
        logger.error("Apollo credit cap reached — skipping %r.", business_name)
        return {}
    if not await _check_daily_limit():
        logger.warning("Apollo daily limit (%d/day) reached — skipping %r.", _DAILY_LIMIT, business_name)
        return {}

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": APOLLO_API_KEY,
    }

    try:
        async with httpx.AsyncClient(timeout=20.0) as client:

            # ── Step 1: Organisation search (1 credit) ─────────────────────
            org_data = await _post_with_retry(
                client, _ORG_SEARCH_URL, headers,
                {
                    "q_organization_name": business_name,
                    "organization_locations": ["United Arab Emirates", "Dubai", "UAE"],
                    "page":     1,
                    "per_page": 3,
                },
            )

            if not (org_data or {}).get("organizations"):
                logger.info(
                    "Apollo: no UAE org found for %r — retrying without location filter.",
                    business_name,
                )
                org_data = await _post_with_retry(
                    client, _ORG_SEARCH_URL, headers,
                    {"q_organization_name": business_name, "page": 1, "per_page": 3},
                )

            await _log_credit(
                "org_search", 1, business_name, run_id,
                success=bool(org_data),
                error_msg="" if org_data else "no response",
            )

            org_id     = None
            org_domain = None
            company_synopsis = ""

            if org_data:
                orgs = org_data.get("organizations") or []
                if orgs:
                    best_org = orgs[0]
                    for candidate in orgs:
                        cand_domain = (
                            candidate.get("primary_domain") or
                            candidate.get("website_url") or ""
                        ).lower()
                        cand_name = (candidate.get("name") or "").lower()
                        core_words = [w for w in business_name.lower().split()
                                      if len(w) > 3 and w not in
                                      ("the", "and", "for", "llc", "ltd", "fze", "co")]
                        domain_match = any(w in cand_domain for w in core_words)
                        name_match   = any(w in cand_name   for w in core_words)
                        if domain_match or name_match:
                            best_org = candidate
                            break

                    org        = best_org
                    org_id     = org.get("id")
                    raw_domain = org.get("primary_domain") or org.get("website_url") or ""
                    org_domain = _clean_domain(raw_domain)
                    company_synopsis = _build_synopsis(org)
                    logger.info(
                        "Apollo: org found — id=%s domain=%r name=%r synopsis=%r for %r.",
                        org_id, org_domain, org.get("name"), bool(company_synopsis), business_name,
                    )

            if not org_id and not org_domain:
                logger.info(
                    "Apollo: no org entity found for %r — proceeding to "
                    "people search with free-text employer-name matching "
                    "instead of giving up.",
                    business_name,
                )

            # ── Step 2: People search — designation + vertical filtered ───
            # Server-side person_titles filter re-enabled (proven to work
            # on this endpoint via prospect_apollo below). Falls back to no
            # filter, then domain retry, then a page-2 retry if page 1
            # yields nobody who passes the client-side gate.
            async def _run_people_search(page: int, with_title_filter: bool = True) -> dict | None:
                payload: dict = {
                    "page":                   page,
                    "page_size":              min(_MAX_PER_COMPANY * 3, 25),
                    "reveal_personal_emails": True,
                    "reveal_phone_number":    True,
                }
                if with_title_filter:
                    payload["person_titles"] = _APOLLO_TITLE_KEYWORDS
                if org_id:
                    payload["organization_ids"] = [org_id]
                elif org_domain:
                    payload["organization_domains"] = [org_domain]
                else:
                    payload["q_keywords"] = business_name
                return await _post_with_retry(client, _PEOPLE_SEARCH_URL, headers, payload)

            people_data = await _run_people_search(page=1, with_title_filter=True)

            if not (people_data or {}).get("people"):
                logger.info(
                    "Apollo: server-side title filter returned nothing for "
                    "%r — retrying without it (client-side gate still applies).",
                    business_name,
                )
                people_data = await _run_people_search(page=1, with_title_filter=False)

            if not (people_data or {}).get("people") and org_domain and org_id:
                logger.info(
                    "Apollo: org_id search returned no people for %r — "
                    "retrying with domain filter.",
                    business_name,
                )
                domain_payload: dict = {
                    "page":                   1,
                    "page_size":              min(_MAX_PER_COMPANY * 3, 25),
                    "organization_domains":   [org_domain],
                    "reveal_personal_emails": True,
                    "reveal_phone_number":    True,
                    "person_titles":          _APOLLO_TITLE_KEYWORDS,
                }
                people_data = await _post_with_retry(
                    client, _PEOPLE_SEARCH_URL, headers, domain_payload,
                )

            await _log_credit(
                "people_search", 1, business_name, run_id,
                success=bool(people_data),
                error_msg="" if people_data else "no response / 422",
            )

            people = (people_data or {}).get("people") or []
            people = [p for p in people if _is_target_title(p.get("title") or "")]

            # Page-2 retry — larger institutions may have their qualifying
            # decision-maker beyond page 1.
            if not people:
                logger.info(
                    "Apollo: no qualifying contact on page 1 for %r — trying page 2.",
                    business_name,
                )
                page2_data = await _run_people_search(page=2, with_title_filter=True)
                await _log_credit(
                    "people_search", 1, business_name, run_id,
                    success=bool(page2_data),
                    error_msg="" if page2_data else "no response (page 2)",
                )
                page2_people = (page2_data or {}).get("people") or []
                people = [p for p in page2_people if _is_target_title(p.get("title") or "")]

            people = people[:_MAX_PER_COMPANY]

            if not people:
                logger.info(
                    "Apollo: no target-title contacts found for %r (cap=%d).",
                    business_name, _MAX_PER_COMPANY,
                )
                if org_domain or company_synopsis:
                    return {"org_domain": org_domain or "", "company_synopsis": company_synopsis}
                return {}

            logger.info(
                "Apollo: %d target-title contacts found for %r (cap=%d).",
                len(people), business_name, _MAX_PER_COMPANY,
            )

            # ── Step 3: Rank by title — VERTICAL FIRST, OWNERSHIP FALLBACK ──
            # Explicit priority change: a Sales/Partnerships/Marketing/
            # Commercial/BD person (or any other vertical match) is now
            # PREFERRED over Founder/Owner/CEO/MD/GM/Managing Partner —
            # the reverse of the previous ranking, where Ownership always
            # scored highest and so was always picked first when both
            # existed at the same company. Rationale: for a partnership
            # inquiry, a dedicated Sales/BD/Partnerships contact is
            # usually the actually-correct point of contact — the
            # Founder/CEO is now only contacted as a FALLBACK when the
            # company has no such dedicated role at all.
            #
            # Every candidate in `people` has already passed
            # _is_target_title() above, so each is guaranteed to be either
            # an ownership-tier match or a vertical/explicit-phrase match —
            # bucket by checking ownership-tier membership only.
            def _is_ownership_candidate(title: str) -> bool:
                t = (title or "").lower().strip()
                return any(
                    re.search(rf"\b{re.escape(k)}\b", t) is not None
                    for k in _OWNERSHIP_TITLES
                )

            vertical_candidates  = [p for p in people if not _is_ownership_candidate(p.get("title") or "")]
            ownership_candidates = [p for p in people if _is_ownership_candidate(p.get("title") or "")]

            if vertical_candidates:
                vertical_candidates.sort(key=lambda p: _score_title(p.get("title") or ""), reverse=True)
                people = vertical_candidates + ownership_candidates
                logger.info(
                    "Apollo: %d vertical-matched candidate(s) found for %r — "
                    "preferring these over %d ownership-tier candidate(s).",
                    len(vertical_candidates), business_name, len(ownership_candidates),
                )
            else:
                ownership_candidates.sort(key=lambda p: _score_title(p.get("title") or ""), reverse=True)
                people = ownership_candidates
                logger.info(
                    "Apollo: no vertical-matched candidate for %r — falling "
                    "back to ownership tier (%d candidate(s)).",
                    business_name, len(ownership_candidates),
                )

            best  = people[0]
            _first = best.get("first_name") or ""
            _last  = best.get("last_name") or ""
            _combined_name = f"{_first} {_last}".strip()
            _raw_name      = best.get("name") or ""

            # Prefer whichever source gives a MORE COMPLETE name — either
            # can be incomplete on its own. Real case that exposed this:
            # Apollo's "name" field returned just "Harriet" (first name
            # only) for a Sales Manager whose first_name/last_name fields
            # were separately populated as "Harriet"/"Buyinza". The old
            # logic (`best.get("name") or combined`) always used the
            # incomplete "name" field first via the `or` chain, silently
            # discarding the fuller first+last combination — which then
            # caused the name+domain reveal fallback (both email Step 4b
            # and phone Step 5) to skip entirely, since both require a
            # real first AND last name (len(parts) >= 2) to construct the
            # /people/match request.
            if len(_combined_name.split()) > len(_raw_name.split()):
                name = _combined_name
            else:
                name = _raw_name or _combined_name

            title  = best.get("title") or ""
            email  = best.get("email")

            logger.info(
                "Apollo: best candidate — name=%r title=%r email=%r masked=%s for %r.",
                name, title, email, _is_masked(email), business_name,
            )

            # ── Step 4: Email reveal (1 credit) ───────────────────────────
            # Three reveal paths, tried in order:
            #   4a. Via linkedin_url (most precise when available)
            #   4b. Via first_name + last_name + organization_name + domain
            #       — mirrors Step 5's phone reveal approach.
            #   4c. Via Apollo person ID + organization_name + domain —
            #       covers cases where the search response has neither a
            #       LinkedIn URL nor a complete name. First attempt sent
            #       bare {"id": ...} and got HTTP 400; re-enabled here with
            #       organization_name + domain added, per Apollo's own
            #       documented guidance that more identifying context
            #       improves match success.
            # Confirmed real cases:
            #   - "Akhmed Khadizov" (Founder, Dopamine eSports Lounge) —
            #     no linkedin_url in search response (path 4a added).
            #   - "Harriet Buyinza" (Sales Manager, Eco Yoga Sanctuary) —
            #     "name" field truncated but first/last were complete
            #     (name-building fix, not a reveal-path fix).
            #   - "Michael" (Owner, Pacific Adventures) / "Kristina" (Head
            #     of Partnership, Museum of the Future) — no linkedin_url
            #     AND no last name in the search response at all; these
            #     are the motivating cases for path 4c.
            email_revealed = False

            if _is_masked(email) or not email:
                linkedin_url = best.get("linkedin_url")
                revealed_fields = []
                match_data = None
                attempted = False  # tracks whether an actual API call fired,
                                    # so we never log a credit for zero calls

                # Path 4a — linkedin_url based reveal (preferred)
                if linkedin_url and _check_credit_cap():
                    attempted = True
                    match_data = await _post_with_retry(
                        client, _PEOPLE_MATCH_URL, headers,
                        {
                            "linkedin_url":           linkedin_url,
                            "reveal_personal_emails": True,
                            "reveal_phone_number":    True,
                        },
                    )

                # Path 4b — name + domain fallback, only if 4a wasn't tried
                # or didn't yield an email. Mirrors Step 5's phone reveal
                # approach exactly, so email reveal is no longer weaker
                # than phone reveal in terms of fallback coverage.
                if (not match_data or not (match_data.get("person") or {}).get("email")) \
                        and org_domain and name and _check_credit_cap():
                    parts = name.strip().split()
                    if len(parts) >= 2:
                        logger.info(
                            "Apollo: no linkedin_url (or 4a yielded no email) for "
                            "%r — trying name+domain email reveal instead.",
                            business_name,
                        )
                        attempted = True
                        match_data = await _post_with_retry(
                            client, _PEOPLE_MATCH_URL, headers,
                            {
                                "first_name":             parts[0],
                                "last_name":              parts[-1],
                                "organization_name":      business_name,
                                "domain":                 org_domain,
                                "reveal_personal_emails": True,
                                "reveal_phone_number":    True,
                            },
                        )

                # Path 4c — Apollo person ID fallback, RE-ENABLED with a
                # corrected payload.
                #
                # First attempt sent {"id": best["id"], ...} ALONE, which
                # Apollo rejected with HTTP 400. Verified against Apollo's
                # own documentation: id-based /people/match IS a real,
                # working pattern (confirmed via a cited real-world usage
                # guide sending exactly {"id": "APOLLO_PERSON_ID"}) — but
                # Apollo's docs also explicitly state "when you provide
                # more information, Apollo is more likely to find matches"
                # and that 400/422 responses are typically caused by
                # "missing identifiers... or incomplete parameters." Our
                # first attempt provided ONLY the id with no supporting
                # context. This attempt pairs the id with organization_name
                # and domain — both already available at this point in the
                # function — following Apollo's own stated best practice
                # rather than sending the id in isolation again.
                #
                # If this STILL fails, the improved error-body logging
                # above (now covers any 4xx, not just 422) will show
                # Apollo's exact rejection reason immediately, rather than
                # requiring another blind guess.
                if (not match_data or not (match_data.get("person") or {}).get("email")) \
                        and best.get("id") and _check_credit_cap():
                    logger.info(
                        "Apollo: name incomplete (%r) and no linkedin_url for "
                        "%r — trying reveal by Apollo person ID + org context.",
                        name, business_name,
                    )
                    attempted = True
                    # reveal_phone_number deliberately OMITTED here — Apollo's
                    # exact rejection reason (now visible thanks to the
                    # improved error logging): "Please add a valid
                    # 'webhook_url' parameter when using 'reveal_phone_number'".
                    # ID-based /people/match lookups route phone reveal
                    # through Apollo's async waterfall enrichment, which
                    # requires a webhook endpoint we don't have configured.
                    # We don't need phone from this specific call anyway —
                    # Step 5 below already handles phone reveal separately
                    # via the name+domain match, which does NOT hit this
                    # webhook requirement.
                    id_payload = {
                        "id":                     best["id"],
                        "reveal_personal_emails": True,
                    }
                    if business_name:
                        id_payload["organization_name"] = business_name
                    if org_domain:
                        id_payload["domain"] = org_domain
                    match_data = await _post_with_retry(
                        client, _PEOPLE_MATCH_URL, headers, id_payload,
                    )

                if match_data:
                    revealed       = match_data.get("person") or {}
                    rev_email      = revealed.get("email")
                    if rev_email and not _is_masked(rev_email) and "@" in rev_email:
                        email          = rev_email
                        email_revealed = True
                        revealed_fields.append("email")
                    rev_phones = revealed.get("phone_numbers") or []
                    if rev_phones:
                        best["phone_numbers"] = rev_phones
                        revealed_fields.append("phone")

                # Only log a credit if an API call actually fired — logging
                # unconditionally here would falsely record a credit even
                # when neither reveal path had enough data to attempt at all
                # (no linkedin_url AND no usable name/domain), corrupting
                # credit tracking with phantom usage.
                if attempted:
                    await _log_credit(
                        "email_reveal", 1, business_name, run_id,
                        result_fields=revealed_fields,
                        success=bool(revealed_fields),
                        error_msg="" if revealed_fields else "reveal returned nothing (all applicable paths tried)",
                    )
                else:
                    logger.info(
                        "Apollo: email reveal skipped entirely for %r — no "
                        "linkedin_url, no usable name+domain combination, "
                        "and no Apollo person ID available.",
                        business_name,
                    )

                if not email_revealed and not (email and "@" in email):
                    email = None

            # ── Step 5: Phone reveal if still missing ─────────────────────
            phone = _extract_phone(best)
            if not phone and org_domain and name and _check_credit_cap():
                parts = name.strip().split()
                if len(parts) >= 2:
                    match_data = await _post_with_retry(
                        client, _PEOPLE_MATCH_URL, headers,
                        {
                            "first_name":          parts[0],
                            "last_name":           parts[-1],
                            "organization_name":   business_name,
                            "domain":              org_domain,
                            "reveal_phone_number": True,
                        },
                    )
                    if match_data:
                        rev_person = match_data.get("person") or {}
                        phone      = _extract_phone(rev_person) or phone

                    await _log_credit(
                        "phone_reveal", 1, business_name, run_id,
                        result_fields=["phone"] if phone else [],
                        success=bool(phone),
                    )

            # ── Step 6: Build result ───────────────────────────────────────
            result: dict = {
                "contact_name":   name,
                "contact_title":  title,
                "email_revealed": email_revealed,
                "org_domain":     org_domain or "",
                "company_synopsis": company_synopsis,  # "" if Apollo has no real description —
                                                        # never fabricated, see _build_synopsis
                "all_contacts": [
                    {
                        "name": (
                            lambda _n, _c: _c if len(_c.split()) > len(_n.split()) else (_n or _c)
                        )(
                            p.get("name") or "",
                            f"{p.get('first_name','')} {p.get('last_name','')}".strip(),
                        ),
                        "title":    p.get("title", ""),
                        "linkedin": p.get("linkedin_url", ""),
                        "email":    p.get("email") if not _is_masked(p.get("email")) else None,
                    }
                    for p in people
                ],
            }

            if email and "@" in email and not _is_masked(email):
                result["email_id"] = email
            if phone:
                result["phone_number"] = phone
            if best.get("linkedin_url"):
                result["linkedin_profile"] = best["linkedin_url"]

            fields_found = [k for k in ("email_id", "phone_number", "linkedin_profile") if result.get(k)]
            logger.info(
                "Apollo: result for %r — fields=%s contacts=%d revealed=%s.",
                business_name, fields_found, len(people), email_revealed,
            )
            return result

    except Exception as exc:
        logger.error("Apollo error for %r: %s", business_name, exc)
        return {}


# ── Apollo Prospecting (Discovery mode) ────────────────────────────────────

async def prospect_apollo(
    category: str,
    region: str = "UAE",
    max_companies: int = 50,
    run_id: str = "",
) -> list[dict]:
    """
    Use Apollo's prospecting API to discover NEW companies not in our DB.
    """
    if not APOLLO_API_KEY:
        logger.warning("APOLLO_API_KEY not set — skipping Apollo prospecting.")
        return []
    if not _check_credit_cap():
        logger.error("Apollo credit cap reached — skipping prospecting.")
        return []
    if not await _check_daily_limit():
        logger.warning("Apollo daily limit (%d/day) reached — skipping prospecting.", _DAILY_LIMIT)
        return []

    _CATEGORY_TO_INDUSTRIES = {
        "adventure":    ["recreational facilities", "sports", "tourism", "outdoor recreation"],
        "wellness":     ["health wellness fitness", "spa", "yoga", "alternative medicine"],
        "food":         ["restaurants", "food beverages", "hospitality"],
        "culture":      ["museums", "arts crafts", "entertainment", "tourism"],
        "travel":       ["leisure travel tourism", "hospitality", "travel arrangements"],
        "experience":   ["entertainment", "events services", "tourism", "recreation"],
    }

    cat_lower = category.lower()
    industries = []
    for key, vals in _CATEGORY_TO_INDUSTRIES.items():
        if key in cat_lower:
            industries.extend(vals)
    if not industries:
        industries = ["leisure travel tourism", "entertainment", "hospitality"]

    headers = {
        "Content-Type": "application/json",
        "Cache-Control": "no-cache",
        "X-Api-Key": APOLLO_API_KEY,
    }

    partners: list[dict] = []
    page = 1
    per_page = 10

    logger.info(
        "Apollo prospecting: category=%r industries=%s max=%d",
        category, industries, max_companies,
    )

    try:
        async with httpx.AsyncClient(timeout=25.0) as client:
            while len(partners) < max_companies:
                if not _check_credit_cap():
                    logger.warning("Apollo credit cap reached during prospecting — stopping.")
                    break

                payload = {
                    "page":                  page,
                    "per_page":              per_page,
                    "organization_locations": [region],
                    "organization_industry_tag_ids": industries,
                    "person_titles":         _APOLLO_TITLE_KEYWORDS,
                    "organization_num_employees_ranges": ["1,200"],
                }

                data = await _post_with_retry(client, _PEOPLE_SEARCH_URL, headers, payload)

                await _log_credit(
                    "prospecting", 1, f"category:{category}", run_id,
                    success=bool(data),
                )

                if not data:
                    break

                people = data.get("people") or []
                if not people:
                    break

                for person in people:
                    if len(partners) >= max_companies:
                        break

                    org  = (person.get("organization") or {})
                    name = org.get("name") or person.get("organization_name") or ""
                    if not name:
                        continue

                    if not _is_target_title(person.get("title") or ""):
                        continue

                    email = person.get("email")
                    if _is_masked(email):
                        email = None

                    partners.append({
                        "partner_name":     name,
                        "category":         category,
                        "subcategories":    category,
                        "website":          org.get("website_url") or "",
                        "org_domain":       _clean_domain(org.get("website_url") or ""),
                        "region":           "Local",
                        "status":           "Yet to Start",
                        "digitisation":     "Semi-digitised",
                        "sheet_source":     "apollo_prospecting",
                        "contact_name":     person.get("name") or "",
                        "contact_title":    person.get("title") or "",
                        "email_id":         email or "",
                        "phone_number":     _extract_phone(person) or "",
                        "linkedin_profile": person.get("linkedin_url") or "",
                    })

                total_pages = data.get("pagination", {}).get("total_pages", 1)
                logger.info(
                    "Apollo prospecting page %d/%d: %d new partners (total=%d)",
                    page, total_pages, len(people), len(partners),
                )

                if page >= total_pages:
                    break
                page += 1

    except Exception as exc:
        logger.error("Apollo prospecting error for category %r: %s", category, exc)

    logger.info(
        "Apollo prospecting: found %d partners for category=%r", len(partners), category,
    )
    return partners


# ── Session credit summary ─────────────────────────────────────────────────

def get_session_credit_summary() -> dict:
    """Return in-memory credit usage for this server session."""
    return {
        **_session_credits,
        "monthly_cap":    _MONTHLY_CAP,
        "remaining_est":  max(0, _MONTHLY_CAP - _session_credits.get("total", 0)),
        "warning_pct":    _WARNING_PCT,
    }