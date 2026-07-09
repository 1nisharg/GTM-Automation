"""
enrichment_sources/website_scraper.py
--------------------------------------
Website Scraper — Fallback source (Priority 6) in the enrichment chain.

Purpose
-------
When Hunter + Apollo + LinkedIn all fail to fill a contact field, this scraper
hits the company's own website directly and extracts whatever is still missing.

It only runs for fields that are STILL missing after the full fallback chain —
never called if Hunter/Apollo already filled everything.

The domain comes from Hunter's result dict ("domain" key) or Apollo's org search.
No website URL needs to be pre-populated in the CRM.

Strategy
--------
1. Try /contact, /contact-us, /about, /about-us pages first (highest yield).
2. Fall back to the homepage if none of those return useful data.
3. Extract email, phone, LinkedIn using regex — no heavy HTML parsing needed.
4. Return only the fields that are still missing (passed in via `missing_fields`).

Return schema
-------------
{
    "email_id":         str | None,
    "phone_number":     str | None,
    "linkedin_profile": str | None,
    "scraped_from":     str,        # URL that yielded the data
}
Returns {} on any error or if nothing found.

No environment variables required.
"""

import logging
import re
from urllib.parse import urljoin, urlparse

import httpx

logger = logging.getLogger(__name__)

# Pages most likely to have contact info — tried in order
_CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/contact_us",
    "/contactus",
    "/about",
    "/about-us",
    "/about_us",
    "/reach-us",
    "/get-in-touch",
]

# Regex patterns
_EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

# Phone regex — strict enough to avoid dates, zip codes, reference numbers.
# Requires a + prefix OR at least 2 consecutive digits to start, then
# allows separators. Minimum 8 DIGITS (not chars) enforced in _extract_phones.
_PHONE_RE = re.compile(
    r"(?:"
    r"\+\d[\d\s\-\.\(\)]{6,20}\d"   # E.164 style: must start with +
    r"|"
    r"\b(?:00\d{1,3}|0\d{1,2})[\d\s\-\.\(\)]{5,18}\d"  # local intl: 00971 or 04...
    r")"
)
_LINKEDIN_RE = re.compile(
    r"https?://(?:www\.)?linkedin\.com/(?:in|company)/[a-zA-Z0-9_\-\%]+"
)

# Generic email prefixes — never use these for outreach.
# Expanded to include location prefixes, department names, and role-based
# addresses that appear personal but go to shared inboxes.
_GENERIC_PREFIXES = {
    # Standard catch-all
    "info", "contact", "hello", "support", "admin",
    "enquiries", "enquiry", "booking", "bookings",
    "reservations", "reception", "office", "team", "mail",
    "general", "service", "services", "help", "noreply",
    "no-reply", "donotreply", "do-not-reply", "no_reply",
    # Sales / marketing shared inboxes
    "sales", "marketing", "pr", "media", "press", "events",
    "promotions", "deals", "offers", "partnerships", "partner",
    # Operations shared inboxes
    "operations", "ops", "billing", "finance", "accounts",
    "procurement", "purchasing", "contracts", "legal",
    "hr", "recruitment", "careers", "jobs",
    # Hospitality / tourism specific
    "concierge", "guestrelations", "guestservices", "guest",
    "experiences", "activities", "tours", "reservations",
    "checkin", "checkout", "frontdesk", "front.desk",
    "complaints", "feedback", "reviews",
    # Location-prefixed (dubai@, uae@, abudhabi@, etc.)
    "dubai", "uae", "abu", "abudhabi", "sharjah", "ajman",
    "rak", "fujairah", "alain", "ae", "gcc", "me",
    "middleeast", "gulf", "ksa", "riyadh", "doha", "qatar",
    # Generic department inboxes
    "webmaster", "web", "digital", "online", "website",
    "tech", "it", "helpdesk", "newsletter", "subscribe",
    "unsubscribe", "postmaster", "abuse", "spam",
    # Common patterns seen in UAE partner bounces
    "mail", "email", "contactus", "getintouch",
    "reach", "connect", "query", "queries",
}

# Additional check: prefixes that are SINGLE geographic words or short codes
_GENERIC_PATTERNS = (
    r"^(info|contact|hello|support|admin|dubai|uae|ae)[\.\-_]",  # prefix + separator
    r"^\d+$",  # pure numeric
    r"^.{1,2}$",  # too short to be a real name
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_generic_email(email: str) -> bool:
    """Return True if this email is a shared/generic inbox — not a real person."""
    import re as _re
    prefix = email.split("@")[0].lower().strip()
    # Exact match against known generic prefixes
    if prefix in _GENERIC_PREFIXES:
        return True
    # Pattern checks for short codes, numeric, or prefixed generics
    for pattern in _GENERIC_PATTERNS:
        if _re.match(pattern, prefix):
            return True
    # If prefix contains no letters at all — not a real person
    if not any(c.isalpha() for c in prefix):
        return True
    return False


def _clean_domain(raw: str) -> str:
    """Normalise a raw domain or URL to a bare https:// base URL."""
    if not raw:
        return ""
    raw = raw.strip()
    if not raw.startswith("http"):
        raw = "https://" + raw
    parsed = urlparse(raw)
    # Rebuild as scheme + netloc only
    return f"{parsed.scheme}://{parsed.netloc}"


def _extract_emails(text: str, prefer_personal: bool = True) -> list[str]:
    """Extract all emails from text. If prefer_personal, sort personal first."""
    found = list(set(_EMAIL_RE.findall(text)))
    if prefer_personal:
        personal = [e for e in found if not _is_generic_email(e)]
        generic  = [e for e in found if _is_generic_email(e)]
        return personal + generic
    return found


# UAE and common GCC country codes — used to validate scraped phone numbers
_UAE_COUNTRY_CODES = {"971", "968", "966", "974", "965", "973"}  # UAE, Oman, KSA, Qatar, Kuwait, Bahrain

def _is_valid_phone_number(raw: str) -> bool:
    """
    Return True only if this string is a plausible real phone number.
    Rejects: dates (2024-01-01), zip codes, reference numbers, short codes,
    numbers with too many repeated digits, pure sequences.
    """
    digits = re.sub(r"\D", "", raw)

    # Must have 7–15 digits (E.164 max is 15)
    if not (7 <= len(digits) <= 15):
        return False

    # Must have at least 8 digits for international numbers
    if len(digits) < 8:
        return False

    # Reject if looks like a year (4 digits starting with 19 or 20)
    if len(digits) == 4 and digits[:2] in ("19", "20"):
        return False

    # Reject pure sequences (1234567, 0000000)
    if digits == digits[0] * len(digits):
        return False
    if digits in ("1234567", "12345678", "123456789", "0123456789"):
        return False

    # Reject if more than 60% of digits are the same character (spam/placeholder)
    from collections import Counter
    most_common_count = Counter(digits).most_common(1)[0][1]
    if most_common_count / len(digits) > 0.6:
        return False

    # Accept UAE/GCC numbers: start with +971, 00971, or 0 (local UAE)
    # Also accept any number with 10+ digits (likely international)
    if raw.lstrip().startswith("+") or raw.lstrip().startswith("00"):
        return True
    if len(digits) >= 10:
        return True
    # Local UAE numbers: 7–9 digits starting with 0 or 5/4/2/3/6/7/8/9
    if len(digits) in (8, 9) and digits[0] in "02345689":
        return True

    return False


def _normalise_phone(raw: str) -> str:
    """
    Normalise a scraped phone number to E.164-ish format.
    Strips excess whitespace and standardises separators.
    Does NOT blindly add +971 — only normalises what's there.
    """
    # Remove everything except digits, +, spaces, hyphens, parens
    cleaned = re.sub(r"[^\d+\s\-\(\)]", "", raw).strip()
    # Collapse multiple spaces/separators
    cleaned = re.sub(r"[\s\-]{2,}", "-", cleaned)
    return cleaned


def _extract_phones(text: str) -> list[str]:
    """
    Extract valid phone numbers from page text.
    Validates each match to reject dates, reference numbers, and short codes.
    """
    raw_phones = _PHONE_RE.findall(text)
    cleaned = []
    for p in raw_phones:
        p = p.strip()
        if _is_valid_phone_number(p):
            cleaned.append(_normalise_phone(p))
    return list(dict.fromkeys(cleaned))  # dedupe preserving order


def _extract_linkedin(text: str) -> str | None:
    """Extract the first LinkedIn company or person URL from text."""
    matches = _LINKEDIN_RE.findall(text)
    # Prefer /company/ URLs for business pages
    company = [m for m in matches if "/company/" in m]
    if company:
        return company[0]
    if matches:
        return matches[0]
    return None


async def _fetch_page(client: httpx.AsyncClient, url: str) -> str | None:
    """Fetch a page and return its text, or None on failure."""
    try:
        resp = await client.get(url, headers=_HEADERS, follow_redirects=True, timeout=10.0)
        if resp.status_code == 200:
            return resp.text
        return None
    except Exception as exc:
        logger.debug("Scraper: failed to fetch %r — %s", url, exc)
        return None


# ── Main Scraper ───────────────────────────────────────────────────────────────

async def scrape_website(
    domain: str,
    missing_fields: list[str],
) -> dict:
    """
    Scrape a company website for missing contact fields.

    Parameters
    ----------
    domain : str
        The company domain from Hunter ("domain" key) or Apollo org search.
        Can be bare domain ("alboommarine.com") or full URL.
    missing_fields : list[str]
        Fields still missing after Hunter + Apollo — only these are extracted.
        Subset of: ["email_id", "phone_number", "linkedin_profile"]

    Returns
    -------
    dict — keys matching missing_fields that were found, plus "scraped_from".
    Returns {} if nothing found or domain is empty.
    """
    if not domain or not missing_fields:
        return {}

    base_url = _clean_domain(domain)
    if not base_url:
        logger.warning("Scraper: could not normalise domain %r — skipping.", domain)
        return {}

    logger.info(
        "Scraper: starting for domain=%r, missing=%s",
        domain, missing_fields,
    )

    result: dict = {}
    scraped_from: str = ""

    # Pages to try — contact/about paths first, then homepage
    pages_to_try = [urljoin(base_url, path) for path in _CONTACT_PATHS] + [base_url]

    async with httpx.AsyncClient(timeout=10.0) as client:
        for page_url in pages_to_try:
            text = await _fetch_page(client, page_url)
            if not text:
                continue

            page_result: dict = {}

            # Extract only what's still missing
            if "email_id" in missing_fields and "email_id" not in result:
                emails = _extract_emails(text, prefer_personal=True)
                if emails:
                    page_result["email_id"] = emails[0]

            if "phone_number" in missing_fields and "phone_number" not in result:
                phones = _extract_phones(text)
                if phones:
                    page_result["phone_number"] = phones[0]

            if "linkedin_profile" in missing_fields and "linkedin_profile" not in result:
                linkedin = _extract_linkedin(text)
                if linkedin:
                    page_result["linkedin_profile"] = linkedin

            if page_result:
                result.update(page_result)
                scraped_from = page_url
                logger.info(
                    "Scraper: found %s on %r",
                    list(page_result.keys()), page_url,
                )

            # Stop if all missing fields are now resolved
            if all(f in result for f in missing_fields):
                logger.info("Scraper: all missing fields resolved from %r.", page_url)
                break

    if result:
        result["scraped_from"] = scraped_from
        logger.info(
            "Scraper: final result for %r — fields=%s source=%r",
            domain, list(result.keys()), scraped_from,
        )
    else:
        logger.info("Scraper: nothing found for domain %r.", domain)

    return result