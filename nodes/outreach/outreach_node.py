"""
nodes/outreach/outreach_node.py
--------------------------------
Outreach Node — Stage 3 of the pipeline.

Implementation
---------------
For each enriched partner with a usable phone_number, places an outbound
voice call via the voice agent (Twilio + Deepgram + Groq), waits for the
call to complete (including post-call summarisation), and records the
result.

Partners with no phone_number are skipped (logged, not failed) — they
will need email/LinkedIn outreach instead, which is a separate concern.

Concurrency
-----------
Calls are placed with a concurrency cap (default 2) since voice calls are
expensive, slow (up to a few minutes each), and Twilio/Deepgram have their
own rate limits. Controlled via OUTREACH_CALL_CONCURRENCY env var.

Input  (GraphState field): enriched_partners: list[dict]
Output (GraphState field): outreach_results: list[dict]
    Each result: {
        "partner_name": str,
        "call_sid": str | None,
        "status": str,           # completed | no-answer | busy | failed | timeout | skipped | error
        "duration_s": int,
        "summary": dict | None,
    }
"""

import asyncio
import logging
import os
import re

from state import GraphState
from voice_agent.engine import place_call
from nodes.outreach.outreach_workflow import (
    send_sequence_touch,
    start_scheduler_loop,
    get_channels_for_partner,
)


import re


def _sanitise_phone(raw) -> "str | None":
    """
    Normalise a raw phone number string to E.164 format, or return None
    if the value is unsalvageable (bad data, date strings, too short, etc).

    Handles common dirty-data patterns seen in the partners table:
      - "1573245411"   → "+1573245411"  (missing leading +)
      - "2020-04-16"   → None           (date mistaken for phone number)
      - "00971..."     → "+971..."      (00 prefix instead of +)
      - "+1 (669)..."  → "+16693..."    (spaces/parens/dashes stripped)
      - None / ""      → None
    """
    if not raw:
        return None

    s = str(raw).strip()

    # Reject obvious date strings: YYYY-MM-DD or DD/MM/YYYY or YYYY/MM/DD
    if re.match(r"^\d{4}-\d{2}-\d{2}$", s):
        return None
    if re.match(r"^\d{2}[/\-]\d{2}[/\-]\d{4}$", s):
        return None
    # Reject reference numbers like "2 2 0 0 1 2-2" (digits separated by spaces)
    if re.match(r"^[\d\s\-]+$", s):
        digits_only = re.sub(r"\D", "", s)
        # If it has lots of whitespace relative to digits, it's a reference/date
        non_digit = len(s) - len(digits_only)
        if non_digit > len(digits_only) * 0.4:  # more than 40% non-digits = suspicious
            return None

    # Strip everything except digits and a leading +
    cleaned = re.sub(r"[^\d+]", "", s)

    # Normalise 00-prefix to +
    if cleaned.startswith("00"):
        cleaned = "+" + cleaned[2:]

    digit_count = len(re.sub(r"\D", "", cleaned))
    if digit_count < 8:
        return None

    # Reject pure sequences and repeated digits
    digits = re.sub(r"\D", "", cleaned)
    if digits == digits[0] * len(digits):  # all same digit e.g. 00000000
        return None
    if digits in ("12345678", "123456789", "0123456789", "11111111"):
        return None

    # Already E.164 — but don't blindly trust the + prefix. Validate:
    #   1. No real country code starts with 0 (e.g. "+031..." is garbage —
    #      this exact pattern is what let a malformed scraped number
    #      through to a live Twilio call that failed with error 21211).
    #   2. E.164 hard maximum is 15 digits total (country code + subscriber
    #      number) — reject anything longer as corrupted data.
    #   3. Reject anything suspiciously short for an international number
    #      once the + is present (under 9 digits total is implausible).
    if cleaned.startswith("+"):
        if digits.startswith("0"):
            return None  # "+0..." is not a real country code
        if digit_count > 15:
            return None  # exceeds E.164 maximum — corrupted data
        if digit_count < 9:
            return None  # too short to be a real international number
        return cleaned

    # UAE local numbers — MUST be checked before the generic 10+-digit
    # branch below. A UAE number written with its trunk-prefix 0 (e.g.
    # "0501234567") is exactly 10 raw digits and was previously falling
    # through to the generic "10+ digits -> prepend +" branch, producing
    # an invalid number like "+0501234567" (found via test case:
    # "050 123 4567" was incorrectly becoming "+0501234567" instead of
    # the correct "+971501234567" — same invalid-country-code defect
    # class as the "+031026941301920" bug that failed against Twilio).
    if digits.startswith("0") and digit_count in (9, 10) and digits[1] in "024569":
        return "+971" + digits[1:]
    if not digits.startswith("0") and digit_count in (8, 9) and digits[0] in "024569":
        return "+971" + digits

    # 10+ digits, no + prefix, not a UAE-local match above — treat as
    # already carrying its own country code, just prepend +.
    #
    # CRITICAL FIX: this branch was blindly prepending "+" to ANY 10+
    # digit string with zero validation on the result — reproducing the
    # exact same "+0 is not a real country code" defect the "already has
    # +" branch above was specifically fixed to catch, just reached via a
    # different path. Confirmed real case: raw DB value "089454561774522"
    # (NO leading + at all, 15 digits starting with 0) has no "+" for the
    # branch above to catch, isn't 8-10 digits for the UAE-local checks,
    # and fell all the way to this final catch-all, which produced the
    # invalid "+089454561774522" that then reached a live Twilio call.
    # Now this branch rejects the same "+0..." pattern the +-prefixed
    # branch already rejects, and also enforces the same 15-digit E.164
    # ceiling, instead of blindly trusting any 10+ digit string.
    if digit_count >= 10:
        if digits.startswith("0"):
            return None  # would produce "+0..." — not a real country code
        if digit_count > 15:
            return None  # exceeds E.164 maximum — corrupted data
        return "+" + cleaned

    return None

logger = logging.getLogger(__name__)

_CALL_CONCURRENCY: int = int(os.getenv("OUTREACH_CALL_CONCURRENCY", "2"))
_CALL_TIMEOUT_S: int = int(os.getenv("OUTREACH_CALL_TIMEOUT_S", "180"))

# NOTE: the old _DEFAULT_SCRIPT / _build_script generic opening-line builder
# has been removed. engine.py's place_call() now owns both the hardcoded
# personalized opening line and the category/company_synopsis-driven pitch
# question — passing a script here would silently override both back to
# generic phrasing (see the comment at the place_call() call site below).


async def _call_one_partner(partner: dict, run_id: str, semaphore: asyncio.Semaphore) -> dict:
    name = partner.get("partner_name", "Unknown")

    try:
        # str() first — defensive against a non-string DB value (e.g. if a
        # column was ever populated with a number). Without this, a truthy
        # non-string phone_number would crash on .strip() and — since the
        # outer gather uses return_exceptions=False — take down the ENTIRE
        # outreach batch for every other partner in this run, not just this one.
        raw_phone = str(partner.get("phone_number") or "").strip()

        # Sanitise and validate before dialling — dirty data in partners table
        # can contain dates ("2020-04-16"), short strings ("123"), etc.
        phone = _sanitise_phone(raw_phone)
        if not phone:
            logger.info(
                "[%s] Outreach: skipping %r — phone %r is invalid or unsalvageable.",
                run_id, name, raw_phone,
            )
            return {
                "partner_name": name, "call_sid": None, "status": "skipped",
                "duration_s": 0, "summary": None,
                "reason": f"invalid phone number: {raw_phone!r}",
            }

        async with semaphore:
            logger.info("[%s] Outreach: calling %r at %s…", run_id, name, phone)
            try:
                result = await place_call(
                    to=phone,
                    # "" (not _build_script(partner)) — engine.py now owns the
                    # opening line itself (hardcoded personalized greeting) and
                    # builds the mid-call pitch question from category/
                    # company_synopsis. Passing a generic script here would
                    # silently override BOTH of those and revert every call
                    # back to fully generic phrasing, which is the opposite
                    # of what personalization requires.
                    script="",
                    mission=f"GTM UAE partner outreach — {partner.get('category', 'General')}",
                    partner_id=partner.get("id"),
                    partner_name=name,
                    partner_email=partner.get("email_id") or "",
                    contact_name=partner.get("contact_name") or None,
                    digitisation=partner.get("digitisation") or "semi",
                    category=partner.get("category") or "",
                    company_synopsis=partner.get("company_synopsis") or "",
                    timeout_s=_CALL_TIMEOUT_S,
                )
            except Exception as exc:
                logger.error("[%s] Outreach: call to %r failed with exception: %s", run_id, name, exc)
                return {
                    "partner_name": name, "call_sid": None, "status": "error",
                    "duration_s": 0, "summary": None, "reason": str(exc),
                }

        logger.info(
            "[%s] Outreach: %r call finished — status=%s duration=%ss",
            run_id, name, result.get("status"), result.get("duration_s"),
        )
        return {
            "partner_name": name,
            "call_sid": result.get("call_sid"),
            "status": result.get("status"),
            "duration_s": result.get("duration_s", 0),
            "summary": result.get("summary"),
        }

    except Exception as exc:
        # Final safety net — an unexpected error anywhere above (malformed
        # partner dict, unexpected type, etc.) is recorded for THIS partner
        # only, rather than propagating up through asyncio.gather and
        # aborting outreach for every other partner in the batch.
        logger.error(
            "[%s] Outreach: unexpected error processing %r — %s", run_id, name, exc,
        )
        return {
            "partner_name": name, "call_sid": None, "status": "error",
            "duration_s": 0, "summary": None, "reason": f"unexpected error: {exc}",
        }


async def outreach_node(state: GraphState) -> dict:
    run_id = state.get("run_id", "")
    prefix = f"[{run_id}] " if run_id else ""

    enriched = state.get("enriched_partners", [])
    logger.info("%sOutreach node: processing %d enriched partners.", prefix, len(enriched))

    if not enriched:
        return {"outreach_results": []}

    semaphore = asyncio.Semaphore(_CALL_CONCURRENCY)

    # return_exceptions=True — belt-and-suspenders on top of the try/except
    # already inside _call_one_partner. Guarantees one partner's failure
    # (even something truly unexpected like a CancelledError) can never
    # abort outreach for every other partner in this batch.
    raw_results = await asyncio.gather(
        *[_call_one_partner(p, run_id, semaphore) for p in enriched],
        return_exceptions=True,
    )

    results = []
    for p, r in zip(enriched, raw_results):
        if isinstance(r, Exception):
            pname = p.get("partner_name", "Unknown")
            logger.error("%sOutreach: %r raised an uncaught exception — %s", prefix, pname, r)
            results.append({
                "partner_name": pname, "call_sid": None, "status": "error",
                "duration_s": 0, "summary": None, "reason": f"uncaught exception: {r}",
            })
        else:
            results.append(r)

    called = sum(1 for r in results if r["status"] not in ("skipped", "error"))
    skipped = sum(1 for r in results if r["status"] == "skipped")
    logger.info(
        "%sOutreach node: complete — %d called, %d skipped (no phone), %d total.",
        prefix, called, skipped, len(results),
    )

    return {"outreach_results": list(results)}


# ── Background scheduler task ──────────────────────────────────────────────────

async def launch_outreach_scheduler() -> None:
    """
    Start the outreach sequence scheduler as a background asyncio task.
    Called once from main.py lifespan startup — runs daily at 09:00 UAE time.
    
    Usage in main.py lifespan:
        import asyncio
        from nodes.outreach.outreach_node import launch_outreach_scheduler
        asyncio.create_task(launch_outreach_scheduler())
    """
    import asyncio
    logger.info("Launching outreach sequence scheduler background task…")
    asyncio.create_task(start_scheduler_loop())