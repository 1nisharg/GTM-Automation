"""
nodes/outreach/outreach_workflow.py
-------------------------------------
Channel-aware outreach workflow with 3-touch automated sequence.

Touch sequence (per Aarna AI Behaviour & Outreach Script Framework):
    Touch 1  — Day 1   : First contact
    Touch 2  — Day 3   : Follow-up #1
    Touch 3  — Day 12  : Soft close (Follow-up #2)

Channel decision by digitisation tier (per PDF):
    Fully Digitised  → Email (Day 1) + LinkedIn (Day 1) → WhatsApp follow-ups
    Semi-Digitised   → WhatsApp-led throughout
    Un-Digitised     → WhatsApp only (highly conversational)

All templates are taken verbatim from the AI Behaviour, Conversation Logic
& Outreach Script Framework document.
"""

import asyncio
import logging
import os
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx
from twilio.rest import Client as TwilioClient

from db.connection import get_pool
from voice_agent.engine import place_call

logger = logging.getLogger(__name__)

# ── Env vars ───────────────────────────────────────────────────────────────────
TWILIO_ACCOUNT_SID   = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN    = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886")

GMAIL_USER     = os.getenv("EMAIL_ADDRESS", "")
GMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SMTP_SERVER    = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT", "587"))
EMAIL_FROM     = os.getenv("EMAIL_FROM", GMAIL_USER)

UNIPILE_API_KEY    = os.getenv("UNIPILE_API_KEY", "")
UNIPILE_DSN        = os.getenv("UNIPILE_DSN", "")
UNIPILE_ACCOUNT_ID = os.getenv("UNIPILE_ACCOUNT_ID", "")

# Touch day offsets from first contact
_TOUCH_DAYS = {1: 0, 2: 3, 3: 12}


# ── Channel decision engine ────────────────────────────────────────────────────

def get_channels_for_partner(partner: dict, touch_number: int) -> list[str]:
    """
    Decide which channels to use for a given partner and touch number.

    Business-preferred ordering by digitisation tier (per AI Behaviour doc):
        Fully Digitised   → prefer email + linkedin on Touch 1, whatsapp after
        Semi/Un-Digitised → prefer whatsapp throughout

    CRITICAL DESIGN RULE: digitisation tier sets PREFERENCE ORDER, never a
    hard gate that discards data. The previous version unconditionally
    returned ["whatsapp"] for any partner not tagged exactly "Fully
    Digitised" — including every partner with a blank/missing digitisation
    field — regardless of whether email_id or linkedin_profile were
    actually populated. This silently discarded real, Apollo-verified
    personal emails for any partner who also lacked a phone number,
    resulting in ZERO outreach attempted on ANY channel even though good
    contact data existed. Now: if the tier-preferred channel's field is
    empty, we fall back to whatever contact data enrichment actually
    found, in order of business preference, rather than giving up.
    """
    tier = (partner.get("digitisation") or "").lower()

    has_email    = bool((partner.get("email_id") or "").strip())
    has_linkedin = bool((partner.get("linkedin_profile") or "").strip())
    has_phone    = bool((partner.get("phone_number") or "").strip())

    if "fully" in tier:
        if touch_number == 1:
            preferred = []
            if has_email:
                preferred.append("email")
            if has_linkedin:
                preferred.append("linkedin")
            if preferred:
                return preferred
            # No email/linkedin found — fall back to whatever exists,
            # rather than the old behaviour of blindly trying WhatsApp
            # with a phone number that may not exist either.
            if has_phone:
                return ["whatsapp"]
            return []
        else:
            if has_phone:
                return ["whatsapp"]
            if has_email:
                return ["email"]
            if has_linkedin:
                return ["linkedin"]
            return []

    # Semi-digitised / Un-digitised / blank tier — WhatsApp-led preference,
    # but fall back to whatever contact data IS available rather than
    # discarding a found email or LinkedIn profile just because phone
    # is missing. This is the fix for the core bug described above.
    if has_phone:
        return ["whatsapp"]
    fallback = []
    if has_email:
        fallback.append("email")
    if has_linkedin:
        fallback.append("linkedin")
    return fallback  # empty list only if truly no contact data exists at all


# ── Message templates (verbatim from PDF) ──────────────────────────────────────

def _whatsapp_touch1(partner: dict) -> str:
    """
    WhatsApp — First Touch
    Source: Aarna AI Behaviour & Outreach Script Framework, Page 5
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}," if contact else "Hi,"

    return (
        f"{greeting}\n\n"
        f"This is Sania from Aarna.\n\n"
        f"We recently came across {name} and really liked what you're offering.\n\n"
        f"We're currently onboarding selected UAE experience and activity partners onto Aarna "
        f"— a platform focused on helping local operators increase discovery and bookings.\n\n"
        f"Thought I'd quickly reach out to see if this is something you'd be open to exploring 🙂\n\n"
        f"We are a local App powered by one of the largest travel consolidators in North & South America."
    )


def _whatsapp_touch2(partner: dict) -> str:
    """
    WhatsApp — Follow-up #1 (Day 3)
    Source: Aarna AI Behaviour & Outreach Script Framework, Page 6
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}," if contact else "Hi,"

    return (
        f"{greeting}\n\n"
        f"Just following up in case my earlier message got buried.\n\n"
        f"We're currently working with UAE-based experience and activity partners "
        f"across various categories.\n\n"
        f"Would be happy to share how the onboarding works and how partners typically "
        f"benefit from additional visibility and customer reach.\n\n"
        f"Would love to have a conversation with you on this."
    )


def _whatsapp_touch3(partner: dict) -> str:
    """
    WhatsApp — Follow-up #2 / Soft Close (Day 12)
    Source: Aarna AI Behaviour & Outreach Script Framework, Page 6
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}," if contact else "Hi,"

    return (
        f"{greeting}\n\n"
        f"Completely understand things get busy.\n\n"
        f"I'll close the loop from my side for now — but if you'd ever like to explore "
        f"listing {name} on Aarna, happy to reconnect anytime.\n\n"
        f"Wishing your team continued success 🙂"
    )


def _email_touch1(partner: dict) -> tuple[str, str]:
    """
    Email — First Touch
    Source: Aarna AI Behaviour & Outreach Script Framework, Page 8
    Returns (subject, body)
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}" if contact else "Hi"

    subject = "Partnership Opportunity with Aarna"
    body = (
        f"{greeting},\n\n"
        f"Hope you're doing well.\n\n"
        f"I recently came across {name} and thought your experiences could be a really "
        f"good fit for what we're building with Aarna here in the UAE.\n\n"
        f"We're currently onboarding selected experience and activity partners across Abhee, "
        f"Miraee and Mondee's travel-agent network, helping suppliers increase visibility among "
        f"corporates, travellers and MICE demand.\n\n"
        f"The model is simple:\n"
        f"- free to list\n"
        f"- no setup fee\n"
        f"- commission only on confirmed bookings\n\n"
        f"What stood out to us about {name} was how strongly it appeals across both "
        f"tourists and corporate/family audiences looking for unique UAE experiences.\n\n"
        f"If you're open to it, happy to either:\n"
        f"a. send across a short overview deck, or\n"
        f"b. arrange a quick 10–15 minute conversation sometime this week\n\n"
        f"No pressure at all, just thought there could be a good fit here.\n\n"
        f"Best Regards,\n"
        f"Sania\n"
        f"Aarna Partnerships\n"
        f"Abhee.ai | Aarna.global"
    )
    return subject, body


def _email_touch2(partner: dict) -> tuple[str, str]:
    """
    Email — Follow-up #1 (Day 3)
    Aligned with WhatsApp follow-up tone from PDF.
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}" if contact else "Hi"

    subject = "Exploring Supplier Partnership Opportunities — Aarna UAE"
    body = (
        f"{greeting},\n\n"
        f"Just following up on my previous note in case it got buried.\n\n"
        f"We're currently working with UAE-based experience and activity partners across "
        f"various categories, helping them increase visibility and bookings through Aarna.\n\n"
        f"Would be happy to share how the onboarding works and how partners typically "
        f"benefit from additional customer reach.\n\n"
        f"Happy to arrange a quick 10-minute call if that works for you.\n\n"
        f"Best Regards,\n"
        f"Sania\n"
        f"Aarna Partnerships\n"
        f"Abhee.ai | Aarna.global"
    )
    return subject, body


def _email_touch3(partner: dict) -> tuple[str, str]:
    """
    Email — Soft Close (Day 12)
    Aligned with WhatsApp soft close tone from PDF.
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}" if contact else "Hi"

    subject = "Potential Collaboration with Aarna UAE"
    body = (
        f"{greeting},\n\n"
        f"Completely understand things get busy.\n\n"
        f"I'll close the loop from my side for now — but if you'd ever like to explore "
        f"listing {name} on Aarna, we'd love to reconnect anytime.\n\n"
        f"Wishing you and the team continued success.\n\n"
        f"Best Regards,\n"
        f"Sania\n"
        f"Aarna Partnerships\n"
        f"Abhee.ai | Aarna.global"
    )
    return subject, body


def _linkedin_touch1(partner: dict) -> str:
    """
    LinkedIn — Connection Request + Post-acceptance DM
    Source: Aarna AI Behaviour & Outreach Script Framework, Page 9
    """
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}" if contact else "Hi"

    # Post-acceptance message (connection request is handled separately)
    return (
        f"{greeting}, came across your work in the UAE experiences space. "
        f"We're currently onboarding selected UAE experience suppliers onto Aarna "
        f"and thought there may be an opportunity to collaborate. "
        f"Would love to briefly connect and explore whether this could help bring "
        f"additional visibility and bookings your way."
    )


def _linkedin_touch2(partner: dict) -> str:
    """LinkedIn — Follow-up (Day 3)"""
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}" if contact else "Hi"

    return (
        f"{greeting}, just following up on my earlier message. "
        f"We're working with UAE experience partners across various categories to help them "
        f"reach corporates, travellers, and Mondee's global B2B agent network. "
        f"Would you be open to a quick 10-minute call this week?"
    )


def _linkedin_touch3(partner: dict) -> str:
    """LinkedIn — Soft Close (Day 12)"""
    contact = partner.get("contact_name") or ""
    name    = partner.get("partner_name", "your business")
    greeting = f"Hi {contact}" if contact else "Hi"

    return (
        f"{greeting}, completely understand if the timing isn't right. "
        f"I'll close the loop from my side for now — but if you'd ever like to explore "
        f"listing {name} on Aarna, feel free to reach out anytime. "
        f"Wishing you continued success."
    )


# Template dispatcher
_TEMPLATES = {
    "whatsapp": {1: _whatsapp_touch1, 2: _whatsapp_touch2, 3: _whatsapp_touch3},
    "email":    {1: _email_touch1,    2: _email_touch2,    3: _email_touch3},
    "linkedin": {1: _linkedin_touch1, 2: _linkedin_touch2, 3: _linkedin_touch3},
}


# ── Sequence DB helpers ────────────────────────────────────────────────────────

def _utc_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


async def _record_touch(
    partner_id: int,
    partner_name: str,
    touch_number: int,
    channel: str,
    digitisation: str,
    status: str,
    note: str = "",
    next_touch_due: datetime | None = None,
) -> None:
    """Write a touch record to outreach_sequence."""
    pool = await get_pool()
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO outreach_sequence
                    (partner_id, partner_name, touch_number, channel,
                     digitisation, status, note, sent_at, next_touch_due)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            """,
                partner_id, partner_name, touch_number, channel,
                digitisation, status, note, _utc_naive(), next_touch_due,
            )
    except Exception as exc:
        logger.warning("outreach_sequence: DB write failed — %s", exc)


async def _get_last_touch(partner_id: int) -> dict | None:
    """Return the most recent touch record for a partner."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT touch_number, channel, sent_at, next_touch_due, status
            FROM outreach_sequence
            WHERE partner_id = $1
            ORDER BY touch_number DESC, sent_at DESC
            LIMIT 1
        """, partner_id)
    return dict(row) if row else None


async def _get_partners_due_for_touch() -> list[dict]:
    """
    Return all partners whose next_touch_due is today or earlier,
    and who haven't completed touch 3 yet.
    Also returns partners with no sequence record (due for touch 1).
    """
    pool = await get_pool()
    now = _utc_naive()

    async with pool.acquire() as conn:
        # Partners due for follow-up touches (touch 2 or 3)
        due_rows = await conn.fetch("""
            SELECT DISTINCT ON (s.partner_id)
                s.partner_id, s.partner_name, s.touch_number,
                s.next_touch_due, s.channel, s.digitisation,
                p.phone_number, p.email_id, p.linkedin_profile,
                p.category, p.digitisation AS partner_digitisation
            FROM outreach_sequence s
            JOIN partners p ON p.id = s.partner_id
            WHERE s.next_touch_due <= $1
              AND s.touch_number < 3
              AND s.status = 'sent'
            ORDER BY s.partner_id, s.touch_number DESC
        """, now)

        # Partners with status 'Partner Outreach' but no sequence record yet
        new_rows = await conn.fetch("""
            SELECT p.id AS partner_id, p.partner_name, p.phone_number,
                   p.email_id, p.linkedin_profile, p.category,
                   p.digitisation AS partner_digitisation
            FROM partners p
            WHERE p.status = 'Partner Outreach'
              AND NOT EXISTS (
                  SELECT 1 FROM outreach_sequence s WHERE s.partner_id = p.id
              )
            LIMIT 50
        """)

    result = []
    for r in due_rows:
        d = dict(r)
        d["next_touch_number"] = d["touch_number"] + 1
        d["is_new"] = False
        result.append(d)

    for r in new_rows:
        d = dict(r)
        d["next_touch_number"] = 1
        d["is_new"] = True
        result.append(d)

    return result


# ── Channel senders ────────────────────────────────────────────────────────────

async def _send_whatsapp(partner: dict, touch_number: int) -> dict:
    phone = (partner.get("phone_number") or "").strip()
    if not phone:
        return {"status": "skipped", "note": "No phone number"}
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        return {"status": "error", "note": "Twilio credentials not set"}

    body = _TEMPLATES["whatsapp"][touch_number](partner)
    to_number = phone if phone.startswith("whatsapp:") else f"whatsapp:{phone}"

    try:
        client = TwilioClient(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        message = client.messages.create(
            body=body, from_=TWILIO_WHATSAPP_FROM, to=to_number,
        )
        logger.info("WhatsApp touch %d: sent to %s — SID=%s", touch_number, phone, message.sid)
        return {"status": "sent", "sid": message.sid, "to": phone, "channel": "whatsapp"}
    except Exception as exc:
        logger.error("WhatsApp touch %d: failed for %r — %s", touch_number, partner.get("partner_name"), exc)
        return {"status": "error", "note": str(exc)}


async def _send_email(partner: dict, touch_number: int) -> dict:
    email = (partner.get("email_id") or "").strip()
    if not email:
        return {"status": "skipped", "note": "No email address"}
    if not GMAIL_USER or not GMAIL_PASSWORD:
        return {"status": "error", "note": "Gmail credentials not set"}

    subject, body_text = _TEMPLATES["email"][touch_number](partner)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = email
    msg.attach(MIMEText(body_text, "plain"))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.ehlo()
            server.starttls()
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, email, msg.as_string())
        logger.info("Email touch %d: sent to %s", touch_number, email)
        return {"status": "sent", "to": email, "subject": subject, "channel": "email"}
    except Exception as exc:
        logger.error("Email touch %d: failed for %r — %s", touch_number, partner.get("partner_name"), exc)
        return {"status": "error", "note": str(exc)}


async def _send_linkedin(partner: dict, touch_number: int) -> dict:
    linkedin_url = (partner.get("linkedin_profile") or "").strip()
    if not linkedin_url:
        return {"status": "skipped", "note": "No LinkedIn profile"}
    if not UNIPILE_API_KEY or not UNIPILE_DSN or not UNIPILE_ACCOUNT_ID:
        return {"status": "error", "note": "Unipile credentials not set"}

    message_text = _TEMPLATES["linkedin"][touch_number](partner)

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            search_resp = await client.get(
                f"https://{UNIPILE_DSN}/api/v1/linkedin/profile",
                headers={"X-API-KEY": UNIPILE_API_KEY, "accept": "application/json"},
                params={"account_id": UNIPILE_ACCOUNT_ID, "linkedin_url": linkedin_url},
            )
            if search_resp.status_code != 200:
                return {"status": "error", "note": f"Unipile profile lookup {search_resp.status_code}"}

            provider_id = search_resp.json().get("provider_id") or search_resp.json().get("id")
            if not provider_id:
                return {"status": "error", "note": "Could not resolve LinkedIn provider_id"}

            dm_resp = await client.post(
                f"https://{UNIPILE_DSN}/api/v1/chats",
                headers={"X-API-KEY": UNIPILE_API_KEY, "accept": "application/json", "content-type": "application/json"},
                json={"account_id": UNIPILE_ACCOUNT_ID, "attendees_ids": [provider_id], "text": message_text},
            )
            if dm_resp.status_code in (200, 201):
                chat_id = dm_resp.json().get("id") or dm_resp.json().get("chat_id")
                logger.info("LinkedIn touch %d: sent to %s — chat_id=%s", touch_number, linkedin_url, chat_id)
                return {"status": "sent", "to": linkedin_url, "chat_id": chat_id, "channel": "linkedin"}
            else:
                return {"status": "error", "note": f"Unipile DM {dm_resp.status_code}: {dm_resp.text[:200]}"}
    except Exception as exc:
        logger.error("LinkedIn touch %d: failed for %r — %s", touch_number, partner.get("partner_name"), exc)
        return {"status": "error", "note": str(exc)}


async def _run_voice_channel(partner: dict, custom_message: str = "") -> dict:
    phone = (partner.get("phone_number") or "").strip()
    if not phone:
        return {"status": "skipped", "note": "No phone number on file"}
    name = partner.get("partner_name", "this business")

    # custom_message (if the caller explicitly provided one, e.g. a manual
    # test override) still wins — that's a deliberate override. Otherwise
    # pass "" rather than a generic hardcoded script: engine.py now owns
    # the personalized opening line and category/company_synopsis-driven
    # pitch question itself, and a truthy generic script here would
    # silently override both back to fully generic phrasing every time
    # this path fires — same bug already fixed in outreach_node.py.
    script = custom_message.strip()

    result = await place_call(
        to=phone, script=script,
        mission=f"Manual outreach launch — {partner.get('category', 'General')}",
        partner_id=partner.get("id"),
        partner_name=name,
        contact_name=partner.get("contact_name") or None,
        digitisation=partner.get("digitisation") or partner.get("partner_digitisation") or "semi",
        category=partner.get("category") or "",
        company_synopsis=partner.get("company_synopsis") or "",
        timeout_s=180,
    )
    return {
        "status": result.get("status"), "call_sid": result.get("call_sid"),
        "duration_s": result.get("duration_s", 0), "summary": result.get("summary"),
        "channel": "voice",
    }


_CHANNEL_SENDERS = {
    "whatsapp": _send_whatsapp,
    "email":    _send_email,
    "linkedin": _send_linkedin,
}


# ── Core touch sender ──────────────────────────────────────────────────────────

async def send_sequence_touch(partner: dict, touch_number: int) -> list[dict]:
    """
    Send the correct touch (1, 2, or 3) to a partner on the right channels.
    Writes result to outreach_sequence. Returns list of channel results.
    """
    partner_id   = partner.get("id") or partner.get("partner_id")
    partner_name = partner.get("partner_name", "Unknown")
    digitisation = partner.get("digitisation") or partner.get("partner_digitisation") or ""

    channels = get_channels_for_partner(
        {**partner, "digitisation": digitisation}, touch_number
    )

    if not channels:
        # Genuinely no usable contact data for this partner/touch — make
        # this visible in logs instead of silently returning an empty
        # result list with no explanation, which previously looked
        # identical to "touch already sent" or other benign no-ops.
        logger.warning(
            "Outreach sequence: %r touch=%d — NO usable channel (no phone, "
            "email, or linkedin on file). Skipping entirely.",
            partner_name, touch_number,
        )
        return [{"channel": None, "status": "skipped", "note": "no contact data available on any channel"}]

    logger.info(
        "Outreach sequence: %r touch=%d channels=%s",
        partner_name, touch_number, channels,
    )

    results = []
    for channel in channels:
        sender = _CHANNEL_SENDERS.get(channel)
        if not sender:
            results.append({"channel": channel, "status": "not_implemented"})
            continue

        result = await sender(partner, touch_number)
        status = result.get("status", "error")
        note   = result.get("note", "")

        # Calculate when next touch is due (only after successful send)
        next_due = None
        if status == "sent" and touch_number < 3:
            next_touch_day = _TOUCH_DAYS[touch_number + 1]
            next_due = _utc_naive() + timedelta(days=next_touch_day)

        await _record_touch(
            partner_id=partner_id,
            partner_name=partner_name,
            touch_number=touch_number,
            channel=channel,
            digitisation=digitisation,
            status=status,
            note=note,
            next_touch_due=next_due,
        )

        results.append({"channel": channel, **result})

    return results


# ── Scheduler ─────────────────────────────────────────────────────────────────

async def run_sequence_scheduler() -> dict:
    """
    Check all partners due for their next touch and fire it.
    Called by the background task loop every 24 hours.
    Returns summary of what was sent.
    """
    logger.info("Outreach scheduler: checking for due touches…")
    due_partners = await _get_partners_due_for_touch()
    logger.info("Outreach scheduler: %d partners due for outreach", len(due_partners))

    if not due_partners:
        return {"fired": 0, "skipped": 0, "errors": 0}

    fired = skipped = errors = 0

    for partner in due_partners:
        touch_number = partner["next_touch_number"]
        try:
            results = await send_sequence_touch(partner, touch_number)
            for r in results:
                if r.get("status") == "sent":
                    fired += 1
                elif r.get("status") == "skipped":
                    skipped += 1
                else:
                    errors += 1
        except Exception as exc:
            logger.error(
                "Scheduler: error sending touch %d to %r — %s",
                touch_number, partner.get("partner_name"), exc,
            )
            errors += 1

    logger.info(
        "Outreach scheduler: done — fired=%d skipped=%d errors=%d",
        fired, skipped, errors,
    )
    return {"fired": fired, "skipped": skipped, "errors": errors}


async def start_scheduler_loop() -> None:
    """
    Background task — checks every hour for partners due for their next touch.
    Fires immediately on startup, then repeats every 60 minutes.
    No timezone restriction — triggers as soon as next_touch_due <= now.

    Called from main.py lifespan via asyncio.create_task().
    """
    import asyncio as _asyncio

    logger.info("Outreach scheduler loop: started — checking every 60 minutes for due touches")

    while True:
        try:
            await run_sequence_scheduler()
        except Exception as exc:
            logger.error("Outreach scheduler loop: error during run — %s", exc)

        # Wait 60 minutes before next check
        await _asyncio.sleep(60 * 60)


# ── Manual single-partner dispatcher (unchanged interface) ─────────────────────

async def run_outreach_workflow(
    partner: dict,
    channels: list,
    custom_message: str = "",
) -> dict:
    """
    Manual single-partner outreach — called from the UI / API.
    Uses touch 1 templates. Does NOT write to outreach_sequence
    (use send_sequence_touch for tracked outreach).
    """
    name = partner.get("partner_name", "Unknown")
    logger.info("Outreach workflow (manual): %r → channels=%s", name, channels)

    _CHANNEL_MAP = {
        "whatsapp": lambda p, _: _send_whatsapp(p, 1),
        "email":    lambda p, _: _send_email(p, 1),
        "linkedin": lambda p, _: _send_linkedin(p, 1),
        "voice":    _run_voice_channel,
    }

    results = []
    for channel in channels:
        handler = _CHANNEL_MAP.get(channel)
        if handler:
            channel_result = await handler(partner, custom_message)
        else:
            channel_result = {"status": "not_implemented", "note": f"{channel} not wired"}
        logger.info("Outreach: %r channel=%s → status=%s", name, channel, channel_result.get("status"))
        results.append({"channel": channel, "result": channel_result})

    return {"lead_name": name, "results": results}