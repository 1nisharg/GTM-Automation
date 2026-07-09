"""
voice_agent/calendar_invite.py
--------------------------------
Sends a calendar invite for a scheduled follow-up call, triggered
automatically from post-call processing when the transcript confirms a
specific date and time was agreed with the partner.

Approach: standard .ics (iCalendar, RFC 5545) file emailed as an
attachment via the existing Gmail SMTP credentials already used
elsewhere in this project (nodes/outreach/outreach_workflow.py). This
deliberately avoids the Google Calendar API — no OAuth consent screen,
no service account, no Google Cloud Console setup. Gmail, Outlook, and
Apple Calendar all auto-detect a .ics attachment and render it as an
interactive "Add to Calendar" / RSVP invite in the recipient's inbox,
which is functionally identical to a native Google Calendar invite from
the partner's point of view.

Public entry point (matches the exact call site already in engine.py's
_post_process()):

    await maybe_send_calendar_invite(
        partner_name=..., partner_email=..., summary=parsed, call_sid=...,
    )

`summary` is the structured dict returned by _summarise_with_groq() in
engine.py, which must include:
    meeting_scheduled : bool
    meeting_date       : str | None   "YYYY-MM-DD"
    meeting_time       : str | None   "HH:MM" (24-hour, Dubai local time)

Only sends an invite when meeting_scheduled is True AND both date and
time parsed successfully into a real datetime — never sends a vague or
speculative invite off a merely "positive sentiment" call.

Environment variables required (all already present in .env for the
existing email outreach channel — no new credentials needed):
    EMAIL_ADDRESS / EMAIL_PASSWORD   — Gmail account + App Password
    SMTP_SERVER (default smtp.gmail.com)
    SMTP_PORT   (default 587)
    EMAIL_FROM  (default = EMAIL_ADDRESS)
"""

import asyncio
import logging
import os
import re
import smtplib
import uuid
from datetime import datetime, timedelta, timezone
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders

logger = logging.getLogger(__name__)

GMAIL_USER     = os.getenv("EMAIL_ADDRESS", "")
GMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD", "")
SMTP_SERVER    = os.getenv("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT      = int(os.getenv("SMTP_PORT", "587"))
EMAIL_FROM     = os.getenv("EMAIL_FROM", GMAIL_USER)

# Meeting duration — matches the "20-minute discussion" promised in the
# live call script (voice_agent/engine.py _system_prompt STEP 6).
_MEETING_DURATION_MINUTES = 20

# The call script promises meetings in Dubai business context. Partner
# times captured during the call are assumed to be Gulf Standard Time
# (UTC+4, no DST) unless stated otherwise — this is the correct default
# for a UAE supplier outreach call.
_DUBAI_UTC_OFFSET = timedelta(hours=4)

_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _is_valid_email(email: str) -> bool:
    return bool(email and _EMAIL_RE.match(email.strip()))


def _parse_meeting_datetime(meeting_date: str, meeting_time: str) -> "datetime | None":
    """
    Parse the LLM-extracted date/time strings into a timezone-aware UTC
    datetime. Returns None if either field is missing or unparseable —
    caller must treat that as "do not send an invite", never guess.
    """
    if not meeting_date or not meeting_time:
        return None

    meeting_date = meeting_date.strip()
    meeting_time = meeting_time.strip()

    # Expected formats: date "YYYY-MM-DD", time "HH:MM" (24-hour).
    # Be tolerant of minor LLM formatting drift (e.g. "9:00" -> "09:00").
    try:
        if re.match(r"^\d{1,2}:\d{2}$", meeting_time):
            parts = meeting_time.split(":")
            meeting_time = f"{int(parts[0]):02d}:{parts[1]}"

        local_naive = datetime.strptime(f"{meeting_date} {meeting_time}", "%Y-%m-%d %H:%M")
    except ValueError:
        logger.warning(
            "calendar_invite: could not parse meeting_date=%r meeting_time=%r — skipping invite.",
            meeting_date, meeting_time,
        )
        return None

    # Reject dates in the past (a mis-parsed year, e.g. "2022" from the
    # partner saying "'22" for a day-of-month, must never send a
    # calendar invite for a date that has already happened).
    local_aware = local_naive.replace(tzinfo=timezone(_DUBAI_UTC_OFFSET))
    now_dubai = datetime.now(timezone(_DUBAI_UTC_OFFSET))
    if local_aware < now_dubai:
        logger.warning(
            "calendar_invite: parsed meeting datetime %s is in the past — skipping invite.",
            local_aware.isoformat(),
        )
        return None

    return local_aware.astimezone(timezone.utc)


def _build_ics(
    start_utc: datetime,
    partner_name: str,
    partner_email: str,
    call_sid: str,
) -> str:
    """
    Build a minimal, valid RFC 5545 .ics calendar invite as plain text.
    No external library needed — this format is simple enough to build
    directly and every major calendar client (Google, Outlook, Apple)
    parses it identically.
    """
    end_utc = start_utc + timedelta(minutes=_MEETING_DURATION_MINUTES)

    def _fmt(dt: datetime) -> str:
        return dt.strftime("%Y%m%dT%H%M%SZ")

    uid = f"{uuid.uuid4()}@aarna-gtm"
    dtstamp = _fmt(datetime.now(timezone.utc))

    summary = "Aarna Partnership Discussion"
    description = (
        f"20-minute call with a Regional Partnership Leader from Aarna "
        f"(part of the Mondee Group) to discuss onboarding {partner_name or 'your business'} "
        f"as a supplier partner.\\n\\nThis time was agreed during our outbound call "
        f"(reference: {call_sid})."
    )

    # \r\n line endings and 75-octet line folding are technically required
    # by RFC 5545, but every major calendar client tolerates unfolded
    # lines for content this short — kept simple deliberately.
    ics = (
        "BEGIN:VCALENDAR\r\n"
        "VERSION:2.0\r\n"
        "PRODID:-//Aarna GTM//Partnership Outreach//EN\r\n"
        "CALSCALE:GREGORIAN\r\n"
        "METHOD:REQUEST\r\n"
        "BEGIN:VEVENT\r\n"
        f"UID:{uid}\r\n"
        f"DTSTAMP:{dtstamp}\r\n"
        f"DTSTART:{_fmt(start_utc)}\r\n"
        f"DTEND:{_fmt(end_utc)}\r\n"
        f"SUMMARY:{summary}\r\n"
        f"DESCRIPTION:{description}\r\n"
        f"ORGANIZER;CN=Aarna Partnerships:mailto:{EMAIL_FROM}\r\n"
        f"ATTENDEE;CN={partner_name or 'Partner'};RSVP=TRUE:mailto:{partner_email}\r\n"
        "STATUS:CONFIRMED\r\n"
        "SEQUENCE:0\r\n"
        "END:VEVENT\r\n"
        "END:VCALENDAR\r\n"
    )
    return ics


def _send_ics_email_blocking(
    to_email: str,
    partner_name: str,
    ics_content: str,
    start_utc: datetime,
) -> bool:
    """
    Send the .ics file as an email attachment via Gmail SMTP.

    NOTE: this is a BLOCKING, synchronous function (smtplib has no async
    API). It must always be called via asyncio.to_thread() — see
    maybe_send_calendar_invite() below — never awaited or called directly
    from async code, or it will freeze the entire event loop (every other
    concurrent call, API request, and outreach on the server) for however
    long the SMTP operation takes.

    An explicit connection timeout is set below specifically to bound the
    worst case — without it, smtplib.SMTP() can hang indefinitely on a
    slow or unreachable connection, which is exactly what happened in a
    real production case: a scheduled call's post-processing stalled for
    ~85 seconds after the phone call itself had already completed
    cleanly, causing the pipeline's outer timeout to fire and incorrectly
    mark a fully successful call as status=timeout.
    """
    if not GMAIL_USER or not GMAIL_PASSWORD:
        logger.warning("calendar_invite: Gmail credentials not set — cannot send invite.")
        return False

    local_time = start_utc + _DUBAI_UTC_OFFSET
    display_time = local_time.strftime("%A, %d %B %Y at %H:%M") + " (Dubai time)"

    subject = "Calendar Invite: Aarna Partnership Discussion"
    body = (
        f"Hi{' ' + partner_name if partner_name else ''},\n\n"
        f"As discussed, here's your calendar invite for our partnership "
        f"discussion on {display_time}.\n\n"
        f"Simply open the attached invite to add it to your calendar — "
        f"it works with Google Calendar, Outlook, and Apple Calendar.\n\n"
        f"Looking forward to speaking with you.\n\n"
        f"Best regards,\n"
        f"Aarna Partnerships\n"
        f"Aarna.global | Mondee Group"
    )

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = to_email
    msg.attach(MIMEText(body, "plain"))

    attachment = MIMEBase("text", "calendar", method="REQUEST", name="invite.ics")
    attachment.set_payload(ics_content)
    encoders.encode_base64(attachment)
    attachment.add_header("Content-Disposition", "attachment", filename="invite.ics")
    msg.attach(attachment)

    try:
        # timeout=15 — explicit ceiling on the SMTP connection itself.
        # Without this, smtplib.SMTP() uses the global default socket
        # timeout, which if unset means NO timeout at all (blocks forever
        # on a hung/unreachable connection).
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(GMAIL_USER, GMAIL_PASSWORD)
            server.sendmail(EMAIL_FROM, to_email, msg.as_string())
        logger.info("calendar_invite: sent to %s for %s", to_email, display_time)
        return True
    except Exception as exc:
        logger.error("calendar_invite: SMTP send failed for %s — %s", to_email, exc)
        return False


async def maybe_send_calendar_invite(
    partner_name: str,
    partner_email: str,
    summary: dict,
    call_sid: str,
) -> bool:
    """
    Send a calendar invite ONLY if the post-call summary confirms a
    specific meeting date and time were actually agreed during the call.

    Deliberately does NOT trigger off sentiment alone — a call can be
    entirely positive with no concrete time agreed (e.g. "call me next
    quarter"), and sending a calendar invite with no real meeting time
    would be actively wrong. This requires the structured
    meeting_scheduled / meeting_date / meeting_time fields from the
    post-call Groq summary (see engine.py _summarise_with_groq).

    Returns True if an invite was sent, False otherwise (including all
    the "correctly decided not to send" cases — not sending is not an
    error condition and should never raise).
    """
    if not summary.get("meeting_scheduled"):
        logger.debug("calendar_invite: no meeting scheduled for SID=%s — skipping.", call_sid)
        return False

    if not _is_valid_email(partner_email):
        logger.warning(
            "calendar_invite: meeting was scheduled for SID=%s but no valid partner "
            "email on file (%r) — cannot send invite.",
            call_sid, partner_email,
        )
        return False

    start_utc = _parse_meeting_datetime(
        summary.get("meeting_date", ""),
        summary.get("meeting_time", ""),
    )
    if start_utc is None:
        return False

    ics_content = _build_ics(start_utc, partner_name, partner_email, call_sid)

    # CRITICAL: _send_ics_email_blocking() is synchronous (smtplib has no
    # async API) and makes real network calls. Running it directly here
    # would freeze the entire event loop — every other concurrent call,
    # API request, and outreach on the server — for the duration of the
    # SMTP operation. asyncio.to_thread() runs it on a separate thread so
    # the event loop stays responsive regardless of how slow the SMTP
    # connection is.
    #
    # asyncio.wait_for() is a second, independent safety net on top of
    # the SMTP-level timeout already set inside the blocking function —
    # even if that inner timeout somehow doesn't fire, this guarantees
    # maybe_send_calendar_invite() itself returns within 20 seconds no
    # matter what.
    try:
        return await asyncio.wait_for(
            asyncio.to_thread(
                _send_ics_email_blocking,
                partner_email, partner_name, ics_content, start_utc,
            ),
            timeout=20,
        )
    except asyncio.TimeoutError:
        logger.error(
            "calendar_invite: SMTP send timed out after 20s for SID=%s — "
            "giving up, call post-processing will continue normally.",
            call_sid,
        )
        return False