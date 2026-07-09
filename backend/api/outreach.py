"""
backend/api/outreach.py
-----------------------
Outreach endpoints.

GET  /api/outreach/              — List enriched partners ready for outreach
POST /api/outreach/launch        — Trigger Touch 1 of the sequence for a partner
POST /api/outreach/run-scheduler — Manually trigger the sequence scheduler
"""

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from db.connection import get_pool

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# Import sequence workflow
# ---------------------------------------------------------------------------
try:
    from nodes.outreach.outreach_workflow import (
        send_sequence_touch,
        run_sequence_scheduler,
        run_outreach_workflow,
        get_channels_for_partner,
    )
    _HAS_REAL_WORKFLOW = True
    logger.info("Outreach: loaded sequence workflow.")
except ImportError as e:
    _HAS_REAL_WORKFLOW = False
    logger.warning("Outreach: workflow import failed — %s", e)

    async def send_sequence_touch(partner, touch_number):
        return [{"channel": "stub", "status": "stub_pending"}]

    async def run_sequence_scheduler():
        return {"fired": 0, "skipped": 0, "errors": 0}

    async def run_outreach_workflow(partner, channels, custom_message=""):
        return {"lead_name": partner.get("partner_name"), "results": []}

    def get_channels_for_partner(partner, touch_number):
        return ["whatsapp"]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

class TestOverride(BaseModel):
    phone_number:     str = ""
    email_id:         str = ""
    linkedin_profile: str = ""
    contact_name:     str = ""
    category:         str = ""
    company_synopsis: str = ""

class OutreachLaunchRequest(BaseModel):
    partner_name:   str
    channels:       list[str] = []        # if empty, auto-decided by digitisation tier
    custom_message: str = ""
    test_override:  TestOverride = TestOverride()
    touch_number:   int = 1               # which touch to send (1/2/3) — default Day 1


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/")
async def list_outreach_partners(
    search: Optional[str] = Query(None),
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    Returns enriched partners with at least one contact field,
    plus per-channel counts for stats cards.
    Also shows outreach sequence status for each partner.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT p.id, p.partner_name, p.category, p.subcategories, p.region,
                   p.phone_number, p.email_id, p.linkedin_profile, p.status,
                   p.sheet_source, p.digitisation, p.company_synopsis,
                   s.touch_number AS last_touch,
                   s.channel      AS last_channel_used,
                   s.sent_at      AS last_sent_at,
                   s.next_touch_due
            FROM partners p
            LEFT JOIN LATERAL (
                SELECT touch_number, channel, sent_at, next_touch_due
                FROM outreach_sequence
                WHERE partner_id = p.id
                ORDER BY touch_number DESC, sent_at DESC
                LIMIT 1
            ) s ON true
            WHERE (p.phone_number IS NOT NULL AND p.phone_number != '')
               OR (p.email_id IS NOT NULL AND p.email_id != '')
               OR (p.linkedin_profile IS NOT NULL AND p.linkedin_profile != '')
            ORDER BY p.partner_name
            LIMIT $1 OFFSET $2
            """,
            limit, offset,
        )

        stats_rows = await conn.fetch(
            """
            SELECT
                SUM(CASE WHEN phone_number IS NOT NULL AND phone_number != '' THEN 1 ELSE 0 END)        AS whatsapp,
                SUM(CASE WHEN email_id IS NOT NULL AND email_id != '' THEN 1 ELSE 0 END)                 AS email,
                SUM(CASE WHEN linkedin_profile IS NOT NULL AND linkedin_profile != '' THEN 1 ELSE 0 END) AS linkedin
            FROM partners
            """
        )

    leads = []
    for r in rows:
        p = dict(r)
        name_lower = (p.get("partner_name") or "").lower()
        if search and search.lower() not in name_lower:
            continue

        last_touch = p.get("last_touch")
        next_due   = p.get("next_touch_due")
        seq_status = "Not started"
        if last_touch == 1:
            seq_status = f"Touch 1 sent — Touch 2 due {next_due.strftime('%Y-%m-%d') if next_due else 'N/A'}"
        elif last_touch == 2:
            seq_status = f"Touch 2 sent — Touch 3 due {next_due.strftime('%Y-%m-%d') if next_due else 'N/A'}"
        elif last_touch == 3:
            seq_status = "Sequence complete (Touch 3 sent)"

        leads.append({
            "id":             str(p["id"]),
            "business_name":  p.get("partner_name"),
            "category":       p.get("category"),
            "company_synopsis": p.get("company_synopsis"),
            "digitisation":   p.get("digitisation"),
            "score":          75,
            "score_tier":     "WARM",
            "phone":          p.get("phone_number"),
            "email":          p.get("email_id"),
            "linkedin_url":   p.get("linkedin_profile"),
            "instagram":      None,
            "last_touch":     last_touch,
            "next_touch_due": next_due.isoformat() if next_due else None,
            "sequence_status": seq_status,
            "last_channel":   p.get("last_channel_used") or _last_channel(p),
            "last_status":    "Pending",
            "region":         p.get("region"),
            "sheet_source":   p.get("sheet_source"),
        })

    stats = dict(stats_rows[0]) if stats_rows else {}
    channels = [
        {"channel": "whatsapp", "count": int(stats.get("whatsapp") or 0)},
        {"channel": "email",    "count": int(stats.get("email")    or 0)},
        {"channel": "linkedin", "count": int(stats.get("linkedin") or 0)},
        {"channel": "voice",    "count": 0},
        {"channel": "instagram","count": 0},
    ]

    return {"leads": leads, "channels": channels}


def _last_channel(partner: dict) -> str:
    if partner.get("phone_number"):  return "whatsapp"
    if partner.get("email_id"):      return "email"
    if partner.get("linkedin_profile"): return "linkedin"
    return "—"


@router.post("/launch")
async def launch_outreach(req: OutreachLaunchRequest):
    """
    Launch Touch 1 of the outreach sequence for a partner.

    - Fires immediately — no time-of-day restriction
    - Channel is auto-selected based on digitisation tier (per AI Behaviour doc)
      unless channels list is explicitly provided
    - Writes to outreach_sequence with next_touch_due set to Day 3
    - Scheduler picks up Day 3 and Day 12 automatically

    touch_number defaults to 1 (first contact). Pass touch_number=2 or 3
    to manually fire a specific touch for testing.
    """
    if not req.partner_name.strip():
        raise HTTPException(status_code=400, detail="partner_name is required")

    if req.touch_number not in (1, 2, 3):
        raise HTTPException(status_code=400, detail="touch_number must be 1, 2, or 3")

    # Fetch partner record from DB
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, partner_name, category, digitisation, contact_name,
                   company_synopsis, phone_number, email_id, linkedin_profile,
                   website, region
            FROM partners WHERE partner_name ILIKE $1 LIMIT 1
            """,
            req.partner_name.strip(),
        )

    partner = dict(row) if row else {"partner_name": req.partner_name}

    # Apply test overrides (lets you send to a test number without changing DB)
    ov = req.test_override
    if ov.phone_number:      partner["phone_number"]     = ov.phone_number
    if ov.email_id:          partner["email_id"]         = ov.email_id
    if ov.linkedin_profile:  partner["linkedin_profile"] = ov.linkedin_profile
    if ov.contact_name:      partner["contact_name"]     = ov.contact_name
    if ov.category:          partner["category"]         = ov.category
    if ov.company_synopsis:  partner["company_synopsis"] = ov.company_synopsis
    if any([ov.phone_number, ov.email_id, ov.linkedin_profile, ov.contact_name, ov.category, ov.company_synopsis]):
        logger.info("Outreach launch: test override active")

    logger.info(
        "Outreach launch: partner=%r touch=%d digitisation=%r workflow=%s",
        req.partner_name, req.touch_number,
        partner.get("digitisation"), _HAS_REAL_WORKFLOW,
    )

    # Fire the sequence touch — channel auto-selected by digitisation tier
    # unless explicitly passed in channels list (for manual override)
    if req.channels:
        # Manual channel override — use run_outreach_workflow (backwards compat)
        result = await run_outreach_workflow(
            partner=partner,
            channels=req.channels,
            custom_message=req.custom_message,
        )
        return result

    # Auto channel decision + sequence tracking
    touch_results = await send_sequence_touch(partner, req.touch_number)

    return {
        "lead_name":    partner.get("partner_name"),
        "touch_number": req.touch_number,
        "results":      touch_results,
        "note": (
            f"Touch {req.touch_number} fired. "
            + ("Day 3 follow-up queued automatically." if req.touch_number == 1 else "")
            + ("Day 12 soft close queued automatically." if req.touch_number == 2 else "")
            + ("Sequence complete." if req.touch_number == 3 else "")
        ),
    }


@router.post("/run-scheduler")
async def run_scheduler_now():
    """
    Manually trigger the outreach sequence scheduler.
    Checks all partners with next_touch_due <= now and fires their next touch.
    Useful for testing without waiting for the hourly background task.
    """
    logger.info("Outreach scheduler: manual trigger via API")
    result = await run_sequence_scheduler()
    return {
        "status": "complete",
        "fired":   result.get("fired", 0),
        "skipped": result.get("skipped", 0),
        "errors":  result.get("errors", 0),
    }