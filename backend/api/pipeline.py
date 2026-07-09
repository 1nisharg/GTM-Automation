"""
backend/api/pipeline.py
-----------------------
Pipeline orchestration endpoint.

POST /api/pipeline/run
    Accepts: { "input_category": "Adventure & Extreme Sports" }
    Returns: Server-Sent Event stream of partial stage results.

GET /api/pipeline/categories
    Returns list of distinct categories for autocomplete.

GET /api/pipeline/status
    Returns current concurrency stats.

SSE Event format:
    { "run_id": "abc123", "stage": "discovery",  "status": "running" }
    { "run_id": "abc123", "stage": "discovery",  "status": "done", "data": {...}, "elapsed_s": 1.2 }
    { "run_id": "abc123", "stage": "error",       "status": "error", "message": "..." }
    { "run_id": "abc123", "stage": "complete",    "status": "done", "data": {...} }

Scalability levers
------------------
- MAX_CONCURRENT_PIPELINES (env var, default 5):
    How many pipeline runs may execute simultaneously across ALL users.
    Requests beyond this limit receive HTTP 503 immediately.
- ENRICH_CONCURRENCY (env var in enrichment_node.py, default 10):
    How many partners are enriched simultaneously within ONE run.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from typing import AsyncGenerator

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from db.connection import get_pool
from nodes.discovery_node import discovery_node
from nodes.enrichment_node import enrichment_node
from nodes.outreach.outreach_node import outreach_node

logger = logging.getLogger(__name__)

router = APIRouter()

# ──────────────────────────────────────────────────────────────────────────────
# Global pipeline concurrency limiter
# ──────────────────────────────────────────────────────────────────────────────
_MAX_CONCURRENT_PIPELINES: int = int(os.getenv("MAX_CONCURRENT_PIPELINES", "5"))
_pipeline_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_PIPELINES)


# ──────────────────────────────────────────────────────────────────────────────
# Request / Response schemas
# ──────────────────────────────────────────────────────────────────────────────

class PipelineRunRequest(BaseModel):
    input_category:     str = ""
    input_partner_name: str = ""


# ──────────────────────────────────────────────────────────────────────────────
# SSE helper
# ──────────────────────────────────────────────────────────────────────────────

def _sse(run_id: str, payload: dict) -> str:
    """Format a dict as an SSE data line, always including run_id."""
    payload["run_id"] = run_id
    return f"data: {json.dumps(payload)}\n\n"


# ──────────────────────────────────────────────────────────────────────────────
# Core streaming generator
# ──────────────────────────────────────────────────────────────────────────────

async def _run_pipeline_stream(input_category: str, run_id: str) -> AsyncGenerator[str, None]:
    """
    Runs each pipeline node sequentially, yielding SSE events between stages.
    Each event carries run_id for log correlation and elapsed_s for timing.
    """
    state: dict = {
        "input_category": input_category.strip(),
        "search_mode": "",          # filled by discovery_node: "name" | "category"
        "run_id": run_id,
        "discovered_partners": [],
        "enriched_partners": [],
    }

    t_total = time.monotonic()
    logger.info("[%s] Pipeline started: category=%r", run_id, input_category)

    # ── Stage 1: Discovery ─────────────────────────────────────────────────────
    t0 = time.monotonic()
    logger.info("[%s] ▶ DISCOVERY: searching for category=%r", run_id, input_category)
    yield _sse(run_id, {"stage": "discovery", "status": "running"})
    await asyncio.sleep(0)

    try:
        discovery_result = await discovery_node(state)
        state.update(discovery_result or {})
        discovered = state.get("discovered_partners", [])
        elapsed = round(time.monotonic() - t0, 2)
        logger.info("[%s] ✓ DISCOVERY: %d partners found in %.2fs (mode=%s).", run_id, len(discovered), elapsed, state.get("search_mode", "?"))
        search_mode = state.get("search_mode", "category")
        logger.info("[%s] ✓ DISCOVERY: mode=%s", run_id, search_mode)
        yield _sse(run_id, {
            "stage": "discovery", "status": "done", "elapsed_s": elapsed,
            "data": {
                "count": len(discovered),
                "search_mode": search_mode,
                "partners": _safe_partners(discovered),
            },
        })
    except Exception as exc:
        logger.exception("[%s] ✗ DISCOVERY failed: %s", run_id, exc)
        yield _sse(run_id, {"stage": "error", "status": "error", "message": f"Discovery failed: {exc}"})
        return

    await asyncio.sleep(0)

    # ── Stage 2: Enrichment ────────────────────────────────────────────────────
    t0 = time.monotonic()
    logger.info(
        "[%s] ▶ ENRICHMENT: enriching %d partners (concurrency=%s).",
        run_id, len(discovered), os.getenv("ENRICH_CONCURRENCY", "10"),
    )
    yield _sse(run_id, {"stage": "enrichment", "status": "running"})
    await asyncio.sleep(0)

    try:
        enrichment_result = await enrichment_node(state)
        state.update(enrichment_result or {})
        enriched = state.get("enriched_partners", [])
        elapsed = round(time.monotonic() - t0, 2)
        fill = _compute_fill_stats(enriched)
        logger.info(
            "[%s] ✓ ENRICHMENT: %d partners in %.2fs (phone=%d email=%d linkedin=%d).",
            run_id, len(enriched), elapsed, fill["phone"], fill["email"], fill["linkedin"],
        )
        yield _sse(run_id, {
            "stage": "enrichment", "status": "done", "elapsed_s": elapsed,
            "data": {"count": len(enriched), "partners": _safe_partners(enriched), "fill_stats": fill},
        })
    except Exception as exc:
        logger.exception("[%s] ✗ ENRICHMENT failed: %s", run_id, exc)
        yield _sse(run_id, {"stage": "error", "status": "error", "message": f"Enrichment failed: {exc}"})
        return

    await asyncio.sleep(0)

    # ── Stage 3: Outreach ──────────────────────────────────────────────────────
    t0 = time.monotonic()
    logger.info("[%s] ▶ OUTREACH: executing outreach sequence.", run_id)
    yield _sse(run_id, {"stage": "outreach", "status": "running"})
    await asyncio.sleep(0)

    try:
        outreach_result = await outreach_node(state)
        state.update(outreach_result or {})
        outreach_data = state.get("outreach_results", [])
        elapsed = round(time.monotonic() - t0, 2)
        count = len(outreach_data) if isinstance(outreach_data, list) else 0
        logger.info("[%s] ✓ OUTREACH: %d results in %.2fs.", run_id, count, elapsed)
        yield _sse(run_id, {
            "stage": "outreach", "status": "done", "elapsed_s": elapsed,
            "data": {"count": count, "results": outreach_data if isinstance(outreach_data, list) else []},
        })
    except Exception as exc:
        logger.exception("[%s] ✗ OUTREACH failed: %s", run_id, exc)
        yield _sse(run_id, {"stage": "error", "status": "error", "message": f"Outreach failed: {exc}"})
        return

    await asyncio.sleep(0)

    # ── Credit summary ─────────────────────────────────────────────────────────
    # Count API calls made this run from apollo_usage + estimate Hunter/Tavily
    # from enriched partner count (1 Hunter call + 1 Tavily call per partner).
    enriched_count = len(state.get("enriched_partners", []))
    apollo_credits = 0
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COALESCE(SUM(credits_used), 0) AS total FROM apollo_usage WHERE run_id = $1",
                run_id,
            )
            apollo_credits = int(row["total"]) if row else 0
    except Exception:
        pass  # non-critical

    hunter_calls  = enriched_count          # 1 domain_search per partner
    tavily_calls  = enriched_count          # 1 linkedin_search per partner
    total_credits = apollo_credits + hunter_calls + tavily_calls

    credit_summary = (
        f"Apollo: {apollo_credits} credits  |  "
        f"Hunter: ~{hunter_calls} requests  |  "
        f"Tavily: ~{tavily_calls} searches  |  "
        f"Total: ~{total_credits} API calls"
    )
    logger.info("[%s] 💳 CREDITS USED THIS RUN — %s", run_id, credit_summary)

    # ── Complete ───────────────────────────────────────────────────────────────
    total_elapsed = round(time.monotonic() - t_total, 2)
    logger.info(
        "[%s] ✅ PIPELINE COMPLETE in %.2fs — discovered=%d enriched=%d outreach=%d",
        run_id, total_elapsed,
        len(state.get("discovered_partners", [])),
        len(state.get("enriched_partners", [])),
        len(state.get("outreach_results", [])) if isinstance(state.get("outreach_results"), list) else 0,
    )
    yield _sse(run_id, {
        "stage": "complete", "status": "done", "elapsed_s": total_elapsed,
        "data": {
            "input_category":   input_category,
            "discovered_count": len(state.get("discovered_partners", [])),
            "enriched_count":   enriched_count,
            "outreach_count":   len(state.get("outreach_results", [])) if isinstance(state.get("outreach_results"), list) else 0,
            "credits_used": {
                "apollo":  apollo_credits,
                "hunter":  hunter_calls,
                "tavily":  tavily_calls,
                "total":   total_credits,
                "summary": credit_summary,
            },
        },
    })


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _safe_partners(partners: list) -> list:
    """Return a JSON-safe subset of partner fields."""
    return [{
        "partner_name":     p.get("partner_name"),
        "category":         p.get("category"),
        "subcategories":    p.get("subcategories"),
        "website":          p.get("website"),
        "region":           p.get("region"),
        "status":           p.get("status"),
        "digitisation":     p.get("digitisation"),
        "phone_number":     p.get("phone_number"),
        "email_id":         p.get("email_id"),
        "linkedin_profile": p.get("linkedin_profile"),
        "sheet_source":     p.get("sheet_source"),
        "contact_name":     p.get("contact_name"),
        "contact_headline": p.get("contact_headline"),
    } for p in partners]


def _compute_fill_stats(partners: list) -> dict:
    """Compute how many partners have each contact field filled."""
    if not partners:
        return {"phone": 0, "email": 0, "linkedin": 0, "total": 0}
    total = len(partners)
    return {
        "total":    total,
        "phone":    sum(1 for p in partners if p.get("phone_number")),
        "email":    sum(1 for p in partners if p.get("email_id")),
        "linkedin": sum(1 for p in partners if p.get("linkedin_profile")),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Endpoints
# ──────────────────────────────────────────────────────────────────────────────

@router.post("/run")
async def run_pipeline(req: PipelineRunRequest):
    """
    Trigger the full pipeline for a given category or partner name.
    Accepts either:
      { "input_category": "Adventure & Extreme Sports" }   ← subcategory mode
      { "input_partner_name": "Adventure HQ" }             ← name mode
    Returns a Server-Sent Event stream.

    Concurrent pipeline runs are capped at MAX_CONCURRENT_PIPELINES (default 5).
    Requests beyond the cap get HTTP 503 immediately.
    """
    # Resolve which field was sent — name takes priority if both provided
    resolved_input = (req.input_partner_name or req.input_category or "").strip()

    if not resolved_input:
        raise HTTPException(status_code=400, detail="Provide either input_category or input_partner_name")

    # Non-blocking check — return 503 immediately if at capacity
    if _pipeline_semaphore._value == 0:  # type: ignore[attr-defined]
        logger.warning(
            "Pipeline capacity reached (%d slots full). Rejecting: input=%r.",
            _MAX_CONCURRENT_PIPELINES, resolved_input,
        )
        raise HTTPException(
            status_code=503,
            detail=f"Pipeline at capacity ({_MAX_CONCURRENT_PIPELINES} concurrent runs). "
                   "Please try again in a moment.",
        )

    run_id = uuid.uuid4().hex[:8]
    active = _MAX_CONCURRENT_PIPELINES - _pipeline_semaphore._value  # type: ignore
    logger.info(
        "New pipeline request: run_id=%s input=%r (slot %d/%d)",
        run_id, resolved_input, active + 1, _MAX_CONCURRENT_PIPELINES,
    )

    async def _guarded_stream():
        async with _pipeline_semaphore:
            async for chunk in _run_pipeline_stream(resolved_input, run_id):
                yield chunk

    return StreamingResponse(
        _guarded_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control":     "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":        "keep-alive",
            "X-Run-ID":          run_id,
        },
    )


@router.get("/categories")
async def get_categories():
    """
    Return individual subcategory tags for pipeline autocomplete.
    Uses the subcategory_tags array column — each tag is a clean individual value.
    Falls back to raw subcategories ILIKE search if tags not populated.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Get individual tags from the array column
        rows = await conn.fetch(
            """
            SELECT DISTINCT TRIM(tag) AS tag
            FROM partners,
                 UNNEST(subcategory_tags) AS tag
            WHERE tag IS NOT NULL
              AND TRIM(tag) != ''
            ORDER BY tag
            """
        )
    tags = [r["tag"] for r in rows if r["tag"]]

    # Fallback — if tags not populated yet, return raw subcategories
    if not tags:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT DISTINCT category FROM partners WHERE category IS NOT NULL ORDER BY category"
            )
        tags = [r["category"] for r in rows if r["category"]]

    return {"categories": tags}


@router.get("/status")
async def pipeline_status():
    """Return current pipeline concurrency info — useful for ops dashboards."""
    available = _pipeline_semaphore._value  # type: ignore[attr-defined]
    active    = _MAX_CONCURRENT_PIPELINES - available
    return {
        "max_concurrent":     _MAX_CONCURRENT_PIPELINES,
        "active_pipelines":   active,
        "available_slots":    available,
        "enrich_concurrency": int(os.getenv("ENRICH_CONCURRENCY", "10")),
    }