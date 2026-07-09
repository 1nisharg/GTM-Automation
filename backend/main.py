"""
backend/main.py
---------------
FastAPI application entry point for the GTM UAE Partner Acquisition Pipeline.

Run from the project root with:
    uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
"""
import sys
from pathlib import Path
import asyncio
sys.path.append(str(Path(__file__).resolve().parent.parent))

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.connection import close_pool, init_pool
from db.db_init import run_startup_migrations
from voice_agent.tunnel import start_tunnel, stop_tunnel
from voice_agent.engine import router as voice_agent_router
from voice_agent.ozonetel_stream import router as ozonetel_router
from backend.api.pipeline import router as pipeline_router
from backend.api.partners import router as partners_router
from backend.api.outreach import router as outreach_router
from backend.api.analytics import router as analytics_router
from backend.api.discovery import router as discovery_router
from backend.api.enrichment import router as enrichment_router
from nodes.outreach.outreach_node import launch_outreach_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan: startup → DB pool + ngrok tunnel | shutdown → close both
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):

    # ── DB pool ────────────────────────────────────────────────────────────
    logger.info("Startup: initialising PostgreSQL connection pool…")
    try:
        await init_pool()
        logger.info("DB pool ready.")
    except Exception as exc:
        logger.warning(
            "DB pool init failed (%s). Endpoints will return 503 until "
            "the database is reachable.",
            exc,
        )

    # ── Auto-migrations + Excel seed ───────────────────────────────────────
    logger.info("Startup: running auto-migrations and Excel seed check…")
    try:
        await run_startup_migrations()
    except Exception as exc:
        logger.warning("Startup: migration/seed failed: %s", exc)

    # ── Outreach sequence scheduler (background task) ────────────────────
    logger.info("Startup: launching outreach sequence scheduler…")
    asyncio.create_task(launch_outreach_scheduler())

    # ── ngrok tunnel (voice agent webhooks) ────────────────────────────────
    logger.info("Startup: starting voice agent ngrok tunnel…")
    try:
        public_host = start_tunnel()
        if public_host:
            logger.info("Voice agent tunnel live — public host: https://%s", public_host)
        else:
            logger.warning(
                "Voice agent tunnel not started — NGROK_AUTH_TOKEN may be missing. "
                "Outreach voice calls will not work without a public webhook URL."
            )
    except Exception as exc:
        logger.warning("Voice agent tunnel startup failed: %s", exc)

    yield

    # ── Teardown ───────────────────────────────────────────────────────────
    logger.info("Shutdown: stopping ngrok tunnel…")
    try:
        stop_tunnel()
        logger.info("Tunnel stopped.")
    except Exception as exc:
        logger.warning("Tunnel stop error: %s", exc)

    logger.info("Shutdown: closing PostgreSQL connection pool…")
    try:
        await close_pool()
        logger.info("DB pool closed.")
    except Exception as exc:
        logger.warning("DB pool close error: %s", exc)


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="GTM UAE Partner Pipeline API",
    description="Agentic pipeline API: Discovery → Enrichment → Outreach → Onboarding",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS — allow the Next.js dev server and any local origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------
app.include_router(pipeline_router,    prefix="/api/pipeline",    tags=["Pipeline"])
app.include_router(partners_router,    prefix="/api/partners",    tags=["Partners"])
app.include_router(outreach_router,    prefix="/api/outreach",    tags=["Outreach"])
app.include_router(analytics_router,   prefix="/api/analytics",   tags=["Analytics"])
app.include_router(discovery_router,   prefix="/api/discovery",   tags=["Discovery"])
app.include_router(enrichment_router,  prefix="/api/enrichment",  tags=["Enrichment"])
app.include_router(voice_agent_router,                            tags=["Voice Agent"])
app.include_router(ozonetel_router,                                tags=["Ozonetel Voice Agent"])


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "service": "GTM UAE Pipeline API"}