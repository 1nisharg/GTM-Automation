"""
db/connection.py
----------------
Async PostgreSQL connection pool using asyncpg.
Credentials are loaded from individual environment variables so each
can be rotated independently without rebuilding a connection string.

Required .env vars
------------------
DB_HOST      e.g. localhost
DB_PORT      e.g. 5432
DB_NAME      e.g. gtm_uae
DB_USER      e.g. postgres
DB_PASSWORD  (your password)
"""

import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Module-level pool — initialised once via init_pool() and reused across nodes
# ---------------------------------------------------------------------------
_pool: asyncpg.Pool | None = None


async def init_pool() -> asyncpg.Pool:
    """
    Create (or return the existing) asyncpg connection pool.

    Call this once at application startup (e.g., in graph.py's __main__ block
    or a graph lifecycle hook).  All nodes then call get_pool() to borrow a
    connection.
    """
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            host=os.getenv("DB_HOST", "localhost"),
            port=int(os.getenv("DB_PORT", "5432")),
            database=os.getenv("DB_NAME", "gtm_uae"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", ""),
            min_size=1,
            max_size=5,
        )
    return _pool


async def get_pool() -> asyncpg.Pool:
    """Return the active pool, initialising it if necessary."""
    return await init_pool()


async def close_pool() -> None:
    """Gracefully close the connection pool.  Call at shutdown."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
