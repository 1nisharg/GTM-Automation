"""
nodes/discovery_node.py
-----------------------
Discovery Node — Stage 1 of the pipeline.

Purpose
-------
Given an input category (subcategory theme), query the PostgreSQL `partners`
table for all records where:
  1. `subcategories` contains the input_category (case-insensitive substring match)
  2. `status` == "Yet to Start"

Returns
-------
Updates GraphState["discovered_partners"] with a list of dicts, one per matching
partner, using the normalised column keys defined in db/models.py.
"""

import logging

from db.connection import get_pool
from db.models import STATUS_TO_ENRICH
from state import GraphState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL query
# ---------------------------------------------------------------------------
# Uses ILIKE for case-insensitive substring matching on subcategories.
# Both track1 and track2 rows are stored in the same `partners` table,
# distinguished by the `sheet_source` column.
_DISCOVERY_QUERY = """
SELECT
    partner_name,
    digitisation,
    category,
    subcategories,
    website,
    product_count,
    status,
    integrated,
    region,
    phone_number,
    email_id,
    linkedin_profile,
    sheet_source
FROM partners
WHERE status = $1
  AND subcategories ILIKE $2
ORDER BY sheet_source, partner_name;
"""


async def discovery_node(state: GraphState) -> dict:
    """
    LangGraph node: discover partners matching the requested subcategory.

    Parameters
    ----------
    state : GraphState
        Must contain `input_category` (str).

    Returns
    -------
    dict
        Partial state update: {"discovered_partners": list[dict]}
    """
    input_category = state["input_category"].strip()
    like_pattern = f"%{input_category}%"

    logger.info(
        "Discovery node: searching for subcategory=%r, status=%r",
        input_category,
        STATUS_TO_ENRICH,
    )

    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_DISCOVERY_QUERY, STATUS_TO_ENRICH, like_pattern)

    discovered = [dict(row) for row in rows]

    logger.info("Discovery node: found %d partners.", len(discovered))

    return {"discovered_partners": discovered}
