"""
nodes/enrichment_node.py
------------------------
Enrichment Node — Stage 2 of the pipeline.

Purpose
-------
For each partner discovered in Stage 1, attempt to fill in any missing contact
fields (phone_number, email_id, linkedin_profile) using a strict prioritised
fallback chain:

    Priority 1 — Partners DB data (already in discovered_partners dict)
                 If the field is non-null and non-empty, use it directly.
    Priority 2 — Internal Database API  (enrichment_sources/database_query.py)
    Priority 3 — Hunter.io              (enrichment_sources/hunter.py)
    Priority 4 — Apollo.io             (enrichment_sources/apollo.py)
    Priority 5 — LinkedIn Sales Nav    (enrichment_sources/linkedin_sales_nav.py)

Rules
-----
- Per-field resolution: each contact field is resolved independently.
  e.g. email may come from Hunter while phone comes from Apollo.
- Stop early per field: as soon as a non-null, non-empty value is found,
  do not call lower-priority sources for that field.
- Failure tolerance: if all sources return null for a field, set it to None.
  Never raise an exception — return partial data and continue.
- Async + concurrent: all external source calls for a single partner are run
  concurrently via asyncio.gather for speed.

Extensibility
-------------
To add a new enrichment source:
  1. Create enrichment_sources/<new_source>.py with an async query function.
  2. Add it to enrichment_sources/__init__.py.
  3. Append it to the FALLBACK_CHAIN list below. That's it.
"""

import asyncio
import logging
from typing import Any

from enrichment_sources.apollo import query_apollo
from enrichment_sources.database_query import query_database
from enrichment_sources.hunter import query_hunter
from enrichment_sources.linkedin_sales_nav import query_linkedin
from state import GraphState

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Fields we attempt to enrich.  Order matters for logging clarity only;
# the fallback chain logic below is field-agnostic.
# ---------------------------------------------------------------------------
ENRICHABLE_FIELDS = ("phone_number", "email_id", "linkedin_profile")


def _is_present(value: Any) -> bool:
    """Return True if `value` is a non-null, non-whitespace-only string."""
    if value is None:
        return False
    return str(value).strip() != ""


async def _enrich_one_partner(partner: dict) -> dict:
    """
    Run the fallback chain for a single partner and return the enriched dict.

    Parameters
    ----------
    partner : dict
        A single record from discovered_partners.

    Returns
    -------
    dict
        Same dict with phone_number / email_id / linkedin_profile filled in
        where possible.  Unresolved fields are set to None.
    """
    enriched = dict(partner)

    business_name: str = partner.get("partner_name", "") or ""
    detail: str = " ".join(
        filter(None, [partner.get("category", ""), partner.get("subcategories", "")])
    )

    # ------------------------------------------------------------------
    # Priority 1: check what's already in the DB record
    # ------------------------------------------------------------------
    fields_needed = [f for f in ENRICHABLE_FIELDS if not _is_present(partner.get(f))]

    if not fields_needed:
        logger.debug("Partner %r: all fields already present in DB.", business_name)
        return enriched

    logger.debug(
        "Partner %r: missing fields %s — starting external enrichment.",
        business_name,
        fields_needed,
    )

    # ------------------------------------------------------------------
    # Priority 2–5: call external sources concurrently, then walk results
    # in priority order per field.
    # ------------------------------------------------------------------
    try:
        db_result, hunter_result, apollo_result, linkedin_result = await asyncio.gather(
            query_database(business_name, detail),
            query_hunter(business_name),
            query_apollo(business_name),
            query_linkedin(business_name),
            return_exceptions=True,  # never let one failure abort the rest
        )
    except Exception as exc:
        logger.error("Unexpected error during gather for %r: %s", business_name, exc)
        db_result = hunter_result = apollo_result = linkedin_result = {}

    # Convert any exceptions returned by gather into empty dicts
    source_results = []
    for name, result in [
        ("database", db_result),
        ("hunter", hunter_result),
        ("apollo", apollo_result),
        ("linkedin", linkedin_result),
    ]:
        if isinstance(result, Exception):
            logger.warning(
                "Partner %r: source %r raised %s — treating as empty.",
                business_name,
                name,
                result,
            )
            source_results.append({})
        else:
            source_results.append(result or {})

    db_res, hunter_res, apollo_res, linkedin_res = source_results

    # Walk fallback chain per field
    for field in fields_needed:
        resolved_value = None
        for source_name, source_data in [
            ("database", db_res),
            ("hunter", hunter_res),
            ("apollo", apollo_res),
            ("linkedin", linkedin_res),
        ]:
            candidate = source_data.get(field)
            if _is_present(candidate):
                enriched[field] = candidate
                resolved_value = candidate
                logger.debug(
                    "Partner %r: field %r resolved from source %r.",
                    business_name,
                    field,
                    source_name,
                )
                break

        if resolved_value is None:
            enriched[field] = None  # explicit None — all sources exhausted
            logger.debug(
                "Partner %r: field %r unresolved after all sources.", business_name, field
            )

    return enriched


async def enrichment_node(state: GraphState) -> dict:
    """
    LangGraph node: enrich all discovered partners concurrently.

    Parameters
    ----------
    state : GraphState
        Must contain `discovered_partners` (list[dict]).

    Returns
    -------
    dict
        Partial state update: {"enriched_partners": list[dict]}
    """
    discovered = state.get("discovered_partners", [])

    if not discovered:
        logger.warning("Enrichment node: no discovered partners to enrich.")
        return {"enriched_partners": []}

    logger.info("Enrichment node: enriching %d partners.", len(discovered))

    # Process all partners concurrently
    enriched_partners = await asyncio.gather(
        *[_enrich_one_partner(partner) for partner in discovered],
        return_exceptions=False,
    )

    logger.info("Enrichment node: completed enrichment for %d partners.", len(enriched_partners))

    return {"enriched_partners": list(enriched_partners)}
