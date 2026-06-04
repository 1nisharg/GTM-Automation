"""
graph.py
--------
GTM UAE Partner Pipeline — LangGraph StateGraph definition.

Pipeline topology
-----------------
  discovery → enrichment → outreach → onboarding → documentation → boarding_live

Stages 1-2 (discovery, enrichment) are fully implemented.
Stage 3 (outreach) is stubbed — owned by the outreach team.
Stages 4-6 (onboarding, documentation, boarding_live) are placeholder stubs
to be wired in later.

Running
-------
  # Quick smoke-test with a sample category:
  python graph.py

  # Or invoke programmatically:
  import asyncio
  from graph import build_graph

  async def main():
      graph = build_graph()
      result = await graph.ainvoke({"input_category": "Adventure & Extreme Sports"})
      print(result)

  asyncio.run(main())
"""

import asyncio
import logging
import pprint

from langgraph.graph import StateGraph

from db.connection import close_pool, init_pool
from nodes.boarding_live_node import boarding_live_node
from nodes.discovery_node import discovery_node
from nodes.documentation_node import documentation_node
from nodes.enrichment_node import enrichment_node
from nodes.onboarding_node import onboarding_node
from nodes.outreach.outreach_node import outreach_node
from state import GraphState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def build_graph() -> StateGraph:
    """
    Construct and compile the GTM UAE partner pipeline graph.

    Returns
    -------
    CompiledStateGraph
        A compiled LangGraph graph ready for ainvoke / invoke.
    """
    builder = StateGraph(GraphState)

    # ------------------------------------------------------------------
    # Register nodes
    # ------------------------------------------------------------------
    builder.add_node("discovery", discovery_node)
    builder.add_node("enrichment", enrichment_node)
    builder.add_node("outreach", outreach_node)           # Stage 3 — stub
    builder.add_node("onboarding", onboarding_node)       # Stage 4 — stub
    builder.add_node("documentation", documentation_node) # Stage 5 — stub
    builder.add_node("boarding_live", boarding_live_node) # Stage 6 — stub

    # ------------------------------------------------------------------
    # Wire edges: discovery → enrichment → outreach → onboarding
    #             → documentation → boarding_live
    # ------------------------------------------------------------------
    builder.set_entry_point("discovery")
    builder.add_edge("discovery", "enrichment")
    builder.add_edge("enrichment", "outreach")
    builder.add_edge("outreach", "onboarding")
    builder.add_edge("onboarding", "documentation")
    builder.add_edge("documentation", "boarding_live")
    builder.set_finish_point("boarding_live")

    return builder.compile()


# ---------------------------------------------------------------------------
# Quick smoke-test entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Replace this value with any subcategory from the `partners` table, e.g.:
    #   "Adventure & Extreme Sports"
    #   "Wellness, Spa & Mindfulness"
    #   "Festivals & Cultural Celebrations"
    SAMPLE_CATEGORY = "Adventure & Extreme Sports"

    async def run() -> None:
        logger.info("=== GTM UAE Partner Pipeline — smoke test ===")
        logger.info("Input category: %r", SAMPLE_CATEGORY)

        # Initialise DB pool once at startup
        await init_pool()

        try:
            graph = build_graph()
            result = await graph.ainvoke(
                {
                    "input_category": SAMPLE_CATEGORY,
                    "discovered_partners": [],
                    "enriched_partners": [],
                }
            )

            discovered = result.get("discovered_partners", [])
            enriched = result.get("enriched_partners", [])

            logger.info("--- Discovery results: %d partners ---", len(discovered))
            logger.info("--- Enrichment results: %d partners ---", len(enriched))

            if enriched:
                logger.info("Sample enriched partner:")
                pprint.pprint(enriched[0], indent=2)
            else:
                logger.info("No partners found for category %r.", SAMPLE_CATEGORY)
                logger.info(
                    "Tip: ensure the `partners` PostgreSQL table is populated "
                    "(see db/models.py for the DDL) and DB_* env vars are set."
                )
        finally:
            await close_pool()

    asyncio.run(run())
