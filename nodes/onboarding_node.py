"""
nodes/onboarding_node.py
-------------------------
Onboarding Node — Stage 4 of the pipeline (post-Outreach).

STUB — not yet implemented.

Purpose (when implemented)
--------------------------
After a partner responds positively to outreach, this node handles the
onboarding process:
  - Collect partner agreements / contracts.
  - Provision partner access to the platform.
  - Update partner status to "Onboarding" in the database.

Input  (GraphState field): TBD (output of outreach_node)
Output (GraphState field): TBD
"""

from state import GraphState


async def onboarding_node(state: GraphState) -> dict:
    # TODO: Implement partner onboarding logic.
    pass
