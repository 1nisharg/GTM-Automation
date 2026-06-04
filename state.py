"""
state.py
--------
Defines the GraphState TypedDict that flows through every node in the pipeline.
All nodes read from and write to this shared state object.
"""

from typing import TypedDict


class GraphState(TypedDict):
    """
    Shared state for the GTM UAE partner pipeline.

    Fields
    ------
    input_category : str
        The subcategory theme to search for, e.g. "Adventure & Extreme Sports".
        Set once at graph invocation time; read-only by all downstream nodes.

    discovered_partners : list[dict]
        Records returned by the Discovery node.
        Each dict maps normalised column names to their values:
            partner_name, digitisation, category, subcategories, website,
            product_count, status, integrated, region,
            phone_number, email_id, linkedin_profile, sheet_source
        Only records with status == "Yet to Start" are included.

    enriched_partners : list[dict]
        Records produced by the Enrichment node.
        Same schema as discovered_partners, but phone_number / email_id /
        linkedin_profile are filled in wherever possible via the fallback chain.
        Fields that remain unknown are set to None.
    """

    input_category: str
    discovered_partners: list[dict]
    enriched_partners: list[dict]
