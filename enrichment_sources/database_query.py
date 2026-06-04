"""
enrichment_sources/database_query.py
-------------------------------------
Internal Database API enrichment source — Priority 2 in the fallback chain.

This is an internal company database (separate from the main `partners` table)
that may hold additional or more up-to-date contact information for known
businesses.

TODO: Implement this function to query your internal database and return a
dict with any of the following contact fields:
    {
        "phone_number": str | None,
        "email_id":     str | None,
        "linkedin_profile": str | None,
    }
Return an empty dict {} if the business is not found.

Environment variable required: DATABASE_API_KEY (or use DB_* vars from .env)
"""

import os

DATABASE_API_KEY = os.getenv("DATABASE_API_KEY", "")


async def query_database(business_name: str, detail: str) -> dict:
    """
    Query the internal company database for contact information.

    Parameters
    ----------
    business_name : str
        The name of the business / partner to look up.
    detail : str
        Additional context about the business (category + subcategories joined).

    Returns
    -------
    dict
        A dict with zero or more of these keys:
            "phone_number", "email_id", "linkedin_profile"
        Missing keys mean the source has no data for that field.
        Return {} if the business is not found at all.
    """
    # TODO: Implement internal database lookup.
    # Example steps:
    #   1. Connect to the internal DB using DATABASE_API_KEY or DB_* env vars.
    #   2. Query by business_name (fuzzy match recommended).
    #   3. Return matched contact fields as a dict.
    #   4. Return {} if no match found.
    return {}
