"""
enrichment_sources/hunter.py
-----------------------------
Hunter.io enrichment source — Priority 3 in the fallback chain.

Hunter.io is an email-finding service.  Given a company domain / name it
returns verified email addresses and sometimes associated names/social profiles.

TODO: Implement this function using the Hunter.io API.
Docs: https://hunter.io/api-documentation

Environment variable required: HUNTER_API_KEY
"""

import os

HUNTER_API_KEY = os.getenv("HUNTER_API_KEY", "")


async def query_hunter(business_name: str) -> dict:
    """
    Query Hunter.io for contact information for the given business.

    Parameters
    ----------
    business_name : str
        The name of the business / partner to look up.

    Returns
    -------
    dict
        A dict with zero or more of these keys:
            "phone_number", "email_id", "linkedin_profile"
        Missing keys mean Hunter.io has no data for that field.
        Return {} if the business is not found or the API call fails.
    """
    # TODO: Implement Hunter.io lookup.
    # Example steps:
    #   1. Use the Hunter.io Domain Search endpoint to find the company domain.
    #      GET https://api.hunter.io/v2/domain-search?company=<business_name>&api_key=HUNTER_API_KEY
    #   2. Extract email(s) from the response.
    #   3. Optionally use the Email Finder endpoint for a specific contact.
    #   4. Map response fields to {"email_id": ..., "phone_number": ..., "linkedin_profile": ...}
    #   5. Return {} on any error or if not found.
    return {}
