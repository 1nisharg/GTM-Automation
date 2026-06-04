"""
enrichment_sources/apollo.py
-----------------------------
Apollo.io enrichment source — Priority 4 in the fallback chain.

Apollo.io is a sales intelligence platform with a large database of company
and contact information, including phone numbers, emails, and LinkedIn profiles.

TODO: Implement this function using the Apollo.io API.
Docs: https://apolloio.github.io/apollo-api-docs/

Environment variable required: APOLLO_API_KEY
"""

import os

APOLLO_API_KEY = os.getenv("APOLLO_API_KEY", "")


async def query_apollo(business_name: str) -> dict:
    """
    Query Apollo.io for contact information for the given business.

    Parameters
    ----------
    business_name : str
        The name of the business / partner to look up.

    Returns
    -------
    dict
        A dict with zero or more of these keys:
            "phone_number", "email_id", "linkedin_profile"
        Missing keys mean Apollo.io has no data for that field.
        Return {} if the business is not found or the API call fails.
    """
    # TODO: Implement Apollo.io lookup.
    # Example steps:
    #   1. Call the Organization Search endpoint:
    #      POST https://api.apollo.io/v1/mixed_companies/search
    #      Body: {"q_organization_name": business_name, "api_key": APOLLO_API_KEY}
    #   2. Extract the first matched organization.
    #   3. If found, call the People Search endpoint to get contact details.
    #   4. Map response fields to {"email_id": ..., "phone_number": ..., "linkedin_profile": ...}
    #   5. Return {} on any error or if not found.
    return {}
