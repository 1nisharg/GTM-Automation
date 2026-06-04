"""
enrichment_sources/linkedin_sales_nav.py
-----------------------------------------
LinkedIn Sales Navigator enrichment source — Priority 5 (last resort) in the
fallback chain.

LinkedIn Sales Navigator provides the most reliable source for LinkedIn profile
URLs and, where permitted, associated contact details.

TODO: Implement this function using the LinkedIn Sales Navigator API or a
compliant third-party wrapper.
Docs: https://learn.microsoft.com/en-us/linkedin/sales/

Environment variable required: LINKEDIN_API_KEY
"""

import os

LINKEDIN_API_KEY = os.getenv("LINKEDIN_API_KEY", "")


async def query_linkedin(business_name: str) -> dict:
    """
    Query LinkedIn Sales Navigator for contact information for the given business.

    Parameters
    ----------
    business_name : str
        The name of the business / partner to look up.

    Returns
    -------
    dict
        A dict with zero or more of these keys:
            "phone_number", "email_id", "linkedin_profile"
        Missing keys mean LinkedIn has no data for that field.
        Return {} if the business is not found or the API call fails.
    """
    # TODO: Implement LinkedIn Sales Navigator lookup.
    # Example steps:
    #   1. Authenticate using LINKEDIN_API_KEY (OAuth2 access token).
    #   2. Search for the company using the Account Search endpoint.
    #   3. Retrieve decision-maker contacts via the People Search endpoint.
    #   4. Extract linkedin_profile URL, email, and phone if available.
    #   5. Map to {"email_id": ..., "phone_number": ..., "linkedin_profile": ...}
    #   6. Return {} on any error or if not found.
    #
    # Note: LinkedIn API TOS restricts automated data extraction.
    # Ensure your use case is compliant before implementing.
    return {}
