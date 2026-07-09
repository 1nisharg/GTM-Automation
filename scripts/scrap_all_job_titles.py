"""
scripts/scrape_job_titles.py
-----------------------------
Pull every job title Apollo has for each partner, searched by NAME
(q_keywords free-text) instead of website domain. Most un-digitised
partners have no real website, so domain-based lookups fail for them —
name search is the reliable path here, same fallback apollo.py itself
already uses in production.

Free: people search never costs Apollo credits (per Apollo's own pricing
docs), regardless of which filter (q_keywords, domain, org_id) you use.

Output: apollo_job_titles.csv (job_title, count) — nothing else.

Usage: python scripts/scrape_job_titles.py
"""

import asyncio
import csv
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv()

import httpx
from db.connection import get_pool
from enrichment_sources.apollo import APOLLO_API_KEY, _PEOPLE_SEARCH_URL, _post_with_retry

OUT_CSV = "apollo_job_titles.csv"


async def titles_for_partner(client: httpx.AsyncClient, headers: dict, partner_name: str) -> list[str]:
    titles: list[str] = []
    page = 1
    while True:
        data = await _post_with_retry(client, _PEOPLE_SEARCH_URL, headers, {
            "page": page,
            "page_size": 100,
            "q_keywords": partner_name,
            "reveal_personal_emails": False,
            "reveal_phone_number": False,
        })
        people = (data or {}).get("people") or []
        if not people:
            break

        titles += [p["title"].strip() for p in people if p.get("title")]

        total_pages = (data or {}).get("pagination", {}).get("total_pages", 1)
        if page >= total_pages:
            break
        page += 1

    return titles


async def main() -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT partner_name FROM partners "
            "WHERE partner_name IS NOT NULL AND partner_name != ''"
        )

    headers = {"Content-Type": "application/json", "X-Api-Key": APOLLO_API_KEY}
    counts: Counter = Counter()

    async with httpx.AsyncClient(timeout=20.0) as client:
        for i, row in enumerate(rows, start=1):
            name = row["partner_name"]
            titles = await titles_for_partner(client, headers, name)
            counts.update(titles)
            print(f"[{i}/{len(rows)}] {name}: {len(titles)} titles")

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["job_title", "count"])
        for title, count in counts.most_common():
            writer.writerow([title, count])

    print(f"Done — {sum(counts.values())} titles, {len(counts)} unique -> {OUT_CSV}")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())