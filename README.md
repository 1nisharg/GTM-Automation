# GTM UAE Partner Pipeline

A **pure agentic workflow** built with [LangGraph](https://github.com/langchain-ai/langgraph) for discovering, enriching, and managing partners for the GTM UAE programme.

> **No API server. No UI. No frontend. Only the agentic pipeline.**

---

## Pipeline Overview

Places to put .env:

\GTM_workflow_mondee\.env

\GTM_workflow_mondee\backend\.env

\GTM_workflow_mondee\enrichment_sources\.env

\GTM_workflow_mondee\frontend\.env
```
Discovery → Enrichment → Outreach → Onboarding → Documentation → Boarding/Live
   ✅           ✅          🔲            🔲              🔲              🔲
```

| Stage | Node | Status | Description |
|-------|------|--------|-------------|
| 1 | `discovery_node` | ✅ Implemented | Queries PostgreSQL for partners matching a subcategory with status `"Yet to Start"` |
| 2 | `enrichment_node` | ✅ Implemented | Fills missing contact fields via a 5-source async fallback chain |
| 3 | `outreach_node` | 🔲 Stub | Owned by the outreach team — wired, not implemented |
| 4 | `onboarding_node` | 🔲 Stub | Post-outreach onboarding — placeholder |
| 5 | `documentation_node` | 🔲 Stub | Partner documentation collection — placeholder |
| 6 | `boarding_live_node` | 🔲 Stub | Final go-live activation — placeholder |

---

## Repo Structure

```
/
├── graph.py                        # LangGraph StateGraph — entry point
├── state.py                        # GraphState TypedDict
├── nodes/
│   ├── discovery_node.py           # Stage 1: PostgreSQL partner lookup
│   ├── enrichment_node.py          # Stage 2: async contact enrichment
│   ├── outreach/
│   │   └── outreach_node.py        # Stage 3: stub (outreach team)
│   ├── onboarding_node.py          # Stage 4: stub
│   ├── documentation_node.py       # Stage 5: stub
│   └── boarding_live_node.py       # Stage 6: stub
├── enrichment_sources/
│   ├── database_query.py           # Internal DB (priority 2)
│   ├── hunter.py                   # Hunter.io (priority 3)
│   ├── apollo.py                   # Apollo.io (priority 4)
│   └── linkedin_sales_nav.py       # LinkedIn Sales Nav (priority 5)
├── db/
│   ├── connection.py               # asyncpg pool — init/get/close
│   ├── models.py                   # partners table DDL + column keys
│   └── init.sql                    # DDL auto-run by Docker on first boot
├── scripts/
│   └── migrate_excel_to_db.py      # One-time Excel → PostgreSQL migration
├── Dockerfile
├── docker-compose.yml
├── .env.example                    # All required env var names
├── requirements.txt
└── README.md
```

---

## Setup

### Quick Start (Docker — recommended)

```bash
# 1. Configure environment
cp .env.example .env
#    Open .env and fill in at minimum: DB_PASSWORD

# 2. Build the pipeline image
docker compose build

# 3. Start PostgreSQL (auto-creates DB + runs DDL from db/init.sql on first boot)
docker compose up postgres -d

# 4. Migrate Excel data → PostgreSQL
docker compose run --rm pipeline python scripts/migrate_excel_to_db.py

# 5. Run the pipeline
docker compose run --rm pipeline python graph.py
```

> **Re-run migration after Excel updates:**
> ```bash
> docker compose run --rm pipeline python scripts/migrate_excel_to_db.py --clear
> ```

> **Full database reset** (destroys volume + reloads from scratch):
> ```bash
> docker compose down -v
> docker compose up postgres -d
> docker compose run --rm pipeline python scripts/migrate_excel_to_db.py
> ```

---

### Step-by-step Detail

#### 1. Configure environment

```bash
cp .env.example .env
```

Edit `.env` and fill in your values:

| Variable | Purpose | Default |
|----------|---------|---------|
| `DB_HOST` | PostgreSQL host | `localhost` (auto-overridden to `postgres` inside Docker) |
| `DB_PORT` | PostgreSQL port | `5432` |
| `DB_NAME` | Database name | `gtm_uae` |
| `DB_USER` | Database user | `postgres` |
| `DB_PASSWORD` | Database password | *(required — no default)* |
| `DATABASE_API_KEY` | Internal company database API key | — |
| `HUNTER_API_KEY` | Hunter.io API key | — |
| `APOLLO_API_KEY` | Apollo.io API key | — |
| `LINKEDIN_API_KEY` | LinkedIn Sales Navigator API key | — |

#### 2. Build the Docker image

```bash
docker compose build
```

Only needed once (or after changing `Dockerfile` / `requirements.txt`). Code changes don't require a rebuild — the repo is volume-mounted into the container.

#### 3. Start PostgreSQL

```bash
docker compose up postgres -d
```

On first run with an empty volume, Docker automatically executes `db/init.sql`, which creates the `partners` table and indexes. On subsequent starts, the existing volume is reused.

#### 4. Migrate Excel → PostgreSQL

Place the Excel file at the repo root as `GTM UAE_ Track 1 & 2 Db.xlsx`, then:

```bash
docker compose run --rm pipeline python scripts/migrate_excel_to_db.py
```

What the migration does:
- Creates the `partners` table if it doesn't exist (safe to re-run)
- Reads **Track 1 Db** (338 rows) and **Track 2 Db** (~9,867 rows)
- Normalises column names across both sheets into a unified schema
- Inserts with `ON CONFLICT DO NOTHING` — safe to re-run, skips existing rows

Optional flags:
```bash
# Custom Excel path
python scripts/migrate_excel_to_db.py --excel /path/to/file.xlsx

# Full reload: truncates the table before inserting
python scripts/migrate_excel_to_db.py --clear
```

#### 5. Run the pipeline

```bash
docker compose run --rm pipeline python graph.py
```

Runs a smoke test with sample category `"Adventure & Extreme Sports"`.

To run programmatically with a different category:

```python
import asyncio
from graph import build_graph
from db.connection import init_pool, close_pool

async def main():
    await init_pool()
    graph = build_graph()
    result = await graph.ainvoke({
        "input_category": "Wellness, Spa & Mindfulness",
        "discovered_partners": [],
        "enriched_partners": [],
    })
    print(f"Discovered: {len(result['discovered_partners'])}")
    print(f"Enriched:   {len(result['enriched_partners'])}")
    await close_pool()

asyncio.run(main())
```

#### Option B — Local Postgres (without Docker)

```bash
pip install -r requirements.txt
psql -U postgres -d gtm_uae -f db/init.sql
python scripts/migrate_excel_to_db.py
python graph.py
```

---

## Excel → PostgreSQL Column Mapping

| Excel Column (Track 1 / Track 2) | PostgreSQL Column |
|---|---|
| Partner Name / Partner name | `partner_name` |
| Digitisation | `digitisation` |
| Category / Categories | `category` |
| Subcategories / Sub Categories | `subcategories` |
| Website | `website` |
| Product Content / Product Count | `product_count` |
| Status | `status` |
| Integrated | `integrated` |
| Region | `region` |
| Phone number | `phone_number` |
| Email ID | `email_id` |
| Linkedin profile | `linkedin_profile` |
| *(derived)* | `sheet_source` (`track1` or `track2`) |

---

## Enrichment Fallback Chain

For each partner, each contact field (`phone_number`, `email_id`, `linkedin_profile`) is resolved independently in this priority order. The chain stops at the first non-empty value:

1. **DB record** — value already present in the `partners` table
2. **Internal Database API** — `enrichment_sources/database_query.py`
3. **Hunter.io** — `enrichment_sources/hunter.py`
4. **Apollo.io** — `enrichment_sources/apollo.py`
5. **LinkedIn Sales Navigator** — `enrichment_sources/linkedin_sales_nav.py`

All 4 external sources are called **concurrently** per partner (`asyncio.gather`), so enrichment is fast regardless of how many sources are wired.

### Adding a new enrichment source

1. Create `enrichment_sources/<new_source>.py` with `async def query_<name>(business_name: str) -> dict`
2. Add it to `enrichment_sources/__init__.py`
3. Add it to the source list in `nodes/enrichment_node.py`

That's it — no other changes needed.

---

## Discovery Filter Logic

| Criterion | Value |
|-----------|-------|
| Column filtered | `subcategories` (ILIKE — case-insensitive substring match) |
| Status filter | `"Yet to Start"` |
| Sheets queried | Both `track1` and `track2` (via `sheet_source` column) |

Sample subcategory values from the data:
- `Adventure & Extreme Sports`
- `Wellness, Spa & Mindfulness`
- `Festivals & Cultural Celebrations`
- `Hiking, Trekking & Expeditions`
- `Cultural & Heritage Experiences`

