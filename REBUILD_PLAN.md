# WNBA2026 Pipeline Rebuild Plan

**Goal:** Clean the pipeline into a tight, maintainable codebase. Single canonical DB connection. Supabase as the API layer instead of Google Sheets. Keep all core logic, tweet generation, and video generation.

---

## What Changes vs. What Stays

### Core Logic — KEEP (logic unchanged, bugs fixed)
- `nbastats.py` — fix hardcoded date bug (line 80), fix season year (2025 → dynamic)
- `injuryadjustments.py` — fix hardcoded path, fix URL typo on line 177, fix path portability
- `dk_pull.py` — fix hardcoded path `C:\Users\Trent\WNBA\`, fix IndexError risk
- `pp_pull.py` — fix silent `except: pass` returning undefined variable
- `ud_pull.py` — minimal changes
- `roto_pull.py` — fix hardcoded path, fix column name mismatch in fallback
- `sportbooks_pull.py` — fix stale 2025 date range in `pull_mgm()`, remove code duplication
- `betonline_pull.py` — minimal changes
- `caesars_pull.py` — simplify to Oddsshopper-only (remove 3 unimplemented fallback methods)
- `caesarsalt.py` — move API key to env var, rename to `dk_alternates_pull.py`
- `injuryadjustments.py` — keep WOWY logic
- `predict_own.py` — fix path, fix LinearRegression waste, keep ML logic
- `parlay.py` — keep WNBAParlayAnalyzer class
- `trifecta.py` — fix import (swap `supabase_db_conn` → `supabase_conn`), wire up actual tweeting
- `wnba_social_video_generator.py` — fix import, fix volume bug
- `run_video_generator.py` — no changes
- `wnba_dbt/` — keep entirely as-is (no changes needed)
- `picgen.py` — fix hardcoded path

### Consolidate Into One File
- `main.py` + `main_local.py` + `main_local_supabase.py` + `wnbamaincloud.py` → **single `main.py`**
  - One `main()` function
  - `RUN_MODE` env var controls cloud vs local behavior
  - `run_script(request)` stays as GCF HTTP handler

### Replace
- `load_google.py` → **Supabase API** (dbt marts are already in Supabase — expose them directly)
- `supabase_db_conn.py` (has hardcoded credentials) → **delete, use `supabase_conn.py`**
- `wnbadagscheduler.py` (broken import) → **`scheduler.py`** with correct imports

### Delete
See "Files to Delete" section below.

---

## New Architecture

```
wnba2026/
│
├── main.py                    ← single pipeline entrypoint (cloud + local)
├── scheduler.py               ← Prefect/cron scheduling (fixed)
│
├── pullers/                   ← all data scrapers (flat imports preserved)
│   ├── __init__.py
│   ├── nbastats.py
│   ├── dk_pull.py
│   ├── pp_pull.py
│   ├── ud_pull.py
│   ├── roto_pull.py
│   ├── sportbooks_pull.py     ← BetMGM + Caesars + BetOnline (consolidated)
│   └── dk_alternates_pull.py  ← renamed from caesarsalt.py
│
├── db/
│   ├── __init__.py
│   └── conn.py                ← renamed from supabase_conn.py (single canonical)
│
├── modeling/
│   ├── __init__.py
│   ├── injuryadjustments.py
│   └── predict_own.py
│
├── distribution/
│   ├── __init__.py
│   ├── trifecta.py            ← fixed to actually post tweets
│   ├── wnba_social_video_generator.py
│   └── run_video_generator.py
│
├── analysis/
│   ├── __init__.py
│   ├── parlay.py
│   └── patlayrun.py
│
├── wnba_dbt/                  ← unchanged
│
├── data/
│   ├── mincheckproj.csv       ← manual minutes override (keep editing this daily)
│   ├── wnbadk.csv             ← written by dk_pull, read by nbastats
│   ├── playerposition.csv
│   ├── wnba_team list.csv
│   ├── players.csv
│   └── boosts.csv
│
├── pics/                      ← player headshots (keep)
├── assets/                    ← logos, misc images
├── articles/                  ← generated articles archive
├── videos/                    ← generated MP4s archive
│
├── .env                       ← all secrets (never commit)
├── .gitignore                 ← updated (add .vscode/, secrets/, *.json credentials)
├── requirements.txt
├── CLAUDE.md
└── README.md
```

**Note on imports:** Moving files into subdirectories requires updating imports in `main.py` and anywhere one module references another. All inter-module imports will use package-style: `from pullers.nbastats import get_schedule` instead of `import nbastats as nb`. This is a one-time refactor.

---

## The API Layer

**Key insight:** Supabase already exposes every table via a built-in REST API (PostgREST). The dbt mart tables are already in Supabase after every pipeline run. No separate API server needed.

### How It Works

After dbt runs, tables like `ppovers`, `udovers`, `medians`, `caesarsovers`, `minutes_projection` are in the `wnba` schema. Supabase auto-generates REST endpoints for all of them:

```
GET https://<project>.supabase.co/rest/v1/ppovers
GET https://<project>.supabase.co/rest/v1/medians
GET https://<project>.supabase.co/rest/v1/minutes_projection
```

The FastbreakSite (Next.js) can query these directly using `@supabase/supabase-js`:

```typescript
const { data } = await supabase
  .from('ppovers')
  .select('player_name, Points, pointsover, Rebounds, rebover')
  .gte('pointsover', 0.65)
  .order('pointsover', { ascending: false })
```

### Setup Required

1. **Row Level Security (RLS):** Enable on mart tables, set public read policy
   ```sql
   ALTER TABLE wnba.ppovers ENABLE ROW LEVEL SECURITY;
   CREATE POLICY "public read" ON wnba.ppovers FOR SELECT USING (true);
   ```
   Apply to: `ppovers`, `udovers`, `mgmovers`, `caesarsovers`, `medians`,
   `minutes_projection`, `pp5over`, `pp10over`, `ud5over`, `ud10over`,
   `mgm5over`, `mgm10over`, `prizepicksrec`, `draftkings_staged`, `standard`

2. **Anon key in frontend:** Use the `anon` public key (not service role) for website queries
   - Service role key: server/pipeline only
   - Anon key: frontend/website (safe to expose with RLS)

3. **Schema exposure:** Supabase only auto-exposes the `public` schema by default.
   Two options:
   - Option A: Add `wnba` schema to PostgREST exposure in Supabase dashboard (API settings → Extra search path)
   - Option B: Create views in `public` schema pointing to `wnba` mart tables

   **Recommended: Option A** — add `wnba` to the extra search path.

4. **Caching:** For the website, wrap Supabase calls with Next.js `fetch` cache or React Query to avoid hammering the DB on every page load.

### What the Website Gets

| Endpoint (table) | Use Case |
|---|---|
| `wnba.medians` | Daily player projections page |
| `wnba.ppovers` | PrizePicks recommendations |
| `wnba.udovers` | Underdog recommendations |
| `wnba.caesarsovers` | Caesars recommendations |
| `wnba.mgmovers` | BetMGM recommendations |
| `wnba.minutes_projection` | Minutes projections page |
| `wnba.pp5over` | Top 5 PrizePicks picks widget |
| `wnba.draftkings_staged` | DFS lineup optimizer |
| `wnba.caesars_range_outcomes` | Range outcome analysis |
| `wnba.caesars_value_opportunities` | Value plays |
| `wnba.*_history` | Backtesting / accuracy tracker |

### Optional: Supabase Edge Functions

For any endpoint that needs business logic (not just a table read), use Supabase Edge Functions (Deno/TypeScript, deployed to Supabase). Examples:
- `/api/daily-slate` — combines medians + ppovers + game schedule into one response
- `/api/player/{name}` — player-specific stats + prop history
- `/api/parlay` — calls parlay logic and returns recommendations

These replace any need for a separate API server.

---

## Files to Delete

### Security (delete immediately)
- `supabase_db_conn.py` — hardcoded Supabase project ID
- `wnba-files-c2213e18569e.json` — Google service account (rotate key, scrub git history)
- `sql/optimized_query.sql` — wrong project (Magento/NetSuite), never belonged here
- `SimpleMarketMetrics.ts` — ThinkorSwim trading script, wrong project

### Legacy DB (pre-Supabase era)
- `db_conn.py`
- `db_test.py`
- `check_tables.py`
- `generate_table_sql.py`

### Duplicate Connection Modules (keep `supabase_conn.py` only)
- `supabase_conn_db_only.py`
- `supabase_conn_final.py`
- `supabase_conn_fixed.py`
- `supabase_conn_working.py`

### One-Time Migration Scripts (migration is done)
- `supabase_migration.py`
- `supabase_migration_rest.py`
- `setup_dbt_supabase.py`
- `verify_dbt_readiness.py`

### Redundant Test Files
- `test_db_only_supabase.py`
- `test_dbt_supabase_simple.py`
- `test_final_supabase.py`
- `test_fixed_supabase.py`
- `test_fixed_supabase_pooling.py`
- `test_migration_script.py`
- `test_simplified_migration.py`

### Duplicate Puller
- `prizepicks.py` — exact duplicate of `pp_pull.py`

### Dead Apps
- `cbbstreamlit.py` — college basketball app, wrong project
- `cbbstreamlit_supabase.py` — incomplete stub, never finished

### Redundant Main Files (keep only `main.py`)
- `main_local.py`
- `main_local_supabase.py`
- `wnbamaincloud.py`

### Planning Docs (done their job)
- `REORG_PLAN.md`
- `UNCOMMITTED_CHANGES_AUDIT_2026-05-01.md`

---

## Bugs to Fix Before Rebuild Launch

These are in the keep files and must be fixed:

| File | Line | Bug | Fix |
|---|---|---|---|
| `nbastats.py` | 80 | Hardcoded `'2025-09-14'` — never fetches today | Use `todaysdate` variable |
| `nbastats.py` | 26,42,56,165 | Season hardcoded as `'2025'` | `os.getenv('WNBA_SEASON', '2026')` |
| `injuryadjustments.py` | 22 | `C:\Users\Trent\WNBA\` path | Use `Path(__file__).parent` |
| `injuryadjustments.py` | 177 | `0Exactly` URL typo | Remove the `0` prefix |
| `injuryadjustments.py` | 68 | Hardcoded injured player list | Remove (live injury report already fetched) |
| `pp_pull.py` | 70 | `except: pass` returns undefined | `return pd.DataFrame()` |
| `dk_pull.py` | 18 | `C:\Users\Trent\WNBA\` path | Remove (no file write needed) |
| `roto_pull.py` | 22 | Same hardcoded path | Remove |
| `roto_pull.py` | 62 | `abbr` vs `team` column mismatch | Rename in fallback response |
| `sportbooks_pull.py` | 29 | Hardcoded 2025 date range for BetMGM | Use `date.today()` ± 1 day |
| `caesarsalt.py` | 523 | Hardcoded Odds API key | `os.getenv('ODDS_API_KEY')` |
| `trifecta.py` | 8 | Imports `supabase_db_conn` (deleted) | Import from `db.conn` |
| `trifecta.py` | — | Only prints tweet, never posts | Wire up Tweepy |
| `wnba_social_video_generator.py` | 8 | Imports `supabase_db_conn` | Import from `db.conn` |
| `wnba_social_video_generator.py` | 497 | Volume control commented out (AttributeError) | Fix moviepy API call |
| `load_google.py` | — | Entire file | Remove (replaced by Supabase API) |
| `predict_own.py` | 17 | Hardcoded path | Use `Path(__file__).parent` |
| `predict_own.py` | 103 | Trains LinearRegression then immediately replaces it | Remove LinearRegression block |
| `wnbadagscheduler.py` | 2 | Imports non-existent `run_wnba` | Rewrite scheduler.py from scratch |
| `wnbaapp.py` | 139 | `&` instead of `and` | Fix operator |

---

## Environment Variables (.env)

All of the following must be in `.env` (never committed):

```bash
# Supabase
SUPABASE_URL=https://<project>.supabase.co
SUPABASE_SERVICE_ROLE_KEY=<service_role_key>
SUPABASE_PASSWORD=<db_password>
SUPABASE_HOST=<db_host>
SUPABASE_USER=postgres
SUPABASE_DBNAME=postgres
SUPABASE_PORT=5432
SUPABASE_SCHEMA=wnba

# Pipeline config
WNBA_SEASON=2026
NAME_REPLACE={"Player Name": "Corrected Name"}

# Twitter/X
TWITTER_API_KEY=
TWITTER_API_SECRET=
TWITTER_ACCESS_TOKEN=
TWITTER_ACCESS_TOKEN_SECRET=

# Google Sheets (keep until API is live)
GOOGLE_SHEETS_CREDS_FILE=wnba-files-<id>.json

# Third-party APIs
ODDS_API_KEY=<the_odds_api_key>

# DBT
DBT_PROFILES_DIR=/tmp
```

---

## `main.py` Consolidated Structure

```python
import os
from dotenv import load_dotenv
from pullers import nbastats, dk_pull, pp_pull, ud_pull, roto_pull, sportbooks_pull
from modeling import injuryadjustments, predict_own
from db.conn import get_db_engine
from distribution import trifecta, run_video_generator

def main():
    load_dotenv()
    engine = get_db_engine()

    # 1. Pull data
    game_log, player_ids, min_start = nbastats.pull_game_logs()
    schedule       = nbastats.get_schedule()
    adv_def        = nbastats.adv_stats()
    player_pos     = nbastats.player_pos()
    proj_minutes   = roto_pull.rotomins()
    rate_boosts, injuries = injuryadjustments.get_adjusted_rates(min_start)
    prizepicks     = pp_pull.prizepicks_pull()
    underdog       = ud_pull.udprops()
    betmgm         = sportbooks_pull.pull_mgm()
    caesars        = sportbooks_pull.pull_caesars()

    # 2. Write to Supabase
    game_log.to_sql('PLAYER_GAME_LOGS', engine, schema='wnba', if_exists='replace', index=False)
    schedule.to_sql('Games', engine, schema='wnba', if_exists='replace', index=False)
    # ... etc.

    # 3. Run dbt
    run_dbt()

    # 4. Distribute
    trifecta.trifecta_tweet()
    run_video_generator.main()

def run_dbt():
    import subprocess, yaml
    profile = { ... }  # build from env vars
    with open('/tmp/profiles.yml', 'w') as f:
        yaml.dump(profile, f)
    subprocess.run(['dbt', 'run', '--project-dir', 'wnba_dbt', '--profiles-dir', '/tmp'], check=True)

def run_script(request):
    """Google Cloud Function HTTP handler"""
    main()
    return ('OK', 200)

if __name__ == '__main__':
    main()
```

---

## .gitignore Updates Needed

Add these lines:

```gitignore
# Credentials
.env
.env.*
*.json
!package.json
!package-lock.json
.vscode/

# Secrets folder
secrets/

# Generated outputs (optional — large files)
videos/wnba_prizepicks_*.mp4
articles/

# Python
__pycache__/
*.pyc
*.pyo
.venv/
venv/
```

---

## Implementation Order

### Phase 1 — Security (do today, before anything else)
1. Delete `supabase_db_conn.py`
2. Add `.vscode/` and `*.json` to `.gitignore`
3. Move `wnba-files-*.json` out of repo, add to `.gitignore`
4. Rotate: Supabase service role key, Google service account, The Odds API key, AWS RDS password
5. Move `ODDS_API_KEY` to `.env` in `caesarsalt.py`

### Phase 2 — Dead File Cleanup
1. Delete all files in the "Files to Delete" list
2. `git add -A && git commit -m "remove dead files and security risks"`

### Phase 3 — Bug Fixes (files that are kept)
1. Fix `nbastats.py` hardcoded date (line 80) and season year
2. Fix `pp_pull.py` silent exception
3. Fix `sportbooks_pull.py` stale BetMGM dates
4. Fix `injuryadjustments.py` path and URL typo
5. Fix all `supabase_db_conn` imports → `supabase_conn`
6. Test pipeline end-to-end with `python main.py`

### Phase 4 — Consolidation
1. Merge `main_local.py` logic into `main.py` (add `if __name__ == '__main__': main()`)
2. Refactor into package structure (`pullers/`, `db/`, `modeling/`, `distribution/`)
3. Update all imports across files
4. Verify dbt run still works

### Phase 5 — API Layer
1. Add `wnba` schema to Supabase PostgREST search path (Supabase dashboard → API settings)
2. Enable RLS on all mart tables, add public read policies
3. Test direct queries from FastbreakSite using Supabase anon key
4. Remove `load_google.py` call from `main.py`
5. Update FastbreakSite to query Supabase instead of Google Sheets

### Phase 6 — Distribution Fixes
1. Wire `trifecta.py` to actually post via Tweepy (add Twitter env vars)
2. Fix `wnba_social_video_generator.py` volume bug
3. Test full run: pipeline → dbt → tweet → video

---

## What the Website Query Looks Like (FastbreakSite)

```typescript
// lib/supabase.ts
import { createClient } from '@supabase/supabase-js'

export const supabase = createClient(
  process.env.NEXT_PUBLIC_SUPABASE_URL!,
  process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
)

// app/picks/page.tsx
const { data: ppPicks } = await supabase
  .schema('wnba')
  .from('pp5over')
  .select('player_name, stat, line, over_probability, team, matchup')
  .order('over_probability', { ascending: false })

const { data: projections } = await supabase
  .schema('wnba')
  .from('medians')
  .select('player_name, projected_pts, projected_reb, projected_ast, projected_dk, salary, team')
  .order('projected_dk', { ascending: false })
```

No API server. No Google Sheets. The dbt run writes the data, Supabase serves it.

---

## Summary

| Aspect | Before | After |
|---|---|---|
| Pipeline entrypoints | 4 (main, main_local, main_local_supabase, wnbamaincloud) | 1 (main.py) |
| DB connection modules | 7 (db_conn + 5 supabase variants + supabase_db_conn) | 1 (db/conn.py) |
| Test files | 11 | 4 (keep: supabase_connection, dbt_quick, rest_api, caesars) |
| Distribution | Google Sheets + print(tweet) + video | Supabase API + actual tweet + video |
| Secret management | Scattered in source code | All in .env |
| Total Python files | ~58 | ~25 |
| Show-stopper bugs | 4 | 0 |
