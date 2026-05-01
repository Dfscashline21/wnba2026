# WNBA2026 Folder Reorganization Plan

**Status:** Proposal only — no files have been moved.
**Repo:** Git working tree left untouched per your instruction.

---

## What's in the root today

359 items at the top level, breaking down as:

| Type | Count | Notes |
|------|------:|-------|
| `.txt` articles | 156 | Daily `WNBA Article` + `WNBA DK Article` from 2025-05-17 onward |
| `.mp4` videos | 64 | `wnba_prizepicks_YYYY-MM-DD.mp4` outputs from the video generator |
| `.py` scripts | 58 | Pullers, modeling, supabase/dbt, video gen, apps, tests |
| `.csv` data | 43 | Reference data + dated scraper outputs + projection files |
| `.json` | 7 | 6 migration reports + 1 Google service-account credential |
| `.md` | 5 | Docs/READMEs |
| Images | 12 | `.jpg/.png/.webp/.svg` — player photos, team logos, misc |
| `.sql` | 4 | Table-creation + an optimized query |
| Audio | 2 | `music.mp3`, `temp_audio_Satou_Sabally.mp3` |
| Other | 4 | `.xlsx`, `.ts`, `.bat`, `.code-workspace` |
| Existing folders | 3 | `minsproj/`, `pics/`, `wnba_dbt/` |

---

## Critical caveat: Python import coupling

A scan of the `.py` files shows a tight web of sibling imports. Examples:

```python
# main.py / main_local.py / wnbamaincloud.py / main_local_supabase.py
import roto_pull as rp
import dk_pull as dk
import ud_pull as ud
import pp_pull as pp
import sportbooks_pull as sb
import injuryadjustments as inj
import nbastats as nb
from db_conn import get_db_engine
from load_google import load_to_google
from trifecta import trifecta_tweet

# patlayrun.py
from parlay import WNBAParlayAnalyzer

# load_google.py
from predict_own import predict_ownership as po

# nbastats.py
import dk_pull as dk

# check_tables.py / db_test.py / generate_table_sql.py
from db_conn import get_db_engine
```

**Implication:** If we split these `.py` files across subfolders without refactoring, `import dk_pull` will break. There are two clean ways to handle that:

1. **Recommended for now — keep the interconnected `.py` files together** in a single `src/` folder (or leave them at root). Move only the *safe* stuff (articles, videos, images, dated data exports, docs, SQL, reports). Low risk, big visual cleanup.
2. **Bigger refactor — convert to packages.** Add `__init__.py` files, move pullers into `sportsbooks/pullers/`, etc., and rewrite the imports to package-style (`from sportsbooks.pullers import dk_pull as dk`). Requires testing every entrypoint.

The proposal below uses **option 1** unless you tell me otherwise.

---

## Proposed structure

```
wnba2026/
│
├── .git/                         (untouched)
├── .gitignore                    (may want updates — see below)
├── .vscode/                      (untouched)
├── fbf.code-workspace            (leave at root)
├── README.md                     (recommend creating one)
│
├── src/                          ── ALL .py STAYS FLAT HERE ──
│   │   (preserves all current `import sibling` statements)
│   │
│   ├── # Entrypoints
│   ├── main.py
│   ├── main_local.py
│   ├── main_local_supabase.py
│   ├── wnbamaincloud.py
│   ├── wnbaapp.py
│   ├── wnbadagscheduler.py
│   ├── cbbstreamlit.py
│   ├── cbbstreamlit_supabase.py
│   │
│   ├── # Sportsbook pullers
│   ├── betonline_pull.py
│   ├── caesars_pull.py
│   ├── caesarsalt.py
│   ├── dk_pull.py
│   ├── pp_pull.py
│   ├── prizepicks.py
│   ├── ud_pull.py
│   ├── roto_pull.py
│   ├── sportbooks_pull.py
│   ├── dk_caesars_comparison.py
│   │
│   ├── # Modeling / projections
│   ├── predict_own.py
│   ├── injuryadjustments.py
│   ├── nbastats.py
│   ├── load_google.py
│   │
│   ├── # Parlays
│   ├── parlay.py
│   ├── patlayrun.py
│   ├── trifecta.py
│   │
│   ├── # Supabase / DB
│   ├── db_conn.py
│   ├── db_test.py
│   ├── supabase_conn.py
│   ├── supabase_conn_db_only.py
│   ├── supabase_conn_final.py
│   ├── supabase_conn_fixed.py
│   ├── supabase_conn_working.py
│   ├── supabase_db_conn.py
│   ├── supabase_rest_api.py
│   ├── supabase_storage.py
│   ├── supabase_migration.py
│   ├── supabase_migration_rest.py
│   ├── setup_dbt_supabase.py
│   ├── verify_dbt_readiness.py
│   ├── check_env_vars.py
│   ├── check_tables.py
│   ├── debug_password_usage.py
│   ├── generate_table_sql.py
│   │
│   ├── # Video generator
│   ├── wnba_social_video_generator.py
│   ├── run_video_generator.py
│   ├── picgen.py
│   │
│   ├── # Tests
│   └── tests/
│       ├── test_caesars.py
│       ├── test_db_only_supabase.py
│       ├── test_dbt_connection_quick.py
│       ├── test_dbt_supabase_simple.py
│       ├── test_final_supabase.py
│       ├── test_fixed_supabase.py
│       ├── test_fixed_supabase_pooling.py
│       ├── test_migration_script.py
│       ├── test_rest_api.py
│       ├── test_simplified_migration.py
│       ├── test_supabase_connection.py
│       └── test_video_generator.py
│
├── data/                         ── INPUT / REFERENCE DATA ──
│   ├── reference/                # Static-ish lookup tables
│   │   ├── WTeams.csv
│   │   ├── players.csv
│   │   ├── playerids.csv
│   │   ├── playerposition.csv
│   │   ├── wnba_team list.csv
│   │   ├── wnbadk.csv
│   │   ├── wnbaload.csv
│   │   ├── wnbashowdown.csv
│   │   ├── wnbaslate.csv
│   │   ├── wnba historical.csv
│   │   └── boosts.csv
│   │
│   ├── projections/              # Hand-tuned + generated projection inputs
│   │   ├── Projection own.csv
│   │   ├── actualown.csv
│   │   ├── adj players.csv
│   │   ├── mincheck.csv
│   │   ├── mincheckproj.csv
│   │   ├── rotomins.csv
│   │   ├── hrtest.csv
│   │   ├── Inj adjustment sheet.xlsx
│   │   └── sample_parlay_results.csv
│   │
│   └── exports/                  ── DATED SCRAPER OUTPUTS ──
│       ├── betonline_wnba_props.csv
│       ├── betonline_wnba_props_20250819_124808.csv
│       ├── caesars_aja_wilson_props.csv
│       ├── caesars_points_props.csv
│       ├── caesars_wnba_props.csv
│       ├── caesars_wnba_alternates_20250821_153124.csv
│       ├── draftkings_wnba_alternates_*.csv          (5 files)
│       ├── dk_caesars_comparison_*.csv               (8 files)
│       ├── postgres caesars_range_outcomes *.csv     (3 files)
│       ├── best_wnba_parlays.csv
│       └── test.csv
│
├── articles/                     ── 156 daily article TXTs ──
│   ├── 2025-05-17 WNBA Article.txt
│   ├── 2025-05-17 WNBA DK Article.txt
│   └── ... (all dated WNBA + WNBA DK articles together)
│
├── videos/                       ── 64 generated MP4s ──
│   ├── wnba_prizepicks_2025-06-17.mp4
│   ├── wnba_prizepicks_2025-06-18.mp4
│   ├── wnba_prizepicks_2025-06-18_script.txt
│   ├── ... (all wnba_prizepicks_*.mp4)
│   ├── wnba_prizepicks_video.mp4
│   └── test_video.mp4
│
├── audio/
│   ├── music.mp3
│   └── temp_audio_Satou_Sabally.mp3
│
├── assets/                       ── IMAGES ──
│   ├── players/                  # rename of existing pics/
│   │   └── ... (all player headshots)
│   ├── logos/
│   │   ├── Atlanta_Dream_logo.svg
│   │   ├── Indiana_Fever_logo.svg.png
│   │   ├── lib.png
│   │   ├── sun.png
│   │   └── wnb.png
│   └── misc/
│       ├── angel-reese.jpg
│       ├── bri.jpg
│       ├── cc.jpg
│       ├── phee.jpg
│       ├── en-US._UR1600,1200_.jpg
│       └── 01k1gvz8abf59fedp3ae.webp
│
├── sql/
│   ├── create_supabase_tables.sql
│   ├── create_supabase_tables_clean.sql
│   ├── create_supabase_tables_dynamic.sql
│   └── optimized_query.sql
│
├── docs/
│   ├── Caesars_Strategy_Analysis.md
│   ├── README_Caesars.md
│   ├── README_video_generator.md
│   ├── DBT_SUPABASE_MIGRATION_GUIDE.md
│   └── SUPABASE_MIGRATION_TODO.md
│
├── reports/                      # Migration run logs etc.
│   ├── migration_report_20250902_151757.json
│   ├── migration_report_20250902_152159.json
│   ├── migration_report_20250902_152335.json
│   ├── migration_report_20250902_153016.json
│   ├── migration_report_20250902_153034.json
│   └── migration_report_20250902_153059.json
│
├── scripts/                      # Shell entrypoints
│   └── wnbasocial.bat
│
├── config/
│   ├── requirements.txt
│   ├── requirements_supabase.txt
│   └── supabase_env_template.txt
│
├── secrets/                      # ⚠️ ADD TO .gitignore
│   └── wnba-files-c2213e18569e.json
│
├── frontend/                     # The orphan TS file
│   └── SimpleMarketMetrics.ts
│
├── minsproj/                     (untouched — already organized)
├── wnba_dbt/                     (untouched — already organized)
└── playerlast_10/                (untouched)
```

---

## Things you'll need to handle if/when we execute this

### 1. `.gitignore` additions
The Google service-account JSON (`wnba-files-c2213e18569e.json`) almost certainly should not be in the repo. After moving it to `secrets/`, add:
```
secrets/
*.json   # if you want a stricter rule — but this would also catch reports/
```
Optionally also ignore the dated scraper outputs and generated videos:
```
data/exports/*_2025*.csv
videos/wnba_prizepicks_2025-*.mp4
```

### 2. Hardcoded relative paths inside scripts
Some scripts probably do `pd.read_csv("players.csv")`. Once `players.csv` lives in `data/reference/`, those reads break. We'd need to grep for filename string literals across `*.py` and update them, or have the script `cd` into the repo root before running. Let me know and I'll do that pass.

### 3. Streamlit / app entrypoints
`streamlit run wnbaapp.py` becomes `streamlit run src/wnbaapp.py`. Same for cron/scheduled tasks and `wnbasocial.bat` (which probably calls a `.py`).

### 4. Duplicate / experiment files worth pruning later
You have 5 `supabase_conn*.py` variants (`supabase_conn.py`, `_db_only`, `_final`, `_fixed`, `_working`) and 7 `test_*supabase*.py` variants — looks like iteration leftovers. Worth a separate cleanup pass to figure out which is canonical and delete the rest. Same goes for `main.py` / `main_local.py` / `main_local_supabase.py` / `wnbamaincloud.py` — four near-identical entrypoints.

---

## What I'd do next

When you're ready to actually move the files, just say the word and tell me which of these you want:

- **(a)** Execute the full plan above with `mv` (no git mv).
- **(b)** Execute only the "safe" parts first — articles, videos, images, audio, sql, docs, reports, exports, config, secrets — leave all `.py` and reference CSVs at root for now.
- **(c)** Go further and refactor the `.py` files into proper packages (rewrites imports).
- **(d)** Just clean up the duplicate `supabase_conn*.py` / `test_*` / `main*` files first, before any moves.

[View the plan](computer://C:\Users\Trenton\Code\WNBAPROJ\wnba2026\REORG_PLAN.md)
