# Architecture Enhancements

Deepening opportunities identified via codebase architecture review. Pick one to explore before implementing.

---

## 1. Name Normalization: Scattered Mapping → One Deep Module

**Files**: main.py, roto_pull.py, nbastats.py, ud_pull.py, wnbaapp.py, predict_own.py (8+ sites)

**Problem**: The same player-name mapping is applied independently in every puller. Each caller must know the mapping exists and apply it themselves. Adding a new alias requires grepping and editing every puller. Missing one causes silent join failures downstream.

**Solution**: A single `normalize_player_name(name)` module that owns the full mapping. Every puller calls it; none of them know the mapping.

**Benefits**: Adding a new alias is one edit in one place. Callers no longer carry mapping knowledge. Testable as a pure function.

---

## 2. CSV Side-Effects: Hidden Inter-Module State → Explicit Data Flow

**Files**: main.py, roto_pull.py, dk_pull.py, nbastats.py, injuryadjustments.py, predict_own.py

**Problem**: 7+ modules write CSVs to disk and others read them back. The interface between modules is the filesystem — callers cannot know if data is stale. Reordering steps in main.py silently breaks the pipeline.

**Solution**: Pullers return DataFrames. `main.py` passes them as arguments. CSV writing becomes an optional persistence adapter, not load-bearing plumbing.

**Benefits**: Ordering is explicit. Functions are testable in isolation. Parallelization becomes possible.

---

## 3. Injury Adjustment: Untestable Monolith → Staged Pipeline with Seams

**File**: injuryadjustments.py (250 lines)

**Problem**: One function mixes three stages — API calls, baseline stat computation, boost calculation — with no seams between them. The boost logic (the valuable part) cannot be tested without hitting live APIs. A bare `except` silently swallows API failures.

**Solution**: Split into three modules with explicit interfaces:
- `fetch_injury_report() → injuries`
- `compute_baselines(game_logs) → baselines`
- `apply_boosts(baselines, injuries) → adjusted_rates`

**Benefits**: Boost calculation gets locality. Baselines can be cached. API failures are visible. Tests can pass a fabricated injury list and verify the math.

---

## 4. Sportsbook Pullers: Copy-Paste Pattern → One Deep Puller Module

**Files**: caesars_pull.py, betonline_pull.py, dk_pull.py, pp_pull.py, ud_pull.py

**Problem**: Each puller re-implements try → fallback endpoint → parse JSON → pivot → fill missing columns. 100+ lines of near-identical boilerplate. A bug fix in one doesn't propagate. Adding a new sportsbook requires a full copy-paste.

**Solution**: A `pull_props(sportsbook_config) → DataFrame` module that owns the fetch/parse/pivot pattern. Each sportsbook becomes a config (endpoint, auth, column map), not a module.

**Benefits**: Adding a new sportsbook is config, not code. Error handling is fixed once. Interface to main.py is uniform.

---

## 5. main.py Orchestrator: 25 try/except Blocks → Meaningful Pipeline

**File**: main.py (lines 72–220)

**Problem**: 25 sequential try/except blocks with no conditional logic or transformation — pure plumbing. No retry logic, no partial-run support, no expression of which steps depend on which.

**Solution**: Once pullers return DataFrames (enhancement 2) and share error handling (enhancement 4), main.py can express a real pipeline: step dependencies, retry behavior, and partial failure semantics.

**Benefits**: Testable pipeline stages. Retry logic lives in one place. Partial runs become possible.

---

## 6. dbt Schema Contract: Silent Failures → Explicit Column Validation

**Files**: wnba_dbt/models/marts/*.sql + all pullers

**Problem**: dbt models assume specific column names from raw tables written by pullers. If a puller renames a column, dbt silently produces wrong output or fails with an opaque SQL error. No contract exists between the two layers.

**Solution**: A schema validation step between "write raw tables" and "run dbt" — assert expected columns exist before handing off. Could be a lightweight Python check or dbt source tests.

**Benefits**: Schema regressions are caught at the seam, not discovered downstream in Google Sheets.
