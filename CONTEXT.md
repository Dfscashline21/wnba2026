---
name: WNBA 2026 Domain Context
description: Canonical domain language for the WNBA 2026 DFS/betting data platform
---

# Domain Context

## Core Concepts

### Schedule
Today's WNBA game matchups, sourced from the WNBA content API (`stats.wnba.com`). Returns home and away team abbreviations for games today and tomorrow. This is the authoritative starting point for the daily pipeline — not the DraftKings slate.

### Players Today
The full player pool for today's games, built from the Schedule + RotoWire depth charts. Contains every rostered player for every team playing today, with their opponent derived from the Schedule. Written to the `players_today` DB table and consumed by the Sim Pipeline. Independent of DraftKings.

**Columns:** `Name`, `TeamAbbrev`, `opponent`, `min_y` (RotoWire projected minutes)

### Roster
All players on a team's depth chart for a given day, sourced from RotoWire (`/wnba/wnba-depth-charts/`). Includes all players regardless of injury status — minutes projections handle downstream filtering.

### Sim Pipeline
The dbt models that produce prop projections and medians: `player_sims` macro → `ppovers`, `udovers`, `medians`. Anchored to `clean_players_today`, not `clean_draftkings`. Runs regardless of DraftKings slate availability.

### DK Optimizer Pipeline
The separate dbt branch that uses DraftKings salary data for lineup optimization: `clean_draftkings` → `optimizer_slate`, `draftkings_staged`, and the analyses models. Requires a live DK slate. Produces empty output when the slate is unavailable — this is acceptable.

### Prop Simulation
Monte Carlo simulation of player stat outcomes for a given game, driven by per-minute rates × projected minutes × defensive adjustment × pace factor. Runs per player in Players Today.

### Minutes Projection (`min_proj`)
The projected minutes for a player in today's game. Priority order in dbt:
1. `todaysmins.min` — manually adjusted projections (from `mincheckproj.csv`)
2. `players_today.min_y` — RotoWire projected minutes
3. Falls back to 0 if neither is available

### Name Map (`NAME_REPLACE`)
Environment variable containing a JSON dict of player name replacements applied across all pullers to normalize names to WNBA stats API format. New mismatches discovered from RotoWire or other sources are added to this map.

### Team Map (`dk_team_map`)
Dict combining `TEAM_REPLACE` env var with hardcoded fixes (`LAV→LVA`, `PHO→PHX`). Applied to all team abbreviations to normalize to NBA stats API format. Applied to depth chart team abbreviations when building Players Today.

## Pipeline Flow

```
WNBA Schedule API     RotoWire Depth Charts     RotoWire Rotomins
      ↓                        ↓                       ↓
 get_schedule()        depth_chart_roster()         rotomins()
      ↓                        ↓                       ↓
      └──────────── players_today ────────────────────┘
                         ↓
              injuryadjustments (wowy)
                         ↓
              pull_game_logs() ──→ PLAYER_GAME_LOGS
                         ↓
              dk.get_dk() [non-blocking, optimizer only]
                         ↓
                    DB writes
                         ↓
                      dbt run
                    ↙         ↘
           Sim Pipeline    DK Optimizer Pipeline
     (clean_players_today)  (clean_draftkings)
```

## Data Sources

| Source | Used For | Blocking? |
|--------|----------|-----------|
| WNBA content API | Schedule (today's matchups) | Yes |
| RotoWire depth charts | Roster (all players per team) | Yes |
| RotoWire rotomins | Projected minutes | Yes |
| WNBA stats API | Game logs, pace, defensive stats | Yes |
| DraftKings API | Salary data for optimizer | No |
| pbpstats wowy API | Injury-adjusted player rates | Yes |
