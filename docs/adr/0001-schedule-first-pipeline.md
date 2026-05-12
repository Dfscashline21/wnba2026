---
id: "0001"
title: Schedule-First Pipeline with Separate Sim and Optimizer Anchors
status: accepted
date: 2026-05-09
---

## Context

The daily pipeline was anchored to the DraftKings slate (`dk.get_dk()`). When the DK slate closes mid-day (games already started), refreshing the pipeline returned an empty slate — causing `players_today`, injury adjustments, and all dbt sim models to produce zero rows. Games scheduled later in the day were silently dropped.

## Decision

Split the pipeline into two independent branches anchored to different sources:

**Sim Pipeline** — anchored to Schedule + RotoWire depth charts (`players_today` table). Runs regardless of DraftKings availability. Feeds `ppovers`, `udovers`, `medians`, and the `player_sims` macro.

**DK Optimizer Pipeline** — anchored to the DraftKings slate (`draftkings` table). Runs when a live slate is available; produces empty output when it is not. Feeds `optimizer_slate`, `draftkings_staged`, and analyses models.

The `players_today` table is written by `main.py` before dbt runs and contains: `Name`, `TeamAbbrev`, `opponent`, `min_y`. The dbt model `clean_players_today` replaces `clean_draftkings` as the join anchor in `player_sims` and `medians.sql`.

## Alternatives Considered

**Single table with nullable DK salary** — populate the existing `draftkings` table from the schedule-first source and overlay DK salary when available. Rejected: mixes two concerns in one table; a schema change to DK columns would still break the sim models.

**Fallback to cached `wnbadk.csv`** — when the live slate is empty, use the last saved CSV. Rejected: the CSV goes stale as games complete and was not designed as a reliable fallback.

## Consequences

- The sim pipeline now runs reliably throughout game day, including after the DK slate closes.
- `pull_game_logs()` in `nbastats.py` drops its `dk.get_dk()` call — it becomes a pure WNBA stats API pull.
- `injuryadjustments.get_adjusted_rates()` receives `players_today` instead of `dksalaries`; team iteration logic is unchanged.
- The analyses models (`leverage_analysis`, `value_analysis`, etc.) continue to reference `clean_draftkings`. They return empty results when DK is unavailable — acceptable since they are DK-context analyses.
- Name mismatches between RotoWire depth charts and the WNBA stats API are resolved by adding entries to the existing `NAME_REPLACE` env var map.
