{{ config(
    materialized='table',
    tags=['marts']
) }}

-- Game Model Projections
-- Summarizes median player point projections (medianppm) by team,
-- joins with the Games slate, and produces team-level projected scores,
-- implied totals, and implied spreads per matchup.

WITH team_projections AS (
    SELECT
        "TeamAbbrev" AS team,
        SUM(medianppm) AS projected_score
    FROM {{ ref('medians') }}
    WHERE medianppm IS NOT NULL
    GROUP BY "TeamAbbrev"
),

matchups AS (
    SELECT DISTINCT
        g."Home_abb" AS home_team,
        g."Away_abb" AS away_team
    FROM {{ source('wnba', 'Games') }} g
    WHERE g."Home_abb" IS NOT NULL
      AND g."Away_abb" IS NOT NULL
)

SELECT
    m.home_team,
    m.away_team,
    ROUND(home.projected_score::numeric, 1) AS home_projected_score,
    ROUND(away.projected_score::numeric, 1) AS away_projected_score,
    ROUND((COALESCE(home.projected_score, 0) + COALESCE(away.projected_score, 0))::numeric, 1) AS implied_total,
    ROUND((COALESCE(home.projected_score, 0) - COALESCE(away.projected_score, 0))::numeric, 1) AS implied_spread
FROM matchups m
LEFT JOIN team_projections home ON m.home_team = home.team
LEFT JOIN team_projections away ON m.away_team = away.team
ORDER BY implied_total DESC
