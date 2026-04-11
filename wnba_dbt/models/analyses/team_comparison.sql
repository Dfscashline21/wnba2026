{{ config(
    materialized='table',
    tags=['analysis']
) }}

-- Team comparison analysis to find missing teams
-- This query compares teams across clean_draftkings, medians, and Games

WITH games_teams AS (
    SELECT DISTINCT "Home_abb" AS team, 'Games (Home)' AS source_type
    FROM {{ source('wnba', 'Games') }}
    UNION
    SELECT DISTINCT "Away_abb" AS team, 'Games (Away)' AS source_type  
    FROM {{ source('wnba', 'Games') }}
),

all_games_teams AS (
    SELECT DISTINCT team, 'Games' AS source_name
    FROM games_teams
),

draftkings_teams AS (
    SELECT DISTINCT "TeamAbbrev" AS team, 'clean_draftkings' AS source_name
    FROM {{ ref('clean_draftkings') }}
),

medians_teams AS (
    SELECT DISTINCT "TeamAbbrev" AS team, 'medians' AS source_name
    FROM {{ ref('medians') }}
),

all_teams AS (
    SELECT team, source_name FROM all_games_teams
    UNION ALL
    SELECT team, source_name FROM draftkings_teams  
    UNION ALL
    SELECT team, source_name FROM medians_teams
),

team_presence AS (
    SELECT 
        team,
        MAX(CASE WHEN source_name = 'Games' THEN 1 ELSE 0 END) AS in_games,
        MAX(CASE WHEN source_name = 'clean_draftkings' THEN 1 ELSE 0 END) AS in_draftkings,
        MAX(CASE WHEN source_name = 'medians' THEN 1 ELSE 0 END) AS in_medians
    FROM all_teams
    GROUP BY team
)

SELECT 
    team,
    CASE WHEN in_games = 1 THEN '✓' ELSE '✗' END AS games,
    CASE WHEN in_draftkings = 1 THEN '✓' ELSE '✗' END AS clean_draftkings,
    CASE WHEN in_medians = 1 THEN '✓' ELSE '✗' END AS medians,
    CASE 
        WHEN in_games = 0 THEN 'Missing from Games'
        WHEN in_draftkings = 0 THEN 'Missing from clean_draftkings'
        WHEN in_medians = 0 THEN 'Missing from medians'
        ELSE 'Present in all'
    END AS status,
    -- Show which specific sources are missing this team
    CASE 
        WHEN in_games = 0 AND in_draftkings = 0 AND in_medians = 0 THEN 'ERROR: Team not found anywhere'
        WHEN in_games = 0 THEN 'Games'
        WHEN in_draftkings = 0 AND in_medians = 0 THEN 'clean_draftkings, medians'
        WHEN in_draftkings = 0 THEN 'clean_draftkings'
        WHEN in_medians = 0 THEN 'medians'
        ELSE 'None'
    END AS missing_from
FROM team_presence
ORDER BY 
    CASE WHEN in_games + in_draftkings + in_medians = 3 THEN 1 ELSE 0 END,
    team 