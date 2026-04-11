{{ config(
    materialized='table',
    tags=['analysis']
) }}

-- Game Stack Analysis
-- Identifies games and team combinations with highest scoring potential

WITH game_totals AS (
    SELECT 
        g."Home_abb",
        g."Away_abb",
        g."Home_abb" || ' vs ' || g."Away_abb" as matchup,
        
        -- Home team projections
        SUM(CASE WHEN m."TeamAbbrev" = g."Home_abb" THEN m.dkpts ELSE 0 END) as home_team_total,
        COUNT(CASE WHEN m."TeamAbbrev" = g."Home_abb" THEN 1 END) as home_players,
        
        -- Away team projections  
        SUM(CASE WHEN m."TeamAbbrev" = g."Away_abb" THEN m.dkpts ELSE 0 END) as away_team_total,
        COUNT(CASE WHEN m."TeamAbbrev" = g."Away_abb" THEN 1 END) as away_players,
        
        -- Combined game total
        SUM(m.dkpts) as game_total_projection,
        COUNT(*) as total_players_in_game,
        
        -- Average salary by team
        AVG(CASE WHEN m."TeamAbbrev" = g."Home_abb" THEN dk."Salary" END) as home_avg_salary,
        AVG(CASE WHEN m."TeamAbbrev" = g."Away_abb" THEN dk."Salary" END) as away_avg_salary
        
    FROM {{ source('wnba', 'Games') }} g
    LEFT JOIN {{ ref('medians') }} m ON g."Home_abb" = m."TeamAbbrev" OR g."Away_abb" = m."TeamAbbrev"
    LEFT JOIN {{ ref('clean_draftkings') }} dk ON m.player_name = dk.name
    WHERE m.dkpts IS NOT NULL
    GROUP BY g."Home_abb", g."Away_abb"
),

stack_opportunities AS (
    SELECT 
        *,
        -- Stack value metrics
        ROUND((game_total_projection::numeric / NULLIF(total_players_in_game, 0)), 1) as avg_points_per_player,
        ROUND(((home_avg_salary + away_avg_salary)::numeric / 2), 0) as avg_game_salary,
        
        -- Stack recommendations
        CASE 
            WHEN game_total_projection > (SELECT AVG(game_total_projection) * 1.1 FROM game_totals) 
            THEN 'High Stack Priority'
            WHEN game_total_projection > (SELECT AVG(game_total_projection) FROM game_totals)
            THEN 'Medium Stack Priority' 
            ELSE 'Low Stack Priority'
        END as stack_priority,
        
        -- Pace/correlation indicators
        ABS(home_team_total - away_team_total) as team_projection_difference,
        CASE 
            WHEN ABS(home_team_total - away_team_total) < 10 THEN 'Close Game (High Correlation)'
            WHEN ABS(home_team_total - away_team_total) < 20 THEN 'Moderate Spread'
            ELSE 'Blowout Risk'
        END as game_script
        
    FROM game_totals
)

SELECT 
    matchup,
    "Home_abb" as home_team,
    "Away_abb" as away_team,
    ROUND(home_team_total::numeric, 1) as home_projected_total,
    ROUND(away_team_total::numeric, 1) as away_projected_total,
    ROUND(game_total_projection::numeric, 1) as game_total,
    avg_points_per_player,
    avg_game_salary,
    stack_priority,
    game_script,
    
    -- Rank games by stack appeal
    ROW_NUMBER() OVER (ORDER BY game_total_projection DESC) as stack_rank
    
FROM stack_opportunities
ORDER BY game_total_projection DESC 