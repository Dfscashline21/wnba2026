{{ config(
    materialized='table',
    tags=['analysis']
) }}

-- Leverage Analysis
-- Analyzes leverage plays, ownership projections, and optimal exposures

WITH player_stats AS (
    SELECT 
    p.game_date::date as "Game_Date",
        p.player_name,
        p.team_abbreviation,
        pl."POSITION" as position,
        m.dkpts as dkpts,
        p.min as minutes_played,
        p.pts as points,
        p.ast as assists,
        p.reb as rebounds,
        p.stl as steals,
        p.blk as blocks,
        p.tov as turnovers,
        -- Calculate fantasy point sources
        p.pts * 1.0 as points_fp,
        p.ast * 1.5 as assists_fp,
        p.reb * 1.25 as rebounds_fp,
        p.stl * 2.0 as steals_fp,
        p.blk * 2.0 as blocks_fp,
        p.tov * -0.5 as turnovers_fp
    FROM {{ ref('clean_logs') }} p
    INNER JOIN {{ ref('clean_players') }} pl ON p.player_name = pl."Player"
    INNER JOIN {{ ref('medians') }} m ON p.player_name = m.player_name
    WHERE m.dkpts > 0  -- Filter out games with no fantasy points
),

player_date_ranges AS (
    SELECT 
        player_name,
        MAX("Game_Date") as latest_game_date
    FROM player_stats
    GROUP BY player_name
),

player_analysis AS (
    SELECT 
        a.player_name,
        a.team_abbreviation,
        a.position,
        -- Basic stats
        COUNT(*) as games_played,
        AVG(a.dkpts) as avg_fp,
        MIN(a.dkpts) as min_fp,
        MAX(a.dkpts) as max_fp,
        STDDEV(a.dkpts) as stddev_fp,
        -- Usage metrics
        AVG(a.minutes_played) as avg_minutes,
        STDDEV(a.minutes_played) as minutes_stddev,
        -- Source of points analysis
        AVG(a.points_fp) as avg_points_fp,
        AVG(a.assists_fp) as avg_assists_fp,
        AVG(a.rebounds_fp) as avg_rebounds_fp,
        AVG(a.steals_fp) as avg_steals_fp,
        AVG(a.blocks_fp) as avg_blocks_fp,
        AVG(a.turnovers_fp) as avg_turnovers_fp,
        -- Recent performance
        AVG(CASE WHEN a."Game_Date" >= d.latest_game_date - INTERVAL '5 days' 
            THEN a.dkpts END) as last_5g_avg_fp,
        AVG(CASE WHEN a."Game_Date" >= d.latest_game_date - INTERVAL '5 days' 
            THEN a.minutes_played END) as last_5g_avg_minutes,
        -- Recent upside
        MAX(CASE WHEN a."Game_Date" >= d.latest_game_date - INTERVAL '5 days' 
            THEN a.dkpts END) as last_5g_max_fp,
        AVG(CASE WHEN a."Game_Date" >= d.latest_game_date - INTERVAL '5 days' 
            AND a.dkpts >= 40 THEN 1 ELSE 0 END) as last_5g_tournament_boom_rate
    FROM player_stats a
    INNER JOIN player_date_ranges d ON a.player_name = d.player_name
    GROUP BY a.player_name, a.team_abbreviation, a.position
    HAVING COUNT(*) >= 5  -- Minimum 5 games played
)

SELECT 
    a.*,
    -- Current DraftKings data
    dk."Salary",
    m.dkpts as projected_points,
    m.minutes_projection,
    -- Salary tier
    CASE 
        WHEN dk."Salary" >= 9000 THEN 'Elite'
        WHEN dk."Salary" >= 8000 THEN 'High'
        WHEN dk."Salary" >= 7000 THEN 'Mid-High'
        WHEN dk."Salary" >= 6000 THEN 'Mid'
        WHEN dk."Salary" >= 5000 THEN 'Mid-Low'
        WHEN dk."Salary" >= 4000 THEN 'Low'
        ELSE 'Value'
    END as salary_tier,
    -- Ownership projection (based on recent performance, consistency, and historical data)
    CASE 
        WHEN last_5g_avg_fp > 35 AND last_5g_tournament_boom_rate > 0.3 AND avg_fp > 30 THEN 'Very High Ownership (>30%)'
        WHEN last_5g_avg_fp > 30 AND last_5g_tournament_boom_rate > 0.2 AND avg_fp > 25 THEN 'High Ownership (20-30%)'
        WHEN last_5g_avg_fp > 25 AND last_5g_tournament_boom_rate > 0.15 AND avg_fp > 20 THEN 'Moderate Ownership (15-20%)'
        WHEN last_5g_avg_fp > 20 AND last_5g_tournament_boom_rate > 0.1 AND avg_fp > 15 THEN 'Low Ownership (10-15%)'
        WHEN last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10 THEN 'Very Low Ownership (5-10%)'
        ELSE 'Minimal Ownership (<5%)'
    END as ownership_projection,
    -- Leverage rating
    CASE 
        WHEN (dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1 THEN 'High Leverage'
        WHEN (dk."Salary" >= 8000 AND dk."Salary" < 9000) OR (dk."Salary" >= 7000 AND dk."Salary" < 8000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp THEN 'Moderate Leverage'
        WHEN (dk."Salary" >= 4000 AND dk."Salary" < 5000) OR (dk."Salary" < 4000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1 THEN 'High Leverage'
        ELSE 'Low Leverage'
    END as leverage_rating,
    -- Uniqueness rating
    CASE 
        WHEN (dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND ((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10))
         AND last_5g_tournament_boom_rate > 0.2 THEN 'Very Unique'
        WHEN (dk."Salary" >= 8000 AND dk."Salary" < 9000) OR (dk."Salary" >= 7000 AND dk."Salary" < 8000)
         AND ((last_5g_avg_fp > 20 AND last_5g_tournament_boom_rate > 0.1 AND avg_fp > 15) OR (last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10))
         AND last_5g_tournament_boom_rate > 0.15 THEN 'Unique'
        WHEN (dk."Salary" >= 4000 AND dk."Salary" < 5000) OR (dk."Salary" < 4000)
         AND ((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10))
         AND last_5g_tournament_boom_rate > 0.2 THEN 'Very Unique'
        ELSE 'Common'
    END as uniqueness_rating,
    -- Leverage value
    (m.dkpts::numeric / (dk."Salary"::numeric / 1000.0)) as leverage_value_score,
    -- Leverage recommendation
    CASE 
        WHEN (dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1
         AND ((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10))
         AND last_5g_tournament_boom_rate > 0.2
         AND ((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10)) THEN 'Strong Leverage Play'
        WHEN ((dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1) OR ((dk."Salary" >= 8000 AND dk."Salary" < 9000) OR (dk."Salary" >= 7000 AND dk."Salary" < 8000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp)
         AND (((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10))
         AND last_5g_tournament_boom_rate > 0.2) OR (((last_5g_avg_fp > 20 AND last_5g_tournament_boom_rate > 0.1 AND avg_fp > 15) OR (last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10))
         AND last_5g_tournament_boom_rate > 0.15) THEN 'Leverage Play'
        WHEN ((dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1) OR ((dk."Salary" >= 8000 AND dk."Salary" < 9000) OR (dk."Salary" >= 7000 AND dk."Salary" < 8000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp) OR ((dk."Salary" >= 4000 AND dk."Salary" < 5000) OR (dk."Salary" < 4000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1) THEN 'Avoid'
        ELSE 'Situational Leverage Play'
    END as leverage_recommendation
FROM player_analysis a
INNER JOIN {{ ref('clean_draftkings') }} dk ON a.player_name = dk.name
INNER JOIN {{ ref('medians') }} m ON a.player_name = m.player_name
WHERE dk."Salary" > 0
ORDER BY 
    CASE 
        WHEN (dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1
         AND ((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10))
         AND last_5g_tournament_boom_rate > 0.2
         AND ((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10)) THEN 1
        WHEN ((dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1) OR ((dk."Salary" >= 8000 AND dk."Salary" < 9000) OR (dk."Salary" >= 7000 AND dk."Salary" < 8000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp)
         AND (((last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10) OR (last_5g_avg_fp <= 15 OR avg_fp <= 10))
         AND last_5g_tournament_boom_rate > 0.2) OR (((last_5g_avg_fp > 20 AND last_5g_tournament_boom_rate > 0.1 AND avg_fp > 15) OR (last_5g_avg_fp > 15 AND last_5g_tournament_boom_rate > 0.05 AND avg_fp > 10))
         AND last_5g_tournament_boom_rate > 0.15) THEN 2
        WHEN ((dk."Salary" >= 6000 AND dk."Salary" < 7000) OR (dk."Salary" >= 5000 AND dk."Salary" < 6000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1) OR ((dk."Salary" >= 8000 AND dk."Salary" < 9000) OR (dk."Salary" >= 7000 AND dk."Salary" < 8000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp) OR ((dk."Salary" >= 4000 AND dk."Salary" < 5000) OR (dk."Salary" < 4000)
         AND m.dkpts > avg_fp * 1.1
         AND last_5g_avg_fp > avg_fp * 1.1) THEN 4
        ELSE 3
    END,
    leverage_value_score DESC 