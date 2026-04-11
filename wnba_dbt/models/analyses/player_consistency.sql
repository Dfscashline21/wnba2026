{{ config(
    materialized='table',
    tags=['analysis']
) }}

-- Player Consistency Analysis
-- Analyzes player consistency, volatility, and reliability in fantasy scoring

WITH player_game_stats AS (
    SELECT 
        p.player_name,
        p.team_abbreviation,
        pl."POSITION" as position,
        CASE
        WHEN matchup LIKE team_abbreviation || ' vs.%' THEN 'Home'
        WHEN matchup LIKE team_abbreviation || ' @%' THEN 'Away'
        ELSE 'Unknown'
        END AS game_location,
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
        p.tov * -0.5 as turnovers_fp,
        p.pts * 1.0 + p.ast * 1.5 + p.reb * 1.25 + p.stl * 2.0 + p.blk * 2.0 - p.tov * 0.5 as dkpts
    FROM {{ ref('clean_logs') }} p
    INNER JOIN {{ ref('clean_players') }} pl ON p.player_name = pl."Player"
    INNER JOIN {{ ref('medians') }} m ON p.player_name = m.player_name
    WHERE dkpts > 0  -- Filter out games with no fantasy points
),

consistency_metrics AS (
    SELECT 
        player_name,
        team_abbreviation,
        position,
        COUNT(*) as games_played,
        -- Basic stats
        AVG(dkpts) as avg_fp,
        MIN(dkpts) as min_fp,
        MAX(dkpts) as max_fp,
        stddev(dkpts) as stddev_fp,
        -- Consistency metrics
        AVG(CASE WHEN dkpts >= 30 THEN 1 ELSE 0 END) as boom_rate,
        AVG(CASE WHEN dkpts <= 15 THEN 1 ELSE 0 END) as bust_rate,
        -- Volatility metrics
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY dkpts) as q1_fp,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY dkpts) as q3_fp,
        -- Source of points analysis
        AVG(points_fp) as avg_points_fp,
        AVG(assists_fp) as avg_assists_fp,
        AVG(rebounds_fp) as avg_rebounds_fp,
        AVG(steals_fp) as avg_steals_fp,
        AVG(blocks_fp) as avg_blocks_fp,
        AVG(turnovers_fp) as avg_turnovers_fp,
        -- Location splits
        AVG(CASE WHEN game_location = 'Home' THEN dkpts END) as home_avg_fp,
        AVG(CASE WHEN game_location = 'Away' THEN dkpts END) as away_avg_fp,
        -- Minutes reliability
        AVG(minutes_played) as avg_minutes,
        stddev(minutes_played) as minutes_stddev
    FROM player_game_stats
    GROUP BY player_name, team_abbreviation, position
    HAVING COUNT(*) >= 5  -- Minimum 5 games played
)

SELECT 
    c.*,
    -- Current DraftKings data
    dk."Salary",
    m.dkpts as projected_points,
    m.minutes_projection,
    -- Consistency ratings
    CASE 
        WHEN stddev_fp < 5 AND boom_rate > 0.2 THEN 'Elite Consistency'
        WHEN stddev_fp < 7 AND boom_rate > 0.15 THEN 'High Consistency'
        WHEN stddev_fp < 10 AND boom_rate > 0.1 THEN 'Moderate Consistency'
        ELSE 'Volatile'
    END as consistency_rating,
    -- Volatility score (lower is better)
    ROUND((stddev_fp::numeric / NULLIF(avg_fp, 0) * 100), 1) as volatility_score,
    -- Floor/Ceiling projections
    ROUND((q1_fp::numeric * 0.8), 1) as floor_projection,
    ROUND((q3_fp::numeric * 1.2), 1) as ceiling_projection,
    -- Value metrics
    ROUND((avg_fp::numeric / (dk."Salary"::numeric / 1000.0)), 2) as avg_value_score,
    -- Minutes reliability
    CASE 
        WHEN minutes_stddev < 3 THEN 'Very Reliable Minutes'
        WHEN minutes_stddev < 5 THEN 'Reliable Minutes'
        WHEN minutes_stddev < 8 THEN 'Variable Minutes'
        ELSE 'Unreliable Minutes'
    END as minutes_reliability,
    -- Usage pattern
    CASE 
        WHEN avg_points_fp > avg_fp * 0.6 THEN 'Scoring Dependent'
        WHEN avg_assists_fp > avg_fp * 0.4 THEN 'Playmaking Dependent'
        WHEN avg_rebounds_fp > avg_fp * 0.4 THEN 'Rebounding Dependent'
        WHEN (avg_steals_fp + avg_blocks_fp) > avg_fp * 0.3 THEN 'Defensive Upside'
        ELSE 'Balanced Production'
    END as usage_pattern,
    -- Game type recommendation
    CASE 
        WHEN CASE 
        WHEN stddev_fp < 5 AND boom_rate > 0.2 THEN 'Elite Consistency'
        WHEN stddev_fp < 7 AND boom_rate > 0.15 THEN 'High Consistency'
        WHEN stddev_fp < 10 AND boom_rate > 0.1 THEN 'Moderate Consistency'
        ELSE 'Volatile'
    END IN ('Elite Consistency', 'High Consistency') 
         AND ROUND((avg_fp::numeric / (dk."Salary"::numeric / 1000.0)), 2) > 3.5 THEN 'Cash Game Core'
        WHEN boom_rate > 0.25 AND ROUND((q3_fp::numeric * 1.2), 1) > 40 THEN 'GPP Target'
        WHEN  (CASE 
        WHEN stddev_fp < 5 AND boom_rate > 0.2 THEN 'Elite Consistency'
        WHEN stddev_fp < 7 AND boom_rate > 0.15 THEN 'High Consistency'
        WHEN stddev_fp < 10 AND boom_rate > 0.1 THEN 'Moderate Consistency'
        ELSE 'Volatile'
    END ) = 'Volatile' AND boom_rate > 0.2 THEN 'GPP Only'
        ELSE 'Situational Play'
    END as game_type_recommendation
FROM consistency_metrics c
INNER JOIN {{ ref('clean_draftkings') }} dk ON c.player_name = dk.name
INNER JOIN {{ ref('medians') }} m ON c.player_name = m.player_name
WHERE dk."Salary" > 0
ORDER BY consistency_rating, volatility_score 