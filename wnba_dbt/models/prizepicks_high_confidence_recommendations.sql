{{ config(materialized='table') }}

WITH historical_props_with_games AS (
    -- Join historical props with actual game performance
    SELECT 
        pp.load_date,
        pp.player_name,
        pp.stat_type,
        pp.line_value,
        pp.over_prob,
        pp.odds_type,
        gl.game_date,
        gl.team_abbreviation,
        gl.pts,
        gl.reb,
        gl.ast,
        gl.fg3m,
        gl.stl,
        gl.blk,
        gl.tov,
        gl.min,
        
        -- Calculate actual stats for each prop type
        CASE 
            WHEN pp.stat_type = 'points' THEN gl.pts
            WHEN pp.stat_type = 'rebounds' THEN gl.reb
            WHEN pp.stat_type = 'assists' THEN gl.ast
            WHEN pp.stat_type = 'threes_made' THEN gl.fg3m
            WHEN pp.stat_type = 'pa' THEN gl.pts + gl.ast
            WHEN pp.stat_type = 'par' THEN gl.pts + gl.reb + gl.ast
            WHEN pp.stat_type = 'ra' THEN gl.reb + gl.ast
            WHEN pp.stat_type = 'pts_rebs' THEN gl.pts + gl.reb
            WHEN pp.stat_type = 'fantasy_points' THEN 
                gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov
            ELSE NULL
        END as actual_stat,
        
        -- Determine if prediction was accurate
        CASE 
            WHEN pp.over_prob > 0.65 THEN
                CASE 
                    WHEN pp.stat_type = 'points' AND gl.pts > pp.line_value THEN 1
                    WHEN pp.stat_type = 'rebounds' AND gl.reb > pp.line_value THEN 1
                    WHEN pp.stat_type = 'assists' AND gl.ast > pp.line_value THEN 1
                    WHEN pp.stat_type = 'threes_made' AND gl.fg3m > pp.line_value THEN 1
                    WHEN pp.stat_type = 'pa' AND (gl.pts + gl.ast) > pp.line_value THEN 1
                    WHEN pp.stat_type = 'par' AND (gl.pts + gl.reb + gl.ast) > pp.line_value THEN 1
                    WHEN pp.stat_type = 'ra' AND (gl.reb + gl.ast) > pp.line_value THEN 1
                    WHEN pp.stat_type = 'pts_rebs' AND (gl.pts + gl.reb) > pp.line_value THEN 1
                    WHEN pp.stat_type = 'fantasy_points' AND 
                         (gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov) > pp.line_value THEN 1
                    ELSE 0
                END
            WHEN pp.over_prob < 0.35 THEN
                CASE 
                    WHEN pp.stat_type = 'points' AND gl.pts < pp.line_value THEN 1
                    WHEN pp.stat_type = 'rebounds' AND gl.reb < pp.line_value THEN 1
                    WHEN pp.stat_type = 'assists' AND gl.ast < pp.line_value THEN 1
                    WHEN pp.stat_type = 'threes_made' AND gl.fg3m < pp.line_value THEN 1
                    WHEN pp.stat_type = 'pa' AND (gl.pts + gl.ast) < pp.line_value THEN 1
                    WHEN pp.stat_type = 'par' AND (gl.pts + gl.reb + gl.ast) < pp.line_value THEN 1
                    WHEN pp.stat_type = 'ra' AND (gl.reb + gl.ast) < pp.line_value THEN 1
                    WHEN pp.stat_type = 'pts_rebs' AND (gl.pts + gl.reb) < pp.line_value THEN 1
                    WHEN pp.stat_type = 'fantasy_points' AND 
                         (gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov) < pp.line_value THEN 1
                    ELSE 0
                END
            ELSE 0
        END as prediction_accurate,
        
        -- Prediction direction
        CASE 
            WHEN pp.over_prob > 0.65 THEN 'OVER'
            WHEN pp.over_prob < 0.35 THEN 'UNDER'
            ELSE 'NEUTRAL'
        END as prediction_direction,
        
        -- Season year
        EXTRACT(YEAR FROM gl.game_date::DATE) as season_year,
        
        -- Row number for historical analysis
        ROW_NUMBER() OVER (
            PARTITION BY pp.player_name, pp.stat_type, pp.odds_type 
            ORDER BY gl.game_date DESC
        ) as game_number
        
    FROM {{ ref('prizepicks_thresholds_with_lines') }} pp
    INNER JOIN {{ ref('clean_logs') }} gl 
        ON pp.player_name = gl.player_name 
        AND pp.load_date = gl.game_date::date
    WHERE gl.min >= 10
),

player_historical_performance AS (
    -- Calculate historical performance for each player/stat/odds_type combination
    SELECT 
        player_name,
        stat_type,
        odds_type,
        game_date,
        prediction_accurate,
        prediction_direction,
        game_number,
        
        -- Calculate last 5 games performance
        SUM(CASE WHEN game_number <= 5 THEN prediction_accurate ELSE 0 END) OVER (
            PARTITION BY player_name, stat_type, odds_type
        ) as last_5_correct,
        
        COUNT(CASE WHEN game_number <= 5 THEN 1 END) OVER (
            PARTITION BY player_name, stat_type, odds_type
        ) as last_5_total,
        
        -- Calculate last 10 games performance
        SUM(CASE WHEN game_number <= 10 THEN prediction_accurate ELSE 0 END) OVER (
            PARTITION BY player_name, stat_type, odds_type
        ) as last_10_correct,
        
        COUNT(CASE WHEN game_number <= 10 THEN 1 END) OVER (
            PARTITION BY player_name, stat_type, odds_type
        ) as last_10_total
        
    FROM historical_props_with_games
),

recommendations AS (
    SELECT 
        hpg.*,
        php.last_5_correct,
        php.last_5_total,
        php.last_10_correct,
        php.last_10_total,
        
        -- Calculate success rates
        CASE 
            WHEN php.last_5_total > 0 THEN ROUND(php.last_5_correct::NUMERIC / php.last_5_total * 100, 1)
            ELSE 0 
        END as last_5_success_rate,
        
        CASE 
            WHEN php.last_10_total > 0 THEN ROUND(php.last_10_correct::NUMERIC / php.last_10_total * 100, 1)
            ELSE 0 
        END as last_10_success_rate,
        
        -- Determine recommendation status
        CASE 
            WHEN hpg.prediction_direction = 'OVER' THEN
                CASE 
                    WHEN php.last_5_total >= 5 AND php.last_5_correct >= 4 
                         AND php.last_10_total >= 10 AND php.last_10_correct >= 8 THEN 'HIGH_CONFIDENCE_OVER'
                    ELSE 'NOT_RECOMMENDED'
                END
            WHEN hpg.prediction_direction = 'UNDER' THEN
                CASE 
                    WHEN php.last_5_total >= 5 AND php.last_5_correct >= 4 THEN 'HIGH_CONFIDENCE_UNDER'
                    ELSE 'NOT_RECOMMENDED'
                END
            ELSE 'NOT_RECOMMENDED'
        END as recommendation_status
        
    FROM historical_props_with_games hpg
    INNER JOIN player_historical_performance php
        ON hpg.player_name = php.player_name
        AND hpg.stat_type = php.stat_type
        AND hpg.odds_type = php.odds_type
        AND hpg.game_date = php.game_date
)

SELECT 
    load_date,
    player_name,
    stat_type,
    line_value,
    over_prob,
    odds_type,
    game_date,
    team_abbreviation,
    actual_stat,
    prediction_accurate,
    prediction_direction,
    season_year,
    recommendation_status,
    last_5_correct,
    last_5_total,
    last_5_success_rate,
    last_10_correct,
    last_10_total,
    last_10_success_rate,
    
    -- Only include as recommendation if it meets criteria
    CASE 
        WHEN recommendation_status IN ('HIGH_CONFIDENCE_OVER', 'HIGH_CONFIDENCE_UNDER') THEN 1
        ELSE 0
    END as is_recommendation
    
FROM recommendations
WHERE recommendation_status IN ('HIGH_CONFIDENCE_OVER', 'HIGH_CONFIDENCE_UNDER')
ORDER BY load_date DESC, over_prob DESC