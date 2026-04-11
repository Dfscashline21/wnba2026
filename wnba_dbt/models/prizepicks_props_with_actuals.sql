{{ config(materialized='table') }}

WITH joined_data AS (
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
        
        -- Season year (cast game_date to DATE first)
        EXTRACT(YEAR FROM gl.game_date::DATE) as season_year
        
    FROM {{ ref('prizepicks_thresholds_with_lines') }} pp
    INNER JOIN {{ ref('clean_logs') }} gl 
        ON pp.player_name = gl.player_name 
        AND pp.load_date = gl.game_date::date)

SELECT * FROM joined_data