{{ config(materialized='table') }}

WITH joined_data AS (
    SELECT 
        tr.load_date,
        tr.player_name,
        tr.stat_type,
        tr.line_value,
        tr.over_prob,
        tr.odds_type,
        tr.pct_over_5,
        tr.pct_over_10,
        tr.over_trend_5,
        tr.over_trend_10,
        tr.games_5,
        tr.games_10,
        tr.overs_5,
        tr.overs_10,
        tr.recommendation_type,
        tr.confidence_score,
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
            WHEN tr.stat_type = 'points' THEN gl.pts
            WHEN tr.stat_type = 'rebounds' THEN gl.reb
            WHEN tr.stat_type = 'assists' THEN gl.ast
            WHEN tr.stat_type = 'threes_made' THEN gl.fg3m
            WHEN tr.stat_type = 'pa' THEN gl.pts + gl.ast
            WHEN tr.stat_type = 'par' THEN gl.pts + gl.reb + gl.ast
            WHEN tr.stat_type = 'ra' THEN gl.reb + gl.ast
            WHEN tr.stat_type = 'pts_rebs' THEN gl.pts + gl.reb
            WHEN tr.stat_type = 'fantasy_points' THEN 
                gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov
            ELSE NULL
        END as actual_stat,
        
        -- Determine if prediction was accurate
        CASE 
            WHEN tr.recommendation_type = 'HIGH_CONFIDENCE_OVER' THEN
                CASE 
                    WHEN tr.stat_type = 'points' AND gl.pts > tr.line_value THEN 1
                    WHEN tr.stat_type = 'rebounds' AND gl.reb > tr.line_value THEN 1
                    WHEN tr.stat_type = 'assists' AND gl.ast > tr.line_value THEN 1
                    WHEN tr.stat_type = 'threes_made' AND gl.fg3m > tr.line_value THEN 1
                    WHEN tr.stat_type = 'pa' AND (gl.pts + gl.ast) > tr.line_value THEN 1
                    WHEN tr.stat_type = 'par' AND (gl.pts + gl.reb + gl.ast) > tr.line_value THEN 1
                    WHEN tr.stat_type = 'ra' AND (gl.reb + gl.ast) > tr.line_value THEN 1
                    WHEN tr.stat_type = 'pts_rebs' AND (gl.pts + gl.reb) > tr.line_value THEN 1
                    WHEN tr.stat_type = 'fantasy_points' AND 
                         (gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov) > tr.line_value THEN 1
                    ELSE 0
                END
            WHEN tr.recommendation_type = 'HIGH_CONFIDENCE_UNDER' THEN
                CASE 
                    WHEN tr.stat_type = 'points' AND gl.pts < tr.line_value THEN 1
                    WHEN tr.stat_type = 'rebounds' AND gl.reb < tr.line_value THEN 1
                    WHEN tr.stat_type = 'assists' AND gl.ast < tr.line_value THEN 1
                    WHEN tr.stat_type = 'threes_made' AND gl.fg3m < tr.line_value THEN 1
                    WHEN tr.stat_type = 'pa' AND (gl.pts + gl.ast) < tr.line_value THEN 1
                    WHEN tr.stat_type = 'par' AND (gl.pts + gl.reb + gl.ast) < tr.line_value THEN 1
                    WHEN tr.stat_type = 'ra' AND (gl.reb + gl.ast) < tr.line_value THEN 1
                    WHEN tr.stat_type = 'pts_rebs' AND (gl.pts + gl.reb) < tr.line_value THEN 1
                    WHEN tr.stat_type = 'fantasy_points' AND 
                         (gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov) < tr.line_value THEN 1
                    ELSE 0
                END
            ELSE 0
        END as prediction_accurate,
        
        -- Season year
        EXTRACT(YEAR FROM gl.game_date::DATE) as season_year
        
    FROM {{ ref('trend_based_recommendations') }} tr
    INNER JOIN {{ ref('clean_logs') }} gl 
        ON tr.player_name = gl.player_name 
        AND tr.load_date = gl.game_date::date
    WHERE gl.min >= 10
)

SELECT * FROM joined_data
ORDER BY 
    confidence_score DESC,
    load_date DESC,
    player_name,
    stat_type