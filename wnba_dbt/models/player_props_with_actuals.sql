{{ config(materialized='table') }}

WITH props AS (
    SELECT * FROM {{ ref('over_thresholds_with_lines') }}
),

logs AS (
    SELECT * FROM {{ ref('clean_logs') }}
),

joined_data AS (
    SELECT 
        -- Props data
        p.load_date,
        p.player_name,
        p.stat_type,
        p.over_prob,
        p.line_value,
        
        -- Actual game data
        l.season_year,
        l.player_id,
        l.team_abbreviation,
        l.team_name,
        l.game_id,
        l.game_date,
        l.matchup,
        l.wl,
        l.min,
        
        -- Stat columns that correspond to prop types
        l.pts as actual_points,
        l.ast as actual_assists,
        l.reb as actual_rebounds,
        l.fg3m as actual_threes_made,
        
        -- Calculated stats for combined props
        (l.pts + l.ast) as actual_pts_asts,
        (l.pts + l.reb + l.ast) as actual_pts_rebs_asts,
        (l.reb + l.ast) as actual_rebs_asts,
        
        -- Fantasy points
        l.wnba_fantasy_pts as actual_fantasy_points,
        
        -- Determine if over/under hit based on stat_type
        CASE 
            WHEN p.stat_type = 'points' THEN 
                CASE WHEN l.pts > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'assists' THEN 
                CASE WHEN l.ast > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'rebounds' THEN 
                CASE WHEN l.reb > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'three_made' THEN 
                CASE WHEN l.fg3m > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'pts_asts' THEN 
                CASE WHEN (l.pts + l.ast) > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'pts_rebs_asts' THEN 
                CASE WHEN (l.pts + l.reb + l.ast) > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'rebs_asts' THEN 
                CASE WHEN (l.reb + l.ast) > p.line_value THEN 'OVER' ELSE 'UNDER' END
            WHEN p.stat_type = 'fantasy_points' THEN 
                CASE WHEN l.wnba_fantasy_pts > p.line_value THEN 'OVER' ELSE 'UNDER' END
            ELSE 'UNKNOWN'
        END as result,
        
        -- Calculate accuracy (1 if prediction was correct, 0 if not)
        CASE 
            WHEN p.over_prob > 0.65 AND 
                CASE 
                    WHEN p.stat_type = 'points' THEN l.pts > p.line_value
                    WHEN p.stat_type = 'assists' THEN l.ast > p.line_value
                    WHEN p.stat_type = 'rebounds' THEN l.reb > p.line_value
                    WHEN p.stat_type = 'three_made' THEN l.fg3m > p.line_value
                    WHEN p.stat_type = 'pts_asts' THEN (l.pts + l.ast) > p.line_value
                    WHEN p.stat_type = 'pts_rebs_asts' THEN (l.pts + l.reb + l.ast) > p.line_value
                    WHEN p.stat_type = 'rebs_asts' THEN (l.reb + l.ast) > p.line_value
                    WHEN p.stat_type = 'fantasy_points' THEN l.wnba_fantasy_pts > p.line_value
                    ELSE FALSE
                END THEN 1
            WHEN p.over_prob < 0.35 AND 
                CASE 
                    WHEN p.stat_type = 'points' THEN l.pts <= p.line_value
                    WHEN p.stat_type = 'assists' THEN l.ast <= p.line_value
                    WHEN p.stat_type = 'rebounds' THEN l.reb <= p.line_value
                    WHEN p.stat_type = 'three_made' THEN l.fg3m <= p.line_value
                    WHEN p.stat_type = 'pts_asts' THEN (l.pts + l.ast) <= p.line_value
                    WHEN p.stat_type = 'pts_rebs_asts' THEN (l.pts + l.reb + l.ast) <= p.line_value
                    WHEN p.stat_type = 'rebs_asts' THEN (l.reb + l.ast) <= p.line_value
                    WHEN p.stat_type = 'fantasy_points' THEN l.wnba_fantasy_pts <= p.line_value
                    ELSE FALSE
                END THEN 1
            ELSE 0
        END as prediction_accurate
        
    FROM props p
    INNER JOIN logs l 
        ON p.player_name = l.player_name
        AND p.load_date = l.game_date::date
)

SELECT * FROM joined_data