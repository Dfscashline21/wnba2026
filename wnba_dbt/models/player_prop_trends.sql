{{ config(materialized='table') }}

WITH historical_games AS (
    -- Get all historical games for each player before their load_date
    SELECT 
        pp.load_date,
        pp.player_name,
        pp.stat_type,
        pp.line_value,
        pp.over_prob,
        pp.odds_type,
        gl.game_date,
        gl.pts,
        gl.reb,
        gl.ast,
        gl.fg3m,
        gl.stl,
        gl.blk,
        gl.tov,
        gl.min,
        
        -- Calculate the actual stat value based on stat_type
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
        
        -- Determine if the player went over the line
        CASE 
            WHEN pp.stat_type = 'points' AND gl.pts IS NOT NULL THEN
                CASE WHEN gl.pts > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'rebounds' AND gl.reb IS NOT NULL THEN
                CASE WHEN gl.reb > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'assists' AND gl.ast IS NOT NULL THEN
                CASE WHEN gl.ast > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'threes_made' AND gl.fg3m IS NOT NULL THEN
                CASE WHEN gl.fg3m > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'pa' AND gl.pts IS NOT NULL AND gl.ast IS NOT NULL THEN
                CASE WHEN (gl.pts + gl.ast) > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'par' AND gl.pts IS NOT NULL AND gl.reb IS NOT NULL AND gl.ast IS NOT NULL THEN
                CASE WHEN (gl.pts + gl.reb + gl.ast) > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'ra' AND gl.reb IS NOT NULL AND gl.ast IS NOT NULL THEN
                CASE WHEN (gl.reb + gl.ast) > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'pts_rebs' AND gl.pts IS NOT NULL AND gl.reb IS NOT NULL THEN
                CASE WHEN (gl.pts + gl.reb) > pp.line_value THEN 1 ELSE 0 END
            WHEN pp.stat_type = 'fantasy_points' AND gl.pts IS NOT NULL AND gl.reb IS NOT NULL AND gl.ast IS NOT NULL AND gl.stl IS NOT NULL AND gl.blk IS NOT NULL AND gl.tov IS NOT NULL THEN
                CASE WHEN (gl.pts + (1.2 * gl.reb) + (1.1 * gl.ast) + (3 * (gl.stl + gl.blk)) - gl.tov) > pp.line_value THEN 1 ELSE 0 END
            ELSE NULL
        END as went_over,
        
        -- Row number to rank games by date (most recent first)
        ROW_NUMBER() OVER (
            PARTITION BY pp.load_date, pp.player_name, pp.stat_type, pp.odds_type
            ORDER BY gl.game_date DESC
        ) as game_rank
        
    FROM {{ ref('prizepicks_thresholds_with_lines') }} pp
    INNER JOIN {{ ref('clean_logs') }} gl 
        ON pp.player_name = gl.player_name 
        AND gl.game_date::date < pp.load_date  -- Only games before the load_date
        AND gl.min >= 10  -- Only games with significant minutes
    WHERE 
        -- Only include games where the relevant stat is not null
        CASE 
            WHEN pp.stat_type = 'points' THEN gl.pts IS NOT NULL
            WHEN pp.stat_type = 'rebounds' THEN gl.reb IS NOT NULL
            WHEN pp.stat_type = 'assists' THEN gl.ast IS NOT NULL
            WHEN pp.stat_type = 'threes_made' THEN gl.fg3m IS NOT NULL
            WHEN pp.stat_type = 'pa' THEN gl.pts IS NOT NULL AND gl.ast IS NOT NULL
            WHEN pp.stat_type = 'par' THEN gl.pts IS NOT NULL AND gl.reb IS NOT NULL AND gl.ast IS NOT NULL
            WHEN pp.stat_type = 'ra' THEN gl.reb IS NOT NULL AND gl.ast IS NOT NULL
            WHEN pp.stat_type = 'pts_rebs' THEN gl.pts IS NOT NULL AND gl.reb IS NOT NULL
            WHEN pp.stat_type = 'fantasy_points' THEN gl.pts IS NOT NULL AND gl.reb IS NOT NULL AND gl.ast IS NOT NULL AND gl.stl IS NOT NULL AND gl.blk IS NOT NULL AND gl.tov IS NOT NULL
            ELSE FALSE
        END
),

trend_calculations AS (
    SELECT 
        load_date,
        player_name,
        stat_type,
        line_value,
        over_prob,
        odds_type,
        
        -- Calculate percentage over in last 5 games
        CASE 
            WHEN COUNT(CASE WHEN game_rank <= 5 THEN 1 END) >= 1 THEN
                ROUND(
                    SUM(CASE WHEN game_rank <= 5 THEN went_over ELSE 0 END)::NUMERIC / 
                    COUNT(CASE WHEN game_rank <= 5 THEN 1 END) * 100, 
                    1
                )
            ELSE NULL
        END as pct_over_5,
        
        -- Calculate percentage over in last 10 games
        CASE 
            WHEN COUNT(CASE WHEN game_rank <= 10 THEN 1 END) >= 1 THEN
                ROUND(
                    SUM(CASE WHEN game_rank <= 10 THEN went_over ELSE 0 END)::NUMERIC / 
                    COUNT(CASE WHEN game_rank <= 10 THEN 1 END) * 100, 
                    1
                )
            ELSE NULL
        END as pct_over_10,
        
        -- Count of games available for analysis
        COUNT(CASE WHEN game_rank <= 5 THEN 1 END) as games_5,
        COUNT(CASE WHEN game_rank <= 10 THEN 1 END) as games_10,
        
        -- Count of games over the line
        SUM(CASE WHEN game_rank <= 5 THEN went_over ELSE 0 END) as overs_5,
        SUM(CASE WHEN game_rank <= 10 THEN went_over ELSE 0 END) as overs_10
        
    FROM historical_games
    GROUP BY load_date, player_name, stat_type, line_value, over_prob, odds_type
)

SELECT 
    load_date,
    player_name,
    stat_type,
    line_value,
    over_prob,
    odds_type,
    pct_over_5,
    pct_over_10,
    
    -- Determine trends based on specific thresholds
    CASE 
        WHEN pct_over_5 IS NULL THEN NULL
        WHEN overs_5 >= 4 THEN 'over'  -- 4/5 or more
        WHEN overs_5 <= 2 THEN 'under' -- 2/5 or less
        ELSE 'neutral'
    END as over_trend_5,
    
    CASE 
        WHEN pct_over_10 IS NULL THEN NULL
        WHEN overs_10 >= 8 THEN 'over'  -- 8/10 or more
        WHEN overs_10 <= 2 THEN 'under' -- 2/10 or less
        ELSE 'neutral'
    END as over_trend_10,
    
    -- Additional context
    games_5,
    games_10,
    overs_5,
    overs_10
    
FROM trend_calculations
ORDER BY load_date DESC, player_name, stat_type