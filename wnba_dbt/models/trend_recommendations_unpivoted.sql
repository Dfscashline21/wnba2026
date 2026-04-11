{{ config(materialized='table') }}

-- Clean view of trend-based recommendations
-- Similar structure to prizepicks_thresholds_with_lines but with trend analysis

SELECT
    load_date,
    player_name,
    stat_type,
    line_value,
    over_prob,
    odds_type,
    
    -- Trend analysis
    pct_over_5,
    pct_over_10,
    over_trend_5,
    over_trend_10,
    
    -- Historical performance counts
    games_5,
    games_10,
    overs_5,
    overs_10,
    
    -- Recommendation details
    recommendation_type,
    confidence_score,
    
    -- Additional context
    CASE 
        WHEN recommendation_type = 'HIGH_CONFIDENCE_OVER' THEN 'OVER'
        WHEN recommendation_type = 'HIGH_CONFIDENCE_UNDER' THEN 'UNDER'
        ELSE 'NEUTRAL'
    END as prediction_direction,
    
    -- Trend strength indicators
    CASE 
        WHEN overs_5 = 5 THEN 'PERFECT_5'
        WHEN overs_5 = 4 THEN 'STRONG_5'
        WHEN overs_5 = 0 THEN 'PERFECT_5_UNDER'
        WHEN overs_5 = 1 THEN 'STRONG_5_UNDER'
        ELSE 'MIXED_5'
    END as trend_strength_5,
    
    CASE 
        WHEN overs_10 = 10 THEN 'PERFECT_10'
        WHEN overs_10 >= 9 THEN 'STRONG_10'
        WHEN overs_10 = 0 THEN 'PERFECT_10_UNDER'
        WHEN overs_10 <= 1 THEN 'STRONG_10_UNDER'
        ELSE 'MIXED_10'
    END as trend_strength_10

FROM {{ ref('trend_based_recommendations') }}

ORDER BY 
    confidence_score DESC,
    load_date DESC,
    player_name,
    stat_type