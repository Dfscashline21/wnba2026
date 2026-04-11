{{ config(materialized='table') }}

SELECT 
    load_date,
    player_name,
    stat_type,
    line_value,
    over_prob,
    odds_type,
    pct_over_5,
    pct_over_10,
    over_trend_5,
    over_trend_10,
    games_5,
    games_10,
    overs_5,
    overs_10,
    
    -- Determine recommendation type
    CASE 
        WHEN over_prob >= 0.65 AND over_trend_5 = 'over' AND over_trend_10 = 'over' THEN 'HIGH_CONFIDENCE_OVER'
        WHEN over_prob <= 0.35 AND over_trend_5 = 'under' AND over_trend_10 = 'under' THEN 'HIGH_CONFIDENCE_UNDER'
        ELSE 'NOT_RECOMMENDED'
    END as recommendation_type,
    
    -- Confidence score based on probability and trend alignment
    CASE 
        WHEN over_prob >= 0.65 AND over_trend_5 = 'over' AND over_trend_10 = 'over' THEN
            ROUND(((over_prob * 100)::NUMERIC + 
                   (CASE WHEN overs_5 = 5 THEN 20 WHEN overs_5 = 4 THEN 10 ELSE 0 END)::NUMERIC +
                   (CASE WHEN overs_10 = 10 THEN 20 WHEN overs_10 >= 9 THEN 10 ELSE 0 END)::NUMERIC), 1)
        WHEN over_prob <= 0.35 AND over_trend_5 = 'under' AND over_trend_10 = 'under' THEN
            ROUND((((1 - over_prob) * 100)::NUMERIC + 
                   (CASE WHEN overs_5 = 0 THEN 20 WHEN overs_5 = 1 THEN 10 ELSE 0 END)::NUMERIC +
                   (CASE WHEN overs_10 = 0 THEN 20 WHEN overs_10 <= 1 THEN 10 ELSE 0 END)::NUMERIC), 1)
        ELSE 0
    END as confidence_score

FROM {{ ref('player_prop_trends') }}
WHERE 
    -- High confidence OVER recommendations
    (over_prob >= 0.65 AND over_trend_5 = 'over' AND over_trend_10 = 'over')
    OR
    -- High confidence UNDER recommendations  
    (over_prob <= 0.35 AND over_trend_5 = 'under' AND over_trend_10 = 'under')

ORDER BY 
    confidence_score DESC,
    load_date DESC,
    player_name,
    stat_type