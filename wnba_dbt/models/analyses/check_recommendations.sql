-- Simple analysis to check the high confidence recommendations
SELECT 
    COUNT(*) as total_recommendations,
    COUNT(CASE WHEN prediction_direction = 'OVER' THEN 1 END) as over_recommendations,
    COUNT(CASE WHEN prediction_direction = 'UNDER' THEN 1 END) as under_recommendations,
    AVG(last_5_success_rate) as avg_last_5_success_rate,
    AVG(last_10_success_rate) as avg_last_10_success_rate,
    MIN(load_date) as earliest_date,
    MAX(load_date) as latest_date
FROM {{ ref('prizepicks_high_confidence_recommendations') }}

UNION ALL

SELECT 
    COUNT(*) as total_recommendations,
    COUNT(CASE WHEN prediction_direction = 'OVER' THEN 1 END) as over_recommendations,
    COUNT(CASE WHEN prediction_direction = 'UNDER' THEN 1 END) as under_recommendations,
    AVG(last_5_success_rate) as avg_last_5_success_rate,
    AVG(last_10_success_rate) as avg_last_10_success_rate,
    MIN(load_date) as earliest_date,
    MAX(load_date) as latest_date
FROM {{ ref('prizepicks_high_confidence_recommendations') }}
WHERE load_date = CURRENT_DATE