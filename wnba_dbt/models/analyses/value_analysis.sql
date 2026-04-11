{{ config(
    materialized='table',
    tags=['analysis']
) }}

-- Value Analysis: Points per Dollar
-- Identifies players with the best value (projected points per salary dollar)

WITH player_value AS (
    SELECT 
        dk.name,
        dk."TeamAbbrev",
        dk."Position",
        dk."Salary",
        dk."AvgPointsPerGame",
        m.dkpts as projected_points,
        m.minutes_projection,
        -- Calculate value metrics
        CASE 
            WHEN dk."Salary" > 0 THEN m.dkpts::numeric / (dk."Salary"::numeric / 1000.0)
            ELSE 0 
        END as points_per_1k_salary,
        
        CASE 
            WHEN m.minutes_projection > 0 THEN m.dkpts::numeric / m.minutes_projection::numeric
            ELSE 0 
        END as points_per_minute,
        
        -- Salary tier classification
        CASE 
            WHEN dk."Salary" >= 10000 THEN 'Elite ($10K+)'
            WHEN dk."Salary" >= 8000 THEN 'High ($8K-$10K)'
            WHEN dk."Salary" >= 6000 THEN 'Mid ($6K-$8K)'
            WHEN dk."Salary" >= 4000 THEN 'Low ($4K-$6K)'
            ELSE 'Value (<$4K)'
        END as salary_tier
        
    FROM {{ ref('clean_draftkings') }} dk
    LEFT JOIN {{ ref('medians') }} m ON dk.name = m.player_name
    WHERE dk."Salary" > 0 
      AND m.dkpts IS NOT NULL
)

SELECT 
    name,
    "TeamAbbrev",
    "Position", 
    "Salary",
    projected_points,
    minutes_projection,
    ROUND(points_per_1k_salary::numeric, 2) as value_score,
    ROUND(points_per_minute::numeric, 2) as efficiency_score,
    salary_tier,
    -- Rank within position
    ROW_NUMBER() OVER (PARTITION BY "Position" ORDER BY points_per_1k_salary DESC) as value_rank_in_position,
    -- Overall value percentile
    ROUND((PERCENT_RANK() OVER (ORDER BY points_per_1k_salary) * 100)::numeric, 1) as value_percentile
FROM player_value
WHERE projected_points > 0
ORDER BY points_per_1k_salary DESC 