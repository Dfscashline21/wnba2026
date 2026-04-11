{{ config(
    materialized='table',
    tags=['analysis']
) }}

-- Ownership Correlation Analysis
-- Analyzes relationships between salary, projections, and expected ownership

WITH player_metrics AS (
    SELECT 
        dk.name,
        dk."TeamAbbrev",
        dk."Position",
        dk."Salary",
        m.dkpts as projected_points,
        m.minutes_projection,
        
        -- Calculate percentile ranks for key metrics
        PERCENT_RANK() OVER (ORDER BY dk."Salary") as salary_percentile,
        PERCENT_RANK() OVER (ORDER BY m.dkpts) as projection_percentile,
        PERCENT_RANK() OVER (ORDER BY m.dkpts / NULLIF(dk."Salary", 0)) as value_percentile,
        
        -- Expected ownership proxy (weighted combination of factors)
        -- Salary has 40% weight, projections 40%, value 20%
        -- Then scaled to realistic ownership range (5-50%)
        (0.4 * PERCENT_RANK() OVER (ORDER BY dk."Salary") + 
         0.4 * PERCENT_RANK() OVER (ORDER BY m.dkpts) +
         0.2 * PERCENT_RANK() OVER (ORDER BY m.dkpts / NULLIF(dk."Salary", 0))) * 45 + 5 as expected_ownership_proxy,
         
        -- Contrarian score (high projection, lower salary)
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY m.dkpts) > 0.7 
             AND PERCENT_RANK() OVER (ORDER BY dk."Salary") < 0.5 
            THEN 'High Contrarian Value'
            WHEN PERCENT_RANK() OVER (ORDER BY m.dkpts) > 0.5 
             AND PERCENT_RANK() OVER (ORDER BY dk."Salary") < 0.3 
            THEN 'Medium Contrarian Value'
            ELSE 'Chalk Play'
        END as contrarian_category
        
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
    ROUND(projected_points::numeric, 1) as projected_points,
    ROUND((salary_percentile * 100)::numeric, 1) as salary_percentile,
    ROUND((projection_percentile * 100)::numeric, 1) as projection_percentile,
    ROUND((value_percentile * 100)::numeric, 1) as value_percentile,
    ROUND(expected_ownership_proxy::numeric, 1) as expected_ownership_pct,
    contrarian_category,
    
    -- Tournament strategy flags
    CASE 
        WHEN expected_ownership_proxy > 35 THEN 'Avoid in GPP'
        WHEN expected_ownership_proxy < 15 AND projection_percentile > 0.6 THEN 'GPP Target'
        WHEN expected_ownership_proxy BETWEEN 20 AND 30 THEN 'Cash Game Viable'
        ELSE 'Situational'
    END as tournament_strategy
    
FROM player_metrics
ORDER BY expected_ownership_proxy DESC 