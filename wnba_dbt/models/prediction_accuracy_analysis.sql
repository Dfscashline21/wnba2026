{{ config(materialized='table') }}

-- Analysis: Prediction Accuracy Evaluation
-- This analysis evaluates the accuracy of prop predictions across various dimensions

WITH accuracy_metrics AS (
    SELECT 
        -- Overall accuracy
        COUNT(*) as total_predictions,
        SUM(prediction_accurate) as correct_predictions,
        ROUND(AVG(prediction_accurate) * 100, 2) as overall_accuracy_pct,
        
        -- Accuracy by stat type
        stat_type,
        COUNT(*) as stat_type_total,
        SUM(prediction_accurate) as stat_type_correct,
        ROUND(AVG(prediction_accurate) * 100, 2) as stat_type_accuracy_pct,
        
        -- Accuracy by confidence level
        CASE 
            WHEN over_prob >= 0.8 THEN 'Very High Confidence (80%+)'
            WHEN over_prob >= 0.7 THEN 'High Confidence (70-79%)'
            WHEN over_prob >= 0.65 THEN 'Moderate Confidence (65-69%)'
            WHEN over_prob <= 0.2 THEN 'Very Low Confidence (≤20%)'
            WHEN over_prob <= 0.3 THEN 'Low Confidence (21-30%)'
            WHEN over_prob <= 0.35 THEN 'Moderate Low Confidence (31-35%)'
            ELSE 'Medium Confidence (36-64%)'
        END as confidence_level,
        
        -- Accuracy by prediction direction
        CASE 
            WHEN over_prob > 0.65 THEN 'OVER Prediction'
            WHEN over_prob < 0.35 THEN 'UNDER Prediction'
            ELSE 'Neutral'
        END as prediction_direction,
        
        -- Player and team info
        player_name,
        team_abbreviation,
        season_year,
        
        -- Date analysis (cast to DATE first)
        DATE_TRUNC('week', game_date::DATE) as week_start,
        DATE_TRUNC('month', game_date::DATE) as month_start,
        
        -- Line value ranges
        CASE 
            WHEN line_value < 10 THEN 'Low Line (<10)'
            WHEN line_value < 20 THEN 'Medium Line (10-19)'
            WHEN line_value < 30 THEN 'High Line (20-29)'
            ELSE 'Very High Line (30+)'
        END as line_value_range
        
    FROM {{ ref('player_props_with_actuals') }}
    GROUP BY 
        stat_type,
        player_name,
        team_abbreviation,
        season_year,
        week_start,
        month_start,
        over_prob,
        line_value
),

-- Overall summary
overall_summary AS (
    SELECT 
        'Overall Performance'::TEXT as analysis_type,
        'All Predictions'::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        ROUND(STDDEV(overall_accuracy_pct), 2)::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
),

-- By stat type
stat_type_summary AS (
    SELECT 
        'By Stat Type'::TEXT as analysis_type,
        stat_type::TEXT as category,
        SUM(stat_type_total)::INTEGER as total_predictions,
        SUM(stat_type_correct)::INTEGER as correct_predictions,
        ROUND(AVG(stat_type_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        COUNT(DISTINCT player_name)::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY stat_type
),

-- By confidence level
confidence_summary AS (
    SELECT 
        'By Confidence Level'::TEXT as analysis_type,
        confidence_level::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    WHERE confidence_level != 'Medium Confidence (36-64%)'
    GROUP BY confidence_level
),

-- By prediction direction
direction_summary AS (
    SELECT 
        'By Prediction Direction'::TEXT as analysis_type,
        prediction_direction::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    WHERE prediction_direction != 'Neutral'
    GROUP BY prediction_direction
),

-- Top performing players
top_players AS (
    SELECT 
        'Top Performing Players'::TEXT as analysis_type,
        player_name::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        COUNT(DISTINCT stat_type)::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY player_name
    HAVING COUNT(*) >= 5  -- Only players with 5+ predictions
    ORDER BY accuracy_pct DESC
    LIMIT 10
),

-- Top performing teams
top_teams AS (
    SELECT 
        'Top Performing Teams'::TEXT as analysis_type,
        team_abbreviation::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        COUNT(DISTINCT player_name)::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY team_abbreviation
    HAVING COUNT(*) >= 10  -- Only teams with 10+ predictions
    ORDER BY accuracy_pct DESC
),

-- Weekly trends
weekly_trends AS (
    SELECT 
        'Weekly Trends'::TEXT as analysis_type,
        TO_CHAR(week_start, 'YYYY-MM-DD')::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY week_start
    ORDER BY week_start DESC
    LIMIT 12  -- Last 12 weeks
),

-- Line value analysis
line_value_summary AS (
    SELECT 
        'By Line Value Range'::TEXT as analysis_type,
        line_value_range::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY line_value_range
),

-- Season comparison
season_summary AS (
    SELECT 
        'By Season'::TEXT as analysis_type,
        season_year::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY season_year
    ORDER BY season_year DESC
)

-- Combine all analyses in a subquery, then apply ORDER BY
SELECT * FROM (
    SELECT * FROM overall_summary
    UNION ALL
    SELECT * FROM stat_type_summary
    UNION ALL
    SELECT * FROM confidence_summary
    UNION ALL
    SELECT * FROM direction_summary
    UNION ALL
    SELECT * FROM top_players
    UNION ALL
    SELECT * FROM top_teams
    UNION ALL
    SELECT * FROM weekly_trends
    UNION ALL
    SELECT * FROM line_value_summary
    UNION ALL
    SELECT * FROM season_summary
) combined_analysis
ORDER BY 
    CASE analysis_type
        WHEN 'Overall Performance' THEN 1
        WHEN 'By Stat Type' THEN 2
        WHEN 'By Confidence Level' THEN 3
        WHEN 'By Prediction Direction' THEN 4
        WHEN 'Top Performing Players' THEN 5
        WHEN 'Top Performing Teams' THEN 6
        WHEN 'Weekly Trends' THEN 7
        WHEN 'By Line Value Range' THEN 8
        WHEN 'By Season' THEN 9
        ELSE 10
    END,
    accuracy_pct DESC