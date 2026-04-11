{{ config(materialized='table') }}

-- Analysis: Trend-Based Prediction Accuracy Evaluation for STANDARD Odds - OVER Only
-- This analysis evaluates the accuracy of trend-based prop predictions for standard odds OVER recommendations

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
            WHEN confidence_score >= 90 THEN 'Very High Confidence (90%+)'
            WHEN confidence_score >= 80 THEN 'High Confidence (80-89%)'
            WHEN confidence_score >= 70 THEN 'Moderate Confidence (70-79%)'
            WHEN confidence_score >= 60 THEN 'Low Confidence (60-69%)'
            ELSE 'Very Low Confidence (<60%)'
        END as confidence_level,
        
        -- Player and team info
        player_name,
        team_abbreviation,
        season_year,
        
        -- Date analysis
        DATE_TRUNC('week', game_date::DATE) as week_start,
        DATE_TRUNC('month', game_date::DATE) as month_start,
        
        -- Line value ranges
        CASE 
            WHEN line_value < 10 THEN 'Low Line (<10)'
            WHEN line_value < 20 THEN 'Medium Line (10-19)'
            WHEN line_value < 30 THEN 'High Line (20-29)'
            ELSE 'Very High Line (30+)'
        END as line_value_range,
        
        -- Trend strength analysis
        CASE 
            WHEN overs_5 = 5 THEN 'Perfect Trend (5/5)'
            WHEN overs_5 = 4 THEN 'Strong Trend (4/5)'
            ELSE 'Mixed Trend (<4/5)'
        END as trend_strength_5,
        
        CASE 
            WHEN overs_10 = 10 THEN 'Perfect Trend (10/10)'
            WHEN overs_10 >= 9 THEN 'Strong Trend (9-10/10)'
            ELSE 'Mixed Trend (<9/10)'
        END as trend_strength_10
        
    FROM {{ ref('trend_recommendations_with_actuals') }}
    WHERE odds_type LIKE 'standard%'
    AND recommendation_type = 'HIGH_CONFIDENCE_OVER'
    GROUP BY 
        stat_type, confidence_level, player_name, 
        team_abbreviation, season_year, week_start, month_start, 
        line_value_range, trend_strength_5, trend_strength_10
),

-- Overall summary statistics
overall_summary AS (
    SELECT 
        'OVERALL' as category,
        'All Standard OVER Predictions' as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        NULL as detail_value
    FROM accuracy_metrics
),

-- Stat type analysis
stat_type_summary AS (
    SELECT 
        'STAT_TYPE' as category,
        stat_type as subcategory,
        SUM(stat_type_total) as total_predictions,
        SUM(stat_type_correct) as correct_predictions,
        ROUND(AVG(stat_type_accuracy_pct), 2) as accuracy_pct,
        stat_type as detail_value
    FROM accuracy_metrics
    GROUP BY stat_type
),

-- Confidence level analysis
confidence_summary AS (
    SELECT 
        'CONFIDENCE' as category,
        confidence_level as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        confidence_level as detail_value
    FROM accuracy_metrics
    GROUP BY confidence_level
),

-- Top performing players
top_players AS (
    SELECT 
        'TOP_PLAYERS' as category,
        player_name as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        player_name as detail_value
    FROM accuracy_metrics
    GROUP BY player_name
    HAVING COUNT(*) >= 3  -- Minimum 3 predictions for standard OVER
    ORDER BY accuracy_pct DESC
    LIMIT 10
),

-- Top performing teams
top_teams AS (
    SELECT 
        'TOP_TEAMS' as category,
        team_abbreviation as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        team_abbreviation as detail_value
    FROM accuracy_metrics
    WHERE team_abbreviation IS NOT NULL
    GROUP BY team_abbreviation
    HAVING COUNT(*) >= 3  -- Minimum 3 predictions for standard OVER
    ORDER BY accuracy_pct DESC
    LIMIT 10
),

-- Weekly trends
weekly_trends AS (
    SELECT 
        'WEEKLY_TRENDS' as category,
        week_start::TEXT as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        week_start::TEXT as detail_value
    FROM accuracy_metrics
    GROUP BY week_start
    ORDER BY week_start DESC
    LIMIT 10
),

-- Line value analysis
line_value_summary AS (
    SELECT 
        'LINE_VALUE' as category,
        line_value_range as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        line_value_range as detail_value
    FROM accuracy_metrics
    GROUP BY line_value_range
),

-- Season analysis
season_summary AS (
    SELECT 
        'SEASON' as category,
        season_year::TEXT as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        season_year::TEXT as detail_value
    FROM accuracy_metrics
    GROUP BY season_year
),

-- Trend strength analysis
trend_strength_5_summary AS (
    SELECT 
        'TREND_STRENGTH_5' as category,
        trend_strength_5 as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        trend_strength_5 as detail_value
    FROM accuracy_metrics
    GROUP BY trend_strength_5
),

trend_strength_10_summary AS (
    SELECT 
        'TREND_STRENGTH_10' as category,
        trend_strength_10 as subcategory,
        COUNT(*) as total_predictions,
        SUM(correct_predictions) as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2) as accuracy_pct,
        trend_strength_10 as detail_value
    FROM accuracy_metrics
    GROUP BY trend_strength_10
),

-- Combine all analyses
all_analyses AS (
    SELECT * FROM overall_summary
    UNION ALL
    SELECT * FROM stat_type_summary
    UNION ALL
    SELECT * FROM confidence_summary
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
    UNION ALL
    SELECT * FROM trend_strength_5_summary
    UNION ALL
    SELECT * FROM trend_strength_10_summary
)

-- Apply ordering to the combined results
SELECT * FROM all_analyses
ORDER BY 
    CASE 
        WHEN category = 'OVERALL' THEN 1
        WHEN category = 'CONFIDENCE' THEN 2
        WHEN category = 'STAT_TYPE' THEN 3
        WHEN category = 'TREND_STRENGTH_5' THEN 4
        WHEN category = 'TREND_STRENGTH_10' THEN 5
        WHEN category = 'TOP_PLAYERS' THEN 6
        WHEN category = 'TOP_TEAMS' THEN 7
        WHEN category = 'LINE_VALUE' THEN 8
        WHEN category = 'WEEKLY_TRENDS' THEN 9
        WHEN category = 'SEASON' THEN 10
        ELSE 11
    END,
    accuracy_pct DESC