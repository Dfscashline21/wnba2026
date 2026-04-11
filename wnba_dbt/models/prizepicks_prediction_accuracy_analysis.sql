{{ config(materialized='table') }}

-- Analysis: PrizePicks Prediction Accuracy Evaluation
-- This analysis evaluates the accuracy of PrizePicks prop predictions across various dimensions

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
        prediction_direction,
        
        -- Player and team info
        player_name,
        team_abbreviation,
        season_year,
        odds_type,
        
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
        
    FROM {{ ref('prizepicks_props_with_actuals') }}
    GROUP BY 
        stat_type,
        player_name,
        team_abbreviation,
        season_year,
        odds_type,
        week_start,
        month_start,
        over_prob,
        line_value,
        prediction_direction
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

-- By stat type and odds type
stat_type_odds_summary AS (
    SELECT 
        'By Stat Type & Odds Type'::TEXT as analysis_type,
        (stat_type || ' - ' || odds_type)::TEXT as category,
        SUM(stat_type_total)::INTEGER as total_predictions,
        SUM(stat_type_correct)::INTEGER as correct_predictions,
        ROUND(AVG(stat_type_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        COUNT(DISTINCT player_name)::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY stat_type, odds_type
),

-- By odds type only
odds_type_summary AS (
    SELECT 
        'By Odds Type'::TEXT as analysis_type,
        odds_type::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        COUNT(DISTINCT player_name)::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY odds_type
),

-- By confidence level and odds type
confidence_odds_summary AS (
    SELECT 
        'By Confidence Level & Odds Type'::TEXT as analysis_type,
        (confidence_level || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    WHERE confidence_level != 'Medium Confidence (36-64%)'
    GROUP BY confidence_level, odds_type
),

-- By prediction direction and odds type
direction_odds_summary AS (
    SELECT 
        'By Prediction Direction & Odds Type'::TEXT as analysis_type,
        (prediction_direction || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    WHERE prediction_direction != 'NEUTRAL'
    GROUP BY prediction_direction, odds_type
),

-- Top performing players by odds type
top_players_odds AS (
    SELECT 
        'Top Performing Players by Odds Type'::TEXT as analysis_type,
        (player_name || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        COUNT(DISTINCT stat_type)::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY player_name, odds_type
    HAVING COUNT(*) >= 3  -- Only players with 3+ predictions per odds type
    ORDER BY accuracy_pct DESC
    LIMIT 15
),

-- Top performing teams by odds type
top_teams_odds AS (
    SELECT 
        'Top Performing Teams by Odds Type'::TEXT as analysis_type,
        (team_abbreviation || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        COUNT(DISTINCT player_name)::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY team_abbreviation, odds_type
    HAVING COUNT(*) >= 5  -- Only teams with 5+ predictions per odds type
    ORDER BY accuracy_pct DESC
),

-- Weekly trends by odds type
weekly_trends_odds AS (
    SELECT 
        'Weekly Trends by Odds Type'::TEXT as analysis_type,
        (TO_CHAR(week_start, 'YYYY-MM-DD') || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY week_start, odds_type
    ORDER BY week_start DESC, odds_type
    LIMIT 20  -- Last 20 week-odds combinations
),

-- Line value analysis by odds type
line_value_odds_summary AS (
    SELECT 
        'By Line Value Range & Odds Type'::TEXT as analysis_type,
        (line_value_range || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY line_value_range, odds_type
),

-- Season comparison by odds type
season_odds_summary AS (
    SELECT 
        'By Season & Odds Type'::TEXT as analysis_type,
        (season_year || ' - ' || odds_type)::TEXT as category,
        COUNT(*)::INTEGER as total_predictions,
        SUM(correct_predictions)::INTEGER as correct_predictions,
        ROUND(AVG(overall_accuracy_pct), 2)::NUMERIC as accuracy_pct,
        NULL::NUMERIC as accuracy_std_dev,
        NULL::INTEGER as unique_players,
        NULL::INTEGER as stat_types_covered
    FROM accuracy_metrics
    GROUP BY season_year, odds_type
    ORDER BY season_year DESC, odds_type
)

-- Combine all analyses in a subquery, then apply ORDER BY
SELECT * FROM (
    SELECT * FROM overall_summary
    UNION ALL
    SELECT * FROM stat_type_odds_summary
    UNION ALL
    SELECT * FROM odds_type_summary
    UNION ALL
    SELECT * FROM confidence_odds_summary
    UNION ALL
    SELECT * FROM direction_odds_summary
    UNION ALL
    SELECT * FROM top_players_odds
    UNION ALL
    SELECT * FROM top_teams_odds
    UNION ALL
    SELECT * FROM weekly_trends_odds
    UNION ALL
    SELECT * FROM line_value_odds_summary
    UNION ALL
    SELECT * FROM season_odds_summary
) combined_analysis
ORDER BY 
    CASE analysis_type
        WHEN 'Overall Performance' THEN 1
        WHEN 'By Stat Type & Odds Type' THEN 2
        WHEN 'By Odds Type' THEN 3
        WHEN 'By Confidence Level & Odds Type' THEN 4
        WHEN 'By Prediction Direction & Odds Type' THEN 5
        WHEN 'Top Performing Players by Odds Type' THEN 6
        WHEN 'Top Performing Teams by Odds Type' THEN 7
        WHEN 'Weekly Trends by Odds Type' THEN 8
        WHEN 'By Line Value Range & Odds Type' THEN 9
        WHEN 'By Season & Odds Type' THEN 10
        ELSE 11
    END,
    accuracy_pct DESC