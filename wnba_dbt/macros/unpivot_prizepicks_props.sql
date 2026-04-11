{% macro unpivot_prizepicks_props(table_ref, props) %}
    {% for prop in props %}
        {% if not loop.first %}UNION ALL{% endif %}
        SELECT 
            load_date,
            player_name,
            '{{ prop.stat_type }}' as stat_type,
            {{ prop.line_col }} as line_value,
            {{ prop.over_col }} as over_prob,
            CASE 
                WHEN "attributes.odds_type" LIKE '%_%' THEN 
                    SPLIT_PART("attributes.odds_type", '_', 1)
                ELSE "attributes.odds_type"
            END as odds_type
        FROM {{ table_ref }}
        WHERE {{ prop.line_col }} IS NOT NULL
        AND {{ prop.over_col }} IS NOT NULL
        AND (
            {{ prop.over_col }} > 0.65 
            OR ({{ prop.over_col }} < 0.35 AND 
                CASE 
                    WHEN "attributes.odds_type" LIKE '%_%' THEN 
                        SPLIT_PART("attributes.odds_type", '_', 1)
                    ELSE "attributes.odds_type"
                END NOT IN ('goblin', 'demon'))
        )
    {% endfor %}
{% endmacro %}