{% macro unpivot_over_props_with_lines(table_ref, props) %}

{% set query_parts = [] %}

{% for prop in props %}
    {% set stat_type = prop.stat_type %}
    {% set over_col = prop.over_col %}
    {% set line_col = prop.line_col %}
    
    {% set query_part %}
        SELECT 
            load_date,
            player_name,
            '{{ stat_type }}' as stat_type,
            {{ over_col }} as over_prob,
            CAST({{ line_col }} AS NUMERIC) as line_value
        FROM {{ table_ref }}
        WHERE {{ over_col }} > 0.65 OR {{ over_col }} < 0.35
    {% endset %}
    
    {% do query_parts.append(query_part) %}
{% endfor %}

{{ query_parts | join('\nUNION ALL\n') }}

{% endmacro %}