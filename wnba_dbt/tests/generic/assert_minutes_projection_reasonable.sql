-- tests/generic/assert_minutes_projection_reasonable.sql
-- Test to ensure minutes projections are within reasonable bounds

{% test assert_minutes_projection_reasonable(model, column_name) %}

with validation_errors as (
    select 
        player_name,
        team_abbreviation,
        {{ column_name }} as minutes_projection,
        confidence_score
    from {{ model }}
    where {{ column_name }} < 0 
                     or {{ column_name }} > 40  -- Maximum 40 minutes per player
       or confidence_score < 0 
       or confidence_score > 1
)

select * from validation_errors

{% endtest %}
