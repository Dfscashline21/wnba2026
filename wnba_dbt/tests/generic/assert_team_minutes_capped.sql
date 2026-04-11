-- tests/generic/assert_team_minutes_capped.sql
-- Test to ensure team totals don't exceed 200 minutes

{% test assert_team_minutes_capped(model) %}

with team_validation as (
    select 
        team_abbreviation,
        sum(minutes_projection) as total_team_minutes
    from {{ model }}
    group by team_abbreviation
    having abs(sum(minutes_projection) - 200) > 1.0  -- Allow 1 minute tolerance for rounding and 40-minute cap constraints
)

select * from team_validation

{% endtest %}
