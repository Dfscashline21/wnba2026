{% test assert_team_minutes(model) %}

select 
    "TeamAbbrev"
    ,sum(minutes_projection) as total_minutes
from {{ model }}
group by "TeamAbbrev"
having sum(minutes_projection) < 195 or sum(minutes_projection) > 205

{% endtest %}