-- Analysis to check team totals from minutes projection
select 
    team_abbreviation, 
    sum(minutes_projection) as total_minutes,
    count(*) as player_count
from {{ ref('minutes_projection') }}
group by team_abbreviation 
order by total_minutes desc
