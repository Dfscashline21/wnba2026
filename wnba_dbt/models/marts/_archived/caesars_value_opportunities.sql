{{config(
    materialized='table',
    alias='caesars_value_opportunities'
)}}

select 
    player,
    team,
    stat,
    threshold,
    threshold_label,
    probability,
    probability_percent,
    american_odds,
    under_probability,
    under_probability_percent,
    under_american_odds,
    case
        when probability > 0.65 then 'High Value Over'
        when under_probability > 0.65 then 'High Value Under'
        else 'Both Sides Value'
    end as value_type,
    greatest(probability, under_probability) as max_probability
from {{ ref('caesars_range_outcomes') }}
where (probability > 0.65 or under_probability > 0.65)
order by greatest(probability, under_probability) desc


