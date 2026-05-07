{{config(
    materialized='table',
    alias='caesars_extreme_overs'
)}}

with extreme_overs as (
    select 
        player,
        team,
        'Points' as stat,
        "Points" as line,
        pointsover as over_value
    from {{ ref('caesarsovers') }}
    where pointsover > 0.65 or pointsover < 0.35
    
    union all
    
    select 
        player,
        team,
        'Rebounds' as stat,
        "Rebounds" as line,
        rebover as over_value
    from {{ ref('caesarsovers') }}
    where rebover > 0.65 or rebover < 0.35
    
    union all
    
    select 
        player,
        team,
        'Assists' as stat,
        "Assists" as line,
        astover as over_value
    from {{ ref('caesarsovers') }}
    where astover > 0.65 or astover < 0.35
    
    union all
    
    select 
        player,
        team,
        '3-Pointers' as stat,
        "3-Pointers" as line,
        threesover as over_value
    from {{ ref('caesarsovers') }}
    where threesover > 0.65 or threesover < 0.35
)

select 
    player,
    team,
    stat,
    line,
    over_value as over,
    case 
        when over_value > 0.65 then 'High Confidence Over'
        when over_value < 0.35 then 'High Confidence Under'
    end as confidence_level
from extreme_overs
where line is not null
order by 
    case 
        when over_value > 0.65 then over_value 
        when over_value < 0.35 then 1 - over_value
    end desc,
    player,
    stat
