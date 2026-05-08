-- models/clean/clean_draftkings.sql

with base as (

    select * from {{ source('wnba', 'draftkings') }}

),

cleaned as (

    select
    case 
        when base."TeamAbbrev" = 'LAV' then 'LVA'
        when base."TeamAbbrev" = 'PHO' then 'PHX'
        else base."TeamAbbrev"
    end as "TeamAbbrev",
    base."Position",
    coalesce(map.stats_player_name, base."Name") as name,
    base."Name + ID",
    base."ID",
    base."Salary",
    base."Game Info",
    base."AvgPointsPerGame",
    base.min_x,
    base.min_y

    from base
    left join {{ ref('draftkings_player_name_map') }} map on base."Name" = map.draftkings_name

)

select * from cleaned
