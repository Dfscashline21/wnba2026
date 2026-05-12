with base as (

    select * from {{ source('wnba', 'draftkings') }}

),

named as (

    select
        base."TeamAbbrev",
        base."Position",
        coalesce(map.stats_player_name, base."Name") as name,
        base."Name + ID",
        base."ID",
        base."Salary",
        base."Game Info",
        base."AvgPointsPerGame",
        base.min_x,
        base.min_y,
        base.opponent
    from base
    left join {{ ref('draftkings_player_name_map') }} map on base."Name" = map.draftkings_name

)

select * from named
