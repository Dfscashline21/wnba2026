-- models/clean/clean_draftkings.sql

with base as (

    select * from {{ source('wnba', 'draftkings') }}
),

cleaned as (

    select
    case 
        when "TeamAbbrev" = 'LAV' then 'LVA'
        when "TeamAbbrev" = 'PHO' then 'PHX'
        else "TeamAbbrev"
    end as "TeamAbbrev",
    "Position","Name" as name ,"Name + ID","ID","Salary","Game Info","AvgPointsPerGame",min_x,min_y

    from base

)

select * from cleaned
