{{config(
    materialized='table',
    alias='mgmovers'
)}}



with base_sims as (
    {{ player_sims(100, 6800) }}
    union all
    {{ player_sims(200, 2700) }}
    union all 
    {{ player_sims(300, 500) }}
)

select player,"date" as prop_date,"TeamAbbrev" as team
,"Points"
, sum(case 
    when "Points" is null then null
    when ppmsim  > "Points" and "Points" is not null then 1 
    else 0
end)/10000 ::float as pointsover
,"Rebounds"
,sum(case
    when "Rebounds" is null then null
    when rebsim > "Rebounds" and "Rebounds" is not null then 1 
    else 0
end)/10000 ::float as rebover
,"Assists"
,sum(case
    when "Assists" is null then null
    when apmsim > "Assists" and "Assists" is not null then 1 
    else 0
end)/10000 ::float as astover
,"3-Pointers"
,sum(case
    when "3-Pointers" is null then null
    when threessim > "3-Pointers" and "3-Pointers" is not null then 1 
    else 0
end)/10000 ::float as threesover
from  base_sims sims
left join {{source('wnba','betmgm')}} pp on pp.player = sims.player_name
where pp.player is not null 
group by player,prop_date,team,"Points","Rebounds","Assists"
,"3-Pointers"
order by player