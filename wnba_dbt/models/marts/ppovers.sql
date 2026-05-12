
with pp_lines as (
    select distinct on (
        "attributes.name",
        "attributes.odds_type",
        "Points",
        "Pts+Rebs",
        "Pts+Asts",
        "Pts+Rebs+Asts",
        "Rebounds",
        "Assists",
        "Fantasy Score",
        "Rebs+Asts",
        "3-PT Made"
    )
    *
    from {{ source('wnba', 'prizepicks') }}
    order by
        "attributes.name",
        "attributes.odds_type",
        "Points",
        "Pts+Rebs",
        "Pts+Asts",
        "Pts+Rebs+Asts",
        "Rebounds",
        "Assists",
        "Fantasy Score",
        "Rebs+Asts",
        "3-PT Made"
),

base_sims as (
    {{ player_sims(100, 6800) }}
    union all
    {{ player_sims(200, 2700) }}
    union all 
    {{ player_sims(300, 500) }}
)

select player_name,"attributes.odds_type"
,"Points"
, sum(case 
    when "Points" is null then null
    when ppmsim  > "Points" and "Points" is not null then 1 
    else 0
end)/10000 ::float as pointsover
,"Pts+Rebs"
, sum(case 
    when "Pts+Rebs" is null then null
    when ppmsim + rebsim > "Pts+Rebs" and "Pts+Rebs" is not null then 1 
    else 0
end ) /10000 ::float as prover
,"Pts+Asts"
,sum(case
    when "Pts+Asts" is null then null
    when ppmsim +apmsim > "Pts+Asts" and "Pts+Asts" is not null then 1 
    else 0
end)/10000 ::float  as paover
,"Pts+Rebs+Asts"
,sum(case
    when "Pts+Rebs+Asts" is null then null
    when ppmsim +apmsim +rebsim > "Pts+Rebs+Asts" and "Pts+Rebs+Asts" is not null then 1 
    else 0
end)/10000 ::float as parover
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
,"Rebs+Asts"
,sum(case
    when "Rebs+Asts" is null then null
    when rebsim +apmsim > "Rebs+Asts" and "Rebs+Asts" is not null then 1 
    else 0
end)/10000 ::float as raover
,"Fantasy Score"
,sum(case
    when "Fantasy Score" is null then null
    when ppmsim  + (1.25 * rebsim ) + (1.5*apmsim ) + (3 * (bpmsim +  spmsim) ) > "Fantasy Score" and "Fantasy Score" is not null then 1 
    else 0
end)/10000 ::float as fantover
,"3-PT Made"
,sum(case
    when "3-PT Made" is null then null
    when threessim > "3-PT Made" and "3-PT Made" is not null then 1 
    else 0
end)/10000 ::float as threesover
from base_sims sims 
left join pp_lines pp on pp."attributes.name" = sims.player_name
where pp."attributes.name" is not null 
group by player_name,"attributes.odds_type","Points","Pts+Rebs", "Pts+Asts", "Pts+Rebs+Asts","Rebs+Asts","Rebounds","Assists","Fantasy Score"
,"3-PT Made"
order by player_name