

with ud_lines as (
    select distinct on (
        "name",
        "points",
        "pts_rebs",
        "pts_asts",
        "pts_rebs_asts",
        "rebounds",
        "assists",
        "fantasy_points",
        "rebs_asts",
        "three_points_made"
    )
    *
    from {{ source('wnba', 'underdog') }}
    order by
        "name",
        "points",
        "pts_rebs",
        "pts_asts",
        "pts_rebs_asts",
        "rebounds",
        "assists",
        "fantasy_points",
        "rebs_asts",
        "three_points_made"
),

base_sims as (
    {{ player_sims(100, 6800) }}
    union all
    {{ player_sims(200, 2700) }}
    union all 
    {{ player_sims(300, 500) }}
)

select player_name
,"points"
, sum(case 
    when "points"::float is null then null
    when ppmsim  > "points"::float  and "points" is not null then 1 
    else 0
end)/10000 ::float as pointsover
,"pts_rebs"
, sum(case 
    when "pts_rebs" is null then null
    when ppmsim + rebsim > "pts_rebs"::float and "pts_rebs" is not null then 1 
    else 0
end ) /10000 ::float as prover
,"pts_asts"
,sum(case
    when "pts_asts" is null then null
    when ppmsim +apmsim > "pts_asts"::float and "pts_asts" is not null then 1 
    else 0
end) /10000 ::float as paover
,"pts_rebs_asts"
,sum(case
    when "pts_rebs_asts" is null then null
    when ppmsim +apmsim +rebsim > "pts_rebs_asts"::float and "pts_rebs_asts" is not null then 1 
    else 0
end)/10000 ::float as parover
,"rebounds"
,sum(case
    when "rebounds" is null then null
    when rebsim > "rebounds"::float and "rebounds" is not null then 1 
    else 0
end) /10000 ::float as rebover
,"assists"
,sum(case
    when "assists" is null then null
    when apmsim > "assists"::float and "assists" is not null then 1 
    else 0
end)/10000 ::float as astover
,"rebs_asts"
,sum(case
    when "rebs_asts" is null then null
    when rebsim +apmsim > "rebs_asts"::float and "rebs_asts" is not null then 1 
    else 0
end)/10000 ::float as raover
,"fantasy_points"
,sum(case
    when "fantasy_points" is null then null
    when ppmsim  + (1.25 * rebsim ) + (1.5*apmsim ) + (2 * (bpmsim +  spmsim) ) > "fantasy_points"::float and "fantasy_points" is not null then 1
    else 0
end)/10000 ::float as fantover
,"three_points_made"::float
,sum(case
    when "three_points_made" is null then null
    when threessim > "three_points_made"::float and "three_points_made" is not null then 1 
    else 0
end)/10000 ::float as threesover
from base_sims sims
left join ud_lines ud on ud."name" = sims.player_name
            where ud."name" is not null 
            group by player_name,"points"
            ,"pts_rebs","pts_asts"
            ,"pts_rebs_asts"
            ,"rebounds","assists"
            ,"fantasy_points"
            ,"rebs_asts"
            ,"three_points_made"
            order by player_name