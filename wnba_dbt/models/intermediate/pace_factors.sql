{{config(
    materialized='table',
    alias='pace_factors'
)}}

select g."Home_abb"
,((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + 
  (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + 
  (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) /pac."PACE"  as homepacefactor
from {{source('wnba','Games')}} g 
left join (
    select tgl.team_abbreviation , p."PACE" 
    from {{source('wnba','pace')}} p
    left outer join(select distinct t.team_name ,t.team_abbreviation  
                   from {{ref('totalgamelogs')}} t)tgl  
                   on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
    select tgl.team_abbreviation , p."PACE" 
    from {{source('wnba','pace')}} p
    left outer join(select distinct t.team_name ,t.team_abbreviation  
                   from {{ref('totalgamelogs')}} t)tgl  
                   on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb"
union all
select g."Away_abb"
, ((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + 
   (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + 
   (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ))/ pace."PACE" as awaypacefactor
from {{source('wnba','Games')}} g 
left join (
    select tgl.team_abbreviation , p."PACE" 
    from {{source('wnba','pace')}} p
    left outer join(select distinct t.team_name ,t.team_abbreviation  
                   from {{ref('totalgamelogs')}} t)tgl  
                   on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
    select tgl.team_abbreviation , p."PACE" 
    from {{source('wnba','pace')}} p
    left outer join(select distinct t.team_name ,t.team_abbreviation  
                   from {{ref('totalgamelogs')}} t)tgl  
                   on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb"