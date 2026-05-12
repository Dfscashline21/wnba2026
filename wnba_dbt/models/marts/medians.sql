select player_name,"TeamAbbrev",percentile_cont(0.5) within group (order by min_proj)  as minutes_projection
, percentile_cont(0.5) within group (order by ppmsim)  as medianppm
, percentile_cont(0.5) within group (order by rebsim)  as medianreb
, percentile_cont(0.5) within group (order by apmsim)  as medianast
, percentile_cont(0.5) within group (order by bpmsim)  as medianblk
, percentile_cont(0.5) within group (order by spmsim)  as medianstl
, percentile_cont(0.5) within group (order by threessim)  as medianthrees
, percentile_cont(0.5) within group (order by ftsim)  as medianft
, (avg(ppmsim) + (.5*avg(threessim) ) + (1.25 * avg(rebsim) ) + (1.5*avg(apmsim) ) + (2 * (avg(bpmsim) + avg(spmsim) ))) as dkpts
from (select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * fgapmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as fgasim
, ((threespm * threespmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * threespmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj ) as threessim
, ((ftpm * ftpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ftpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ftsim
, ((bpm * bpmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * bpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as bpmsim
, ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * rpmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as rebsim
, ((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * apmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as apmsim
, ((spm * spmdef) + ((floor(random() * (100-(-100)+1) -100 ))/100 * spmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as spmsim
, ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (100-(-100)+1) -100 ))/100 * ppmstd)) *  (((floor(random() * (100-(-100)+1) -100 ))/100 * minutesstd) +min_proj )  as ppmsim
from (select 
    -- Priority-based minutes selection: todaysmins.min first, then DraftKings fallback
    coalesce(
        tm."min",              -- First priority: todaysmins.min column
        pt."min_y"             -- Fallback: DraftKings minutes
    ) as min_proj,
    pl.player_name,pl.playerposition,pt."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,minutesstd,ppmstd
, pt.opponent as opponent
    ,usage_boost, reb_boost,ast_boost
from {{ref('playerpermin')}} pl
left join {{source('wnba','wowy')}} w on pl.player_name = w."Name"
left join (select * from {{ref('playerstdpermin')}}) std on pl.player_name = std.player_name
left join {{ref('clean_players_today')}} pt on pl.player_name = pt."Name"
-- Join todaysmins table for priority minutes
left join (
    select 
        "Name",           -- player_name column
        "min",            -- minutes column  
        "gamedate"        -- gamedate column
    from {{source('wnba','todaysmins')}} 
    where "gamedate"::date in(current_date,current_date+1)
) tm on pl.player_name = tm."Name"
where pt.opponent is not null
) mai
left join(select * from {{ref('defpermin')}} ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
left join (
select g."Home_abb" as team,
    g."Away_abb" as opponent,
    coalesce(((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) /pac."PACE",1)  as homepacefactor
from {{source('wnba','Games')}} g 
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb"
union all
select g."Away_abb" as team,
    g."Home_abb" as opponent,
    coalesce(((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}}  p )) + (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ))/ pace."PACE",1) as homepacefactor
from {{source('wnba','Games')}} g 
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join (select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
) pace on pace.team_abbreviation = g."Away_abb") pa on pa.team = mai."TeamAbbrev" and pa.opponent = mai.opponent
cross join (select i 
from generate_series(1,(6800)) i) seq
union all 
select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * fgapmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as fgasim
, ((threespm * threespmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * threespmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj ) as threessim
, ((ftpm * ftpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ftpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ftsim
, ((bpm * bpmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * bpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as bpmsim
, ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * rpmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as rebsim
, ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * apmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as apmsim
, ((spm * spmdef) + ((floor(random() * (200-(-200)+1) -200 ))/100 * spmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as spmsim
, ((mai.ppm * ppmdef* homepacefactor *coalesce(usage_boost,1)) + ((floor(random() * (200-(-200)+1) -200 ))/100 * ppmstd)) *  (((floor(random() * (200-(-200)+1) -200 ))/100 * minutesstd) +min_proj )  as ppmsim
from (select 
    -- Priority-based minutes selection: todaysmins.min first, then DraftKings fallback
    coalesce(
        tm."min",              -- First priority: todaysmins.min column
        pt."min_y"             -- Fallback: DraftKings minutes
    ) as min_proj,
    pl.player_name,pl.playerposition,pt."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,minutesstd,ppmstd
, pt.opponent as opponent
    ,usage_boost, reb_boost,ast_boost
from {{ref('playerpermin')}} pl
left join {{source('wnba','wowy')}} w on pl.player_name = w."Name"
left join (select * from {{ref('playerstdpermin')}}) std on pl.player_name = std.player_name
left join {{ref('clean_players_today')}} pt on pl.player_name = pt."Name"
-- Join todaysmins table for priority minutes
left join (
    select 
        "Name",           -- player_name column
        "min",            -- minutes column  
        "gamedate"        -- gamedate column
    from {{source('wnba','todaysmins')}} 
    where "gamedate"::date in(current_date,current_date+1)
) tm on pl.player_name = tm."Name"
where pt.opponent is not null
) mai
left join(select * from {{ref('defpermin')}} ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
left join (
select g."Home_abb" as team,
    g."Away_abb" as opponent,
    coalesce(((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) /pac."PACE",1)  as homepacefactor
from {{source('wnba','Games')}} g 
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb"
union all
select g."Away_abb" as team,
    g."Home_abb" as opponent,
    coalesce(((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ))/ pace."PACE",1) as homepacefactor
from {{source('wnba','Games')}} g 
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb") pa on pa.team = mai."TeamAbbrev" and pa.opponent = mai.opponent
cross join (select i 
from generate_series(1,(2700)) i) seq
union all
select player_name,min_proj,"TeamAbbrev", ((mai.fgapm * fgapmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * fgapmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as fgasim
, ((threespm * threespmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * threespmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj ) as threessim
, ((ftpm * ftpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ftpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ftsim
, ((bpm * bpmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * bpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as bpmsim
, ((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1) ) + ((floor(random() * (300-(-300)+1) -300 ))/100 * rpmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as rebsim
, ((apm * apmdef* homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * apmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as apmsim
, ((spm * spmdef) + ((floor(random() * (300-(-300)+1) -300 ))/100 * spmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as spmsim
, ((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * (300-(-300)+1) -300 ))/100 * ppmstd)) *  (((floor(random() * (300-(-300)+1) -300 ))/100 * minutesstd) +min_proj )  as ppmsim
from (select 
    -- Priority-based minutes selection: todaysmins.min first, then DraftKings fallback
    coalesce(
        tm."min",              -- First priority: todaysmins.min column
        pt."min_y"             -- Fallback: DraftKings minutes
    ) as min_proj,
    pl.player_name,pl.playerposition,pt."TeamAbbrev", fgapm,threespm, ftpm,rpm,apm,spm,bpm,ppm,minutes, fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,minutesstd,ppmstd
, pt.opponent as opponent
    ,usage_boost, reb_boost,ast_boost
from {{ref('playerpermin')}} pl
left join {{source('wnba','wowy')}} w on pl.player_name = w."Name"
left join (select * from {{ref('playerstdpermin')}}) std on pl.player_name = std.player_name
left join {{ref('clean_players_today')}} pt on pl.player_name = pt."Name"
-- Join todaysmins table for priority minutes
left join (
    select 
        "Name",           -- player_name column
        "min",            -- minutes column  
        "gamedate"        -- gamedate column
    from {{source('wnba','todaysmins')}} 
    where "gamedate"::date in(current_date,current_date+1)
) tm on pl.player_name = tm."Name"
where pt.opponent is not null
) mai
left join(select * from {{ref('defpermin')}} ) def on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
left join (
select g."Home_abb" as team,
    g."Away_abb" as opponent,
    coalesce(((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) /pac."PACE",1)  as homepacefactor
from {{source('wnba','Games')}} g 
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb"
union all
select g."Away_abb" as team,
    g."Home_abb" as opponent,
    coalesce(((pac."PACE" - (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ) ) + (pace."PACE" -(select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p )) + (select avg(p."PACE") as league_pace from {{source('wnba','pace')}} p ))/ pace."PACE",1) as homepacefactor
from {{source('wnba','Games')}} g 
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pac on pac.team_abbreviation = g."Home_abb"
left join (
select tgl.team_abbreviation , p."PACE" from {{source('wnba','pace')}} p
left outer join(select distinct t.team_name ,t.team_abbreviation  from {{ref('clean_logs')}} t)tgl  on p."TEAM_NAME" = tgl.team_name 
)pace on pace.team_abbreviation = g."Away_abb") pa on pa.team = mai."TeamAbbrev" and pa.opponent = mai.opponent
cross join (select i 
from generate_series(1,(500	)) i) seq
order by player_name) tot
group by player_name,"TeamAbbrev"