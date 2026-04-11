{% macro player_sims(random_range, num_sims) %}

select player_name,min_proj,"TeamAbbrev", 
((mai.fgapm * fgapmdef) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * fgapmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj ) as fgasim,
((threespm * threespmdef) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * threespmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj ) as threessim,
((ftpm * ftpmdef) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * ftpmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj )  as ftsim,
((bpm * bpmdef) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * bpmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj )  as bpmsim,
((rpm * rpmdef* homepacefactor *coalesce(reb_boost,1)) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * rpmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj )  as rebsim,
((apm * apmdef * homepacefactor *coalesce(ast_boost,1)) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * apmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj )  as apmsim,
((spm * spmdef) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * spmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj )  as spmsim,
((mai.ppm * ppmdef* homepacefactor * coalesce(usage_boost,1)) + ((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * ppmstd)) *  (((floor(random() * ({{random_range}}-(-{{random_range}})+1) -{{random_range}} ))/100 * minutesstd) +min_proj )  as ppmsim

from (select 
      -- Priority-based minutes selection: todaysmins.min first, then projmins.min fallback
      coalesce(tm."min", p."min") as min_proj,
      pl.player_name,pl.playerposition,dk."TeamAbbrev", 
      fgapm,threespm, ftpm,rpm,apm,spm,bpm,ppm,minutes, 
      fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,minutesstd,ppmstd,
      case when dk."TeamAbbrev"= gm."Home_abb" then gm."Away_abb" 
           when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
      end as opponent,
      usage_boost, reb_boost,ast_boost
      from {{ref('playerpermin')}} pl
      left join {{source('wnba','wowy')}} w on pl.player_name = w."Name"
      left join (select * from {{ref('playerstdpermin')}}) std on pl.player_name = std.player_name
      left join (select distinct player_name, team_abbreviation as "TeamAbbrev" 
                from {{ref('playerlast_10')}} where game_number =1) dk 
                on pl.player_name = dk.player_name
      -- Join todaysmins table for priority minutes (if available for today's date)
      left join (
          select "Name", "TeamAbbrev", "min", "gamedate"
          from {{source('wnba','todaysmins')}}
          where "gamedate"::date in(current_date,current_date+1)
      ) tm on tm."Name" = dk.player_name and tm."TeamAbbrev" = dk."TeamAbbrev"
      -- Fallback to projmins table
      left join (select "Name", min as min from {{source('wnba','projmins')}}) p 
                on p."Name" = dk.player_name
      left join (select * from {{source('wnba','Games')}}) gm on dk."TeamAbbrev" = gm."Home_abb"
      left join (select * from {{source('wnba','Games')}}) gmt on dk."TeamAbbrev" = gmt."Away_abb"
      where case when dk."TeamAbbrev" = gm."Home_abb" then gm."Away_abb" 
                when dk."TeamAbbrev" = gmt."Away_abb" then gmt."Home_abb"
           end is not null
) mai
left join(select * from {{ref('defpermin')}} ) def 
      on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
left join {{ ref('pace_factors') }} pa on pa."Home_abb" = mai."TeamAbbrev"
cross join (select i from generate_series(1,({{num_sims}})) i) seq

{% endmacro %}