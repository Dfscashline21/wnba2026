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
      coalesce(tm."min", pt."min_y") as min_proj,
      pl.player_name,pl.playerposition,pt."TeamAbbrev", 
      fgapm,threespm, ftpm,rpm,apm,spm,bpm,ppm,minutes, 
      fgapmstd,threespmstd, ftpmstd,rpmstd,apmstd,spmstd,bpmstd,minutesstd,ppmstd,
      pt.opponent as opponent,
      usage_boost, reb_boost,ast_boost
      from {{ref('playerpermin')}} pl
      left join {{source('wnba','wowy')}} w on pl.player_name = w."Name"
      left join (select * from {{ref('playerstdpermin')}}) std on pl.player_name = std.player_name
      left join {{ref('clean_players_today')}} pt on pl.player_name = pt."Name"
      left join (
          select 
              "Name",
              "min",
              "gamedate"
          from {{source('wnba','todaysmins')}}
          where "gamedate"::date in(current_date,current_date+1)
      ) tm on pl.player_name = tm."Name"
      where pt.opponent is not null
) mai
left join(select * from {{ref('defpermin')}} ) def 
      on mai.opponent = trim(def.opponent) and mai.playerposition = def.positionone
left join {{ ref('pace_factors') }} pa on pa.team = mai."TeamAbbrev" and pa.opponent = mai.opponent
cross join (select i from generate_series(1,({{num_sims}})) i) seq

{% endmacro %}
