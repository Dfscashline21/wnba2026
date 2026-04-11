
select ts.opponent
, lea.positionone
, (sum(totalfga) / sum(totalmins)) /avg(lea.league_fga) as fgapmdef 
, sum(totalfg3a) /sum(totalmins) /avg(lea.league_three) as threespmdef
, sum(totalfta) /sum(totalmins) / avg(lea.leaguefree) as ftpmdef
, (sum(totaloreb) + sum(totaldreb)) / sum(totalmins) / avg(lea.leaguereb) as rpmdef
, sum(totalast)/ sum(totalmins) / avg(lea.leagueast) as apmdef
, sum(totalstl)/sum(totalmins) / avg(lea.leaguestl) as spmdef
,sum(totalblk) /sum(totalmins)/ avg(lea.leagueblk)  as bpmdef
, sum(totalpts) /sum(totalmins) / avg(lea.leagueppm) as ppmdef
from {{ref('teamstats')}} ts
left join(select sa.positionone,avg(ppm) as leagueppm, avg(fgapm) as league_fga , avg(threespm) as league_three ,avg(ftpm) as leaguefree , avg(rpm) as leaguereb , avg(apm) as leagueast , avg(spm) as leaguestl , avg(bpm) as leagueblk 
from {{ref('statsallowedpermin')}}  sa
group by sa.positionone) lea on ts.playerposition = lea.positionone
where ts.game_number <=10
group by ts.opponent, lea.positionone
order by ts.opponent  