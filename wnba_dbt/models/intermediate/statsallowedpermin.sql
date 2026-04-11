
select ts.opponent
, ts.playerposition as positionone
, sum(totalfga) / sum(totalmins) as fgapm 
, sum(totalpts) / sum(totalmins) as ppm 
, sum(totalfg3a) /sum(totalmins)  as threespm
, sum(totalfta) /sum(totalmins)  as ftpm
, (sum(totaloreb) + sum(totaldreb)) / sum(totalmins) as rpm
, sum(totalast)/ sum(totalmins)  as apm
, sum(totalstl)/sum(totalmins)  as spm
,sum(totalblk) /sum(totalmins)  as bpm
from {{ref('teamstats')}} ts
where ts.game_number <=10
group by ts.opponent, ts.playerposition
order by ts.opponent