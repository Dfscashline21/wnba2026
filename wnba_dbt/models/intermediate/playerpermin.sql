   --player per min 
    select pla.player_name
    , pla.playerposition
    ,sum(totalpts) / sum(totalmins) as ppm 
    , sum(totalfga) / sum(totalmins) as fgapm 
    , sum(totalfg3m) /sum(totalmins)  as threespm
    , sum(totalftm) /sum(totalmins)  as ftpm
    , (sum(totaloreb) + sum(totaldreb)) / sum(totalmins) as rpm
    , sum(totalast)/ sum(totalmins)  as apm
    , sum(totalstl)/sum(totalmins)  as spm
    ,sum(totalblk) /sum(totalmins)  as bpm
    , avg(totalmins) as minutes
    from {{ref('playerlast_10')}} pla
    where pla.game_number <=10 and pla.totalmins >0
    group by pla.player_name, pla.playerposition
    order by pla.player_name