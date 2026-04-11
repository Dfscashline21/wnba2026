
    select pla.player_name
    , pla.playerposition
    , coalesce(stddev_pop(totalpts/nullif(totalmins,0)),0)  as ppmstd
    , coalesce(stddev_pop(totalfga/nullif(totalmins,0)),0)  as fgapmstd
    , coalesce(stddev_pop(totalfg3m/nullif(totalmins,0)),0) as threespmstd
    , coalesce(stddev_pop(totalftm/nullif(totalmins,0)),0) as ftpmstd
    , coalesce(stddev_pop((totaloreb+totaldreb)/nullif(totalmins,0)),0) as rpmstd
    , coalesce(stddev_pop(totalast/nullif(totalmins,0)),0)as apmstd
    , coalesce(stddev_pop(totalstl/nullif(totalmins,0)),0)  as spmstd
    ,coalesce(stddev_pop(totalblk/nullif(totalmins,0)),0) as bpmstd
    , coalesce(stddev_pop(nullif(totalmins,0)),0) as minutesstd
    , coalesce(stddev_pop(dkfant),0) as dkstd 
    from {{ref('playerlast_10')}} pla
    group by pla.player_name, pla.playerposition
    