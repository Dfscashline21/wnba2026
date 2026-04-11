select pla.player_name
        , stddev_pop(dkfant) as dkstd 
        from {{ref('playerlast_10')}} pla
        group by pla.player_name