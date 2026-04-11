-- This SQL script creates a table named playerlast_10 that aggregates player performance data from the totalgamelogs table.
    select gl.player_name, playerposition , team_abbreviation, game_date, sum(fga) as totalfga, sum(fg3m) as totalfg3m , sum(ftm) as totalftm , sum(oreb) as totaloreb , sum(dreb) as totaldreb, sum(ast) as totalast , sum(stl) as totalstl , sum(blk) as totalblk , sum(pts) as totalpts , sum("min") as  totalmins
    ,(sum(pts) + (1.5* sum(ast)) + (1.2 * (sum(oreb) + sum(dreb))) +(2 * (sum(stl) + sum(blk))) + (.5*sum(fg3m))) as dkfant
    ,row_number() over (partition by gl.player_name , playerposition order by game_date desc )  game_number
    from {{ ref('clean_logs') }} gl
    left join (select pl."Player",case when split_part(pl."POSITION",'-',1) = '' then pl."POSITION" 
    when split_part(pl."POSITION",'-',1) = 'C2K' then 'Center' else split_part(pl."POSITION",'-',1) end as playerposition
    from {{ref('clean_players')}} pl) play on gl.player_name = play."Player"
    where gl.game_date > '2023-01-01'
    group by gl.player_name,playerposition, team_abbreviation ,gl.game_date
    