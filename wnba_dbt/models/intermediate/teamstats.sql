
select case when split_part(gl.matchup,'@',2) = '' then split_part(gl.matchup,'vs.',2) else  split_part(gl.matchup,'@',2) end as opponent ,playerposition , game_date, sum(fga) as totalfga, sum(fg3a) as totalfg3a , sum(fta) as totalfta , sum(oreb) as totaloreb , sum(dreb) as totaldreb, sum(ast) as totalast , sum(stl) as totalstl , sum(blk) as totalblk , sum(pts) as totalpts , sum("min") as  totalmins
, row_number() over (partition by case when split_part(gl.matchup,'@',2) = '' then split_part(gl.matchup,'vs.',2) else  split_part(gl.matchup,'@',2) end , playerposition order by game_date desc )  game_number
from {{ref('clean_logs')}} gl
left join (select pl."Player",case when split_part(pl."POSITION",'-',1) = '' then pl."POSITION" 
        when split_part(pl."POSITION",'-',1) = 'C2K' then 'Center' else split_part(pl."POSITION",'-',1) end as playerposition
from {{ref('clean_players')}} pl) play on gl.player_name = play."Player"
group by opponent, playerposition ,game_date
having playerposition <> '' 