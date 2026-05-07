with overs as (select *
            ,(pts + (1.2*reb)+(1.10*ast)+ (2*(stl+blk))- (.10*tov)) as udfant
            ,row_number() over (partition by t.player_name order by game_date desc )  game_number 
            ,case
            	when ud."three_points_made"::float is null then 0
            	when ud."three_points_made"::float > fg3m then 0
            	when ud."three_points_made"::float <= fg3m then 1
            end as "3-PT Over"
            ,case
            	when ud."assists"::float is null then 0
            	when ud."assists"::float > ast then 0
            	when ud."assists"::float <= ast then 1
            end as "Assists Over"
            ,case
            	when ud."fantasy_points"::float is null then 0
            	when ud."fantasy_points"::float > (pts + (1.2*reb)+(1.10*ast)+ (2*(stl+blk))- (.10*tov)) then 0
            	when ud."fantasy_points"::float <= (pts + (1.2*reb)+(1.10*ast)+ (2*(stl+blk))- (.10*tov)) then 1
            end as "Fantasy Score Over"
            ,case
            	when ud."blks_stls"::float is null then 0
            	when ud."blks_stls"::float > (blk + stl) then 0
            	when ud."blks_stls"::float < (blk + stl) then 1
            end as "Blks+Stls Over"
            ,case
            	when ud."free_throws_made"::float is null then 0
            	when ud."free_throws_made"::float > ftm then 0
            	when ud."free_throws_made"::float <= ftm then 1
            end as "Free Throws Made Over"
            ,case
            	when ud."points"::float is null then 0
            	when ud."points"::float > pts then 0
            	when ud."points"::float <= pts then 1
            end as "Points Over"
            ,case
            	when ud."pts_asts"::float is null then 0
            	when ud."pts_asts"::float > pts + ast then 0
            	when ud."pts_asts"::float <= pts +ast then 1
            end as "Pts+Asts Over"
            ,case
            	when ud."pts_rebs"::float is null then 0
            	when ud."pts_rebs"::float > pts + reb then 0
            	when ud."pts_rebs"::float <= pts + reb then 1
            end as "Pts+Rebs Over"
            ,case
            	when ud."pts_rebs_asts"::float is null then 0
            	when ud."pts_rebs_asts"::float > pts +reb+ ast then 0
            	when ud."pts_rebs_asts"::float <= pts +reb+ ast then 1
            end as "Pts+Rebs+Asts Over"
            ,case
            	when ud."rebounds"::float is null then 0
            	when ud."rebounds"::float > reb then 0
            	when ud."rebounds"::float <= reb then 1
            end as "Rebounds Over"
            ,case
            	when ud."rebs_asts"::float is null then 0
            	when ud."rebs_asts"::float > reb+ ast then 0
            	when ud."rebs_asts"::float <= reb+ ast then 1
            end as "Rebs+Asts Over"
            from {{ref('clean_logs')}} t 
            left join {{source('wnba','underdog')}} ud on t.player_name = ud."name")
            select player_name,team_abbreviation
            ,"three_points_made",sum("3-PT Over") /10 ::float as threesover
            ,"assists", sum("Assists Over")/10 ::float as astover,"fantasy_points",sum("Fantasy Score Over")/10 ::float as fantasyscoreover
            ,"free_throws_made",sum("Free Throws Made Over")/10 ::float as ftmadeover
            ,"points",sum("Points Over")/10 ::float as pointsover
            ,"pts_asts",sum("Pts+Asts Over")/10 ::float as points_ast_over,"pts_rebs",sum("Pts+Rebs Over")/10 ::float as points_rebover
            ,"pts_rebs_asts",sum("Pts+Rebs+Asts Over")/10 ::float as praover
            ,"rebounds",sum("Rebounds Over")/10 ::float as rebover
            ,"rebs_asts",sum("Rebs+Asts Over")/10 ::float as reb_astover
            ,"blks_stls",sum("Blks+Stls Over")/10 ::float as blk_stlover
            from overs
            where game_number <=10 and overs."name" is not null
            group by player_name, team_abbreviation,"three_points_made","assists","fantasy_points","free_throws_made","points","pts_asts","pts_rebs","pts_rebs_asts","rebounds","rebs_asts","blks_stls"
            order by player_name