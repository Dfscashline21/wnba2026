
with overs as (select *
            ,(pts + (1.2*reb)+(1.10*ast)+ (3*(stl+blk))- tov) as prizepicksfant
            ,row_number() over (partition by t.player_name,p."attributes.odds_type"  order by game_date desc )  game_number 
            ,case
            	when p."3-PT Made" is null then 0
            	when p."3-PT Made" > fg3m then 0
            	when p."3-PT Made" < fg3m then 1
            end as "3-PT Over"
            ,case
            	when p."Assists" is null then 0
            	when p."Assists" > ast then 0
            	when p."Assists" < ast then 1
            end as "Assists Over"
            ,case
            	when p."Blks+Stls" is null then 0
            	when p."Blks+Stls" > (blk + stl) then 0
            	when p."Blks+Stls" < (blk + stl) then 1
            end as "Blks+Stls Over"
            ,case
            	when p."Fantasy Score" is null then 0
            	when p."Fantasy Score" > (pts + (1.2*reb)+(1.10*ast)+ (3*(stl+blk))- tov) then 0
            	when p."Fantasy Score" < (pts + (1.2*reb)+(1.10*ast)+ (3*(stl+blk))- tov) then 1
            end as "Fantasy Score Over"
            ,case
            	when p."Points" is null then 0
            	when p."Points" > pts then 0
            	when p."Points" < pts then 1
            end as "Points Over"
            ,case
            	when p."Pts+Asts" is null then 0
            	when p."Pts+Asts" > pts + ast then 0
            	when p."Pts+Asts" < pts +ast then 1
            end as "Pts+Asts Over"
            ,case
            	when p."Pts+Rebs" is null then 0
            	when p."Pts+Rebs" > pts + reb then 0
            	when p."Pts+Rebs" < pts + reb then 1
            end as "Pts+Rebs Over"
            ,case
            	when p."Pts+Rebs+Asts" is null then 0
            	when p."Pts+Rebs+Asts" > pts +reb+ ast then 0
            	when p."Pts+Rebs+Asts" < pts +reb+ ast then 1
            end as "Pts+Rebs+Asts Over"
            ,case
            	when p."Rebounds" is null then 0
            	when p."Rebounds" > reb then 0
            	when p."Rebounds" < reb then 1
            end as "Rebounds Over"
            ,case
            	when p."Rebs+Asts" is null then 0
            	when p."Rebs+Asts" > reb+ ast then 0
            	when p."Rebs+Asts" < reb+ ast then 1
            end as "Rebs+Asts Over"
            ,case
            	when p."Turnovers" is null then 0
            	when p."Turnovers" > tov then 0
            	when p."Turnovers" < tov then 1
            end as "Turnovers Over"
            from {{ref('clean_logs')}} t 
            left join {{source('wnba','prizepicks')}} p on t.player_name = p."attributes.name")
            select player_name,"attributes.odds_type" ,"attributes.team","3-PT Made",
            sum("3-PT Over") /10 ::float as threesover
            ,"Assists", sum("Assists Over")/10 ::float as astover,"Blks+Stls",sum("Blks+Stls Over")/10 ::float as stocksover,"Fantasy Score",sum("Fantasy Score Over")/10 ::float as fantasyscoreover,"Points",sum("Points Over")/10 ::float as pointsover,"Pts+Asts",sum("Pts+Asts Over")/10 ::float as points_ast_over,"Pts+Rebs",sum("Pts+Rebs Over")/10 ::float as points_rebover,"Pts+Rebs+Asts",sum("Pts+Rebs+Asts Over")/10 ::float as praover
            ,"Rebounds",sum("Rebounds Over")/10 ::float as rebover,"Rebs+Asts",sum("Rebs+Asts Over")/10 ::float as reb_astover,"Turnovers",sum("Turnovers Over")/10 ::float as tovover
            from overs
            where game_number <=10 and overs."attributes.name" is not null
            group by player_name,"attributes.odds_type", "attributes.team","3-PT Made","Assists","Rebounds","Blks+Stls","Rebs+Asts","Fantasy Score","Turnovers","Points","Pts+Asts","Pts+Rebs","Pts+Rebs+Asts"
            order by player_name
                        