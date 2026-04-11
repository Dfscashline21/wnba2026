
with overs as (select *
            ,(pts + (1.2*reb)+(1.5*ast)+ (3*(stl+blk))- tov) as prizepicksfant
            ,row_number() over (partition by t.player_name  order by game_date desc )  game_number 
            ,case
            	when p."3-Pointers" is null then 0
            	when p."3-Pointers" > fg3m then 0
            	when p."3-Pointers" < fg3m then 1
            end as "3-PT Over"
            ,case
            	when p."Assists" is null then 0
            	when p."Assists" > ast then 0
            	when p."Assists" < ast then 1
            end as "Assists Over"
            ,case
            	when p."Points" is null then 0
            	when p."Points" > pts then 0
            	when p."Points" < pts then 1
            end as "Points Over"
            ,case
            	when p."Rebounds" is null then 0
            	when p."Rebounds" > reb then 0
            	when p."Rebounds" < reb then 1
            end as "Rebounds Over"
            from {{ref('clean_logs')}} t 
            left join {{source('wnba','betmgm')}} p on t.player_name = p.player)
            select player,team_abbreviation,"3-Pointers",
            sum("3-PT Over") /5 ::float as threesover
            ,"Assists", sum("Assists Over")/5 ::float as astover,"Points",sum("Points Over")/5 ::float as pointsover
            ,"Rebounds",sum("Rebounds Over")/5 ::float as rebover
            from overs
            where game_number <=5 and overs.player is not null and overs."date"::date = current_date
            group by player, team_abbreviation,"3-Pointers","Assists","Rebounds","Points"
            
                        