-- models/clean/clean_data.sql

with base as (

    select * from {{ ref('totalgamelogs') }}

),

cleaned as (
 
select
    case player_name
        when 'Amanda Zahui B' then  'Amanda Zahui B.'
        when 'Ivana Dojkić' then  'Ivana Dojkic'
        when 'Dorka Juhász' then  'Dorka Juhasz'
        when 'Marine Johannès' then  'Marine Johannes'
        when 'Asia (AD) Durr' then  'Asia Durr'
        when 'Sika Koné' then  'Sika Kone'
        when 'Li Meng' then  'Meng Li'
        when 'Temi Fágbénlé' then 'Temi Fagbenle'
        when 'Lexi Held' then 'Alexa Held'
        when 'Janelle Salaün' then 'Janelle Salaun'
        when 'Marième Badiane' then 'Marieme Badiane'
        else player_name
    end as player_name,
    season_year,player_id,nickname,team_id,team_abbreviation,team_name,game_id,game_date,matchup,wl,min,fgm,fga,fg_pct,fg3m,fg3a,fg3_pct,ftm,fta,ft_pct,oreb,dreb,reb,ast,tov,stl,blk,blka,pf,pfd,pts,plus_minus,nba_fantasy_pts,dd2,td3,wnba_fantasy_pts,available_flag,"GameKey"
    from base

)

select * from cleaned
