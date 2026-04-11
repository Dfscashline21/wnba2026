-- models/clean/clean_players.sql

with base as (

    select * from {{ source('wnba', 'PLAYERS') }}

),

cleaned as (

    select 
        case 
            when "Player" = 'Dorka Juhász' then 'Dorka Juhasz'
            when "Player" = 'Marine Johannès' then 'Marine Johannes'
            when "Player" = 'Asia (AD) Durr' then 'Asia Durr'
            when "Player" = 'Sika Koné'  then  'Sika Kone'
            when "Player" = 'Li Meng'  then 'Meng Li'
            when "Player" = 'Ivana Dojkić'  then 'Ivana Dojkic'
            when "Player" = 'Amanda Zahui B' then 'Amanda Zahui B.'
        else "Player"    
        END  as "Player"
         ,"Position" as "POSITION"

    from base

)

select * from cleaned
