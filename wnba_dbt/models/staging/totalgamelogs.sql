
select * from {{ source('wnba', 'PLAYER_GAME_LOGS') }}
union
select * from {{ source('wnba', 'HISTORICAL_PLAYER_GAME_LOGS') }}