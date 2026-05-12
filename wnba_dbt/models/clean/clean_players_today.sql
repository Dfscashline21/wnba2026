select * from {{ source('wnba', 'players_today') }}
