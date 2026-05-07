
select
    season_year, player_id, player_name, nickname, team_id, team_abbreviation,
    team_name, game_id, game_date, matchup, wl, min, fgm, fga, fg_pct,
    fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov,
    stl, blk, blka, pf, pfd, pts, plus_minus, nba_fantasy_pts, dd2, td3,
    wnba_fantasy_pts, available_flag, "GameKey"
from {{ source('wnba', 'PLAYER_GAME_LOGS') }}
union
select
    season_year, player_id, player_name, nickname, team_id, team_abbreviation,
    team_name, game_id, game_date, matchup, wl, min, fgm, fga, fg_pct,
    fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, tov,
    stl, blk, blka, pf, pfd, pts, plus_minus, nba_fantasy_pts, dd2, td3,
    wnba_fantasy_pts, available_flag, "GameKey"
from {{ source('wnba', 'HISTORICAL_PLAYER_GAME_LOGS') }}