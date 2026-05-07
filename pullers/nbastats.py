# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 16:22:18 2025

@author: trent
"""

# import requests
import datetime
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
import json
from . import dk_pull as dk
# import urllib.request as ul
import httpx
import pytz




load_dotenv()  # Load .env variables into environment

namesrep = os.getenv('NAME_REPLACE')

SEASON = os.getenv('WNBA_SEASON', '2026')
season_id = [SEASON]


headers  = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  +str(season_id) +'&SeasonSegment=&SeasonType=Playoffs&ShotClockRange=&VsConference=&VsDivision='
team_stats_url = 'https://stats.wnba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerMinute&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='

teamdef ='https://stats.wnba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Opponent&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season=2024&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='


seasontype =['Pre%20Season','Regular%20Season','Playoffs']

timezone =pytz.timezone("America/Los_Angeles")

switch_hour = 20

def get_schedule():
    now = datetime.datetime.now(timezone)
    today = now.date()

    if now.hour >= switch_hour:
        todaysdate = today + datetime.timedelta(days=1)
    else:
        todaysdate = today

    url = f'https://content-api-prod.nba.com/public/1/leagues/wnba/schedule?addEvents=true&seasonYear=2026'
    response = httpx.get(url).json()
    games = response['results']['schedule']

    target_dates = {str(todaysdate), str(todaysdate + datetime.timedelta(days=1))}

    main = []
    for game in games:
        if game.get('type') != 'game':
            continue
        game_date = game['easternTime'][:10]
        if game_date in target_dates:
            main.append([game['home']['abbr'], game['visitor']['abbr']])

    teams = pd.DataFrame(main, columns=['Home_abb', 'Away_abb'])
    return teams



# res = requests.get(url = team_stats_url, headers=headers).json()
    
 
def pull_game_logs():    
    dksalaries = dk.get_dk()
    
    # historical_logs = []
    # for year in season_id:    
    #         print(year)
    #         game_log_url= 'https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season=2025&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision='
    #         # game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  + str(year) +'&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision='                    
    #         response = requests.get(url = game_log_url, headers=headers).json()
    #         columns_list = response['resultSets'][0]['headers']
    #         player_info = response['resultSets'][0]['rowSet']    
    #         wnba_game_log = pd.DataFrame(player_info,columns = columns_list)    
    #         wnba_game_log.columns = wnba_game_log.columns.str.lower()
    #         historical_logs.append(wnba_game_log)
            
    historical_logs = []
    for year in season_id:    
            print(year)
            game_log_url= f'https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season={SEASON}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision='
            # game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  + str(year) +'&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&VsConference=&VsDivision='                    
            response=  httpx.get(url = game_log_url, headers=headers).json()
            columns_list = response['resultSets'][0]['headers']
            player_info = response['resultSets'][0]['rowSet']    
            wnba_game_log = pd.DataFrame(player_info,columns = columns_list)    
            wnba_game_log.columns = wnba_game_log.columns.str.lower()
            historical_logs.append(wnba_game_log)
    
    # for year in season_id:   
    #         game_log_url ='https://stats.wnba.com/stats/playergamelogs?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N&Season='  + str(year) +'&SeasonSegment=&SeasonType=Pre+Season&ShotClockRange=&VsConference=&VsDivision='    
    #         response = requests.get(url = game_log_url, headers=headers).json()
    #         columns_list = response['resultSets'][0]['headers']
    #         player_info = response['resultSets'][0]['rowSet']    
    #         wnba_game_log = pd.DataFrame(player_info,columns = columns_list)    
    #         wnba_game_log.columns = wnba_game_log.columns.str.lower()
    #         historical_logs.append(wnba_game_log) 
            
    
    wnba_game_log = pd.concat(historical_logs)
    wnba_game_log = wnba_game_log.drop('min_sec', axis=1)
    # wnba_game_log = wnba_game_log.drop('team_count', axis=1)
    wnba_game_log  = wnba_game_log.replace(json.loads(os.getenv("NAME_REPLACE")))
    
    
    wnba_game_log['GameKey'] = wnba_game_log.game_date.astype(str) + '-' + wnba_game_log.team_abbreviation.astype(str)
    gamekey = wnba_game_log[['GameKey','game_date','team_abbreviation']]
    gamekey = gamekey.drop_duplicates(subset=['GameKey'])
    gamekey = gamekey.sort_values(by=['team_abbreviation','game_date'], ascending = (True,False))
    gamekey['gamenumber'] = gamekey.groupby('team_abbreviation').cumcount() +1
    
    wnba_mins = pd.merge(wnba_game_log,gamekey[['GameKey','gamenumber']],how='left',on='GameKey')

    last_3 = wnba_mins[wnba_mins['gamenumber'] <=3].fillna(0)

    min_avg = last_3[['player_name','min']].groupby('player_name').mean(numeric_only=True).reset_index()
     
    min_start = pd.merge(dksalaries,min_avg, how='left',left_on='Name', right_on ='player_name').fillna(0)    
  
    playerids = wnba_game_log['player_id'].unique().tolist()

    return wnba_game_log, playerids,min_start



def adv_stats():
    adv = f'https://stats.wnba.com/stats/leaguedashteamstats?Conference=&DateFrom=&DateTo=&Division=&GameScope=&GameSegment=&LastNGames=0&LeagueID=10&Location=&MeasureType=Advanced&Month=0&OpponentTeamID=0&Outcome=&PORound=0&PaceAdjust=N&PerMode=PerGame&Period=0&PlayerExperience=&PlayerPosition=&PlusMinus=N&Rank=N&Season={SEASON}&SeasonSegment=&SeasonType=Regular+Season&ShotClockRange=&StarterBench=&TeamID=0&TwoWay=0&VsConference=&VsDivision='
        
    reso = httpx.get(url = adv, headers=headers).json()
    check = reso['resultSets'][0]['headers']
    advdata = reso['resultSets'][0]['rowSet']
    advdef = pd.DataFrame(advdata,columns = check)
    advdef = advdef[['TEAM_ID','TEAM_NAME','PACE']]

    return advdef

def player_pos():
    # playersjs = 'https://stats.nba.com/stats/playerindex?College=&Country=&DraftPick=&DraftRound=&DraftYear=&Height=&Historical=1&LeagueID=00&Season=2024-25&SeasonType=Regular%20Season&TeamID=0&Weight='
    # playerres = requests.get(url = playersjs, headers=headers).json()
    # player_name = playerres['resultSets'][0]['rowSet']
    # player_position_table = pd.DataFrame(player_name, columns =playerres['resultSets'][0]['headers'] )
    player_position_table = pd.read_csv(Path(__file__).parent.parent / 'playerposition.csv')
    return player_position_table 


