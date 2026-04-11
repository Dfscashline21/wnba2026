# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 16:08:28 2025

@author: trent
"""
import requests 
import time 
import pandas as pd 
import numpy as np
from dotenv import load_dotenv
import os 
import json 
from pathlib import Path
import os, traceback

load_dotenv()  # Load .env variables into environment


teamrep = os.getenv('TEAM_REPLACE')

p = Path(r"C:\\Users\\Trent\\WNBA\\")
print("Path:", p)
print("Exists:", p.exists(), "| Is file:", p.is_file(), "| Is dir:", p.is_dir())
print("Parent writable:", os.access(p.parent, os.W_OK))

def get_adjusted_rates(dksalaries):

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

    rotoinjuries = 'https://www.rotowire.com/wnba/tables/injury-report.php?team=ALL&pos=ALL'
    
    rotoinj = requests.get(rotoinjuries).json()
    
    rotoinj = pd.DataFrame(rotoinj)
    
    # rotoinj.loc[rotoinj.player=='Allisha Gray', 'status'] = 'OUT'
    # rotoinj.append(addplayer,ignore_index =True)
    # rotoinj.loc[rotoinj.player=='DiJonai Carrington', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Arike Ogunbowale', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Maddy Siegrist', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Tyasha Harris', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Caitlin Clark', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Cameron Brink', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Megan Gustafson', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Karlie Samuelson', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Jonquel Jones', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Alexa Held', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Katie Lou Samuelson', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Lexie Brown', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Kahleah Copper', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=='Angel Reese', 'status'] = 'OUT'
    # rotoinj.loc[rotoinj.player=="Kayla McBride", 'status'] = 'OUT'
    status_replace = {'Out For Season':'OUT'}
    rotoinj = rotoinj.replace(status_replace)
    rotoinj = rotoinj[rotoinj['status'] == 'OUT']
    addplayer= {'player':['Brittany Sykes','DeWanna Bonner','Aaliyah Edwards','Alysha Clark','DiJonai Carrington','NaLyssa Smith','Teaira McCowan'],'team':['WAS','IND','WAS','SEA','DAL','DAL','DAL']}
    test = pd.DataFrame(addplayer)
    rotoinj = rotoinj[['player','team' ]]
    rotoinj = pd.concat([rotoinj,test])
    rotoinj = rotoinj.replace(json.loads(os.getenv("TEAM_REPLACE")))
    
    injuredteams = list(rotoinj.team.unique())
    
    

    players = 'https://sheetdb.io/api/v1/nks0ku9x5oswq'
    dksalaries = pd.read_json(players)
    minutes = dksalaries[['Name','TeamAbbrev']]
    minutes.to_csv(str(p)+'\\mincheck.csv')
    
    teams_response = requests.get("https://api.pbpstats.com/get-teams/wnba")
    teams = teams_response.json()
    all_teams = teams['teams']
    all_teams[0]['id']
    team_list = []
    for i in range(len(all_teams)):
            team_id = all_teams[i]['id']
            team_list.append(team_id)
        
    
    wowy_url = "https://api.pbpstats.com/get-wowy-stats/wnba"
    lineups =[]
    for i in range(len(team_list)):
        try:
            
            teamid = team_list[i]   
            print(teamid)
            wowy_params = {
                "TeamId": f"{teamid}", # Seattle
                "Season": "2025",
                "SeasonType": "Regular Season",
                "Type": "Player" # Team stats
            }
            wowy_response = requests.get(wowy_url, params=wowy_params,headers=headers)
            wowy = wowy_response.json()
            team_stats = wowy["single_row_table_data"]
            lineup_stats = wowy["multi_row_table_data"]
            lineups.append(lineup_stats)
            time.sleep(6)
        except:
            pass

    frame = []
    for i in range(len(lineups)):
            player1 = pd.json_normalize(lineups[i])
            frame.append(player1)
        
    baseline= pd.concat(frame).fillna(0)
    
    baseline.to_csv(str(p)+'\\players.csv')
    
    baseline_rates = baseline[['TeamId','Name','TeamAbbreviation','Minutes','Usage','Assists','Rebounds']]
    team_totals = baseline_rates.groupby(['TeamAbbreviation']).sum(['Assists','Rebounds']).reset_index()
    baseline_rates = pd.merge(baseline_rates,team_totals[['TeamAbbreviation','Assists','Rebounds']],on='TeamAbbreviation',how='left')
    baseline_rates['Ast%'] = baseline_rates['Assists_x'] / baseline_rates['Assists_y']
    baseline_rates['Reb%'] = baseline_rates['Rebounds_x'] / baseline_rates['Rebounds_y']
    baseline_rates['APM'] = baseline_rates['Assists_x'] / baseline_rates['Minutes']
    baseline_rates['RPM'] = baseline_rates['Rebounds_x'] / baseline_rates['Minutes']
    
    
    baseline_rates = baseline_rates[['TeamId','Name','TeamAbbreviation','Minutes','Usage','Ast%','Reb%','APM','RPM']]
    
    
    wowy_url = "https://api.pbpstats.com/get-wowy-stats/wnba"
    
    injuries = list(rotoinj.player.unique())
    
 
    check = injuries[0]
   
    teamdicts = []
    for team in injuredteams:
            players = rotoinj[rotoinj['team'] == team]
            key = team
            value = list(players['player'].unique())
            playerinj =[]
            for i in range(len(value)):
                try:
                    player = value[i]
                    player_id = baseline.loc[baseline['Name'] == player , 'Name'].values[0]
                    playerinj.append(player_id)
                except:
                    pass
            injdict = {key:playerinj}
            teamdicts.append(injdict)
    

    teamsinju = list(dksalaries.TeamAbbrev.unique())
    mapping = {"PHO": "PHX"}
    teamsinju = [mapping.get(t, t) for t in teamsinju]
    
    def wowyadjustment(team, teamname):
            edits = []
            currentteam = team
            print(currentteam)
            total = len(injuredplayers[teamname])
            teamcheck = baseline[baseline['TeamId'] == currentteam]
            
            teamplayers = teamcheck['Name'].tolist()
            for i in range(len(injuredplayers[teamname])):
                pl = injuredplayers[teamname][i]
                pl_id = baseline.loc[baseline['Name'] == pl , 'EntityId'].values[0]
                edits.append(pl_id)
            inj = ",".join(edits)
            wowy_url2 =f"https://api.pbpstats.com/get-wowy-stats/wnba?0Exactly{total}OffFloor={inj}&Season=2025&SeasonType=Regular%20Season&TeamId={currentteam}&Type=Player"    
            wowy_response = requests.get(wowy_url2,headers=headers)
            wowy = wowy_response.json()
            team_stats = wowy["single_row_table_data"]
            lineup_stats = wowy["multi_row_table_data"]
            lineups.append(lineup_stats)

                       
    lineups = []
    
    teamiddict = []
    for i in range(len(all_teams)):
            teamn = all_teams[i]
            teamiddict.append(teamn)
    
    for team in teamsinju:
            for element in teamiddict:
                # print(element)
                if element['text'] == team:
                    teamid = element['id']
                    print(element['text'])
            try:
                injuredplayers = [ele for ele in teamdicts if team in ele][0]
                print(injuredplayers[team])
                wowyadjustment(teamid, team)
            except:
                print(f'{team} Failed for some reason')
                pass
    
    frame = []
    for i in range(len(lineups)):
            player = pd.json_normalize(lineups[i])
            frame.append(player)
    

    try:
            adjusted= pd.concat(frame).fillna(0)
            adjusted.to_csv(str(p)+'\\adj players.csv')
            adjusted_rates = adjusted[['TeamId','Name','TeamAbbreviation','Minutes','Usage','Assists','Rebounds','Points']]
            team_totals = adjusted_rates.groupby(['TeamAbbreviation']).sum(['Assists','Rebounds','Points']).reset_index()
            adjusted_rates = pd.merge(adjusted_rates,team_totals[['TeamAbbreviation','Assists','Rebounds','Points']],on='TeamAbbreviation',how='left')
            adjusted_rates['APM'] = adjusted_rates['Assists_x'] / adjusted_rates['Minutes']
            adjusted_rates['RPM'] = adjusted_rates['Rebounds_x'] / adjusted_rates['Minutes']
            adjusted_rates['PPM'] = adjusted_rates['Points_x'] / adjusted_rates['Minutes']
        
        
        
            adjusted_rates['Ast%'] = adjusted_rates['Assists_x'] / adjusted_rates['Assists_y']
            adjusted_rates['Reb%'] = adjusted_rates['Rebounds_x'] / adjusted_rates['Rebounds_y']
        
            adjusted_rates = adjusted_rates[['TeamId','Name','TeamAbbreviation','Minutes','Usage','APM','RPM','PPM']]
            baseline_rates = baseline_rates.sort_values(by ='Minutes',ascending=False)
            baseline_rates = baseline_rates.drop_duplicates(subset ='Name')
            adjusted_rates = adjusted_rates.sort_values(by ='Minutes',ascending=False)
            adjusted_rates = adjusted_rates.drop_duplicates(subset ='Name')
        
            new_rates = pd.merge(baseline_rates[['Name','Usage','APM','RPM']],adjusted_rates,how='left',on='Name')
        
            new_rates['usage_boost'] = new_rates['Usage_y'] / new_rates['Usage_x']
            new_rates['ast_boost'] = new_rates['APM_y'] / new_rates['APM_x']
            new_rates['reb_boost'] = new_rates['RPM_y'] / new_rates['RPM_x']
            new_rates = new_rates.fillna(1)
        
            rate_boosts = new_rates[['Name','usage_boost','ast_boost','reb_boost']]
            rate_boosts['usage_boost'] = np.where(rate_boosts['usage_boost'] > 1.5, 1.5, rate_boosts['usage_boost'] )
            rate_boosts['usage_boost'] = np.where(rate_boosts['usage_boost'] < .5, .5, rate_boosts['usage_boost'] )
            rate_boosts['ast_boost'] = np.where(rate_boosts['ast_boost'] > 1.5, 1.5, rate_boosts['ast_boost'] )
            rate_boosts['ast_boost'] = np.where(rate_boosts['ast_boost'] < .5, .5, rate_boosts['ast_boost'] )
            rate_boosts['reb_boost'] = np.where(rate_boosts['reb_boost'] > 1.5, 1.5, rate_boosts['reb_boost'] )
            rate_boosts['reb_boost'] = np.where(rate_boosts['reb_boost'] < .5, .5, rate_boosts['reb_boost'] )
            rate_boosts.to_csv(str(p)+'\\boosts.csv')
    except :
            adjusted_rates = baseline_rates
            rates = adjusted_rates[['Name']]
            rates['usage_boost'],rates['ast_boost'],rates['reb_boost'] = [1,1,1]
            rate_boosts = rates.copy()
            rate_boosts.to_csv(str(p)+'\\boosts.csv')


    rate_boosts = rate_boosts.drop_duplicates(subset=['Name'])
    return rate_boosts,rotoinj