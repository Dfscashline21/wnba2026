# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:20:07 2025

@author: trent
"""

import datetime
import json
import os
from datetime import date 
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


p = Path(__file__).parent.parent  # project root
     
def rotomins():
    
    try:
        slatepull = requests.get('https://www.rotowire.com/daily/wnba/api/slate-list.php?siteID=1').json()['slates']
        # today = '2025-05-27'
        today = date.today().isoformat()
                
        filtered_data = [entry for entry in slatepull if entry.get("startDateOnly") == today]

        if not filtered_data:
            upcoming = sorted(
                [entry for entry in slatepull if entry.get("startDateOnly", "") > today],
                key=lambda x: x["startDateOnly"]
            )
            filtered_data = upcoming[:1]

        slateid = filtered_data[0]['slateID']
        
        rotowire = 'https://www.rotowire.com/daily/wnba/api/players.php?slateID=' +str(slateid)        
        
        
        rotoreq = requests.get(rotowire).json()
        
        
        roto = pd.DataFrame(rotoreq)
        
        roto['player'] = roto['firstName'] + ' ' + roto['lastName']
        
        teamsets = pd.json_normalize(roto['team'])
        
        rotolist = pd.merge(roto, teamsets,how='left',left_index = True , right_index = True)
        
        minutes = rotolist[['player','abbr','minutes']]
    except:
        slatepull= requests.get('https://www.rotowire.com/wnba/tables/projections.php?type=pergame').json()
        roto = pd.DataFrame(slatepull)

        if isinstance(roto['team'].iloc[0], dict):
            teamsets = pd.json_normalize(roto['team'])
            rotolist = pd.merge(roto, teamsets,how='left',left_index = True , right_index = True)
            minutes = rotolist[['player','abbr','minutes']]
        else:
            minutes = roto[['player','team','minutes']].rename(columns={'team': 'abbr'})

    
    # seasonavg = requests.get('https://www.rotowire.com/wnba/tables/projections.php?type=pergame').json()
    
    # frames = pd.DataFrame(seasonavg)

    


    minutes['minutes'] = minutes['minutes'].astype(float)
    
    minutes.columns = ['Name','TeamAbbrev','min']
    
    roto_replace ={"Michael Porter": "Michael Porter Jr."}
    
    minutes = minutes.replace(roto_replace)
    minutes.to_csv(p / 'rotomins.csv', index=False)
    
    return minutes 


def depth_chart_roster():
    columns = ["Name", "TeamAbbrev"]
    url = "https://www.rotowire.com/wnba/wnba-depth-charts/"

    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        records = []

        for block in soup.select(".depth-charts__block"):
            team_alt = block.select_one(".depth-charts__team img")
            team_abbrev = (team_alt.get("alt") if team_alt else "") or ""

            for player_link in block.select(".depth-charts__pos-list li a"):
                first_name = player_link.select_one(".hide-until-xs")
                if first_name:
                    last_name = player_link.contents[-1]
                    player_name = f"{first_name.get_text(strip=True)} {str(last_name).strip()}".strip()
                else:
                    player_name = player_link.get_text(" ", strip=True)

                if player_name and team_abbrev:
                    records.append({"Name": player_name, "TeamAbbrev": team_abbrev})

        roster = pd.DataFrame(records, columns=columns).drop_duplicates(ignore_index=True)
        name_map_raw = os.getenv("NAME_REPLACE")
        if name_map_raw:
            roster["Name"] = roster["Name"].replace(json.loads(name_map_raw))

        return roster
    except Exception as e:
        print(f"Error in depth_chart_roster: {e}")
        return pd.DataFrame(columns=columns)

