# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:20:07 2025

@author: trent
"""

import requests
import datetime
import pandas as pd
from datetime import date 
from pathlib import Path
import os


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

