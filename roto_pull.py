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
import os, traceback


p = Path(r"C:\\Users\\Trent\\WNBA\\")
print("Path:", p)
print("Exists:", p.exists(), "| Is file:", p.is_file(), "| Is dir:", p.is_dir())
print("Parent writable:", os.access(p.parent, os.W_OK))

try:
    if p.exists() and p.is_file():
        with open(p, "a", encoding="utf-8") as f:
            f.write("")   # test append
        print("Append test OK")
except Exception:
    traceback.print_exc()
     
def rotomins():
    
    try:
        slatepull = requests.get('https://www.rotowire.com/daily/wnba/api/slate-list.php?siteID=1').json()['slates']
        # today = '2025-05-27'
        today = date.today().isoformat()
                
        filtered_data = [entry for entry in slatepull if entry.get("startDateOnly") == today]
    
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
        
        teamsets = pd.json_normalize(roto['team'])
        
        rotolist = pd.merge(roto, teamsets,how='left',left_index = True , right_index = True)
        minutes = rotolist[['player','team','minutes']]

    
    # seasonavg = requests.get('https://www.rotowire.com/wnba/tables/projections.php?type=pergame').json()
    
    # frames = pd.DataFrame(seasonavg)

    


    minutes['minutes'] = minutes['minutes'].astype(float)
    
    minutes.columns = ['Name','TeamAbbrev','min']
    
    roto_replace ={"Michael Porter": "Michael Porter Jr."}
    
    minutes = minutes.replace(roto_replace)
    minutes.to_csv(str(p)+'\\rotomins.csv')
    
    return minutes 

