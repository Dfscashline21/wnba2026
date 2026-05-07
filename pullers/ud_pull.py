# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:48:59 2025

@author: trent
"""
import json 
import requests
import pandas as pd
import os 
from dotenv import load_dotenv


load_dotenv()  # Load .env variables into environment

def udprops():
   # underdog = 'https://api.underdogfantasy.com/beta/v3/over_under_lines'
   underdog = 'https://api.underdogfantasy.com/beta/v6/over_under_lines?sport_id=wnba&state_config_id=0a4fe68a-2598-4796-ba48-e3faa5ecf5c3'
   
   # Initialize wnbaund as None
   wnbaund = None
   
   try:    
           req = requests.get(underdog).json()
           dog = pd.json_normalize(req['over_under_lines'])
           
           main = pd.json_normalize(req['appearances'])
           players = pd.json_normalize(req['players'])
           players.columns
           players['name'] = players['first_name'].astype(str) + ' ' + players['last_name'].astype(str)
           
           game =pd.json_normalize(req['games'])
           
           
           dog = dog[['id', 'stat_value','over_under.appearance_stat.appearance_id','over_under.appearance_stat.stat','over_under.title']]
           
           prop = pd.merge(dog,main[['id','player_id']],how='left',left_on='over_under.appearance_stat.appearance_id', right_on='id')
           
           prop = pd.merge(prop,players[['id','name','sport_id']],how='left',left_on='player_id',right_on = 'id')
           prop.columns
           underdogprops =  prop[['name','sport_id','stat_value','over_under.appearance_stat.stat']]
           underdogprops = underdogprops.copy()
           underdogprops['stat_value'] = pd.to_numeric(underdogprops['stat_value'], errors='coerce')
           udprops = pd.pivot_table(underdogprops,values = 'stat_value',index=['name','sport_id'],columns='over_under.appearance_stat.stat',aggfunc = sum)
           
           wnbaund = udprops.reset_index()
           wnbaund = wnbaund[wnbaund['sport_id'] =='WNBA']
           wnbaund['three_points_made'] = wnbaund.get('three_points_made', float('NaN'))
           wnbaund['free_throws_made'] = wnbaund.get('free_throws_made', float('NaN'))
           wnbaund['blks_stls'] = wnbaund.get('blks_stls', float('NaN'))
           wnbaund['fantasy_points'] = wnbaund.get('fantasy_points', float('NaN'))
           
           wnbaund['name'] = wnbaund['name'].replace(json.loads(os.getenv("NAME_REPLACE")))
        #    wnbaund.to_csv('test.csv')
   except Exception as e:
           print(f"Error in udprops: {e}")
           # Create an empty DataFrame as fallback
           wnbaund = pd.DataFrame()
   
   return wnbaund