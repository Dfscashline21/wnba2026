# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:26:57 2025

@author: trent
"""
import json
import requests
import pandas as pd
from dateutil.parser import parse
import datetime
from pytz import timezone
import os 
from dotenv import load_dotenv
from pathlib import Path
import os

p = Path(__file__).parent.parent  # project root


load_dotenv()  # Load .env variables into environment
     

single_game ='N'

def get_dk():
    req = requests.get('https://www.draftkings.com/lobby/getcontests?sport=NBA')
    
    slate = req.json()
    
    draftgroups = slate['DraftGroups']
    
    df2 = pd.DataFrame(draftgroups).sort_values(by='StartDate')
    
    if single_game =='Y':
        wnbacontest = df2[df2['ContestTypeId']==81]
        groupid = wnbacontest['DraftGroupId'].values[0]
    else:
        wnbacontest = df2[df2['GameTypeId']==37]
        if wnbacontest.empty:
            wnbacontest = df2[df2['ContestStartTimeSuffix'].str.contains('WNBA', na=False, regex=False)]
        groupid = wnbacontest['DraftGroupId'].values[0]
        
    

    
    
    dg = 'https://api.draftkings.com/draftgroups/v1/draftgroups/' + str(groupid) +'/draftables'
    
    draft = requests.get(dg).json()
    
    draftables = draft['draftables']
    player_list = []
    for i in range(len(draftables)):
            player = draftables[i]
            name = player['displayName']
            position = player['position']
            ident = player['draftableId']
            key = str(name) + " (" + str(ident) + ")"
            salary = player['salary']
            game = player['competitions'][0]['name'] + ' '+ player['competitions'][0]['startTime']
            team = player['teamAbbreviation']
            fppg = player['draftStatAttributes'][0]['value']
            dkplayer= [position,key,name,ident,salary,game,team,fppg]
            player_list.append(dkplayer)
        
    dksalaries = pd.DataFrame(player_list , columns= ['Position','Name + ID','Name','ID','Salary','Game Info','TeamAbbrev','AvgPointsPerGame'])
    dksalaries = dksalaries.drop_duplicates(subset=['Name','TeamAbbrev'])
    dksalaries.to_csv(p / 'wnbadk.csv', index=False)
    
    dksalaries[['Away','@','Home','Time']] = dksalaries['Game Info'].str.split(' ',n=4,expand = True)
    
    dksalaries['Time']  = [parse(x) for x in dksalaries['Time']]
    
    dksalaries['Time'] = [x.replace(tzinfo=datetime.timezone.utc) for x in dksalaries['Time']]
          
    dksalaries['Time'] =  [x.astimezone(timezone('US/Eastern')) for x in dksalaries['Time']]      
    
    dksalaries['Time'] = [x.strftime("%m/%d/%Y %I:%M%p %Z") for x in dksalaries['Time']]
    
    dksalaries['Game Info'] = dksalaries['Away'] + '@' +dksalaries['Home'] +' ' + dksalaries['Time']
    
    dksalaries = dksalaries[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev', 'AvgPointsPerGame']]
    
    
    return dksalaries