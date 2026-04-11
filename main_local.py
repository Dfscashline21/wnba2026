

import roto_pull as rp
import dk_pull as dk 
import ud_pull as ud 
import pp_pull as pp
import injuryadjustments as inj
import nbastats as nb
import pandas as pd 
from db_conn import get_db_engine
import os 
from dotenv import load_dotenv
import json
import subprocess
import sportbooks_pull as sb
from load_google import load_to_google
from trifecta import trifecta_tweet
import requests 


load_dotenv()  # Load .env variables into environment


namesrep = os.getenv('NAME_REPLACE')

# headers  = {
#     'Connection': 'keep-alive',
#     'Accept': 'application/json, text/plain, */*',
#     'x-nba-stats-token': 'true',
#     'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
#     'x-nba-stats-origin': 'stats',
#     'Sec-Fetch-Site': 'same-origin',
#     'Sec-Fetch-Mode': 'cors',
#     'Referer': 'https://stats.nba.com/',
#     'Accept-Encoding': 'gzip, deflate, br',
#     'Accept-Language': 'en-US,en;q=0.9',
# }
# rotoinjuries = 'https://www.rotowire.com/wnba/tables/injury-report.php?team=ALL&pos=ALL'

# rotoinj = requests.get(rotoinjuries).json()

# rotoinj = pd.DataFrame(rotoinj)

# # rotoinj.loc[rotoinj.player=='Allisha Gray', 'status'] = 'OUT'
# # rotoinj.append(addplayer,ignore_index =True)
# # rotoinj.loc[rotoinj.player=='DiJonai Carrington', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Arike Ogunbowale', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Maddy Siegrist', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Tyasha Harris', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Caitlin Clark', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Cameron Brink', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Megan Gustafson', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Karlie Samuelson', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Jonquel Jones', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Alexa Held', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Katie Lou Samuelson', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Lexie Brown', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Kahleah Copper', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=='Satou Sabally', 'status'] = 'OUT'
# # rotoinj.loc[rotoinj.player=="Kayla McBride", 'status'] = 'OUT'
# status_replace = {'Out For Season':'OUT'}
# rotoinj = rotoinj.replace(status_replace)
# rotoinj = rotoinj[rotoinj['status'] == 'OUT']
# addplayer= {'player':['Brittany Sykes','DeWanna Bonner','Aaliyah Edwards','Alysha Clark','DiJonai Carrington','NaLyssa Smith','Teaira McCowan'],'team':['WAS','IND','WAS','SEA','DAL','DAL','DAL']}
# test = pd.DataFrame(addplayer)
# rotoinj = rotoinj[['player','team' ]]
# rotoinj = pd.concat([rotoinj,test])
minutesprojected  ='N'  
        
print("🚀 Starting WNBA data pipeline...")


projminutes = pd.read_csv('mincheckproj.csv',encoding = 'latin-1')

minutes = rp.rotomins()

minutes= minutes.replace(json.loads(os.getenv("NAME_REPLACE")))

projminutes= projminutes.replace(json.loads(os.getenv("NAME_REPLACE")))

    
print("🤑 Pulling DraftKings salary data...")    
# dksalaries = dk.get_dk()
dksalaries = pd.read_csv('wnbadk.csv')
print("📊 Pulling Underdog props...")
underdog = ud.udprops()

print("📊 Pulling PrizePicks props...")
prizepicks = pp.prizepicks_pull()


print("📊 Pulling BetMGM props...")
try:
    betmgm = sb.pull_mgm()
except:
    pass 

print("📊 Pulling Caesars props...")
try:
    caesars = sb.pull_caesars()
except:
    pass 
print("🧮 Calculating injury-adjusted rates...")
rate_boosts,rotoinj = inj.get_adjusted_rates(dksalaries)

print("📆 Pulling team game schedule...")
teams= nb.get_schedule()

print("📂 Loading team list from local CSV...")    
team_list = pd.read_csv('wnba_team list.csv')

print("📈 Pulling player game logs and starter projections...")    

wnba_game_log, playerids,min_start = nb.pull_game_logs()

if 'team_count' in wnba_game_log.columns:
    wnba_game_log = wnba_game_log.drop(columns=['team_count'])

min_start = min_start.replace(json.loads(os.getenv("NAME_REPLACE")))

min_start = pd.merge(min_start,minutes, how='left', on='Name')

min_start = min_start[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev_x', 'AvgPointsPerGame',  'min_x','min_y']]

min_start.columns = ['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info','TeamAbbrev', 'AvgPointsPerGame', 'min_x','min_y']

print("📊 Pulling advanced defensive team stats...")
advdef = nb.adv_stats()

print("👕 Getting player position table...")
player_position_table = nb.player_pos()

print("🔌 Connecting to PostgreSQL database...")
engine = get_db_engine()

print("📝 Writing PLAYER_GAME_LOGS to database...") 
wnba_game_log.to_sql(name ='PLAYER_GAME_LOGS', schema = 'wnba',con = engine, if_exists='replace',index = False)


rotoinj.to_sql(name ='injuries', schema = 'wnba',con = engine, if_exists='replace',index = False)

# wnba_game_log.to_sql(name ='HISTORICAL_PLAYER_GAME_LOGS', schema = 'wnba',con = engine, if_exists='replace',index = False)

print("📝 Writing TEAMS table to database...")
team_list.to_sql(name ='TEAMS', schema = 'wnba', con =engine, if_exists='replace',index = False)

print("📝 Writing PLAYERS (position table) to database...")
player_position_table.to_sql(name ='PLAYERS', schema = 'wnba',con =engine, if_exists='replace',index = False)

print("📝 Writing Games (schedule) to database...")
teams.to_sql(name ='Games',schema = 'wnba',con = engine, if_exists='replace',index = False)

print("📝 Writing WOWY (injury-adjusted rates) to database...")

rate_boosts.to_sql(name ='wowy', schema = 'wnba',con =engine, if_exists = 'replace',index=False)

print("📝 Writing advanced defensive stats (pace) to database...")
advdef.to_sql(name ='pace', schema = 'wnba',con =engine, if_exists = 'replace',index=False)

print("📝 Writing Underdog props to database...")
try:
    underdog.to_sql(name ='underdog',schema = 'wnba', con = engine,if_exists = 'replace', index=False)
except:
    pass
# min_avg.to_sql('3gameminavg', engine, if_exists='replace',index = False)
print("📝 Writing DraftKings starter data to database...")
min_start.to_sql(name ='draftkings',schema = 'wnba',con =   engine, if_exists='replace',index = False)

print("📝 Writing PrizePicks props to database...")
try:
    prizepicks.to_sql(name ='prizepicks',schema = 'wnba', con = engine, if_exists = 'replace' , index =False)
except Exception as e:
    print(f"⚠️ Failed to write PrizePicks data: {e}")
try:
    betmgm.to_sql(name ='betmgm',schema = 'wnba', con = engine, if_exists = 'replace' , index =False)   
except:
    pass
try:
    caesars.to_sql(name ='caesars',schema = 'wnba', con = engine, if_exists = 'replace' , index =False)   
except: 
    pass

print("📝 Writing projected minutes to database...")    
minutes.to_sql(name ='projmins' ,schema = 'wnba', con = engine, if_exists = 'replace' , index=False)

projminutes.to_sql(name ='todaysmins' ,schema = 'wnba', con = engine, if_exists = 'replace' , index=False)


print("✅ All data written to the database successfully!")


def run_dbt():
    print("✅ Starting dbt run...\n")

    process = subprocess.Popen(
        ["dbt", "run", "--project-dir", "wnba_dbt"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )

    # Stream real-time output line-by-line
    for line in process.stdout:
        print(line, end='')

    process.wait()

    if process.returncode == 0:
        print("\n🎉 dbt run completed successfully!")
    else:
        print("\n❌ dbt run failed!")


run_dbt()

print("📝 Loading to Sheets")   

load_to_google()

trifecta_tweet()




