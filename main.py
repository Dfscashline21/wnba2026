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
import yaml  # Add this import for creating the DBT profile

def create_dbt_profile():
    """Create DBT profile for cloud environment."""
    try:
        # Create the profiles directory
        profiles_dir = '/tmp'
        os.makedirs(profiles_dir, exist_ok=True)
        
        # Create the profiles.yml content
        profile_config = {
            'wnba_dbt': {
                'target': 'prod',
                'outputs': {
                    'prod': {
                        'type': 'postgres',
                        'host': os.getenv('DB_HOST'),
                        'user': os.getenv('DB_USER'),
                        'password': os.getenv('DB_PASSWORD'),
                        'port': int(os.getenv('DB_PORT', 5432)),
                        'dbname': os.getenv('DB_NAME'),
                        'schema': 'wnba',
                        'threads': 1,
                        'keepalives_idle': 0
                    }
                }
            }
        }
        
        # Write the profiles.yml file
        profiles_file = os.path.join(profiles_dir, 'profiles.yml')
        with open(profiles_file, 'w') as f:
            yaml.dump(profile_config, f, default_flow_style=False)
        
        print(f"✅ Created DBT profile at {profiles_file}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create DBT profile: {e}")
        return False

def main():
    load_dotenv()  # Load .env variables into environment

    namesrep = os.getenv('NAME_REPLACE')
    minutesprojected  ='N'  
            
    print("🚀 Starting WNBA data pipeline...")

    projminutes = pd.read_csv('mincheckproj.csv',encoding = 'latin-1')

    minutes = rp.rotomins()


    minutes= minutes.replace(json.loads(os.getenv("NAME_REPLACE")))
    projminutes= projminutes.replace(json.loads(os.getenv("NAME_REPLACE")))

    print("🤑 Pulling DraftKings salary data...")    
    dksalaries = pd.read_csv('wnbadk.csv')
    
    print("📊 Pulling Underdog props...")
    try:
        underdog = ud.udprops()
        if underdog is None or underdog.empty:
            print("⚠️ No Underdog data available")
            underdog = pd.DataFrame()
    except Exception as e:
        print(f"⚠️ Underdog error: {e}")
        underdog = pd.DataFrame()

    print("📊 Pulling PrizePicks props...")
    try:
        prizepicks = pp.prizepicks_pull()
    except Exception as e:
        print(f"⚠️ PrizePicks error: {e}")
        prizepicks = pd.DataFrame()

    print("📊 Pulling BetMGM props...")
    try:
        betmgm = sb.pull_mgm()
    except Exception as e:
        print(f"⚠️ BetMGM error: {e}")
        betmgm = None

    print("🧮 Calculating injury-adjusted rates...")
    rate_boosts = inj.get_adjusted_rates(dksalaries)

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
    if not underdog.empty:
        underdog.to_sql(name ='underdog',schema = 'wnba', con = engine,if_exists = 'replace', index=False)

    print("📝 Writing DraftKings starter data to database...")
    min_start.to_sql(name ='draftkings',schema = 'wnba',con =   engine, if_exists='replace',index = False)

    print("📝 Writing PrizePicks props to database...")
    if not prizepicks.empty:
        prizepicks.to_sql(name ='prizepicks',schema = 'wnba', con = engine, if_exists = 'replace' , index =False)

    if betmgm is not None:
        betmgm.to_sql(name ='betmgm',schema = 'wnba', con = engine, if_exists = 'replace' , index =False)   

    print("📝 Writing projected minutes to database...")    
    minutes.to_sql(name ='projmins' ,schema = 'wnba', con = engine, if_exists = 'replace' , index=False)
    
    
    projminutes.to_sql(name ='todaysmins' ,schema = 'wnba', con = engine, if_exists = 'replace' , index=False)


    print("✅ All data written to the database successfully!")

    # Enhanced Solution 2: Create DBT profile and set environment variables
    try:
        print("✅ Creating DBT profile...")
        profile_created = create_dbt_profile()
        
        if profile_created:
            print("✅ Setting up DBT environment variables...")
            
            # Set DBT environment variables to use /tmp directory
            os.environ['DBT_PROFILES_DIR'] = '/tmp'
            os.environ['DBT_PROJECT_DIR'] = './wnba_dbt'
            
            print("✅ Starting dbt run...\n")
            process = subprocess.Popen(
                ["dbt", "run", "--project-dir", "wnba_dbt", "--profiles-dir", "/tmp"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in process.stdout:
                print(line, end='')
            process.wait()
            if process.returncode == 0:
                print("\n🎉 dbt run completed successfully!")
            else:
                print("\n❌ dbt run failed!")
        else:
            print("❌ Skipping DBT run due to profile creation failure")
            
    except Exception as e:
        print(f"⚠️ dbt run failed: {e}")

    print("📝 Loading to Sheets")   
    load_to_google()
    trifecta_tweet()

def run_script(request):
    """HTTP Cloud Function entry point."""
    try:
        print("🚀 Cloud Function starting...")
        main()
        return ("Script executed successfully!", 200)
    except Exception as e:
        print(f"❌ Script failed: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return (f"Script failed: {e}", 500) 