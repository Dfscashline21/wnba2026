from pullers import roto_pull as rp, dk_pull as dk, ud_pull as ud, pp_pull as pp
from pullers import nbastats as nb, sportbooks_pull as sb
from modeling import injuryadjustments as inj
from db.conn import get_db_engine
from distribution.trifecta import trifecta_tweet
import pandas as pd
import os
import time
from pathlib import Path
from dotenv import load_dotenv
import json
import subprocess
import yaml
from sqlalchemy import text

ROOT = Path(__file__).parent

def create_dbt_profile():
    try:
        profiles_dir = os.getenv('DBT_PROFILES_DIR', str(Path.home() / '.dbt'))
        os.makedirs(profiles_dir, exist_ok=True)

        # SUPABASE_HOST may be a full connection URL — parse it if so
        host_val = os.getenv('SUPABASE_HOST', '')
        if host_val.startswith('postgresql://') or host_val.startswith('postgres://'):
            from sqlalchemy.engine import make_url
            parsed = make_url(host_val)
            db_host = parsed.host
            db_port = parsed.port or 5432
            db_user = parsed.username
            db_password = str(parsed.password) if parsed.password else ''
            db_name = parsed.database.lstrip('/')
        else:
            db_host = host_val
            db_port = int(os.getenv('SUPABASE_PORT') or 5432)
            db_user = os.getenv('SUPABASE_USER', 'postgres')
            db_password = os.getenv('SUPABASE_PASSWORD', '')
            db_name = os.getenv('SUPABASE_DBNAME', 'postgres')

        profile_config = {
            'wnba_dbt': {
                'target': 'prod',
                'outputs': {
                    'prod': {
                        'type': 'postgres',
                        'host': db_host,
                        'user': db_user,
                        'password': db_password,
                        'port': db_port,
                        'dbname': db_name,
                        'schema': 'wnba',
                        'threads': 1,
                        'keepalives_idle': 0,
                        'sslmode': 'require'
                    }
                }
            }
        }

        profiles_file = os.path.join(profiles_dir, 'profiles.yml')
        with open(profiles_file, 'w') as f:
            yaml.dump(profile_config, f, default_flow_style=False)

        print(f"  dbt profile written to {profiles_file}")
        return True

    except Exception as e:
        print(f"  ERROR creating dbt profile: {e}")
        return False


def main():
    load_dotenv()

    t0 = time.time()
    n = 0
    TOTAL = 25

    def step(name):
        nonlocal n
        n += 1
        print(f"[{time.time() - t0:6.1f}s] ({n}/{TOTAL}) {name}")

    minutesprojected = 'N'

    step("Load CSV files")
    projminutes = pd.read_csv(ROOT / 'mincheckproj.csv', encoding='latin-1')
    dksalaries = pd.read_csv(ROOT / 'wnbadk.csv')

    step("roto_pull.rotomins + name replacement")
    try:
        minutes = rp.rotomins()
        name_map = json.loads(os.getenv("NAME_REPLACE"))
        minutes = minutes.replace(name_map)
        projminutes = projminutes.replace(name_map)
    except Exception as e:
        print(f"  WARN rotomins: {e}")
        minutes = pd.DataFrame()

    step("Underdog props")
    try:
        underdog = ud.udprops()
        if underdog is None or underdog.empty:
            print("  WARN no Underdog data available")
            underdog = pd.DataFrame()
    except Exception as e:
        print(f"  WARN Underdog: {e}")
        underdog = pd.DataFrame()

    step("PrizePicks props")
    try:
        prizepicks = pp.prizepicks_pull()
    except Exception as e:
        print(f"  WARN PrizePicks: {e}")
        prizepicks = pd.DataFrame()

    step("BetMGM props")
    try:
        betmgm = sb.pull_mgm()
    except Exception as e:
        print(f"  WARN BetMGM: {e}")
        betmgm = None

    step("Injury-adjusted rates")
    rate_boosts, rotoinj = inj.get_adjusted_rates(dksalaries)

    step("Schedule")
    teams = nb.get_schedule()

    step("Team list CSV")
    team_list = pd.read_csv(ROOT / 'wnba_team list.csv')

    step("Game logs + starter projections")
    wnba_game_log, playerids, min_start = nb.pull_game_logs()
    if 'team_count' in wnba_game_log.columns:
        wnba_game_log = wnba_game_log.drop(columns=['team_count'])
    min_start = min_start.replace(json.loads(os.getenv("NAME_REPLACE")))
    min_start = pd.merge(min_start, minutes, how='left', on='Name')
    min_start = min_start[['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info', 'TeamAbbrev_x', 'AvgPointsPerGame', 'min_x', 'min_y']]
    min_start.columns = ['Position', 'Name + ID', 'Name', 'ID', 'Salary', 'Game Info', 'TeamAbbrev', 'AvgPointsPerGame', 'min_x', 'min_y']

    step("Advanced defensive stats")
    advdef = nb.adv_stats()

    step("Player position table")
    player_position_table = nb.player_pos()

    step("Connect to database")
    engine = get_db_engine()

    step("Write PLAYER_GAME_LOGS")
    wnba_game_log.to_sql(name='PLAYER_GAME_LOGS', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write TEAMS")
    team_list.to_sql(name='TEAMS', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write PLAYERS")
    player_position_table.to_sql(name='PLAYERS', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write Games")
    teams.to_sql(name='Games', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write wowy")
    rate_boosts.to_sql(name='wowy', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write pace")
    advdef.to_sql(name='pace', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write underdog")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE IF NOT EXISTS wnba.underdog (name text)"))
    if not underdog.empty:
        underdog.to_sql(name='underdog', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write draftkings")
    min_start.to_sql(name='draftkings', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write prizepicks")
    if not prizepicks.empty:
        prizepicks.to_sql(name='prizepicks', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write betmgm")
    if betmgm is not None and not betmgm.empty:
        betmgm.to_sql(name='betmgm', schema='wnba', con=engine, if_exists='replace', index=False)

    step("Write projmins + todaysmins")
    minutes.to_sql(name='projmins', schema='wnba', con=engine, if_exists='replace', index=False)
    projminutes.to_sql(name='todaysmins', schema='wnba', con=engine, if_exists='replace', index=False)

    step("dbt run")
    try:
        profile_created = create_dbt_profile()
        if profile_created:
            profiles_dir = os.getenv('DBT_PROFILES_DIR', str(Path.home() / '.dbt'))
            DBT_EXE = os.getenv(
                "DBT_EXE",
                r"C:\Users\Trenton\AppData\Local\Programs\Python\Python313\Scripts\dbt.exe"
            )
            process = subprocess.Popen(
                [DBT_EXE, "run", "--project-dir", str(ROOT / "wnba_dbt"), "--profiles-dir", profiles_dir],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in process.stdout:
                print(line, end='')
            process.wait()
            if process.returncode != 0:
                print("  ERROR dbt run failed")
        else:
            print("  ERROR skipping dbt run due to profile creation failure")
    except Exception as e:
        print(f"  WARN dbt: {e}")

    step("trifecta_tweet")
    trifecta_tweet()

    print(f"\nDone in {time.time() - t0:.1f}s")


def run_script(request):
    """HTTP Cloud Function entry point."""
    try:
        print("Cloud Function starting...")
        main()
        return ("Script executed successfully!", 200)
    except Exception as e:
        print(f"ERROR script failed: {e}")
        import traceback
        print(f"Full traceback: {traceback.format_exc()}")
        return (f"Script failed: {e}", 500)


if __name__ == '__main__':
    main()
