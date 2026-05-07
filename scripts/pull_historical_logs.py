"""
One-time script: pull WNBA Regular Season game logs for 2023, 2024, 2025
and write them to HISTORICAL_PLAYER_GAME_LOGS in the wnba schema.

Run from the project root:
    python scripts/pull_historical_logs.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import httpx
import pandas as pd
from dotenv import load_dotenv
from db.conn import get_db_engine

load_dotenv()

HEADERS = {
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

SEASONS = ['2023', '2024', '2025']

# Columns dropped by main.py before writing PLAYER_GAME_LOGS — must match here
DROP_COLS = {'min_sec', 'team_count'}


def pull_season(year: str, retries: int = 3) -> pd.DataFrame:
    url = (
        f'https://stats.wnba.com/stats/playergamelogs'
        f'?DateFrom=&DateTo=&GameSegment=&LastNGames=0&LeagueID=10'
        f'&Location=&MeasureType=Base&Month=0&OpponentTeamID=0&Outcome='
        f'&PORound=0&PaceAdjust=N&PerMode=Totals&Period=0&PlusMinus=N&Rank=N'
        f'&Season={year}&SeasonSegment=&SeasonType=Regular+Season'
        f'&ShotClockRange=&VsConference=&VsDivision='
    )
    print(f'  Pulling {year}...')
    for attempt in range(1, retries + 1):
        try:
            with httpx.Client(timeout=120) as client:
                response = client.get(url, headers=HEADERS).json()
            break
        except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
            if attempt == retries:
                raise
            print(f'    timeout on attempt {attempt}, retrying...')
    columns = response['resultSets'][0]['headers']
    rows = response['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=columns)
    df.columns = df.columns.str.lower()
    to_drop = [c for c in df.columns if c in DROP_COLS]
    if to_drop:
        df = df.drop(columns=to_drop)
    df['GameKey'] = df['game_date'].astype(str) + '-' + df['team_abbreviation'].astype(str)
    print(f'    {year}: {len(df)} rows, columns: {list(df.columns)}')
    return df


def main():
    all_logs = [pull_season(y) for y in SEASONS]

    # Align on the intersection of columns across all seasons so the UNION
    # in totalgamelogs.sql sees a consistent schema
    common_cols = list(all_logs[0].columns)
    for df in all_logs[1:]:
        common_cols = [c for c in common_cols if c in df.columns]
    all_logs = [df[common_cols] for df in all_logs]

    combined = pd.concat(all_logs, ignore_index=True)
    print(f'\nTotal rows: {len(combined)}, columns: {len(common_cols)}')

    engine = get_db_engine()
    combined.to_sql(
        name='HISTORICAL_PLAYER_GAME_LOGS',
        schema='wnba',
        con=engine,
        if_exists='replace',
        index=False,
    )
    print('Written to wnba.HISTORICAL_PLAYER_GAME_LOGS')


if __name__ == '__main__':
    main()
