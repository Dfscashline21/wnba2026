"""
One-time script: pull 2026 WNBA preseason game logs and merge into PLAYER_GAME_LOGS.
Deduplicates on (game_id, player_id) so it's safe to re-run.

Run from project root:
    python pull_preseason_2026.py
"""

import json
import os
import sys

import httpx
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text

load_dotenv()

from db.conn import get_db_engine  # noqa: E402 — after load_dotenv

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

PRESEASON_URL = (
    'https://stats.wnba.com/stats/leaguegamefinder'
    '?Conference=&DateFrom=04%2F01%2F2026&DateTo=05%2F07%2F2026'
    '&DraftNumber=&DraftRound=&DraftYear=&GB=N&LeagueID=10&Location=&Outcome='
    '&PlayerOrTeam=P&Season=&SeasonType=Pre+Season'
    '&StatCategory=PTS&TeamID=&VsConference=&VsTeamID='
)


def pull_preseason_logs() -> pd.DataFrame:
    print("Fetching 2026 preseason logs from stats.wnba.com...")
    response = httpx.get(PRESEASON_URL, headers=HEADERS, timeout=30).json()
    columns = response['resultSets'][0]['headers']
    rows = response['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=columns)
    df.columns = df.columns.str.lower()

    if 'min_sec' in df.columns:
        df = df.drop('min_sec', axis=1)

    # leaguegamefinder uses season_id; rename to match PLAYER_GAME_LOGS
    if 'season_id' in df.columns:
        df = df.rename(columns={'season_id': 'season_year'})

    name_map = os.getenv('NAME_REPLACE')
    if name_map:
        df = df.replace(json.loads(name_map))

    df['GameKey'] = df['game_date'].astype(str) + '-' + df['team_abbreviation'].astype(str)

    print(f"  Pulled {len(df)} preseason rows for {df['player_name'].nunique()} players.")
    return df


def get_existing_keys(engine) -> set:
    with engine.connect() as conn:
        result = conn.execute(
            text('SELECT game_id::text, player_id::text FROM wnba."HISTORICAL_PLAYER_GAME_LOGS"')
        )
        return {(row[0], row[1]) for row in result}


def main():
    df = pull_preseason_logs()

    if df.empty:
        print("No preseason data returned. Exiting.")
        sys.exit(0)

    engine = get_db_engine()
    existing = get_existing_keys(engine)
    print(f"  {len(existing)} existing rows in HISTORICAL_PLAYER_GAME_LOGS.")

    new_rows = df[
        ~df.apply(lambda r: (str(r['game_id']), str(r['player_id'])) in existing, axis=1)
    ]

    print(f"  {len(new_rows)} new preseason rows to insert (skipping {len(df) - len(new_rows)} already present).")

    if new_rows.empty:
        print("Nothing to insert.")
        return

    new_rows.to_sql(
        name='HISTORICAL_PLAYER_GAME_LOGS',
        schema='wnba',
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=500,
    )
    print(f"Done. Inserted {len(new_rows)} preseason rows into wnba.HISTORICAL_PLAYER_GAME_LOGS.")


if __name__ == '__main__':
    main()
