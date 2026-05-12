"""
Individual pull tests — run the whole file or comment out what you don't need.
Usage: python test_pulls.py
"""
import sys, io
if hasattr(sys.stdout, 'buffer') and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

from dotenv import load_dotenv
load_dotenv()

import pandas as pd

results = {}

def run(name, fn):
    print(f"\n{'='*50}")
    print(f"TEST: {name}")
    print('='*50)
    try:
        result = fn()
        if isinstance(result, tuple):
            for i, r in enumerate(result):
                if isinstance(r, pd.DataFrame):
                    print(f"  [{i}] DataFrame: {r.shape}  cols: {list(r.columns)}")
                    print(r.head(3).to_string())
                else:
                    print(f"  [{i}] {type(r).__name__}: {r}")
        elif isinstance(result, pd.DataFrame):
            print(f"  DataFrame: {result.shape}  cols: {list(result.columns)}")
            print(result.head(3).to_string())
        else:
            print(f"  Result: {result}")
        results[name] = 'PASS'
    except Exception as e:
        import traceback
        print(f"  FAIL: {e}")
        traceback.print_exc()
        results[name] = f'FAIL: {e}'


# ── Pullers ────────────────────────────────────────────────────────────────────

from pullers import roto_pull as rp
run("roto_pull.rotomins", rp.rotomins)

from pullers import dk_pull as dk
run("dk_pull.get_dk", dk.get_dk)

from pullers import ud_pull as ud
run("ud_pull.udprops", ud.udprops)

from pullers import pp_pull as pp
run("pp_pull.prizepicks_pull", pp.prizepicks_pull)

from pullers import sportbooks_pull as sb
run("sportbooks_pull.pull_mgm", sb.pull_mgm)

from pullers import betonline_pull as bo
run("betonline_pull.pull_betonline", bo.pull_betonline)

from pullers import caesars_pull as cs
run("caesars_pull.pull_caesars", cs.pull_caesars)

def _dk_alternates():
    import os
    from pullers import dk_alternates_pull as dka
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise RuntimeError("ODDS_API_KEY not set — skipping dk_alternates")
    client = dka.WNBAPlayerPropsAPI(api_key)
    events = client.get_events()
    return f"{len(events)} events found"
run("dk_alternates_pull.get_events", _dk_alternates)

from pullers import nbastats as nb
run("nbastats.get_schedule", nb.get_schedule)
run("nbastats.adv_stats", nb.adv_stats)
run("nbastats.player_pos", nb.player_pos)

def _pull_game_logs():
    return nb.pull_game_logs()
run("nbastats.pull_game_logs", _pull_game_logs)

# ── Modeling ───────────────────────────────────────────────────────────────────

from pathlib import Path
from modeling import injuryadjustments as inj

def _inj():
    dksalaries = pd.read_csv(Path(__file__).parent / 'wnbadk.csv')
    players_today = dksalaries[['Name', 'TeamAbbrev']].drop_duplicates(subset=['Name', 'TeamAbbrev'])
    return inj.get_adjusted_rates(players_today)
run("injuryadjustments.get_adjusted_rates", _inj)


# ── Summary ────────────────────────────────────────────────────────────────────

print(f"\n{'='*50}")
print("SUMMARY")
print('='*50)
for name, status in results.items():
    icon = 'OK' if status == 'PASS' else 'XX'
    print(f"  [{icon}] {name}: {status}")
