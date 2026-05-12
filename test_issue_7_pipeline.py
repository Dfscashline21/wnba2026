import pandas as pd

import main as main_module


class DummyConnection:
    def execute(self, _):
        return None


class DummyEngine:
    def begin(self):
        return self

    def __enter__(self):
        return DummyConnection()

    def __exit__(self, exc_type, exc, tb):
        return False


class DummyProcess:
    def __init__(self, events):
        self.stdout = ["dbt run complete\n"]
        self.returncode = 0
        events.append("dbt_started")

    def wait(self):
        return 0


def run_main_with_mocks(monkeypatch, dk_mode):
    events = []
    writes = {}

    monkeypatch.setattr(main_module, "load_dotenv", lambda: None)
    monkeypatch.setattr(main_module, "trifecta_tweet", lambda: None)
    monkeypatch.setattr(main_module, "create_dbt_profile", lambda: True)
    monkeypatch.setattr(main_module, "get_db_engine", lambda: DummyEngine())

    monkeypatch.setenv("TEAM_REPLACE", "{}")
    monkeypatch.setenv("NAME_REPLACE", "{}")

    def fake_read_csv(path, *args, **kwargs):
        path_str = str(path)
        if path_str.endswith("mincheckproj.csv"):
            return pd.DataFrame({"Name": ["A"], "min": [30]})
        if path_str.endswith("wnbadk.csv"):
            return pd.DataFrame({"TeamAbbrev": ["LVA"]})
        if path_str.endswith("wnba_team list.csv"):
            return pd.DataFrame({"TeamAbbrev": ["LVA"]})
        raise AssertionError(f"Unexpected read_csv path: {path_str}")

    monkeypatch.setattr(main_module.pd, "read_csv", fake_read_csv)

    monkeypatch.setattr(main_module.rp, "rotomins", lambda: pd.DataFrame({"Name": ["A"], "TeamAbbrev": ["LVA"], "min": [30]}))
    monkeypatch.setattr(main_module.rp, "depth_chart_roster", lambda: pd.DataFrame({"Name": ["A"], "TeamAbbrev": ["LVA"]}))
    monkeypatch.setattr(main_module.nb, "get_schedule", lambda: pd.DataFrame({"Home_abb": ["LVA"], "Away_abb": ["SEA"]}))
    monkeypatch.setattr(
        main_module.nb,
        "pull_game_logs",
        lambda: (
            pd.DataFrame(
                {
                    "game_date": ["2026-05-10"],
                    "team_abbreviation": ["LVA"],
                    "player_name": ["A"],
                    "min": [31],
                    "player_id": [101],
                }
            ),
            [101],
        ),
    )
    monkeypatch.setattr(main_module.nb, "adv_stats", lambda: pd.DataFrame({"TEAM_ID": [1], "TEAM_NAME": ["Aces"], "PACE": [98.0]}))
    monkeypatch.setattr(main_module.nb, "player_pos", lambda: pd.DataFrame({"Name": ["A"], "Position": ["G"]}))
    monkeypatch.setattr(
        main_module.inj,
        "get_adjusted_rates",
        lambda players_today: (players_today.assign(boost=1.0), pd.DataFrame({"Name": []})),
    )
    monkeypatch.setattr(main_module.ud, "udprops", lambda: pd.DataFrame())
    monkeypatch.setattr(main_module.pp, "prizepicks_pull", lambda: pd.DataFrame())
    monkeypatch.setattr(main_module.sb, "pull_mgm", lambda: pd.DataFrame())

    if dk_mode == "available":
        monkeypatch.setattr(
            main_module.dk,
            "get_dk",
            lambda: pd.DataFrame(
                {
                    "Position": ["G"],
                    "Name + ID": ["A (101)"],
                    "Name": ["A"],
                    "ID": [101],
                    "Salary": [9000],
                    "Game Info": ["LVA@SEA 05/12/2026 07:00PM ET"],
                    "TeamAbbrev": ["LVA"],
                    "AvgPointsPerGame": [36.5],
                }
            ),
        )
    else:
        monkeypatch.setattr(main_module.dk, "get_dk", lambda: (_ for _ in ()).throw(RuntimeError("No DK slate")))

    def fake_to_sql(self, name, schema, con, if_exists, index):
        events.append(f"to_sql:{name}")
        writes[name] = self.copy()

    monkeypatch.setattr(pd.DataFrame, "to_sql", fake_to_sql, raising=False)

    def fake_popen(*args, **kwargs):
        return DummyProcess(events)

    monkeypatch.setattr(main_module.subprocess, "Popen", fake_popen)

    main_module.main()
    return events, writes


def test_issue_7_pipeline_with_dk_available(monkeypatch):
    events, writes = run_main_with_mocks(monkeypatch, "available")
    assert "players_today" in writes
    assert "draftkings" in writes
    assert not writes["players_today"].empty
    assert not writes["draftkings"].empty
    assert events.index("to_sql:players_today") < events.index("dbt_started")


def test_issue_7_pipeline_with_dk_unavailable(monkeypatch):
    events, writes = run_main_with_mocks(monkeypatch, "unavailable")
    assert "players_today" in writes
    assert "draftkings" in writes
    assert not writes["players_today"].empty
    assert writes["draftkings"].empty
    assert list(writes["draftkings"].columns) == [
        "Position",
        "Name + ID",
        "Name",
        "ID",
        "Salary",
        "Game Info",
        "TeamAbbrev",
        "AvgPointsPerGame",
        "min_x",
        "min_y",
        "opponent",
    ]
    assert events.index("to_sql:players_today") < events.index("dbt_started")
