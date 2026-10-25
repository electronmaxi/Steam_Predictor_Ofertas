"""
collect_data.py — daily update de los juegos activos.

Re-consulta los juegos 'active' que toca actualizar (7 días si tienen jugadores,
30 si no), del más atrasado al más reciente, hasta agotar el presupuesto de tiempo.
Lo que no alcanza queda para la siguiente corrida.

Uso local:
    python collect_data.py
    MAX_RUNTIME_MIN=10 python collect_data.py   # corrida corta de prueba
"""

import pandas as pd

from pipeline.config import (
    ACTIVE_INTERVAL_DAYS, IDLE_INTERVAL_DAYS, HISTORY_OVERLAP_DAYS, env_int,
)
from pipeline.itad import fetch_game
from pipeline.runner import run_games
from pipeline.storage import (
    load_tracked, latest_players_recent, build_history_rows, build_snapshot_row,
)

MAX_RUNTIME_MIN = env_int("MAX_RUNTIME_MIN", 300)


def select_due(tracked):
    active = tracked[tracked["status"] == "active"].copy()
    last = pd.to_datetime(active["last_updated"], utc=True, errors="coerce", format="ISO8601")

    players = active["appid"].map(latest_players_recent()).fillna(0)
    interval_days = players.gt(0).map({True: ACTIVE_INTERVAL_DAYS, False: IDLE_INTERVAL_DAYS})
    cutoff = pd.Timestamp.now(tz="UTC") - pd.to_timedelta(interval_days, unit="D")

    active["_last"] = last
    due = active[last.isna() | (last < cutoff)]
    return due.sort_values("_last", na_position="first"), active


def process_one(row):
    since = None
    if pd.notna(row["history_synced_at"]):
        synced = pd.Timestamp(row["history_synced_at"]) - pd.Timedelta(days=HISTORY_OVERLAP_DAYS)
        since = synced.strftime("%Y-%m-%dT%H:%M:%SZ")

    game_id = row["game_id"] if pd.notna(row["game_id"]) else None
    result = fetch_game(row["appid"], game_id=game_id, since=since)

    now = pd.Timestamp.now(tz="UTC").isoformat()
    updates = {"last_updated": now, "history_synced_at": now, "game_id": result["game_id"]}
    return build_history_rows(result), build_snapshot_row(result), updates


def main():
    tracked = load_tracked()
    due, active = select_due(tracked)
    print(f"{len(active)} juegos activos, {len(due)} toca actualizar. "
          f"Presupuesto: {MAX_RUNTIME_MIN} min.")
    if due.empty:
        print("Todo al día.")
        return
    run_games(tracked, due, process_one, MAX_RUNTIME_MIN)


if __name__ == "__main__":
    main()
