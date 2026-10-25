"""
collect_data.py — daily update de los juegos activos.

Re-consulta los juegos 'active' que toca actualizar según su nivel de popularidad
(ver TIER_INTERVAL_DAYS en pipeline/config.py): primero los populares y, dentro de
cada nivel, el más atrasado primero, hasta agotar el presupuesto de tiempo.
Lo que no alcanza queda para la siguiente corrida.

Uso local:
    python collect_data.py
    MAX_RUNTIME_MIN=10 python collect_data.py   # corrida corta de prueba
"""

import pandas as pd

from pipeline.config import (
    POPULAR_TOP_N, MID_TOP_N, TIER_INTERVAL_DAYS, TIER_ORDER, HISTORY_OVERLAP_DAYS, env_int,
)
from pipeline.itad import fetch_game
from pipeline.runner import run_games
from pipeline.storage import (
    load_tracked, latest_popularity, build_history_rows, build_snapshot_row,
)

MAX_RUNTIME_MIN = env_int("MAX_RUNTIME_MIN", 300)


def assign_tiers(active):
    """Nivel de popularidad de cada juego activo (Series alineada con `active`)."""
    pop = latest_popularity().reindex(active["appid"].to_numpy())
    pop.index = active.index

    pct = lambda col: pop[col].rank(pct=True).fillna(0)
    score = (pct("itad_waitlisted") + pct("steam_count")) / 2
    position = score.rank(ascending=False, method="first")

    tier = pd.Series("cola", index=active.index)
    tier[(position <= MID_TOP_N) | (pop["players_recent"].fillna(0) > 0)] = "medio"
    tier[position <= POPULAR_TOP_N] = "popular"
    return tier


def select_due(tracked):
    active = tracked[tracked["status"] == "active"].copy()
    active["_last"] = pd.to_datetime(active["last_updated"], utc=True, errors="coerce", format="ISO8601")
    active["_tier"] = assign_tiers(active)

    interval_days = active["_tier"].map(TIER_INTERVAL_DAYS)
    cutoff = pd.Timestamp.now(tz="UTC") - pd.to_timedelta(interval_days, unit="D")
    due = active[active["_last"].isna() | (active["_last"] < cutoff)].copy()

    # Primero los populares; dentro de cada nivel, el más atrasado primero.
    due["_tier_order"] = due["_tier"].map({t: i for i, t in enumerate(TIER_ORDER)})
    due = due.sort_values(["_tier_order", "_last"], na_position="first")
    return due, active


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
    for tier in TIER_ORDER:
        print(f"  {tier:8} {(active['_tier'] == tier).sum():>7} activos, "
              f"{(due['_tier'] == tier).sum():>7} toca actualizar "
              f"(cada {TIER_INTERVAL_DAYS[tier]} días)")
    print(f"{len(active)} juegos activos, {len(due)} toca actualizar. "
          f"Presupuesto: {MAX_RUNTIME_MIN} min.")
    if due.empty:
        print("Todo al día.")
        return
    run_games(tracked, due, process_one, MAX_RUNTIME_MIN)


if __name__ == "__main__":
    main()
