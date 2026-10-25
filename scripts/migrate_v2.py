"""
migrate_v2.py — correr UNA vez, desde la raíz del repo:

    python -m scripts.migrate_v2

1. Re-particiona price_history de shard_XX.csv (appid % 20, todo caía en 2 archivos)
   a shard_XXX.csv (ver pipeline.storage.shard_id), sin duplicados.
2. Agrega a tracked_games las columnas game_id (sacado de snapshots, para no repetir
   el lookup en cada update) e history_synced_at (hasta cuándo hay historial guardado;
   vacío = el próximo update baja el historial completo).

Es idempotente: si ya no hay shards viejos, el paso 1 no hace nada.
"""

import glob
import os

import pandas as pd

from pipeline.config import PRICE_HISTORY_DIR, SNAPSHOTS_PATH
from pipeline.storage import (
    HISTORY_COLUMNS, _clean_history, _normalize_history, atomic_write_csv,
    shard_id, shard_path, load_tracked, save_tracked, load_price_history,
)


def reshard():
    old_paths = sorted(glob.glob(os.path.join(PRICE_HISTORY_DIR, "shard_[0-9][0-9].csv")))
    if not old_paths:
        print("No hay shards viejos, nada que re-particionar.")
        return

    df = pd.concat([pd.read_csv(p) for p in old_paths], ignore_index=True)
    df = _clean_history(_normalize_history(df))
    print(f"{len(old_paths)} shards viejos: {len(df)} filas, {df['appid'].nunique()} juegos.")

    for sid, group in df.groupby(df["appid"].map(shard_id)):
        atomic_write_csv(group[HISTORY_COLUMNS], shard_path(sid))
    for p in old_paths:
        os.remove(p)
    print(f"Escritos {df['appid'].map(shard_id).nunique()} shards nuevos.")


def add_tracked_columns():
    tracked = load_tracked()

    snaps = pd.read_csv(SNAPSHOTS_PATH, usecols=["appid", "game_id", "date_checked"])
    game_ids = snaps.sort_values("date_checked").groupby("appid")["game_id"].last()
    missing = tracked["game_id"].isna()
    tracked.loc[missing, "game_id"] = tracked.loc[missing, "appid"].map(game_ids)

    history = load_price_history()
    if not history.empty:
        # Hasta donde llega lo guardado: el último timestamp de todo el historial.
        synced_until = history["timestamp"].max().isoformat()
        has_history = tracked["appid"].isin(history["appid"].unique())
        no_sync = tracked["history_synced_at"].isna()
        tracked.loc[has_history & no_sync, "history_synced_at"] = synced_until

    save_tracked(tracked)
    active = tracked["status"] == "active"
    print(f"tracked_games: {tracked.loc[active, 'game_id'].notna().sum()} activos con game_id, "
          f"{tracked.loc[active, 'history_synced_at'].isna().sum()} activos sin historial "
          f"(se bajará completo en el próximo update).")


if __name__ == "__main__":
    reshard()
    add_tracked_columns()
