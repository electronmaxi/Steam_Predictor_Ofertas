"""
onboard_batch.py — toma los siguientes appids 'pending' de tracked_games.csv,
verifica que existan en ITAD y hace su primera carga (historial completo + snapshot).

Procesa hasta BATCH_SIZE juegos o hasta agotar el presupuesto de tiempo, lo que pase antes.

Uso local:
    python onboard_batch.py
"""

import pandas as pd

from pipeline.config import env_int
from pipeline.itad import fetch_game
from pipeline.runner import run_games
from pipeline.storage import load_tracked, build_history_rows, build_snapshot_row

BATCH_SIZE = env_int("BATCH_SIZE", 10000)
MAX_RUNTIME_MIN = env_int("MAX_RUNTIME_MIN", 150)


def process_one(row):
    result = fetch_game(row["appid"])
    now = pd.Timestamp.now(tz="UTC")
    updates = {
        "status": "active",
        "date_added": now.date().isoformat(),
        "last_updated": now.isoformat(),
        "history_synced_at": now.isoformat(),
        "game_id": result["game_id"],
    }
    return build_history_rows(result), build_snapshot_row(result), updates


def main():
    tracked = load_tracked()
    pending = tracked[tracked["status"] == "pending"].head(BATCH_SIZE)
    if pending.empty:
        print("No quedan appids pendientes. Onboarding completo.")
        return

    print(f"Onboarding de hasta {len(pending)} appids. Presupuesto: {MAX_RUNTIME_MIN} min.")
    # Solo 'no existe en ITAD' descarta el juego; un error de red lo deja pending para reintentar.
    run_games(tracked, pending, process_one, MAX_RUNTIME_MIN,
              on_not_found=lambda row: {"status": "not_found"})

    counts = tracked["status"].value_counts()
    print(f"Activos: {counts.get('active', 0)} | Descartados: {counts.get('not_found', 0)} | "
          f"Pendientes: {counts.get('pending', 0)}")


if __name__ == "__main__":
    main()
