"""
migrate_to_shards.py — corre UNA SOLA VEZ para convertir el price_history.csv
monolítico actual en el nuevo formato particionado (data/price_history/shard_XX.csv).

Después de correr esto y confirmar que los shards quedaron bien,
BORRA manualmente el archivo viejo data/price_history.csv (o lo hace este script).

Uso local:
    python migrate_to_shards.py
"""

import os
import pandas as pd

from collect_data import DATA_DIR, PRICE_HISTORY_DIR, NUM_SHARDS

OLD_PATH = os.path.join(DATA_DIR, "price_history.csv")


def migrate():
    if not os.path.exists(OLD_PATH):
        print(f"No se encontró {OLD_PATH}. Nada que migrar (¿ya migraste antes?).")
        return

    print(f"Leyendo {OLD_PATH}...")
    df = pd.read_csv(OLD_PATH, parse_dates=["timestamp"])
    print(f"Total filas a migrar: {len(df)}")

    os.makedirs(PRICE_HISTORY_DIR, exist_ok=True)
    df["_shard"] = df["appid"] % NUM_SHARDS

    for shard_id, group in df.groupby("_shard"):
        group = group.drop(columns="_shard")
        path = os.path.join(PRICE_HISTORY_DIR, f"shard_{shard_id:02d}.csv")
        group = group.sort_values(["appid", "timestamp"]).reset_index(drop=True)
        group.to_csv(path, index=False)
        print(f"  shard_{shard_id:02d}.csv: {len(group)} filas")

    print("Migración completa.")
    print(f"Ahora BORRA manualmente el archivo viejo: {OLD_PATH}")
    print("(ej. 'git rm data/price_history.csv' para que también se quite del repo)")


if __name__ == "__main__":
    migrate()