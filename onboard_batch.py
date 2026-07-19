"""
onboard_batch.py — toma el siguiente lote de appids "pending" en tracked_games.csv,
verifica que existan en ITAD, y hace su primera carga de precios + snapshot.

Uso local:
    python onboard_batch.py
"""

import os
import time
import pandas as pd

from collect_data import get_game_data, save_price_history, save_snapshot, DATA_DIR, RateLimitReached

TRACKED_PATH = os.path.join(DATA_DIR, "tracked_games.csv")
BATCH_SIZE = 300


def onboard_next_batch():
    if not os.path.exists(TRACKED_PATH):
        raise RuntimeError(
            f"No se encontró {TRACKED_PATH}. Corre build_appid_pool.py primero."
        )

    df = pd.read_csv(TRACKED_PATH, dtype={"appid": int})
    if "last_updated" not in df.columns:
        df["last_updated"] = pd.NA
    # Forzar dtype object en columnas de texto/fecha para evitar errores
    # al asignar strings sobre columnas que pandas infiere como float64
    # cuando están vacías (todo NaN)
    for col in ["date_added", "last_updated"]:
        df[col] = df[col].astype("object")
    pending = df[df["status"] == "pending"].head(BATCH_SIZE)

    if pending.empty:
        print("No quedan appids pendientes. Onboarding completo.")
        return

    print(f"Procesando lote de {len(pending)} appids...")

    processed = 0
    for idx, row in pending.iterrows():
        appid = row["appid"]
        try:
            result = get_game_data(appid)
            save_price_history(result)
            save_snapshot(result)
            df.loc[idx, "status"] = "active"
            df.loc[idx, "date_added"] = pd.Timestamp.now(tz="UTC").date().isoformat()
            df.loc[idx, "last_updated"] = pd.Timestamp.now(tz="UTC").isoformat()
            processed += 1
        except RateLimitReached:
            print(f"Rate limit alcanzado tras procesar {processed} juegos de este lote. "
                  f"Guardando progreso, el resto sigue en la próxima ejecución.")
            break
        except Exception as e:
            df.loc[idx, "status"] = "not_found"
            print(f"  appid {appid} descartado: {e}")
        time.sleep(0.3)

    df.to_csv(TRACKED_PATH, index=False)

    active_count = (df["status"] == "active").sum()
    not_found_count = (df["status"] == "not_found").sum()
    pending_count = (df["status"] == "pending").sum()
    print(f"Lote terminado. Procesados esta corrida: {processed}")
    print(f"  Activos: {active_count} | Descartados: {not_found_count} | Pendientes restantes: {pending_count}")


if __name__ == "__main__":
    onboard_next_batch()