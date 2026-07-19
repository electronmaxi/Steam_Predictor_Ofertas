"""
Steam Price Tracker - Recolector de datos vía IsThereAnyDeal API

Uso local:
    python collect_data.py

La API key se lee de la variable de entorno ITAD_API_KEY.
- En local: usa python-dotenv para cargarla desde keys.env
- En GitHub Actions: se inyecta directamente como secret, sin necesidad de dotenv
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone

# --- Cargar API key ---
# Intenta cargar desde archivo .env local si existe (no falla si no está, ej. en CI)
try:
    from dotenv import load_dotenv
    load_dotenv("keys.env")
except ImportError:
    pass

API_KEY = os.getenv("ITAD_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "No se encontró ITAD_API_KEY. "
        "En local: revisa tu archivo keys.env. "
        "En GitHub Actions: revisa que el secret ITAD_API_KEY esté configurado."
    )

BASE = "https://api.isthereanydeal.com"
DATA_DIR = "data"
PRICE_HISTORY_PATH = os.path.join(DATA_DIR, "price_history.csv")
SNAPSHOTS_PATH = os.path.join(DATA_DIR, "snapshots.csv")

TRACKED_PATH = os.path.join(DATA_DIR, "tracked_games.csv")


class RateLimitReached(Exception):
    """Se lanza cuando ITAD responde 429. El caller debe detener el loop,
    guardar el progreso, y dejar que la siguiente ejecución continúe."""
    pass


def itad_get(url, params):
    """Wrapper simple de requests.get. Si hay 429, lanza RateLimitReached
    para que el loop principal pare limpio en vez de reintentar o fallar."""
    r = requests.get(url, params=params)
    if r.status_code == 429:
        raise RateLimitReached(f"Rate limit alcanzado en {url}")
    r.raise_for_status()
    return r


def get_game_data(appid, since="2015-01-01T00:00:00Z", country="us"):
    """Trae lookup + history + info para un appid de Steam."""

    r = itad_get(f"{BASE}/games/lookup/v1",
                 {"key": API_KEY, "appid": appid})
    lookup = r.json()
    if not lookup.get("found"):
        raise ValueError(f"Juego con appid {appid} no encontrado en ITAD")
    game_id = lookup["game"]["id"]
    title = lookup["game"]["title"]

    r = itad_get(f"{BASE}/games/history/v2",
                 {"key": API_KEY, "id": game_id, "shops": 61,
                  "since": since, "country": country})
    history = r.json()

    r = itad_get(f"{BASE}/games/info/v2",
                 {"key": API_KEY, "id": game_id})
    info = r.json()

    return {"appid": appid, "game_id": game_id, "title": title,
            "history": history, "info": info}


def save_price_history(result):
    """Guarda el histórico de precios sin duplicar filas ya existentes."""
    appid = result["appid"]
    game_id = result["game_id"]
    title = result["title"]

    df_new = pd.DataFrame([{
        "appid": appid,
        "game_id": game_id,
        "title": title,
        "timestamp": h["timestamp"],
        "shop": h["shop"]["name"],
        "price": h["deal"]["price"]["amount"],
        "regular": h["deal"]["regular"]["amount"],
        "discount_pct": h["deal"]["cut"]
    } for h in result["history"]])

    if df_new.empty:
        print(f"[{title}] Sin registros de historial nuevos para guardar.")
        return

    df_new["timestamp"] = pd.to_datetime(df_new["timestamp"], utc=True)

    if os.path.exists(PRICE_HISTORY_PATH):
        df_old = pd.read_csv(PRICE_HISTORY_PATH, parse_dates=["timestamp"])
        df_old["timestamp"] = pd.to_datetime(df_old["timestamp"], utc=True)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all = df_all.drop_duplicates(subset=["appid", "timestamp", "shop"])
    df_all = df_all.sort_values(["appid", "timestamp"]).reset_index(drop=True)

    df_all.to_csv(PRICE_HISTORY_PATH, index=False)
    print(f"[{title}] Historial actualizado. Total filas en archivo: {len(df_all)}")


def save_snapshot(result):
    """Guarda un snapshot diario de metadata (jugadores, reviews, rank, etc.)."""
    info = result["info"]
    today = datetime.now(timezone.utc).date().isoformat()

    row = {
        "appid": info["appid"],
        "game_id": info["id"],
        "title": info["title"],
        "date_checked": today,
        "release_date": info.get("releaseDate"),
        "tags": ", ".join(info.get("tags", [])),
        "developers": ", ".join(d["name"] for d in info.get("developers", [])),
        "players_recent": info.get("players", {}).get("recent"),
        "players_day": info.get("players", {}).get("day"),
        "players_week": info.get("players", {}).get("week"),
        "players_peak": info.get("players", {}).get("peak"),
        "itad_rank": info.get("stats", {}).get("rank"),
        "itad_waitlisted": info.get("stats", {}).get("waitlisted"),
        "itad_collected": info.get("stats", {}).get("collected"),
    }

    for review in info.get("reviews", []):
        source = review["source"].lower().replace(" ", "_")
        row[f"{source}_score"] = review["score"]
        row[f"{source}_count"] = review["count"]

    df_new = pd.DataFrame([row])

    if os.path.exists(SNAPSHOTS_PATH):
        df_old = pd.read_csv(SNAPSHOTS_PATH)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all = df_all.drop_duplicates(subset=["appid", "date_checked"], keep="last")
    df_all = df_all.sort_values(["appid", "date_checked"]).reset_index(drop=True)

    df_all.to_csv(SNAPSHOTS_PATH, index=False)
    print(f"[{info['title']}] Snapshot de {today} guardado. Total filas en archivo: {len(df_all)}")


def get_active_games_sorted():
    """Devuelve el dataframe completo y la sublista de juegos activos,
    ordenados por last_updated ascendente (nunca actualizados primero)."""
    if not os.path.exists(TRACKED_PATH):
        print(f"No se encontró {TRACKED_PATH}. Corre build_appid_pool.py y onboard_batch.py primero.")
        return None, pd.DataFrame()

    df = pd.read_csv(TRACKED_PATH, dtype={"appid": int})

    if "last_updated" not in df.columns:
        df["last_updated"] = pd.NA

    active = df[df["status"] == "active"].copy()
    # Los que nunca se han actualizado (NaN) quedan primero, luego los más antiguos
    active["_sort_key"] = pd.to_datetime(active["last_updated"], utc=True, errors="coerce")
    active = active.sort_values("_sort_key", na_position="first")

    return df, active


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    df, active = get_active_games_sorted()

    if active.empty:
        print("No hay juegos activos para actualizar todavía.")
        return

    print(f"{len(active)} juegos activos en total. Actualizando en orden de antigüedad...")

    updated_count = 0
    for idx, row in active.iterrows():
        appid = row["appid"]
        try:
            result = get_game_data(appid)
            save_price_history(result)
            save_snapshot(result)
            df.loc[df["appid"] == appid, "last_updated"] = pd.Timestamp.now(tz="UTC").isoformat()
            updated_count += 1
        except RateLimitReached:
            print(f"Rate limit alcanzado tras actualizar {updated_count} juegos. "
                  f"Guardando progreso, el resto sigue en la próxima ejecución.")
            break
        except Exception as e:
            print(f"Error procesando appid {appid}: {e}")
            # no se detiene el loop por errores puntuales (juego eliminado, etc.)
        time.sleep(0.3)

    df.to_csv(TRACKED_PATH, index=False)
    print(f"Corrida terminada. Juegos actualizados esta vez: {updated_count} / {len(active)}")


if __name__ == "__main__":
    main()