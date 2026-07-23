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

# Cada cuántos días se re-consulta un juego ya activo.
# El histórico de precios NO pierde datos por consultar menos seguido
# (ITAD ya lo registra con su propio timestamp); lo único que pierde
# resolución es el snapshot de jugadores/reviews del día.
UPDATE_INTERVAL_DAYS = 7


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


# --- Funciones "build": arman las filas en memoria, SIN tocar disco ---

def build_history_rows(result):
    appid = result["appid"]
    game_id = result["game_id"]
    title = result["title"]

    return [{
        "appid": appid,
        "game_id": game_id,
        "title": title,
        "timestamp": h["timestamp"],
        "shop": h["shop"]["name"],
        "price": h["deal"]["price"]["amount"],
        "regular": h["deal"]["regular"]["amount"],
        "discount_pct": h["deal"]["cut"]
    } for h in result["history"]]


def build_snapshot_row(result):
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

    return row


# --- Funciones "write bulk": leen el CSV UNA sola vez, agregan todo, guardan UNA sola vez ---

def write_price_history_bulk(new_rows):
    """Escribe todas las filas nuevas de historial en una sola operación de IO."""
    if not new_rows:
        print("Sin registros nuevos de historial para guardar.")
        return

    df_new = pd.DataFrame(new_rows)
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
    print(f"price_history.csv actualizado. Total filas: {len(df_all)}")


def write_snapshots_bulk(new_rows):
    """Escribe todos los snapshots nuevos en una sola operación de IO."""
    if not new_rows:
        print("Sin snapshots nuevos para guardar.")
        return

    df_new = pd.DataFrame(new_rows)

    if os.path.exists(SNAPSHOTS_PATH):
        df_old = pd.read_csv(SNAPSHOTS_PATH)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all = df_all.drop_duplicates(subset=["appid", "date_checked"], keep="last")
    df_all = df_all.sort_values(["appid", "date_checked"]).reset_index(drop=True)

    df_all.to_csv(SNAPSHOTS_PATH, index=False)
    print(f"snapshots.csv actualizado. Total filas: {len(df_all)}")


def get_active_games_sorted():
    """Devuelve el dataframe completo y la sublista de juegos activos,
    ordenados por last_updated ascendente (nunca actualizados primero)."""
    if not os.path.exists(TRACKED_PATH):
        print(f"No se encontró {TRACKED_PATH}. Corre build_appid_pool.py y onboard_batch.py primero.")
        return None, pd.DataFrame()

    df = pd.read_csv(TRACKED_PATH, dtype={"appid": int})

    if "last_updated" not in df.columns:
        df["last_updated"] = pd.NA
    df["last_updated"] = df["last_updated"].astype("object")

    active = df[df["status"] == "active"].copy()
    active["_sort_key"] = pd.to_datetime(active["last_updated"], utc=True, errors="coerce")
    active = active.sort_values("_sort_key", na_position="first")

    return df, active


def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    df, active = get_active_games_sorted()

    if active.empty:
        print("No hay juegos activos para actualizar todavía.")
        return

    # Filtro: solo los que nunca se actualizaron o superan el intervalo definido
    cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=UPDATE_INTERVAL_DAYS)
    due = active[active["_sort_key"].isna() | (active["_sort_key"] < cutoff)]

    print(f"{len(active)} juegos activos en total. "
          f"{len(due)} necesitan actualización (sin datos hace más de {UPDATE_INTERVAL_DAYS} días).")

    if due.empty:
        print("Todos los juegos activos están al día. Nada que hacer por ahora.")
        return

    history_rows = []
    snapshot_rows = []
    updated_count = 0
    rate_limited = False

    for idx, row in due.iterrows():
        appid = row["appid"]
        try:
            result = get_game_data(appid)
            history_rows.extend(build_history_rows(result))
            snapshot_rows.append(build_snapshot_row(result))
            df.loc[df["appid"] == appid, "last_updated"] = pd.Timestamp.now(tz="UTC").isoformat()
            updated_count += 1
        except RateLimitReached:
            print(f"Rate limit alcanzado tras actualizar {updated_count} juegos. "
                  f"Guardando progreso, el resto sigue en la próxima ejecución.")
            rate_limited = True
            break
        except Exception as e:
            print(f"Error procesando appid {appid}: {e}")
        time.sleep(0.3)

    # Guardado en bloque, UNA sola vez, sin importar cuántos juegos se procesaron
    write_price_history_bulk(history_rows)
    write_snapshots_bulk(snapshot_rows)
    df.to_csv(TRACKED_PATH, index=False)

    print(f"Corrida terminada. Juegos actualizados esta vez: {updated_count} / {len(due)} pendientes de actualizar"
          f"{' (detenido por rate limit)' if rate_limited else ''}")


if __name__ == "__main__":
    main()