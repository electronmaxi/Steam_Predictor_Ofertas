"""
Lectura/escritura de los CSV de data/.

Reglas para que un corte a mitad de corrida no deje nada roto:
- Los archivos que se reescriben enteros (tracked, snapshots) se escriben a un .tmp
  y se reemplazan con os.replace, que es atómico.
- price_history es solo-append: no se relee al guardar, así cada checkpoint es barato.
  Puede quedar alguna fila duplicada (re-consultas con solape); se limpia al leer
  con load_price_history() o con compact_price_history().
- Orden de guardado: price_history → snapshots → tracked. Si algo se corta en medio,
  lo peor que pasa es que el juego se vuelve a procesar en la próxima corrida.
"""

import glob
import os
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import (
    TRACKED_PATH, SNAPSHOTS_PATH, PRICE_HISTORY_DIR, NUM_SHARDS,
)

HISTORY_COLUMNS = ["appid", "game_id", "title", "timestamp", "shop",
                   "price", "regular", "discount_pct"]
HISTORY_KEY = ["appid", "timestamp", "shop"]

TRACKED_OPTIONAL_COLUMNS = ["date_added", "last_updated", "game_id", "history_synced_at"]


def atomic_write_csv(df, path):
    tmp = path + ".tmp"
    df.to_csv(tmp, index=False)
    os.replace(tmp, path)


# --- tracked_games ---

def load_tracked():
    df = pd.read_csv(TRACKED_PATH, dtype={"appid": int}, low_memory=False)
    for col in TRACKED_OPTIONAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype("object")
    return df


def save_tracked(df):
    atomic_write_csv(df, TRACKED_PATH)


# --- filas a partir de la respuesta de ITAD ---

def build_history_rows(result):
    since = result.get("since")
    since_ts = pd.Timestamp(since) if since else None

    rows = []
    for h in result["history"]:
        # Con `since`, ITAD puede devolver una fila con el estado justo en ese instante;
        # se descarta para no meter filas artificiales en cada re-consulta.
        if since_ts is not None and pd.Timestamp(h["timestamp"]) <= since_ts:
            continue
        rows.append({
            "appid": result["appid"],
            "game_id": result["game_id"],
            "title": result["title"],
            "timestamp": h["timestamp"],
            "shop": h["shop"]["name"],
            "price": h["deal"]["price"]["amount"],
            "regular": h["deal"]["regular"]["amount"],
            "discount_pct": h["deal"]["cut"],
        })
    return rows


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


# --- price_history (shards solo-append) ---

def shard_id(appid):
    return (appid // 10) % NUM_SHARDS


def shard_path(sid):
    return os.path.join(PRICE_HISTORY_DIR, f"shard_{sid:03d}.csv")


def _normalize_history(df):
    df = df[HISTORY_COLUMNS].copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="ISO8601")
    return df


def append_price_history(rows):
    if not rows:
        return
    os.makedirs(PRICE_HISTORY_DIR, exist_ok=True)
    df = _normalize_history(pd.DataFrame(rows))
    for sid, group in df.groupby(df["appid"].map(shard_id)):
        path = shard_path(sid)
        group.to_csv(path, mode="a", header=not os.path.exists(path), index=False)


def _clean_history(df):
    return (df.drop_duplicates(subset=HISTORY_KEY, keep="last")
              .sort_values(["appid", "timestamp"])
              .reset_index(drop=True))


def load_price_history():
    """Todos los shards juntos, sin duplicados. Para análisis / entrenar el modelo."""
    paths = sorted(glob.glob(os.path.join(PRICE_HISTORY_DIR, "shard_*.csv")))
    if not paths:
        return pd.DataFrame(columns=HISTORY_COLUMNS)
    df = pd.concat([pd.read_csv(p) for p in paths], ignore_index=True)
    return _clean_history(_normalize_history(df))


def compact_price_history():
    """Quita duplicados y ordena cada shard. Opcional, de mantención."""
    for path in sorted(glob.glob(os.path.join(PRICE_HISTORY_DIR, "shard_*.csv"))):
        df = _clean_history(_normalize_history(pd.read_csv(path)))
        atomic_write_csv(df, path)


# --- snapshots ---

def write_snapshots(rows):
    if not rows:
        return
    df_new = pd.DataFrame(rows)
    if os.path.exists(SNAPSHOTS_PATH):
        df_all = pd.concat([pd.read_csv(SNAPSHOTS_PATH), df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all = df_all.drop_duplicates(subset=["appid", "date_checked"], keep="last")
    df_all = df_all.sort_values(["appid", "date_checked"]).reset_index(drop=True)
    atomic_write_csv(df_all, SNAPSHOTS_PATH)


POPULARITY_COLUMNS = ["players_recent", "itad_waitlisted", "steam_count"]


def latest_popularity():
    """Señales de popularidad del último snapshot de cada juego (DataFrame indexado por appid)."""
    if not os.path.exists(SNAPSHOTS_PATH):
        return pd.DataFrame(columns=POPULARITY_COLUMNS)
    s = pd.read_csv(SNAPSHOTS_PATH, usecols=["appid", "date_checked", *POPULARITY_COLUMNS])
    # .last() de groupby toma el último valor no nulo de cada columna
    return s.sort_values("date_checked").groupby("appid")[POPULARITY_COLUMNS].last()
