"""build_appid_pool.py — corre una sola vez para crear el catálogo inicial"""
import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
STEAM_API_KEY = os.getenv("STEAM_API_KEY")
if not STEAM_API_KEY:
    raise RuntimeError("Falta STEAM_API_KEY en env")

DATA_DIR = "data"
TRACKED_PATH = os.path.join(DATA_DIR, "tracked_games.csv")


def build_pool():
    if os.path.exists(TRACKED_PATH):
        print("tracked_games.csv ya existe, no se sobreescribe.")
        return

    all_apps = []
    last_appid = 0
    have_more = True

    while have_more:
        r = requests.get(
            "https://api.steampowered.com/IStoreService/GetAppList/v1/",
            params={
                "key": STEAM_API_KEY,
                "max_results": 50000,
                "last_appid": last_appid,
                "include_games": True,
                "include_dlc": False,
                "include_software": False,
                "include_videos": False,
                "include_hardware": False,
            }
        )
        r.raise_for_status()
        data = r.json()["response"]

        apps = data.get("apps", [])
        all_apps.extend(apps)

        have_more = data.get("have_more_results", False)
        last_appid = data.get("last_appid", 0)
        print(f"  Traídos {len(all_apps)} juegos hasta ahora...")

    df = pd.DataFrame(all_apps)[["appid", "name"]]
    df = df[df["name"].str.strip() != ""]
    df["status"] = "pending"
    df["date_added"] = None

    os.makedirs(DATA_DIR, exist_ok=True)
    df.to_csv(TRACKED_PATH, index=False)
    print(f"Catálogo creado con {len(df)} juegos pendientes (solo tipo 'game', sin DLC/software).")


if __name__ == "__main__":
    build_pool()