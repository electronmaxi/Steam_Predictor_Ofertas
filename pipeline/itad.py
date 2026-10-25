"""
Cliente mínimo de la API de IsThereAnyDeal.

La API key se lee de ITAD_API_KEY:
- En local: python-dotenv la carga desde el archivo .env / keys.env
- En GitHub Actions: se inyecta como secret
"""

import os
import requests

from pipeline.config import ITAD_BASE, STEAM_SHOP_ID, HISTORY_SINCE_DEFAULT, REQUEST_TIMEOUT

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


class RateLimitReached(Exception):
    """ITAD respondió 429. El caller debe detener el loop, guardar el progreso
    y dejar que la siguiente ejecución continúe."""


class GameNotFound(Exception):
    """El appid no existe en ITAD."""


_api_key = None


def _get_api_key():
    global _api_key
    if _api_key is None:
        _api_key = os.getenv("ITAD_API_KEY")
        if not _api_key:
            raise RuntimeError(
                "No se encontró ITAD_API_KEY. "
                "En local: revisa tu archivo .env. "
                "En GitHub Actions: revisa que el secret ITAD_API_KEY esté configurado."
            )
    return _api_key


def itad_get(path, params):
    r = requests.get(f"{ITAD_BASE}{path}",
                     params={"key": _get_api_key(), **params},
                     timeout=REQUEST_TIMEOUT)
    if r.status_code == 429:
        raise RateLimitReached(f"Rate limit alcanzado en {path}")
    r.raise_for_status()
    return r.json()


def lookup_game_id(appid):
    lookup = itad_get("/games/lookup/v1", {"appid": appid})
    if not lookup.get("found"):
        raise GameNotFound(f"Juego con appid {appid} no encontrado en ITAD")
    return lookup["game"]["id"]


def fetch_game(appid, game_id=None, since=None):
    """Trae history + info de un juego. Si no se conoce el game_id, hace lookup
    (1 request extra). `since` permite bajar solo el historial nuevo."""
    if not game_id:
        game_id = lookup_game_id(appid)

    history = itad_get("/games/history/v2",
                       {"id": game_id, "shops": STEAM_SHOP_ID,
                        "since": since or HISTORY_SINCE_DEFAULT, "country": "us"})
    info = itad_get("/games/info/v2", {"id": game_id})

    return {"appid": appid, "game_id": game_id, "title": info["title"],
            "history": history, "info": info, "since": since}
