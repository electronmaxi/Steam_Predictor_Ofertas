"""
Constantes del pipeline. Las que dependen del entorno (presupuesto de tiempo, etc.)
se pueden sobreescribir con variables de entorno desde el workflow.
"""

import os

DATA_DIR = "data"
TRACKED_PATH = os.path.join(DATA_DIR, "tracked_games.csv")
SNAPSHOTS_PATH = os.path.join(DATA_DIR, "snapshots.csv")
PRICE_HISTORY_DIR = os.path.join(DATA_DIR, "price_history")

# Los appid de Steam son casi todos múltiplos de 10, por eso el shard se calcula
# sobre appid // 10 (con appid % N todos caían en 2 shards).
# ~10 KB de historial por juego → 175k juegos ≈ 1,8 GB → ~18 MB por shard con 100.
NUM_SHARDS = 100

ITAD_BASE = "https://api.isthereanydeal.com"
STEAM_SHOP_ID = 61
HISTORY_SINCE_DEFAULT = "2015-01-01T00:00:00Z"
# Al re-consultar se pide el historial desde la última sincronización menos este margen,
# por si ITAD registra un cambio con timestamp anterior a cuando lo publica.
HISTORY_OVERLAP_DAYS = 2

# (conexión, lectura) en segundos. Sin esto un request colgado espera para siempre.
REQUEST_TIMEOUT = (10, 30)
SLEEP_BETWEEN_GAMES = 0.3

# Cada cuántos juegos procesados se escribe a disco. Si el job muere,
# se pierde como máximo este número de juegos.
CHECKPOINT_EVERY = 500

# Si fallan tantos juegos seguidos, probablemente la API está caída: se corta limpio.
MAX_CONSECUTIVE_ERRORS = 25

# Niveles de popularidad para el daily. Puntaje = promedio del percentil en
# itad_waitlisted y steam_count (último snapshot). Se recalcula en cada corrida.
# - popular: top POPULAR_TOP_N
# - medio:   hasta el puesto MID_TOP_N, o cualquier juego con jugadores recientes
# - cola:    el resto
# El historial de precios no pierde datos por consultar menos seguido (ITAD lo guarda
# con su timestamp); lo que pierde resolución es el snapshot del día.
POPULAR_TOP_N = 10_000
MID_TOP_N = 60_000
TIER_INTERVAL_DAYS = {"popular": 3, "medio": 14, "cola": 60}
TIER_ORDER = ["popular", "medio", "cola"]   # prioridad dentro de cada corrida
# Un juego cuenta como vencido si le faltan menos de estas horas para cumplir su intervalo.
# Sin margen, "cada 3 días" se vuelve cada 4: el daily parte a la misma hora (o antes,
# si GitHub lo atrasa menos) y los juegos aún no cumplen las 72 h exactas.
DUE_TOLERANCE_HOURS = 12


def env_int(name, default):
    value = os.getenv(name)
    return int(value) if value else default
