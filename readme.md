# STEAM PREDICTOR DE Ofertas

Recolecta precios e info de juegos de Steam vía la API de IsThereAnyDeal y la guarda en `data/`, automatizado con GitHub Actions. Ver [HANDOFF.md](HANDOFF.md) para el detalle y el estado actual.

## Estructura

```
collect_data.py          daily update de juegos activos  (workflow daily_collect.yml)
onboard_batch.py         primera carga de juegos pending (workflow onboard_batch.yml)
pipeline/
  config.py              constantes: rutas, intervalos, timeouts, checkpoints
  itad.py                cliente de la API de ITAD
  storage.py             lectura/escritura de los CSV
  runner.py              loop común con presupuesto de tiempo y checkpoints
scripts/
  build_appid_pool.py    crea tracked_games.csv desde Steam (una vez)
  migrate_v2.py          migración de shards/columnas de oct-2026 (una vez, ya corrida)
notebooks/
  obtain_info.ipynb      exploración
data/
  tracked_games.csv      catálogo y estado de cada juego
  snapshots.csv          1 fila por (appid, fecha): jugadores, rank, reviews
  price_history/         historial de precios de Steam en 100 shards
```

## Uso local

```
pip install -r requirements.txt
python onboard_batch.py
MAX_RUNTIME_MIN=10 python collect_data.py
```

Para análisis, `from pipeline.storage import load_price_history` devuelve todo el historial sin duplicados.
