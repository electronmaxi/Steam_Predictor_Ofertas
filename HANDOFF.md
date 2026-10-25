# HANDOFF — Steam Predictor de Ofertas

Última revisión: 2026-10-25 (noche). Horas en UTC salvo que diga "Chile" (UTC−3 en horario de verano). El repo pasó a ser **público**, así que los minutos de Actions ya no tienen tope (el límite de 6 h por job sigue).

## Qué es

Pipeline Python que recolecta precios e info de juegos de Steam vía la API de IsThereAnyDeal (ITAD) y guarda CSVs en este repo. Corre en GitHub Actions. Objetivo final: un modelo que prediga ofertas. Estructura de carpetas en [readme.md](readme.md).

## Cómo funciona

- **Onboarding** (`onboard_batch.py`, cron `0 21,0,3,6,9,12 * * *`, cada 3 h fuera de la ventana del daily): toma los `pending` en orden de appid, hace `lookup` + historial completo desde 2015 + `info` y los pasa a `active`. Solo `GameNotFound` los marca `not_found`; un error de red deja el juego `pending` para reintentarlo.
- **Daily** (`collect_data.py`, cron `0 15 * * *`): re-consulta los `active` que toca actualizar según su nivel de popularidad. Procesa primero los populares y, dentro de cada nivel, el más atrasado primero.
  - Puntaje = promedio del percentil en `itad_waitlisted` y `steam_count` del último snapshot. Se recalcula en cada corrida.
  - **popular**: top 10.000, cada 3 días. **medio**: hasta el puesto 60.000 o con `players_recent` > 0, cada 14 días. **cola**: el resto, cada 60 días. Se configura en `pipeline/config.py`.
  - Margen de 12 h (`DUE_TOLERANCE_HOURS`): un juego cuenta como vencido si le faltan menos de 12 h para cumplir su intervalo. Sin ese margen, "cada 3 días" terminaba siendo cada 4, porque el daily parte siempre a una hora parecida.
  - Con los 175k juegos son ~8.800 por día, ~2 h de run.
  - Usa el `game_id` guardado en tracked, así que no repite el lookup: 2 requests por juego.
  - Pide el historial desde `history_synced_at` menos 2 días. Si `history_synced_at` está vacío, baja el historial completo.
- **Loop común** (`pipeline/runner.py`): corta limpio y guarda cuando pasa cualquiera de estas cosas: termina la lista, se agota `MAX_RUNTIME_MIN`, ITAD responde 429 o hay 25 errores seguidos. Además guarda un checkpoint a disco cada 500 juegos.

### Protección contra timeouts (3 capas)
1. El script se detiene solo al cumplir `MAX_RUNTIME_MIN`: 300 en el daily, 150 en el onboarding.
2. El paso de Python tiene `timeout-minutes` propio (320 / 165). Si algo se cuelga, GitHub corta ese paso y no todo el job.
3. El paso de commit tiene `if: always()`: sube lo que haya en disco aunque el paso anterior haya fallado o lo hayan cortado. Gracias a los checkpoints, como mucho se pierden 500 juegos.

Además: los requests tienen `timeout=(10, 30)`, y los dos workflows comparten `concurrency: steam-data-write`, así que nunca corren a la vez. Antes del push se hace `git pull --rebase`.

### Formato de datos
- `tracked_games.csv`: `appid, name, status, date_added, last_updated, game_id, history_synced_at`.
- `snapshots.csv`: se reescribe entero en cada checkpoint, de forma atómica (`.tmp` + `os.replace`). La clave es (appid, date_checked).
- `price_history/shard_XXX.csv`: 100 shards con `shard = (appid // 10) % 100` (los appid de Steam son múltiplos de 10). Los shards **solo crecen por append**, así que puede haber filas duplicadas por el solape de 2 días. Para leer hay que usar `load_price_history()`, que quita los duplicados. `compact_price_history()` limpia los archivos si alguna vez hace falta.
- Orden de guardado: price_history → snapshots → tracked. Si se corta en medio, el juego simplemente se vuelve a procesar en la próxima corrida.

## Historia de problemas (resueltos el 25-oct)
- Del 26-jul al 25-oct el historial de precios **no se commiteó**, porque `git add data/*.csv` no entra a subcarpetas. Al migrar solo 9.771 juegos tenían historial guardado. Los otros ~48.550 activos quedaron con `history_synced_at` vacío y el daily les baja el historial completo de forma automática (ITAD guarda todo, no se perdió nada irrecuperable).
- Con el sharding `appid % 20` todo caía en 2 archivos de ~50 MB.
- El daily moría por el timeout de 6 h sin guardar nada: no tenía timeouts en los requests, escribía una sola vez al final y hacía 3 requests por juego.
- Repo privado: se acababan los 2.000 min/mes hacia el día 7 de cada mes.
- Onboarding anterior: cualquier excepción (incluso un timeout de red) marcaba el juego `not_found` para siempre.

## Mediciones reales (25-oct)

| | Ritmo | Requests/min | % del límite ITAD (200/min) |
|---|---|---|---|
| Onboarding (3 requests/juego) | 49 juegos/min, ~7.350 por run de 150 min | ~147 | ~74 % |
| Daily (2 requests/juego) | 74,3 juegos/min, ~22.300 por run de 300 min | ~150 | ~75 % |

- **Sin ningún 429.** El `sleep(0,3)` por juego es más de un tercio del tiempo del daily. Si los logs siguen limpios, se podría cambiar por un control de ritmo por request a ~180/min (+20 %). Por ahora se deja así por estabilidad.
- **GitHub atrasa los crons**: el daily de las 15:00 partió a las 18:03 el 25-oct. El horario de onboarding lo aguanta porque el grupo de concurrencia deja un run en espera y no se pierde ninguno.
- **No agregar crons de onboarding entre las 15 y las ~21**: el grupo de concurrencia admite un solo run en espera, y uno nuevo cancela al que estaba esperando (podría cancelar el daily).
- Si aparecen 429 seguidos, hay que bajar el ritmo subiendo `SLEEP_BETWEEN_GAMES` en `pipeline/config.py`.

### Primer daily v2 (25-oct, 18:03–23:03, se detuvo por presupuesto de tiempo)
- 22.303 juegos: **populares 9.601 de 9.617 (listos a las ~20:12)**, medios 12.700 de 46.163, cola 2.
- Recuperó el historial completo de 17.764 juegos. **Quedan 30.786 activos con `history_synced_at` vacío**, que se recuperan solos en los próximos dailies.
- 16 errores por `"players": null` / `"stats": null` desde ITAD, ya corregidos (`or {}` en `build_snapshot_row`).
- Backlog restante: popular 17, medio 33.461, cola 1.049.

### Proyección (ritmos medidos; horas de Chile)

| Hito | Cuándo |
|---|---|
| Populares con snapshot fresco | ✅ dom 25, 17:12 |
| Medios atrasados al día | mar 27, ~17:30 (daily del lunes ~22.300 + martes ~11.200) |
| Cola atrasada al día, backlog completo | mar 27, ~17:45 |
| Onboarding completo (pending 111.757 → 0, 16 runs) | mié 28, ~05:00 |
| 2ª vuelta de populares | mié 28, daily de la tarde (gracias al margen de 12 h) |
| Medios vuelven todos juntos (actualizados en bloque) | 8–11 nov, daily más cargado (~20–25k), cabe en 5 h |
| Régimen estable | ~8.800 juegos/día, ~2 h de daily. Los onboardings quedan vacíos (terminan en segundos); se pueden apagar |

Cuando entren los 111k juegos nuevos, el ranking se recalcula: muchos medios con 0 jugadores bajan a cola (60 días), y eso puede adelantar los hitos de los medios.

## Pendientes / riesgos
- **Tamaño del repo**: con los 175k juegos, price_history pesaría ~1,8 GB y snapshots seguiría creciendo. Por ahora sirve, pero a mediano plazo conviene Parquet o sacarlo de git (por ejemplo releases o almacenamiento externo).
- GitHub desactiva los crons de un repo público si pasan 60 días sin actividad. Los commits del bot deberían contar como actividad.
- Revisar los términos de ITAD sobre redistribuir datos, ahora que el repo es público.

## Calidad de datos (análisis del 25-oct)
- `itad_rank` cambia de escala el 10-ago-2026 (antes llegaba a ~92k, después queda topado en ~3.460 con empates). No mezclar los dos periodos.
- appid ↔ game_id no es 1:1 → usar `appid` como clave.
- Desalineaciones chicas entre tracked y snapshots (14 / 63 / 80 juegos). Hay 80 activos sin `game_id`, que hacen lookup.
- `developers` está unido con ", " y se corta mal con nombres como "PopCap Games, Inc.".
- En el 80 % de los juegos el último snapshot tiene `players_recent` = 0 o vacío.

## Preferencias del usuario
Prefiere estabilidad a velocidad, que los runs paren limpio y el siguiente continúe (sin reintentos ni sleeps largos), y nada de sobre-ingeniería. Trabaja en Windows con VS Code, Jupyter y pandas. El Python 3.14 global no tiene pandas; el entorno de Jupyter es otro.
