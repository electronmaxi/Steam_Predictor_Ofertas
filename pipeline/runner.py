"""
Loop común de onboarding y daily update.

Una corrida se detiene limpio (guarda y termina con éxito) por cualquiera de estas razones:
- se acabó la lista
- se acabó el presupuesto de tiempo (max_minutes), para no llegar nunca al timeout de GitHub
- ITAD respondió 429
- demasiados errores seguidos (API caída)
Además guarda a disco cada CHECKPOINT_EVERY juegos, así un corte duro pierde poco.
La siguiente corrida sigue donde quedó porque los juegos se eligen por estado / antigüedad.
"""

import time

from pipeline.config import CHECKPOINT_EVERY, MAX_CONSECUTIVE_ERRORS, SLEEP_BETWEEN_GAMES
from pipeline.itad import RateLimitReached, GameNotFound
from pipeline.storage import append_price_history, write_snapshots, save_tracked


def _apply(tracked, idx, updates):
    for col, value in updates.items():
        tracked.at[idx, col] = value


def run_games(tracked, todo, process_one, max_minutes, on_not_found=None):
    """
    tracked:      dataframe completo de tracked_games (se modifica en el lugar y se guarda).
    todo:         subconjunto de filas de tracked a procesar, en orden de prioridad.
    process_one:  fn(row) -> (history_rows, snapshot_row, updates_para_tracked)
    on_not_found: fn(row) -> updates_para_tracked, cuando el juego no existe en ITAD.
    Devuelve (juegos procesados, razón de término).
    """
    start = time.monotonic()
    deadline = start + max_minutes * 60
    history_rows, snapshot_rows = [], []
    done = 0
    consecutive_errors = 0
    stop_reason = "lista completa"

    def checkpoint():
        append_price_history(history_rows)
        write_snapshots(snapshot_rows)
        save_tracked(tracked)
        history_rows.clear()
        snapshot_rows.clear()

    for idx, row in todo.iterrows():
        if time.monotonic() > deadline:
            stop_reason = f"presupuesto de tiempo ({max_minutes} min)"
            break

        try:
            history, snapshot, updates = process_one(row)
        except RateLimitReached:
            stop_reason = "rate limit de ITAD (429)"
            break
        except GameNotFound as e:
            print(f"  appid {row['appid']}: {e}")
            consecutive_errors = 0
            if on_not_found:
                _apply(tracked, idx, on_not_found(row))
        except Exception as e:
            consecutive_errors += 1
            print(f"  Error en appid {row['appid']}: {e}")
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                stop_reason = f"{consecutive_errors} errores seguidos"
                break
        else:
            history_rows.extend(history)
            snapshot_rows.append(snapshot)
            _apply(tracked, idx, updates)
            done += 1
            consecutive_errors = 0

            if done % CHECKPOINT_EVERY == 0:
                checkpoint()
                elapsed = (time.monotonic() - start) / 60
                print(f"Checkpoint: {done}/{len(todo)} juegos guardados "
                      f"({elapsed:.0f} min, {done / elapsed:.0f} juegos/min)")

        time.sleep(SLEEP_BETWEEN_GAMES)

    checkpoint()
    elapsed = (time.monotonic() - start) / 60
    print(f"Fin: {done}/{len(todo)} juegos procesados en {elapsed:.0f} min. Motivo: {stop_reason}.")
    return done, stop_reason
