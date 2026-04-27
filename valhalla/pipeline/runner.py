"""Subprocess wrappers and Discord pull orchestration."""

import csv
import os
import random
import subprocess
import sys
import time
from datetime import datetime

from .chunking import (
    CHUNK_DELAY_MAX_SEC,
    CHUNK_DELAY_MIN_SEC,
    build_chunks,
    dt_to_dce_str,
    fmt_dt,
)
from .state import DCE_PULL, PARSER, PROJECT_ROOT, RECALC_PENDING, save_state


def run_pull(after: datetime, before: datetime) -> tuple:
    """
    Wywolaj dce_pull.py --after X --before Y.
    Stdout/stderr przechodzi bezposrednio do terminala uzytkownika.
    Zwraca (returncode, error_str).
    """
    cmd = [
        sys.executable,
        str(DCE_PULL),
        '--after', dt_to_dce_str(after),
        '--before', dt_to_dce_str(before),
    ]
    try:
        result = subprocess.run(cmd)
        return result.returncode, ''
    except KeyboardInterrupt:
        raise
    except Exception as e:
        return 1, str(e)


def run_parser() -> int:
    """
    Wywolaj valhalla_parser_v2.py. Stdout/stderr trafia prosto do terminala.
    Zwraca returncode.
    """
    cmd = [sys.executable, str(PARSER)]
    try:
        result = subprocess.run(cmd)
        return result.returncode
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f'\n[main] Blad uruchamiania parsera: {e}', file=sys.stderr)
        return 1


def run_recalc_pending(parser_rc: int = 0) -> int:
    """
    Wywolaj tools/recalc_pending.py w podprocesie z wymuszona kodowaniem UTF-8.

    Zwraca returncode (0 = sukces lub krok pominieto).
    """
    if parser_rc != 0:
        print('\n[recalc_pending] Parser failed (rc={}) \u2014 pomijam recalkulacje pending.'.format(parser_rc))
        return 0

    if os.environ.get('SKIP_PENDING_RECALC', '').strip() == '1':
        print('\n[recalc_pending] SKIP_PENDING_RECALC=1 \u2014 pomijam recalkulacje pending.')
        return 0

    csv_path = PROJECT_ROOT / 'output' / 'positions.csv'
    pending_count = 0
    bug1_count = 0
    if csv_path.exists():
        try:
            with open(csv_path, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    src = row.get('pnl_source', '')
                    if src == 'pending' and row.get('full_address', ''):
                        pending_count += 1
                    elif src == 'meteora' and row.get('meteora_deposited', '').strip() in ('0.0000', '0', ''):
                        bug1_count += 1
        except Exception:
            pending_count = -1

    total = pending_count + bug1_count if pending_count >= 0 else -1
    print(
        f'\n[recalc_pending] Starting Meteora API recalculation '
        f'({pending_count} pending + {bug1_count} Bug#1 = {total} candidates)...'
    )
    print()

    env = dict(os.environ)
    env['PYTHONIOENCODING'] = 'utf-8'
    env['PYTHONUTF8'] = '1'

    cmd = [sys.executable, str(RECALC_PENDING)]
    try:
        result = subprocess.run(cmd, env=env)
        return result.returncode
    except KeyboardInterrupt:
        raise
    except Exception as e:
        print(f'\n[recalc_pending] Blad uruchamiania recalc_pending: {e}', file=sys.stderr)
        return 1


def ask_run_parser_despite_error() -> bool:
    """Zapytaj uzytkownika czy odpalic parser mimo bledu pullu."""
    try:
        answer = input('\nMimo bledu odpalic parser na tym co juz pobrane? [y/N]: ').strip().lower()
        return answer == 'y'
    except (EOFError, KeyboardInterrupt):
        return False


def handle_interrupt(last_successful: 'datetime | None') -> None:
    """Pokaz komunikat po Ctrl+C."""
    if last_successful:
        print(
            f'\n\n[main] Przerwano. Stan zachowany '
            f'(ostatni udany chunk: {fmt_dt(last_successful)}).'
        )
    else:
        print('\n\n[main] Przerwano. Brak zmian w stanie (zaden chunk nie zostal ukonczony).')


def pull_day_by_day(
    start: datetime,
    end: datetime,
    last_success: 'datetime | None',
) -> tuple:
    """
    Wykonaj pobieranie w trybie day-by-day.

    Returns:
        (run_parser, last_successful_until, chunks_count)
    """
    chunks = build_chunks(start, end)
    total = len(chunks)
    print(f'\n[main] Tryb day-by-day: {total} chunk(ow) do pobrania.')

    last_successful = last_success
    chunks_done = 0

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        is_last = (i == total)
        print(f'\n[main] Chunk {i}/{total}: {fmt_dt(chunk_start)} -> {fmt_dt(chunk_end)}')

        try:
            rc, err = run_pull(chunk_start, chunk_end)
        except KeyboardInterrupt:
            handle_interrupt(last_successful)
            run_parser_after_error = ask_run_parser_despite_error()
            return run_parser_after_error, last_successful, chunks_done

        if rc != 0:
            msg = f'\n[main] Chunk {i}/{total} zakonczony bledem (kod {rc}).'
            if err:
                msg += f'\n{err}'
            print(msg, file=sys.stderr)
            run_parser_after_error = ask_run_parser_despite_error()
            return run_parser_after_error, last_successful, chunks_done

        save_state(chunk_end)
        last_successful = chunk_end
        chunks_done += 1

        if not is_last:
            delay = random.uniform(CHUNK_DELAY_MIN_SEC, CHUNK_DELAY_MAX_SEC)
            print(f'\n[main] Oczekiwanie {delay:.0f}s przed nastepnym chunkiem...', file=sys.stderr)
            try:
                time.sleep(delay)
            except KeyboardInterrupt:
                handle_interrupt(last_successful)
                run_parser_after_error = ask_run_parser_despite_error()
                return run_parser_after_error, last_successful, chunks_done

    return True, last_successful, chunks_done


def pull_single(after: datetime, before: datetime) -> tuple:
    """
    Wykonaj pojedynczy pull.

    Returns:
        (success, run_parser)
    """
    print(f'\n[main] Pobieranie: {fmt_dt(after)} -> {fmt_dt(before)}')
    try:
        rc, err = run_pull(after, before)
    except KeyboardInterrupt:
        print('\n\n[main] Przerwano. Stan nie zostal zmieniony.')
        return False, False

    if rc != 0:
        print(f'\n[main] Pull zakonczony bledem (kod {rc}).', file=sys.stderr)
        run_parser_after_error = ask_run_parser_despite_error()
        return False, run_parser_after_error

    save_state(before)
    return True, True
