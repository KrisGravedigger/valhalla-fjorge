#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valhalla Fjorge - interaktywny pipeline entry point.

Laczy pobieranie wiadomosci z Discorda (dce_pull.py) z parserem
(valhalla_parser_v2.py) w jeden spojny, stan-swiadomy flow.

Uzycie:
    python main.py

Stan poprzednich pullow jest przechowywany w .dce_state.json.
"""

import csv
import io
import json
import os
import random
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Force UTF-8 output on Windows (avoids CP1250 UnicodeEncodeError for Polish chars).
# Must happen before any print() calls.
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ('utf-8', 'utf8'):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# ---------------------------------------------------------------------------
# Konfiguracja (zmien tutaj jesli potrzebujesz)
# ---------------------------------------------------------------------------

CHUNK_THRESHOLD_DAYS = 3    # powyzej tego progu zakres jest "za dlugi" -> day-by-day
CHUNK_DELAY_MIN_SEC = 32    # losowy odstep miedzy chunkami (dolna granica, rate-limit)
CHUNK_DELAY_MAX_SEC = 64    # losowy odstep miedzy chunkami (gorna granica)

# ---------------------------------------------------------------------------
# Sciezki
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).parent
STATE_FILE = PROJECT_ROOT / '.dce_state.json'
DCE_PULL = PROJECT_ROOT / 'dce_pull.py'
PARSER = PROJECT_ROOT / 'valhalla_parser_v2.py'
RECALC_PENDING = PROJECT_ROOT / 'tools' / 'recalc_pending.py'


# ---------------------------------------------------------------------------
# Stan
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    """Wczytaj .dce_state.json. Zwraca pusty dict jesli plik nie istnieje."""
    if not STATE_FILE.exists():
        return {}
    try:
        data = json.loads(STATE_FILE.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def _save_state(last_pull_until: datetime) -> None:
    """Zapisz last_pull_until do .dce_state.json (ISO 8601, timezone-aware)."""
    state = {'last_pull_until': last_pull_until.isoformat()}
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding='utf-8')


def _parse_state_dt(state: dict) -> 'datetime | None':
    """Parsuj last_pull_until ze stanu. Zwraca None jesli brak lub blad."""
    raw = state.get('last_pull_until')
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        # Upewnij sie, ze jest timezone-aware
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Formatowanie czasu
# ---------------------------------------------------------------------------

def _fmt_dt(dt: datetime) -> str:
    """Formatuj datetime do czytelnego stringa: YYYY-MM-DD HH:MM."""
    return dt.strftime('%Y-%m-%d %H:%M')


def _fmt_ago(dt: datetime, now: datetime) -> str:
    """Czytelna informacja 'X dni temu' / 'X godz. Y min temu'."""
    delta = now - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return 'w przyszlosci?'
    days = delta.days
    hours, remainder = divmod(total_seconds % 86400, 3600)
    minutes = remainder // 60
    if days > 0:
        return f'{days} dni temu' if days != 1 else '1 dzien temu'
    if hours > 0:
        return f'{hours} godz. {minutes} min temu'
    return f'{minutes} min temu'


def _dt_to_dce_str(dt: datetime) -> str:
    """Konwertuj datetime do formatu YYYY-MM-DDTHH:MM wymaganego przez dce_pull.py."""
    return dt.strftime('%Y-%m-%dT%H:%M')


# ---------------------------------------------------------------------------
# Parsowanie daty od uzytkownika
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    '%Y-%m-%dT%H:%M',   # z godzina
    '%Y-%m-%d',          # tylko data -> godzina 00:00
]


def _parse_user_date(raw: str) -> 'datetime | None':
    """Parsuj YYYY-MM-DD lub YYYY-MM-DDTHH:MM -> timezone-aware datetime (local)."""
    raw = raw.strip()
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=local_tz)
        except ValueError:
            continue
    return None


def _yesterday_midnight() -> datetime:
    """Wczoraj o 00:00 w czasie lokalnym (timezone-aware)."""
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    today = datetime.now(local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=1)


def _now_local() -> datetime:
    """Teraz w czasie lokalnym (timezone-aware)."""
    return datetime.now(timezone.utc).astimezone()


# ---------------------------------------------------------------------------
# Chunking (day-by-day)
# ---------------------------------------------------------------------------

def _build_chunks(start: datetime, end: datetime) -> list:
    """
    Podziel zakres [start, end] na dobowe chunki w czasie lokalnym.

    - Pierwszy chunk: [start, nastepna_polnoc_lokalna)
    - Srodkowe chunki: pelne doby [dzien_N 00:00, dzien_N+1 00:00)
    - Ostatni chunk: [dzisiaj 00:00, end]

    Jesli start i end sa w tej samej dobie -> jeden chunk [start, end].
    """
    local_tz = _now_local().tzinfo
    start = start.astimezone(local_tz)
    end = end.astimezone(local_tz)

    chunks = []
    cursor = start

    while cursor < end:
        # Nastepna polnoc po cursor
        next_midnight = (cursor + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        chunk_end = min(next_midnight, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end

    return chunks


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------

def _run_pull(after: datetime, before: datetime) -> tuple:
    """
    Wywolaj dce_pull.py --after X --before Y.
    Stdout/stderr przechodzi bezposrednio do terminala uzytkownika.
    Zwraca (returncode, error_str).
    """
    cmd = [
        sys.executable,
        str(DCE_PULL),
        '--after', _dt_to_dce_str(after),
        '--before', _dt_to_dce_str(before),
    ]
    try:
        result = subprocess.run(cmd)
        return result.returncode, ''
    except KeyboardInterrupt:
        raise
    except Exception as e:
        return 1, str(e)


def _run_parser() -> int:
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


def _run_recalc_pending() -> int:
    """
    Wywolaj tools/recalc_pending.py w podprocesie z wymuszona kodowaniem UTF-8.

    Uzyj zmiennej srodowiskowej SKIP_PENDING_RECALC=1 zeby poминac ten krok
    podczas lokalnego development (bez dostepu do Meteora API lub celowo).

    Zwraca returncode (0 = sukces lub krok pominieto).
    """
    if os.environ.get('SKIP_PENDING_RECALC', '').strip() == '1':
        print('\n[recalc_pending] SKIP_PENDING_RECALC=1 — pomijam recalkulacje pending.')
        return 0

    # Count pending positions before calling the subprocess so we can log N
    csv_path = PROJECT_ROOT / 'output' / 'positions.csv'
    pending_count = 0
    if csv_path.exists():
        try:
            with open(csv_path, encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get('pnl_source', '') == 'pending' and row.get('full_address', ''):
                        pending_count += 1
        except Exception:
            pending_count = -1  # unknown — still run recalc

    print(f'\n[recalc_pending] Starting Meteora API recalculation for {pending_count} pending positions...')
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


def _ask_run_parser_despite_error() -> bool:
    """Zapytaj uzytkownika czy odpalic parser mimo bledu pullu."""
    try:
        answer = input('\nMimo bledu odpalic parser na tym co juz pobrane? [y/N]: ').strip().lower()
        return answer == 'y'
    except (EOFError, KeyboardInterrupt):
        return False


# ---------------------------------------------------------------------------
# Day-by-day pull z obsluga Ctrl+C i bledow
# ---------------------------------------------------------------------------

def _pull_day_by_day(
    start: datetime,
    end: datetime,
    last_success: 'datetime | None',
) -> tuple:
    """
    Wykonaj pobieranie w trybie day-by-day.

    Returns:
        (run_parser, last_successful_until, chunks_count)
        - run_parser: czy uruchomic parser po zakonczeniu
        - last_successful_until: datetime ostatniego udanego chunku (lub None)
        - chunks_count: ile chunkow zostalo wykonanych
    """
    chunks = _build_chunks(start, end)
    total = len(chunks)
    print(f'\n[main] Tryb day-by-day: {total} chunk(ow) do pobrania.')

    last_successful = last_success
    chunks_done = 0

    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        is_last = (i == total)
        print(f'\n[main] Chunk {i}/{total}: {_fmt_dt(chunk_start)} -> {_fmt_dt(chunk_end)}')

        try:
            rc, err = _run_pull(chunk_start, chunk_end)
        except KeyboardInterrupt:
            _handle_interrupt(last_successful)
            run_parser = _ask_run_parser_despite_error()
            return run_parser, last_successful, chunks_done

        if rc != 0:
            msg = f'\n[main] Chunk {i}/{total} zakonczony bledem (kod {rc}).'
            if err:
                msg += f'\n{err}'
            print(msg, file=sys.stderr)
            run_parser = _ask_run_parser_despite_error()
            return run_parser, last_successful, chunks_done

        # Sukces - zapisz stan
        _save_state(chunk_end)
        last_successful = chunk_end
        chunks_done += 1

        if not is_last:
            delay = random.uniform(CHUNK_DELAY_MIN_SEC, CHUNK_DELAY_MAX_SEC)
            print(f'\n[main] Oczekiwanie {delay:.0f}s przed nastepnym chunkiem...', file=sys.stderr)
            try:
                time.sleep(delay)
            except KeyboardInterrupt:
                _handle_interrupt(last_successful)
                run_parser = _ask_run_parser_despite_error()
                return run_parser, last_successful, chunks_done

    return True, last_successful, chunks_done


def _handle_interrupt(last_successful: 'datetime | None') -> None:
    """Pokaz komunikat po Ctrl+C."""
    if last_successful:
        print(
            f'\n\n[main] Przerwano. Stan zachowany '
            f'(ostatni udany chunk: {_fmt_dt(last_successful)}).'
        )
    else:
        print('\n\n[main] Przerwano. Brak zmian w stanie (zaden chunk nie zostal ukonczony).')


# ---------------------------------------------------------------------------
# Single-chunk pull
# ---------------------------------------------------------------------------

def _pull_single(after: datetime, before: datetime) -> tuple:
    """
    Wykonaj pojedynczy pull.

    Returns:
        (success, run_parser)
    """
    print(f'\n[main] Pobieranie: {_fmt_dt(after)} -> {_fmt_dt(before)}')
    try:
        rc, err = _run_pull(after, before)
    except KeyboardInterrupt:
        print('\n\n[main] Przerwano. Stan nie zostal zmieniony.')
        return False, False

    if rc != 0:
        print(f'\n[main] Pull zakonczony bledem (kod {rc}).', file=sys.stderr)
        run_parser = _ask_run_parser_despite_error()
        return False, run_parser

    _save_state(before)
    return True, True


# ---------------------------------------------------------------------------
# Obsluga inputu uzytkownika
# ---------------------------------------------------------------------------

def _prompt(prompt_text: str, default: str = '') -> str:
    """Wyswietl prompt i wczytaj odpowiedz. Obsluguje EOF."""
    try:
        answer = input(prompt_text)
        return answer.strip() if answer.strip() else default
    except EOFError:
        return default


def _prompt_date(prompt_text: str, default_fn=None) -> 'datetime | None':
    """
    Wyswietl prompt o date. Jesli uzytkownik wcisnie Enter -> default_fn().
    Powtarza do skutku lub EOF/Ctrl+C.
    """
    while True:
        try:
            raw = input(prompt_text).strip()
        except (EOFError, KeyboardInterrupt):
            return None

        if not raw and default_fn is not None:
            return default_fn()

        dt = _parse_user_date(raw)
        if dt is not None:
            return dt
        print('  Nieprawidlowy format. Uzyj YYYY-MM-DD lub YYYY-MM-DDTHH:MM.')


def _prompt_int(prompt_text: str, default: int) -> int:
    """Wczytaj liczbe calkowita. Zwraca default przy Enter lub bledzie."""
    while True:
        try:
            raw = input(prompt_text).strip()
        except (EOFError, KeyboardInterrupt):
            return default

        if not raw:
            return default
        try:
            val = int(raw)
            if val > 0:
                return val
            print('  Podaj dodatnia liczbe calkowita.')
        except ValueError:
            print('  Nieprawidlowa wartosc. Podaj liczbe calkowita.')


# ---------------------------------------------------------------------------
# Logika menu
# ---------------------------------------------------------------------------

def _menu_first_run(now: datetime) -> tuple:
    """
    Case A: brak historii - pierwszy run.

    Returns:
        (skip_pull, start, end, day_by_day)
    """
    print('\nBrak historii pobierania. Pierwsze uruchomienie.')
    print()
    print('Opcje:')
    print('  [1] Pobierz od podanej daty   [default]')
    print('  [N] Pomin pobieranie, przejdz do parsera')
    print()

    choice = _prompt('Wybor [1]: ', default='1').upper()
    if choice == 'N':
        return True, None, None, False

    print()
    start = _prompt_date(
        'Od kiedy pobrac? (YYYY-MM-DD lub Enter = wczoraj): ',
        default_fn=_yesterday_midnight,
    )
    if start is None:
        # Ctrl+C lub EOF
        return True, None, None, False

    delta_days = (now - start).total_seconds() / 86400
    day_by_day = delta_days > CHUNK_THRESHOLD_DAYS

    if day_by_day:
        chunks = _build_chunks(start, now)
        print(f'\nZakres {delta_days:.0f} dni - automatycznie tryb day-by-day ({len(chunks)} chunkow).')

    return False, start, now, day_by_day


def _menu_recent(last_pull: datetime, now: datetime) -> tuple:
    """
    Case B: last_pull <= CHUNK_THRESHOLD_DAYS dni temu.

    Returns:
        (skip_pull, start, end, day_by_day)
    """
    ago_str = _fmt_ago(last_pull, now)
    delta_hours = (now - last_pull).total_seconds() / 3600

    print(f'\nOstatnie pobranie: {_fmt_dt(last_pull)} ({ago_str})')
    print()
    print('Pobrac nowe wiadomosci z Discorda?')
    print(f'  [1] Tak - od ostatniego pobrania (zakres: {delta_hours:.0f} godz.)   [default]')
    print('  [2] Tak - od konkretnej daty')
    print('  [3] Tak - tylko ostatnie N godzin')
    print('  [N] Nie, przejdz do parsera')
    print()

    choice = _prompt('Wybor [1]: ', default='1').upper()

    if choice == 'N':
        return True, None, None, False

    if choice in ('', '1'):
        return False, last_pull, now, False

    if choice == '2':
        start = _prompt_date('Data startowa (YYYY-MM-DD lub YYYY-MM-DDTHH:MM): ')
        if start is None:
            return True, None, None, False
        delta_days = (now - start).total_seconds() / 86400
        day_by_day = delta_days > CHUNK_THRESHOLD_DAYS
        if day_by_day:
            chunks = _build_chunks(start, now)
            print(f'\nZakres {delta_days:.0f} dni - automatycznie tryb day-by-day ({len(chunks)} chunkow).')
        return False, start, now, day_by_day

    if choice == '3':
        hours = _prompt_int('Ile godzin wstecz? [24]: ', default=24)
        start = now - timedelta(hours=hours)
        return False, start, now, False

    print(f'  Nieznana opcja: {choice!r}. Pomijam pobieranie.')
    return True, None, None, False


def _menu_stale(last_pull: datetime, now: datetime) -> tuple:
    """
    Case C: last_pull > CHUNK_THRESHOLD_DAYS dni temu.

    Returns:
        (skip_pull, start, end, day_by_day)
    """
    ago_str = _fmt_ago(last_pull, now)
    delta_days = int((now - last_pull).total_seconds() / 86400)

    print(f'\nOstatnie pobranie: {_fmt_dt(last_pull)} ({ago_str}) - zakres za dlugi')
    print()
    print('Opcje:')
    print(f'  [1] Dzien po dniu ({delta_days} chunkow, ~{CHUNK_DELAY_MIN_SEC}-{CHUNK_DELAY_MAX_SEC}s przerwy)   [default]')
    print('  [2] Od konkretnej daty (jeden plik - na wlasna odpowiedzialnosc)')
    print('  [3] Ostatnie N godzin')
    print('  [N] Pomin pobieranie')
    print()

    choice = _prompt('Wybor [1]: ', default='1').upper()

    if choice == 'N':
        return True, None, None, False

    if choice in ('', '1'):
        return False, last_pull, now, True

    if choice == '2':
        start = _prompt_date('Data startowa (YYYY-MM-DD lub YYYY-MM-DDTHH:MM): ')
        if start is None:
            return True, None, None, False
        delta = (now - start).total_seconds() / 86400
        day_by_day = delta > CHUNK_THRESHOLD_DAYS
        if day_by_day:
            chunks = _build_chunks(start, now)
            print(f'\nZakres {delta:.0f} dni - tryb day-by-day ({len(chunks)} chunkow).')
        return False, start, now, day_by_day

    if choice == '3':
        hours = _prompt_int('Ile godzin wstecz? [24]: ', default=24)
        start = now - timedelta(hours=hours)
        return False, start, now, False

    print(f'  Nieznana opcja: {choice!r}. Pomijam pobieranie.')
    return True, None, None, False


# ---------------------------------------------------------------------------
# Podsumowanie
# ---------------------------------------------------------------------------

def _count_input_files() -> tuple:
    """
    Policz pliki i wiadomosci w input/.
    Wiadomosci szacowane na podstawie podwojnych nowych linii.
    Zwraca (liczba_plikow, szacowana_liczba_wiadomosci).
    """
    input_dir = PROJECT_ROOT / 'input'
    if not input_dir.exists():
        return 0, 0

    files = list(input_dir.glob('*.txt'))
    total_msgs = 0
    for f in files:
        try:
            text = f.read_text(encoding='utf-8', errors='replace')
            # Kazdy blok wiadomosci oddzielony jest podwojnym nowym wierszem
            blocks = [b for b in text.split('\n\n') if b.strip()]
            total_msgs += len(blocks)
        except OSError:
            pass
    return len(files), total_msgs


def _print_summary(chunks_done: int, skipped_pull: bool) -> None:
    """Wydrukuj podsumowanie koncowe."""
    files, msgs = _count_input_files()

    state = _load_state()
    final_dt = _parse_state_dt(state)
    state_str = _fmt_dt(final_dt) if final_dt else 'brak'

    sep = '=' * 39
    print()
    print(sep)
    print(' Pipeline zakonczony')
    print(sep)

    if skipped_pull:
        print(' Pobieranie: pominiete')
    else:
        print(f' Pobrane:    {chunks_done} chunk(ow), ~{msgs:,} wiadomosci, {files} plik(ow) w input/')

    print(' Parser:     (stdout parsera powyzej)')
    print(f' Stan:       last_pull_until = {state_str}')
    print(sep)


# ---------------------------------------------------------------------------
# Glowna funkcja
# ---------------------------------------------------------------------------

def main() -> None:
    now = _now_local()
    state = _load_state()
    last_pull = _parse_state_dt(state)

    # --- Wybor menu ---
    if last_pull is None:
        # Case A: pierwszy run
        skip_pull, start, end, day_by_day = _menu_first_run(now)
    else:
        delta_days = (now - last_pull).total_seconds() / 86400
        if delta_days <= CHUNK_THRESHOLD_DAYS:
            # Case B: ostatnie pobranie niedawno
            skip_pull, start, end, day_by_day = _menu_recent(last_pull, now)
        else:
            # Case C: zakres za dlugi
            skip_pull, start, end, day_by_day = _menu_stale(last_pull, now)

    # --- Pobieranie ---
    run_parser = True
    chunks_done = 0

    if not skip_pull and start is not None and end is not None:
        if day_by_day:
            run_parser, _, chunks_done = _pull_day_by_day(start, end, last_pull)
        else:
            success, run_parser = _pull_single(start, end)
            if success:
                chunks_done = 1
    else:
        print('\n[main] Pobieranie pominiete - przechodze do parsera.')

    # --- Parser ---
    if run_parser:
        print('\n[main] Uruchamiam parser...')
        print()
        parser_rc = _run_parser()
        if parser_rc != 0:
            print(f'\n[main] Parser zakonczony kodem {parser_rc}.', file=sys.stderr)
    else:
        print('\n[main] Parser pominieto.')
        _print_summary(chunks_done, skip_pull)
        sys.exit(1)

    # --- Recalc pending (Meteora API) ---
    _run_recalc_pending()

    # --- Podsumowanie ---
    _print_summary(chunks_done, skip_pull)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('\n\n[main] Przerwano przez uzytkownika.')
        sys.exit(130)
