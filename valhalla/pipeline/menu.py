"""Interactive menus and prompt helpers for the pipeline."""

from datetime import datetime, timedelta

from .chunking import (
    CHUNK_DELAY_MAX_SEC,
    CHUNK_DELAY_MIN_SEC,
    CHUNK_THRESHOLD_DAYS,
    build_chunks,
    fmt_ago,
    fmt_dt,
    parse_user_date,
    yesterday_midnight,
)


def prompt(prompt_text: str, default: str = '') -> str:
    """Wyswietl prompt i wczytaj odpowiedz. Obsluguje EOF."""
    try:
        answer = input(prompt_text)
        return answer.strip() if answer.strip() else default
    except EOFError:
        return default


def prompt_date(prompt_text: str, default_fn=None) -> 'datetime | None':
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

        dt = parse_user_date(raw)
        if dt is not None:
            return dt
        print('  Nieprawidlowy format. Uzyj YYYY-MM-DD lub YYYY-MM-DDTHH:MM.')


def prompt_int(prompt_text: str, default: int) -> int:
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


def menu_first_run(now: datetime) -> tuple:
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

    choice = prompt('Wybor [1]: ', default='1').upper()
    if choice == 'N':
        return True, None, None, False

    print()
    start = prompt_date(
        'Od kiedy pobrac? (YYYY-MM-DD lub Enter = wczoraj): ',
        default_fn=yesterday_midnight,
    )
    if start is None:
        return True, None, None, False

    delta_days = (now - start).total_seconds() / 86400
    day_by_day = delta_days > CHUNK_THRESHOLD_DAYS

    if day_by_day:
        chunks = build_chunks(start, now)
        print(f'\nZakres {delta_days:.0f} dni - automatycznie tryb day-by-day ({len(chunks)} chunkow).')

    return False, start, now, day_by_day


def menu_recent(last_pull: datetime, now: datetime) -> tuple:
    """
    Case B: last_pull <= CHUNK_THRESHOLD_DAYS dni temu.

    Returns:
        (skip_pull, start, end, day_by_day)
    """
    ago_str = fmt_ago(last_pull, now)
    delta_hours = (now - last_pull).total_seconds() / 3600

    print(f'\nOstatnie pobranie: {fmt_dt(last_pull)} ({ago_str})')
    print()
    print('Pobrac nowe wiadomosci z Discorda?')
    print(f'  [1] Tak - od ostatniego pobrania (zakres: {delta_hours:.0f} godz.)   [default]')
    print('  [2] Tak - od konkretnej daty')
    print('  [3] Tak - tylko ostatnie N godzin')
    print('  [N] Nie, przejdz do parsera')
    print()

    choice = prompt('Wybor [1]: ', default='1').upper()

    if choice == 'N':
        return True, None, None, False

    if choice in ('', '1'):
        return False, last_pull, now, False

    if choice == '2':
        start = prompt_date('Data startowa (YYYY-MM-DD lub YYYY-MM-DDTHH:MM): ')
        if start is None:
            return True, None, None, False
        delta_days = (now - start).total_seconds() / 86400
        day_by_day = delta_days > CHUNK_THRESHOLD_DAYS
        if day_by_day:
            chunks = build_chunks(start, now)
            print(f'\nZakres {delta_days:.0f} dni - automatycznie tryb day-by-day ({len(chunks)} chunkow).')
        return False, start, now, day_by_day

    if choice == '3':
        hours = prompt_int('Ile godzin wstecz? [24]: ', default=24)
        start = now - timedelta(hours=hours)
        return False, start, now, False

    print(f'  Nieznana opcja: {choice!r}. Pomijam pobieranie.')
    return True, None, None, False


def menu_stale(last_pull: datetime, now: datetime) -> tuple:
    """
    Case C: last_pull > CHUNK_THRESHOLD_DAYS dni temu.

    Returns:
        (skip_pull, start, end, day_by_day)
    """
    ago_str = fmt_ago(last_pull, now)
    delta_days = int((now - last_pull).total_seconds() / 86400)

    print(f'\nOstatnie pobranie: {fmt_dt(last_pull)} ({ago_str}) - zakres za dlugi')
    print()
    print('Opcje:')
    print(f'  [1] Dzien po dniu ({delta_days} chunkow, ~{CHUNK_DELAY_MIN_SEC}-{CHUNK_DELAY_MAX_SEC}s przerwy)   [default]')
    print('  [2] Od konkretnej daty (jeden plik - na wlasna odpowiedzialnosc)')
    print('  [3] Ostatnie N godzin')
    print('  [N] Pomin pobieranie')
    print()

    choice = prompt('Wybor [1]: ', default='1').upper()

    if choice == 'N':
        return True, None, None, False

    if choice in ('', '1'):
        return False, last_pull, now, True

    if choice == '2':
        start = prompt_date('Data startowa (YYYY-MM-DD lub YYYY-MM-DDTHH:MM): ')
        if start is None:
            return True, None, None, False
        delta = (now - start).total_seconds() / 86400
        day_by_day = delta > CHUNK_THRESHOLD_DAYS
        if day_by_day:
            chunks = build_chunks(start, now)
            print(f'\nZakres {delta:.0f} dni - tryb day-by-day ({len(chunks)} chunkow).')
        return False, start, now, day_by_day

    if choice == '3':
        hours = prompt_int('Ile godzin wstecz? [24]: ', default=24)
        start = now - timedelta(hours=hours)
        return False, start, now, False

    print(f'  Nieznana opcja: {choice!r}. Pomijam pobieranie.')
    return True, None, None, False
