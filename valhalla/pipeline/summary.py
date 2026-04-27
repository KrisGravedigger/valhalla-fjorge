"""Pipeline summary helpers."""

from .chunking import fmt_dt
from .state import PROJECT_ROOT, load_state, parse_state_dt


def count_input_files() -> tuple:
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
            blocks = [b for b in text.split('\n\n') if b.strip()]
            total_msgs += len(blocks)
        except OSError:
            pass
    return len(files), total_msgs


def print_summary(chunks_done: int, skipped_pull: bool) -> None:
    """Wydrukuj podsumowanie koncowe."""
    files, msgs = count_input_files()

    state = load_state()
    final_dt = parse_state_dt(state)
    state_str = fmt_dt(final_dt) if final_dt else 'brak'

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
