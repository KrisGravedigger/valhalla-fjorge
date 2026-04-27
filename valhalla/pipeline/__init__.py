"""Valhalla Fjorge interactive pipeline package."""

import sys

from .chunking import CHUNK_THRESHOLD_DAYS, now_local
from .menu import menu_first_run, menu_recent, menu_stale
from .runner import (
    pull_day_by_day,
    pull_single,
    run_parser,
    run_pull,
    run_recalc_pending,
)
from .state import load_state, parse_state_dt, save_state
from .summary import print_summary


def main() -> None:
    now = now_local()
    state = load_state()
    last_pull = parse_state_dt(state)

    if last_pull is None:
        skip_pull, start, end, day_by_day = menu_first_run(now)
    else:
        delta_days = (now - last_pull).total_seconds() / 86400
        if delta_days <= CHUNK_THRESHOLD_DAYS:
            skip_pull, start, end, day_by_day = menu_recent(last_pull, now)
        else:
            skip_pull, start, end, day_by_day = menu_stale(last_pull, now)

    should_run_parser = True
    chunks_done = 0

    if not skip_pull and start is not None and end is not None:
        if day_by_day:
            should_run_parser, _, chunks_done = pull_day_by_day(start, end, last_pull)
        else:
            success, should_run_parser = pull_single(start, end)
            if success:
                chunks_done = 1
    else:
        print('\n[main] Pobieranie pominiete - przechodze do parsera.')

    if should_run_parser:
        print('\n[main] Uruchamiam parser...')
        print()
        parser_rc = run_parser()
        if parser_rc != 0:
            print(f'\n[main] Parser zakonczony kodem {parser_rc}.', file=sys.stderr)
    else:
        print('\n[main] Parser pominieto.')
        print_summary(chunks_done, skip_pull)
        sys.exit(1)

    run_recalc_pending(parser_rc=parser_rc)
    print_summary(chunks_done, skip_pull)
