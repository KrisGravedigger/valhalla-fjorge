"""LpAgent watermark and cross-check orchestration helpers."""
import csv
import json
import os
from pathlib import Path

_LPAGENT_WATERMARK_DEFAULT = "2026-02-11"


def read_watermark(output_dir: str) -> str:
    """Read last_synced_date from lpagent_sync.json.

    Returns the stored YYYY-MM-DD string, or the hardcoded default
    (2026-02-11, the first day of tracking) if the file does not exist.
    """
    sync_path = Path(output_dir) / "lpagent_sync.json"
    if not sync_path.exists():
        return _LPAGENT_WATERMARK_DEFAULT
    try:
        data = json.loads(sync_path.read_text(encoding="utf-8"))
        return data.get("last_synced_date", _LPAGENT_WATERMARK_DEFAULT)
    except (json.JSONDecodeError, OSError):
        return _LPAGENT_WATERMARK_DEFAULT


def write_watermark(output_dir: str, date: str) -> None:
    """Write last_synced_date to output/lpagent_sync.json."""
    sync_path = Path(output_dir) / "lpagent_sync.json"
    try:
        sync_path.write_text(
            json.dumps({"last_synced_date": date}, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as e:
        print(f"  Warning: could not write lpagent_sync.json: {e}")


def run_cross_check(
    from_date: str,
    to_date: str,
    positions_csv_path: str,
    output_dir: str,
    silent_if_empty: bool = False,
) -> int:
    """Run full cross-check: fetch from LpAgent, compare, append missing rows.

    Returns the count of missing positions found (and backfilled).
    Raises ValueError if LPAGENT_API_KEY is not set.
    """
    from valhalla.lpagent_client import LpAgentClient, DEFAULT_WALLET
    from valhalla.cross_check import CrossChecker

    api_key = os.environ.get("LPAGENT_API_KEY", "")
    if not api_key:
        raise ValueError(
            "LPAGENT_API_KEY is required but not set. "
            "Add it to .env or set it as an environment variable."
        )
    wallet = os.environ.get("LPAGENT_WALLET", DEFAULT_WALLET)

    client = LpAgentClient(
        api_key=api_key,
        wallet=wallet,
        cache_dir=str(Path(output_dir) / "lpagent_cache"),
    )
    raw_positions = client.fetch_range(from_date, to_date)

    checker = CrossChecker(positions_csv_path)
    missing = checker.find_missing(raw_positions)

    if not missing and silent_if_empty:
        return 0

    checker.report(missing)

    if missing:
        checker.backfill(missing)

    return len(missing)


def retro_enrich_lpagent_from_archive(positions_csv_path: str) -> None:
    """Scan archive/ files for events matching existing lpagent backfill rows.

    The normal parse path only reads input/, so Discord events that already got
    archived in a prior run are invisible to merge_with_existing_csv. When
    lpagent cross-check later backfills a row for the same position_id, that
    row stays as lpagent_backfill forever, even though archive/ already holds
    the real open/close events. This function replays those archived events
    through the existing merge logic (Rule 3.5 handles the replacement).
    """
    from valhalla.models import extract_date_from_filename
    from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format
    from valhalla.event_parser import EventParser as _EP
    from valhalla.matcher import PositionMatcher as _PM
    from valhalla.merge import merge_with_existing_csv as _merge
    from valhalla.csv_writer import CsvWriter as _CW
    from valhalla.alias_resolver import apply_aliases

    csv_path = Path(positions_csv_path)
    if not csv_path.exists():
        return

    lpagent_ids = set()
    with open(csv_path, 'r', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            if row.get('pnl_source') == 'lpagent':
                pid = (row.get('position_id') or '').strip()
                if pid:
                    lpagent_ids.add(pid)

    if not lpagent_ids:
        return

    archive_dir = Path('archive')
    if not archive_dir.exists():
        return

    archive_files = sorted([f for f in archive_dir.iterdir()
                            if f.is_file() and f.suffix in ('.txt', '.html')])
    if not archive_files:
        return

    print(f"\n[Retro-enrich] Scanning {len(archive_files)} archive file(s) for {len(lpagent_ids)} lpagent position(s)...")

    event_parser = _EP()
    seen_opens, seen_closes, seen_failsafes, seen_rugs = set(), set(), set(), set()

    for filepath in archive_files:
        try:
            fmt = detect_input_format(str(filepath))
            reader = HtmlReader(str(filepath)) if fmt == 'html' else PlainTextReader(str(filepath))
            messages = reader.read()
            file_date = extract_date_from_filename(str(filepath))
            if not file_date and reader.header_date:
                file_date = reader.header_date
            has_full_ts = any(
                '[' in m.timestamp and 'T' in m.timestamp and len(m.timestamp) > 7
                for m in messages
            )
            if has_full_ts:
                file_date = None
            fp = _EP(base_date=file_date)
            fp.parse_messages(messages)
            for e in fp.open_events:
                if e.position_id in lpagent_ids and e.position_id not in seen_opens:
                    seen_opens.add(e.position_id)
                    event_parser.open_events.append(e)
            for e in fp.close_events:
                if e.position_id in lpagent_ids and e.position_id not in seen_closes:
                    seen_closes.add(e.position_id)
                    event_parser.close_events.append(e)
            for e in fp.failsafe_events:
                if e.position_id in lpagent_ids and e.position_id not in seen_failsafes:
                    seen_failsafes.add(e.position_id)
                    event_parser.failsafe_events.append(e)
            for e in fp.rug_events:
                rpid = getattr(e, 'position_id', None)
                if rpid and rpid in lpagent_ids and rpid not in seen_rugs:
                    seen_rugs.add(rpid)
                    event_parser.rug_events.append(e)
        except Exception as ex:
            print(f"  Warning: failed to parse {filepath.name}: {ex}")
            continue

    n_open = len(event_parser.open_events)
    n_close = len(event_parser.close_events)
    n_fs = len(event_parser.failsafe_events)
    if n_open == 0 and n_close == 0 and n_fs == 0:
        print("  No matching archived events found.")
        return

    print(f"  Found {n_open} open, {n_close} close, {n_fs} failsafe event(s) in archive")

    matcher = _PM(event_parser)
    matched_positions, unmatched_opens = matcher.match_positions({}, {}, use_discord_pnl=False)

    merged_matched, merged_still_open = _merge(
        matched_positions, unmatched_opens, positions_csv_path
    )

    csv_writer = _CW()
    csv_writer.generate_positions_csv(merged_matched, merged_still_open, positions_csv_path)

    # Reapply wallet aliases so target_wallet columns stay normalized
    try:
        apply_aliases(
            csv_path=Path(positions_csv_path),
            aliases_path=Path("wallet_aliases.json")
        )
    except Exception:
        pass

    print(f"  Retro-enriched {positions_csv_path}")
