"""LpAgent watermark and cross-check orchestration helpers."""
import csv
import json
import logging
import os
from pathlib import Path
from typing import Any

from valhalla.lpagent_client import WATERMARK_DEFAULT_DATE, REFRESH_WINDOW_HOURS

logger = logging.getLogger(__name__)

_WATERMARK_FILENAME = "lpagent_sync.json"


def read_watermark(output_dir: str) -> dict:
    """Read the structured watermark from lpagent_sync.json.

    Returns a dict with keys:
      wallet, min_safe_open_date, last_full_refresh_at, refresh_window_hours

    If the file is missing, returns defaults.
    If the file has the old format (last_synced_date key, no wallet key),
    auto-promotes to the new format, logs a migration notice, and writes back.
    """
    sync_path = Path(output_dir) / _WATERMARK_FILENAME
    if not sync_path.exists():
        return _default_watermark()

    try:
        data: Any = json.loads(sync_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return _default_watermark()

    if not isinstance(data, dict):
        return _default_watermark()

    # Detect legacy format: has last_synced_date but no wallet key
    if "last_synced_date" in data and "wallet" not in data:
        promoted = _promote_legacy_watermark(data)
        logger.info(
            "lpagent_sync.json: legacy format detected (last_synced_date=%s) — "
            "promoted to new format with min_safe_open_date=%s",
            data.get("last_synced_date"),
            promoted.get("min_safe_open_date"),
        )
        try:
            sync_path.write_text(
                json.dumps(promoted, indent=2) + "\n", encoding="utf-8"
            )
        except OSError as exc:
            logger.warning("Could not write promoted watermark: %s", exc)
        return promoted

    # New format: return as-is, filling any missing keys with defaults
    defaults = _default_watermark()
    return {**defaults, **data}


def write_watermark(output_dir: str, watermark_dict: Any) -> None:
    """Write the watermark dict to output/lpagent_sync.json.

    Raises TypeError if called with a string (old signature) — callers must
    be updated to build and pass the full dict.
    """
    if isinstance(watermark_dict, str):
        raise TypeError(
            "write_watermark() no longer accepts a date string. "
            "Pass a watermark dict with keys: wallet, min_safe_open_date, "
            "last_full_refresh_at, refresh_window_hours. "
            "See docs/026-lpagent-client-jsonl-rewrite.md §Watermark Schema."
        )
    sync_path = Path(output_dir) / _WATERMARK_FILENAME
    try:
        sync_path.write_text(
            json.dumps(watermark_dict, indent=2) + "\n", encoding="utf-8"
        )
    except OSError as exc:
        logger.warning("Could not write lpagent_sync.json: %s", exc)


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
    from valhalla.lpagent_client import LpAgentClient, DEFAULT_WALLET  # noqa: PLC0415
    from valhalla.cross_check import CrossChecker  # noqa: PLC0415

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
    from valhalla.models import extract_date_from_filename  # noqa: PLC0415
    from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format  # noqa: PLC0415
    from valhalla.event_parser import EventParser as _EP  # noqa: PLC0415
    from valhalla.matcher import PositionMatcher as _PM  # noqa: PLC0415
    from valhalla.merge import merge_with_existing_csv as _merge  # noqa: PLC0415
    from valhalla.csv_writer import CsvWriter as _CW  # noqa: PLC0415
    from valhalla.alias_resolver import apply_aliases  # noqa: PLC0415

    csv_path = Path(positions_csv_path)
    if not csv_path.exists():
        return

    lpagent_ids = set()
    with open(csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("pnl_source") == "lpagent":
                pid = (row.get("position_id") or "").strip()
                if pid:
                    lpagent_ids.add(pid)

    if not lpagent_ids:
        return

    archive_dir = Path("archive")
    if not archive_dir.exists():
        return

    archive_files = sorted(
        [f for f in archive_dir.iterdir() if f.is_file() and f.suffix in (".txt", ".html")]
    )
    if not archive_files:
        return

    print(
        f"\n[Retro-enrich] Scanning {len(archive_files)} archive file(s) "
        f"for {len(lpagent_ids)} lpagent position(s)..."
    )

    event_parser = _EP()
    seen_opens, seen_closes, seen_failsafes, seen_rugs = set(), set(), set(), set()

    for filepath in archive_files:
        try:
            fmt = detect_input_format(str(filepath))
            reader = HtmlReader(str(filepath)) if fmt == "html" else PlainTextReader(str(filepath))
            messages = reader.read()
            file_date = extract_date_from_filename(str(filepath))
            if not file_date and reader.header_date:
                file_date = reader.header_date
            has_full_ts = any(
                "[" in m.timestamp and "T" in m.timestamp and len(m.timestamp) > 7
                for m in messages
            )
            if has_full_ts:
                file_date = None
            fp = _EP(base_date=file_date)
            fp.parse_messages(messages)
            for open_ev in fp.open_events:
                if open_ev.position_id in lpagent_ids and open_ev.position_id not in seen_opens:
                    seen_opens.add(open_ev.position_id)
                    event_parser.open_events.append(open_ev)
            for close_ev in fp.close_events:
                if close_ev.position_id in lpagent_ids and close_ev.position_id not in seen_closes:
                    seen_closes.add(close_ev.position_id)
                    event_parser.close_events.append(close_ev)
            for fs_ev in fp.failsafe_events:
                if fs_ev.position_id in lpagent_ids and fs_ev.position_id not in seen_failsafes:
                    seen_failsafes.add(fs_ev.position_id)
                    event_parser.failsafe_events.append(fs_ev)
            for rug_ev in fp.rug_events:
                rpid = getattr(rug_ev, "position_id", None)
                if rpid and rpid in lpagent_ids and rpid not in seen_rugs:
                    seen_rugs.add(rpid)
                    event_parser.rug_events.append(rug_ev)
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

    merged_matched, merged_still_open = _merge(matched_positions, unmatched_opens, positions_csv_path)

    csv_writer = _CW()
    csv_writer.generate_positions_csv(merged_matched, merged_still_open, positions_csv_path)

    # Reapply wallet aliases so target_wallet columns stay normalized
    try:
        apply_aliases(
            csv_path=Path(positions_csv_path),
            aliases_path=Path("wallet_aliases.json"),
        )
    except Exception:
        pass

    print(f"  Retro-enriched {positions_csv_path}")


# ------------------------------------------------------------------
# Private helpers
# ------------------------------------------------------------------

def _default_watermark() -> dict:
    return {
        "wallet": None,
        "min_safe_open_date": WATERMARK_DEFAULT_DATE,
        "last_full_refresh_at": None,
        "refresh_window_hours": REFRESH_WINDOW_HOURS,
    }


def _promote_legacy_watermark(data: dict) -> dict:
    return {
        "wallet": None,
        "min_safe_open_date": data.get("last_synced_date", WATERMARK_DEFAULT_DATE),
        "last_full_refresh_at": None,
        "refresh_window_hours": REFRESH_WINDOW_HOURS,
    }
