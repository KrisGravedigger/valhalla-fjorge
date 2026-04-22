#!/usr/bin/env python3
"""
Valhalla Bot Discord DM Log Parser v2
Parses Discord DM plain text logs and calculates per-position PnL using Meteora DLMM API.
"""

import json
import os
import re
import argparse
import csv
import shutil
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta
from decimal import Decimal

# Import from valhalla package
import valhalla.analysis_config as _cfg
from valhalla.analysis_config import (
    RECOMMENDATION_LOOKBACK_DAYS,
    PORTFOLIO_TOTAL_SOL,
    MAX_POSITION_FRACTION,
    REDUCE_CAPITAL_CONSECUTIVE_DAYS,
    LOSS_DETAIL_MIN_SOL,
    LOSS_DETAIL_LOOKBACK_DAYS,
    SCORECARD_RECENT_DAYS,
    MIN_FILTER_GAIN_SOL,
    MIN_POSITIONS_FOR_FILTER_REC,
    UTILIZATION_LOOKBACK_HOURS,
    UTILIZATION_LOW_THRESHOLD,
    UTILIZATION_CONSECUTIVE_DAYS,
    UTILIZATION_MAX_INSUF_EVENTS_24H,
)
from valhalla.models import extract_date_from_filename, make_iso_datetime, MeteoraPnlResult, parse_iso_datetime
from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format
from valhalla.event_parser import EventParser
from valhalla.solana_rpc import AddressCache, SolanaRpcClient, PositionResolver
from valhalla.meteora import MeteoraPnlCalculator
from valhalla.matcher import PositionMatcher
from valhalla.csv_writer import CsvWriter
from valhalla.json_io import export_to_json, import_from_json, merge_with_imported
from valhalla.merge import merge_with_existing_csv, merge_positions_csvs
from valhalla.charts import generate_charts, generate_insufficient_balance_chart
from valhalla.alias_resolver import apply_aliases


# ---------------------------------------------------------------------------
# LpAgent cross-check helpers
# ---------------------------------------------------------------------------

_LPAGENT_WATERMARK_DEFAULT = "2026-02-11"


def _read_watermark(output_dir: str) -> str:
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


def _write_watermark(output_dir: str, date: str) -> None:
    """Write last_synced_date to output/lpagent_sync.json."""
    sync_path = Path(output_dir) / "lpagent_sync.json"
    try:
        sync_path.write_text(
            json.dumps({"last_synced_date": date}, indent=2) + "\n",
            encoding="utf-8",
        )
    except OSError as e:
        print(f"  Warning: could not write lpagent_sync.json: {e}")


def _run_cross_check(
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


def _retro_enrich_lpagent_from_archive(positions_csv_path: str) -> None:
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


def _detect_coverage_gaps(positions_csv_path):
    """Detect and report coverage gaps in position timestamps.

    Args:
        positions_csv_path: Path to positions.csv file
    """
    # Check if CSV exists
    csv_path = Path(positions_csv_path)
    if not csv_path.exists():
        return

    # Read all timestamps from CSV
    timestamps = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Parse datetime_open
                dt_open = row.get('datetime_open', '').strip()
                if dt_open:
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M'):
                        try:
                            timestamps.append(datetime.strptime(dt_open, fmt))
                            break
                        except ValueError:
                            continue

                # Parse datetime_close
                dt_close = row.get('datetime_close', '').strip()
                if dt_close:
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M'):
                        try:
                            timestamps.append(datetime.strptime(dt_close, fmt))
                            break
                        except ValueError:
                            continue
    except Exception:
        return  # Silently fail on any CSV read error

    # Need at least 2 timestamps to detect gaps
    if len(timestamps) < 2:
        return

    # Sort timestamps
    timestamps.sort()

    # Calculate consecutive gaps in minutes
    gaps = []
    for i in range(len(timestamps) - 1):
        gap_minutes = (timestamps[i + 1] - timestamps[i]).total_seconds() / 60
        gaps.append((gap_minutes, timestamps[i], timestamps[i + 1]))

    # Calculate median gap
    gap_values = [g[0] for g in gaps]
    median_gap = statistics.median(gap_values)

    # Dynamic threshold: max(180, min(median * 20, 360))
    threshold_minutes = max(180, min(median_gap * 20, 360))

    # Filter gaps above threshold
    significant_gaps = [(gap_min, start, end) for gap_min, start, end in gaps if gap_min > threshold_minutes]

    # Format time values (hours if >= 60, else minutes)
    def format_time(minutes):
        if minutes >= 60:
            return f"{minutes / 60:.1f}h"
        else:
            return f"{int(minutes)}m"

    # Load allowlist (paste full gap lines or just "MM-DD HH:MM -> MM-DD HH:MM")
    allowlist = set()
    allowlist_path = csv_path.parent / 'gap_allowlist.txt'
    if allowlist_path.exists():
        for line in allowlist_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Extract date range: "MM-DD HH:MM -> MM-DD HH:MM" from anywhere in line
            m = re.search(r'(\d{2}-\d{2} \d{2}:\d{2}) -> (\d{2}-\d{2} \d{2}:\d{2})', line)
            if m:
                allowlist.add(f"{m.group(1)} -> {m.group(2)}")

    # Print results
    threshold_str = format_time(threshold_minutes)
    median_str = format_time(median_gap)

    if significant_gaps:
        # Sort chronologically (oldest first)
        significant_gaps.sort(key=lambda x: x[1])

        new_gaps = []
        allowed_count = 0
        for gap_min, start, end in significant_gaps:
            start_str = start.strftime('%m-%d %H:%M')
            end_str = end.strftime('%m-%d %H:%M')
            range_key = f"{start_str} -> {end_str}"
            if range_key in allowlist:
                allowed_count += 1
            else:
                new_gaps.append((gap_min, start_str, end_str))

        if new_gaps:
            print(f"\nCoverage Gaps (threshold: {threshold_str}, median: {median_str})")
            for gap_min, start_str, end_str in new_gaps:
                gap_str = format_time(gap_min)
                print(f"  ! {gap_str} gap: {start_str} -> {end_str}")
            if allowed_count:
                print(f"  ({allowed_count} allowed gap(s) hidden - see gap_allowlist.txt)")
            print(f"  {len(new_gaps)} potential gap(s) detected")
        elif allowed_count:
            print(f"\nNo new coverage gaps (threshold: {threshold_str}, {allowed_count} allowed gap(s) hidden)")
    else:
        print(f"\nNo coverage gaps detected (threshold: {threshold_str}, median: {median_str})")


def _recover_insufficient_balance_history(output_dir: str) -> None:
    """Scan archive files and recover all insufficient_balance events into CSV."""
    from valhalla.models import extract_date_from_filename
    from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format
    from valhalla.event_parser import EventParser
    from valhalla.csv_writer import CsvWriter

    archive_dir = Path('archive')
    if not archive_dir.exists():
        print("No archive/ directory found")
        return

    archive_files = [f for f in archive_dir.iterdir()
                     if f.is_file() and f.suffix in ('.txt', '.html')]

    if not archive_files:
        print("No .txt or .html files in archive/")
        return

    print(f"Scanning {len(archive_files)} archive file(s) for insufficient balance events...")

    all_events = []
    for filepath in sorted(archive_files):
        fmt = detect_input_format(str(filepath))
        if fmt == 'html':
            reader = HtmlReader(str(filepath))
        else:
            reader = PlainTextReader(str(filepath))

        messages = reader.read()

        # Detect date for this file
        file_date = extract_date_from_filename(str(filepath))
        if not file_date and reader.header_date:
            file_date = reader.header_date

        # Check for embedded timestamps
        has_full_timestamps = any(
            '[' in msg.timestamp and 'T' in msg.timestamp and len(msg.timestamp) > 7
            for msg in messages
        )
        if has_full_timestamps:
            file_date = None  # dates embedded

        file_parser = EventParser(base_date=file_date)
        file_parser.parse_messages(messages)

        if file_parser.insufficient_balance_events:
            all_events.extend(file_parser.insufficient_balance_events)
            print(f"  {filepath.name}: {len(file_parser.insufficient_balance_events)} event(s)")

    if all_events:
        insuf_csv = Path(output_dir) / 'insufficient_balance.csv'
        csv_writer = CsvWriter()
        csv_writer.generate_insufficient_balance_csv(all_events, str(insuf_csv))
        print(f"\nRecovered {len(all_events)} insufficient balance events -> {insuf_csv}")
    else:
        print("No insufficient balance events found in archive files")


# ---------------------------------------------------------------------------
# Loss analysis helpers
# ---------------------------------------------------------------------------


def _generate_wallet_recommendations(positions: List) -> List[str]:
    """
    Analyze per-wallet daily data and return recommendation lines.

    Rules:
      A — "Verify or change tracked wallet":
          avg positions/day < 3 OR any single day has >= 2 loss events (rug/SL/failsafe)
      B — "Investigate underperformance":
          wallet has Daily PnL% ROI < 0.02% for >= 3 consecutive days
      C — "Consider increasing tracking level":
          wallet has >= 3 consecutive days where daily PnL SOL < portfolio avg SOL
          AND daily PnL% ROI > portfolio avg % ROI

    Args:
        positions: Full list of positions (all close_reasons).

    Returns:
        List of human-readable recommendation strings.
        Empty list if no recommendations.
    """
    from collections import defaultdict
    from datetime import date, timedelta

    LOSS_R = {
        "stop_loss", "rug", "rug_unknown_open",
        "failsafe", "failsafe_unknown_open",
        "stop_loss_unknown_open",
    }

    # Only closed positions with valid datetime_close and pnl_sol
    closed = [
        p for p in positions
        if p.close_reason != "still_open"
        and p.pnl_sol is not None
        and getattr(p, 'datetime_close', None)
        and p.datetime_close
    ]

    if not closed:
        return []

    # Compute reference date (most recent close) and 3-day cutoff
    all_close_dates = []
    for pos in closed:
        dt = parse_iso_datetime(pos.datetime_close)
        if dt is not None:
            all_close_dates.append(dt.date())

    if not all_close_dates:
        return []

    reference_date = max(all_close_dates)
    cutoff_3d = reference_date - timedelta(days=3)

    # Build: wallet -> date -> list of positions
    daily_by_wallet: Dict[str, Dict[date, List]] = defaultdict(lambda: defaultdict(list))
    for pos in closed:
        if getattr(pos, 'target_wallet', None) in (None, 'unknown'):
            continue
        dt = parse_iso_datetime(pos.datetime_close)
        if dt is None:
            continue
        daily_by_wallet[pos.target_wallet][dt.date()].append(pos)

    if not daily_by_wallet:
        return []

    # Compute portfolio averages per date
    # portfolio_avg_sol[d] = mean of all wallets' daily pnl_sol on date d
    # portfolio_avg_pct[d] = mean of all wallets' daily pnl_pct on date d (wallets with deployed > 0)
    all_dates: set = set()
    for wallet_days in daily_by_wallet.values():
        all_dates.update(wallet_days.keys())

    portfolio_avg_sol: Dict[date, float] = {}
    portfolio_avg_pct: Dict[date, float] = {}

    for d in all_dates:
        sol_vals = []
        pct_vals = []
        for wallet_days in daily_by_wallet.values():
            day_positions = wallet_days.get(d, [])
            if not day_positions:
                continue
            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            sol_vals.append(day_sol)
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )
            if day_deployed > 0:
                day_pct = day_sol / day_deployed * 100.0
                pct_vals.append(day_pct)
        if sol_vals:
            portfolio_avg_sol[d] = sum(sol_vals) / len(sol_vals)
        if pct_vals:
            portfolio_avg_pct[d] = sum(pct_vals) / len(pct_vals)

    recommendations: List[str] = []

    for wallet, wallet_days in daily_by_wallet.items():
        sorted_dates = sorted(wallet_days.keys())
        wallet_recs: List[str] = []

        # ----------------------------------------------------------------
        # Rule A: avg positions/day < 3 (over all history) OR any day
        #         within the recent 3-day window with >= 2 loss events
        # ----------------------------------------------------------------
        total_positions_count = sum(len(wallet_days[d]) for d in sorted_dates)
        num_days = len(sorted_dates)
        avg_per_day = total_positions_count / num_days if num_days > 0 else 0.0

        rule_a_reasons = []
        if avg_per_day < 3:
            rule_a_reasons.append(f"avg {avg_per_day:.1f} pos/day")

        # Only flag days within the 3-day window
        for d in sorted_dates:
            if d < cutoff_3d:
                continue
            loss_count_day = sum(
                1 for p in wallet_days[d] if p.close_reason in LOSS_R
            )
            if loss_count_day >= 2:
                rule_a_reasons.append(f"2+ losses on {d}")

        if rule_a_reasons:
            reason_str = ", ".join(rule_a_reasons)
            wallet_recs.append(
                f"{wallet}: Verify or change tracked wallet ({reason_str})"
            )

        # ----------------------------------------------------------------
        # Rule B: Daily PnL% ROI < 0.02% for >= 3 consecutive days
        # ----------------------------------------------------------------
        consec_b = 0
        b_start: Optional[date] = None
        b_end: Optional[date] = None
        best_b_streak = 0
        best_b_start: Optional[date] = None
        best_b_end: Optional[date] = None

        for d in sorted_dates:
            day_positions = wallet_days[d]
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )
            if day_deployed <= 0:
                # Reset streak — no deployed means can't compute ROI
                consec_b = 0
                b_start = None
                b_end = None
                continue

            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            day_pct = day_sol / day_deployed * 100.0

            if day_pct < 0.02:
                if consec_b == 0:
                    b_start = d
                b_end = d
                consec_b += 1
                if consec_b > best_b_streak:
                    best_b_streak = consec_b
                    best_b_start = b_start
                    best_b_end = b_end
            else:
                consec_b = 0
                b_start = None
                b_end = None

        # Only flag if the streak ends within the 3-day window
        if best_b_streak >= 3 and best_b_start and best_b_end and best_b_end >= cutoff_3d:
            wallet_recs.append(
                f"{wallet}: Investigate underperformance "
                f"(Daily ROI < 0.02% for {best_b_streak} consecutive days: "
                f"{best_b_start} to {best_b_end})"
            )

        # ----------------------------------------------------------------
        # Rule C: >= 3 consecutive days where daily PnL SOL < portfolio avg SOL
        #         AND daily PnL% ROI > portfolio avg % ROI
        # ----------------------------------------------------------------
        consec_c = 0
        c_start: Optional[date] = None
        c_end: Optional[date] = None
        best_c_streak = 0
        best_c_start: Optional[date] = None
        best_c_end: Optional[date] = None

        for d in sorted_dates:
            if d not in portfolio_avg_sol or d not in portfolio_avg_pct:
                consec_c = 0
                c_start = None
                c_end = None
                continue

            day_positions = wallet_days[d]
            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )

            if day_deployed <= 0:
                consec_c = 0
                c_start = None
                c_end = None
                continue

            day_pct = day_sol / day_deployed * 100.0
            port_sol = portfolio_avg_sol[d]
            port_pct = portfolio_avg_pct[d]

            if day_sol < port_sol and day_pct > port_pct:
                if consec_c == 0:
                    c_start = d
                c_end = d
                consec_c += 1
                if consec_c > best_c_streak:
                    best_c_streak = consec_c
                    best_c_start = c_start
                    best_c_end = c_end
            else:
                consec_c = 0
                c_start = None
                c_end = None

        # Only flag if the streak ends within the 3-day window
        if best_c_streak >= 3 and best_c_start and best_c_end and best_c_end >= cutoff_3d:
            wallet_recs.append(
                f"{wallet}: Consider increasing tracking level "
                f"(good ROI% but low absolute SOL for {best_c_streak} consecutive days: "
                f"{best_c_start} to {best_c_end})"
            )

        # ----------------------------------------------------------------
        # Rule F: wallet's daily pnl_pct < portfolio_avg_pct for N
        #         consecutive days → reduce capital
        # ----------------------------------------------------------------
        consec_f = 0
        f_start: Optional[date] = None
        f_end: Optional[date] = None
        best_f_streak = 0
        best_f_start: Optional[date] = None
        best_f_end: Optional[date] = None

        for d in sorted_dates:
            # Only count days where BOTH wallet AND portfolio have pnl_pct data
            if d not in portfolio_avg_pct:
                consec_f = 0
                f_start = None
                f_end = None
                continue

            day_positions = wallet_days[d]
            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )

            if day_deployed <= 0:
                # Skip days where we can't compute wallet pnl_pct
                consec_f = 0
                f_start = None
                f_end = None
                continue

            day_pct = day_sol / day_deployed * 100.0
            port_pct = portfolio_avg_pct[d]

            if day_pct < port_pct:
                if consec_f == 0:
                    f_start = d
                f_end = d
                consec_f += 1
                if consec_f > best_f_streak:
                    best_f_streak = consec_f
                    best_f_start = f_start
                    best_f_end = f_end
            else:
                consec_f = 0
                f_start = None
                f_end = None

        # Flag if streak >= threshold and ends within the 3-day window
        if (best_f_streak >= REDUCE_CAPITAL_CONSECUTIVE_DAYS
                and best_f_start and best_f_end
                and best_f_end >= cutoff_3d):
            wallet_recs.append(
                f"[REDUCE] {wallet}: underperforming (PnL%) for {best_f_streak} consecutive days "
                f"vs. portfolio avg — consider reducing capital"
            )

        recommendations.extend(wallet_recs)

    # ----------------------------------------------------------------
    # Rule D: sweet spot not at lowest threshold (per wallet + aggregate)
    # ----------------------------------------------------------------
    from valhalla.loss_analyzer import FilterBacktester as _FilterBacktester

    def _fmt_threshold_d(param: str, threshold: float) -> str:
        """Format threshold for Rule D messages."""
        if param == "mc_at_open":
            if threshold >= 1_000_000:
                return f"${threshold / 1_000_000:.1f}M"
            elif threshold >= 1_000:
                return f"${threshold / 1_000:.0f}K"
            return f"${threshold:.0f}"
        elif param == "token_age_hours":
            if threshold < 24:
                return f"{threshold:.0f}h"
            return f"{int(threshold // 24)}d"
        else:
            return f"{threshold:.0f}" if threshold == int(threshold) else str(threshold)

    PARAM_DISPLAY_D = {
        "jup_score": "jup_score",
        "mc_at_open": "mc_at_open",
        "token_age_hours": "token_age_hours",
    }

    def _run_rule_d(label: str, rule_d_positions: List) -> List[str]:
        """Run Rule D for a given set of positions; label is wallet name or 'Portfolio'."""
        rule_d_recs: List[str] = []
        bt = _FilterBacktester()
        bt_results = bt.sweep_all(rule_d_positions)
        for param, bt_rows in bt_results.items():
            if not bt_rows or len(bt_rows) < 2:
                continue
            # Skip params where all net_sol_impact <= 0
            if all(r.net_sol_impact <= Decimal("0") for r in bt_rows):
                continue
            # Find sweet spot (highest positive net_sol_impact)
            best_idx = None
            best_impact = Decimal("0")
            for i, brow in enumerate(bt_rows):
                if brow.net_sol_impact > best_impact:
                    best_impact = brow.net_sol_impact
                    best_idx = i
            if best_idx is None or best_idx == 0:
                continue  # sweet spot is already at lowest threshold — no recommendation
            if best_impact < Decimal(str(MIN_FILTER_GAIN_SOL)):
                continue  # net gain too small to be material
            sweet_threshold = bt_rows[best_idx].threshold
            min_threshold = bt_rows[0].threshold
            param_display = PARAM_DISPLAY_D.get(param, param)
            rule_d_recs.append(
                f"{label}: Consider tightening {param_display} filter — "
                f"sweet spot at >= {_fmt_threshold_d(param, sweet_threshold)}, "
                f"not the minimum (>= {_fmt_threshold_d(param, min_threshold)}). "
                f"Net gain: +{best_impact:.3f} SOL"
            )
        return rule_d_recs

    # Per-wallet Rule D (MIN_POSITIONS_FOR_FILTER_REC+ closed positions)
    for wallet in daily_by_wallet:
        wallet_all_positions = [
            p for p in positions
            if getattr(p, 'target_wallet', None) == wallet
        ]
        closed_wallet_d = [
            p for p in wallet_all_positions
            if p.close_reason not in ("still_open", "unknown_open")
        ]
        if len(closed_wallet_d) < MIN_POSITIONS_FOR_FILTER_REC:
            continue
        recommendations.extend(_run_rule_d(wallet, wallet_all_positions))

    # Aggregate Rule D (Portfolio)
    all_closed_d = [
        p for p in positions
        if p.close_reason not in ("still_open", "unknown_open")
    ]
    if len(all_closed_d) >= 10:
        recommendations.extend(_run_rule_d("Portfolio", positions))

    return recommendations


def _filter_recent_positions(positions: List, days: int) -> List:
    """Return only positions whose datetime_open falls within the last `days` days.
    If days <= 0, returns the full list unchanged.
    """
    if days <= 0 or not positions:
        return positions
    dates = [parse_iso_datetime(getattr(p, "datetime_open", None) or "") for p in positions]
    valid_dates = [d for d in dates if d is not None]
    if not valid_dates:
        return positions
    ref = max(valid_dates)
    from datetime import timedelta
    cutoff = ref - timedelta(days=days)
    return [
        p for p, d in zip(positions, dates)
        if d is not None and d >= cutoff
    ]


def _check_position_size_guard(
    positions: List,
    portfolio_sol: float,
    max_fraction: float,
) -> List[str]:
    """
    Check if any position exceeds max_fraction of portfolio_sol.

    Returns list of action item strings (warnings + recommendations).
    Empty list if portfolio_sol <= 0 (feature disabled).
    """
    if portfolio_sol <= 0:
        return []

    max_sol = Decimal(str(portfolio_sol)) * Decimal(str(max_fraction))

    # Find positions exceeding the limit (use RECOMMENDATION_LOOKBACK_DAYS window)
    recent = _filter_recent_positions(positions, RECOMMENDATION_LOOKBACK_DAYS)

    from collections import defaultdict
    oversized_by_wallet: dict = defaultdict(list)
    for pos in recent:
        deployed = getattr(pos, "sol_deployed", None)
        if deployed is not None and deployed > max_sol:
            oversized_by_wallet[pos.target_wallet].append(deployed)

    items: List[str] = []
    for wallet, sizes in oversized_by_wallet.items():
        largest = max(sizes)
        items.append(
            f"WARN {wallet}: position {largest:.3f} SOL exceeds "
            f"1/{round(1/max_fraction):.0f} portfolio limit ({max_sol:.2f} SOL) "
            f"— consider reducing position size"
        )
    return items


def _build_action_items(
    result: object,
    positions: List,
    wallet_recs: object = None,
    insufficient_balance_events: List = None,
    util_points: List = None,
) -> List[str]:
    """
    Build a prioritized list of action item strings for the report.

    Combines scorecard-based triggers with the existing Rules A-D from
    _generate_wallet_recommendations(), plus Rule E (insufficient balance).

    Priority order in output:
      0. position size guard warnings (Feature 1)
      1. consider_replacing wallets
      2. increase_capital wallets
      3. Rule F: consecutive underperformance → reduce capital
      4. insufficient balance warnings (Rule E)
      5. filter sweet-spot recommendations (Rule D)
      6. inactive wallets
      7. deteriorating wallets (WalletTrendAnalyzer flags)
      8. remaining A-B-C rules

    Args:
        result: LossAnalysisResult from LossAnalyzer.analyze().
        positions: Full list of MatchedPosition (passed to _generate_wallet_recommendations).
        wallet_recs: Optional pre-computed wallet recommendations (reserved for doc 007).
        insufficient_balance_events: List[InsufficientBalanceEvent] from event parser.

    Returns:
        List of recommendation strings, each starting with a wallet name or
        "Portfolio:".
    """
    replacing: List[str] = []
    increasing: List[str] = []
    inactive_items: List[str] = []

    for sc in result.wallet_scorecards:
        # consider_replacing triggers
        if sc.status == "consider_replacing":
            if sc.pnl_7d_sol < Decimal("0"):
                replacing.append(
                    f"{sc.wallet}: negative 7d PnL ({sc.pnl_7d_sol:+.3f} SOL) "
                    f"across {sc.closed_positions} positions — candidate for replacement"
                )
            elif sc.win_rate_7d_pct is not None and sc.win_rate_7d_pct < 45.0:
                replacing.append(
                    f"{sc.wallet}: 7d win rate dropped to {sc.win_rate_7d_pct:.0f}% "
                    f"(overall: {sc.win_rate_pct:.0f}%) — consider replacing"
                )
            else:
                # fallback if neither sub-condition is specifically True
                wr_str = f"{sc.win_rate_7d_pct:.0f}%" if sc.win_rate_7d_pct is not None else "N/A"
                replacing.append(
                    f"{sc.wallet}: poor performance — candidate for replacement "
                    f"(PnL: {sc.total_pnl_sol:+.3f} SOL, WR 7d: {wr_str})"
                )

        # Win rate decline trigger (separate bullet, regardless of status)
        if sc.win_rate_trend_pp is not None and sc.win_rate_trend_pp < -15.0:
            replacing.append(
                f"{sc.wallet}: win rate declining — {sc.win_rate_7d_pct:.0f}% (7d) "
                f"vs {sc.win_rate_pct:.0f}% (overall)"
            )

        # High rug rate trigger (separate bullet, regardless of status)
        if sc.rug_rate_pct > 15.0:
            replacing.append(
                f"{sc.wallet}: high rug rate ({sc.rug_rate_pct:.0f}%) — "
                f"wallet trades riskier tokens"
            )

        # increase_capital trigger
        if sc.status == "increase_capital":
            wr_7d = sc.win_rate_7d_pct if sc.win_rate_7d_pct is not None else sc.win_rate_pct
            increasing.append(
                f"{sc.wallet}: 7d win rate {wr_7d:.0f}% across {sc.closed_positions} positions "
                f"— consider increasing capital"
            )

        # inactive trigger
        if sc.status == "inactive" and sc.days_since_last_position is not None:
            inactive_items.append(
                f"{sc.wallet}: no activity for {sc.days_since_last_position}+ days "
                f"— verify wallet is still active"
            )

    # Filter positions to the recommendation lookback window (applies to Rules A-D and Rule E)
    recent_positions = _filter_recent_positions(positions, RECOMMENDATION_LOOKBACK_DAYS)

    # Rule E: Insufficient balance events
    insuf_items: List[str] = []
    if insufficient_balance_events:
        from valhalla.loss_analyzer import InsufficientBalanceAnalyzer
        ib_results = InsufficientBalanceAnalyzer().analyze(
            insufficient_balance_events, recent_positions
        )
        for ib in ib_results:
            rate_pct = ib.rate * 100
            insuf_items.append(
                f"{ib.wallet}: {ib.total_events} insufficient balance events "
                f"({rate_pct:.0f}% of {ib.total_positions} positions, "
                f"avg. required {ib.avg_required_sol:.2f} SOL) "
                f"— consider increasing SOL balance or decreasing position size"
            )

    # Utilization-based suggestion (Doc 009)
    utilization_items: List[str] = []
    if PORTFOLIO_TOTAL_SOL > 0 and util_points is not None:
        from valhalla.utilization import check_low_utilization_days
        low_util = check_low_utilization_days(
            util_points,
            Decimal(str(PORTFOLIO_TOTAL_SOL)),
            UTILIZATION_LOW_THRESHOLD,
            UTILIZATION_CONSECUTIVE_DAYS,
        )
        if low_util:
            # Count insuf-balance events in last 24h
            insuf_24h = 0
            if insufficient_balance_events:
                cutoff_dt = datetime.now() - timedelta(hours=24)
                cutoff_date = cutoff_dt.date()
                for ev in insufficient_balance_events:
                    # Support both InsufficientBalanceEvent (.date/.timestamp)
                    # and _InsuFEvent (.event_date) from CSV loader
                    ev_date = getattr(ev, "event_date", None)
                    if ev_date is not None:
                        # _InsuFEvent: event_date is datetime.date
                        if ev_date >= cutoff_date:
                            insuf_24h += 1
                    else:
                        # InsufficientBalanceEvent: has .date and .timestamp
                        ev_dt = parse_iso_datetime(
                            make_iso_datetime(ev.date, ev.timestamp) if ev.date else ev.timestamp
                        )
                        if ev_dt and ev_dt >= cutoff_dt:
                            insuf_24h += 1
            if insuf_24h <= UTILIZATION_MAX_INSUF_EVENTS_24H:
                # Find wallets with status "increase_capital"
                ic_wallets = [
                    sc.wallet for sc in result.wallet_scorecards
                    if sc.status == "increase_capital"
                ]
                for w in ic_wallets:
                    utilization_items.append(
                        f"{w}: capital utilization below "
                        f"{UTILIZATION_LOW_THRESHOLD*100:.0f}% for "
                        f"{UTILIZATION_CONSECUTIVE_DAYS} consecutive days — "
                        f"consider increasing capital per position"
                    )

    # Position size guard warnings (Feature 1) — highest priority
    size_guard_items = _check_position_size_guard(positions, PORTFOLIO_TOTAL_SOL, MAX_POSITION_FRACTION)

    # Existing Rules A-D and Rule F (filtered to the same lookback window)
    existing_recs = _generate_wallet_recommendations(recent_positions)

    # Extract Rule F items (prefixed with "[REDUCE] ") from recommendations
    reduce_capital_items = [
        r[len("[REDUCE] "):] for r in existing_recs if r.startswith("[REDUCE] ")
    ]
    non_reduce_recs = [r for r in existing_recs if not r.startswith("[REDUCE] ")]

    filter_recs = [r for r in non_reduce_recs if "sweet spot" in r.lower() or "tightening" in r.lower()]
    other_recs = [r for r in non_reduce_recs if r not in filter_recs]

    # Deteriorating flag from result.wallet_flags
    deteriorating_recs = [
        f"{wf.wallet}: deteriorating stop-loss rate — {wf.message}"
        for wf in result.wallet_flags
        if wf.flag == "deteriorating"
    ]

    all_items = (
        size_guard_items + replacing + increasing + reduce_capital_items
        + insuf_items + utilization_items + filter_recs
        + inactive_items + deteriorating_recs + other_recs
    )
    return all_items


def _scenario_label(scenario: Optional[str]) -> str:
    """Map source wallet scenario to a human-readable label."""
    return {
        "source_held_longer": "Held longer (SL too tight)",
        "source_exited_early": "Exited before us (copy lag)",
        "source_recovered": "Recovered (unclear mechanism)",
        "both_loss": "Both lost",
        "comparable": "Comparable outcome",
    }.get(scenario or "", "No data")


def _fmt_sol(val: Optional[Decimal]) -> str:
    """Format SOL value or return 'N/A'."""
    return f"{val:.4f} SOL" if val is not None else "N/A"


def _fmt_pct(val: Optional[float]) -> str:
    """Format percentage with sign or return 'N/A'."""
    return f"{val:+.1f}%" if val is not None else "N/A"


def _fmt_mc(val: float) -> str:
    """Format market cap: 1_500_000 -> '$1.5M'."""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    elif val >= 1_000:
        return f"${val / 1_000:.0f}K"
    return f"${val:.0f}"


def _scorecard_action_hints(wallet: str, action_items: List[str]) -> str:
    """Return brief comma-separated action hints for a wallet from the action items list."""
    hints: List[str] = []
    for item in action_items:
        if not (item.startswith(f"{wallet}:") or item.startswith(f"WARN {wallet}:")):
            continue
        il = item.lower()
        if "portfolio limit" in il:
            if "↓ size" not in hints:
                hints.append("↓ size")
        elif "candidate for replacement" in il or "consider replacing" in il or "poor performance" in il:
            if "replace" not in hints:
                hints.append("replace")
        elif "win rate declining" in il:
            if "↓ WR" not in hints:
                hints.append("↓ WR")
        elif "high rug rate" in il:
            if "high rug" not in hints:
                hints.append("high rug")
        elif "capital utilization below" in il:
            if "↑ capital (util)" not in hints:
                hints.append("↑ capital (util)")
        elif "increasing capital" in il or "increase capital" in il:
            if "↑ capital" not in hints:
                hints.append("↑ capital")
        elif "reduce capital" in il or "reducing capital" in il:
            if "↓ capital" not in hints:
                hints.append("↓ capital")
        elif "insufficient balance" in il:
            if "↑ SOL" not in hints:
                hints.append("↑ SOL")
        elif "sweet spot" in il or "tightening" in il:
            if "tighten filter" not in hints:
                hints.append("tighten filter")
        elif "verify or change" in il or "no activity" in il:
            if "verify" not in hints:
                hints.append("verify")
        elif "pos/day" in il:
            if "low activity" not in hints:
                hints.append("low activity")
        elif "deteriorating" in il:
            if "↑ SL" not in hints:
                hints.append("↑ SL")
    # Resolve contradictory capital signals
    if "↑ capital" in hints and "↓ capital" in hints:
        hints = [h for h in hints if h not in ("↑ capital", "↓ capital")]
        hints.append("⚠️ mixed capital signal")

    return ", ".join(hints) if hints else "—"


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    """Render a markdown table string."""
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    def _pad(text: str, width: int) -> str:
        return str(text).ljust(width)

    header_line = "| " + " | ".join(_pad(h, col_widths[i]) for i, h in enumerate(headers)) + " |"
    sep_line = "| " + " | ".join("-" * w for w in col_widths) + " |"
    data_lines = [
        "| " + " | ".join(_pad(str(cell), w) for cell, w in zip(row, col_widths)) + " |"
        for row in rows
    ]
    return "\n".join([header_line, sep_line] + data_lines)


def _build_loss_detail_table(positions: List) -> str:
    """
    Build a markdown table of recent large losses.

    Filters: pnl_sol <= -LOSS_DETAIL_MIN_SOL (any close_reason except still_open),
    datetime_close within last LOSS_DETAIL_LOOKBACK_DAYS days.
    Sorted by pnl_sol ascending (largest loss first).
    """
    from datetime import timedelta

    header = "## 3. Recent Large Losses {#large-losses}"
    min_sol = Decimal(str(LOSS_DETAIL_MIN_SOL))

    # Collect qualifying positions: any closed position with loss >= threshold
    candidates = []
    for p in positions:
        if p.close_reason == "still_open":
            continue
        pnl = getattr(p, "pnl_sol", None)
        if pnl is None or pnl > -min_sol:
            continue
        dt_close = getattr(p, "datetime_close", None)
        if not dt_close:
            continue
        candidates.append(p)

    empty_msg = (
        f"{header}\n\n"
        f"_No losses above {LOSS_DETAIL_MIN_SOL:.2f} SOL in the last "
        f"{LOSS_DETAIL_LOOKBACK_DAYS} days._\n"
    )

    if not candidates:
        return empty_msg

    # Filter by datetime_close within last LOSS_DETAIL_LOOKBACK_DAYS days
    close_dates = []
    for p in candidates:
        dt = parse_iso_datetime(p.datetime_close)
        close_dates.append(dt)

    valid_close = [(p, d) for p, d in zip(candidates, close_dates) if d is not None]
    if not valid_close:
        return empty_msg

    ref_date = max(d for _, d in valid_close)
    cutoff = ref_date - timedelta(days=LOSS_DETAIL_LOOKBACK_DAYS)
    recent_losses = [(p, d) for p, d in valid_close if d >= cutoff]

    if not recent_losses:
        return empty_msg

    # Sort by datetime_open ascending (chronological order)
    recent_losses.sort(key=lambda x: getattr(x[0], "datetime_open", "") or "")

    include_portfolio_pct = PORTFOLIO_TOTAL_SOL > 0
    portfolio_dec = Decimal(str(PORTFOLIO_TOTAL_SOL)) if include_portfolio_pct else None

    headers = ["Open", "Close", "Wallet", "Token", "ID", "Reason", "Loss (SOL)", "Loss (%)", "Source PnL (%)", "Source hold (min)", "Source action"]
    if include_portfolio_pct:
        headers.append("% portfolio")

    rows = []
    for p, _ in recent_losses:
        pnl = p.pnl_sol
        pnl_pct = getattr(p, "pnl_pct", None)
        source_pnl_pct = getattr(p, "source_wallet_pnl_pct", None)
        source_hold = getattr(p, "source_wallet_hold_min", None)
        scenario = getattr(p, "source_wallet_scenario", None)

        open_str = getattr(p, "datetime_open", None) or "N/A"
        close_str = p.datetime_close if p.datetime_close else "N/A"
        token_pair_str = p.token if p.token else "N/A"
        position_id_str = p.position_id[:8] if getattr(p, "position_id", None) else "N/A"
        loss_sol_str = f"-{abs(pnl):.3f}"
        loss_pct_str = f"{float(pnl_pct):.1f}%" if pnl_pct is not None else "N/A"
        src_pnl_str = f"{float(source_pnl_pct):.1f}%" if source_pnl_pct is not None else "N/A"
        src_hold_str = str(source_hold) if source_hold is not None else "N/A"

        row = [
            open_str,
            close_str,
            p.target_wallet,
            token_pair_str,
            position_id_str,
            p.close_reason,
            loss_sol_str,
            loss_pct_str,
            src_pnl_str,
            src_hold_str,
            _scenario_label(scenario),
        ]
        if include_portfolio_pct:
            portfolio_pct_val = abs(pnl) / portfolio_dec * 100
            row.append(f"{float(portfolio_pct_val):.1f}%")
        rows.append(row)

    table_str = _md_table(headers, rows)
    return f"{header}\n\n{table_str}\n"


def _load_insuf_balance_csv(csv_path: str) -> List:
    """Load insufficient balance events from CSV.

    Returns simple objects with .target, .required_amount, .event_date (date | None).
    """
    import csv as _csv
    from datetime import date as _date
    from pathlib import Path

    class _InsuFEvent:
        __slots__ = ("target", "required_amount", "event_date")
        def __init__(self, target: str, required_amount: float, event_date):
            self.target = target
            self.required_amount = required_amount
            self.event_date = event_date  # datetime.date or None

    path = Path(csv_path)
    if not path.exists():
        return []
    events = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                target = row.get("target_wallet", "").strip()
                if not target:
                    continue
                try:
                    req = float(row.get("required_amount", 0))
                except ValueError:
                    req = 0.0
                # Parse event_date from the "datetime" column (ISO format: YYYY-MM-DDTHH:MM:SS)
                event_date = None
                raw_dt = row.get("datetime", "").strip()
                if raw_dt:
                    try:
                        event_date = _date.fromisoformat(raw_dt[:10])
                    except ValueError:
                        pass
                events.append(_InsuFEvent(target, req, event_date))
    except Exception:
        pass
    return events


def _generate_loss_report(
    positions: List,
    output_path: str,
    insufficient_balance_csv: str = None,
) -> None:
    """Generate loss_analysis.md from matched positions."""
    from valhalla.loss_analyzer import (
        LossAnalyzer, FilterBacktester, LOSS_REASONS,
    )
    from valhalla import recommendations_tracker as _tracker

    analyzer = LossAnalyzer()
    result = analyzer.analyze(positions)
    inactive_wallets = {sc.wallet for sc in result.wallet_scorecards if sc.status == "inactive" and sc.wallet}

    # Load persistent recommendation state
    state_path = str(Path(output_path).parent / ".recommendations_state.json")
    rec_state = _tracker.load_state(state_path)
    wallet_recs = _generate_wallet_recommendations(positions)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    date_str = datetime.now().strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # Local helpers (preserved from original function)
    # ------------------------------------------------------------------
    def _fmt_age_hours(hours: float) -> str:
        return f"{hours:.0f}h" if hours < 24 else f"{hours / 24:.0f}d"

    def _fmt_age_threshold(threshold: float) -> str:
        """Format token_age_hours threshold: hours < 24 as Xh, hours >= 24 as Xd."""
        if threshold < 24:
            return f"{threshold:.0f}h"
        return f"{threshold / 24:.0f}d"

    # PARAM_LABELS: used in Section 7 (Filter Backtest) and Section 8 (per-wallet loop)
    PARAM_LABELS = {
        "jup_score": "jup_score (minimum threshold)",
        "mc_at_open": "mc_at_open (minimum threshold)",
        "token_age_hours": "token_age_hours (minimum threshold)",
    }

    lines: List[str] = []

    # ------------------------------------------------------------------
    # Report header
    # ------------------------------------------------------------------
    lines.append(f"# Loss Analysis Report — {date_str}")
    lines.append(f"Generated: {now_str}")
    lines.append("")

    # ------------------------------------------------------------------
    # Table of Contents
    # ------------------------------------------------------------------
    lines.append("## Table of Contents")
    lines.append("")
    lines.append("- [1. Executive Summary](#executive-summary)")
    lines.append("- [2. Action Items](#action-items)")
    lines.append("- [3. Recent Large Losses](#large-losses)")
    lines.append("- [4. Wallet Scorecard](#wallet-scorecard)")
    lines.append("- [5. Filter Recommendations](#filter-recommendations)")
    lines.append("- [6. Loss Analysis](#loss-analysis)")
    lines.append("- [7. Global Filter Backtest](#filter-backtest)")
    lines.append("- [8. Per-Wallet Details](#per-wallet-details)")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 1: Executive Summary
    # ------------------------------------------------------------------
    lines.append("## 1. Executive Summary {#executive-summary}")
    lines.append("")

    loss_rate = (
        result.loss_positions / result.closed_positions * 100.0
        if result.closed_positions > 0 else 0.0
    )

    active_scorecards = [
        sc for sc in result.wallet_scorecards
        if sc.status not in ("inactive", "insufficient_data")
    ]
    best_wallet = max(
        active_scorecards,
        key=lambda sc: sc.pnl_per_day_sol,
        default=None,
    )
    replacing_wallets = [
        sc for sc in result.wallet_scorecards
        if sc.status == "consider_replacing"
    ]

    lines.append(
        f"> Portfolio closed {result.closed_positions} positions"
        f" with total PnL {result.total_pnl_sol:+.4f} SOL."
    )
    lines.append(
        f"> Loss rate (SL+Rug+Failsafe): {loss_rate:.1f}%"
        f" ({result.loss_positions} positions)."
    )
    if best_wallet is not None:
        lines.append(
            f"> Top wallet: {best_wallet.wallet}"
            f" ({best_wallet.pnl_per_day_sol:+.4f} SOL/day,"
            f" WR {best_wallet.win_rate_pct:.0f}%)."
        )
    if replacing_wallets:
        lines.append(f"> {len(replacing_wallets)} wallet(s) flagged for replacement.")
    elif active_scorecards:
        lines.append("> All wallets within normal range — no urgent actions.")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 2: Pilne działania
    # ------------------------------------------------------------------
    lines.append("## 2. Action Items {#action-items}")
    lines.append("")

    # Portfolio size info line
    if PORTFOLIO_TOTAL_SOL > 0:
        max_pos_sol = PORTFOLIO_TOTAL_SOL * MAX_POSITION_FRACTION
        lines.append(
            f"**Portfolio:** {PORTFOLIO_TOTAL_SOL:.1f} SOL total, "
            f"max position {max_pos_sol:.2f} SOL "
            f"(1/{round(1/MAX_POSITION_FRACTION):.0f})"
        )
    else:
        lines.append(
            "**Portfolio size:** not configured — position size guard disabled "
            "(set PORTFOLIO_TOTAL_SOL in analysis_config.py to enable)"
        )
    lines.append("")

    insuf_events = _load_insuf_balance_csv(insufficient_balance_csv) if insufficient_balance_csv else []
    # Compute utilization once for both chart and action items
    _util_points = None
    if PORTFOLIO_TOTAL_SOL > 0:
        from valhalla.utilization import compute_hourly_utilization
        _util_points = compute_hourly_utilization(positions, UTILIZATION_LOOKBACK_HOURS)
    action_items = _build_action_items(result, positions, wallet_recs, insuf_events, _util_points)
    action_items = [item for item in action_items if not any(item.startswith(w) for w in inactive_wallets)]

    # Annotate each action item with its persistent status
    annotated_items = _tracker.annotate_items(action_items, rec_state)
    # Items considered "active" (not yet done/ignored) drive the Scorecard Action column
    active_action_items = [item for item, _id, status in annotated_items
                           if status == _tracker.STATUS_NEW]

    if not action_items:
        lines.append("_No urgent actions._")
    else:
        for idx, (item, rec_id, status) in enumerate(annotated_items, start=1):
            badge = _tracker.STATUS_BADGE[status]
            lines.append(f"{idx}. `{rec_id}` {badge} {item}")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 3: Recent Large Losses
    # ------------------------------------------------------------------
    loss_detail_section = _build_loss_detail_table(positions)
    lines.append(loss_detail_section)

    # ------------------------------------------------------------------
    # Section 4: Wallet Scorecard
    # ------------------------------------------------------------------
    lines.append("## 4. Wallet Scorecard {#wallet-scorecard}")
    lines.append("")

    if not result.wallet_scorecards:
        lines.append("_No scorecard data (no closed positions)._")
    else:
        from datetime import timedelta

        # Determine which wallets had any position opened within SCORECARD_RECENT_DAYS
        open_dates_by_wallet: Dict[str, datetime] = {}
        for pos in positions:
            dt_open = parse_iso_datetime(pos.datetime_open)
            if dt_open is not None:
                w = pos.target_wallet
                if w not in open_dates_by_wallet or dt_open > open_dates_by_wallet[w]:
                    open_dates_by_wallet[w] = dt_open

        if open_dates_by_wallet:
            reference_open = max(open_dates_by_wallet.values())
            cutoff_open = reference_open - timedelta(days=SCORECARD_RECENT_DAYS)
            recent_wallets = {w for w, d in open_dates_by_wallet.items() if d >= cutoff_open}
        else:
            recent_wallets = None  # no date info — show all

        sc_rows = []
        for sc in result.wallet_scorecards:
            if sc.wallet in inactive_wallets:
                continue
            if recent_wallets is not None and sc.wallet not in recent_wallets:
                continue
            wr_7d_str = f"{sc.win_rate_7d_pct:.0f}%" if sc.win_rate_7d_pct is not None else "N/A"
            hold_str = f"{sc.avg_hold_minutes:.0f}m" if sc.avg_hold_minutes is not None else "N/A"
            trend_str = f"{sc.win_rate_trend_pp:+.0f}pp" if sc.win_rate_trend_pp is not None else "N/A"
            hint_str = _scorecard_action_hints(sc.wallet, active_action_items)
            sc_rows.append([
                sc.wallet,
                str(sc.closed_positions),
                f"{sc.win_rate_pct:.0f}%",
                wr_7d_str,
                f"{sc.total_pnl_sol:+.4f}",
                f"{sc.pnl_per_day_sol:+.4f}",
                f"{sc.rug_rate_pct:.0f}%",
                hold_str,
                trend_str,
                sc.status,
                hint_str,
            ])

        ref_date_str = reference_open.strftime("%Y-%m-%d %H:%M") if open_dates_by_wallet else "N/A"
        lines.append(
            f"_Showing wallets active in last {SCORECARD_RECENT_DAYS}d "
            f"(last open: {ref_date_str}). "
            f"Change `SCORECARD_RECENT_DAYS` in analysis_config.py to adjust._"
        )
        lines.append("")
        if sc_rows:
            lines.append(_md_table(
                ["Wallet", "Pos.", "WR%", "WR 7d%", "PnL (SOL)", "SOL/day",
                 "Rug Rate", "Avg Hold", "Trend", "Status", "Action"],
                sc_rows,
            ))
        else:
            lines.append("_No wallets with recent activity._")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 5: Rekomendacje filtrów
    # ------------------------------------------------------------------
    lines.append("## 5. Filter Recommendations {#filter-recommendations}")
    lines.append("")

    filter_recs = [
        r for r in wallet_recs
        if ("sweet spot" in r.lower() or "tightening" in r.lower())
        and not any(r.startswith(w) for w in inactive_wallets)
    ]

    if not filter_recs:
        lines.append("_No actionable filter recommendations._")
    else:
        for rec in filter_recs:
            lines.append(f"- {rec.strip()}")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 6: Analiza strat
    # ------------------------------------------------------------------
    lines.append("## 6. Loss Analysis {#loss-analysis}")
    lines.append("")

    # ---- 5a. Risk Profile ----
    lines.append("### 5a. Risk Profile: Stop-Loss vs Profitable Trades")
    lines.append("")
    lines.append("Compares average token quality metrics for loss groups vs profitable trades only.")
    lines.append("Lower quality metrics in the stop-loss group may indicate avoidable entries.")
    lines.append("")

    if result.stop_loss_positions == 0:
        lines.append("_No stop-loss positions found — section not applicable._")
    else:
        rp_rows = []
        for row in result.risk_profile:
            metric_label = {
                "jup_score": "jup_score",
                "mc_at_open": "mc_at_open",
                "token_age_hours": "token_age_hours",
            }.get(row.metric, row.metric)

            if row.metric == "mc_at_open":
                sl_val = _fmt_mc(row.sl_avg) if row.sl_avg is not None else "N/A"
                sl_rug_val = _fmt_mc(row.sl_rug_avg) if row.sl_rug_avg is not None else "N/A"
                all_val = _fmt_mc(row.all_avg) if row.all_avg is not None else "N/A"
            elif row.metric == "token_age_hours":
                sl_val = _fmt_age_hours(row.sl_avg) if row.sl_avg is not None else "N/A"
                sl_rug_val = _fmt_age_hours(row.sl_rug_avg) if row.sl_rug_avg is not None else "N/A"
                all_val = _fmt_age_hours(row.all_avg) if row.all_avg is not None else "N/A"
            else:
                sl_val = f"{row.sl_avg:.0f}" if row.sl_avg is not None else "N/A"
                sl_rug_val = f"{row.sl_rug_avg:.0f}" if row.sl_rug_avg is not None else "N/A"
                all_val = f"{row.all_avg:.0f}" if row.all_avg is not None else "N/A"

            sl_note = ""
            if row.sl_count < 3:
                sl_note = f" (n={row.sl_count}, insufficient)"
            sl_rug_note = ""
            if row.sl_rug_count < 3:
                sl_rug_note = f" (n={row.sl_rug_count}, insufficient)"

            rp_rows.append([
                metric_label,
                sl_val + sl_note,
                sl_rug_val + sl_rug_note,
                all_val,
                _fmt_pct(row.diff_pct),
                _fmt_pct(row.sl_rug_diff_pct),
            ])

        lines.append(_md_table(
            ["Metric", "SL Only Avg", "SL+Rug/FS Avg", "Profitable Avg", "SL Diff", "SL+Rug Diff"],
            rp_rows,
        ))
    lines.append("")

    # ---- 5b. Stop-Loss Level Distribution ----
    lines.append("### 5b. Stop-Loss Level Distribution")
    lines.append("")
    lines.append("If your stop-loss had been set tighter, how many positions would have been saved?")
    lines.append("")

    # Sub-table A: SL exits only
    lines.append("**SL exits only** (stop_loss / stop_loss_unknown_open):")
    lines.append("")
    if not result.sl_buckets_sl_only or all(b.count == 0 for b in result.sl_buckets_sl_only):
        lines.append("_No SL-only positions with PnL percentage data available._")
    else:
        sl_only_rows = [
            [b.bucket_label, str(b.count), f"{b.sol_saved:.3f} SOL"]
            for b in result.sl_buckets_sl_only
        ]
        lines.append(_md_table(
            ["SL Level", "Positions Below", "SOL Saved vs Actual"],
            sl_only_rows,
        ))
    lines.append("")

    # Sub-table B: All losses (SL + Rug/Failsafe)
    lines.append("**All losses** (SL + Rug/Failsafe):")
    lines.append("")
    if not result.sl_buckets or all(b.count == 0 for b in result.sl_buckets):
        lines.append("_No loss positions with PnL percentage data available._")
    else:
        sl_rows = [
            [b.bucket_label, str(b.count), f"{b.sol_saved:.3f} SOL"]
            for b in result.sl_buckets
        ]
        lines.append(_md_table(
            ["SL Level", "Positions Below", "SOL Saved vs Actual"],
            sl_rows,
        ))
    lines.append("")

    # ---- 5c. Source Wallet Comparison ----
    lines.append("### 5c. Source Wallet Comparison")
    lines.append("")

    # Consider positions eligible for source wallet analysis (by PnL threshold)
    from valhalla.analysis_config import SOURCE_WALLET_MIN_LOSS_PCT
    if SOURCE_WALLET_MIN_LOSS_PCT is not None:
        sw_threshold = Decimal(str(SOURCE_WALLET_MIN_LOSS_PCT))
        sw_eligible_all = [
            p for p in positions
            if p.close_reason != "still_open"
            and p.pnl_pct is not None and p.pnl_pct <= sw_threshold
        ]
        threshold_label = f"positions with loss > {abs(SOURCE_WALLET_MIN_LOSS_PCT):.0f}%"
    else:
        sw_eligible_all = [p for p in positions if p.close_reason != "still_open"]
        threshold_label = "closed positions"
    sw_eligible_total = len(sw_eligible_all)

    # Positions with source_wallet_scenario populated (excluding failed attempts)
    with_scenario = [
        p for p in sw_eligible_all
        if getattr(p, 'source_wallet_scenario', None)
        and p.source_wallet_scenario != "no_data"
    ]

    if not with_scenario:
        lines.append("_No source wallet data available yet._")
    else:
        scenario_count = len(with_scenario)
        lines.append(
            f"Source wallet data available for {scenario_count} of {sw_eligible_total} {threshold_label}."
        )
        lines.append("")

        # Scenario distribution
        from collections import Counter
        scenario_counts: Counter = Counter()
        for p in with_scenario:
            scenario_counts[p.source_wallet_scenario] += 1

        # Avg pnl_pct per scenario
        scenario_pnl_pcts: dict = {}
        for scenario in scenario_counts:
            pcts = [
                float(p.source_wallet_pnl_pct)
                for p in with_scenario
                if p.source_wallet_scenario == scenario
                and getattr(p, 'source_wallet_pnl_pct', None) is not None
            ]
            if pcts:
                scenario_pnl_pcts[scenario] = sum(pcts) / len(pcts)

        scenario_order = [
            "source_held_longer", "source_exited_early", "source_recovered",
            "held_longer", "exited_first",  # legacy labels from old CSV
            "both_loss", "comparable",
            "unknown",  # legacy label from old CSV
            "no_data", "error",
        ]
        dist_rows = []
        for sc in scenario_order:
            count = scenario_counts.get(sc, 0)
            pct_of_total = count / scenario_count * 100.0 if scenario_count > 0 else 0.0
            avg_pnl = scenario_pnl_pcts.get(sc)
            avg_pnl_str = f"{avg_pnl:+.1f}%" if avg_pnl is not None else "N/A"
            dist_rows.append([sc, str(count), f"{pct_of_total:.0f}%", avg_pnl_str])

        lines.append(_md_table(
            ["Scenario", "Count", "% of Source Data", "Avg Source PnL%"],
            dist_rows,
        ))
        lines.append("")
        lines.append("**Scenario guide:**")
        lines.append("- `source_held_longer` / `held_longer`: source wallet stayed in the position longer and recovered — consider widening stop-loss margin or enabling non-SOL token top-up")
        lines.append("- `source_exited_early` / `exited_first`: source wallet exited before the drop — copy lag or entry speed issue")
        lines.append("- `source_recovered`: source wallet ended positive while bot lost — timing data unavailable to determine mechanism")
        lines.append("- `both_loss`: both source and bot lost — likely bad luck or market conditions, not a strategy issue")
        lines.append("- `comparable`: similar outcomes on both sides — no clear lesson from this position")
        lines.append("- `no_data` / `no_data`: API error or missing transaction data — could not analyze")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 7: Filter Backtest (globalny)
    # ------------------------------------------------------------------
    lines.append("## 7. Global Filter Backtest {#filter-backtest}")
    lines.append("")
    lines.append("For each parameter: what if only trades meeting the threshold were taken?")
    lines.append("")

    for param, bt_rows in result.backtest_results.items():
        lines.append(f"### {PARAM_LABELS.get(param, param)}")
        lines.append("")
        if not bt_rows:
            lines.append("_No data._")
            lines.append("")
            continue

        # Find sweet spot: row with highest net_sol_impact > 0
        best_idx = None
        best_impact = Decimal("0")
        for i, brow in enumerate(bt_rows):
            if brow.net_sol_impact > best_impact:
                best_impact = brow.net_sol_impact
                best_idx = i

        table_rows = []
        for i, brow in enumerate(bt_rows):
            if param == "mc_at_open":
                threshold_str = _fmt_mc(brow.threshold)
            elif param == "token_age_hours":
                threshold_str = _fmt_age_threshold(brow.threshold)
            else:
                threshold_str = f"{brow.threshold:.0f}" if brow.threshold == int(brow.threshold) else f"{brow.threshold}"

            net_str = f"{brow.net_sol_impact:+.4f} SOL"
            marker = " <- sweet spot" if i == best_idx else ""
            table_rows.append([
                f">= {threshold_str}",
                str(brow.wins_kept),
                str(brow.wins_excluded),
                str(brow.losses_avoided),
                str(brow.losses_kept),
                net_str + marker,
            ])

        lines.append(_md_table(
            ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"],
            table_rows,
        ))
        lines.append("")

    # ------------------------------------------------------------------
    # Section 8: Szczegóły per wallet
    # ------------------------------------------------------------------
    lines.append("## 8. Per-Wallet Details {#per-wallet-details}")
    lines.append("")

    # Collect unique wallet names from all positions
    all_wallets = sorted(set(
        p.target_wallet for p in positions
        if getattr(p, 'target_wallet', None) and p.target_wallet != "unknown"
    ))

    RUG_FAILSAFE_REASONS = {"rug", "rug_unknown_open", "failsafe", "failsafe_unknown_open"}

    wallet_sections_written = 0

    for wallet_name in all_wallets:
        if wallet_name in inactive_wallets:
            continue
        # Filter to this wallet's positions only
        wallet_positions = [p for p in positions if p.target_wallet == wallet_name]

        # Only include wallets with at least 3 closed positions (not still_open, not unknown_open)
        closed_wallet = [
            p for p in wallet_positions
            if p.close_reason not in ("still_open", "unknown_open")
        ]
        if len(closed_wallet) < 3:
            continue

        # Skip backtest if fewer than 10 closed positions (too little data)
        run_backtest = len(closed_wallet) >= 10

        # Compute header stats
        WIN_THRESHOLD = Decimal("0.01")
        LOSS_THRESHOLD = Decimal("-0.01")

        TP_REASONS = {"take_profit"}
        SL_REASONS = {"stop_loss", "stop_loss_unknown_open"}
        RF_REASONS = {"rug", "rug_unknown_open", "failsafe", "failsafe_unknown_open"}

        wins = sum(1 for p in closed_wallet if p.pnl_sol is not None and p.pnl_sol > WIN_THRESHOLD)
        neutral = sum(1 for p in closed_wallet if p.pnl_sol is not None and LOSS_THRESHOLD <= p.pnl_sol <= WIN_THRESHOLD)
        losses = sum(1 for p in closed_wallet if p.pnl_sol is not None and p.pnl_sol < LOSS_THRESHOLD)
        no_pnl = sum(1 for p in closed_wallet if p.pnl_sol is None)
        tp_count = sum(1 for p in closed_wallet if p.close_reason in TP_REASONS)
        sl_count = sum(1 for p in closed_wallet if p.close_reason in SL_REASONS)
        rf_count = sum(1 for p in closed_wallet if p.close_reason in RF_REASONS)
        wallet_pnl = sum((p.pnl_sol for p in closed_wallet if p.pnl_sol is not None), Decimal("0"))

        lines.append(f"### Wallet: {wallet_name}")
        lines.append("")
        lines.append(
            f"{len(closed_wallet)} total positions | "
            f"{wins} wins (>{WIN_THRESHOLD} SOL) | {neutral} neutral | {losses} losses (<{LOSS_THRESHOLD} SOL)"
            f" | [TP: {tp_count} | SL: {sl_count} | Rug/FS: {rf_count}]"
            f" | PnL: {wallet_pnl:.4f} SOL"
        )
        lines.append("")

        if run_backtest:
            lines.append("#### Filter Backtest")
            lines.append("")

            wallet_bt_results = FilterBacktester().sweep_all(wallet_positions)

            for param, bt_rows in wallet_bt_results.items():
                lines.append(f"**{PARAM_LABELS.get(param, param)}**")
                lines.append("")
                if not bt_rows:
                    lines.append("_No data._")
                    lines.append("")
                    continue

                # Find sweet spot: row with highest net_sol_impact > 0
                best_idx = None
                best_impact = Decimal("0")
                for i, brow in enumerate(bt_rows):
                    if brow.net_sol_impact > best_impact:
                        best_impact = brow.net_sol_impact
                        best_idx = i

                table_rows = []
                for i, brow in enumerate(bt_rows):
                    if param == "mc_at_open":
                        threshold_str = _fmt_mc(brow.threshold)
                    elif param == "token_age_hours":
                        threshold_str = _fmt_age_threshold(brow.threshold)
                    else:
                        threshold_str = (
                            f"{brow.threshold:.0f}"
                            if brow.threshold == int(brow.threshold)
                            else f"{brow.threshold}"
                        )

                    net_str = f"{brow.net_sol_impact:+.4f} SOL"
                    marker = " <- sweet spot" if i == best_idx else ""
                    table_rows.append([
                        f">= {threshold_str}",
                        str(brow.wins_kept),
                        str(brow.wins_excluded),
                        str(brow.losses_avoided),
                        str(brow.losses_kept),
                        net_str + marker,
                    ])

                lines.append(_md_table(
                    ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"],
                    table_rows,
                ))
                lines.append("")
        else:
            lines.append(
                f"_Filter backtest requires at least 10 closed positions "
                f"(this wallet has {len(closed_wallet)})._"
            )
            lines.append("")

        wallet_sections_written += 1

    if wallet_sections_written == 0:
        lines.append("_No wallets with sufficient data for per-wallet analysis._")
        lines.append("")

    # Write file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))


def _print_backtest_table(param: str, rows: List) -> None:
    """Print a backtest sweep result table to terminal."""
    PARAM_LABELS = {
        "jup_score": "jup_score",
        "mc_at_open": "mc_at_open",
        "token_age_hours": "token_age_hours",
    }
    print(f"\n--- Backtest: {PARAM_LABELS.get(param, param)} ---")

    if not rows:
        print("  No data.")
        return

    def _fmt_age_threshold_t(threshold: float) -> str:
        if threshold < 24:
            return f"{threshold:.0f}h"
        return f"{threshold / 24:.0f}d"

    headers = ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"]
    table_rows = []
    for brow in rows:
        if param == "mc_at_open":
            threshold_str = _fmt_mc(brow.threshold)
        elif param == "token_age_hours":
            threshold_str = _fmt_age_threshold_t(brow.threshold)
        else:
            threshold_str = f"{brow.threshold:.0f}" if brow.threshold == int(brow.threshold) else f"{brow.threshold}"

        net_str = f"{brow.net_sol_impact:+.4f} SOL"
        table_rows.append([
            f">= {threshold_str}",
            str(brow.wins_kept),
            str(brow.wins_excluded),
            str(brow.losses_avoided),
            str(brow.losses_kept),
            net_str,
        ])

    print(_md_table(headers, table_rows))


def _run_custom_backtest(
    positions: List,
    param_value_strs: List[str],
    wallet: Optional[str] = None,
) -> None:
    """Parse --backtest arguments and run FilterBacktester sweeps, printing to terminal."""
    from valhalla.loss_analyzer import FilterBacktester

    backtester = FilterBacktester()
    PARAM_ALIASES = {
        'mc': 'mc_at_open',
        'mc_at_open': 'mc_at_open',
        'age': 'token_age_hours',
        'token_age': 'token_age_hours',
        'token_age_days': 'token_age_hours',
        'token_age_hours': 'token_age_hours',
        'jup': 'jup_score',
        'jup_score': 'jup_score',
    }

    for pv in param_value_strs:
        try:
            param_raw, value_str = pv.split('=', 1)
            param = PARAM_ALIASES.get(param_raw, param_raw)
            threshold = float(value_str)
        except ValueError:
            print(f"  Warning: could not parse --backtest argument '{pv}', expected param=value")
            continue

        rows = backtester.sweep(positions, param, thresholds=[threshold], wallet=wallet)
        _print_backtest_table(param, rows)


def _run_track_mode(output_dir: str) -> None:
    """
    Load positions from existing positions.csv, rebuild action items, and
    run the interactive recommendation tracker CLI.

    Called when --track flag is passed. Exits after tracker session ends.
    """
    from valhalla import recommendations_tracker as _tracker
    from valhalla.loss_analyzer import LossAnalyzer, InsufficientBalanceAnalyzer

    output_path = Path(output_dir)
    positions_csv = output_path / "positions.csv"
    insuf_csv = output_path / "insufficient_balance.csv"
    state_path = str(output_path / ".recommendations_state.json")

    if not positions_csv.exists():
        print(f"Error: {positions_csv} not found. Run the parser first to generate it.")
        return

    # Load positions from CSV
    import csv as _csv
    from valhalla.models import MatchedPosition

    print(f"Loading positions from {positions_csv}...")
    try:
        matched_positions = []
        with open(positions_csv, "r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                def _dec(key):
                    v = row.get(key, "").strip()
                    return Decimal(v) if v else None

                def _float(key):
                    v = row.get(key, "").strip()
                    return float(v) if v else None

                pos = MatchedPosition(
                    target_wallet=row.get("target_wallet", ""),
                    token=row.get("token", ""),
                    position_type=row.get("position_type", ""),
                    sol_deployed=_dec("sol_deployed"),
                    sol_received=_dec("sol_received"),
                    pnl_sol=_dec("pnl_sol"),
                    pnl_pct=_dec("pnl_pct"),
                    close_reason=row.get("close_reason", ""),
                    mc_at_open=float(row["mc_at_open"]) if row.get("mc_at_open", "").strip() else 0.0,
                    jup_score=int(float(row["jup_score"])) if row.get("jup_score", "").strip() else 0,
                    token_age=row.get("token_age", ""),
                    token_age_days=int(float(row["token_age_days"])) if row.get("token_age_days", "").strip() else None,
                    token_age_hours=int(float(row["token_age_hours"])) if row.get("token_age_hours", "").strip() else None,
                    price_drop_pct=_float("price_drop_pct"),
                    position_id=row.get("position_id", ""),
                    full_address=row.get("full_address", ""),
                    pnl_source=row.get("pnl_source", "pending"),
                    meteora_deposited=_dec("meteora_deposited"),
                    meteora_withdrawn=_dec("meteora_withdrawn"),
                    meteora_fees=_dec("meteora_fees"),
                    meteora_pnl=_dec("meteora_pnl"),
                    datetime_open=row.get("datetime_open", ""),
                    datetime_close=row.get("datetime_close", ""),
                    target_wallet_address=row.get("target_wallet_address") or None,
                    target_tx_signature=row.get("target_tx_signature") or None,
                    source_wallet_hold_min=int(float(row["source_wallet_hold_min"])) if row.get("source_wallet_hold_min", "").strip() else None,
                    source_wallet_pnl_pct=_dec("source_wallet_pnl_pct"),
                    source_wallet_scenario=row.get("source_wallet_scenario") or None,
                )
                matched_positions.append(pos)
    except Exception as e:
        print(f"Error loading positions CSV: {e}")
        return

    print(f"  Loaded {len(matched_positions)} positions.")

    # Build action items (same logic as report generation)
    result = LossAnalyzer().analyze(matched_positions)
    inactive_wallets = {sc.wallet for sc in result.wallet_scorecards if sc.status == "inactive" and sc.wallet}
    wallet_recs = _generate_wallet_recommendations(matched_positions)
    insuf_events = _load_insuf_balance_csv(str(insuf_csv)) if insuf_csv.exists() else []
    _util_points = None
    if PORTFOLIO_TOTAL_SOL > 0:
        from valhalla.utilization import compute_hourly_utilization
        _util_points = compute_hourly_utilization(matched_positions, UTILIZATION_LOOKBACK_HOURS)
    action_items = _build_action_items(result, matched_positions, wallet_recs, insuf_events, _util_points)
    action_items = [item for item in action_items if not any(item.startswith(w) for w in inactive_wallets)]

    if not action_items:
        print("No recommendations to track.")
        return

    updates = _tracker.run_interactive_tracker(action_items, state_path)

    if updates > 0:
        print("\nRegenerating loss_analysis.md...")
        insuf_csv_str = str(insuf_csv) if insuf_csv.exists() else None
        output_md = str(output_path / "loss_analysis.md")
        _generate_loss_report(matched_positions, output_md, insuf_csv_str)
        print(f"Report updated: {output_md}")


def _interactive_menu():
    """Show a simple numbered menu when script is run with no arguments.
    Returns a list of CLI args to inject into sys.argv, or None to exit.
    """
    print()
    print("=" * 60)
    print("  Valhalla Parser - Interactive Menu")
    print("=" * 60)
    print("  1) Parse Discord logs (normal run)")
    print("  2) Parse + lpagent cross-check")
    print("  3) Cross-check only (date range)")
    print("  4) Fast fill positions (no charts, no loss analysis)")
    print("  0) Exit")
    print("=" * 60)
    while True:
        choice = input("Select option [0-4]: ").strip()
        if choice == "0":
            return None
        if choice == "1":
            return []
        if choice == "2":
            return ["--lpagent"]
        if choice == "3":
            frm = input("  FROM date (YYYY-MM-DD): ").strip()
            to = input("  TO date (YYYY-MM-DD, empty = same as FROM): ").strip()
            args = ["--cross-check", frm]
            if to:
                args.append(to)
            return args
        if choice == "4":
            return ["--skip-charts", "--no-loss-analysis"]
        print("  Invalid choice, try again.")


def main():
    # If run with no arguments, show interactive menu
    if len(sys.argv) == 1:
        injected = _interactive_menu()
        if injected is None:
            return
        sys.argv.extend(injected)

    parser = argparse.ArgumentParser(
        description='Parse Valhalla Bot Discord DM logs and generate PnL analysis with Meteora API.'
    )
    parser.add_argument('input_files', nargs='*', help='Path(s) to Discord DM log file(s) (default: all files in input/ folder)')
    parser.add_argument('--output-dir', default='output', help='Output directory for CSV files (default: output/)')
    parser.add_argument('--rpc-url', default='https://api.mainnet-beta.solana.com',
                       help='Solana RPC URL (default: public mainnet)')
    parser.add_argument('--skip-rpc', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--skip-meteora', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--use-discord-pnl', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--no-archive', action='store_true', help='Skip moving processed files to archive/')
    parser.add_argument('--cache-file', help='Address cache JSON file (default: address_cache.json in output-dir)')
    parser.add_argument('--date', help='Date for logs in YYYY-MM-DD format (optional, will try to detect from filename)')
    parser.add_argument('--input-format', choices=['auto', 'text', 'html'], default='auto',
                       help='Input format: auto (detect), text (plain text), html (HTML from browser)')
    parser.add_argument('--merge', nargs='+', metavar='CSV_FILE',
                       help='Merge multiple positions.csv files (use instead of input_files)')
    parser.add_argument('--export-json', metavar='FILE',
                       help='Export results as .valhalla.json for incremental workflows')
    parser.add_argument('--import-json', metavar='FILE',
                       help='Import previous .valhalla.json to merge with new data')
    parser.add_argument('--skip-charts', action='store_true', help='Skip chart generation')
    parser.add_argument('--no-clipboard', action='store_true', help='Skip auto-running save_clipboard.ps1')
    parser.add_argument('--recover-insuf', action='store_true',
                       help='Recover insufficient balance history from archive files')
    parser.add_argument('--backtest', nargs='+', metavar='PARAM=VALUE',
                       help='Run filter backtest with custom thresholds. '
                            'E.g.: --backtest jup_score=80 mc=5000000 age=1')
    parser.add_argument('--wallet', metavar='WALLET_ID',
                       help='Filter --backtest to a specific wallet alias')
    parser.add_argument('--no-loss-analysis', action='store_true',
                       help='Skip loss analysis report generation')
    parser.add_argument('--no-wallet-trend', action='store_true',
                       help='Skip wallet trend report generation (output/wallet_trend.md)')
    parser.add_argument('--no-input', action='store_true',
                       help='Skip input file processing and load from existing positions.csv. '
                            'Useful to re-run analysis without processing new logs.')
    parser.add_argument('--report', default='all',
                       help='Comma-separated list of report modules to generate. '
                            'Options: loss,per-wallet,source,charts,recommendations,all (default: all)')
    parser.add_argument('--track', action='store_true',
                       help='Interactively mark recommendation statuses (done/ignored/new). '
                            'Loads positions from output/positions.csv and shows current items.')
    parser.add_argument('--lpagent', action='store_true',
                       help='Enable auto lpagent cross-check after parsing (off by default)')
    parser.add_argument('--cross-check', nargs='*', metavar='DATE',
                       help='Run lpagent cross-check. Optional: FROM_DATE [TO_DATE] in YYYY-MM-DD format. '
                            'If no dates given, uses watermark to yesterday. '
                            'Skips normal log processing.')

    args = parser.parse_args()

    # Load .env so LPAGENT_API_KEY / LPAGENT_WALLET are available via os.environ
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # python-dotenv not installed; rely on env vars already set

    # --track mode: interactive recommendation status editor
    if args.track:
        _run_track_mode(args.output_dir)
        return

    # --cross-check mode: skip normal pipeline, run lpagent cross-check only
    if args.cross_check is not None:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        positions_csv = str(output_dir / "positions.csv")

        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

        dates = args.cross_check
        if len(dates) == 0:
            # No dates given: use watermark to yesterday
            watermark = _read_watermark(str(output_dir))
            from_date = (
                datetime.strptime(watermark, "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            to_date = yesterday
        elif len(dates) == 1:
            from_date = to_date = dates[0]
        else:
            from_date, to_date = dates[0], dates[1]

        if from_date > to_date:
            print(f"[Cross-check] Nothing to sync: from_date {from_date} > to_date {to_date}")
            return

        print(f"[Cross-check] {from_date} -> {to_date}")
        try:
            count = _run_cross_check(
                from_date, to_date, positions_csv, str(output_dir), silent_if_empty=False
            )
            if count > 0:
                _write_watermark(str(output_dir), to_date)
                print(f"  Watermark updated to {to_date}")
            else:
                # Also update watermark when sync is clean (avoid re-querying)
                _write_watermark(str(output_dir), to_date)
                print(f"  Watermark updated to {to_date}")
        except ValueError as e:
            print(f"[Cross-check] Error: {e}")
        return

    # NOTE: Auto-fetch of on-chain SOL balance is intentionally disabled.
    # The on-chain balance excludes SOL locked in open Meteora positions, which
    # would lead to underreporting of total portfolio value. Until Meteora open
    # positions can be queried and valued, the balance must be set manually in
    # analysis_config.py (PORTFOLIO_TOTAL_SOL).

    # Parse --report modules
    report_modules = set(args.report.split(',')) if args.report != 'all' else {'all'}
    want_all = 'all' in report_modules

    # Initialize shared variables (overwritten in normal mode; used as-is in --no-input mode)
    event_parser = EventParser()
    matched_positions = []
    unmatched_opens = []
    processed_files = []
    meteora_results: Dict[str, MeteoraPnlResult] = {}
    meteora_failed: Dict[str, str] = {}

    # Auto-run save_clipboard.ps1 (skip in merge mode and --no-input mode)
    if not args.merge and not args.no_clipboard and not args.recover_insuf and not args.no_input:
        clipboard_script = Path('save_clipboard.ps1')
        if clipboard_script.exists():
            # Ask before reading clipboard (interactive only) — avoids dumping
            # unrelated content (e.g. YouTube transcripts) into input/ when the
            # clipboard doesn't hold Discord HTML.
            run_clipboard = False
            if sys.stdin.isatty():
                try:
                    answer = input("Import Discord messages from clipboard? [y/N]: ").strip().lower()
                    run_clipboard = (answer == 'y')
                except EOFError:
                    run_clipboard = False
            if run_clipboard:
                print("Running save_clipboard.ps1...")
                try:
                    result = subprocess.run(
                        ['powershell', '-File', str(clipboard_script)],
                        stdin=sys.stdin
                    )
                    if result.returncode != 0:
                        print(f"  Warning: save_clipboard.ps1 exited with code {result.returncode}")
                except Exception as e:
                    print(f"  Warning: Could not run save_clipboard.ps1: {e}")
            else:
                print("  Skipping clipboard import.")
        else:
            print("  save_clipboard.ps1 not found, skipping clipboard import")

    # Create output directory if needed
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Handle recover-insuf mode
    if args.recover_insuf:
        _recover_insufficient_balance_history(str(output_dir))
        return

    # Handle merge mode
    if args.merge:
        merge_positions_csvs(args.merge, str(output_dir))
        return

    # positions_csv path (needed in both normal and --no-input mode for Step 5.6)
    positions_csv = output_dir / 'positions.csv'

    if not args.no_input:
        # Get input files - either from args or all files in input/ folder
        if not args.input_files:
            input_dir = Path('input')
            if input_dir.exists() and input_dir.is_dir():
                # Get all .txt and .html files in input/
                input_files = [str(f) for f in input_dir.iterdir() if f.is_file() and f.suffix in ['.txt', '.html']]
                if not input_files:
                    parser.error("No .txt or .html files found in input/ folder")
                print(f"Processing all files in input/ folder: {len(input_files)} file(s)")
            else:
                parser.error("No input files specified and input/ folder not found")
        else:
            input_files = args.input_files

        # Determine cache file path
        cache_file = args.cache_file if args.cache_file else str(output_dir / 'address_cache.json')

        # Step 1: Read and parse all input files
        all_messages = []

        # Dedup: same position_id across files = same Discord message, keep first seen
        seen_open_ids = set()
        seen_close_ids = set()
        seen_failsafe_ids = set()
        seen_rug_ids = set()

        for input_file in input_files:
            # Detect format and create appropriate reader
            print(f"\nReading Discord logs: {input_file}")

            fmt = args.input_format
            if fmt == 'auto':
                fmt = detect_input_format(input_file)
                print(f"  Auto-detected format: {fmt}")

            if fmt == 'html':
                reader = HtmlReader(input_file)
            else:
                reader = PlainTextReader(input_file)

            messages = reader.read()
            print(f"  Found {len(messages)} Valhalla messages")

            # Determine date for this file - priority order:
            # 1. Embedded in timestamps ([YYYY-MM-DDTHH:MM] format) — no base_date needed
            # 2. Filename prefix (YYYYMMDD_*.txt)
            # 3. In-file date header (first line)
            # 4. User prompt if neither found
            file_date = None
            date_source = None

            # Check if messages contain full datetime timestamps
            has_full_timestamps = any(
                '[' in msg.timestamp and 'T' in msg.timestamp and len(msg.timestamp) > 7
                for msg in messages
            )

            if has_full_timestamps:
                # Dates are embedded in timestamps — no base_date needed
                date_source = "embedded timestamps"
                print(f"  Dates embedded in timestamps (no base date needed)")
            else:
                # Try filename first
                file_date = extract_date_from_filename(input_file)
                if file_date:
                    date_source = "filename"
                # Then try in-file header
                elif reader.header_date:
                    file_date = reader.header_date
                    date_source = "in-file header"
                # Finally, prompt user
                else:
                    print(f"  No date found in filename or file header")
                    user_input = input(f"  Enter date for {Path(input_file).name} (YYYYMMDD): ").strip()
                    if user_input and len(user_input) == 8 and user_input.isdigit():
                        try:
                            year = int(user_input[0:4])
                            month = int(user_input[4:6])
                            day = int(user_input[6:8])
                            datetime(year, month, day)
                            file_date = f"{year:04d}-{month:02d}-{day:02d}"
                            date_source = "user input"
                        except ValueError:
                            print(f"  Invalid date format, continuing without date")

                if file_date:
                    print(f"  Date detected from {date_source}: {file_date}")
                elif not has_full_timestamps:
                    print(f"  No date available")

            # Parse events with date context
            print(f"Parsing events (date: {file_date or 'none'})...")
            file_parser = EventParser(base_date=file_date)
            file_parser.parse_messages(messages)

            # Merge events into main parser (deduplicate by position_id across files)
            dedup_count = 0
            for e in file_parser.open_events:
                if e.position_id not in seen_open_ids:
                    seen_open_ids.add(e.position_id)
                    event_parser.open_events.append(e)
                else:
                    dedup_count += 1
            for e in file_parser.close_events:
                if e.position_id not in seen_close_ids:
                    seen_close_ids.add(e.position_id)
                    event_parser.close_events.append(e)
                else:
                    dedup_count += 1
            for e in file_parser.failsafe_events:
                if e.position_id not in seen_failsafe_ids:
                    seen_failsafe_ids.add(e.position_id)
                    event_parser.failsafe_events.append(e)
                else:
                    dedup_count += 1
            for e in file_parser.rug_events:
                pid = e.position_id or id(e)  # rug events may lack position_id
                if pid not in seen_rug_ids:
                    seen_rug_ids.add(pid)
                    event_parser.rug_events.append(e)
                else:
                    dedup_count += 1
            # Non-position events: no dedup needed
            event_parser.skip_events.extend(file_parser.skip_events)
            event_parser.swap_events.extend(file_parser.swap_events)
            event_parser.add_liquidity_events.extend(file_parser.add_liquidity_events)
            event_parser.insufficient_balance_events.extend(file_parser.insufficient_balance_events)
            event_parser.already_closed_events.extend(file_parser.already_closed_events)
            if dedup_count:
                print(f"  Skipped {dedup_count} duplicate events (already seen in earlier file)")

            # Collect per-file datetime range for archive naming
            file_datetimes = []
            for evt in (file_parser.open_events + file_parser.close_events +
                        file_parser.failsafe_events + file_parser.rug_events):
                ts = evt.timestamp  # "[HH:MM]" or "[YYYY-MM-DDTHH:MM]"
                if not ts:
                    continue
                if 'T' in ts:
                    # Extract "YYYY-MM-DDTHH:MM" from "[YYYY-MM-DDTHH:MM]"
                    dt_str = ts.strip('[]')
                    file_datetimes.append(dt_str)
                elif file_date:
                    # [HH:MM] timestamp — build full datetime from file_date + time
                    time_part = ts.strip('[]')  # "HH:MM"
                    file_datetimes.append(f"{file_date}T{time_part}")

            # Track for archiving
            processed_files.append((input_file, file_date, file_datetimes))

        # Step 2: Print aggregated event counts
        print(f"\nTotal parsed events across {len(input_files)} file(s):")

        print(f"  Open positions: {len(event_parser.open_events)}")
        print(f"  Close events: {len(event_parser.close_events)}")
        print(f"  Failsafe events: {len(event_parser.failsafe_events)}")
        print(f"  Add liquidity events: {len(event_parser.add_liquidity_events)}")
        print(f"  Rug events: {len(event_parser.rug_events)}")
        print(f"  Skip events: {len(event_parser.skip_events)}")
        print(f"  Swap events: {len(event_parser.swap_events)}")
        print(f"  Insufficient balance events: {len(event_parser.insufficient_balance_events)}")

        # Load already-complete and already-meteora position IDs from existing CSV
        already_complete_ids = set()   # meteora + both dates + enrichable close_reason → skip everything
        already_meteora_ids = set()    # any meteora PnL → skip Meteora re-fetch
        if positions_csv.exists():
            with open(positions_csv, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    pid = row.get('position_id', '').strip()
                    if not pid:
                        continue
                    if row.get('pnl_source') == 'meteora':
                        already_meteora_ids.add(pid)
                    if (row.get('pnl_source') == 'meteora'
                            and row.get('datetime_open')
                            and row.get('datetime_close')
                            and row.get('close_reason') not in (
                                'unknown_open', 'rug_unknown_open', 'failsafe_unknown_open', 'still_open',
                                'take_profit_unknown_open', 'stop_loss_unknown_open'
                            )):
                        already_complete_ids.add(pid)
            if already_complete_ids:
                print(f"  Skipping {len(already_complete_ids)} already-complete positions")

        # Step 3: Resolve addresses
        resolved_addresses: Dict[str, str] = {}
        cache = AddressCache(cache_file)

        if not args.skip_rpc:
            print(f"\nResolving position addresses via Solana RPC...")
            rpc_client = SolanaRpcClient(args.rpc_url)
            resolver = PositionResolver(cache, rpc_client)

            # Collect all events with position IDs and tx signatures
            # Check cache first - only hit RPC for positions not already cached
            seen_pids = set()
            events_to_resolve = []
            cache_hits = 0
            for event in event_parser.open_events + event_parser.close_events + event_parser.failsafe_events:
                if event.position_id not in seen_pids:
                    if event.position_id not in already_complete_ids:
                        seen_pids.add(event.position_id)
                        cached_addr = cache.get(event.position_id)
                        if cached_addr:
                            resolved_addresses[event.position_id] = cached_addr
                            cache_hits += 1
                        elif event.tx_signatures:
                            events_to_resolve.append((event.position_id, event.tx_signatures))

            total = len(events_to_resolve)
            if cache_hits:
                print(f"  {cache_hits} positions loaded from cache, {total} to resolve via RPC")
            for i, (pid, sigs) in enumerate(events_to_resolve, 1):
                print(f"  Resolving {i}/{total}: {pid}...", end='', flush=True)
                full_addr = resolver.resolve(pid, sigs)
                if full_addr:
                    resolved_addresses[pid] = full_addr
                    print(f" OK ({full_addr[:8]}...)")
                else:
                    print(f" NOT FOUND")

            print(f"  Resolved {len(resolved_addresses)} addresses")
            cache.save()
        else:
            print(f"\nSkipping RPC resolution (--skip-rpc)")
            # Load from cache only
            for event in event_parser.open_events + event_parser.close_events + event_parser.failsafe_events:
                cached = cache.get(event.position_id)
                if cached:
                    resolved_addresses[event.position_id] = cached
            print(f"  Loaded {len(resolved_addresses)} addresses from cache")

        # Seed resolved_addresses from existing lpagent rows in positions.csv.
        # Without this, lpagent_backfill positions that appear in a new Discord
        # archive file can't have Meteora called (no cache hit, no tx_sig from Discord)
        # → Discord merge produces pnl_source=pending with empty full_address.
        if positions_csv.exists():
            lpagent_seeded = 0
            with open(positions_csv, 'r', encoding='utf-8') as _f:
                for _row in csv.DictReader(_f):
                    _pid = _row.get('position_id', '').strip()
                    _addr = _row.get('full_address', '').strip()
                    if _pid and _addr and _row.get('pnl_source') == 'lpagent' and _pid not in resolved_addresses:
                        resolved_addresses[_pid] = _addr
                        lpagent_seeded += 1
            if lpagent_seeded:
                print(f"  Seeded {lpagent_seeded} address(es) from lpagent_backfill rows")

        # Step 4: Calculate Meteora PnL
        meteora_results: Dict[str, MeteoraPnlResult] = {}
        meteora_failed: Dict[str, str] = {}  # pid -> full_addr for retry

        if not args.skip_meteora and resolved_addresses:
            print(f"\nFetching Meteora PnL data...")

            meteora_calc = MeteoraPnlCalculator()

            # Build closeable_ids set (only positions that will be used)
            closeable_ids = set()
            for e in event_parser.close_events:
                closeable_ids.add(e.position_id)
            for e in event_parser.rug_events:
                if e.position_id:
                    closeable_ids.add(e.position_id)
            for e in event_parser.failsafe_events:
                closeable_ids.add(e.position_id)

            # Filter to only fetch closeable positions that aren't already complete or already have Meteora data
            addresses_to_fetch = {pid: addr for pid, addr in resolved_addresses.items()
                                  if pid in closeable_ids
                                  and pid not in already_complete_ids
                                  and pid not in already_meteora_ids}

            total = len(addresses_to_fetch)
            for i, (pid, full_addr) in enumerate(addresses_to_fetch.items(), 1):
                print(f"  Fetching {i}/{total}: {pid}...", end='', flush=True)
                result = meteora_calc.calculate_pnl(full_addr)
                if result:
                    recovered = result.withdrawn_sol + result.fees_sol
                    if recovered < Decimal('0.001'):
                        print(f" PnL: unknown (recovered {recovered:.4f} SOL ≈ total loss, unreliable)")
                    else:
                        meteora_results[pid] = result
                        print(f" PnL: {result.pnl_sol:.4f} SOL (${result.pnl_usd:.2f})")
                else:
                    print(f" FAILED")
                    meteora_failed[pid] = full_addr

            print(f"  Retrieved PnL for {len(meteora_results)} positions")
        elif args.skip_meteora:
            print(f"\nSkipping Meteora API (--skip-meteora)")
        else:
            print(f"\nSkipping Meteora API (no resolved addresses)")

        # Step 5: Match positions
        print(f"\nMatching positions...")
        matcher = PositionMatcher(event_parser)
        matched_positions, unmatched_opens = matcher.match_positions(
            meteora_results, resolved_addresses, use_discord_pnl=args.use_discord_pnl
        )
        print(f"  Matched positions: {len(matched_positions)}")
        print(f"  Still open: {len(unmatched_opens)}")

        # Step 5.5: Import and merge with previous data if requested
        if args.import_json:
            print(f"\nImporting previous data from {args.import_json}...")
            imported_positions, imported_still_open = import_from_json(args.import_json)
            print(f"  Merging with new data...")
            matched_positions, unmatched_opens = merge_with_imported(
                matched_positions, imported_positions,
                unmatched_opens, imported_still_open
            )

    if args.no_input:
        if not positions_csv.exists():
            print(f"Error: --no-input requires {positions_csv} to exist.")
            return
        print("No-input mode: loading from existing positions.csv...")

    # Step 5.6: Merge with existing output if present
    summary_csv = output_dir / 'summary.csv'

    if positions_csv.exists():
        print(f"\nMerging with existing output...")
        matched_positions, unmatched_opens = merge_with_existing_csv(
            matched_positions, unmatched_opens, str(positions_csv)
        )

    if args.no_input:
        print(f"  Loaded {len(matched_positions)} position(s) from existing CSV.")

    # Step 5.7: Source wallet analysis (runs always, idempotent — skips already-analyzed positions)
    if True:
        print(f"\nAnalyzing source wallet positions...")
        from valhalla.source_wallet_analyzer import SourceWalletAnalyzer
        # In --no-input mode, cache is not initialized yet — create it now
        if args.no_input:
            _cache_file = args.cache_file if args.cache_file else str(output_dir / 'address_cache.json')
            cache = AddressCache(_cache_file)
        analyzer_rpc = SolanaRpcClient(args.rpc_url)
        analyzer = SourceWalletAnalyzer(analyzer_rpc, cache)
        source_results = analyzer.analyze_batch(matched_positions)
        # Apply results back to matched_positions
        results_by_id = {r.position_id: r for r in source_results}
        updated_count = 0
        for pos in matched_positions:
            result = results_by_id.get(pos.position_id)
            if result and not result.error:
                pos.source_wallet_hold_min = result.source_hold_min
                pos.source_wallet_pnl_pct = result.source_pnl_pct
                pos.source_wallet_scenario = result.scenario
                updated_count += 1
            elif result and result.error:
                # Mark as attempted so it's not retried on every run
                pos.source_wallet_scenario = "no_data"
        cache.save()
        print(f"  Updated {updated_count} position(s) with source wallet data")

    # Step 6: Generate CSVs

    print(f"\nGenerating CSV files...")

    csv_writer = CsvWriter()
    csv_writer.generate_positions_csv(matched_positions, unmatched_opens, str(positions_csv))
    csv_writer.generate_summary_csv(matched_positions, event_parser.skip_events, str(summary_csv))

    # Generate insufficient balance CSV
    if event_parser.insufficient_balance_events:
        insuf_csv = output_dir / 'insufficient_balance.csv'
        csv_writer.generate_insufficient_balance_csv(
            event_parser.insufficient_balance_events, str(insuf_csv)
        )
        print(f"  {insuf_csv}")

    # Generate skip events CSV
    if event_parser.skip_events:
        skip_csv = output_dir / 'skip_events.csv'
        csv_writer.generate_skip_events_csv(event_parser.skip_events, str(skip_csv))
        print(f"  {skip_csv}")

    print(f"  {positions_csv}")
    print(f"  {summary_csv}")

    # Step 6.5a: Apply wallet aliases
    apply_aliases(
        csv_path=positions_csv,
        aliases_path=Path("wallet_aliases.json")
    )

    # Step 6.5b: Generate loss analysis report
    if not args.no_loss_analysis and (want_all or 'loss' in report_modules):
        loss_report_path = output_dir / 'loss_analysis.md'
        insuf_csv_path = str(output_dir / 'insufficient_balance.csv')
        print(f"\nGenerating loss analysis report...")
        try:
            _generate_loss_report(
                matched_positions,
                str(loss_report_path),
                insuf_csv_path,
            )
            print(f"  {loss_report_path}")
        except Exception as e:
            print(f"  Warning: loss analysis failed: {e}")

    # Step 6.5c: Generate wallet trend report (always on by default)
    if not args.no_wallet_trend:
        wallet_trend_path = output_dir / 'wallet_trend.md'
        print(f"\nGenerating wallet trend report...")
        try:
            from valhalla.wallet_trend_report import generate_wallet_trend_report
            from valhalla.loss_analyzer import WalletScorecardAnalyzer
            trend_scorecards = WalletScorecardAnalyzer().analyze(matched_positions)
            generate_wallet_trend_report(
                trend_scorecards,
                matched_positions,
                str(wallet_trend_path),
                PORTFOLIO_TOTAL_SOL,
            )
            print(f"  {wallet_trend_path}")
        except Exception as e:
            print(f"  Warning: wallet trend report failed: {e}")

    # Print wallet recommendations to terminal
    if want_all or 'recommendations' in report_modules:
        recs = _generate_wallet_recommendations(matched_positions)
        if recs:
            print(f"\n{'='*60}")
            print("Wallet Recommendations")
            print(f"{'='*60}")
            for rec in recs:
                print(f"  {rec.strip()}")

    # --backtest custom run (additive, prints to terminal)
    if hasattr(args, 'backtest') and args.backtest:
        _run_custom_backtest(matched_positions, args.backtest,
                             getattr(args, 'wallet', None))

    # Step 6.5: Generate charts
    if not args.skip_charts and (want_all or 'charts' in report_modules):
        print(f"\nGenerating charts...")
        generate_charts(matched_positions, str(output_dir), skip_events=event_parser.skip_events)
        insuf_csv = output_dir / 'insufficient_balance.csv'
        generate_insufficient_balance_chart(str(insuf_csv), str(output_dir))
        # Doc 009: Hourly capital utilization chart
        if PORTFOLIO_TOTAL_SOL > 0:
            from valhalla.utilization import (
                compute_hourly_utilization, generate_utilization_chart,
            )
            util_points = compute_hourly_utilization(matched_positions, UTILIZATION_LOOKBACK_HOURS)
            generate_utilization_chart(util_points, Decimal(str(PORTFOLIO_TOTAL_SOL)), str(output_dir))
            print(f"  Saved: {output_dir}/hourly_utilization.png")

    # Step 6.6: Export to JSON if requested
    if args.export_json:
        print(f"\nExporting to JSON...")
        export_to_json(matched_positions, unmatched_opens, event_parser.skip_events, args.export_json)

    # Step 6.7: Archive processed files
    if not args.no_archive and processed_files:
        print(f"\nArchiving processed files...")
        archive_dir = Path('archive')
        archive_dir.mkdir(parents=True, exist_ok=True)

        def _format_archive_dt(iso_str: str) -> str:
            """Convert '2026-02-13T15:08' to '20260213T1508'"""
            return iso_str.replace('-', '').replace(':', '')[:13]

        for input_file, file_date, file_datetimes in processed_files:
            input_path = Path(input_file)

            if file_datetimes:
                min_dt = _format_archive_dt(min(file_datetimes))
                max_dt = _format_archive_dt(max(file_datetimes))
                dt_prefix = f"{min_dt}-{max_dt}_"
            else:
                dt_prefix = ""

            archive_name = f"{dt_prefix}{input_path.name}"
            archive_path = archive_dir / archive_name

            try:
                shutil.move(str(input_path), str(archive_path))
                print(f"  Archived: {archive_path}")
            except Exception as e:
                print(f"  Failed to archive {input_path}: {e}")

    # Step 7: Print summary stats
    print(f"\n{'='*60}")
    print(f"Summary Statistics")
    print(f"{'='*60}")

    total_pnl = sum(p.pnl_sol for p in matched_positions if p.pnl_sol is not None)
    meteora_count = sum(1 for p in matched_positions if p.pnl_source == 'meteora')
    pending_count = sum(1 for p in matched_positions if p.pnl_source == 'pending')
    discord_count = sum(1 for p in matched_positions if p.pnl_source == 'discord')

    print(f"Total matched positions: {len(matched_positions)}")
    print(f"  - Meteora PnL: {meteora_count}")
    print(f"  - Pending PnL: {pending_count}")
    if discord_count:
        print(f"  - Discord PnL: {discord_count}")
    print(f"Still open positions: {len(unmatched_opens)}")
    print(f"Total PnL: {total_pnl:.4f} SOL")

    _detect_coverage_gaps(str(positions_csv))

    # Print parsed messages time range
    all_events_for_range = (
        event_parser.open_events + event_parser.close_events +
        event_parser.failsafe_events + event_parser.rug_events +
        event_parser.insufficient_balance_events
    )
    all_timestamps = [e.timestamp for e in all_events_for_range if e.timestamp]
    if all_timestamps:
        def _extract_dt_str(ts: str) -> str:
            """Extract datetime string from '[HH:MM]' or '[YYYY-MM-DDTHH:MM]'"""
            inner = ts.strip('[]')
            return inner  # either 'HH:MM' or 'YYYY-MM-DDTHH:MM'

        dt_strings = [_extract_dt_str(ts) for ts in all_timestamps]
        min_dt = min(dt_strings)
        max_dt = max(dt_strings)
        # Display with space instead of 'T' for readability
        min_display = min_dt.replace('T', ' ')
        max_display = max_dt.replace('T', ' ')
        print(f"\nParsed messages: {min_display} -> {max_display}")

    # Step 8: Retry failed Meteora API calls
    if meteora_failed:
        failed_ids = ', '.join(meteora_failed.keys())
        print(f"\n{'!'*60}")
        print(f"WARNING: {len(meteora_failed)} Meteora API error(s): {failed_ids}")
        print(f"{'!'*60}")
        try:
            retry = input("Retry failed positions? [Y/n]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            retry = 'n'

        if retry != 'n':
            print(f"\nRetrying {len(meteora_failed)} Meteora fetch(es)...")
            meteora_calc = MeteoraPnlCalculator()
            retry_ok = 0
            still_failed = []
            for pid, full_addr in meteora_failed.items():
                print(f"  Retrying {pid}...", end='', flush=True)
                result = meteora_calc.calculate_pnl(full_addr)
                if result:
                    recovered = result.withdrawn_sol + result.fees_sol
                    if recovered < Decimal('0.001'):
                        print(f" PnL: unknown (total loss, unreliable)")
                    else:
                        meteora_results[pid] = result
                        retry_ok += 1
                        print(f" PnL: {result.pnl_sol:.4f} SOL (${result.pnl_usd:.2f})")
                else:
                    print(f" FAILED again")
                    still_failed.append(pid)

            if retry_ok > 0:
                print(f"\n  Recovered {retry_ok} position(s), regenerating output...")

                # Redo matching
                matcher = PositionMatcher(event_parser)
                matched_positions, unmatched_opens = matcher.match_positions(
                    meteora_results, resolved_addresses, use_discord_pnl=args.use_discord_pnl
                )

                # Redo import merge if applicable
                if args.import_json:
                    imported_positions, imported_still_open = import_from_json(args.import_json)
                    matched_positions, unmatched_opens = merge_with_imported(
                        matched_positions, imported_positions,
                        unmatched_opens, imported_still_open
                    )

                # Redo CSV merge
                if positions_csv.exists():
                    matched_positions, unmatched_opens = merge_with_existing_csv(
                        matched_positions, unmatched_opens, str(positions_csv)
                    )

                # Regenerate CSVs
                csv_writer = CsvWriter()
                csv_writer.generate_positions_csv(matched_positions, unmatched_opens, str(positions_csv))
                csv_writer.generate_summary_csv(matched_positions, event_parser.skip_events, str(summary_csv))

                # Re-apply wallet aliases after CSV regeneration
                apply_aliases(
                    csv_path=positions_csv,
                    aliases_path=Path("wallet_aliases.json")
                )

                print(f"  Updated {positions_csv}")

                # Regenerate charts
                if not args.skip_charts and (want_all or 'charts' in report_modules):
                    generate_charts(matched_positions, str(output_dir), skip_events=event_parser.skip_events)

                # Regenerate loss report with updated positions
                if not args.no_loss_analysis and (want_all or 'loss' in report_modules):
                    try:
                        _generate_loss_report(
                            matched_positions,
                            str(loss_report_path),
                            insuf_csv_path,
                        )
                    except Exception as e:
                        print(f"  Warning: loss analysis failed: {e}")

                # Updated summary
                total_pnl = sum(p.pnl_sol for p in matched_positions if p.pnl_sol is not None)
                meteora_count = sum(1 for p in matched_positions if p.pnl_source == 'meteora')
                print(f"  Updated PnL: {total_pnl:.4f} SOL ({meteora_count} meteora)")

            if still_failed:
                print(f"\n  Still failed: {', '.join(still_failed)}")
                print(f"  These positions will appear as 'pending' in CSV.")

    # Auto-run lpagent cross-check (after positions.csv is written)
    # Skip in --merge mode. Fully silent when API key is not configured.
    if args.lpagent and not args.merge:
        _lpagent_api_key = os.environ.get("LPAGENT_API_KEY", "")
        if _lpagent_api_key:
            try:
                yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                watermark = _read_watermark(str(output_dir))
                next_day = (
                    datetime.strptime(watermark, "%Y-%m-%d") + timedelta(days=1)
                ).strftime("%Y-%m-%d")
                if next_day <= yesterday:
                    print(f"\n[Cross-check] Syncing {next_day} -> {yesterday}...")
                    count = _run_cross_check(
                        next_day,
                        yesterday,
                        str(positions_csv),
                        str(output_dir),
                        silent_if_empty=True,
                    )
                    # Always update watermark on successful sync (avoids re-querying clean days)
                    _write_watermark(str(output_dir), yesterday)
                    if count > 0:
                        print(f"  Watermark updated to {yesterday}")
            except Exception as e:
                # Never crash the main pipeline due to cross-check errors
                print(f"\n[Cross-check] Warning: auto-run failed: {e}")

    # Retroactively enrich lpagent_backfill rows using events already in archive/
    # (Discord events archived in earlier runs are invisible to the normal merge path.)
    if not args.merge:
        try:
            _retro_enrich_lpagent_from_archive(str(positions_csv))
        except Exception as e:
            print(f"\n[Retro-enrich] Warning: failed: {e}")

    from valhalla.discord_gaps import report_discord_gaps
    report_discord_gaps(str(positions_csv))

    print(f"\nDone!")


if __name__ == '__main__':
    main()
