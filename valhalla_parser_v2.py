#!/usr/bin/env python3
"""
Valhalla Bot Discord DM Log Parser v2
Parses Discord DM plain text logs and calculates per-position PnL using Meteora DLMM API.
"""

import json
import os
import argparse
import csv
import shutil
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
from valhalla.coverage_gaps import detect_coverage_gaps as _detect_coverage_gaps
from valhalla.balance_recovery import recover_insufficient_balance_history as _recover_insufficient_balance_history


# ---------------------------------------------------------------------------
# LpAgent cross-check helpers
# ---------------------------------------------------------------------------
from valhalla.lpagent_pipeline import (
    read_watermark as _read_watermark,
    write_watermark as _write_watermark,
    run_cross_check as _run_cross_check,
    retro_enrich_lpagent_from_archive as _retro_enrich_lpagent_from_archive,
)


# ---------------------------------------------------------------------------
# Loss analysis helpers
# ---------------------------------------------------------------------------
from valhalla.recommendations import (
    generate_wallet_recommendations as _generate_wallet_recommendations,
)
from valhalla.loss_report import (
    generate_loss_report as _generate_loss_report,
    build_action_items as _build_action_items,
    load_insuf_balance_csv as _load_insuf_balance_csv,
)
from valhalla.loss_report.tables import md_table as _md_table
from valhalla.loss_report.formatters import (
    scenario_label as _scenario_label,
    fmt_sol as _fmt_sol,
    fmt_pct as _fmt_pct,
    fmt_mc as _fmt_mc,
)




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
            from valhalla.models import parse_iso_datetime as _piso

            trend_scorecards = WalletScorecardAnalyzer().analyze(matched_positions)

            # Still-open positions live in unmatched_opens (as OpenEvent), not in
            # matched_positions. The analyzer only sees closed ones, so we override
            # three fields with data that includes open positions:
            #   current_exposure_sol  — sum of your_sol for currently open positions
            #   positions_{7,3,1}d    — count of ALL opens (closed + still-open)
            #   days_since_last_position — based on most recent activity (open OR close)
            def _open_ev_dt(o):
                if not o.date or not o.timestamp:
                    return None
                try:
                    return datetime.strptime(
                        f"{o.date}T{o.timestamp.strip('[]')}:00",
                        "%Y-%m-%dT%H:%M:%S",
                    )
                except (ValueError, AttributeError):
                    return None

            exposure_by_wallet: Dict[str, Decimal] = {}
            opens_by_wallet: Dict[str, List[datetime]] = {}
            for p in matched_positions:
                dt = _piso(p.datetime_open)
                if dt is not None:
                    opens_by_wallet.setdefault(p.target_wallet, []).append(dt)
            for open_ev in unmatched_opens:
                dt = _open_ev_dt(open_ev)
                if dt is not None:
                    opens_by_wallet.setdefault(open_ev.target, []).append(dt)
                if open_ev.your_sol is not None:
                    exposure_by_wallet[open_ev.target] = (
                        exposure_by_wallet.get(open_ev.target, Decimal("0"))
                        + Decimal(str(open_ev.your_sol))
                    )

            closes_by_wallet: Dict[str, List[datetime]] = {}
            for p in matched_positions:
                dt = _piso(p.datetime_close)
                if dt is not None:
                    closes_by_wallet.setdefault(p.target_wallet, []).append(dt)

            all_activity = [
                d for lst in opens_by_wallet.values() for d in lst
            ] + [
                d for lst in closes_by_wallet.values() for d in lst
            ]
            ref_dt = max(all_activity) if all_activity else datetime.utcnow()
            cut_7d = ref_dt - timedelta(days=7)
            cut_3d = ref_dt - timedelta(days=3)
            cut_1d = ref_dt - timedelta(days=1)

            for sc in trend_scorecards:
                opens = opens_by_wallet.get(sc.wallet, [])
                sc.positions_7d = sum(1 for d in opens if d >= cut_7d)
                sc.positions_3d = sum(1 for d in opens if d >= cut_3d)
                sc.positions_1d = sum(1 for d in opens if d >= cut_1d)
                sc.current_exposure_sol = exposure_by_wallet.get(sc.wallet, Decimal("0"))
                last = None
                for d in opens + closes_by_wallet.get(sc.wallet, []):
                    if last is None or d > last:
                        last = d
                if last is not None:
                    sc.days_since_last_position = (ref_dt - last).days

            generate_wallet_trend_report(
                trend_scorecards,
                matched_positions,
                str(wallet_trend_path),
                PORTFOLIO_TOTAL_SOL,
                reference_date=ref_dt,
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
