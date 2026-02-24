#!/usr/bin/env python3
"""
Valhalla Bot Discord DM Log Parser v2
Parses Discord DM plain text logs and calculates per-position PnL using Meteora DLMM API.
"""

import re
import argparse
import csv
import shutil
import statistics
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
from decimal import Decimal

# Import from valhalla package
from valhalla.models import extract_date_from_filename, MeteoraPnlResult
from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format
from valhalla.event_parser import EventParser
from valhalla.solana_rpc import AddressCache, SolanaRpcClient, PositionResolver
from valhalla.meteora import MeteoraPnlCalculator
from valhalla.matcher import PositionMatcher
from valhalla.csv_writer import CsvWriter
from valhalla.json_io import export_to_json, import_from_json, merge_with_imported
from valhalla.merge import merge_with_existing_csv, merge_positions_csvs
from valhalla.charts import generate_charts, generate_insufficient_balance_chart


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


def _generate_loss_report(
    positions: List,
    output_path: str,
) -> None:
    """Generate loss_analysis.md from matched positions."""
    from valhalla.loss_analyzer import (
        LossAnalyzer, FilterBacktester, StopLossLevelAnalyzer,
        WalletTrendAnalyzer, LOSS_REASONS,
    )

    analyzer = LossAnalyzer()
    result = analyzer.analyze(positions)

    lines: List[str] = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines.append("# Loss Analysis Report")
    lines.append(f"Generated: {now_str}")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 1: Overview
    # ------------------------------------------------------------------
    lines.append("## Overview")
    lines.append("")

    loss_rate = (
        result.loss_positions / result.closed_positions * 100.0
        if result.closed_positions > 0 else 0.0
    )
    rug_failsafe_count = sum(
        1 for p in positions
        if p.close_reason in {"rug", "rug_unknown_open", "failsafe", "failsafe_unknown_open"}
    )

    overview_rows = [
        ["Total positions (closed)", str(result.closed_positions)],
        ["Stop-loss exits", str(result.stop_loss_positions)],
        ["Rug / failsafe", str(rug_failsafe_count)],
        ["Total loss PnL", _fmt_sol(result.loss_pnl_sol)],
        ["Loss rate", f"{loss_rate:.1f}%"],
    ]
    lines.append(_md_table(["Metric", "Value"], overview_rows))
    lines.append("")

    # ------------------------------------------------------------------
    # Section 2: Risk Profile
    # ------------------------------------------------------------------
    lines.append("## Risk Profile: Stop-Loss vs All Trades")
    lines.append("")
    lines.append("Compares average token quality metrics for stop-loss exits vs all closed trades.")
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
                "token_age_days": "token_age_days",
            }.get(row.metric, row.metric)

            if row.metric == "mc_at_open":
                sl_val = _fmt_mc(row.sl_avg) if row.sl_avg is not None else "N/A"
                all_val = _fmt_mc(row.all_avg) if row.all_avg is not None else "N/A"
            elif row.metric == "token_age_days":
                sl_val = f"{row.sl_avg:.1f}d" if row.sl_avg is not None else "N/A"
                all_val = f"{row.all_avg:.1f}d" if row.all_avg is not None else "N/A"
            else:
                sl_val = f"{row.sl_avg:.0f}" if row.sl_avg is not None else "N/A"
                all_val = f"{row.all_avg:.0f}" if row.all_avg is not None else "N/A"

            diff_str = _fmt_pct(row.diff_pct)
            note = ""
            if row.sl_count < 3:
                note = f" (n={row.sl_count}, insufficient)"
            rp_rows.append([metric_label + note, sl_val, all_val, diff_str])

        lines.append(_md_table(
            ["Metric", "Stop-Loss Avg", "All Trades Avg", "Difference"],
            rp_rows,
        ))
    lines.append("")

    # ------------------------------------------------------------------
    # Section 3: Filter Backtest
    # ------------------------------------------------------------------
    lines.append("## Filter Backtest")
    lines.append("")
    lines.append("For each parameter: what if only trades meeting the threshold were taken?")
    lines.append("")

    PARAM_LABELS = {
        "jup_score": "jup_score (minimum threshold)",
        "mc_at_open": "mc_at_open (minimum threshold)",
        "token_age_days": "token_age_days (minimum threshold)",
    }

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
    # Section 4: Stop-Loss Level Distribution
    # ------------------------------------------------------------------
    lines.append("## Stop-Loss Level Distribution")
    lines.append("")
    lines.append("If your stop-loss had been set tighter:")
    lines.append("")

    if not result.sl_buckets or all(b.count == 0 for b in result.sl_buckets):
        lines.append("_No stop-loss positions with PnL percentage data available._")
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

    # ------------------------------------------------------------------
    # Section 5: Wallet Stop-Loss Flags
    # ------------------------------------------------------------------
    lines.append("## Wallet Stop-Loss Flags")
    lines.append("")

    if not result.wallet_flags:
        lines.append("No wallets flagged (all within normal stop-loss rates).")
    else:
        wf_rows = [
            [
                wf.wallet,
                f"{wf.overall_sl_rate_pct:.0f}%",
                f"{wf.recent_sl_rate_pct:.0f}%",
                str(wf.recent_position_count),
                wf.flag,
            ]
            for wf in result.wallet_flags
        ]
        lines.append(_md_table(
            ["Wallet", "Overall SL Rate", "Recent 7d SL Rate", "Recent Positions", "Flag"],
            wf_rows,
        ))
    lines.append("")

    # ------------------------------------------------------------------
    # Section 6: Source Wallet Comparison (placeholder)
    # ------------------------------------------------------------------
    lines.append("## Source Wallet Comparison")
    lines.append("")

    has_target_tx = any(
        getattr(pos, 'target_tx_signature', None)
        for pos in positions
        if pos.close_reason in LOSS_REASONS
    )

    if has_target_tx:
        tx_count = sum(
            1 for pos in positions
            if getattr(pos, 'target_tx_signature', None)
            and pos.close_reason in LOSS_REASONS
        )
        lines.append(f"{tx_count} position(s) have target transaction signatures available.")
        lines.append("Run `python valhalla_parser_v2.py --analyze-source` to populate source wallet comparison.")
    else:
        lines.append("No source wallet data available. Run Phase C (source_wallet_analyzer) to enable this section.")
    lines.append("")

    # Write file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))


def _print_backtest_table(param: str, rows: List) -> None:
    """Print a backtest sweep result table to terminal."""
    PARAM_LABELS = {
        "jup_score": "jup_score",
        "mc_at_open": "mc_at_open",
        "token_age_days": "token_age_days",
    }
    print(f"\n--- Backtest: {PARAM_LABELS.get(param, param)} ---")

    if not rows:
        print("  No data.")
        return

    headers = ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"]
    table_rows = []
    for brow in rows:
        if param == "mc_at_open":
            threshold_str = _fmt_mc(brow.threshold)
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
        'age': 'token_age_days',
        'token_age': 'token_age_days',
        'token_age_days': 'token_age_days',
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


def main():
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

    args = parser.parse_args()

    # Auto-run save_clipboard.ps1 (skip in merge mode)
    if not args.merge and not args.no_clipboard and not args.recover_insuf:
        clipboard_script = Path('save_clipboard.ps1')
        if clipboard_script.exists():
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
    event_parser = EventParser()  # Will initialize per-file
    processed_files = []  # Track successfully processed files for archiving

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
    positions_csv = output_dir / 'positions.csv'
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

    # Step 5.6: Merge with existing output if present
    summary_csv = output_dir / 'summary.csv'

    if positions_csv.exists():
        print(f"\nMerging with existing output...")
        matched_positions, unmatched_opens = merge_with_existing_csv(
            matched_positions, unmatched_opens, str(positions_csv)
        )

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

    print(f"  {positions_csv}")
    print(f"  {summary_csv}")

    # Step 6.5b: Generate loss analysis report
    if not args.no_loss_analysis:
        loss_report_path = output_dir / 'loss_analysis.md'
        print(f"\nGenerating loss analysis report...")
        try:
            _generate_loss_report(matched_positions, str(loss_report_path))
            print(f"  {loss_report_path}")
        except Exception as e:
            print(f"  Warning: loss analysis failed: {e}")

    # --backtest custom run (additive, prints to terminal)
    if hasattr(args, 'backtest') and args.backtest:
        _run_custom_backtest(matched_positions, args.backtest,
                             getattr(args, 'wallet', None))

    # Step 6.5: Generate charts
    if not args.skip_charts:
        print(f"\nGenerating charts...")
        generate_charts(matched_positions, str(output_dir))
        insuf_csv = output_dir / 'insufficient_balance.csv'
        generate_insufficient_balance_chart(str(insuf_csv), str(output_dir))

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
                print(f"  Updated {positions_csv}")

                # Regenerate charts
                if not args.skip_charts:
                    generate_charts(matched_positions, str(output_dir))

                # Regenerate loss report with updated positions
                if not args.no_loss_analysis:
                    try:
                        _generate_loss_report(matched_positions, str(loss_report_path))
                    except Exception as e:
                        print(f"  Warning: loss analysis failed: {e}")

                # Updated summary
                total_pnl = sum(p.pnl_sol for p in matched_positions if p.pnl_sol is not None)
                meteora_count = sum(1 for p in matched_positions if p.pnl_source == 'meteora')
                print(f"  Updated PnL: {total_pnl:.4f} SOL ({meteora_count} meteora)")

            if still_failed:
                print(f"\n  Still failed: {', '.join(still_failed)}")
                print(f"  These positions will appear as 'pending' in CSV.")

    print(f"\nDone!")


if __name__ == '__main__':
    main()
