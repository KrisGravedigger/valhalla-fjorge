#!/usr/bin/env python3
"""
Valhalla Bot Discord DM Log Parser v2
Parses Discord DM plain text logs and calculates per-position PnL using Meteora DLMM API.
"""

import re
import argparse
import shutil
from pathlib import Path
from typing import Dict
from datetime import datetime

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
from valhalla.charts import generate_charts


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

    args = parser.parse_args()

    # Create output directory if needed
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

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
        # 1. Filename prefix (YYYYMMDD_*.txt)
        # 2. In-file date header (first line)
        # 3. User prompt if neither found
        file_date = None
        date_source = None

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
        else:
            print(f"  No date available")

        # Parse events with date context
        print(f"Parsing events (date: {file_date or 'none'})...")
        file_parser = EventParser(base_date=file_date)
        file_parser.parse_messages(messages)

        # Merge events into main parser
        event_parser.open_events.extend(file_parser.open_events)
        event_parser.close_events.extend(file_parser.close_events)
        event_parser.rug_events.extend(file_parser.rug_events)
        event_parser.skip_events.extend(file_parser.skip_events)
        event_parser.swap_events.extend(file_parser.swap_events)
        event_parser.failsafe_events.extend(file_parser.failsafe_events)
        event_parser.add_liquidity_events.extend(file_parser.add_liquidity_events)

        # Track for archiving
        processed_files.append((input_file, file_date))

    # Step 2: Print aggregated event counts
    print(f"\nTotal parsed events across {len(input_files)} file(s):")

    print(f"  Open positions: {len(event_parser.open_events)}")
    print(f"  Close events: {len(event_parser.close_events)}")
    print(f"  Failsafe events: {len(event_parser.failsafe_events)}")
    print(f"  Add liquidity events: {len(event_parser.add_liquidity_events)}")
    print(f"  Rug events: {len(event_parser.rug_events)}")
    print(f"  Skip events: {len(event_parser.skip_events)}")
    print(f"  Swap events: {len(event_parser.swap_events)}")

    # Step 3: Resolve addresses
    resolved_addresses: Dict[str, str] = {}
    cache = AddressCache(cache_file)

    if not args.skip_rpc:
        print(f"\nResolving position addresses via Solana RPC...")
        rpc_client = SolanaRpcClient(args.rpc_url)
        resolver = PositionResolver(cache, rpc_client)

        # Collect all events with position IDs and tx signatures
        seen_pids = set()
        events_to_resolve = []
        for event in event_parser.open_events + event_parser.close_events:
            if event.tx_signatures and event.position_id not in seen_pids:
                seen_pids.add(event.position_id)
                events_to_resolve.append((event.position_id, event.tx_signatures))

        total = len(events_to_resolve)
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
        for event in event_parser.open_events + event_parser.close_events:
            cached = cache.get(event.position_id)
            if cached:
                resolved_addresses[event.position_id] = cached
        print(f"  Loaded {len(resolved_addresses)} addresses from cache")

    # Step 4: Calculate Meteora PnL
    meteora_results: Dict[str, MeteoraPnlResult] = {}

    if not args.skip_meteora and resolved_addresses:
        print(f"\nFetching Meteora PnL data...")
        meteora_calc = MeteoraPnlCalculator()

        total = len(resolved_addresses)
        for i, (pid, full_addr) in enumerate(resolved_addresses.items(), 1):
            print(f"  Fetching {i}/{total}: {pid}...", end='', flush=True)
            result = meteora_calc.calculate_pnl(full_addr)
            if result:
                meteora_results[pid] = result
                print(f" PnL: {result.pnl_sol:.4f} SOL (${result.pnl_usd:.2f})")
            else:
                print(f" FAILED")

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
    positions_csv = output_dir / 'positions.csv'
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

    print(f"  {positions_csv}")
    print(f"  {summary_csv}")

    # Step 6.5: Generate charts
    if not args.skip_charts:
        print(f"\nGenerating charts...")
        generate_charts(matched_positions, str(output_dir))

    # Step 6.6: Export to JSON if requested
    if args.export_json:
        print(f"\nExporting to JSON...")
        export_to_json(matched_positions, unmatched_opens, event_parser.skip_events, args.export_json)

    # Step 6.7: Archive processed files
    if not args.no_archive and processed_files:
        print(f"\nArchiving processed files...")
        archive_dir = Path('archive')
        archive_dir.mkdir(parents=True, exist_ok=True)

        for input_file, file_date in processed_files:
            input_path = Path(input_file)
            base_name = input_path.name

            # Determine archived filename - prepend date if not already present
            if file_date and not re.match(r'^\d{8}', base_name):
                # Prepend date to filename
                date_compact = file_date.replace('-', '')
                archive_name = f"{date_compact}_{base_name}"
            else:
                archive_name = base_name

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
    print(f"\nDone!")


if __name__ == '__main__':
    main()
