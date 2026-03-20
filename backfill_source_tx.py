#!/usr/bin/env python3
"""
backfill_source_tx.py — Backfill missing target_tx_signature values in positions.csv.

Older positions in output/positions.csv may have target_tx_signature empty even though
the original Discord logs (now in archive/) contained the Target Tx data.

This script re-parses all archive files and fills in any missing signatures for
positions with losses exceeding the SOURCE_WALLET_MIN_LOSS_PCT threshold.

Algorithm:
  1. Read output/positions.csv
  2. Find positions where close_reason is in LOSS_REASONS and target_tx_signature is empty
  3. Scan archive/*.txt and archive/*.html files
  4. For each archive file, re-parse to get OpenEvents
  5. For each OpenEvent whose position_id is in missing_ids, take the first
     target_tx_signature and store it
  6. Update positions.csv rows with found signatures
  7. Print summary: how many found, how many still missing
"""

import argparse
import csv
import os
import tempfile
from pathlib import Path

from decimal import Decimal

from valhalla.analysis_config import SOURCE_WALLET_MIN_LOSS_PCT
from valhalla.models import extract_date_from_filename
from valhalla.readers import PlainTextReader, HtmlReader, detect_input_format
from valhalla.event_parser import EventParser


def _parse_archive_file(filepath: Path) -> EventParser:
    """Parse a single archive file and return an EventParser with populated open_events."""
    fmt = detect_input_format(str(filepath))
    if fmt == "html":
        reader = HtmlReader(str(filepath))
    else:
        reader = PlainTextReader(str(filepath))

    messages = reader.read()

    # Determine base date (same logic as valhalla_parser_v2.py)
    file_date = extract_date_from_filename(str(filepath))
    if not file_date and reader.header_date:
        file_date = reader.header_date

    # If messages contain embedded full timestamps, no base_date is needed
    has_full_timestamps = any(
        "[" in msg.timestamp and "T" in msg.timestamp and len(msg.timestamp) > 7
        for msg in messages
    )
    if has_full_timestamps:
        file_date = None

    file_parser = EventParser(base_date=file_date)
    file_parser.parse_messages(messages)
    return file_parser


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill missing target_tx_signature values in positions.csv from archive files."
    )
    parser.add_argument(
        "--positions-csv",
        default="output/positions.csv",
        help="Path to positions.csv (default: output/positions.csv)",
    )
    parser.add_argument(
        "--archive-dir",
        default="archive",
        help="Directory containing archived Discord log files (default: archive/)",
    )
    args = parser.parse_args()

    positions_csv = Path(args.positions_csv)
    archive_dir = Path(args.archive_dir)

    # ------------------------------------------------------------------
    # Step 1: Read positions.csv
    # ------------------------------------------------------------------
    if not positions_csv.exists():
        print(f"Error: {positions_csv} not found.")
        return

    print(f"Reading {positions_csv}...")
    with open(positions_csv, "r", newline="", encoding="utf-8") as f:
        reader_obj = csv.DictReader(f)
        fieldnames = reader_obj.fieldnames or []
        rows = list(reader_obj)

    print(f"  Loaded {len(rows)} position(s).")

    # ------------------------------------------------------------------
    # Step 2: Find positions missing target_tx_signature (by PnL threshold)
    # ------------------------------------------------------------------
    threshold = Decimal(str(SOURCE_WALLET_MIN_LOSS_PCT)) if SOURCE_WALLET_MIN_LOSS_PCT is not None else None

    missing_ids: set = set()
    for row in rows:
        tx_sig = row.get("target_tx_signature", "").strip()
        position_id = row.get("position_id", "").strip()
        close_reason = row.get("close_reason", "").strip()
        pnl_pct_str = row.get("pnl_pct", "").strip()

        if tx_sig or not position_id or close_reason == "still_open":
            continue

        # Filter by PnL threshold (same logic as source_wallet_analyzer)
        if threshold is not None:
            try:
                pnl_pct = Decimal(pnl_pct_str) if pnl_pct_str else None
            except Exception:
                pnl_pct = None
            if pnl_pct is None or pnl_pct > threshold:
                continue

        missing_ids.add(position_id)

    if not missing_ids:
        print("No positions with missing target_tx_signature found. Nothing to do.")
        return

    print(f"  Found {len(missing_ids)} position(s) with missing target_tx_signature.")

    # ------------------------------------------------------------------
    # Step 3: Scan archive files
    # ------------------------------------------------------------------
    if not archive_dir.exists():
        print(f"Error: archive directory '{archive_dir}' not found.")
        return

    archive_files = [
        f for f in archive_dir.iterdir()
        if f.is_file() and f.suffix in (".txt", ".html")
    ]

    if not archive_files:
        print(f"No .txt or .html files found in {archive_dir}/")
        return

    print(f"\nScanning {len(archive_files)} archive file(s)...")

    # Collect found signatures: position_id -> tx_signature
    found_signatures: dict = {}

    for filepath in sorted(archive_files):
        if not missing_ids - set(found_signatures.keys()):
            # All missing IDs have been found; no need to scan further
            break

        try:
            file_parser = _parse_archive_file(filepath)
        except Exception as exc:
            print(f"  Warning: could not parse {filepath.name}: {exc}")
            continue

        found_in_file = 0
        for event in file_parser.open_events:
            pid = event.position_id
            if pid in missing_ids and pid not in found_signatures:
                if event.target_tx_signatures:
                    found_signatures[pid] = event.target_tx_signatures[0]
                    found_in_file += 1

        if found_in_file:
            print(f"  {filepath.name}: found {found_in_file} signature(s)")

    # ------------------------------------------------------------------
    # Step 6: Update rows in memory
    # ------------------------------------------------------------------
    updated_count = 0
    for row in rows:
        pid = row.get("position_id", "").strip()
        if pid in found_signatures:
            row["target_tx_signature"] = found_signatures[pid]
            updated_count += 1

    # ------------------------------------------------------------------
    # Step 7: Write updated CSV atomically (temp file + rename)
    # ------------------------------------------------------------------
    if updated_count > 0:
        tmp_fd, tmp_path = tempfile.mkstemp(
            dir=positions_csv.parent, suffix=".tmp"
        )
        try:
            with os.fdopen(tmp_fd, "w", newline="", encoding="utf-8") as out_f:
                writer = csv.DictWriter(out_f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)
            # Atomic rename
            os.replace(tmp_path, positions_csv)
        except Exception:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        print(f"\nUpdated {updated_count} position(s) in {positions_csv}.")
    else:
        print("\nNo rows were updated (no matching open events found in archive files).")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    still_missing = missing_ids - set(found_signatures.keys())
    print(f"\nSummary:")
    print(f"  Positions with missing signature:  {len(missing_ids)}")
    print(f"  Signatures found and backfilled:   {len(found_signatures)}")
    print(f"  Still missing:                     {len(still_missing)}")

    if still_missing:
        print("\nPosition IDs still missing target_tx_signature:")
        for pid in sorted(still_missing):
            print(f"  {pid}")


if __name__ == "__main__":
    main()
