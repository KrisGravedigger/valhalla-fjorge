"""
CSV merge functions for incremental processing.
"""

import csv
from pathlib import Path
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from .models import MatchedPosition, OpenEvent, normalize_token_age
from .csv_writer import CsvWriter


def merge_with_existing_csv(
    new_matched: List[MatchedPosition],
    new_still_open: List[OpenEvent],
    existing_csv_path: str
) -> Tuple[List[MatchedPosition], List[OpenEvent]]:
    """
    Merge newly parsed positions with existing output CSV.

    Merge rules:
    - Positions with pnl_source="meteora" in existing CSV are NEVER overwritten (fully populated)
    - New positions are added
    - Positions with pnl_source="pending" or "discord" can be upgraded with new data
    - Still-open positions can be upgraded (e.g., if we now have close data)
    - Existing positions not in new run are preserved

    Args:
        new_matched: Newly parsed matched positions (closed)
        new_still_open: Newly parsed still-open positions (OpenEvent objects)
        existing_csv_path: Path to existing positions.csv

    Returns:
        Tuple of (merged_matched, merged_still_open) where still_open are OpenEvent objects
    """
    print(f"  Reading existing CSV: {existing_csv_path}")

    # Read existing CSV rows
    existing_rows = []
    with open(existing_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        existing_rows = list(reader)

    print(f"  Existing positions: {len(existing_rows)}")

    # Helper functions for parsing CSV values
    def parse_optional_decimal(val: str) -> Optional[Decimal]:
        if not val or val.strip() == '':
            return None
        return Decimal(val)

    def parse_int(val: str) -> int:
        if not val or val.strip() == '':
            return 0
        return int(val)

    def parse_optional_int(val: str) -> Optional[int]:
        if not val or val.strip() == '':
            return None
        return int(val)

    def parse_float(val: str) -> float:
        if not val or val.strip() == '':
            return 0.0
        return float(val)

    def parse_optional_float(val: str) -> Optional[float]:
        if not val or val.strip() == '':
            return None
        return float(val)

    # Convert existing rows to MatchedPosition objects, indexed by position_id
    existing_by_id = {}

    for row in existing_rows:
        position_id = row.get('position_id', '').strip()

        # Skip rows without position_id (shouldn't happen, but be safe)
        if not position_id:
            continue

        existing_pos = MatchedPosition(
            timestamp_open=row.get('timestamp_open', ''),
            timestamp_close=row.get('timestamp_close', ''),
            target_wallet=row.get('target_wallet', ''),
            token=row.get('token', ''),
            position_type=row.get('position_type', ''),
            sol_deployed=parse_optional_decimal(row.get('sol_deployed', '')),
            sol_received=parse_optional_decimal(row.get('sol_received', '')),
            pnl_sol=parse_optional_decimal(row.get('pnl_sol', '')),
            pnl_pct=parse_optional_decimal(row.get('pnl_pct', '')),
            close_reason=row.get('close_reason', ''),
            mc_at_open=parse_float(row.get('mc_at_open', '0')),
            jup_score=parse_int(row.get('jup_score', '0')),
            token_age=row.get('token_age', ''),
            token_age_days=parse_optional_int(row.get('token_age_days', '')),
            token_age_hours=parse_optional_int(row.get('token_age_hours', '')),
            price_drop_pct=parse_optional_float(row.get('price_drop_pct', '')),
            position_id=position_id,
            full_address=row.get('full_address', ''),
            pnl_source=row.get('pnl_source', 'pending'),
            meteora_deposited=parse_optional_decimal(row.get('meteora_deposited', '')),
            meteora_withdrawn=parse_optional_decimal(row.get('meteora_withdrawn', '')),
            meteora_fees=parse_optional_decimal(row.get('meteora_fees', '')),
            meteora_pnl=parse_optional_decimal(row.get('meteora_pnl', '')),
            datetime_open=row.get('datetime_open', ''),
            datetime_close=row.get('datetime_close', '')
        )

        existing_by_id[position_id] = existing_pos

    # Index new positions by position_id
    new_matched_by_id = {p.position_id: p for p in new_matched if p.position_id}
    new_still_open_by_id = {e.position_id: e for e in new_still_open if e.position_id}

    # Helper: check if position is fully complete (has open + close + meteora PnL)
    def is_fully_complete(pos: MatchedPosition) -> bool:
        has_open = pos.close_reason not in ("unknown_open", "rug_unknown_open")
        has_close = pos.close_reason != "still_open"
        has_meteora = pos.pnl_source == "meteora"
        return has_open and has_close and has_meteora

    # Helper: merge open data from new into existing (keep existing Meteora PnL)
    def enrich_existing_with_open(existing: MatchedPosition, new_pos: MatchedPosition) -> MatchedPosition:
        """Take open-side data from new_pos, keep close+Meteora data from existing."""
        existing.timestamp_open = new_pos.timestamp_open or existing.timestamp_open
        existing.datetime_open = new_pos.datetime_open or existing.datetime_open
        if new_pos.token and new_pos.token != 'unknown':
            existing.token = new_pos.token
        if new_pos.position_type and new_pos.position_type != 'unknown':
            existing.position_type = new_pos.position_type
        if new_pos.mc_at_open and new_pos.mc_at_open > 0:
            existing.mc_at_open = new_pos.mc_at_open
        if new_pos.jup_score and new_pos.jup_score > 0:
            existing.jup_score = new_pos.jup_score
        if new_pos.token_age:
            existing.token_age = new_pos.token_age
            existing.token_age_days = new_pos.token_age_days
            existing.token_age_hours = new_pos.token_age_hours
        if existing.close_reason in ("unknown_open", "rug_unknown_open"):
            # Upgrade close_reason: rug_unknown_open -> rug, unknown_open -> normal
            if existing.close_reason == "rug_unknown_open":
                existing.close_reason = "rug"
            else:
                existing.close_reason = "normal"
        return existing

    # Merge logic
    merged_matched = []
    merged_still_open = []

    kept_complete_count = 0
    enriched_count = 0
    upgraded_count = 0
    new_count = 0
    kept_from_existing_count = 0

    # Process existing positions
    for position_id, existing_pos in existing_by_id.items():
        new_matched_pos = new_matched_by_id.get(position_id)
        new_still_open_event = new_still_open_by_id.get(position_id)

        # Rule 1: Fully complete (open + close + meteora) - keep as-is
        if is_fully_complete(existing_pos):
            merged_matched.append(existing_pos)
            kept_complete_count += 1
            continue

        # Rule 2: Has Meteora PnL but missing open data (unknown_open)
        # -> enrich with open data from new run if available
        if existing_pos.pnl_source == "meteora" and existing_pos.close_reason in ("unknown_open", "rug_unknown_open"):
            if new_matched_pos and new_matched_pos.timestamp_open:
                enriched = enrich_existing_with_open(existing_pos, new_matched_pos)
                merged_matched.append(enriched)
                enriched_count += 1
            elif new_still_open_event:
                # Open data came as still_open (new file only had the open, not the close)
                # Build a temporary MatchedPosition from the OpenEvent to use enrich helper
                open_as_matched = MatchedPosition(
                    timestamp_open=new_still_open_event.timestamp,
                    timestamp_close='',
                    target_wallet=new_still_open_event.target,
                    token=new_still_open_event.token_name,
                    position_type=new_still_open_event.position_type,
                    sol_deployed=Decimal(str(new_still_open_event.your_sol)) if new_still_open_event.your_sol else None,
                    sol_received=None, pnl_sol=None, pnl_pct=None,
                    close_reason='', mc_at_open=new_still_open_event.market_cap,
                    jup_score=new_still_open_event.jup_score,
                    token_age=new_still_open_event.token_age,
                    token_age_days=None, token_age_hours=None,
                    price_drop_pct=None, position_id=position_id,
                    full_address='', pnl_source='',
                    meteora_deposited=None, meteora_withdrawn=None,
                    meteora_fees=None, meteora_pnl=None,
                    datetime_open=f"{new_still_open_event.date}T{new_still_open_event.timestamp.strip('[]')}:00" if new_still_open_event.date and new_still_open_event.timestamp else '',
                    datetime_close=''
                )
                # Normalize token_age
                if new_still_open_event.token_age:
                    days, hours = normalize_token_age(new_still_open_event.token_age)
                    open_as_matched.token_age_days = days
                    open_as_matched.token_age_hours = hours
                enriched = enrich_existing_with_open(existing_pos, open_as_matched)
                merged_matched.append(enriched)
                enriched_count += 1
            else:
                # No new open data - keep as-is
                merged_matched.append(existing_pos)
                kept_from_existing_count += 1
            continue

        # Rule 3: Has Meteora PnL but still some edge case -> keep
        if existing_pos.pnl_source == "meteora":
            merged_matched.append(existing_pos)
            kept_complete_count += 1
            continue

        # Rule 4: No Meteora PnL (pending/discord) - upgrade if we have better data
        if new_matched_pos:
            merged_matched.append(new_matched_pos)
            upgraded_count += 1
        elif new_still_open_event:
            merged_still_open.append(new_still_open_event)
            upgraded_count += 1
        else:
            # No new data - keep existing
            if existing_pos.close_reason == 'still_open':
                open_event = OpenEvent(
                    timestamp=existing_pos.timestamp_open,
                    position_type=existing_pos.position_type,
                    token_name=existing_pos.token,
                    token_pair=f"{existing_pos.token}-SOL",
                    target=existing_pos.target_wallet,
                    market_cap=existing_pos.mc_at_open,
                    token_age=existing_pos.token_age,
                    jup_score=existing_pos.jup_score,
                    target_sol=float(existing_pos.sol_deployed) if existing_pos.sol_deployed else 0.0,
                    your_sol=float(existing_pos.sol_deployed) if existing_pos.sol_deployed else 0.0,
                    position_id=position_id,
                    tx_signatures=[],
                    date=existing_pos.datetime_open.split('T')[0] if existing_pos.datetime_open and 'T' in existing_pos.datetime_open else ''
                )
                merged_still_open.append(open_event)
            else:
                merged_matched.append(existing_pos)
            kept_from_existing_count += 1

    # Add truly new positions (not in existing CSV)
    for position_id, new_pos in new_matched_by_id.items():
        if position_id not in existing_by_id:
            merged_matched.append(new_pos)
            new_count += 1

    for position_id, new_event in new_still_open_by_id.items():
        if position_id not in existing_by_id:
            merged_still_open.append(new_event)
            new_count += 1

    # Print merge stats
    print(f"  Merge results:")
    print(f"    - Kept complete (open+close+meteora): {kept_complete_count}")
    print(f"    - Enriched (added open data to meteora positions): {enriched_count}")
    print(f"    - Upgraded (pending/discord -> better data): {upgraded_count}")
    print(f"    - New positions: {new_count}")
    print(f"    - Kept from existing (no new data): {kept_from_existing_count}")
    print(f"    - Total merged: {len(merged_matched)} matched, {len(merged_still_open)} still open")

    return merged_matched, merged_still_open


def merge_positions_csvs(csv_paths: List[str], output_dir: str) -> None:
    """
    Merge multiple positions.csv files, deduplicating by position_id.

    Args:
        csv_paths: List of paths to positions.csv files
        output_dir: Output directory for merged positions.csv and summary.csv
    """
    print(f"\nMerging {len(csv_paths)} positions.csv file(s)...")

    # Read all CSV files
    all_rows = []
    for csv_path in csv_paths:
        print(f"  Reading {csv_path}...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            print(f"    {len(rows)} positions")
            all_rows.extend(rows)

    print(f"  Total positions before deduplication: {len(all_rows)}")

    # Deduplicate by position_id
    # - Empty position_id: keep all (they're unique)
    # - Same position_id: keep the one from the LAST file (later file = newer data)
    seen_ids = {}
    deduplicated_rows = []

    for row in all_rows:
        position_id = row.get('position_id', '').strip()

        if not position_id:
            # Empty position_id - keep all of them
            deduplicated_rows.append(row)
        else:
            # Non-empty position_id - track and potentially replace
            seen_ids[position_id] = row

    # Add all tracked position_ids (latest version of each)
    deduplicated_rows.extend(seen_ids.values())

    print(f"  Positions after deduplication: {len(deduplicated_rows)}")

    # Convert rows back to MatchedPosition objects for summary calculation
    matched_positions = []

    for row in deduplicated_rows:
        # Skip still_open positions for summary (they have no PnL yet)
        if row.get('close_reason') == 'still_open':
            continue

        # Parse Decimal fields safely
        def parse_decimal(val: str) -> Decimal:
            if not val or val.strip() == '':
                return Decimal('0')
            return Decimal(val)

        def parse_optional_decimal(val: str) -> Optional[Decimal]:
            if not val or val.strip() == '':
                return None
            return Decimal(val)

        def parse_int(val: str) -> int:
            if not val or val.strip() == '':
                return 0
            return int(val)

        def parse_optional_int(val: str) -> Optional[int]:
            if not val or val.strip() == '':
                return None
            return int(val)

        def parse_float(val: str) -> float:
            if not val or val.strip() == '':
                return 0.0
            return float(val)

        def parse_optional_float(val: str) -> Optional[float]:
            if not val or val.strip() == '':
                return None
            return float(val)

        matched_positions.append(MatchedPosition(
            timestamp_open=row.get('timestamp_open', ''),
            timestamp_close=row.get('timestamp_close', ''),
            target_wallet=row.get('target_wallet', ''),
            token=row.get('token', ''),
            position_type=row.get('position_type', ''),
            sol_deployed=parse_optional_decimal(row.get('sol_deployed', '')),
            sol_received=parse_optional_decimal(row.get('sol_received', '')),
            pnl_sol=parse_optional_decimal(row.get('pnl_sol', '')),
            pnl_pct=parse_optional_decimal(row.get('pnl_pct', '')),
            close_reason=row.get('close_reason', ''),
            mc_at_open=parse_float(row.get('mc_at_open', '0')),
            jup_score=parse_int(row.get('jup_score', '0')),
            token_age=row.get('token_age', ''),
            token_age_days=parse_optional_int(row.get('token_age_days', '')),
            token_age_hours=parse_optional_int(row.get('token_age_hours', '')),
            price_drop_pct=parse_optional_float(row.get('price_drop_pct', '')),
            position_id=row.get('position_id', ''),
            full_address=row.get('full_address', ''),
            pnl_source=row.get('pnl_source', 'pending'),
            meteora_deposited=parse_optional_decimal(row.get('meteora_deposited', '')),
            meteora_withdrawn=parse_optional_decimal(row.get('meteora_withdrawn', '')),
            meteora_fees=parse_optional_decimal(row.get('meteora_fees', '')),
            meteora_pnl=parse_optional_decimal(row.get('meteora_pnl', '')),
            datetime_open=row.get('datetime_open', ''),
            datetime_close=row.get('datetime_close', '')
        ))

    # Write merged positions.csv
    output_path = Path(output_dir)
    positions_csv = output_path / 'positions.csv'
    summary_csv = output_path / 'summary.csv'

    print(f"\nWriting merged files...")

    # Write positions CSV (write all deduplicated rows, including still_open)
    with open(positions_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'timestamp_open', 'date_open', 'timestamp_close', 'date_close',
            'datetime_open', 'datetime_close',
            'target_wallet', 'token', 'position_type',
            'sol_deployed', 'sol_received', 'pnl_sol', 'pnl_pct', 'close_reason',
            'mc_at_open', 'jup_score', 'token_age', 'token_age_days', 'token_age_hours',
            'price_drop_pct', 'position_id',
            'full_address', 'pnl_source', 'meteora_deposited', 'meteora_withdrawn',
            'meteora_fees', 'meteora_pnl'
        ])
        writer.writeheader()
        writer.writerows(deduplicated_rows)

    print(f"  {positions_csv}")

    # Generate summary CSV (using only matched_positions, not still_open)
    csv_writer = CsvWriter()
    csv_writer.generate_summary_csv(matched_positions, [], str(summary_csv))  # Empty skip_events list

    print(f"  {summary_csv}")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Merge Summary")
    print(f"{'='*60}")
    print(f"Total positions in merged file: {len(deduplicated_rows)}")
    print(f"Closed positions: {len(matched_positions)}")
    print(f"Still open positions: {len(deduplicated_rows) - len(matched_positions)}")

    if matched_positions:
        total_pnl = sum(p.pnl_sol for p in matched_positions if p.pnl_sol is not None)
        print(f"Total PnL: {total_pnl:.4f} SOL")

    print(f"\nDone!")
