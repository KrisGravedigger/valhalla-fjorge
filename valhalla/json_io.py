"""
JSON import/export functions for portable data persistence.
"""

import json
from decimal import Decimal
from typing import List, Tuple
from datetime import datetime

from .models import MatchedPosition, OpenEvent, SkipEvent, make_iso_datetime, normalize_token_age


def export_to_json(positions: List[MatchedPosition], unmatched_opens: List[OpenEvent],
                   skip_events: List[SkipEvent], output_path: str) -> None:
    """
    Export positions to JSON file for portable data persistence.

    Args:
        positions: List of matched positions
        unmatched_opens: List of still-open positions
        skip_events: List of skip events
        output_path: Path to output .valhalla.json file
    """
    # Convert positions to dicts
    positions_data = []
    for pos in positions:
        pos_dict = {
            "position_id": pos.position_id,
            "token": pos.token,
            "target_wallet": pos.target_wallet,
            "position_type": pos.position_type,
            "datetime_open": pos.datetime_open,
            "datetime_close": pos.datetime_close,
            "sol_deployed": str(pos.sol_deployed) if pos.sol_deployed is not None else None,
            "sol_received": str(pos.sol_received) if pos.sol_received is not None else None,
            "pnl_sol": str(pos.pnl_sol) if pos.pnl_sol is not None else None,
            "pnl_pct": str(pos.pnl_pct) if pos.pnl_pct is not None else None,
            "close_reason": pos.close_reason,
            "mc_at_open": pos.mc_at_open,
            "jup_score": pos.jup_score,
            "token_age": pos.token_age,
            "token_age_days": pos.token_age_days,
            "token_age_hours": pos.token_age_hours,
            "price_drop_pct": pos.price_drop_pct,
            "full_address": pos.full_address,
            "pnl_source": pos.pnl_source,
            "meteora_deposited": str(pos.meteora_deposited) if pos.meteora_deposited is not None else None,
            "meteora_withdrawn": str(pos.meteora_withdrawn) if pos.meteora_withdrawn is not None else None,
            "meteora_fees": str(pos.meteora_fees) if pos.meteora_fees is not None else None,
            "meteora_pnl": str(pos.meteora_pnl) if pos.meteora_pnl is not None else None
        }
        positions_data.append(pos_dict)

    # Convert unmatched opens to dicts
    still_open_data = []
    for open_event in unmatched_opens:
        age_days, age_hours = normalize_token_age(open_event.token_age)
        datetime_open = make_iso_datetime(open_event.date, open_event.timestamp)

        open_dict = {
            "position_id": open_event.position_id,
            "token": open_event.token_name,
            "target_wallet": open_event.target,
            "position_type": open_event.position_type,
            "datetime_open": datetime_open,
            "sol_deployed": str(Decimal(str(open_event.your_sol))),
            "mc_at_open": open_event.market_cap,
            "jup_score": open_event.jup_score,
            "token_age": open_event.token_age,
            "token_age_days": age_days,
            "token_age_hours": age_hours
        }
        still_open_data.append(open_dict)

    # Gather metadata
    target_wallets = set()
    dates = set()
    for pos in positions:
        target_wallets.add(pos.target_wallet)
        # Extract dates from datetime fields
        if pos.datetime_open and 'T' in pos.datetime_open:
            dates.add(pos.datetime_open.split('T')[0])
        if pos.datetime_close and 'T' in pos.datetime_close:
            dates.add(pos.datetime_close.split('T')[0])

    for open_event in unmatched_opens:
        target_wallets.add(open_event.target)
        datetime_open = make_iso_datetime(open_event.date, open_event.timestamp)
        if datetime_open and 'T' in datetime_open:
            dates.add(datetime_open.split('T')[0])

    # Create JSON structure
    export_data = {
        "version": "1.0",
        "export_timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "positions": positions_data,
        "still_open": still_open_data,
        "metadata": {
            "total_positions": len(positions),
            "total_still_open": len(unmatched_opens),
            "target_wallets": sorted(target_wallets),
            "date_range": sorted([d for d in dates if d])  # Filter out empty dates
        }
    }

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    print(f"  Exported {len(positions)} positions and {len(unmatched_opens)} still-open to {output_path}")


def import_from_json(json_path: str) -> Tuple[List[MatchedPosition], List[dict]]:
    """
    Import positions from .valhalla.json file.

    Args:
        json_path: Path to .valhalla.json file

    Returns:
        Tuple of (positions, still_open_dicts)
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Validate version
    version = data.get('version', 'unknown')
    major_version = version.split('.')[0]
    if major_version != '1':
        print(f"  Warning: JSON file version {version} may not be compatible (expected 1.x)")

    # Convert position dicts to MatchedPosition objects
    positions = []
    for pos_dict in data.get('positions', []):
        # Helper to parse optional Decimal
        def parse_optional_decimal(val):
            if val is None or val == '':
                return None
            return Decimal(str(val))

        positions.append(MatchedPosition(
            target_wallet=pos_dict.get('target_wallet', ''),
            token=pos_dict.get('token', ''),
            position_type=pos_dict.get('position_type', ''),
            sol_deployed=parse_optional_decimal(pos_dict.get('sol_deployed')),
            sol_received=parse_optional_decimal(pos_dict.get('sol_received')),
            pnl_sol=parse_optional_decimal(pos_dict.get('pnl_sol')),
            pnl_pct=parse_optional_decimal(pos_dict.get('pnl_pct')),
            close_reason=pos_dict.get('close_reason', ''),
            mc_at_open=float(pos_dict.get('mc_at_open', 0.0)),
            jup_score=int(pos_dict.get('jup_score', 0)),
            token_age=pos_dict.get('token_age', ''),
            token_age_days=pos_dict.get('token_age_days'),
            token_age_hours=pos_dict.get('token_age_hours'),
            price_drop_pct=pos_dict.get('price_drop_pct'),
            position_id=pos_dict.get('position_id', ''),
            full_address=pos_dict.get('full_address', ''),
            pnl_source=pos_dict.get('pnl_source', 'pending'),
            meteora_deposited=parse_optional_decimal(pos_dict.get('meteora_deposited')),
            meteora_withdrawn=parse_optional_decimal(pos_dict.get('meteora_withdrawn')),
            meteora_fees=parse_optional_decimal(pos_dict.get('meteora_fees')),
            meteora_pnl=parse_optional_decimal(pos_dict.get('meteora_pnl')),
            datetime_open=pos_dict.get('datetime_open', ''),
            datetime_close=pos_dict.get('datetime_close', '')
        ))

    still_open_dicts = data.get('still_open', [])

    print(f"  Imported {len(positions)} positions and {len(still_open_dicts)} still-open from {json_path}")

    return positions, still_open_dicts


def merge_with_imported(new_positions: List[MatchedPosition],
                        imported_positions: List[MatchedPosition],
                        new_opens: List[OpenEvent],
                        imported_still_open: List[dict]) -> Tuple[List[MatchedPosition], List[OpenEvent]]:
    """
    Merge new data with imported data.

    Args:
        new_positions: Newly parsed matched positions
        imported_positions: Positions from imported JSON
        new_opens: Newly parsed open events (still open)
        imported_still_open: Still-open dicts from imported JSON

    Returns:
        Tuple of (merged_positions, merged_unmatched_opens)
    """
    # Index positions by position_id (new data wins on conflicts)
    positions_by_id = {}

    # Add imported positions first
    for pos in imported_positions:
        if pos.position_id:
            positions_by_id[pos.position_id] = pos

    # Add new positions (overwriting if same position_id)
    for pos in new_positions:
        if pos.position_id:
            positions_by_id[pos.position_id] = pos

    # Check if any imported still_open are now closed in new data
    new_position_ids = {p.position_id for p in new_positions if p.position_id}

    # Convert imported still_open to OpenEvents, excluding those now closed
    still_open_events = []
    for open_dict in imported_still_open:
        position_id = open_dict.get('position_id', '')

        # Skip if this position is now closed
        if position_id and position_id in new_position_ids:
            print(f"    Position {position_id} was still-open in import, now closed")
            continue

        # Convert to OpenEvent
        # Extract timestamp from datetime_open
        datetime_open_str = open_dict.get('datetime_open', '')
        timestamp = f"[{datetime_open_str.split('T')[1][:5]}]" if datetime_open_str and 'T' in datetime_open_str else ''

        still_open_events.append(OpenEvent(
            timestamp=timestamp,
            position_type=open_dict.get('position_type', ''),
            token_name=open_dict.get('token', ''),
            token_pair=f"{open_dict.get('token', '')}-SOL",
            target=open_dict.get('target_wallet', ''),
            market_cap=float(open_dict.get('mc_at_open', 0.0)),
            token_age=open_dict.get('token_age', ''),
            jup_score=int(open_dict.get('jup_score', 0)),
            target_sol=float(open_dict.get('sol_deployed', '0')),
            your_sol=float(open_dict.get('sol_deployed', '0')),
            position_id=position_id,
            tx_signatures=[],
            date=open_dict.get('datetime_open', '').split('T')[0] if open_dict.get('datetime_open') else ''
        ))

    # Merge new_opens with still_open_events (dedup by position_id)
    opens_by_id = {}
    for event in still_open_events:
        if event.position_id:
            opens_by_id[event.position_id] = event

    for event in new_opens:
        if event.position_id:
            opens_by_id[event.position_id] = event  # New data wins

    merged_positions = list(positions_by_id.values())
    merged_opens = list(opens_by_id.values())

    print(f"  Merged: {len(merged_positions)} total positions, {len(merged_opens)} still open")

    return merged_positions, merged_opens
