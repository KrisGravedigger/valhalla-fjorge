"""
CSV file generation for positions and summary.
"""

import csv
from collections import defaultdict
from decimal import Decimal
from typing import Dict, List
from datetime import datetime

from .models import (
    MatchedPosition, OpenEvent, SkipEvent, InsufficientBalanceEvent,
    make_iso_datetime, normalize_token_age, parse_iso_datetime
)


class CsvWriter:
    """Generate CSV files"""

    def generate_positions_csv(self, matched_positions: List[MatchedPosition],
                               unmatched_opens: List[OpenEvent],
                               output_path: str) -> None:
        """Generate positions.csv with all matched positions"""
        # Sort: newest datetime_open first, positions without datetime at the end
        def _sort_key(pos):
            return pos.datetime_open if pos.datetime_open and pos.datetime_open[0].isdigit() else ""

        sorted_positions = sorted(matched_positions, key=_sort_key, reverse=True)

        # Sort still-open by datetime_open too (newest first)
        def _sort_key_open(ev):
            dt = make_iso_datetime(ev.date, ev.timestamp)
            return dt if dt and dt[0].isdigit() else ""

        sorted_opens = sorted(unmatched_opens, key=_sort_key_open, reverse=True)

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'datetime_open', 'datetime_close',
                'target_wallet', 'token', 'position_type',
                'sol_deployed', 'sol_received', 'pnl_sol', 'pnl_pct', 'close_reason',
                'mc_at_open', 'jup_score', 'token_age', 'token_age_days', 'token_age_hours',
                'price_drop_pct', 'position_id',
                'full_address', 'pnl_source', 'meteora_deposited', 'meteora_withdrawn',
                'meteora_fees', 'meteora_pnl'
            ])

            for pos in sorted_positions:
                writer.writerow([
                    pos.datetime_open,
                    pos.datetime_close,
                    pos.target_wallet,
                    pos.token,
                    pos.position_type,
                    f"{pos.sol_deployed:.4f}" if pos.sol_deployed is not None else "",
                    f"{pos.sol_received:.4f}" if pos.sol_received is not None else "",
                    f"{pos.pnl_sol:.4f}" if pos.pnl_sol is not None else "",
                    f"{pos.pnl_pct:.2f}" if pos.pnl_pct is not None else "",
                    pos.close_reason,
                    f"{pos.mc_at_open:.2f}",
                    pos.jup_score,
                    pos.token_age,
                    pos.token_age_days if pos.token_age_days is not None else "",
                    pos.token_age_hours if pos.token_age_hours is not None else "",
                    f"{pos.price_drop_pct:.2f}" if pos.price_drop_pct else "",
                    pos.position_id,
                    pos.full_address,
                    pos.pnl_source,
                    f"{pos.meteora_deposited:.4f}" if pos.meteora_deposited is not None else "",
                    f"{pos.meteora_withdrawn:.4f}" if pos.meteora_withdrawn is not None else "",
                    f"{pos.meteora_fees:.4f}" if pos.meteora_fees is not None else "",
                    f"{pos.meteora_pnl:.4f}" if pos.meteora_pnl is not None else ""
                ])

            # Add still-open positions
            for open_event in sorted_opens:
                datetime_open = make_iso_datetime(open_event.date, open_event.timestamp)
                age_days, age_hours = normalize_token_age(open_event.token_age)

                writer.writerow([
                    datetime_open,
                    "",  # No datetime_close
                    open_event.target,
                    open_event.token_name,
                    open_event.position_type,
                    f"{open_event.your_sol:.4f}",
                    "",  # No received amount
                    "",  # No PnL
                    "",  # No PnL %
                    "still_open",
                    f"{open_event.market_cap:.2f}",
                    open_event.jup_score,
                    open_event.token_age,
                    age_days if age_days is not None else "",
                    age_hours if age_hours is not None else "",
                    "",  # No price_drop_pct
                    open_event.position_id,
                    "",  # No full_address
                    "",  # No pnl_source
                    "",  # No meteora_deposited
                    "",  # No meteora_withdrawn
                    "",  # No meteora_fees
                    ""   # No meteora_pnl
                ])

    def generate_summary_csv(self, matched_positions: List[MatchedPosition],
                            skip_events: List[SkipEvent],
                            output_path: str) -> None:
        """Generate summary.csv with per-target statistics"""
        # Find reference time (latest datetime_close across all positions)
        ref_time = None
        for pos in matched_positions:
            if pos.datetime_close:
                dt = parse_iso_datetime(pos.datetime_close)
                if dt:
                    if ref_time is None or dt > ref_time:
                        ref_time = dt

        # Aggregate by target wallet
        target_stats: Dict[str, Dict] = defaultdict(lambda: {
            'total_positions': 0,
            'wins': 0,
            'losses': 0,
            'rugs': 0,
            'total_pnl_sol': Decimal('0'),
            'total_sol_deployed': Decimal('0'),
            'min_date': None,
            'max_date': None,
            'max_datetime_open': '',
            'mc_values': [],
            'jup_scores': [],
            'age_days_values': [],
            'positions_24h': [],
            'positions_72h': [],
            'positions_7d': []
        })

        for pos in matched_positions:
            stats = target_stats[pos.target_wallet]

            # Skip positions with no PnL data from counting
            if pos.pnl_sol is None:
                continue

            stats['total_positions'] += 1

            if pos.close_reason in ("rug", "rug_unknown_open"):
                stats['rugs'] += 1
                stats['losses'] += 1
            elif pos.close_reason == "unknown_open":
                stats['losses'] += 1  # Unknown close, count as loss
            elif pos.pnl_sol > 0:
                stats['wins'] += 1
            else:
                stats['losses'] += 1

            stats['total_pnl_sol'] += pos.pnl_sol
            if pos.sol_deployed is not None:
                stats['total_sol_deployed'] += pos.sol_deployed

            # Track latest datetime_open for this wallet
            if pos.datetime_open and pos.datetime_open > stats['max_datetime_open']:
                stats['max_datetime_open'] = pos.datetime_open

            # Collect token metrics (skip 0/None values)
            if pos.mc_at_open and pos.mc_at_open > 0:
                stats['mc_values'].append(pos.mc_at_open)
            if pos.jup_score and pos.jup_score > 0:
                stats['jup_scores'].append(pos.jup_score)
            if pos.token_age_days is not None and pos.token_age_days >= 0:
                stats['age_days_values'].append(pos.token_age_days)

            # Track date range
            for dt_str in [pos.datetime_open, pos.datetime_close]:
                if dt_str and 'T' in dt_str:
                    date_part = dt_str.split('T')[0]
                    if date_part:  # Not empty (e.g., "2026-02-12")
                        if stats['min_date'] is None or date_part < stats['min_date']:
                            stats['min_date'] = date_part
                        if stats['max_date'] is None or date_part > stats['max_date']:
                            stats['max_date'] = date_part

            # Collect positions for time windows (if ref_time is available)
            if ref_time and pos.datetime_close:
                close_dt = parse_iso_datetime(pos.datetime_close)
                if close_dt:
                    # Check if position falls within each time window
                    hours_diff = (ref_time - close_dt).total_seconds() / 3600

                    if hours_diff <= 24:
                        stats['positions_24h'].append(pos)
                    if hours_diff <= 72:
                        stats['positions_72h'].append(pos)
                    if hours_diff <= 168:  # 7 days = 168 hours
                        stats['positions_7d'].append(pos)

        # Add skip counts
        skip_counts = defaultdict(int)
        for skip_event in skip_events:
            skip_counts[skip_event.target] += 1

        # Sort wallets by most recent activity (newest first)
        sorted_targets = sorted(
            target_stats.items(),
            key=lambda item: item[1]['max_datetime_open'],
            reverse=True
        )

        # Write summary
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'target_wallet', 'total_positions', 'wins', 'losses', 'rugs', 'skips',
                'total_pnl_sol', 'avg_pnl_sol', 'win_rate_pct', 'avg_sol_deployed',
                'avg_mc', 'avg_jup_score', 'avg_token_age_days',
                'positions_24h', 'pnl_24h', 'win_rate_24h', 'rugs_24h',
                'positions_72h', 'pnl_72h', 'win_rate_72h', 'rugs_72h',
                'positions_7d', 'pnl_7d', 'win_rate_7d', 'rugs_7d',
                'avg_positions_per_day', 'date_range'
            ])

            for target, stats in sorted_targets:
                total_pos = stats['total_positions']
                wins = stats['wins']
                total_pnl = stats['total_pnl_sol']
                total_deployed = stats['total_sol_deployed']

                avg_pnl = total_pnl / total_pos if total_pos > 0 else Decimal('0')
                win_rate = Decimal(wins) / Decimal(total_pos) * Decimal('100') if total_pos > 0 else Decimal('0')
                avg_deployed = total_deployed / total_pos if total_pos > 0 else Decimal('0')

                # Calculate aggregate token metrics
                avg_mc = sum(stats['mc_values']) / len(stats['mc_values']) if stats['mc_values'] else 0
                avg_jup_score = sum(stats['jup_scores']) / len(stats['jup_scores']) if stats['jup_scores'] else 0
                avg_age_days = sum(stats['age_days_values']) / len(stats['age_days_values']) if stats['age_days_values'] else 0

                # Format date range
                if stats['min_date'] and stats['max_date']:
                    if stats['min_date'] == stats['max_date']:
                        date_range = stats['min_date']
                    else:
                        date_range = f"{stats['min_date']} to {stats['max_date']}"
                else:
                    date_range = ""

                # Calculate time-windowed stats
                def calc_window_stats(positions):
                    """Calculate stats for a time window of positions"""
                    if not positions:
                        return 0, Decimal('0'), Decimal('0'), 0

                    # Filter out positions with no PnL data
                    positions_with_pnl = [p for p in positions if p.pnl_sol is not None]
                    if not positions_with_pnl:
                        return 0, Decimal('0'), Decimal('0'), 0

                    count = len(positions_with_pnl)
                    pnl = sum(p.pnl_sol for p in positions_with_pnl)
                    wins_in_window = sum(1 for p in positions_with_pnl if p.pnl_sol > 0 and p.close_reason not in ("rug", "rug_unknown_open", "unknown_open"))
                    rugs = sum(1 for p in positions_with_pnl if p.close_reason in ("rug", "rug_unknown_open"))
                    win_rate = Decimal(wins_in_window) / Decimal(count) * Decimal('100') if count > 0 else Decimal('0')

                    return count, pnl, win_rate, rugs

                count_24h, pnl_24h, wr_24h, rugs_24h = calc_window_stats(stats['positions_24h'])
                count_72h, pnl_72h, wr_72h, rugs_72h = calc_window_stats(stats['positions_72h'])
                count_7d, pnl_7d, wr_7d, rugs_7d = calc_window_stats(stats['positions_7d'])

                # Calculate avg positions per day
                avg_pos_per_day = ""
                if stats['min_date'] and stats['max_date']:
                    try:
                        min_dt = datetime.strptime(stats['min_date'], "%Y-%m-%d")
                        max_dt = datetime.strptime(stats['max_date'], "%Y-%m-%d")
                        days_diff = (max_dt - min_dt).days + 1  # +1 to include both start and end
                        if days_diff >= 1:
                            avg_pos_per_day = f"{total_pos / days_diff:.2f}"
                    except ValueError:
                        pass

                writer.writerow([
                    target,
                    total_pos,
                    wins,
                    stats['losses'],
                    stats['rugs'],
                    skip_counts.get(target, 0),
                    f"{total_pnl:.4f}",
                    f"{avg_pnl:.4f}",
                    f"{win_rate:.2f}",
                    f"{avg_deployed:.4f}",
                    f"{avg_mc:.2f}",
                    f"{avg_jup_score:.2f}",
                    f"{avg_age_days:.2f}",
                    count_24h if ref_time else "",
                    f"{pnl_24h:.4f}" if ref_time and count_24h > 0 else "",
                    f"{wr_24h:.2f}" if ref_time and count_24h > 0 else "",
                    rugs_24h if ref_time and count_24h > 0 else "",
                    count_72h if ref_time else "",
                    f"{pnl_72h:.4f}" if ref_time and count_72h > 0 else "",
                    f"{wr_72h:.2f}" if ref_time and count_72h > 0 else "",
                    rugs_72h if ref_time and count_72h > 0 else "",
                    count_7d if ref_time else "",
                    f"{pnl_7d:.4f}" if ref_time and count_7d > 0 else "",
                    f"{wr_7d:.2f}" if ref_time and count_7d > 0 else "",
                    rugs_7d if ref_time and count_7d > 0 else "",
                    avg_pos_per_day,
                    date_range
                ])

    def generate_insufficient_balance_csv(self, events: List[InsufficientBalanceEvent],
                                          output_path: str) -> None:
        """Generate insufficient_balance.csv"""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'datetime', 'target_wallet', 'sol_balance',
                'effective_balance', 'required_amount'
            ])

            for event in events:
                dt = make_iso_datetime(event.date, event.timestamp)
                writer.writerow([
                    dt,
                    event.target,
                    f"{event.sol_balance:.4f}",
                    f"{event.effective_balance:.4f}",
                    f"{event.required_amount:.4f}"
                ])
