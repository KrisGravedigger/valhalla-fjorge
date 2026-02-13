#!/usr/bin/env python3
"""
Valhalla Bot Discord DM Log Parser
Parses trading logs and generates PnL analysis CSV files.
"""

import re
import csv
import argparse
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from collections import defaultdict


@dataclass
class OpenEvent:
    timestamp: str          # "[20:28]"
    position_type: str      # "Spot" / "BidAsk"
    token_name: str         # "BadBunny"
    token_pair: str         # "BadBunny-SOL"
    target: str             # "20260125_C5JXfmK"
    market_cap: float       # 1047047.227
    token_age: str          # "5h ago"
    jup_score: int          # 81
    target_sol: float       # 2.0000
    your_sol: float         # 4.0000
    position_id: str        # "BUeWH73d"


@dataclass
class CloseEvent:
    timestamp: str
    target: str
    starting_sol: float
    starting_usd: float
    ending_sol: float
    ending_usd: float
    total_sol: float
    active_positions: int
    position_id: str        # "BUeWH73d"


@dataclass
class RugEvent:
    timestamp: str
    target: str
    token_pair: str
    position_address: str
    price_drop: float
    threshold: float
    position_id: Optional[str] = None


@dataclass
class SkipEvent:
    timestamp: str
    target: str
    reason: str
    token_name: str
    token_address: str


@dataclass
class FailsafeEvent:
    timestamp: str
    position_id: str


@dataclass
class AddLiquidityEvent:
    timestamp: str
    position_id: str
    target: str
    amount_sol: float


@dataclass
class SwapEvent:
    timestamp: str
    amount: str
    token_name: str
    token_address: str


@dataclass
class MatchedPosition:
    """A matched open/close position with PnL calculation"""
    timestamp_open: str
    timestamp_close: str
    target_wallet: str
    token: str
    position_type: str
    sol_deployed: Decimal
    sol_received: Decimal
    pnl_sol: Decimal
    pnl_pct: Decimal
    close_reason: str
    mc_at_open: float
    jup_score: int
    token_age: str
    price_drop_pct: Optional[float] = None
    position_id: str = ""


class ValhallaParser:
    """Parser for Valhalla Bot Discord logs"""

    # Regex patterns
    TIMESTAMP_PATTERN = r'\[(\d{2}:\d{2})\]'
    TARGET_PATTERN = r'Target:\s*(\S+)'
    POSITION_TYPE_PATTERN = r'(Spot|BidAsk)\s+1-Sided Position\s*\|\s*(\S+)-SOL'
    MARKET_CAP_PATTERN = r'MC:\s*\$([\d,]+\.?\d*)'
    TOKEN_AGE_PATTERN = r'Age:\s*(.+?)(?:\n|$)'
    JUP_SCORE_PATTERN = r'Jup Score:\s*(\d+)'
    YOUR_POS_PATTERN = r'Your Pos:.*?SOL:\s*([\d.]+)'
    TARGET_POS_PATTERN = r'Target Pos:.*?SOL:\s*([\d.]+)'

    # Position ID patterns
    OPEN_POSITION_ID_PATTERN = r'Opened New DLMM Position!\s*\((\w+)\)'
    CLOSE_POSITION_ID_PATTERN = r'Closed DLMM Position!\s*\((\w+)\)'
    FAILSAFE_POSITION_ID_PATTERN = r'Failsafe Activated \(DLMM\)\s*\((\w+)\)'
    ADD_LIQUIDITY_POSITION_ID_PATTERN = r'Added DLMM Liquidity\s*\((\w+)\)'
    LIQUIDITY_AMOUNT_PATTERN = r'Amount:\s*([\d.]+)\s*SOL'

    # Close event patterns
    STARTING_SOL_PATTERN = r'Starting SOL balance:\s*([\d.]+)\s*SOL\s*\(\$([\d,.]+)\s*USD\)'
    ENDING_SOL_PATTERN = r'Ending SOL balance:\s*([\d.]+)\s*SOL\s*\(\$([\d,.]+)\s*USD\)'
    TOTAL_SOL_PATTERN = r'Total SOL balance:\s*([\d.]+)\s*SOL.*?\((\d+)\s*Active'

    # Rug event patterns
    RUG_TARGET_PATTERN = r'Copied From:\s*(\S+)\)'
    PRICE_DROP_PATTERN = r'Price Drop:\s*([\d.]+)%'
    RUG_THRESHOLD_PATTERN = r'Rug Check Threshold:\s*([\d.]+)%'
    POSITION_ADDRESS_PATTERN = r'Position:\s*(\S+)'
    PAIR_PATTERN = r'Pair:\s*(\S+)'

    # Skip event markers (plain strings, matched with `in`)
    SKIP_REASON_AGE_MARKER = 'Skipping position due to token age restriction'
    SKIP_REASON_JUP_MARKER = 'Skipping position due to low Jupiter organic score restriction'
    SKIP_REASON_SOL_ONLY_MARKER = 'Skipping position due to SOL-only deposit restriction'
    SKIP_TOKEN_PATTERN = r'Token\s+([^:]+?):\s*(\S+)'

    # Swap pattern
    SWAP_PATTERN = r'Swapped\s+([\d,]+|all)\s+(.+?)\s+\((\S+)\)'

    def __init__(self):
        self.open_events: List[OpenEvent] = []
        self.close_events: List[CloseEvent] = []
        self.rug_events: List[RugEvent] = []
        self.skip_events: List[SkipEvent] = []
        self.swap_events: List[SwapEvent] = []
        self.failsafe_events: List[FailsafeEvent] = []
        self.add_liquidity_events: List[AddLiquidityEvent] = []

    def parse_file(self, file_path: str) -> None:
        """Parse the log file and extract all events"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Split into individual messages by timestamp at start of line
        messages = re.split(r'^(?=\[\d{2}:\d{2}\])', content, flags=re.MULTILINE)

        for message in messages:
            if not message.strip():
                continue

            self._classify_and_parse_message(message)

    def _classify_and_parse_message(self, message: str) -> None:
        """Classify message type and parse accordingly"""
        # Skip "already closed" messages
        if "was already closed" in message:
            return

        # Check for each event type
        if "Opened New DLMM Position!" in message:
            event = self._parse_open_event(message)
            if event:
                self.open_events.append(event)

        elif "Closed DLMM Position!" in message:
            event = self._parse_close_event(message)
            if event:
                self.close_events.append(event)

        elif "Failsafe Activated (DLMM)" in message:
            event = self._parse_failsafe_event(message)
            if event:
                self.failsafe_events.append(event)

        elif "Added DLMM Liquidity" in message:
            event = self._parse_add_liquidity_event(message)
            if event:
                self.add_liquidity_events.append(event)

        elif "Rug Check Stop Loss Executed" in message:
            event = self._parse_rug_event(message)
            if event:
                self.rug_events.append(event)

        elif "Skipping position due to" in message:
            event = self._parse_skip_event(message)
            if event:
                self.skip_events.append(event)

        elif "Insufficient Effective Balance" in message:
            # Handle as a skip event or separate tracking
            pass

        elif "Swapped" in message:
            event = self._parse_swap_event(message)
            if event:
                self.swap_events.append(event)

    def _parse_open_event(self, message: str) -> Optional[OpenEvent]:
        """Parse an open position event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            target_match = re.search(self.TARGET_PATTERN, message)
            position_type_match = re.search(self.POSITION_TYPE_PATTERN, message)
            mc_match = re.search(self.MARKET_CAP_PATTERN, message)
            age_match = re.search(self.TOKEN_AGE_PATTERN, message)
            jup_match = re.search(self.JUP_SCORE_PATTERN, message)
            your_sol_match = re.search(self.YOUR_POS_PATTERN, message)
            target_sol_match = re.search(self.TARGET_POS_PATTERN, message)
            position_id_match = re.search(self.OPEN_POSITION_ID_PATTERN, message)

            if not all([timestamp_match, target_match, position_type_match,
                       mc_match, age_match, jup_match, your_sol_match, target_sol_match, position_id_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            position_type = position_type_match.group(1)
            token_name = position_type_match.group(2)
            token_pair = f"{token_name}-SOL"
            target = target_match.group(1)
            market_cap = float(mc_match.group(1).replace(',', ''))
            token_age = age_match.group(1).strip()
            jup_score = int(jup_match.group(1))
            target_sol = float(target_sol_match.group(1))
            your_sol = float(your_sol_match.group(1))
            position_id = position_id_match.group(1)

            return OpenEvent(
                timestamp=timestamp,
                position_type=position_type,
                token_name=token_name,
                token_pair=token_pair,
                target=target,
                market_cap=market_cap,
                token_age=token_age,
                jup_score=jup_score,
                target_sol=target_sol,
                your_sol=your_sol,
                position_id=position_id
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse open event: {e}")
            return None

    def _parse_close_event(self, message: str) -> Optional[CloseEvent]:
        """Parse a close position event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            target_match = re.search(self.TARGET_PATTERN, message)
            starting_match = re.search(self.STARTING_SOL_PATTERN, message)
            ending_match = re.search(self.ENDING_SOL_PATTERN, message)
            total_match = re.search(self.TOTAL_SOL_PATTERN, message)
            position_id_match = re.search(self.CLOSE_POSITION_ID_PATTERN, message)

            if not all([timestamp_match, target_match, starting_match, ending_match, total_match, position_id_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            target = target_match.group(1)
            starting_sol = float(starting_match.group(1))
            starting_usd = float(starting_match.group(2).replace(',', ''))
            ending_sol = float(ending_match.group(1))
            ending_usd = float(ending_match.group(2).replace(',', ''))
            total_sol = float(total_match.group(1))
            active_positions = int(total_match.group(2))
            position_id = position_id_match.group(1)

            return CloseEvent(
                timestamp=timestamp,
                target=target,
                starting_sol=starting_sol,
                starting_usd=starting_usd,
                ending_sol=ending_sol,
                ending_usd=ending_usd,
                total_sol=total_sol,
                active_positions=active_positions,
                position_id=position_id
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse close event: {e}")
            return None

    def _parse_failsafe_event(self, message: str) -> Optional[FailsafeEvent]:
        """Parse a failsafe activation event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            position_id_match = re.search(self.FAILSAFE_POSITION_ID_PATTERN, message)

            if not all([timestamp_match, position_id_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            position_id = position_id_match.group(1)

            return FailsafeEvent(
                timestamp=timestamp,
                position_id=position_id
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse failsafe event: {e}")
            return None

    def _parse_add_liquidity_event(self, message: str) -> Optional[AddLiquidityEvent]:
        """Parse an add liquidity event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            position_id_match = re.search(self.ADD_LIQUIDITY_POSITION_ID_PATTERN, message)
            target_match = re.search(self.TARGET_PATTERN, message)
            amount_match = re.search(self.LIQUIDITY_AMOUNT_PATTERN, message)

            if not all([timestamp_match, position_id_match, target_match, amount_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            position_id = position_id_match.group(1)
            target = target_match.group(1)
            amount_sol = float(amount_match.group(1))

            return AddLiquidityEvent(
                timestamp=timestamp,
                position_id=position_id,
                target=target,
                amount_sol=amount_sol
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse add liquidity event: {e}")
            return None

    def _parse_rug_event(self, message: str) -> Optional[RugEvent]:
        """Parse a rug pull event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            target_match = re.search(self.RUG_TARGET_PATTERN, message)
            pair_match = re.search(self.PAIR_PATTERN, message)
            position_match = re.search(self.POSITION_ADDRESS_PATTERN, message)
            drop_match = re.search(self.PRICE_DROP_PATTERN, message)
            threshold_match = re.search(self.RUG_THRESHOLD_PATTERN, message)

            if not all([timestamp_match, target_match, pair_match, position_match, drop_match, threshold_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            target = target_match.group(1)
            token_pair = pair_match.group(1)
            position_address = position_match.group(1)
            price_drop = float(drop_match.group(1))
            threshold = float(threshold_match.group(1))

            return RugEvent(
                timestamp=timestamp,
                target=target,
                token_pair=token_pair,
                position_address=position_address,
                price_drop=price_drop,
                threshold=threshold
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse rug event: {e}")
            return None

    def _parse_skip_event(self, message: str) -> Optional[SkipEvent]:
        """Parse a skip event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            target_match = re.search(self.TARGET_PATTERN, message)

            if not all([timestamp_match, target_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            target = target_match.group(1)

            # Determine reason
            if self.SKIP_REASON_AGE_MARKER in message:
                reason = "token age restriction"
            elif self.SKIP_REASON_JUP_MARKER in message:
                reason = "low Jupiter organic score"
            elif self.SKIP_REASON_SOL_ONLY_MARKER in message:
                reason = "SOL-only deposit restriction"
            else:
                reason = "unknown"

            # Extract token info - first token line is the actual token
            token_matches = re.findall(self.SKIP_TOKEN_PATTERN, message)
            if token_matches:
                token_name = token_matches[0][0].strip()
                token_address = token_matches[0][1].strip()
            else:
                token_name = "unknown"
                token_address = "unknown"

            return SkipEvent(
                timestamp=timestamp,
                target=target,
                reason=reason,
                token_name=token_name,
                token_address=token_address
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse skip event: {e}")
            return None

    def _parse_swap_event(self, message: str) -> Optional[SwapEvent]:
        """Parse a swap event"""
        try:
            timestamp_match = re.search(self.TIMESTAMP_PATTERN, message)
            swap_match = re.search(self.SWAP_PATTERN, message)

            if not all([timestamp_match, swap_match]):
                return None

            timestamp = f"[{timestamp_match.group(1)}]"
            amount = swap_match.group(1)
            token_name = swap_match.group(2)
            token_address = swap_match.group(3)

            return SwapEvent(
                timestamp=timestamp,
                amount=amount,
                token_name=token_name,
                token_address=token_address
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse swap event: {e}")
            return None

    def match_positions(self) -> Tuple[List[MatchedPosition], List[OpenEvent]]:
        """
        Match open events with close/rug events using position_id lookup.
        Returns (matched_positions, unmatched_opens)
        """
        matched_positions: List[MatchedPosition] = []

        # Index opens by position_id
        open_by_id: Dict[str, OpenEvent] = {}
        for event in self.open_events:
            open_by_id[event.position_id] = event

        # Index failsafe events by position_id
        failsafe_ids = {e.position_id for e in self.failsafe_events}

        # Index add_liquidity events by position_id
        liquidity_by_id: Dict[str, List[AddLiquidityEvent]] = defaultdict(list)
        for event in self.add_liquidity_events:
            liquidity_by_id[event.position_id].append(event)

        # Match closes to opens by position_id
        matched_ids = set()

        for close_event in self.close_events:
            pid = close_event.position_id
            matched_ids.add(pid)

            if pid in open_by_id:
                open_event = open_by_id[pid]
                sol_deployed = Decimal(str(open_event.your_sol))

                # Add any extra liquidity
                for liq in liquidity_by_id.get(pid, []):
                    sol_deployed += Decimal(str(liq.amount_sol))

                sol_received = Decimal(str(close_event.ending_sol)) - Decimal(str(close_event.starting_sol))
                pnl_sol = sol_received - sol_deployed
                pnl_pct = (pnl_sol / sol_deployed * Decimal('100')) if sol_deployed > 0 else Decimal('0')

                close_reason = "failsafe" if pid in failsafe_ids else "normal"

                matched_positions.append(MatchedPosition(
                    timestamp_open=open_event.timestamp,
                    timestamp_close=close_event.timestamp,
                    target_wallet=close_event.target,
                    token=open_event.token_name,
                    position_type=open_event.position_type,
                    sol_deployed=sol_deployed,
                    sol_received=sol_received,
                    pnl_sol=pnl_sol,
                    pnl_pct=pnl_pct,
                    close_reason=close_reason,
                    mc_at_open=open_event.market_cap,
                    jup_score=open_event.jup_score,
                    token_age=open_event.token_age,
                    price_drop_pct=None,
                    position_id=pid
                ))
            else:
                # Close without matching open (pre-existing position)
                sol_received = Decimal(str(close_event.ending_sol)) - Decimal(str(close_event.starting_sol))
                matched_positions.append(MatchedPosition(
                    timestamp_open="",
                    timestamp_close=close_event.timestamp,
                    target_wallet=close_event.target,
                    token="unknown",
                    position_type="unknown",
                    sol_deployed=Decimal('0'),
                    sol_received=sol_received,
                    pnl_sol=Decimal('0'),
                    pnl_pct=Decimal('0'),
                    close_reason="unknown_open",
                    mc_at_open=0.0,
                    jup_score=0,
                    token_age="",
                    price_drop_pct=None,
                    position_id=pid
                ))

        # Handle rug events (match by position_id if available)
        for rug_event in self.rug_events:
            if rug_event.position_id:
                pid = rug_event.position_id
                matched_ids.add(pid)

                if pid in open_by_id:
                    open_event = open_by_id[pid]
                    sol_deployed = Decimal(str(open_event.your_sol))

                    # Add any extra liquidity
                    for liq in liquidity_by_id.get(pid, []):
                        sol_deployed += Decimal(str(liq.amount_sol))

                    estimated_loss = sol_deployed * Decimal(str(rug_event.price_drop)) / Decimal('100')
                    pnl_sol = -estimated_loss
                    pnl_pct = -Decimal(str(rug_event.price_drop))

                    matched_positions.append(MatchedPosition(
                        timestamp_open=open_event.timestamp,
                        timestamp_close=rug_event.timestamp,
                        target_wallet=rug_event.target,
                        token=open_event.token_name,
                        position_type=open_event.position_type,
                        sol_deployed=sol_deployed,
                        sol_received=Decimal('0'),
                        pnl_sol=pnl_sol,
                        pnl_pct=pnl_pct,
                        close_reason="rug",
                        mc_at_open=open_event.market_cap,
                        jup_score=open_event.jup_score,
                        token_age=open_event.token_age,
                        price_drop_pct=rug_event.price_drop,
                        position_id=pid
                    ))
                else:
                    matched_positions.append(MatchedPosition(
                        timestamp_open="",
                        timestamp_close=rug_event.timestamp,
                        target_wallet=rug_event.target,
                        token="unknown",
                        position_type="unknown",
                        sol_deployed=Decimal('0'),
                        sol_received=Decimal('0'),
                        pnl_sol=Decimal('0'),
                        pnl_pct=Decimal('0'),
                        close_reason="rug_unknown_open",
                        mc_at_open=0.0,
                        jup_score=0,
                        token_age="",
                        price_drop_pct=rug_event.price_drop,
                        position_id=pid
                    ))
            else:
                # Rug event without position_id - can't match properly
                matched_positions.append(MatchedPosition(
                    timestamp_open="",
                    timestamp_close=rug_event.timestamp,
                    target_wallet=rug_event.target,
                    token="unknown",
                    position_type="unknown",
                    sol_deployed=Decimal('0'),
                    sol_received=Decimal('0'),
                    pnl_sol=Decimal('0'),
                    pnl_pct=Decimal('0'),
                    close_reason="rug_unknown_open",
                    mc_at_open=0.0,
                    jup_score=0,
                    token_age="",
                    price_drop_pct=rug_event.price_drop,
                    position_id=""
                ))

        # Unmatched opens = opens whose position_id was never closed
        unmatched_opens = [o for o in self.open_events if o.position_id not in matched_ids]

        return matched_positions, unmatched_opens

    def generate_positions_csv(self, matched_positions: List[MatchedPosition],
                               unmatched_opens: List[OpenEvent],
                               output_path: str) -> None:
        """Generate positions.csv with all matched positions"""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp_open', 'timestamp_close', 'target_wallet', 'token', 'position_type',
                'sol_deployed', 'sol_received', 'pnl_sol', 'pnl_pct', 'close_reason',
                'mc_at_open', 'jup_score', 'token_age', 'price_drop_pct', 'position_id'
            ])

            for pos in matched_positions:
                writer.writerow([
                    pos.timestamp_open,
                    pos.timestamp_close,
                    pos.target_wallet,
                    pos.token,
                    pos.position_type,
                    f"{pos.sol_deployed:.4f}",
                    f"{pos.sol_received:.4f}",
                    f"{pos.pnl_sol:.4f}",
                    f"{pos.pnl_pct:.2f}",
                    pos.close_reason,
                    f"{pos.mc_at_open:.2f}",
                    pos.jup_score,
                    pos.token_age,
                    f"{pos.price_drop_pct:.2f}" if pos.price_drop_pct else "",
                    pos.position_id
                ])

            # Add still-open positions
            for open_event in unmatched_opens:
                writer.writerow([
                    open_event.timestamp,
                    "",  # No close timestamp
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
                    "",  # No price_drop_pct
                    open_event.position_id
                ])

    def generate_summary_csv(self, matched_positions: List[MatchedPosition],
                            output_path: str) -> None:
        """Generate summary.csv with per-target statistics"""
        # Aggregate by target wallet
        target_stats: Dict[str, Dict] = defaultdict(lambda: {
            'total_positions': 0,
            'wins': 0,
            'losses': 0,
            'rugs': 0,
            'total_pnl_sol': Decimal('0'),
            'total_sol_deployed': Decimal('0')
        })

        for pos in matched_positions:
            stats = target_stats[pos.target_wallet]
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
            stats['total_sol_deployed'] += pos.sol_deployed

        # Add skip counts
        skip_counts = defaultdict(int)
        for skip_event in self.skip_events:
            skip_counts[skip_event.target] += 1

        # Write summary
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'target_wallet', 'total_positions', 'wins', 'losses', 'rugs', 'skips',
                'total_pnl_sol', 'avg_pnl_sol', 'win_rate_pct', 'avg_sol_deployed'
            ])

            for target, stats in target_stats.items():
                total_pos = stats['total_positions']
                wins = stats['wins']
                total_pnl = stats['total_pnl_sol']
                total_deployed = stats['total_sol_deployed']

                avg_pnl = total_pnl / total_pos if total_pos > 0 else Decimal('0')
                win_rate = Decimal(wins) / Decimal(total_pos) * Decimal('100') if total_pos > 0 else Decimal('0')
                avg_deployed = total_deployed / total_pos if total_pos > 0 else Decimal('0')

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
                    f"{avg_deployed:.4f}"
                ])


def main():
    parser = argparse.ArgumentParser(
        description='Parse Valhalla Bot Discord DM logs and generate PnL analysis CSV files.'
    )
    parser.add_argument('log_file', help='Path to the log file')
    parser.add_argument('--output-dir', default='.', help='Output directory for CSV files (default: current directory)')

    args = parser.parse_args()

    # Create output directory if needed
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Parse logs
    print(f"Parsing log file: {args.log_file}")
    valhalla = ValhallaParser()
    valhalla.parse_file(args.log_file)

    print(f"\nParsed events:")
    print(f"  Open positions: {len(valhalla.open_events)}")
    print(f"  Close events: {len(valhalla.close_events)}")
    print(f"  Failsafe events: {len(valhalla.failsafe_events)}")
    print(f"  Add liquidity events: {len(valhalla.add_liquidity_events)}")
    print(f"  Rug events: {len(valhalla.rug_events)}")
    print(f"  Skip events: {len(valhalla.skip_events)}")
    print(f"  Swap events: {len(valhalla.swap_events)}")

    # Match positions
    print(f"\nMatching positions...")
    matched_positions, unmatched_opens = valhalla.match_positions()
    print(f"  Matched positions: {len(matched_positions)}")
    print(f"  Still open: {len(unmatched_opens)}")

    # Generate CSVs
    positions_csv = output_dir / 'positions.csv'
    summary_csv = output_dir / 'summary.csv'

    print(f"\nGenerating CSV files...")
    valhalla.generate_positions_csv(matched_positions, unmatched_opens, str(positions_csv))
    valhalla.generate_summary_csv(matched_positions, str(summary_csv))

    print(f"  {positions_csv}")
    print(f"  {summary_csv}")
    print(f"\nDone!")


if __name__ == '__main__':
    main()
