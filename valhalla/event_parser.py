"""
Event parser for Discord bot messages.
"""

import re
from typing import List, Tuple, Optional
from datetime import datetime, timedelta

from .models import (
    OpenEvent, CloseEvent, RugEvent, SkipEvent, FailsafeEvent,
    AddLiquidityEvent, SwapEvent, InsufficientBalanceEvent, short_id
)


class EventParser:
    """Parse events from Discord message text"""

    # Regex patterns (from v1)
    TIMESTAMP_PATTERN = r'\[((?:\d{4}-\d{2}-\d{2}T)?\d{2}:\d{2})\]'
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
    RUG_POSITION_ID_PATTERN = r'Rug Check Stop Loss Executed\s*\(DLMM\)\s*\((\w+)\)'
    PRICE_DROP_PATTERN = r'Price Drop:\s*([\d.]+)%'
    RUG_THRESHOLD_PATTERN = r'Rug Check Threshold:\s*([\d.]+)%'
    POSITION_ADDRESS_PATTERN = r'Position:\s*(\S+)'
    PAIR_PATTERN = r'Pair:\s*(\S+)'

    # Skip event markers
    SKIP_REASON_AGE_MARKER = 'Skipping position due to token age restriction'
    SKIP_REASON_JUP_MARKER = 'Skipping position due to low Jupiter organic score restriction'
    SKIP_REASON_SOL_ONLY_MARKER = 'Skipping position due to SOL-only deposit restriction'
    SKIP_TOKEN_PATTERN = r'Token\s+([^:]+?):\s*(\S+)'

    # Swap pattern
    SWAP_PATTERN = r'Swapped\s+([\d,]+|all)\s+(.+?)\s+\((\S+)\)'

    # Insufficient balance patterns
    INSUF_TARGET_PATTERN = r'Trade copied from:\s*(\S+)'
    INSUF_SOL_BALANCE_PATTERN = r'Your SOL balance:\s*([\d.]+)\s*SOL'
    INSUF_EFFECTIVE_PATTERN = r'Total effective balance:\s*([\d.]+)\s*SOL'
    INSUF_REQUIRED_PATTERN = r'Required amount for this trade:\s*([\d.]+)\s*SOL'

    def __init__(self, base_date: Optional[str] = None):
        """
        Initialize EventParser.

        Args:
            base_date: Starting date in YYYY-MM-DD format, or None
        """
        self.open_events: List[OpenEvent] = []
        self.close_events: List[CloseEvent] = []
        self.rug_events: List[RugEvent] = []
        self.skip_events: List[SkipEvent] = []
        self.swap_events: List[SwapEvent] = []
        self.failsafe_events: List[FailsafeEvent] = []
        self.add_liquidity_events: List[AddLiquidityEvent] = []
        self.insufficient_balance_events: List[InsufficientBalanceEvent] = []
        self.base_date = base_date
        self.current_date = base_date

    def parse_messages(self, messages: List[Tuple[str, str, List[str]]]) -> None:
        """Parse all messages from PlainTextReader with midnight rollover detection"""
        prev_hour = None

        for timestamp, text, tx_signatures in messages:
            # No need to strip non-ASCII - patterns match ASCII keywords only
            clean_text = text

            # Check if timestamp contains a full date [YYYY-MM-DDTHH:MM]
            full_dt_match = re.search(r'\[(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})\]', timestamp)
            if full_dt_match:
                # Date is embedded in timestamp — use it directly
                self.current_date = full_dt_match.group(1)
            elif self.base_date:
                # Old [HH:MM] format — use midnight rollover detection
                time_match = re.search(r'\[(\d{2}):(\d{2})\]', timestamp)
                if time_match:
                    hour = int(time_match.group(1))

                    # If time drops significantly (e.g., 23:50 -> 00:10), we crossed midnight
                    # Require >6h drop to avoid false triggers from out-of-order messages
                    if prev_hour is not None and (prev_hour - hour) > 6:
                        # Increment the current date by 1 day
                        current_dt = datetime.strptime(self.current_date, "%Y-%m-%d")
                        next_dt = current_dt + timedelta(days=1)
                        self.current_date = next_dt.strftime("%Y-%m-%d")
                        print(f"  Midnight rollover detected: now using {self.current_date}")

                    prev_hour = hour

            self._classify_and_parse_message(timestamp, clean_text, tx_signatures)

    def _classify_and_parse_message(self, timestamp: str, message: str, tx_signatures: List[str]) -> None:
        """Classify message type and parse accordingly"""
        # Skip "already closed" messages
        if "was already closed" in message:
            return

        # Check for each event type
        if "Opened New DLMM Position!" in message:
            event = self._parse_open_event(timestamp, message, tx_signatures)
            if event:
                event.date = self.current_date or ""
                self.open_events.append(event)

        elif "Closed DLMM Position!" in message:
            event = self._parse_close_event(timestamp, message, tx_signatures)
            if event:
                event.date = self.current_date or ""
                self.close_events.append(event)

        elif "Failsafe Activated (DLMM)" in message:
            event = self._parse_failsafe_event(timestamp, message, tx_signatures)
            if event:
                event.date = self.current_date or ""
                self.failsafe_events.append(event)

        elif "Added DLMM Liquidity" in message:
            event = self._parse_add_liquidity_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.add_liquidity_events.append(event)

        elif "Rug Check Stop Loss Executed" in message:
            event = self._parse_rug_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.rug_events.append(event)

        elif "Skipping position due to" in message:
            event = self._parse_skip_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.skip_events.append(event)

        elif "Insufficient Effective Balance" in message:
            event = self._parse_insufficient_balance_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.insufficient_balance_events.append(event)

        elif "Swapped" in message:
            event = self._parse_swap_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.swap_events.append(event)

    def _parse_open_event(self, timestamp: str, message: str, tx_signatures: List[str]) -> Optional[OpenEvent]:
        """Parse an open position event"""
        try:
            target_match = re.search(self.TARGET_PATTERN, message)
            position_type_match = re.search(self.POSITION_TYPE_PATTERN, message)
            mc_match = re.search(self.MARKET_CAP_PATTERN, message)
            age_match = re.search(self.TOKEN_AGE_PATTERN, message)
            jup_match = re.search(self.JUP_SCORE_PATTERN, message)
            your_sol_match = re.search(self.YOUR_POS_PATTERN, message)
            target_sol_match = re.search(self.TARGET_POS_PATTERN, message)
            position_id_match = re.search(self.OPEN_POSITION_ID_PATTERN, message)

            if not all([target_match, position_type_match, mc_match, age_match,
                       jup_match, your_sol_match, target_sol_match, position_id_match]):
                return None

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
                position_id=position_id,
                tx_signatures=tx_signatures
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse open event: {e}")
            return None

    def _parse_close_event(self, timestamp: str, message: str, tx_signatures: List[str]) -> Optional[CloseEvent]:
        """Parse a close position event"""
        try:
            target_match = re.search(self.TARGET_PATTERN, message)
            starting_match = re.search(self.STARTING_SOL_PATTERN, message)
            ending_match = re.search(self.ENDING_SOL_PATTERN, message)
            total_match = re.search(self.TOTAL_SOL_PATTERN, message)
            position_id_match = re.search(self.CLOSE_POSITION_ID_PATTERN, message)

            # total_match is optional (some close events don't have it)
            if not all([target_match, starting_match, ending_match, position_id_match]):
                return None

            target = target_match.group(1)
            starting_sol = float(starting_match.group(1))
            starting_usd = float(starting_match.group(2).replace(',', ''))
            ending_sol = float(ending_match.group(1))
            ending_usd = float(ending_match.group(2).replace(',', ''))
            total_sol = float(total_match.group(1)) if total_match else 0.0
            active_positions = int(total_match.group(2)) if total_match else 0
            position_id = position_id_match.group(1)

            return CloseEvent(
                timestamp=timestamp,
                target=target,
                starting_sol=starting_sol,
                starting_usd=starting_usd,
                ending_sol=ending_sol,
                ending_usd=ending_usd,
                position_id=position_id,
                tx_signatures=tx_signatures,
                total_sol=total_sol,
                active_positions=active_positions
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse close event: {e}")
            return None

    def _parse_failsafe_event(self, timestamp: str, message: str, tx_signatures: List[str]) -> Optional[FailsafeEvent]:
        """Parse a failsafe activation event"""
        try:
            position_id_match = re.search(self.FAILSAFE_POSITION_ID_PATTERN, message)

            if not position_id_match:
                return None

            position_id = position_id_match.group(1)

            return FailsafeEvent(
                timestamp=timestamp,
                position_id=position_id,
                tx_signatures=tx_signatures
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse failsafe event: {e}")
            return None

    def _parse_add_liquidity_event(self, timestamp: str, message: str) -> Optional[AddLiquidityEvent]:
        """Parse an add liquidity event"""
        try:
            position_id_match = re.search(self.ADD_LIQUIDITY_POSITION_ID_PATTERN, message)
            target_match = re.search(self.TARGET_PATTERN, message)
            amount_match = re.search(self.LIQUIDITY_AMOUNT_PATTERN, message)

            if not all([position_id_match, target_match, amount_match]):
                return None

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

    def _parse_rug_event(self, timestamp: str, message: str) -> Optional[RugEvent]:
        """Parse a rug pull event"""
        try:
            target_match = re.search(self.RUG_TARGET_PATTERN, message)
            pair_match = re.search(self.PAIR_PATTERN, message)
            position_match = re.search(self.POSITION_ADDRESS_PATTERN, message)
            drop_match = re.search(self.PRICE_DROP_PATTERN, message)
            threshold_match = re.search(self.RUG_THRESHOLD_PATTERN, message)
            position_id_match = re.search(self.RUG_POSITION_ID_PATTERN, message)

            if not all([target_match, pair_match, position_match, drop_match, threshold_match]):
                return None

            target = target_match.group(1)
            token_pair = pair_match.group(1)
            position_address = position_match.group(1)
            price_drop = float(drop_match.group(1))
            threshold = float(threshold_match.group(1))
            position_id = position_id_match.group(1) if position_id_match else None

            # Sanity check: verify position_id matches short_id of position_address
            if position_id and position_address:
                expected_short_id = short_id(position_address)
                if position_id != expected_short_id:
                    print(f"Warning: Rug event position_id mismatch: header={position_id}, address short_id={expected_short_id}")

            return RugEvent(
                timestamp=timestamp,
                target=target,
                token_pair=token_pair,
                position_address=position_address,
                price_drop=price_drop,
                threshold=threshold,
                position_id=position_id
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse rug event: {e}")
            return None

    def _parse_skip_event(self, timestamp: str, message: str) -> Optional[SkipEvent]:
        """Parse a skip event"""
        try:
            target_match = re.search(self.TARGET_PATTERN, message)

            if not target_match:
                return None

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

    def _parse_insufficient_balance_event(self, timestamp: str, message: str) -> Optional[InsufficientBalanceEvent]:
        """Parse an insufficient balance event"""
        try:
            target_match = re.search(self.INSUF_TARGET_PATTERN, message)
            sol_match = re.search(self.INSUF_SOL_BALANCE_PATTERN, message)
            effective_match = re.search(self.INSUF_EFFECTIVE_PATTERN, message)
            required_match = re.search(self.INSUF_REQUIRED_PATTERN, message)

            if not all([target_match, sol_match, effective_match, required_match]):
                return None

            return InsufficientBalanceEvent(
                timestamp=timestamp,
                target=target_match.group(1),
                sol_balance=float(sol_match.group(1)),
                effective_balance=float(effective_match.group(1)),
                required_amount=float(required_match.group(1))
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse insufficient balance event: {e}")
            return None

    def _parse_swap_event(self, timestamp: str, message: str) -> Optional[SwapEvent]:
        """Parse a swap event"""
        try:
            swap_match = re.search(self.SWAP_PATTERN, message)

            if not swap_match:
                return None

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
