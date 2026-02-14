"""
Position matcher - matches open/close events and enriches with Meteora PnL.
"""

from collections import defaultdict
from decimal import Decimal
from typing import Dict, List, Tuple

from .models import (
    MatchedPosition, MeteoraPnlResult, OpenEvent, AddLiquidityEvent,
    make_iso_datetime, normalize_token_age
)
from .event_parser import EventParser


class PositionMatcher:
    """Match open/close events and enrich with Meteora PnL"""

    def __init__(self, parser: EventParser):
        self.parser = parser

    def match_positions(self, meteora_results: Dict[str, MeteoraPnlResult],
                       resolved_addresses: Dict[str, str],
                       use_discord_pnl: bool = False) -> Tuple[List[MatchedPosition], List[OpenEvent]]:
        """
        Match open events with close/rug events using position_id lookup.
        Returns (matched_positions, unmatched_opens)

        Args:
            meteora_results: Dict of position_id -> MeteoraPnlResult
            resolved_addresses: Dict of position_id -> full address
            use_discord_pnl: If True, use Discord PnL when Meteora unavailable. Otherwise leave as None.
        """
        matched_positions: List[MatchedPosition] = []

        # Index opens by position_id
        open_by_id: Dict[str, OpenEvent] = {}
        for event in self.parser.open_events:
            open_by_id[event.position_id] = event

        # Index failsafe events by position_id
        failsafe_ids = {e.position_id for e in self.parser.failsafe_events}

        # Index add_liquidity events by position_id
        liquidity_by_id: Dict[str, List[AddLiquidityEvent]] = defaultdict(list)
        for event in self.parser.add_liquidity_events:
            liquidity_by_id[event.position_id].append(event)

        # Match closes to opens by position_id
        matched_ids = set()

        for close_event in self.parser.close_events:
            pid = close_event.position_id
            if pid in matched_ids:
                continue  # Skip duplicate close events (e.g., from overlapping log files)
            matched_ids.add(pid)

            if pid in open_by_id:
                open_event = open_by_id[pid]
                sol_deployed = Decimal(str(open_event.your_sol))

                # Add any extra liquidity
                for liq in liquidity_by_id.get(pid, []):
                    sol_deployed += Decimal(str(liq.amount_sol))

                # Discord PnL (fallback)
                sol_received = Decimal(str(close_event.ending_sol)) - Decimal(str(close_event.starting_sol))
                pnl_sol = sol_received - sol_deployed
                pnl_pct = (pnl_sol / sol_deployed * Decimal('100')) if sol_deployed > 0 else Decimal('0')

                close_reason = "failsafe" if pid in failsafe_ids else "normal"

                # Check if we have Meteora data
                full_addr = resolved_addresses.get(pid, "")
                meteora_result = meteora_results.get(pid)

                if meteora_result:
                    # Use Meteora PnL (USD-based, accounts for both token sides)
                    pnl_source = "meteora"
                    meteora_pnl = meteora_result.pnl_sol
                    meteora_pnl_pct = (meteora_pnl / meteora_result.deposited_sol * Decimal('100')) if meteora_result.deposited_sol > 0 else Decimal('0')

                    matched_positions.append(MatchedPosition(
                        timestamp_open=open_event.timestamp,
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token=open_event.token_name,
                        position_type=open_event.position_type,
                        sol_deployed=meteora_result.deposited_sol,
                        sol_received=meteora_result.withdrawn_sol,
                        pnl_sol=meteora_pnl,
                        pnl_pct=meteora_pnl_pct,
                        close_reason=close_reason,
                        mc_at_open=open_event.market_cap,
                        jup_score=open_event.jup_score,
                        token_age=open_event.token_age,
                        token_age_days=normalize_token_age(open_event.token_age)[0],
                        token_age_hours=normalize_token_age(open_event.token_age)[1],
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source=pnl_source,
                        meteora_deposited=meteora_result.deposited_sol,
                        meteora_withdrawn=meteora_result.withdrawn_sol,
                        meteora_fees=meteora_result.fees_sol,
                        meteora_pnl=meteora_pnl,
                        datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
                elif use_discord_pnl:
                    # Use Discord PnL (only if flag enabled)
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
                        token_age_days=normalize_token_age(open_event.token_age)[0],
                        token_age_hours=normalize_token_age(open_event.token_age)[1],
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source="discord",
                        datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
                else:
                    # Meteora not available and Discord PnL not enabled - leave PnL as None
                    matched_positions.append(MatchedPosition(
                        timestamp_open=open_event.timestamp,
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token=open_event.token_name,
                        position_type=open_event.position_type,
                        sol_deployed=None,
                        sol_received=None,
                        pnl_sol=None,
                        pnl_pct=None,
                        close_reason=close_reason,
                        mc_at_open=open_event.market_cap,
                        jup_score=open_event.jup_score,
                        token_age=open_event.token_age,
                        token_age_days=normalize_token_age(open_event.token_age)[0],
                        token_age_hours=normalize_token_age(open_event.token_age)[1],
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source="pending",
                        datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
            else:
                # Close without matching open (pre-existing position)
                # Check if we have Meteora data for this position
                full_addr = resolved_addresses.get(pid, "")
                meteora_result = meteora_results.get(pid)

                if meteora_result:
                    # Use Meteora PnL even for unknown_open (Meteora gives us full position data)
                    meteora_pnl = meteora_result.pnl_sol
                    meteora_pnl_pct = (meteora_pnl / meteora_result.deposited_sol * Decimal('100')) if meteora_result.deposited_sol > 0 else Decimal('0')

                    matched_positions.append(MatchedPosition(
                        timestamp_open="",
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token="unknown",
                        position_type="unknown",
                        sol_deployed=meteora_result.deposited_sol,
                        sol_received=meteora_result.withdrawn_sol,
                        pnl_sol=meteora_pnl,
                        pnl_pct=meteora_pnl_pct,
                        close_reason="unknown_open",
                        mc_at_open=0.0,
                        jup_score=0,
                        token_age="",
                        token_age_days=None,
                        token_age_hours=None,
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source="meteora",
                        meteora_deposited=meteora_result.deposited_sol,
                        meteora_withdrawn=meteora_result.withdrawn_sol,
                        meteora_fees=meteora_result.fees_sol,
                        meteora_pnl=meteora_pnl,
                        datetime_open="",
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
                elif use_discord_pnl:
                    # Use Discord PnL (only if flag enabled)
                    sol_received = Decimal(str(close_event.ending_sol)) - Decimal(str(close_event.starting_sol))

                    matched_positions.append(MatchedPosition(
                        timestamp_open="",
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token="unknown",
                        position_type="unknown",
                        sol_deployed=Decimal('0'),
                        sol_received=sol_received,
                        pnl_sol=sol_received,  # For unknown_open, all received is PnL
                        pnl_pct=Decimal('0'),  # Can't calculate % without deployed
                        close_reason="unknown_open",
                        mc_at_open=0.0,
                        jup_score=0,
                        token_age="",
                        token_age_days=None,
                        token_age_hours=None,
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source="discord",
                        datetime_open="",
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
                else:
                    # No Meteora and Discord PnL not enabled - leave as None
                    matched_positions.append(MatchedPosition(
                        timestamp_open="",
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token="unknown",
                        position_type="unknown",
                        sol_deployed=None,
                        sol_received=None,
                        pnl_sol=None,
                        pnl_pct=None,
                        close_reason="unknown_open",
                        mc_at_open=0.0,
                        jup_score=0,
                        token_age="",
                        token_age_days=None,
                        token_age_hours=None,
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source="pending",
                        datetime_open="",
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))

        # Handle rug events (match by position_id if available)
        for rug_event in self.parser.rug_events:
            if rug_event.position_id:
                pid = rug_event.position_id
                matched_ids.add(pid)

                # If rug event has full address, add to resolved_addresses
                if rug_event.position_address and pid not in resolved_addresses:
                    resolved_addresses[pid] = rug_event.position_address

                if pid in open_by_id:
                    open_event = open_by_id[pid]
                    sol_deployed = Decimal(str(open_event.your_sol))

                    # Add any extra liquidity
                    for liq in liquidity_by_id.get(pid, []):
                        sol_deployed += Decimal(str(liq.amount_sol))

                    full_addr = resolved_addresses.get(pid, "")
                    meteora_result = meteora_results.get(pid)

                    if meteora_result:
                        # Use Meteora PnL even for rug events
                        meteora_pnl = meteora_result.pnl_sol
                        meteora_pnl_pct = (meteora_pnl / meteora_result.deposited_sol * Decimal('100')) if meteora_result.deposited_sol > 0 else Decimal('0')

                        matched_positions.append(MatchedPosition(
                            timestamp_open=open_event.timestamp,
                            timestamp_close=rug_event.timestamp,
                            target_wallet=rug_event.target,
                            token=open_event.token_name,
                            position_type=open_event.position_type,
                            sol_deployed=meteora_result.deposited_sol,
                            sol_received=meteora_result.withdrawn_sol,
                            pnl_sol=meteora_pnl,
                            pnl_pct=meteora_pnl_pct,
                            close_reason="rug",
                            mc_at_open=open_event.market_cap,
                            jup_score=open_event.jup_score,
                            token_age=open_event.token_age,
                            token_age_days=normalize_token_age(open_event.token_age)[0],
                            token_age_hours=normalize_token_age(open_event.token_age)[1],
                            price_drop_pct=rug_event.price_drop,
                            position_id=pid,
                            full_address=full_addr,
                            pnl_source="meteora",
                            meteora_deposited=meteora_result.deposited_sol,
                            meteora_withdrawn=meteora_result.withdrawn_sol,
                            meteora_fees=meteora_result.fees_sol,
                            meteora_pnl=meteora_pnl,
                            datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                            datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                        ))
                    elif use_discord_pnl:
                        # Use Discord PnL estimate (price drop based)
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
                            token_age_days=normalize_token_age(open_event.token_age)[0],
                            token_age_hours=normalize_token_age(open_event.token_age)[1],
                            price_drop_pct=rug_event.price_drop,
                            position_id=pid,
                            full_address=full_addr,
                            pnl_source="discord",
                            datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                            datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                        ))
                    else:
                        # No Meteora and Discord PnL not enabled - leave as None
                        matched_positions.append(MatchedPosition(
                            timestamp_open=open_event.timestamp,
                            timestamp_close=rug_event.timestamp,
                            target_wallet=rug_event.target,
                            token=open_event.token_name,
                            position_type=open_event.position_type,
                            sol_deployed=None,
                            sol_received=None,
                            pnl_sol=None,
                            pnl_pct=None,
                            close_reason="rug",
                            mc_at_open=open_event.market_cap,
                            jup_score=open_event.jup_score,
                            token_age=open_event.token_age,
                            token_age_days=normalize_token_age(open_event.token_age)[0],
                            token_age_hours=normalize_token_age(open_event.token_age)[1],
                            price_drop_pct=rug_event.price_drop,
                            position_id=pid,
                            full_address=full_addr,
                            pnl_source="pending",
                            datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                            datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                        ))
                else:
                    # Rug event without matching open
                    full_addr = resolved_addresses.get(pid, "")
                    meteora_result = meteora_results.get(pid)

                    if meteora_result:
                        # Use Meteora PnL
                        meteora_pnl = meteora_result.pnl_sol
                        meteora_pnl_pct = (meteora_pnl / meteora_result.deposited_sol * Decimal('100')) if meteora_result.deposited_sol > 0 else Decimal('0')

                        matched_positions.append(MatchedPosition(
                            timestamp_open="",
                            timestamp_close=rug_event.timestamp,
                            target_wallet=rug_event.target,
                            token="unknown",
                            position_type="unknown",
                            sol_deployed=meteora_result.deposited_sol,
                            sol_received=meteora_result.withdrawn_sol,
                            pnl_sol=meteora_pnl,
                            pnl_pct=meteora_pnl_pct,
                            close_reason="rug_unknown_open",
                            mc_at_open=0.0,
                            jup_score=0,
                            token_age="",
                            token_age_days=None,
                            token_age_hours=None,
                            price_drop_pct=rug_event.price_drop,
                            position_id=pid,
                            full_address=full_addr,
                            pnl_source="meteora",
                            meteora_deposited=meteora_result.deposited_sol,
                            meteora_withdrawn=meteora_result.withdrawn_sol,
                            meteora_fees=meteora_result.fees_sol,
                            meteora_pnl=meteora_pnl,
                            datetime_open="",
                            datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                        ))
                    elif use_discord_pnl:
                        # Use Discord PnL (can't estimate without deployed amount)
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
                            token_age_days=None,
                            token_age_hours=None,
                            price_drop_pct=rug_event.price_drop,
                            position_id=pid,
                            full_address=full_addr,
                            pnl_source="discord",
                            datetime_open="",
                            datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                        ))
                    else:
                        # No Meteora and Discord PnL not enabled
                        matched_positions.append(MatchedPosition(
                            timestamp_open="",
                            timestamp_close=rug_event.timestamp,
                            target_wallet=rug_event.target,
                            token="unknown",
                            position_type="unknown",
                            sol_deployed=None,
                            sol_received=None,
                            pnl_sol=None,
                            pnl_pct=None,
                            close_reason="rug_unknown_open",
                            mc_at_open=0.0,
                            jup_score=0,
                            token_age="",
                            token_age_days=None,
                            token_age_hours=None,
                            price_drop_pct=rug_event.price_drop,
                            position_id=pid,
                            full_address=full_addr,
                            pnl_source="pending",
                            datetime_open="",
                            datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                        ))
            else:
                # Rug event without position_id - can't match or get Meteora data
                if use_discord_pnl:
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
                        token_age_days=None,
                        token_age_hours=None,
                        price_drop_pct=rug_event.price_drop,
                        position_id="",
                        pnl_source="discord",
                        datetime_open="",
                        datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                    ))
                else:
                    matched_positions.append(MatchedPosition(
                        timestamp_open="",
                        timestamp_close=rug_event.timestamp,
                        target_wallet=rug_event.target,
                        token="unknown",
                        position_type="unknown",
                        sol_deployed=None,
                        sol_received=None,
                        pnl_sol=None,
                        pnl_pct=None,
                        close_reason="rug_unknown_open",
                        mc_at_open=0.0,
                        jup_score=0,
                        token_age="",
                        token_age_days=None,
                        token_age_hours=None,
                        price_drop_pct=rug_event.price_drop,
                        position_id="",
                        pnl_source="pending",
                        datetime_open="",
                        datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                    ))

        # Unmatched opens = opens whose position_id was never closed
        unmatched_opens = [o for o in self.parser.open_events if o.position_id not in matched_ids]

        return matched_positions, unmatched_opens
