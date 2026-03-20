"""
Source wallet analyzer for Phase C.

For each position with a target_tx_signature, resolves the source wallet's
DLMM position address and fetches its PnL via the Meteora API. Classifies the
scenario to explain how the bot performed compared to the source wallet.
"""

import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from .analysis_config import SOURCE_WALLET_MIN_LOSS_PCT
from .meteora import MeteoraPnlCalculator
from .models import KNOWN_PROGRAMS, MatchedPosition, parse_iso_datetime
from .solana_rpc import AddressCache, SolanaRpcClient


# ---------------------------------------------------------------------------
# Scenario constants
# ---------------------------------------------------------------------------

SCENARIO_HELD_LONGER = "held_longer"
SCENARIO_EXITED_FIRST = "exited_first"
SCENARIO_BOTH_LOSS = "both_loss"
SCENARIO_UNKNOWN = "unknown"

# New scenario constants
SCENARIO_NO_DATA = "no_data"                     # actual data missing (API error, no tx, 404)
SCENARIO_SOURCE_HELD_LONGER = "source_held_longer"   # source won, closed AFTER bot → SL too tight
SCENARIO_SOURCE_EXITED_EARLY = "source_exited_early" # source won, closed BEFORE bot → copy lag
SCENARIO_SOURCE_RECOVERED = "source_recovered"       # source won, timing unknown → actionable
SCENARIO_COMPARABLE = "comparable"                   # both similar outcome or both positive → no clear lesson

# Aliases for backward compatibility (old values still work in existing CSVs)
# SCENARIO_HELD_LONGER and SCENARIO_EXITED_FIRST retained above as legacy aliases


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class SourceWalletResult:
    """Result of analyzing a single source wallet position."""
    position_id: str
    source_position_address: Optional[str]
    source_open_time: Optional[datetime]
    source_close_time: Optional[datetime]
    source_hold_min: Optional[int]       # hold duration in minutes
    source_pnl_pct: Optional[Decimal]
    source_pnl_sol: Optional[Decimal]
    scenario: str                         # SCENARIO_* constant
    error: Optional[str]                  # None if successful


# ---------------------------------------------------------------------------
# Analyzer class
# ---------------------------------------------------------------------------

class SourceWalletAnalyzer:
    """
    Analyze source wallet positions for all positions that have a
    target_tx_signature populated (Phase A data required).
    """

    def __init__(self, rpc_client: SolanaRpcClient, cache: AddressCache):
        self.rpc_client = rpc_client
        self.cache = cache
        self.meteora = MeteoraPnlCalculator()

    def analyze_position(self, position: MatchedPosition) -> SourceWalletResult:
        """
        Analyze source wallet for a single loss position.

        Returns SourceWalletResult with error field set on failure.
        All failures log a warning and return scenario=SCENARIO_UNKNOWN.
        """
        pid = position.position_id
        sig = position.target_tx_signature

        if not sig:
            return SourceWalletResult(
                position_id=pid,
                source_position_address=None,
                source_open_time=None,
                source_close_time=None,
                source_hold_min=None,
                source_pnl_pct=None,
                source_pnl_sol=None,
                scenario=SCENARIO_NO_DATA,
                error="no target_tx_signature"
            )

        # Step 1: Resolve target tx to DLMM position address
        # Exclude the source wallet address so we don't return the signer as a candidate
        exclude = set()
        if position.target_wallet_address:
            exclude.add(position.target_wallet_address)
        try:
            source_addr = self._resolve_target_position_address(sig, exclude_addresses=exclude)
        except Exception as e:
            print(f"  Warning: source wallet RPC resolution failed for {pid}: {e}")
            return SourceWalletResult(
                position_id=pid,
                source_position_address=None,
                source_open_time=None,
                source_close_time=None,
                source_hold_min=None,
                source_pnl_pct=None,
                source_pnl_sol=None,
                scenario=SCENARIO_NO_DATA,
                error=str(e)
            )

        if not source_addr:
            print(f"  Warning: could not resolve source position address for {pid} (tx: {sig[:16]}...)")
            return SourceWalletResult(
                position_id=pid,
                source_position_address=None,
                source_open_time=None,
                source_close_time=None,
                source_hold_min=None,
                source_pnl_pct=None,
                source_pnl_sol=None,
                scenario=SCENARIO_NO_DATA,
                error="could not resolve source position address from tx"
            )

        # Step 2: Fetch PnL from Meteora API
        time.sleep(0.3)
        try:
            pnl_result = self.meteora.calculate_pnl(source_addr)
        except Exception as e:
            print(f"  Warning: Meteora PnL fetch failed for {pid} ({source_addr[:8]}...): {e}")
            return SourceWalletResult(
                position_id=pid,
                source_position_address=source_addr,
                source_open_time=None,
                source_close_time=None,
                source_hold_min=None,
                source_pnl_pct=None,
                source_pnl_sol=None,
                scenario=SCENARIO_NO_DATA,
                error=str(e)
            )

        if not pnl_result:
            print(f"  Meteora returned no data for {source_addr[:8]}..., skipping")
            return SourceWalletResult(
                position_id=pid,
                source_position_address=source_addr,
                source_open_time=None,
                source_close_time=None,
                source_hold_min=None,
                source_pnl_pct=None,
                source_pnl_sol=None,
                scenario=SCENARIO_NO_DATA,
                error="Meteora returned no PnL data"
            )

        # Step 3: Compute source PnL%
        source_pnl_sol = pnl_result.pnl_sol
        source_pnl_pct: Optional[Decimal] = None
        if pnl_result.deposited_sol and pnl_result.deposited_sol > 0:
            source_pnl_pct = (source_pnl_sol / pnl_result.deposited_sol) * Decimal('100')

        # Step 4: Get timestamps from Meteora API
        source_open_time: Optional[datetime] = None
        source_close_time: Optional[datetime] = None
        source_hold_min: Optional[int] = None
        try:
            time.sleep(0.3)
            ts_result = self.meteora.get_position_timestamps(source_addr)
            if ts_result:
                source_open_time, source_close_time = ts_result
                hold_seconds = (source_close_time - source_open_time).total_seconds()
                source_hold_min = max(0, int(hold_seconds / 60))
        except Exception as e:
            print(f"  Warning: could not fetch timestamps for {pid}: {e}")
            # Non-fatal — continue without timestamps

        # Step 5: Classify scenario
        scenario = self._classify_scenario(
            source_pnl_pct=source_pnl_pct,
            bot_pnl_pct=position.pnl_pct,
            source_close_time=source_close_time,
            bot_close_time_str=position.datetime_close
        )

        return SourceWalletResult(
            position_id=pid,
            source_position_address=source_addr,
            source_open_time=source_open_time,
            source_close_time=source_close_time,
            source_hold_min=source_hold_min,
            source_pnl_pct=source_pnl_pct,
            source_pnl_sol=source_pnl_sol,
            scenario=scenario,
            error=None
        )

    def analyze_batch(
        self,
        positions: List[MatchedPosition],
        max_positions: Optional[int] = None
    ) -> List[SourceWalletResult]:
        """
        Run analyze_position() for all eligible positions.

        Eligible means:
        - has target_tx_signature
        - pnl_pct <= SOURCE_WALLET_MIN_LOSS_PCT (filters by actual loss, not close_reason)
        - source_wallet_scenario not already set (idempotent)

        Prints progress. Returns list of results (only eligible positions).
        """
        threshold = Decimal(str(SOURCE_WALLET_MIN_LOSS_PCT)) if SOURCE_WALLET_MIN_LOSS_PCT is not None else None

        eligible = [
            p for p in positions
            if p.target_tx_signature
            and not p.source_wallet_scenario
            and (threshold is None or (p.pnl_pct is not None and p.pnl_pct <= threshold))
        ]

        if max_positions is not None:
            eligible = eligible[:max_positions]

        total = len(eligible)
        if total == 0:
            print("  No eligible positions for source wallet analysis.")
            return []

        print(f"  Found {total} eligible position(s) for source wallet analysis.")
        results: List[SourceWalletResult] = []

        for i, pos in enumerate(eligible, 1):
            print(f"  Analyzing {i}/{total}: {pos.position_id}...", end='', flush=True)
            result = self.analyze_position(pos)
            results.append(result)
            if result.error:
                print(f" ERROR: {result.error}")
            else:
                pnl_str = f"{result.source_pnl_pct:.2f}%" if result.source_pnl_pct is not None else "N/A"
                hold_str = f"{result.source_hold_min}min" if result.source_hold_min is not None else "N/A"
                print(f" OK (PnL: {pnl_str}, hold: {hold_str}, scenario: {result.scenario})")

        return results

    # -----------------------------------------------------------------------
    # Private helpers
    # -----------------------------------------------------------------------

    def _resolve_target_position_address(
        self, tx_signature: str, exclude_addresses: Optional[set] = None
    ) -> Optional[str]:
        """
        Resolve a target tx signature to its DLMM position address.

        Fetches the transaction, filters out known system programs, the signer
        wallet (if provided in exclude_addresses), and very short addresses.
        Returns the first candidate that looks like a position account.

        Args:
            tx_signature: The transaction signature to inspect.
            exclude_addresses: Additional addresses to exclude (e.g. the source
                               wallet address, which is the tx fee payer/signer).
        """
        account_keys = self.rpc_client.get_transaction(tx_signature)
        if not account_keys:
            return None

        excluded = set(KNOWN_PROGRAMS)
        if exclude_addresses:
            excluded.update(exclude_addresses)

        # Always exclude the first account key (fee payer / signer = source wallet)
        # This is critical when target_wallet_address is not available
        if account_keys:
            excluded.add(account_keys[0])

        # Filter out known programs, excluded addresses, and short keys
        candidates = [
            key for key in account_keys
            if key not in excluded and len(key) >= 32
        ]

        if not candidates:
            return None

        # Return the first candidate — in DLMM transactions the position account
        # is typically one of the first non-program, non-signer accounts listed
        return candidates[0]

    @staticmethod
    def _classify_scenario(
        source_pnl_pct: Optional[Decimal],
        bot_pnl_pct: Optional[Decimal],
        source_close_time: Optional[datetime],
        bot_close_time_str: str
    ) -> str:
        """
        Classify the scenario based on source vs bot PnL and timing.

        Rules:
        - If data missing: no_data
        - If both sides lost: both_loss (bad luck, no actionable lesson)
        - If source escaped (positive) and bot lost: check timing
            - source closed AFTER bot: source_held_longer (SL too tight)
            - source closed BEFORE bot: source_exited_early (copy lag)
            - timing unknown: source_recovered (actionable, mechanism unclear)
        - Otherwise (both positive, or bot won more): comparable (no clear lesson)
        """
        if source_pnl_pct is None or bot_pnl_pct is None:
            return SCENARIO_NO_DATA

        ZERO = Decimal('0')

        # Both sides lost → bad luck, no actionable lesson
        if source_pnl_pct <= ZERO and bot_pnl_pct <= ZERO:
            return SCENARIO_BOTH_LOSS

        # Source escaped (positive), bot lost (non-positive) → actionable
        if source_pnl_pct > ZERO and bot_pnl_pct <= ZERO:
            bot_close_time = parse_iso_datetime(bot_close_time_str)
            if source_close_time and bot_close_time:
                if source_close_time > bot_close_time:
                    return SCENARIO_SOURCE_HELD_LONGER
                else:
                    return SCENARIO_SOURCE_EXITED_EARLY
            return SCENARIO_SOURCE_RECOVERED

        # Both positive, or bot won more, or source marginally better → no clear lesson
        return SCENARIO_COMPARABLE
