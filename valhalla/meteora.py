"""
Meteora DLMM API client for PnL calculation.
"""

import json
import time
import urllib.request
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional, Tuple

from .models import MeteoraPnlResult, SOL_MINT, short_id


class MeteoraPnlCalculator:
    """Calculate PnL using Meteora DLMM API"""

    def __init__(self):
        self.base_url = "https://dlmm.datapi.meteora.ag"
        self._pair_cache: Dict[str, Tuple[bool, bool, str]] = {}  # pair_addr -> (sol_is_x, sol_is_y, token_mint)
        self._events_cache: Dict[str, list] = {}  # position_addr -> events list

    def _get_sol_side(self, pair_address: str) -> Optional[Tuple[bool, bool]]:
        """Determine which token (x or y) is SOL for a pair. Returns (sol_is_x, sol_is_y)."""
        if pair_address in self._pair_cache:
            sol_is_x, sol_is_y, _ = self._pair_cache[pair_address]
            return (sol_is_x, sol_is_y)

        pair_info = self._meteora_get(f"/pools/{pair_address}")
        if not pair_info:
            return None

        mint_x = pair_info.get('token_x', {}).get('address', '')
        mint_y = pair_info.get('token_y', {}).get('address', '')
        sol_is_x = (mint_x == SOL_MINT)
        sol_is_y = (mint_y == SOL_MINT)

        if not (sol_is_x or sol_is_y):
            return None

        # Cache includes token mint (non-SOL side)
        token_mint = mint_y if sol_is_x else mint_x
        self._pair_cache[pair_address] = (sol_is_x, sol_is_y, token_mint)
        return (sol_is_x, sol_is_y)

    def _get_token_mint(self, pair_address: str) -> Optional[str]:
        """Return the non-SOL token mint for a pair, or None if not cached."""
        cached = self._pair_cache.get(pair_address)
        if cached:
            return cached[2]
        return None

    def calculate_pnl(self, address: str) -> Optional[MeteoraPnlResult]:
        """
        Calculate PnL for a position address.
        Returns MeteoraPnlResult or None if failed.
        """
        try:
            # Fetch all events in one call — replaces separate deposits/withdraws/claim_fees calls
            historical = self._meteora_get(f"/positions/{address}/historical")
            if not historical:
                return None

            all_events = historical.get('events', [])
            if not all_events:
                print(f"  Warning: No events for {short_id(address)}")
                return None

            # Cache events so get_position_timestamps() can reuse them
            self._events_cache[address] = all_events

            # Derive pool address from first event (replaces dead /position/{addr} call)
            pair_address = all_events[0].get('poolAddress', '')
            if not pair_address:
                print(f"  Warning: No poolAddress in events for {short_id(address)}")
                return None

            time.sleep(0.3)

            # Get mint info from pool to determine which token is SOL
            sol_side = self._get_sol_side(pair_address)
            if not sol_side:
                print(f"  Warning: No SOL token found in pair for {short_id(address)}")
                return None

            sol_is_x, sol_is_y = sol_side

            time.sleep(0.3)

            # Split events by type — new API returns all events in one array.
            # Ignore claim_reward events.
            deposits = [e for e in all_events if e.get('eventType') == 'add']
            withdraws = [e for e in all_events if e.get('eventType') == 'remove']
            fees_list = [e for e in all_events if e.get('eventType') == 'claim_fee']

            # Helper: compute SOL equivalent for a transaction entry.
            # Converts the non-SOL token to SOL using the per-transaction SOL price
            # derived from the SOL-side USD amount.
            # NOTE: new API returns amounts as decimal strings (e.g. "3.1100433"),
            # NOT lamports — no division by LAMPORTS needed.
            def _tx_sol_equiv(
                entry,
                fallback_sol_price: Decimal,
            ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
                """Returns (sol_amount, sol_equiv_total, total_usd, sol_price_used)"""
                sol_key = 'amountX' if sol_is_x else 'amountY'
                sol_usd_key = 'amountXUsd' if sol_is_x else 'amountYUsd'
                tok_usd_key = 'amountYUsd' if sol_is_x else 'amountXUsd'

                sol_amt = Decimal(str(entry.get(sol_key, 0)))
                sol_usd = Decimal(str(entry.get(sol_usd_key, 0)))
                tok_usd = Decimal(str(entry.get(tok_usd_key, 0)))

                # Derive per-tx SOL price from SOL portion
                if sol_amt > 0 and sol_usd > 0:
                    sol_price = sol_usd / sol_amt
                else:
                    sol_price = fallback_sol_price

                # Convert token side to SOL at this tx's SOL price
                if tok_usd > 0 and sol_price > 0:
                    token_sol_equiv = tok_usd / sol_price
                else:
                    token_sol_equiv = Decimal('0')

                total_sol_equiv = sol_amt + token_sol_equiv
                total_usd = sol_usd + tok_usd
                return sol_amt, total_sol_equiv, total_usd, sol_price

            # Pre-scan ALL transactions to find an initial SOL price.
            # This prevents token-only deposits from being valued at 0
            # when they appear before any SOL-bearing transaction.
            sol_key = 'amountX' if sol_is_x else 'amountY'
            sol_usd_key = 'amountXUsd' if sol_is_x else 'amountYUsd'
            initial_sol_price = Decimal('0')
            for entry in (deposits or []) + (withdraws or []) + (fees_list or []):
                amt = Decimal(str(entry.get(sol_key, 0)))
                usd = Decimal(str(entry.get(sol_usd_key, 0)))
                if amt > 0 and usd > 0:
                    initial_sol_price = usd / amt
                    break  # use the first available SOL price

            running_sol_price = initial_sol_price

            # Process deposits
            dep_sol_equiv = Decimal('0')
            dep_usd = Decimal('0')
            for dep in deposits:
                _, equiv, usd, price = _tx_sol_equiv(dep, running_sol_price)
                dep_sol_equiv += equiv
                dep_usd += usd
                if price > 0:
                    running_sol_price = price

            # Process withdrawals
            wdr_sol_equiv = Decimal('0')
            wdr_usd = Decimal('0')
            for w in withdraws:
                _, equiv, usd, price = _tx_sol_equiv(w, running_sol_price)
                wdr_sol_equiv += equiv
                wdr_usd += usd
                if price > 0:
                    running_sol_price = price

            # Process claimed fees
            fee_sol_equiv = Decimal('0')
            fee_usd = Decimal('0')
            for f_entry in fees_list:
                _, equiv, usd, price = _tx_sol_equiv(f_entry, running_sol_price)
                fee_sol_equiv += equiv
                fee_usd += usd
                if price > 0:
                    running_sol_price = price

            # PnL via per-transaction SOL equivalents
            pnl_sol = wdr_sol_equiv + fee_sol_equiv - dep_sol_equiv
            pnl_usd = wdr_usd + fee_usd - dep_usd

            return MeteoraPnlResult(
                deposited_sol=dep_sol_equiv,
                withdrawn_sol=wdr_sol_equiv,
                fees_sol=fee_sol_equiv,
                deposited_usd=dep_usd,
                withdrawn_usd=wdr_usd,
                fees_usd=fee_usd,
                pnl_usd=pnl_usd,
                pnl_sol=pnl_sol
            )

        except Exception as e:
            print(f"  Meteora API error for {short_id(address)}: {e}")
            return None

    def get_position_timestamps(
        self, address: str, events: Optional[list] = None
    ) -> Optional[Tuple[datetime, datetime]]:
        """
        Return (open_time, close_time) from position transaction history.

        If `events` is provided, uses them directly (avoids duplicate API call).
        Otherwise fetches /positions/{address}/historical.
        blockTime is in milliseconds — divide by 1000.
        Returns None if not available.
        """
        if events is None:
            # Check cache first (populated by calculate_pnl)
            events = self._events_cache.pop(address, None)
        if events is None:
            historical = self._meteora_get(f"/positions/{address}/historical")
            if not historical:
                return None
            events = historical.get('events', [])
        if not events:
            return None
        timestamps = [
            e.get('blockTime')
            for e in events
            if e.get('blockTime')
        ]
        if not timestamps:
            return None
        # blockTime is milliseconds in new API — convert to seconds for fromtimestamp()
        open_ts = datetime.fromtimestamp(min(timestamps) / 1000)
        close_ts = datetime.fromtimestamp(max(timestamps) / 1000)
        return open_ts, close_ts

    def _meteora_get(self, path: str):
        """Make GET request to Meteora API"""
        try:
            url = f"{self.base_url}{path}"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            })

            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())

        except Exception as e:
            print(f"  Meteora GET error ({path}): {e}")
            return None
