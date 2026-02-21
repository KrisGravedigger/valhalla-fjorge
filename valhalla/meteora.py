"""
Meteora DLMM API client for PnL calculation.
"""

import json
import time
import urllib.request
from decimal import Decimal
from typing import Dict, Optional, Tuple

from .models import MeteoraPnlResult, SOL_MINT, short_id


class MeteoraPnlCalculator:
    """Calculate PnL using Meteora DLMM API"""

    def __init__(self):
        self.base_url = "https://dlmm-api.meteora.ag"
        self._pair_cache: Dict[str, Tuple[bool, bool, str]] = {}  # pair_addr -> (sol_is_x, sol_is_y, token_mint)

    def _get_sol_side(self, pair_address: str) -> Optional[Tuple[bool, bool]]:
        """Determine which token (x or y) is SOL for a pair. Returns (sol_is_x, sol_is_y)."""
        if pair_address in self._pair_cache:
            sol_is_x, sol_is_y, _ = self._pair_cache[pair_address]
            return (sol_is_x, sol_is_y)

        pair_info = self._meteora_get(f"/pair/{pair_address}")
        if not pair_info:
            return None

        mint_x = pair_info.get('mint_x', '')
        mint_y = pair_info.get('mint_y', '')
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
            # Get position info to find pair_address
            pos_info = self._meteora_get(f"/position/{address}")
            if not pos_info:
                return None

            pair_address = pos_info.get('pair_address', '')
            if not pair_address:
                print(f"  Warning: No pair_address for {short_id(address)}")
                return None

            time.sleep(0.3)

            # Get mint info from pair to determine which token is SOL
            sol_side = self._get_sol_side(pair_address)
            if not sol_side:
                print(f"  Warning: No SOL token found in pair for {short_id(address)}")
                return None

            sol_is_x, sol_is_y = sol_side

            time.sleep(0.3)

            LAMPORTS = Decimal('1000000000')

            # Helper: compute SOL equivalent for a transaction entry.
            # Converts the non-SOL token to SOL using the per-transaction SOL price
            # derived from the SOL-side USD amount.
            def _tx_sol_equiv(
                entry,
                fallback_sol_price: Decimal,
            ) -> tuple[Decimal, Decimal, Decimal, Decimal]:
                """Returns (sol_amount, sol_equiv_total, total_usd, sol_price_used)"""
                sol_key = 'token_x_amount' if sol_is_x else 'token_y_amount'
                tok_key = 'token_y_amount' if sol_is_x else 'token_x_amount'
                sol_usd_key = 'token_x_usd_amount' if sol_is_x else 'token_y_usd_amount'
                tok_usd_key = 'token_y_usd_amount' if sol_is_x else 'token_x_usd_amount'

                sol_raw = Decimal(str(entry.get(sol_key, entry.get(sol_key.replace('_', 'X_' if sol_is_x else 'Y_'), 0))))
                sol_amt = sol_raw / LAMPORTS
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

            # Track a running SOL price for fallback (for txs with no SOL portion)
            running_sol_price = Decimal('0')

            # Get deposits - None means API error, [] means no deposits
            deposits = self._meteora_get(f"/position/{address}/deposits")
            if deposits is None:
                print(f"  API error on deposits for {short_id(address)}")
                return None
            dep_sol_total = Decimal('0')
            dep_sol_equiv = Decimal('0')
            dep_usd = Decimal('0')
            for dep in deposits:
                sol_amt, equiv, usd, price = _tx_sol_equiv(dep, running_sol_price)
                dep_sol_total += sol_amt
                dep_sol_equiv += equiv
                dep_usd += usd
                if price > 0:
                    running_sol_price = price

            time.sleep(0.3)

            # Get withdrawals - None means API error, [] means no withdrawals
            withdraws = self._meteora_get(f"/position/{address}/withdraws")
            if withdraws is None:
                print(f"  API error on withdraws for {short_id(address)}")
                return None
            wdr_sol_total = Decimal('0')
            wdr_sol_equiv = Decimal('0')
            wdr_usd = Decimal('0')
            for w in withdraws:
                sol_amt, equiv, usd, price = _tx_sol_equiv(w, running_sol_price)
                wdr_sol_total += sol_amt
                wdr_sol_equiv += equiv
                wdr_usd += usd
                if price > 0:
                    running_sol_price = price

            time.sleep(0.3)

            # Get claimed fees - None means API error, [] means no fees
            fees_list = self._meteora_get(f"/position/{address}/claim_fees")
            if fees_list is None:
                print(f"  API error on claim_fees for {short_id(address)}")
                return None
            fee_sol_total = Decimal('0')
            fee_sol_equiv = Decimal('0')
            fee_usd = Decimal('0')
            for f_entry in fees_list:
                sol_amt, equiv, usd, price = _tx_sol_equiv(f_entry, running_sol_price)
                fee_sol_total += sol_amt
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
