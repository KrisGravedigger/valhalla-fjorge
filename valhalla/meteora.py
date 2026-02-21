"""
Meteora DLMM API client for PnL calculation.
"""

import json
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from typing import Dict, Optional, Tuple

from .models import MeteoraPnlResult, SOL_MINT, short_id


class MoralisMarketPricer:
    """Fetch market prices from Moralis for accurate token valuations."""
    BASE_URL = "https://solana-gateway.moralis.io"

    def __init__(self, api_key: str):
        self._headers = {
            'X-API-Key': api_key,
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        }
        self._pair_cache: Dict[str, Optional[str]] = {}      # token_mint -> best pair addr (or None)
        self._price_cache: Dict[tuple, Optional[float]] = {}  # (pair_addr, minute_bucket) -> price_usd

    def get_best_pair(self, token_mint: str) -> Optional[str]:
        """Find the most liquid indexed pair for a token via Moralis."""
        if token_mint in self._pair_cache:
            return self._pair_cache[token_mint]
        data = self._moralis_get(f'/token/mainnet/{token_mint}/pairs?limit=10')
        pairs = data.get('pairs', []) if data else []
        # Prefer high-liquidity DEXes; PumpSwap first for pump.fun tokens
        preferred = ['pumpswap', 'raydium', 'orca']
        best = None
        for pref in preferred:
            for p in pairs:
                if pref in p.get('exchangeName', '').lower():
                    best = p.get('pairAddress')
                    break
            if best:
                break
        if not best and pairs:
            best = pairs[0].get('pairAddress')
        self._pair_cache[token_mint] = best
        time.sleep(0.3)
        return best

    def get_price_usd(self, pair_addr: str, timestamp: int) -> Optional[float]:
        """Get USD price at a Unix timestamp using 1-min OHLCV. Returns None if unavailable."""
        bucket = (pair_addr, timestamp // 60)
        if bucket in self._price_cache:
            return self._price_cache[bucket]

        from_dt = datetime.utcfromtimestamp(timestamp - 120).strftime('%Y-%m-%dT%H:%M:%SZ')
        to_dt = datetime.utcfromtimestamp(timestamp + 60).strftime('%Y-%m-%dT%H:%M:%SZ')
        url = f'/token/mainnet/pairs/{pair_addr}/ohlcv?timeframe=1min&fromDate={from_dt}&toDate={to_dt}&limit=5&currency=usd'

        data = self._moralis_get(url)
        price = None
        if data and data.get('result'):
            # Results are newest-first; find candle where candle_start <= timestamp
            for r in data['result']:
                ts_str = r.get('timestamp', '')[:19]
                try:
                    candle_dt = datetime.strptime(ts_str, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
                    if int(candle_dt.timestamp()) <= timestamp:
                        raw = r.get('close') or r.get('open')
                        price = float(raw) if raw else None
                        break
                except (ValueError, TypeError):
                    pass
        time.sleep(0.2)
        self._price_cache[bucket] = price
        return price

    def _moralis_get(self, path: str) -> Optional[dict]:
        try:
            req = urllib.request.Request(self.BASE_URL + path, headers=self._headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except Exception as e:
            print(f'  Moralis GET error ({path[:60]}): {e}')
            return None


class MeteoraPnlCalculator:
    """Calculate PnL using Meteora DLMM API"""

    def __init__(self, moralis_pricer: Optional['MoralisMarketPricer'] = None):
        self.base_url = "https://dlmm-api.meteora.ag"
        self._pair_cache: Dict[str, Tuple[bool, bool, str]] = {}  # pair_addr -> (sol_is_x, sol_is_y, token_mint)
        self._pricer = moralis_pricer

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

            # Resolve Moralis market pair for this position's token (if pricer available)
            token_mint = self._get_token_mint(pair_address)
            market_pair: Optional[str] = None
            if self._pricer and token_mint:
                market_pair = self._pricer.get_best_pair(token_mint)
                if market_pair:
                    print(f"  Moralis pair: {market_pair[:8]}... for token {token_mint[:8]}...")

            # Helper: compute SOL equivalent for a transaction entry.
            # Converts the non-SOL token to SOL using the per-transaction SOL price
            # derived from the SOL-side USD amount.
            # market_price_usd: optional external USD price to correct DLMM bin price lag
            def _tx_sol_equiv(
                entry,
                fallback_sol_price: Decimal,
                market_price_usd: Optional[float] = None,
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

                # Apply market price correction for token-only entries where DLMM
                # active bin price can lag the external market (PumpSwap/Raydium/Orca).
                # Only applies when: no SOL in this entry, market price is available,
                # and the entry has a DLMM bin price field.
                if sol_amt == Decimal('0') and market_price_usd and sol_price > 0:
                    entry_dlmm_price = Decimal(str(entry.get('price', 0)))
                    if entry_dlmm_price > 0:
                        market_price_sol = Decimal(str(market_price_usd)) / sol_price
                        if market_price_sol > 0:
                            token_sol_equiv = (tok_usd / sol_price) * (market_price_sol / entry_dlmm_price)

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
                # For token-only withdrawals (no SOL), fetch Moralis market price
                # to correct the DLMM active bin price lag
                market_price_usd: Optional[float] = None
                if self._pricer and market_pair:
                    sol_key_w = 'token_x_amount' if sol_is_x else 'token_y_amount'
                    w_sol_raw = Decimal(str(w.get(sol_key_w, 0)))
                    if w_sol_raw == Decimal('0'):  # token-only withdrawal
                        ts = w.get('onchain_timestamp', 0)
                        if ts:
                            market_price_usd = self._pricer.get_price_usd(market_pair, int(ts))
                sol_amt, equiv, usd, price = _tx_sol_equiv(w, running_sol_price, market_price_usd)
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
                # Fee entries don't have a 'price' field, so market price correction
                # inside _tx_sol_equiv will not trigger (entry_dlmm_price will be 0).
                # We pass None for market_price_usd to skip the Moralis lookup.
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
