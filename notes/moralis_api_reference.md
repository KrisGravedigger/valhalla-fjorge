# Moralis API Reference: Solana Token Pricing

Działający przykład integracji z Moralis Solana Gateway API.
Zaimplementowany w `valhalla/meteora.py` (branch `claude/fix-meteora-token-pricing`),
następnie usunięty ponieważ ceny DLMM są wystarczająco dokładne (błąd ~1-2%).

## Kiedy może być przydatny

- Pobieranie aktualnej ceny tokena (mark-to-market)
- Pobieranie historycznej ceny tokena z dokładnością do minuty
- Weryfikacja czy cena DLMM mocno odbiega od ceny rynkowej
- Ogólne zapytania o pary handlowe dla tokena

## Klucz API

Wymaga klucza w zmiennej środowiskowej lub konfiguracji:
`X-API-Key: {moralis_api_key}`

Endpoint bazowy: `https://solana-gateway.moralis.io`

## Pełny kod klasy

```python
import json
import time
import urllib.request
from datetime import datetime, timezone
from typing import Dict, Optional


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
        """Find the most liquid indexed pair for a token via Moralis.

        Returns the pair address (e.g. from PumpSwap, Raydium, or Orca)
        that can be used to query OHLCV price history.

        Preference order: PumpSwap > Raydium > Orca > any first result
        """
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
        time.sleep(0.3)  # rate limit
        return best

    def get_price_usd(self, pair_addr: str, timestamp: int) -> Optional[float]:
        """Get USD price at a Unix timestamp using 1-min OHLCV candles.

        Returns the close price of the candle that started at or before
        the given timestamp. Returns None if no data available.

        timestamp: Unix timestamp (seconds)
        """
        bucket = (pair_addr, timestamp // 60)
        if bucket in self._price_cache:
            return self._price_cache[bucket]

        from_dt = datetime.utcfromtimestamp(timestamp - 120).strftime('%Y-%m-%dT%H:%M:%SZ')
        to_dt   = datetime.utcfromtimestamp(timestamp + 60).strftime('%Y-%m-%dT%H:%M:%SZ')
        url = (
            f'/token/mainnet/pairs/{pair_addr}/ohlcv'
            f'?timeframe=1min&fromDate={from_dt}&toDate={to_dt}&limit=5&currency=usd'
        )

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

        time.sleep(0.2)  # rate limit
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
```

## Przykład użycia

```python
from decimal import Decimal

pricer = MoralisMarketPricer(api_key="YOUR_KEY")

token_mint = "oBeMrKMEqaLN8hYeuTiHDx91MXTP5zrsuKnhS2Spump"  # The Automaton
sol_price_usd = 81.60  # aktualny kurs SOL

# Krok 1: znajdź parę handlową
pair_addr = pricer.get_best_pair(token_mint)
print(f"Best pair: {pair_addr}")  # np. adres pary na PumpSwap

# Krok 2: pobierz cenę USD w danym momencie
timestamp = 1740038400  # Unix timestamp zamknięcia pozycji
price_usd = pricer.get_price_usd(pair_addr, timestamp)
print(f"Token price at close: ${price_usd}")

# Przelicz na SOL
if price_usd:
    price_sol = Decimal(str(price_usd)) / Decimal(str(sol_price_usd))
    print(f"Token price in SOL: {price_sol:.8f}")
```

## Endpointy API użyte w kodzie

### GET /token/mainnet/{token_mint}/pairs
Zwraca listę par handlowych dla danego tokena.

Odpowiedź:
```json
{
  "pairs": [
    {
      "pairAddress": "...",
      "exchangeName": "PumpSwap",
      "baseToken": {...},
      "quoteToken": {...}
    }
  ]
}
```

### GET /token/mainnet/pairs/{pair_addr}/ohlcv
Zwraca świece OHLCV (Open/High/Low/Close/Volume).

Parametry:
- `timeframe`: `1min`, `5min`, `1h`, `1d`
- `fromDate`: ISO 8601 (np. `2026-02-19T05:00:00Z`)
- `toDate`: ISO 8601
- `limit`: max liczba świec
- `currency`: `usd` lub `native`

Odpowiedź:
```json
{
  "result": [
    {
      "timestamp": "2026-02-19T05:05:00.000Z",
      "open": "0.00121",
      "high": "0.00124",
      "low": "0.00119",
      "close": "0.00120",
      "volume": "12345.67"
    }
  ]
}
```

## Dlaczego Moralis został usunięty z kalkulatora PnL

Weryfikacja na danych on-chain (Solana RPC, luty 2026):

| Pozycja | Meteora DLMM | Moralis-corrected | Rzeczywisty swap |
|---|---|---|---|
| 3KSCok (automaton) | -0.658 SOL | -0.541 SOL | ~-0.69 SOL |
| 5QYhYU (Punch) | -0.036 SOL | -0.095 SOL | ~-0.01 SOL |

Cena DLMM jest bliższa rzeczywistemu swapowi (~1-2% różnicy ze względu na slippage)
niż cena rynkowa Moralis. Korekcja Moralis działa w złą stronę lub przekracza
rzeczywistą różnicę. Szczegóły: `notes/pnl_discrepancy_analysis.md`.
