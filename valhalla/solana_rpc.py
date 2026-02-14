"""
Solana RPC client and address resolution.
"""

import json
import time
import urllib.request
from pathlib import Path
from typing import Dict, List, Optional

from .models import KNOWN_PROGRAMS, short_id


class AddressCache:
    """JSON file persistence for short_id -> full_address mapping"""

    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.cache: Dict[str, str] = {}
        self.load()

    def load(self) -> None:
        """Load cache from JSON file"""
        if Path(self.cache_file).exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load cache: {e}")
                self.cache = {}

    def save(self) -> None:
        """Save cache to JSON file"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save cache: {e}")

    def get(self, short_id_val: str) -> Optional[str]:
        """Get full address from short ID"""
        return self.cache.get(short_id_val)

    def set(self, short_id_val: str, full_address: str) -> None:
        """Set short ID -> full address mapping"""
        self.cache[short_id_val] = full_address


class SolanaRpcClient:
    """Solana JSON-RPC client using urllib"""

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self._delay = 0.7  # seconds between requests, increases on rate limit

    def get_transaction(self, signature: str) -> Optional[List[str]]:
        """
        Get transaction and extract account keys.
        Returns list of account key strings.
        """
        for attempt in range(5):
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        signature,
                        {
                            "encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                }

                req = urllib.request.Request(
                    self.rpc_url,
                    data=json.dumps(payload).encode('utf-8'),
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0",
                    }
                )

                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read())

                result = data.get('result')
                if not result:
                    return None

                # Extract account keys
                tx = result.get('transaction', {})
                message = tx.get('message', {})
                account_keys = message.get('accountKeys', [])

                # Account keys can be strings or objects with "pubkey" field
                keys = []
                for key in account_keys:
                    if isinstance(key, str):
                        keys.append(key)
                    elif isinstance(key, dict) and 'pubkey' in key:
                        keys.append(key['pubkey'])

                # Rate limiting - adaptive delay
                time.sleep(self._delay)
                return keys

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    sleep_time = 5 * (2 ** attempt)  # 5, 10, 20, 40, 80s
                    print(f"  Rate limited, waiting {sleep_time}s...", end='', flush=True)
                    time.sleep(sleep_time)
                    # Increase base delay after rate limit hit
                    self._delay = min(self._delay * 2, 5.0)
                else:
                    print(f"  HTTP error {e.code}: {e.reason}")
                    return None

            except Exception as e:
                print(f"  RPC error: {e}")
                if attempt < 4:
                    time.sleep(5 * (2 ** attempt))
                else:
                    return None

        return None


class PositionResolver:
    """Resolve position short IDs to full addresses using RPC"""

    def __init__(self, cache: AddressCache, rpc_client: SolanaRpcClient):
        self.cache = cache
        self.rpc_client = rpc_client

    def resolve(self, position_id: str, tx_signatures: List[str]) -> Optional[str]:
        """
        Resolve position_id to full address.
        Returns full address or None if not found.
        """
        # Check cache first
        cached = self.cache.get(position_id)
        if cached:
            return cached

        # Try each transaction signature
        for sig in tx_signatures:
            account_keys = self.rpc_client.get_transaction(sig)
            if not account_keys:
                continue

            # Filter out known programs and short addresses
            candidates = [
                key for key in account_keys
                if key not in KNOWN_PROGRAMS and len(key) >= 32
            ]

            # Check each candidate
            for candidate in candidates:
                if short_id(candidate) == position_id:
                    # Found match
                    self.cache.set(position_id, candidate)
                    return candidate

        return None
