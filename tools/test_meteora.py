"""Test Meteora API with real position addresses found on-chain"""
import json
import os
import urllib.request
import time

# Load from .env or environment
WALLET = os.environ.get("WALLET_ADDRESS", "")
SOL_MINT = "So11111111111111111111111111111111111111112"

# Load .env file if present
_env_path = os.path.join(os.path.dirname(__file__), ".env")
if os.path.exists(_env_path):
    with open(_env_path) as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _key, _val = _line.split("=", 1)
                os.environ.setdefault(_key.strip(), _val.strip())
    WALLET = os.environ.get("WALLET_ADDRESS", WALLET)

if not WALLET:
    print("Error: Set WALLET_ADDRESS in .env or environment")
    exit(1)

# Position addresses to test (pass as args or set in .env)
POSITIONS = [p.strip() for p in os.environ.get("TEST_POSITIONS", "").split(",") if p.strip()]
if not POSITIONS:
    print("Error: Set TEST_POSITIONS in .env (comma-separated position addresses)")
    exit(1)

BASE_URL = "https://dlmm.datapi.meteora.ag"

def meteora_get(path):
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())

def short_id(addr):
    return addr[:4] + addr[-4:]

for addr in POSITIONS:
    sid = short_id(addr)
    print(f"\n{'='*60}")
    print(f"Position: {addr} (short: {sid})")
    print(f"{'='*60}")

    try:
        # Get all events in one call from /positions/{addr}/historical
        historical = meteora_get(f"/positions/{addr}/historical")
        all_events = historical.get('events', [])
        print(f"Total events: {len(all_events)}")

        if not all_events:
            print("  No events found.")
            continue

        # Derive pool address from first event
        pool_address = all_events[0].get('poolAddress', '')
        print(f"Pool address: {pool_address}")

        time.sleep(0.5)

        # Split by eventType
        deposits = [e for e in all_events if e.get('eventType') == 'add']
        withdraws = [e for e in all_events if e.get('eventType') == 'remove']
        fees = [e for e in all_events if e.get('eventType') == 'claim_fee']
        other = [e for e in all_events if e.get('eventType') not in ('add', 'remove', 'claim_fee')]

        print(f"  add={len(deposits)}, remove={len(withdraws)}, claim_fee={len(fees)}, other={len(other)}")
        if other:
            print(f"  Other event types: {set(e.get('eventType') for e in other)}")

        print(f"\nDeposits ({len(deposits)} entries):")
        for dep in deposits:
            sig = dep.get("signature", "?")
            x_amt = dep.get("amountX", 0)
            y_amt = dep.get("amountY", 0)
            block_time = dep.get("blockTime", "?")
            print(f"  sig={sig[:16]}... | X={x_amt} | Y={y_amt} | blockTime={block_time}")

        print(f"\nWithdrawals ({len(withdraws)} entries):")
        for w in withdraws:
            sig = w.get("signature", "?")
            x_amt = w.get("amountX", 0)
            y_amt = w.get("amountY", 0)
            block_time = w.get("blockTime", "?")
            print(f"  sig={sig[:16]}... | X={x_amt} | Y={y_amt} | blockTime={block_time}")

        print(f"\nClaimed Fees ({len(fees)} entries):")
        for f_entry in fees:
            sig = f_entry.get("signature", "?")
            x_amt = f_entry.get("amountX", 0)
            y_amt = f_entry.get("amountY", 0)
            block_time = f_entry.get("blockTime", "?")
            print(f"  sig={sig[:16]}... | X={x_amt} | Y={y_amt} | blockTime={block_time}")

        time.sleep(0.5)

        # Get pool info to verify token mints
        if pool_address:
            pool_info = meteora_get(f"/pools/{pool_address}")
            token_x_addr = pool_info.get('token_x', {}).get('address', '?')
            token_y_addr = pool_info.get('token_y', {}).get('address', '?')
            print(f"\nPool token_x.address: {token_x_addr}")
            print(f"Pool token_y.address: {token_y_addr}")
            sol_is_x = (token_x_addr == SOL_MINT)
            sol_is_y = (token_y_addr == SOL_MINT)
            print(f"SOL is X: {sol_is_x}, SOL is Y: {sol_is_y}")

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
