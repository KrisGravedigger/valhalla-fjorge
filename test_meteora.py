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

def meteora_get(path):
    url = f"https://dlmm-api.meteora.ag{path}"
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
        # Get position info
        try:
            pos = meteora_get(f"/position/{addr}")
            print(f"Position info: {json.dumps(pos, indent=2)[:400]}")
        except Exception as e:
            print(f"Position info: {e}")

        time.sleep(0.5)

        # Get deposits
        deposits = meteora_get(f"/position/{addr}/deposits")
        print(f"\nDeposits ({len(deposits)} entries):")
        for dep in deposits:
            # Convert lamports to SOL (1 SOL = 10^9 lamports)
            tx = dep.get("tx_id", dep.get("txn_id", "?"))
            x_amt = dep.get("token_x_amount", dep.get("tokenX_amount", 0))
            y_amt = dep.get("token_y_amount", dep.get("tokenY_amount", 0))
            x_addr = dep.get("token_x_mint", dep.get("tokenX_address", "?"))
            y_addr = dep.get("token_y_mint", dep.get("tokenY_address", "?"))
            ts = dep.get("onchain_timestamp", "?")
            print(f"  tx={tx[:16]}... | X={x_amt} ({x_addr[:8]}...) | Y={y_amt} ({y_addr[:8]}...) | ts={ts}")

        time.sleep(0.5)

        # Get withdrawals
        withdraws = meteora_get(f"/position/{addr}/withdraws")
        print(f"\nWithdrawals ({len(withdraws)} entries):")
        for w in withdraws:
            tx = w.get("tx_id", w.get("txn_id", "?"))
            x_amt = w.get("token_x_amount", w.get("tokenX_amount", 0))
            y_amt = w.get("token_y_amount", w.get("tokenY_amount", 0))
            ts = w.get("onchain_timestamp", "?")
            print(f"  tx={tx[:16]}... | X={x_amt} | Y={y_amt} | ts={ts}")

        time.sleep(0.5)

        # Get claimed fees
        fees = meteora_get(f"/position/{addr}/claim_fees")
        print(f"\nClaimed Fees ({len(fees)} entries):")
        for f_entry in fees:
            tx = f_entry.get("tx_id", f_entry.get("txn_id", "?"))
            x_amt = f_entry.get("token_x_amount", f_entry.get("tokenX_amount", 0))
            y_amt = f_entry.get("token_y_amount", f_entry.get("tokenY_amount", 0))
            ts = f_entry.get("onchain_timestamp", "?")
            print(f"  tx={tx[:16]}... | X={x_amt} | Y={y_amt} | ts={ts}")

        time.sleep(0.5)

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
