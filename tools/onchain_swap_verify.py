"""
Verify actual SOL received from on-chain swap TXs after position close.
Finds Jupiter swap TX by tracking the token account, then compares real vs estimated PnL.
"""
import urllib.request
import json
import time
from datetime import datetime
from decimal import Decimal

SOLANA_RPC = "https://api.mainnet-beta.solana.com"
SOL_MINT = "So11111111111111111111111111111111111111112"
LAMPORTS = Decimal("1000000000")

# Positions: (pos_id, token, withdrawal_tx_with_tokens, our_pnl, lp_pnl, deposited, sol_withdrawn, fees)
POSITIONS = [
    ("3KSCw7vD", "automaton",
     "3djVhhG8LqibgJEHaft2ALbgG6ZHqVugkRzRWSUpGKcaMcQ35FfPJAJACHduxiGiQs6DFRkQj8iw5Ci3pruGVnEn",
     Decimal("-0.5991"), Decimal("-0.6708"), Decimal("4.0000"), Decimal("0.7517"), Decimal("0.3309")),
    ("ABZp12yw", "TOTO",
     "5VeDNN4GjNtXiZDukV7U2JeaWT8p1rQDTervMtmQA2UNHtn84h3bgjnyjK9qSAArJffaYrUEfHfBTnVFQXdqqGqJ",
     Decimal("-0.0696"), Decimal("-0.1145"), Decimal("2.8000"), Decimal("0.7141"), Decimal("0.1134")),
    ("BvapUjjU", "Lobstar",
     "5SmEeAUKzbX34xszigtt51n1WZaU2p95GTZKPoTh1bB3X1qha5nTh8YRZCFRxhKDTtVruFsnfq4BbyhoHX4b3x38",
     Decimal("-0.4822"), Decimal("-0.5164"), Decimal("4.0000"), Decimal("0.0000"), Decimal("0.1255")),
    ("9Q3Tk7PW", "MUSHU",
     "5qQmbMk9awNGkNovXZd2fEabBEeybuxmhgL11TaDWmkueNTsZXjQ9pULipAq1TQQ8EcGJ2FNQGqhauozTVF4YGPa",
     Decimal("-0.4070"), Decimal("-0.3157"), Decimal("2.9839"), Decimal("0.0000"), Decimal("0.1782")),
    ("Azsxa6HU", "Jellycat",
     "5X3orHgRZu1eoZG6Wa6JS1UYCzJyrJR4FjA5DNaZYzptrqB8X9tS9yNVCnkNCCvr49FzMKFQhPiBKDuhHtcuEcdF",
     Decimal("-0.3584"), Decimal("-0.2797"), Decimal("3.2000"), Decimal("0.0000"), Decimal("0.1437")),
]


def rpc_call(method, params):
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    req = urllib.request.Request(SOLANA_RPC, data=payload.encode(), headers={"Content-Type": "application/json"})
    for attempt in range(4):
        try:
            resp = urllib.request.urlopen(req, timeout=30)
            return json.loads(resp.read()).get("result")
        except Exception as e:
            if "429" in str(e) and attempt < 3:
                wait = (attempt + 1) * 4
                print(f"    [rate limited, waiting {wait}s]")
                time.sleep(wait)
            else:
                print(f"    [RPC error: {e}]")
                return None


def find_token_account(tx_result, wallet):
    """Find the non-SOL token account owned by wallet in a TX."""
    meta = tx_result.get("meta", {})
    post_tb = meta.get("postTokenBalances", [])
    keys = tx_result.get("transaction", {}).get("message", {}).get("accountKeys", [])

    for tb in post_tb:
        if tb.get("owner") == wallet and tb.get("mint") != SOL_MINT:
            idx = tb.get("accountIndex")
            acct = keys[idx]["pubkey"] if isinstance(keys[idx], dict) else keys[idx]
            mint = tb.get("mint")
            amount = tb.get("uiTokenAmount", {}).get("uiAmountString", "0")
            return acct, mint, amount
    return None, None, None


def find_swap_after(token_account, withdrawal_sig, withdrawal_bt, wallet):
    """Find the Jupiter swap TX that drains the token account after withdrawal."""
    # Get all sigs for the token account
    all_sigs = rpc_call("getSignaturesForAddress", [token_account, {"limit": 1000}])
    if not all_sigs:
        return None

    # Sort chronologically
    sorted_sigs = sorted(all_sigs, key=lambda s: s.get("blockTime", 0))

    # Find withdrawal, then check TXs after it
    found_withdrawal = False
    for sig_info in sorted_sigs:
        if sig_info["signature"] == withdrawal_sig:
            found_withdrawal = True
            continue
        if not found_withdrawal:
            continue

        # This is a TX after withdrawal - check if it's a swap
        sig = sig_info["signature"]
        bt = sig_info.get("blockTime", 0)

        # Only look within 10 minutes
        if bt - withdrawal_bt > 600:
            break

        time.sleep(2)
        result = rpc_call("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        if not result:
            continue

        meta = result.get("meta", {})
        if meta.get("err"):
            continue

        keys = result.get("transaction", {}).get("message", {}).get("accountKeys", [])
        key_list = [k["pubkey"] if isinstance(k, dict) else k for k in keys]
        is_jupiter = any(k.startswith("JUP") for k in key_list)

        if not is_jupiter:
            continue

        # Get SOL change
        pre = meta.get("preBalances", [])
        post = meta.get("postBalances", [])
        fee = Decimal(str(meta.get("fee", 0)))

        wallet_idx = None
        for i, k in enumerate(keys):
            pubkey = k["pubkey"] if isinstance(k, dict) else k
            if pubkey == wallet:
                wallet_idx = i
                break

        if wallet_idx is None:
            continue

        sol_change = (Decimal(str(post[wallet_idx])) - Decimal(str(pre[wallet_idx])) + fee) / LAMPORTS

        # Check token was drained
        post_tb = meta.get("postTokenBalances", [])
        token_drained = False
        for tb in post_tb:
            if tb.get("owner") == wallet and tb.get("mint") != SOL_MINT:
                amt = Decimal(tb.get("uiTokenAmount", {}).get("uiAmountString", "0") or "0")
                if amt == 0:
                    token_drained = True

        return {
            "signature": sig,
            "sol_received": sol_change,
            "block_time": bt,
            "token_drained": token_drained,
        }

    return None


def main():
    print("=" * 70)
    print("ON-CHAIN SWAP VERIFICATION: Real SOL from Jupiter swaps")
    print("=" * 70)

    results = []

    for pos_id, token, withdrawal_tx, our_pnl, lp_pnl, deposited, sol_withdrawn, fees in POSITIONS:
        print(f"\n{'=' * 60}")
        print(f"  {pos_id} ({token})")
        print(f"{'=' * 60}")

        # Step 1: Get wallet and token account from withdrawal TX
        print("  Fetching withdrawal TX...")
        time.sleep(2)
        tx_result = rpc_call("getTransaction", [withdrawal_tx, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}])
        if not tx_result:
            print("  FAILED")
            continue

        keys = tx_result.get("transaction", {}).get("message", {}).get("accountKeys", [])
        wallet = None
        for k in keys:
            if isinstance(k, dict) and k.get("signer"):
                wallet = k["pubkey"]
                break

        withdrawal_bt = tx_result.get("blockTime", 0)
        print(f"  Wallet: {wallet[:8]}...{wallet[-4:]}")
        print(f"  Withdrawal: {datetime.fromtimestamp(withdrawal_bt)}")

        # Find token account
        token_acct, mint, token_amount = find_token_account(tx_result, wallet)
        if not token_acct:
            print("  No token account found in withdrawal")
            continue
        print(f"  Token account: {token_acct[:8]}...{token_acct[-4:]}")
        print(f"  Tokens received: {token_amount}")

        # Step 2: Find swap TX
        print("  Searching for Jupiter swap...")
        time.sleep(2)
        swap = find_swap_after(token_acct, withdrawal_tx, withdrawal_bt, wallet)

        if swap:
            sol_from_swap = swap["sol_received"]
            swap_time = datetime.fromtimestamp(swap["block_time"])
            delay = swap["block_time"] - withdrawal_bt

            # Real PnL = sol_withdrawn + sol_from_swap + fees - deposited
            real_pnl = sol_withdrawn + sol_from_swap + fees - deposited

            print(f"\n  SWAP FOUND!")
            print(f"  TX: {swap['signature']}")
            print(f"  Time: {swap_time} (+{delay}s after withdrawal)")
            print(f"  SOL received: {sol_from_swap:.6f}")
            print(f"  Token drained: {swap['token_drained']}")

            print(f"\n  --- PnL COMPARISON ---")
            print(f"  Deposited:         {deposited:.4f} SOL")
            print(f"  SOL from pool:     {sol_withdrawn:.4f} SOL")
            print(f"  SOL from swap:     {sol_from_swap:.6f} SOL")
            print(f"  Fees (DLMM est):   {fees:.4f} SOL")
            print(f"  ---")
            print(f"  REAL PnL (swap):   {real_pnl:+.4f} SOL")
            print(f"  Our PnL (DLMM):    {our_pnl:+.4f} SOL  (diff: {our_pnl - real_pnl:+.4f})")
            print(f"  lpagent PnL:       {lp_pnl:+.4f} SOL  (diff: {lp_pnl - real_pnl:+.4f})")

            results.append((pos_id, token, real_pnl, our_pnl, lp_pnl, swap["signature"]))
        else:
            print("  NO SWAP FOUND within 10 minutes")
            results.append((pos_id, token, None, our_pnl, lp_pnl, None))

    # Summary
    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    print(f"{'Pos':>10s} {'Token':>10s} {'Real PnL':>10s} {'Our PnL':>10s} {'lp PnL':>10s} {'Our err':>10s} {'lp err':>10s}")
    for pos_id, token, real_pnl, our_pnl, lp_pnl, swap_sig in results:
        if real_pnl is not None:
            print(f"{pos_id:>10s} {token:>10s} {real_pnl:>+10.4f} {our_pnl:>+10.4f} {lp_pnl:>+10.4f} "
                  f"{our_pnl - real_pnl:>+10.4f} {lp_pnl - real_pnl:>+10.4f}")
        else:
            print(f"{pos_id:>10s} {token:>10s} {'N/A':>10s} {our_pnl:>+10.4f} {lp_pnl:>+10.4f} {'N/A':>10s} {'N/A':>10s}")


if __name__ == "__main__":
    main()
