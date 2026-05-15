#!/usr/bin/env python3
"""
Spike: compute portfolio NAV from on-chain Solana data.
doc 028 — throwaway script, no production quality needed.

Usage:
    python tools/spike_internal_nav.py --lpagent-nav 67.10
"""

import argparse
import base64
import json
import os
import struct
import urllib.request
import urllib.error
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

import base58
from dotenv import load_dotenv
from solders.pubkey import Pubkey

load_dotenv()

# Constants
METEORA_PROGRAM = Pubkey.from_string(
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
)
POSITION_V2_DISC = bytes([117, 176, 212, 199, 245, 180, 133, 182])
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
LAMPORTS = 1_000_000_000

# PositionV2 struct layout (offsets from Meteora DLMM IDL, Borsh encoding):
#   8   discriminator
#  32   lb_pair        pubkey         → offset 8
#  32   owner          pubkey         → offset 40
# 1120  liquidity_shares [u128; 70]  → offset 72
# 3360  reward_infos [UserRewardInfo; 70] → offset 1192
#       UserRewardInfo: [u128; NUM_REWARDS=2] + [u64; NUM_REWARDS=2] = 48B
#       70 * 48 = 3360B  (NOT [UserRewardInfo; 2] — NUM_REWARDS is reward-token
#       count per bin, not the array length; array length = MAX_BIN_PER_POSITION=70)
# 3360  fee_infos [FeeInfo; 70]      → offset 4552
#       FeeInfo: u128 + u128 + u64 + u64 = 48B each → 70*48 = 3360B
#   4   lower_bin_id   i32           → offset 7912
#   4   upper_bin_id   i32           → offset 7916
# Total account size ≈ 8041 bytes
POS_LB_PAIR = 8
POS_OWNER = 40
POS_LIQ_SHARES = 72
POS_FEE_INFOS = 4552
POS_LOWER_BIN = 7912
POS_UPPER_BIN = 7916
# Within each FeeInfo (48 bytes): u128(16) + u128(16) + u64(8) + u64(8)
FEE_INFO_SIZE = 48
FEE_X_PENDING_OFF = 32  # offset within FeeInfo
FEE_Y_PENDING_OFF = 40  # offset within FeeInfo

# BinArray layout:
#  8   discriminator
#  8   index           i64
# 32   lb_pair         pubkey
# = 48 bytes header
# Each Bin (112 bytes):
#  8   amount_x        u64
#  8   amount_y        u64
# 16   price           u128
# 16   liquidity_supply u128
# 32   reward_per_token_stored [u128; 2]
# 32   fee_amounts_per_token_stored [u128; 2]
BA_HEADER = 48
BIN_SIZE = 112
BIN_AMOUNT_X = 0     # u64 within bin
BIN_AMOUNT_Y = 8     # u64 within bin
BIN_LIQ_SUPPLY = 32  # u128 within bin (lo+hi)


# ─── RPC helpers ─────────────────────────────────────────────────────────────

def rpc_call(url: str, method: str, params: list) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    if "error" in data:
        raise RuntimeError(f"RPC {method} error: {data['error']}")
    return data["result"]


def http_get(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "spike-internal-nav/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read())


def fetch_accounts(rpc_url: str, pubkeys: list[str]) -> list[Optional[bytes]]:
    """Fetch base64 account data for pubkeys, chunked at 100."""
    results = []
    for i in range(0, len(pubkeys), 100):
        chunk = pubkeys[i : i + 100]
        result = rpc_call(
            rpc_url, "getMultipleAccounts", [chunk, {"encoding": "base64"}]
        )
        for acct in result["value"]:
            if acct is None:
                results.append(None)
            else:
                results.append(base64.b64decode(acct["data"][0]))
    return results


# ─── Step 1: enumerate positions ─────────────────────────────────────────────

def get_position_addresses(rpc_url: str, wallet: str) -> list[str]:
    """Return list of PositionV2 account addresses owned by wallet."""
    disc_b58 = base58.b58encode(POSITION_V2_DISC).decode()
    result = rpc_call(
        rpc_url,
        "getProgramAccounts",
        [
            str(METEORA_PROGRAM),
            {
                "encoding": "base64",
                "filters": [
                    {"memcmp": {"offset": 0, "bytes": disc_b58}},
                    {"memcmp": {"offset": 40, "bytes": wallet}},
                ],
                "dataSlice": {"offset": 0, "length": 0},
            },
        ],
    )
    return [item["pubkey"] for item in result]


# ─── Step 2: decode PositionV2 ────────────────────────────────────────────────

def decode_position(data: bytes) -> dict:
    # Expected ~8041 bytes; need at least through upper_bin_id at 7916+4=7920
    print(f"    [debug] account size: {len(data)} bytes (expected ~8041)")
    if len(data) < 7920:
        raise ValueError(
            f"PositionV2 too short: {len(data)} bytes "
            f"(expected ~8041; wrong offsets or wrong account type?)"
        )

    lb_pair_bytes = data[POS_LB_PAIR : POS_LB_PAIR + 32]
    lb_pair = base58.b58encode(lb_pair_bytes).decode()
    lower_bin_id = struct.unpack_from("<i", data, POS_LOWER_BIN)[0]
    upper_bin_id = struct.unpack_from("<i", data, POS_UPPER_BIN)[0]

    liq_shares = []
    for j in range(70):
        lo, hi = struct.unpack_from("<QQ", data, POS_LIQ_SHARES + j * 16)
        liq_shares.append(lo + (hi << 64))

    fee_x_pending = 0
    fee_y_pending = 0
    for j in range(70):
        base_off = POS_FEE_INFOS + j * FEE_INFO_SIZE
        fee_x_pending += struct.unpack_from(
            "<Q", data, base_off + FEE_X_PENDING_OFF
        )[0]
        fee_y_pending += struct.unpack_from(
            "<Q", data, base_off + FEE_Y_PENDING_OFF
        )[0]

    return {
        "lb_pair": lb_pair,
        "lb_pair_bytes": lb_pair_bytes,
        "lower_bin_id": lower_bin_id,
        "upper_bin_id": upper_bin_id,
        "width": upper_bin_id - lower_bin_id + 1,
        "liquidity_shares": liq_shares,
        "fee_x_pending_raw": fee_x_pending,
        "fee_y_pending_raw": fee_y_pending,
    }


# ─── Step 3: fetch BinArrays ──────────────────────────────────────────────────

def bin_array_address(lb_pair_pk: Pubkey, array_idx: int) -> Pubkey:
    # Seeds match Meteora TS SDK: [b"bin_array", lb_pair, i64_le]
    seeds = [b"bin_array", bytes(lb_pair_pk), struct.pack("<q", array_idx)]
    addr, _ = Pubkey.find_program_address(seeds, METEORA_PROGRAM)
    return addr


def decode_bin_array(data: bytes, array_idx: int) -> dict[int, dict]:
    """Decode BinArray → dict of bin_id → {amount_x, amount_y, liq_supply}."""
    bins = {}
    base_bin_id = array_idx * 70
    for slot in range(70):
        off = BA_HEADER + slot * BIN_SIZE
        amount_x = struct.unpack_from("<Q", data, off + BIN_AMOUNT_X)[0]
        amount_y = struct.unpack_from("<Q", data, off + BIN_AMOUNT_Y)[0]
        lo, hi = struct.unpack_from("<QQ", data, off + BIN_LIQ_SUPPLY)
        liq_supply = lo + (hi << 64)
        bins[base_bin_id + slot] = {
            "amount_x": amount_x,
            "amount_y": amount_y,
            "liquidity_supply": liq_supply,
        }
    return bins


# ─── Meteora pool API ─────────────────────────────────────────────────────────

def get_pool_mints(lb_pair: str) -> dict:
    """Get token_x/token_y mint addresses from Meteora DLMM API."""
    for url in [
        f"https://dlmm-api.meteora.ag/pair/{lb_pair}",
        f"https://app.meteora.ag/clmm-api/pair/{lb_pair}",
    ]:
        try:
            data = http_get(url)
            mint_x = (
                data.get("mint_x")
                or data.get("token_x_mint")
                or data.get("tokenXMint")
            )
            mint_y = (
                data.get("mint_y")
                or data.get("token_y_mint")
                or data.get("tokenYMint")
            )
            name = data.get("name", "?/?")
            if mint_x and mint_y:
                return {"mint_x": mint_x, "mint_y": mint_y, "name": name}
        except Exception as e:
            print(f"    WARN: pool API failed ({url[:50]}): {e}")
    return {"mint_x": None, "mint_y": None, "name": "?/?"}


# ─── Token decimals ───────────────────────────────────────────────────────────

def get_decimals(rpc_url: str, mint: str) -> int:
    """Fetch token decimals from SPL mint account (offset 44)."""
    if mint == SOL_MINT:
        return 9
    result = rpc_call(rpc_url, "getAccountInfo", [mint, {"encoding": "base64"}])
    if result["value"] is None:
        print(f"    WARN: mint {mint[:8]}... not found, assuming 6")
        return 6
    data = base64.b64decode(result["value"]["data"][0])
    return data[44]


# ─── Jupiter quote ────────────────────────────────────────────────────────────

def jupiter_to_sol(mint: str, amount_raw: int) -> float:
    """Convert raw token units to SOL via Jupiter. Returns 0.0 if no route."""
    if mint == SOL_MINT:
        return amount_raw / LAMPORTS
    if amount_raw < 100:
        return 0.0

    url = (
        f"https://api.jup.ag/swap/v1/quote"
        f"?inputMint={mint}&outputMint={SOL_MINT}"
        f"&amount={amount_raw}&slippageBps=50"
    )
    try:
        data = http_get(url)
        return int(data["outAmount"]) / LAMPORTS
    except urllib.error.HTTPError as e:
        body = e.read().decode(errors="replace")
        no_route = any(
            k in body
            for k in (
                "NO_ROUTES_FOUND",
                "TOKEN_NOT_TRADABLE",
                "COULD_NOT_FIND_ANY_ROUTE",
            )
        )
        if no_route:
            print(f"    WARN: no Jupiter route for {mint[:8]}..., skipping")
        else:
            print(f"    WARN: Jupiter HTTP {e.code} for {mint[:8]}..., skipping")
        return 0.0
    except Exception as e:
        print(f"    WARN: Jupiter error for {mint[:8]}...: {e}")
        return 0.0


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spike: compute portfolio NAV from on-chain data"
    )
    parser.add_argument(
        "--lpagent-nav",
        type=float,
        required=True,
        help="lpagent portfolio widget value read NOW (SOL)",
    )
    parser.add_argument(
        "--wallet", default=None, help="wallet address (default: LPAGENT_WALLET env)"
    )
    parser.add_argument(
        "--rpc-url",
        default=None,
        help="Helius RPC URL (default: built from HELIUS_API_KEY)",
    )
    parser.add_argument(
        "--one-position",
        default=None,
        help="debug: process only this position address",
    )
    args = parser.parse_args()

    api_key = os.getenv("HELIUS_API_KEY")
    rpc_url = args.rpc_url or os.getenv("HELIUS_RPC_URL") or (
        f"https://mainnet.helius-rpc.com/?api-key={api_key}" if api_key else None
    )
    if not rpc_url:
        raise SystemExit("ERROR: set HELIUS_API_KEY or HELIUS_RPC_URL in .env")

    wallet = (
        args.wallet
        or os.getenv("LPAGENT_WALLET")
        or os.getenv("WALLET_ADDRESS")
    )
    if not wallet:
        raise SystemExit("ERROR: set LPAGENT_WALLET in .env or pass --wallet")

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print("=" * 60)
    print(f"Internal NAV Spike — {now_utc}")
    print("=" * 60)
    print(f"Wallet: {wallet[:6]}...{wallet[-4:]}")
    print()

    # ── Step 1: enumerate positions ──────────────────────────────────────────
    print("Step 1: Enumerating PositionV2 accounts...")
    pos_addrs = get_position_addresses(rpc_url, wallet)

    if args.one_position:
        if args.one_position not in pos_addrs:
            print(f"  WARN: {args.one_position} not in enumerated list, adding")
        pos_addrs = [args.one_position]

    print(f"  AC-1: found {len(pos_addrs)} position(s)")
    for a in pos_addrs:
        print(f"    {a}")

    if not pos_addrs:
        raise SystemExit("KILL: getProgramAccounts returned 0 positions.")

    # ── Step 2: decode positions ──────────────────────────────────────────────
    print("\nStep 2: Fetching and decoding position accounts...")
    pos_data_list = fetch_accounts(rpc_url, pos_addrs)

    positions = []
    for addr, raw in zip(pos_addrs, pos_data_list):
        if raw is None:
            print(f"  WARN: no data for {addr}, skipping")
            continue
        try:
            pos = decode_position(raw)
            pos["address"] = addr
            positions.append(pos)
            lower = pos["lower_bin_id"]
            upper = pos["upper_bin_id"]
            print(
                f"  {addr[:8]}... lb_pair={pos['lb_pair'][:8]}... "
                f"lower={lower} upper={upper} width={pos['width']}"
            )
        except Exception as e:
            print(f"  KILL candidate: decode failed for {addr}: {e}")

    # AC-2 validation
    for pos in positions:
        lid = pos["lower_bin_id"]
        assert -10_000 <= lid <= 10_000, f"lower_bin_id={lid} out of range"
        w = pos["width"]
        assert 1 <= w <= 200, f"width={w} suspicious"
        assert any(
            s > 0 for s in pos["liquidity_shares"]
        ), "all liquidity_shares are zero"
    n = len(positions)
    print(f"  AC-2 PASS: all {n} position(s) decoded with sensible fields")

    # ── Steps 3-5: BinArrays → bin math → SOL conversion ────────────────────
    print("\nSteps 3-5: BinArrays, bin math, Jupiter conversion...")

    position_results = []
    total_unclaimed_fee_sol = Decimal("0")

    for pos in positions:
        addr = pos["address"]
        lb_pair_str = pos["lb_pair"]
        lb_pair_pk = Pubkey.from_bytes(pos["lb_pair_bytes"])
        lower = pos["lower_bin_id"]
        upper = pos["upper_bin_id"]

        print(f"\n  Position {addr[:8]}...")

        # Pool mints
        pool = get_pool_mints(lb_pair_str)
        mint_x = pool["mint_x"] or SOL_MINT
        mint_y = pool["mint_y"] or SOL_MINT
        print(f"    Pool: {pool['name']}")
        print(f"    mintX={mint_x[:8]}...  mintY={mint_y[:8]}...")

        dec_x = get_decimals(rpc_url, mint_x)
        dec_y = get_decimals(rpc_url, mint_y)
        print(f"    Decimals: X={dec_x} Y={dec_y}")

        # Required BinArrays
        required_arrays = sorted(
            {bin_id // 70 for bin_id in range(lower, upper + 1)}
        )
        print(f"    BinArrays: {required_arrays}")

        ba_addr_map = {
            idx: bin_array_address(lb_pair_pk, idx) for idx in required_arrays
        }
        ba_addrs_str = [str(ba_addr_map[idx]) for idx in required_arrays]
        ba_raw_list = fetch_accounts(rpc_url, ba_addrs_str)

        bin_arrays: dict[int, dict] = {}
        for idx, ba_raw in zip(required_arrays, ba_raw_list):
            if ba_raw is None:
                print(f"    WARN: BinArray idx={idx} not found (bad PDA?)")
                continue
            try:
                bin_arrays[idx] = decode_bin_array(ba_raw, idx)
            except Exception as e:
                print(f"    WARN: BinArray idx={idx} decode error: {e}")

        if not bin_arrays:
            print(f"    KILL candidate: no BinArray data for {addr[:8]}...")
            position_results.append(
                {
                    "address": addr,
                    "pair": pool["name"],
                    "reserves_sol": Decimal("0"),
                    "fees_sol": Decimal("0"),
                }
            )
            continue

        n_ok = len(bin_arrays)
        n_needed = len(required_arrays)
        print(f"    AC-3 PASS: {n_ok}/{n_needed} BinArray(s) fetched")

        # Bin math
        total_x_raw = Decimal(0)
        total_y_raw = Decimal(0)

        for bin_offset, bin_id in enumerate(range(lower, upper + 1)):
            array_idx = bin_id // 70
            if array_idx not in bin_arrays:
                continue
            bd = bin_arrays[array_idx].get(bin_id)
            if bd is None:
                continue

            liq_supply = bd["liquidity_supply"]
            liq_share = pos["liquidity_shares"][bin_offset]

            if liq_supply == 0 or liq_share == 0:
                continue

            frac = Decimal(liq_share) / Decimal(liq_supply)
            total_x_raw += frac * Decimal(bd["amount_x"])
            total_y_raw += frac * Decimal(bd["amount_y"])

        fee_x_raw = pos["fee_x_pending_raw"]
        fee_y_raw = pos["fee_y_pending_raw"]

        x_human = total_x_raw / Decimal(10 ** dec_x)
        y_human = total_y_raw / Decimal(10 ** dec_y)
        fx_human = Decimal(fee_x_raw) / Decimal(10 ** dec_x)
        fy_human = Decimal(fee_y_raw) / Decimal(10 ** dec_y)
        print(f"    Reserves:  X={float(x_human):.4f}  Y={float(y_human):.4f}")
        print(
            f"    Uncl.fees: X={float(fx_human):.4f}  Y={float(fy_human):.4f}"
        )

        # Convert reserves to SOL
        reserves_sol = Decimal("0")
        if mint_x == SOL_MINT:
            reserves_sol += total_x_raw / Decimal(LAMPORTS)
        else:
            amt = int(total_x_raw)
            if amt > 0:
                reserves_sol += Decimal(str(jupiter_to_sol(mint_x, amt)))

        if mint_y == SOL_MINT:
            reserves_sol += total_y_raw / Decimal(LAMPORTS)
        else:
            amt = int(total_y_raw)
            if amt > 0:
                reserves_sol += Decimal(str(jupiter_to_sol(mint_y, amt)))

        # Convert fees to SOL
        fees_sol = Decimal("0")
        if fee_x_raw > 0:
            if mint_x == SOL_MINT:
                fees_sol += Decimal(fee_x_raw) / Decimal(LAMPORTS)
            else:
                fees_sol += Decimal(str(jupiter_to_sol(mint_x, fee_x_raw)))

        if fee_y_raw > 0:
            if mint_y == SOL_MINT:
                fees_sol += Decimal(fee_y_raw) / Decimal(LAMPORTS)
            else:
                fees_sol += Decimal(str(jupiter_to_sol(mint_y, fee_y_raw)))

        print(f"    Reserves NAV: {float(reserves_sol):.4f} SOL")
        print(f"    Fees NAV:     {float(fees_sol):.4f} SOL")

        total_unclaimed_fee_sol += fees_sol
        position_results.append(
            {
                "address": addr,
                "pair": pool["name"],
                "reserves_sol": reserves_sol,
                "fees_sol": fees_sol,
            }
        )

    # ── Step 6: free SOL balance ─────────────────────────────────────────────
    print("\nStep 6: Free SOL balance...")
    bal = rpc_call(rpc_url, "getBalance", [wallet])
    free_sol = Decimal(bal["value"]) / Decimal(LAMPORTS)
    print(f"  Free SOL: {float(free_sol):.4f}")

    # ── Step 6b: idle SPL tokens ──────────────────────────────────────────────
    print("\nStep 6b: Idle SPL tokens...")
    ta_result = rpc_call(
        rpc_url,
        "getTokenAccountsByOwner",
        [wallet, {"programId": TOKEN_PROGRAM}, {"encoding": "jsonParsed"}],
    )
    idle_spl_sol = Decimal("0")
    token_accounts = ta_result.get("value", [])
    print(f"  SPL token accounts: {len(token_accounts)}")

    for ta in token_accounts:
        info = ta["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        amount_raw = int(info["tokenAmount"]["amount"])
        if amount_raw == 0:
            continue
        if mint == SOL_MINT:
            idle_spl_sol += Decimal(amount_raw) / Decimal(LAMPORTS)
            continue
        sol_val = jupiter_to_sol(mint, amount_raw)
        if sol_val > 0:
            ui = info["tokenAmount"].get("uiAmountString", str(amount_raw))
            print(f"    {mint[:8]}... = {ui} tokens → {sol_val:.4f} SOL")
            idle_spl_sol += Decimal(str(sol_val))

    # ── Step 7: summary ───────────────────────────────────────────────────────
    positions_nav_sol = sum(r["reserves_sol"] for r in position_results)
    internal_nav_sol = (
        positions_nav_sol + total_unclaimed_fee_sol + free_sol + idle_spl_sol
    )
    lpagent_nav = Decimal(str(args.lpagent_nav))
    diff_pct = abs(internal_nav_sol - lpagent_nav) / lpagent_nav * 100
    verdict = "PASS" if diff_pct <= 5 else "FAIL"

    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Wallet: {wallet}")
    print(f"Active positions found: {len(position_results)}")
    print()

    if position_results:
        hdr = f"{'Addr':<10} {'Pair':<20} {'Reserves(SOL)':>14} {'Fees(SOL)':>10}"
        print(hdr)
        print("-" * 58)
        for r in position_results:
            print(
                f"{r['address'][:8]:<10} {r['pair'][:20]:<20} "
                f"{float(r['reserves_sol']):>14.4f} "
                f"{float(r['fees_sol']):>10.4f}"
            )
        print()

    print(f"Active positions NAV:     {float(positions_nav_sol):>10.4f} SOL")
    print(
        f"Unclaimed fees:           {float(total_unclaimed_fee_sol):>10.4f} SOL"
    )
    print(f"Free SOL:                 {float(free_sol):>10.4f} SOL")
    print(f"Idle SPL tokens:          {float(idle_spl_sol):>10.4f} SOL")
    print("─" * 46)
    print(f"internal_nav_sol:         {float(internal_nav_sol):>10.4f} SOL")
    nav_line = f"{float(lpagent_nav):>10.4f} SOL   (user-supplied)"
    print(f"lpagent_nav_sol:          {nav_line}")
    print(f"diff_pct:                 {float(diff_pct):>9.1f}%")
    print(f"verdict:                  {verdict:>10}  (threshold 5%)")
    print()

    if verdict == "PASS":
        print("DECISION: GO — implement sub-project E.")
    else:
        items = [
            ("Active positions", float(positions_nav_sol)),
            ("Unclaimed fees", float(total_unclaimed_fee_sol)),
            ("Free SOL", float(free_sol)),
            ("Idle SPL", float(idle_spl_sol)),
        ]
        largest = max(items, key=lambda x: x[1])
        print(f"DECISION: KILL — diff_pct={float(diff_pct):.1f}% > 5% threshold.")
        print(
            f"  Largest component: {largest[0]} = {largest[1]:.4f} SOL"
            " — investigate first."
        )

    print("=" * 60)


if __name__ == "__main__":
    main()
