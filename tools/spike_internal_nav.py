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
import time
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
JUPITER_DELAY = 0.15   # throttle between Jupiter quote calls (seconds)

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
POS_LIQ_SHARES = 72    # liq_shares data start (N u128 values, no prefix)
POSV2_FIXED = 8120
POSBIN_SIZE = 112
# fee_data_off = 72 + N*64 (computed dynamically in decode_position)
# lower_bin_off = 72 + N*112 (computed dynamically)
# Within each FeeInfo (48 bytes): u128(16) + u128(16) + u64(8) + u64(8)
FEE_INFO_SIZE = 48
FEE_X_PENDING_OFF = 32  # offset within FeeInfo
FEE_Y_PENDING_OFF = 40  # offset within FeeInfo
REWARD_INFO_OFF = 1192
REWARD_INFO_SIZE = 48
REWARD_PENDING_OFF = 32  # offset within UserRewardInfo

# BinArray layout (from Meteora DLMM IDL):
#  8   discriminator
#  8   index           i64
#  1   version         u8
#  7   _padding        [u8; 7]
# 32   lb_pair         pubkey
# = 56 bytes header   (NOT 48 — version+padding add 8 bytes)
# Each Bin is a bytemuck C struct (144 bytes in the current IDL):
#  8   amount_x        u64
#  8   amount_y        u64
# 16   price           u128
# 16   liquidity_supply u128
#  8   fulfilled_order_amount_x u64
#  8   fulfilled_order_amount_y u64
#  8   limit_order_fee_ask_side u64
#  8   limit_order_fee_bid_side u64
# 16   fee_amount_x_per_token_stored u128
# 16   fee_amount_y_per_token_stored u128
#  8   open_order_amount u64
#  8   total_processing_order_amount u64
#  8   processed_order_remaining_amount u64
#  4   order_age u32
#  1   limit_order_ask_side u8
#  3   _padding_1 [u8; 3]
BA_HEADER = 56
BIN_SIZE = 144
BIN_AMOUNT_X = 0     # u64 within bin
BIN_AMOUNT_Y = 8     # u64 within bin
BIN_LIQ_SUPPLY = 32  # u128 within bin (lo+hi)


# ─── RPC helpers ─────────────────────────────────────────────────────────────

def rpc_call(url: str, method: str, params: list, _retries: int = 3) -> dict:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode()
    for attempt in range(_retries):
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            if "error" in data:
                raise RuntimeError(f"RPC {method} error: {data['error']}")
            return data["result"]
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < _retries - 1:
                wait = 2 ** attempt
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"RPC {method} failed after {_retries} retries")


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
    # PositionV2 layout (MAX_BIN_PER_POSITION=70, always fixed):
    #   8  discriminator
    #  32  lb_pair          → offset 8
    #  32  owner            → offset 40
    #  N*16 liq_shares      → offset 72      (N=70, 1120B)
    #  N*48 reward_infos    → offset 1192    (3360B)
    #  N*48 fee_infos       → offset 4552    (3360B)
    #   4  lower_bin_id     → offset 7912
    #   4  upper_bin_id     → offset 7916
    #  = 7920 bytes baseline; larger accounts have trailing limit-order data
    N = 70  # MAX_BIN_PER_POSITION — constant in Meteora DLMM
    FEE_DATA_OFF = 4552   # 72 + 70*64
    LOWER_BIN_OFF = 7912  # 72 + 70*112

    raw_len = len(data)
    if raw_len < POSV2_FIXED:
        raise ValueError(
            f"PositionV2 too short: {raw_len} bytes (need >= {POSV2_FIXED})"
        )

    lb_pair_bytes = data[POS_LB_PAIR : POS_LB_PAIR + 32]
    lb_pair = base58.b58encode(lb_pair_bytes).decode()

    lower_bin_id = struct.unpack_from("<i", data, LOWER_BIN_OFF)[0]
    upper_bin_id = struct.unpack_from("<i", data, LOWER_BIN_OFF + 4)[0]
    active_width = upper_bin_id - lower_bin_id + 1

    liq_shares = []
    for j in range(N):
        lo, hi = struct.unpack_from("<QQ", data, POS_LIQ_SHARES + j * 16)
        liq_shares.append(lo + (hi << 64))

    fee_x_pending = 0
    fee_y_pending = 0
    reward0_pending = 0
    reward1_pending = 0
    for j in range(N):
        base = REWARD_INFO_OFF + j * REWARD_INFO_SIZE
        reward0_pending += struct.unpack_from(
            "<Q", data, base + REWARD_PENDING_OFF
        )[0]
        reward1_pending += struct.unpack_from(
            "<Q", data, base + REWARD_PENDING_OFF + 8
        )[0]

    for j in range(N):
        base = FEE_DATA_OFF + j * FEE_INFO_SIZE
        fee_x_pending += struct.unpack_from(
            "<Q", data, base + FEE_X_PENDING_OFF
        )[0]
        fee_y_pending += struct.unpack_from(
            "<Q", data, base + FEE_Y_PENDING_OFF
        )[0]

    ext_liq_shares = []
    ext_fee_x_pending = 0
    ext_fee_y_pending = 0
    ext_reward0_pending = 0
    ext_reward1_pending = 0
    ext_count = 0
    if raw_len > POSV2_FIXED:
        ext_count = (raw_len - POSV2_FIXED) // POSBIN_SIZE
        for j in range(ext_count):
            base = POSV2_FIXED + j * POSBIN_SIZE
            lo, hi = struct.unpack_from("<QQ", data, base)
            ext_liq_shares.append(lo + (hi << 64))
            ext_reward0_pending += struct.unpack_from("<Q", data, base + 48)[0]
            ext_reward1_pending += struct.unpack_from("<Q", data, base + 56)[0]
            ext_fee_x_pending += struct.unpack_from("<Q", data, base + 96)[0]
            ext_fee_y_pending += struct.unpack_from("<Q", data, base + 104)[0]

    fee_x_pending += ext_fee_x_pending
    fee_y_pending += ext_fee_y_pending
    reward0_pending += ext_reward0_pending
    reward1_pending += ext_reward1_pending

    return {
        "lb_pair": lb_pair,
        "lb_pair_bytes": lb_pair_bytes,
        "lower_bin_id": lower_bin_id,
        "upper_bin_id": upper_bin_id,
        "width": active_width,
        "n_slots": N,
        "liquidity_shares": liq_shares,
        "ext_liq_shares": ext_liq_shares,
        "ext_count": ext_count,
        "ext_fee_x_raw": ext_fee_x_pending,
        "ext_fee_y_raw": ext_fee_y_pending,
        "ext_reward0_raw": ext_reward0_pending,
        "ext_reward1_raw": ext_reward1_pending,
        "fee_x_pending_raw": fee_x_pending,
        "fee_y_pending_raw": fee_y_pending,
        "reward0_raw": reward0_pending,
        "reward1_raw": reward1_pending,
    }


# ─── Step 3: fetch BinArrays ──────────────────────────────────────────────────

def bin_array_address(lb_pair_pk: Pubkey, array_idx: int) -> Pubkey:
    # Seeds match Meteora TS SDK: [b"bin_array", lb_pair, i64_le]
    seeds = [b"bin_array", bytes(lb_pair_pk), struct.pack("<q", array_idx)]
    addr, _ = Pubkey.find_program_address(seeds, METEORA_PROGRAM)
    return addr


def decode_bin_array(
    data: bytes, array_idx: int, expected_lb_pair: Optional[bytes] = None
) -> dict[int, dict]:
    """Decode BinArray → dict of bin_id → {amount_x, amount_y, liq_supply}."""
    # Verify lb_pair field to confirm BA_HEADER is correct.
    # With BA_HEADER=56: lb_pair at bytes 24-55 (after disc+index+ver+pad)
    # With BA_HEADER=48: lb_pair at bytes 16-47 (after disc+index, no ver/pad)
    if expected_lb_pair and len(data) >= 56:
        lbp_56 = data[24:56]
        lbp_48 = data[16:48]
        if lbp_56 == expected_lb_pair:
            header = 56
        elif lbp_48 == expected_lb_pair:
            header = 48
            print("    [debug] BA_HEADER=48 matches! (no version+padding)")
        else:
            header = BA_HEADER  # fallback
            print(
                f"    [debug] lb_pair mismatch: "
                f"h56={base58.b58encode(lbp_56).decode()[:8]} "
                f"h48={base58.b58encode(lbp_48).decode()[:8]}"
            )
    else:
        header = BA_HEADER

    payload_len = len(data) - header
    if payload_len % 70 == 0 and payload_len // 70 in (112, 144):
        bin_size = payload_len // 70
    else:
        bin_size = BIN_SIZE
        expected_len = header + 70 * bin_size
        if len(data) < expected_len:
            raise ValueError(
                f"BinArray too short: {len(data)} bytes "
                f"(need {expected_len} for {bin_size}-byte bins)"
            )

    bins = {}
    base_bin_id = array_idx * 70
    for slot in range(70):
        off = header + slot * bin_size
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


# ─── Meteora pool API + on-chain LbPair fallback ─────────────────────────────

SOL_MINT_BYTES = base58.b58decode(SOL_MINT)


def _lbpair_mints_onchain(
    rpc_url: str, lb_pair: str
) -> tuple[Optional[str], Optional[str]]:
    """Scan LbPair account bytes for token mint pubkeys.

    Searches for the SOL_MINT pattern at every byte offset. When found,
    the adjacent 32-byte block is the other mint. Works for any SOL-paired
    pool regardless of whether it is indexed by the Meteora REST API.
    """
    try:
        result = rpc_call(
            rpc_url, "getAccountInfo", [lb_pair, {"encoding": "base64"}]
        )
        if not result["value"]:
            return None, None
        data = base64.b64decode(result["value"]["data"][0])
    except Exception as e:
        print(f"    WARN: on-chain LbPair fetch failed: {e}")
        return None, None

    sol = SOL_MINT_BYTES
    for off in range(8, len(data) - 31):
        if data[off : off + 32] == sol:
            # SOL found as X-side mint
            if off + 64 <= len(data):
                y = data[off + 32 : off + 64]
                if any(b != 0 for b in y):
                    return SOL_MINT, base58.b58encode(y).decode()
        if off >= 32 and data[off : off + 32] == sol:
            # (already handled above; this block for Y-side)
            pass
        if off + 32 < len(data) and data[off + 32 : off + 64] == sol:
            # SOL found as Y-side mint
            x = data[off : off + 32]
            if any(b != 0 for b in x):
                return base58.b58encode(x).decode(), SOL_MINT
    return None, None


def get_pool_mints(rpc_url: str, lb_pair: str) -> dict:
    """Get token_x/token_y mint addresses: Meteora API first, on-chain fallback."""
    for url in [
        f"https://dlmm-api.meteora.ag/pair/{lb_pair}",
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
        except Exception:
            pass

    # On-chain fallback: scan LbPair account for SOL_MINT bytes
    mint_x, mint_y = _lbpair_mints_onchain(rpc_url, lb_pair)
    if mint_x and mint_y:
        return {"mint_x": mint_x, "mint_y": mint_y, "name": "?/?"}

    return {"mint_x": None, "mint_y": None, "name": "?/?"}


_reward_mints_cache: dict[str, list[Optional[str]]] = {}

def _first_present(data: dict, keys: tuple[str, ...]) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if value:
            return value
    return None


def _reward_mint_from_info(info: object) -> Optional[str]:
    if not isinstance(info, dict):
        return None
    return _first_present(
        info,
        (
            "mint",
            "reward_mint",
            "rewardMint",
            "reward_mint_address",
            "rewardMintAddress",
            "token_mint",
            "tokenMint",
        ),
    )


def get_reward_mints(rpc_url: str, lb_pair_str: str) -> list[Optional[str]]:
    """Get reward token mints from Meteora API, or [None, None] if unknown."""
    del rpc_url  # Reserved for a future on-chain fallback; API-only for this spike.
    if lb_pair_str in _reward_mints_cache:
        return _reward_mints_cache[lb_pair_str]

    reward_mints: list[Optional[str]] = [None, None]
    try:
        data = http_get(f"https://dlmm-api.meteora.ag/pair/{lb_pair_str}")
        one_based_0 = _first_present(data, ("reward_mint_1", "rewardMint1"))
        one_based_1 = _first_present(data, ("reward_mint_2", "rewardMint2"))
        zero_based_0 = _first_present(data, ("reward_mint_0", "rewardMint0"))
        zero_based_1 = _first_present(data, ("reward_mint_1", "rewardMint1"))
        if one_based_0 or one_based_1:
            reward_mints = [one_based_0, one_based_1]
        elif zero_based_0 or zero_based_1:
            reward_mints = [zero_based_0, zero_based_1]
        else:
            reward_mints[0] = _first_present(
                data, ("reward_mint_x", "rewardMintX")
            )
            reward_mints[1] = _first_present(
                data, ("reward_mint_y", "rewardMintY")
            )

        infos = (
            data.get("reward_infos")
            or data.get("rewardInfos")
            or data.get("rewards")
            or []
        )
        if isinstance(infos, list):
            if len(infos) > 0:
                reward_mints[0] = reward_mints[0] or _reward_mint_from_info(
                    infos[0]
                )
            if len(infos) > 1:
                reward_mints[1] = reward_mints[1] or _reward_mint_from_info(
                    infos[1]
                )
    except Exception as e:
        print(f"    WARN: reward mints API lookup failed: {e}")

    if not reward_mints[0] and not reward_mints[1]:
        print("    WARN: reward mints unknown")

    _reward_mints_cache[lb_pair_str] = reward_mints
    return reward_mints


# ─── Token decimals ───────────────────────────────────────────────────────────

_decimals_cache: dict[str, int] = {}

def get_decimals(rpc_url: str, mint: str) -> int:
    """Fetch token decimals from SPL mint account (offset 44)."""
    if mint == SOL_MINT:
        return 9
    if mint in _decimals_cache:
        return _decimals_cache[mint]
    result = rpc_call(rpc_url, "getAccountInfo", [mint, {"encoding": "base64"}])
    if result["value"] is None:
        print(f"    WARN: mint {mint[:8]}... not found, assuming 6")
        _decimals_cache[mint] = 6
        return 6
    data = base64.b64decode(result["value"]["data"][0])
    dec = data[44]
    _decimals_cache[mint] = dec
    return dec


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
    _retries = 4
    for attempt in range(_retries):
        time.sleep(JUPITER_DELAY)
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
                return 0.0
            if e.code == 429 and attempt < _retries - 1:
                wait = 2 ** attempt
                print(f"    WARN: Jupiter 429, retry in {wait}s...")
                time.sleep(wait)
                continue
            print(f"    WARN: Jupiter HTTP {e.code} for {mint[:8]}..., skipping")
            return 0.0
        except Exception as e:
            print(f"    WARN: Jupiter error for {mint[:8]}...: {e}")
            return 0.0
    return 0.0


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Spike: compute portfolio NAV from on-chain data"
    )
    parser.add_argument(
        "--lpagent-nav",
        type=float,
        default=None,
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
                f"lower={lower} upper={upper} width={pos['width']} "
                f"n_slots={pos['n_slots']} ext_count={pos['ext_count']} "
                f"raw_len={len(raw)}"
            )
        except Exception as e:
            print(f"  KILL candidate: decode failed for {addr}: {e}")

    # Filter empty/closed positions (all liq_shares == 0)
    active = [
        p
        for p in positions
        if any(s > 0 for s in p["liquidity_shares"])
        or any(s > 0 for s in p["ext_liq_shares"])
    ]
    empty = len(positions) - len(active)
    if empty:
        print(f"  Skipped {empty} position(s) with zero liquidity (closed/empty)")
    positions = active

    # AC-2 validation on active positions only
    for pos in positions:
        lid = pos["lower_bin_id"]
        assert -10_000 <= lid <= 10_000, f"lower_bin_id={lid} out of range"
        w = pos["width"]
        assert 1 <= w <= 500, f"width={w} suspicious"
    n = len(positions)
    print(f"  AC-2 PASS: {n} active position(s) with sensible fields")

    # ── Steps 3-5: BinArrays → bin math → SOL conversion ────────────────────
    print("\nSteps 3-5: BinArrays, bin math, Jupiter conversion...")

    position_results = []
    total_unclaimed_fee_sol = Decimal("0")
    total_unclaimed_rewards_sol = Decimal("0")

    for pos in positions:
        addr = pos["address"]
        lb_pair_str = pos["lb_pair"]
        lb_pair_pk = Pubkey.from_bytes(pos["lb_pair_bytes"])
        lower = pos["lower_bin_id"]
        upper = pos["upper_bin_id"]

        print(f"\n  Position {addr[:8]}...")

        # Pool mints
        pool = get_pool_mints(rpc_url, lb_pair_str)
        mint_x = pool["mint_x"]
        mint_y = pool["mint_y"]
        print(f"    Pool: {pool['name']}")
        if not mint_x or not mint_y:
            print("    WARN: could not determine token mints — skipping position")
            position_results.append(
                {
                    "address": addr,
                    "pair": "?/?",
                    "reserves_sol": Decimal("0"),
                    "fees_sol": Decimal("0"),
                    "rewards_sol": Decimal("0"),
                }
            )
            continue
        print(f"    mintX={mint_x[:8]}...  mintY={mint_y[:8]}...")

        dec_x = get_decimals(rpc_url, mint_x)
        dec_y = get_decimals(rpc_url, mint_y)
        print(f"    Decimals: X={dec_x} Y={dec_y}")

        # Required BinArrays
        fixed_width = min(pos["width"], pos["n_slots"])
        position_bin_ids = [lower + i for i in range(fixed_width)]
        position_bin_ids.extend(
            lower + pos["n_slots"] + j for j in range(pos["ext_count"])
        )
        required_arrays = sorted({bin_id // 70 for bin_id in position_bin_ids})
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
                bin_arrays[idx] = decode_bin_array(
                    ba_raw, idx, pos["lb_pair_bytes"]
                )
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
                    "rewards_sol": Decimal("0"),
                }
            )
            continue

        n_ok = len(bin_arrays)
        n_needed = len(required_arrays)
        print(f"    AC-3 PASS: {n_ok}/{n_needed} BinArray(s) fetched")

        # Bin math — iterate active bins [lower, upper]
        # liq_shares[i] corresponds to bin lower+i (i=0..active_width-1)
        active_width = pos["width"]
        n_slots = pos["n_slots"]
        if active_width > n_slots:
            print(
                f"    WARN: active_width={active_width} > n_slots={n_slots}"
                " — capping iteration"
            )
        iter_width = min(active_width, n_slots)

        total_x_raw = Decimal(0)
        total_y_raw = Decimal(0)

        position_bins = [
            (lower + bin_offset, pos["liquidity_shares"][bin_offset])
            for bin_offset in range(iter_width)
        ]
        position_bins.extend(
            (lower + n_slots + j, liq_share)
            for j, liq_share in enumerate(pos["ext_liq_shares"])
        )

        for bin_id, liq_share in position_bins:
            array_idx = bin_id // 70
            if array_idx not in bin_arrays:
                continue
            bd = bin_arrays[array_idx].get(bin_id)
            if bd is None:
                continue

            liq_supply = bd["liquidity_supply"]

            if liq_supply == 0 or liq_share == 0:
                continue

            frac = Decimal(liq_share) / Decimal(liq_supply)
            if frac > Decimal("1.01"):
                # liq_share > liq_supply: impossible in correct data
                # signals a decode error (wrong BinArray PDA or BA_HEADER)
                print(
                    f"    WARN: bin {bin_id} frac={float(frac):.2f} "
                    f"(liq_share={liq_share} liq_supply={liq_supply})"
                    " — decode error, skipping bin"
                )
                continue
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

        # Convert reward pendings to SOL
        reward_mints = get_reward_mints(rpc_url, lb_pair_str)
        reward_raws = [pos["reward0_raw"], pos["reward1_raw"]]
        rewards_sol = Decimal("0")
        reward_parts = []
        print(
            f"    Reward mints: R0="
            f"{reward_mints[0][:8] + '...' if reward_mints[0] else '?'} "
            f"R1={reward_mints[1][:8] + '...' if reward_mints[1] else '?'}"
        )
        for idx, (mint, amount_raw) in enumerate(zip(reward_mints, reward_raws)):
            if amount_raw <= 0:
                continue
            if not mint:
                print(
                    f"    WARN: reward{idx}_raw={amount_raw} but reward mint unknown"
                )
                continue
            sol_val = Decimal(str(jupiter_to_sol(mint, amount_raw)))
            rewards_sol += sol_val
            dec = get_decimals(rpc_url, mint)
            amount_human = Decimal(amount_raw) / Decimal(10 ** dec)
            reward_parts.append(
                f"R{idx}={float(amount_human):.4f} {mint[:8]}..."
                f" ({float(sol_val):.4f} SOL)"
            )
        if reward_parts:
            print(f"    Rewards:    {'; '.join(reward_parts)}")
        else:
            print("    Rewards:    none")

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
        print(f"    Rewards NAV:  {float(rewards_sol):.4f} SOL")

        total_unclaimed_fee_sol += fees_sol
        total_unclaimed_rewards_sol += rewards_sol
        position_results.append(
            {
                "address": addr,
                "pair": pool["name"],
                "reserves_sol": reserves_sol,
                "fees_sol": fees_sol,
                "rewards_sol": rewards_sol,
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
            print(f"    {mint[:8]}... = {ui} tokens = {sol_val:.4f} SOL")
            idle_spl_sol += Decimal(str(sol_val))

    # ── Step 7: summary ───────────────────────────────────────────────────────
    positions_nav_sol = sum(r["reserves_sol"] for r in position_results)
    internal_nav_sol = (
        positions_nav_sol
        + total_unclaimed_fee_sol
        + total_unclaimed_rewards_sol
        + free_sol
        + idle_spl_sol
    )
    lpagent_nav = (
        Decimal(str(args.lpagent_nav)) if args.lpagent_nav is not None else None
    )
    diff_pct = (
        abs(internal_nav_sol - lpagent_nav) / lpagent_nav * 100
        if lpagent_nav is not None and lpagent_nav != 0
        else None
    )
    verdict = "PASS" if diff_pct is not None and diff_pct <= 5 else "FAIL"

    print()
    print("=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Wallet: {wallet}")
    print(f"Active positions found: {len(position_results)}")
    print()

    if position_results:
        hdr = (
            f"{'Addr':<10} {'Pair':<20} {'Reserves(SOL)':>14} "
            f"{'Fees(SOL)':>10} {'Rewards(SOL)':>13}"
        )
        print(hdr)
        print("-" * 72)
        for r in position_results:
            print(
                f"{r['address'][:8]:<10} {r['pair'][:20]:<20} "
                f"{float(r['reserves_sol']):>14.4f} "
                f"{float(r['fees_sol']):>10.4f} "
                f"{float(r['rewards_sol']):>13.4f}"
            )
        print()

    print(f"Active positions NAV:     {float(positions_nav_sol):>10.4f} SOL")
    print(
        f"Unclaimed fees:           {float(total_unclaimed_fee_sol):>10.4f} SOL"
    )
    print(
        f"Unclaimed rewards:        {float(total_unclaimed_rewards_sol):>10.4f} SOL"
    )
    print(f"Free SOL:                 {float(free_sol):>10.4f} SOL")
    print(f"Idle SPL tokens:          {float(idle_spl_sol):>10.4f} SOL")
    print("-" * 46)
    print(f"internal_nav_sol:         {float(internal_nav_sol):>10.4f} SOL")
    if lpagent_nav is not None:
        nav_line = f"{float(lpagent_nav):>10.4f} SOL   (user-supplied)"
        print(f"lpagent_nav_sol:          {nav_line}")
        print(f"diff_pct:                 {float(diff_pct):>9.1f}%")
        print(f"verdict:                  {verdict:>10}  (threshold 5%)")
    else:
        print("lpagent_nav_sol:             n/a       (not supplied)")
        print("diff_pct:                    n/a")
        print("verdict:                     n/a")
    print()

    if lpagent_nav is None:
        print("DECISION: n/a -- rerun with --lpagent-nav for portfolio comparison.")
    elif verdict == "PASS":
        print("DECISION: GO — implement sub-project E.")
    else:
        items = [
            ("Active positions", float(positions_nav_sol)),
            ("Unclaimed fees", float(total_unclaimed_fee_sol)),
            ("Unclaimed rewards", float(total_unclaimed_rewards_sol)),
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
