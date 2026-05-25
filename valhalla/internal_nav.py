"""Compute portfolio NAV directly from on-chain Solana state."""

from __future__ import annotations

import base64
import json
import logging
import struct
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional, cast

import base58
from solders.pubkey import Pubkey


@dataclass
class NavResult:
    wallet: str
    timestamp: datetime
    positions_nav_sol: Decimal
    fees_sol: Decimal
    rewards_sol: Decimal
    free_sol: Decimal
    idle_spl_sol: Decimal
    total_nav_sol: Decimal
    n_positions: int
    degraded: bool
    degraded_mints: list[str] = field(default_factory=list)


# Verified constants - DO NOT derive from Meteora IDL docs.
# Source: tools/spike_internal_nav.py (2026-05-24, mainnet, 0.014% diff)
METEORA_PROGRAM = Pubkey.from_string("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo")
POSITION_V2_DISC = bytes([117, 176, 212, 199, 245, 180, 133, 182])
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
LAMPORTS = 1_000_000_000
JUPITER_DELAY = 0.15

# PositionV2 layout constants
POS_LB_PAIR = 8
POS_OWNER = 40
POS_LIQ_SHARES = 72
N_BINS = 70
POSV2_FIXED = 8120
POSBIN_SIZE = 112
REWARD_INFO_OFF = 1192
REWARD_INFO_SIZE = 48
REWARD_PENDING_OFF = 32
FEE_DATA_OFF = 4552
FEE_INFO_SIZE = 48
FEE_X_PENDING_OFF = 32
FEE_Y_PENDING_OFF = 40

# BinArray layout constants
BA_HEADER = 56
BIN_SIZE = 144
BIN_AMOUNT_X = 0
BIN_AMOUNT_Y = 8
BIN_LIQ_SUPPLY = 32

SOL_MINT_BYTES = base58.b58decode(SOL_MINT)
_ZERO = Decimal("0")
_decimals_cache: dict[str, int] = {}
_reward_mints_cache: dict[str, list[Optional[str]]] = {}


class _IdleJupiterWarningFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return not (
            record.levelno == logging.WARNING
            and (
                message.startswith("No Jupiter route for ")
                or message.startswith("Jupiter HTTP ")
                or message.startswith("Jupiter error for ")
            )
        )


def compute_nav(rpc_url: str, wallet: str) -> NavResult:
    """Compute portfolio NAV from on-chain Solana state."""
    degraded_mints: list[str] = []

    pos_addrs = _get_position_addresses(rpc_url, wallet)
    position_data = _fetch_accounts(rpc_url, pos_addrs)
    positions: list[dict[str, Any]] = []
    for addr, raw in zip(pos_addrs, position_data):
        if raw is None:
            logging.warning("Position account %s missing, skipping", addr)
            _add_degraded(degraded_mints, addr)
            continue
        try:
            pos = _decode_position(raw)
        except Exception as exc:
            logging.warning("Position account %s decode failed: %s", addr, exc)
            _add_degraded(degraded_mints, addr)
            continue
        pos["address"] = addr
        if any(s > 0 for s in pos["liquidity_shares"]) or any(
            s > 0 for s in pos["ext_liq_shares"]
        ):
            positions.append(pos)

    positions_nav_sol = _ZERO
    fees_sol = _ZERO
    rewards_sol = _ZERO
    for pos in positions:
        pos_nav, pos_fees, pos_rewards = _compute_position_nav(
            rpc_url, pos, degraded_mints
        )
        positions_nav_sol += pos_nav
        fees_sol += pos_fees
        rewards_sol += pos_rewards

    balance = _rpc_call(rpc_url, "getBalance", [wallet])
    free_sol = Decimal(int(balance["value"])) / Decimal(LAMPORTS)
    if not pos_addrs and free_sol == 0:
        raise RuntimeError(
            "zero NAV result: 0 positions and 0 free SOL - RPC failure suspected"
        )

    idle_spl_sol = _compute_idle_spl_sol(rpc_url, wallet, degraded_mints)
    total_nav_sol = positions_nav_sol + fees_sol + rewards_sol + free_sol + idle_spl_sol
    return NavResult(
        wallet=wallet,
        timestamp=datetime.now(timezone.utc),
        positions_nav_sol=positions_nav_sol,
        fees_sol=fees_sol,
        rewards_sol=rewards_sol,
        free_sol=free_sol,
        idle_spl_sol=idle_spl_sol,
        total_nav_sol=total_nav_sol,
        n_positions=len(positions),
        degraded=bool(degraded_mints),
        degraded_mints=degraded_mints,
    )


def _compute_position_nav(
    rpc_url: str, pos: dict[str, Any], degraded_mints: list[str]
) -> tuple[Decimal, Decimal, Decimal]:
    lb_pair_str = str(pos["lb_pair"])
    pool = _get_pool_mints(rpc_url, lb_pair_str)
    mint_x = pool.get("mint_x")
    mint_y = pool.get("mint_y")
    if not mint_x or not mint_y:
        logging.warning("Could not determine token mints for %s", lb_pair_str)
        return _ZERO, _ZERO, _ZERO

    lb_pair_pk = Pubkey.from_bytes(pos["lb_pair_bytes"])
    lower = int(pos["lower_bin_id"])
    active_width = int(pos["width"])
    n_slots = int(pos["n_slots"])
    fixed_width = min(active_width, n_slots)
    position_bin_ids = [lower + i for i in range(fixed_width)]
    position_bin_ids.extend(lower + n_slots + j for j in range(int(pos["ext_count"])))
    required_arrays = sorted({bin_id // N_BINS for bin_id in position_bin_ids})
    ba_addr_map = {
        idx: _bin_array_address(lb_pair_pk, idx) for idx in required_arrays
    }
    ba_raw_list = _fetch_accounts(rpc_url, [str(ba_addr_map[idx]) for idx in required_arrays])

    bin_arrays: dict[int, dict[int, dict[str, int]]] = {}
    for idx, raw in zip(required_arrays, ba_raw_list):
        binarray_id = f"binarray:{lb_pair_str}:{idx}"
        if raw is None:
            logging.warning("BinArray idx=%s missing for position %s", idx, pos["address"])
            _add_degraded(degraded_mints, binarray_id)
            continue
        try:
            bin_arrays[idx] = _decode_bin_array(raw, idx, pos["lb_pair_bytes"])
        except Exception as exc:
            logging.warning("BinArray idx=%s decode failed: %s", idx, exc)
            _add_degraded(degraded_mints, binarray_id)

    position_bins = [
        (lower + bin_offset, int(pos["liquidity_shares"][bin_offset]))
        for bin_offset in range(fixed_width)
    ]
    position_bins.extend(
        (lower + n_slots + j, int(liq_share))
        for j, liq_share in enumerate(pos["ext_liq_shares"])
    )
    total_x_raw, total_y_raw = _accumulate_bin_reserves(position_bins, bin_arrays)

    positions_nav_sol = _convert_amount(rpc_url, mint_x, total_x_raw, degraded_mints)
    positions_nav_sol += _convert_amount(rpc_url, mint_y, total_y_raw, degraded_mints)

    fees_sol = _convert_amount(
        rpc_url, mint_x, Decimal(int(pos["fee_x_pending_raw"])), degraded_mints
    )
    fees_sol += _convert_amount(
        rpc_url, mint_y, Decimal(int(pos["fee_y_pending_raw"])), degraded_mints
    )

    rewards_sol = _ZERO
    reward_mints = _get_reward_mints(rpc_url, lb_pair_str)
    reward_raws = [int(pos["reward0_raw"]), int(pos["reward1_raw"])]
    for idx, (mint, amount_raw) in enumerate(zip(reward_mints, reward_raws)):
        if amount_raw <= 0:
            continue
        if not mint:
            logging.warning("reward%s raw=%s but reward mint unknown", idx, amount_raw)
            continue
        rewards_sol += _convert_amount(rpc_url, mint, Decimal(amount_raw), degraded_mints)

    return positions_nav_sol, fees_sol, rewards_sol


def _compute_idle_spl_sol(
    rpc_url: str, wallet: str, degraded_mints: list[str]
) -> Decimal:
    result = _rpc_call(
        rpc_url,
        "getTokenAccountsByOwner",
        [wallet, {"programId": TOKEN_PROGRAM}, {"encoding": "jsonParsed"}],
    )
    idle_spl_sol = _ZERO
    for token_account in result.get("value", []):
        info = token_account["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        amount_raw = int(info["tokenAmount"]["amount"])
        idle_spl_sol += _convert_idle_amount(rpc_url, mint, Decimal(amount_raw))
    return idle_spl_sol


def _convert_amount(
    rpc_url: str, mint: str, amount_raw: Decimal, degraded_mints: list[str]
) -> Decimal:
    del rpc_url
    if amount_raw <= 0:
        return _ZERO
    if mint == SOL_MINT:
        return amount_raw / Decimal(LAMPORTS)
    sol_value, degraded = _jupiter_to_sol(mint, int(amount_raw))
    if degraded:
        _add_degraded(degraded_mints, mint)
    return sol_value


def _convert_idle_amount(rpc_url: str, mint: str, amount_raw: Decimal) -> Decimal:
    del rpc_url
    if amount_raw <= 0:
        return _ZERO
    if mint == SOL_MINT:
        return amount_raw / Decimal(LAMPORTS)
    root_logger = logging.getLogger()
    warning_filter = _IdleJupiterWarningFilter()
    root_logger.addFilter(warning_filter)
    try:
        sol_value, degraded = _jupiter_to_sol(mint, int(amount_raw))
    finally:
        root_logger.removeFilter(warning_filter)
    if degraded:
        logging.debug("Idle SPL mint %s has no reliable Jupiter value; using 0", mint)
    return sol_value


def _add_degraded(degraded_mints: list[str], mint: str) -> None:
    if mint not in degraded_mints:
        degraded_mints.append(mint)


def _accumulate_bin_reserves(
    position_bins: list[tuple[int, int]],
    bin_arrays: dict[int, dict[int, dict[str, int]]],
) -> tuple[Decimal, Decimal]:
    total_x_raw = _ZERO
    total_y_raw = _ZERO
    for bin_id, liq_share in position_bins:
        bin_array = bin_arrays.get(bin_id // N_BINS)
        if not bin_array:
            continue
        bin_data = bin_array.get(bin_id)
        if not bin_data:
            continue
        liq_supply = int(bin_data["liquidity_supply"])
        if liq_supply == 0 or liq_share == 0:
            continue
        frac = Decimal(liq_share) / Decimal(liq_supply)
        if frac > Decimal("1.01"):
            logging.warning(
                "Skipping bin %s with impossible liquidity fraction %s", bin_id, frac
            )
            continue
        total_x_raw += frac * Decimal(int(bin_data["amount_x"]))
        total_y_raw += frac * Decimal(int(bin_data["amount_y"]))
    return total_x_raw, total_y_raw


def _rpc_call(url: str, method: str, params: list[Any], retries: int = 3) -> dict[str, Any]:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode()
    for attempt in range(retries):
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                data = json.loads(response.read())
            if "error" in data:
                raise RuntimeError(f"RPC {method} error: {data['error']}")
            return data["result"]
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise RuntimeError(f"RPC {method} failed: HTTP {exc.code}") from exc
    raise RuntimeError(f"RPC {method} failed after {retries} retries")


def _http_get(url: str) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "internal-nav/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        return json.loads(response.read())


def _fetch_accounts(rpc_url: str, pubkeys: list[str]) -> list[Optional[bytes]]:
    results: list[Optional[bytes]] = []
    for i in range(0, len(pubkeys), 100):
        chunk = pubkeys[i : i + 100]
        result = _rpc_call(rpc_url, "getMultipleAccounts", [chunk, {"encoding": "base64"}])
        for account in result["value"]:
            if account is None:
                results.append(None)
            else:
                results.append(base64.b64decode(account["data"][0]))
    return results


def _get_position_addresses(rpc_url: str, wallet: str) -> list[str]:
    disc_b58 = base58.b58encode(POSITION_V2_DISC).decode()
    result = _rpc_call(
        rpc_url,
        "getProgramAccounts",
        [
            str(METEORA_PROGRAM),
            {
                "encoding": "base64",
                "filters": [
                    {"memcmp": {"offset": 0, "bytes": disc_b58}},
                    {"memcmp": {"offset": POS_OWNER, "bytes": wallet}},
                ],
                "dataSlice": {"offset": 0, "length": 0},
            },
        ],
    )
    items = cast(list[dict[str, Any]], result)
    return [str(item["pubkey"]) for item in items]


def _decode_position(data: bytes) -> dict[str, Any]:
    raw_len = len(data)
    if raw_len < POSV2_FIXED:
        raise ValueError(f"PositionV2 too short: {raw_len} bytes")

    lb_pair_bytes = data[POS_LB_PAIR : POS_LB_PAIR + 32]
    lower_bin_id = struct.unpack_from("<i", data, 7912)[0]
    upper_bin_id = struct.unpack_from("<i", data, 7916)[0]
    liq_shares = []
    for j in range(N_BINS):
        lo, hi = struct.unpack_from("<QQ", data, POS_LIQ_SHARES + j * 16)
        liq_shares.append(lo + (hi << 64))

    fee_x_pending = 0
    fee_y_pending = 0
    reward0_pending = 0
    reward1_pending = 0
    for j in range(N_BINS):
        reward_base = REWARD_INFO_OFF + j * REWARD_INFO_SIZE
        reward0_pending += struct.unpack_from(
            "<Q", data, reward_base + REWARD_PENDING_OFF
        )[0]
        reward1_pending += struct.unpack_from(
            "<Q", data, reward_base + REWARD_PENDING_OFF + 8
        )[0]

        fee_base = FEE_DATA_OFF + j * FEE_INFO_SIZE
        fee_x_pending += struct.unpack_from("<Q", data, fee_base + FEE_X_PENDING_OFF)[0]
        fee_y_pending += struct.unpack_from("<Q", data, fee_base + FEE_Y_PENDING_OFF)[0]

    ext_liq_shares: list[int] = []
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

    return {
        "lb_pair": base58.b58encode(lb_pair_bytes).decode(),
        "lb_pair_bytes": lb_pair_bytes,
        "lower_bin_id": lower_bin_id,
        "upper_bin_id": upper_bin_id,
        "width": upper_bin_id - lower_bin_id + 1,
        "n_slots": N_BINS,
        "liquidity_shares": liq_shares,
        "ext_liq_shares": ext_liq_shares,
        "ext_count": ext_count,
        "ext_fee_x_raw": ext_fee_x_pending,
        "ext_fee_y_raw": ext_fee_y_pending,
        "ext_reward0_raw": ext_reward0_pending,
        "ext_reward1_raw": ext_reward1_pending,
        "fee_x_pending_raw": fee_x_pending + ext_fee_x_pending,
        "fee_y_pending_raw": fee_y_pending + ext_fee_y_pending,
        "reward0_raw": reward0_pending + ext_reward0_pending,
        "reward1_raw": reward1_pending + ext_reward1_pending,
    }


def _bin_array_address(lb_pair_pk: Pubkey, array_idx: int) -> Pubkey:
    seeds = [b"bin_array", bytes(lb_pair_pk), struct.pack("<q", array_idx)]
    address, _ = Pubkey.find_program_address(seeds, METEORA_PROGRAM)
    return address


def _decode_bin_array(
    data: bytes, array_idx: int, expected_lb_pair: Optional[bytes] = None
) -> dict[int, dict[str, int]]:
    header = BA_HEADER
    if expected_lb_pair and len(data) >= BA_HEADER:
        lb_pair_at_56 = data[24:56]
        lb_pair_at_48 = data[16:48]
        if lb_pair_at_56 == expected_lb_pair:
            header = 56
        elif lb_pair_at_48 == expected_lb_pair:
            header = 48
            logging.debug("BA_HEADER=48 matches for BinArray idx=%s", array_idx)
        else:
            logging.debug("BinArray idx=%s lb_pair header mismatch", array_idx)

    payload_len = len(data) - header
    if payload_len % N_BINS == 0 and payload_len // N_BINS in (112, 144):
        bin_size = payload_len // N_BINS
    else:
        bin_size = BIN_SIZE
        expected_len = header + N_BINS * bin_size
        if len(data) < expected_len:
            raise ValueError(f"BinArray too short: {len(data)} bytes")

    bins: dict[int, dict[str, int]] = {}
    base_bin_id = array_idx * N_BINS
    for slot in range(N_BINS):
        offset = header + slot * bin_size
        amount_x = struct.unpack_from("<Q", data, offset + BIN_AMOUNT_X)[0]
        amount_y = struct.unpack_from("<Q", data, offset + BIN_AMOUNT_Y)[0]
        lo, hi = struct.unpack_from("<QQ", data, offset + BIN_LIQ_SUPPLY)
        bins[base_bin_id + slot] = {
            "amount_x": amount_x,
            "amount_y": amount_y,
            "liquidity_supply": lo + (hi << 64),
        }
    return bins


def _get_pool_mints(rpc_url: str, lb_pair: str) -> dict[str, Optional[str]]:
    try:
        data = _http_get(f"https://dlmm-api.meteora.ag/pair/{lb_pair}")
        mint_x = data.get("mint_x") or data.get("token_x_mint") or data.get("tokenXMint")
        mint_y = data.get("mint_y") or data.get("token_y_mint") or data.get("tokenYMint")
        name = data.get("name", "?/?")
        if mint_x and mint_y:
            return {"mint_x": str(mint_x), "mint_y": str(mint_y), "name": str(name)}
    except Exception as exc:
        logging.debug("Meteora pair API lookup failed for %s: %s", lb_pair, exc)

    mint_x, mint_y = _lbpair_mints_onchain(rpc_url, lb_pair)
    return {"mint_x": mint_x, "mint_y": mint_y, "name": "?/?"}


def _lbpair_mints_onchain(
    rpc_url: str, lb_pair: str
) -> tuple[Optional[str], Optional[str]]:
    try:
        result = _rpc_call(rpc_url, "getAccountInfo", [lb_pair, {"encoding": "base64"}])
        if not result["value"]:
            return None, None
        data = base64.b64decode(result["value"]["data"][0])
    except Exception as exc:
        logging.warning("On-chain LbPair fetch failed for %s: %s", lb_pair, exc)
        return None, None

    for offset in range(8, len(data) - 31):
        if data[offset : offset + 32] == SOL_MINT_BYTES and offset + 64 <= len(data):
            mint_y = data[offset + 32 : offset + 64]
            if any(byte != 0 for byte in mint_y):
                return SOL_MINT, base58.b58encode(mint_y).decode()
        if offset + 64 <= len(data) and data[offset + 32 : offset + 64] == SOL_MINT_BYTES:
            mint_x = data[offset : offset + 32]
            if any(byte != 0 for byte in mint_x):
                return base58.b58encode(mint_x).decode(), SOL_MINT
    return None, None


def _first_present(data: dict[str, Any], keys: tuple[str, ...]) -> Optional[str]:
    for key in keys:
        value = data.get(key)
        if value:
            return str(value)
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


def _get_reward_mints(rpc_url: str, lb_pair: str) -> list[Optional[str]]:
    del rpc_url
    if lb_pair in _reward_mints_cache:
        return _reward_mints_cache[lb_pair]

    reward_mints: list[Optional[str]] = [None, None]
    try:
        data = _http_get(f"https://dlmm-api.meteora.ag/pair/{lb_pair}")
        one_based_0 = _first_present(data, ("reward_mint_1", "rewardMint1"))
        one_based_1 = _first_present(data, ("reward_mint_2", "rewardMint2"))
        zero_based_0 = _first_present(data, ("reward_mint_0", "rewardMint0"))
        zero_based_1 = _first_present(data, ("reward_mint_1", "rewardMint1"))
        if one_based_0 or one_based_1:
            reward_mints = [one_based_0, one_based_1]
        elif zero_based_0 or zero_based_1:
            reward_mints = [zero_based_0, zero_based_1]
        else:
            reward_mints[0] = _first_present(data, ("reward_mint_x", "rewardMintX"))
            reward_mints[1] = _first_present(data, ("reward_mint_y", "rewardMintY"))

        infos = data.get("reward_infos") or data.get("rewardInfos") or data.get("rewards") or []
        if isinstance(infos, list):
            if len(infos) > 0:
                reward_mints[0] = reward_mints[0] or _reward_mint_from_info(infos[0])
            if len(infos) > 1:
                reward_mints[1] = reward_mints[1] or _reward_mint_from_info(infos[1])
    except Exception as exc:
        logging.debug("Reward mints API lookup failed for %s: %s", lb_pair, exc)

    if not reward_mints[0] and not reward_mints[1]:
        logging.debug("Reward mints unknown for %s", lb_pair)
    _reward_mints_cache[lb_pair] = reward_mints
    return reward_mints


def _get_decimals(rpc_url: str, mint: str) -> int:
    if mint == SOL_MINT:
        return 9
    if mint in _decimals_cache:
        return _decimals_cache[mint]
    result = _rpc_call(rpc_url, "getAccountInfo", [mint, {"encoding": "base64"}])
    if result["value"] is None:
        logging.warning("Mint %s not found, assuming 6 decimals", mint)
        _decimals_cache[mint] = 6
        return 6
    data = base64.b64decode(result["value"]["data"][0])
    decimals = int(data[44])
    _decimals_cache[mint] = decimals
    return decimals


def _jupiter_to_sol(mint: str, amount_raw: int) -> tuple[Decimal, bool]:
    if mint == SOL_MINT:
        return Decimal(amount_raw) / Decimal(LAMPORTS), False
    if amount_raw <= 0:
        return _ZERO, False
    if amount_raw <= 100:
        return _ZERO, False

    url = (
        "https://api.jup.ag/swap/v1/quote"
        f"?inputMint={mint}&outputMint={SOL_MINT}"
        f"&amount={amount_raw}&slippageBps=50"
    )
    retries = 4
    for attempt in range(retries):
        time.sleep(JUPITER_DELAY)
        try:
            data = _http_get(url)
            return Decimal(int(data["outAmount"])) / Decimal(LAMPORTS), False
        except urllib.error.HTTPError as exc:
            body = exc.read().decode(errors="replace")
            no_route = any(
                marker in body
                for marker in (
                    "NO_ROUTES_FOUND",
                    "TOKEN_NOT_TRADABLE",
                    "COULD_NOT_FIND_ANY_ROUTE",
                )
            )
            if no_route:
                logging.warning("No Jupiter route for %s", mint)
                return _ZERO, True
            if exc.code == 429 and attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            logging.warning("Jupiter HTTP %s for %s", exc.code, mint)
            return _ZERO, True
        except Exception as exc:
            logging.warning("Jupiter error for %s: %s", mint, exc)
            return _ZERO, True
    return _ZERO, True
