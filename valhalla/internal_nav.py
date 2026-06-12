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
from pathlib import Path
from typing import Any, Callable, Optional, cast

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
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class _MintQuoteResult:
    value_sol: Decimal
    degraded: bool
    reason: str = ""
    warning: Optional[str] = None


class TransientPricingError(RuntimeError):
    """Raised when Jupiter pricing is temporarily unavailable after retries."""


# Verified constants - DO NOT derive from Meteora IDL docs.
# Source: tools/spike_internal_nav.py (2026-05-24, mainnet, 0.014% diff)
METEORA_PROGRAM = Pubkey.from_string("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo")
POSITION_V2_DISC = bytes([117, 176, 212, 199, 245, 180, 133, 182])
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
LAMPORTS = 1_000_000_000
JUPITER_DELAY = 0.15
U64_MAX = 2**64 - 1
JUPITER_REFERENCE_AMOUNT_RAW = 1_000_000_000
IMMATERIAL_NAV_THRESHOLD_SOL = Decimal("0.01")
IMMATERIAL_FALLBACK_SUM_THRESHOLD_SOL = Decimal("0.05")
NO_ROUTE_MARKERS = (
    "NO_ROUTES_FOUND",
    "TOKEN_NOT_TRADABLE",
    "COULD_NOT_FIND_ANY_ROUTE",
)

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
LBPAIR_MINT_X = 88
LBPAIR_MINT_Y = 120

SOL_MINT_BYTES = base58.b58decode(SOL_MINT)
_ZERO = Decimal("0")
_decimals_cache: dict[str, int] = {}
# Price cache: SOL per 1 raw token unit, populated per NAV run to avoid redundant Jupiter calls.
_jupiter_price_cache: dict[str, Decimal] = {}
_jupiter_skip_cache: Optional[set[str]] = None
_reward_mints_cache: dict[str, list[Optional[str]]] = {}
JUPITER_SKIP_CACHE_PATH = (
    Path(__file__).resolve().parents[1] / "output" / "internal_nav_skipped_mints.json"
)


ProgressCallback = Callable[[str], None]


def compute_nav(
    rpc_url: str, wallet: str, progress: Optional[ProgressCallback] = None
) -> NavResult:
    """Compute portfolio NAV from on-chain Solana state."""
    _jupiter_price_cache.clear()
    degraded_mints: list[str] = []
    started_at = time.perf_counter()

    def emit(message: str) -> None:
        if progress:
            elapsed = time.perf_counter() - started_at
            progress(f"{message} ({elapsed:.1f}s)")

    emit(f"starting NAV for {wallet[:8]}...")
    pos_addrs = _get_position_addresses(rpc_url, wallet)
    emit(f"found {len(pos_addrs)} Meteora position accounts")
    position_data = _fetch_accounts(rpc_url, pos_addrs)
    emit(f"fetched {len(position_data)} position accounts")
    positions: list[dict[str, Any]] = []
    warning_mints: list[str] = []
    for addr, raw in zip(pos_addrs, position_data):
        if raw is None:
            logging.warning("Position account %s missing, skipping", addr)
            _add_degraded(degraded_mints, addr)
            continue
        try:
            pos = _decode_position(raw)
        except TransientPricingError:
            raise
        except Exception as exc:
            logging.warning("Position account %s decode failed: %s", addr, exc)
            _add_degraded(degraded_mints, addr)
            continue
        pos["address"] = addr
        if any(s > 0 for s in pos["liquidity_shares"]) or any(
            s > 0 for s in pos["ext_liq_shares"]
        ):
            positions.append(pos)
    emit(f"decoded {len(positions)} active positions")

    positions_nav_sol = _ZERO
    fees_sol = _ZERO
    rewards_sol = _ZERO
    for idx, pos in enumerate(positions, start=1):
        logging.debug(
            "computing position %s/%s %s", idx, len(positions), pos["address"][:8]
        )
        pos_nav, pos_fees, pos_rewards = _compute_position_nav(
            rpc_url, pos, degraded_mints, warning_mints
        )
        positions_nav_sol += pos_nav
        fees_sol += pos_fees
        rewards_sol += pos_rewards
    emit("finished active position NAV")

    emit("fetching free SOL balance")
    balance = _rpc_call(rpc_url, "getBalance", [wallet])
    free_sol = Decimal(int(balance["value"])) / Decimal(LAMPORTS)
    if not pos_addrs and free_sol == 0:
        raise RuntimeError(
            "zero NAV result: 0 positions and 0 free SOL - RPC failure suspected"
        )

    emit("fetching idle SPL balances")
    idle_spl_sol = _compute_idle_spl_sol(
        rpc_url, wallet, degraded_mints, warning_mints, progress=emit
    )
    total_nav_sol = positions_nav_sol + fees_sol + rewards_sol + free_sol + idle_spl_sol
    emit(f"NAV total computed: {total_nav_sol:.6f} SOL")
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
        warnings=warning_mints,
    )


def _compute_position_nav(
    rpc_url: str,
    pos: dict[str, Any],
    degraded_mints: list[str],
    warning_mints: list[str],
) -> tuple[Decimal, Decimal, Decimal]:
    lb_pair_str = str(pos["lb_pair"])
    logging.debug("resolving pool mints for %s", lb_pair_str[:8])
    pool = _get_pool_mints(rpc_url, lb_pair_str)
    mint_x = pool.get("mint_x")
    mint_y = pool.get("mint_y")
    if not mint_x or not mint_y:
        logging.warning("Could not determine token mints for %s", lb_pair_str)
        _add_degraded(degraded_mints, f"lbpair-mints:{lb_pair_str}")
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
    logging.debug("fetching %s bin arrays", len(required_arrays))
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
        except TransientPricingError:
            raise
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

    positions_nav_sol = _convert_amount(
        rpc_url, mint_x, total_x_raw, degraded_mints, warning_mints
    )
    positions_nav_sol += _convert_amount(
        rpc_url, mint_y, total_y_raw, degraded_mints, warning_mints
    )

    fees_sol = _convert_amount(
        rpc_url,
        mint_x,
        Decimal(int(pos["fee_x_pending_raw"])),
        degraded_mints,
        warning_mints,
    )
    fees_sol += _convert_amount(
        rpc_url,
        mint_y,
        Decimal(int(pos["fee_y_pending_raw"])),
        degraded_mints,
        warning_mints,
    )

    rewards_sol = _ZERO
    logging.debug("resolving reward mints")
    reward_mints = _get_reward_mints(rpc_url, lb_pair_str)
    reward_raws = [int(pos["reward0_raw"]), int(pos["reward1_raw"])]
    for idx, (mint, amount_raw) in enumerate(zip(reward_mints, reward_raws)):
        if amount_raw <= 0:
            continue
        if not mint:
            marker = f"reward-mint:{lb_pair_str}:{idx}"
            warning = (
                f"reward{idx} raw={amount_raw} for {lb_pair_str} "
                "but reward mint unknown"
            )
            logging.warning(warning)
            _add_degraded(degraded_mints, marker)
            _add_warning(warning_mints, warning)
            continue
        rewards_sol += _value_mint_amount(
            mint,
            Decimal(amount_raw),
            degraded_mints,
            warning_mints,
            suppress_immaterial_warning=False,
        )

    return positions_nav_sol, fees_sol, rewards_sol


def _compute_idle_spl_sol(
    rpc_url: str,
    wallet: str,
    degraded_mints: list[str],
    warning_mints: list[str],
    progress: Optional[ProgressCallback] = None,
) -> Decimal:
    result = _rpc_call(
        rpc_url,
        "getTokenAccountsByOwner",
        [wallet, {"programId": TOKEN_PROGRAM}, {"encoding": "jsonParsed"}],
    )
    idle_spl_sol = _ZERO
    token_accounts = result.get("value", [])
    if progress:
        progress(f"idle SPL token accounts: {len(token_accounts)}")
    priced_accounts = 0
    for token_account in token_accounts:
        info = token_account["account"]["data"]["parsed"]["info"]
        mint = info["mint"]
        amount_raw = int(info["tokenAmount"]["amount"])
        if mint != SOL_MINT and amount_raw > 100:
            priced_accounts += 1
            logging.debug("pricing idle SPL %s: %s", priced_accounts, mint[:8])
        idle_spl_sol += _convert_idle_amount(
            rpc_url, mint, Decimal(amount_raw), degraded_mints, warning_mints
        )
    return idle_spl_sol


def _convert_amount(
    rpc_url: str,
    mint: str,
    amount_raw: Decimal,
    degraded_mints: list[str],
    warning_mints: Optional[list[str]] = None,
) -> Decimal:
    del rpc_url
    return _value_mint_amount(
        mint,
        amount_raw,
        degraded_mints,
        warning_mints if warning_mints is not None else [],
        suppress_immaterial_warning=False,
    )


def _convert_idle_amount(
    rpc_url: str,
    mint: str,
    amount_raw: Decimal,
    degraded_mints: Optional[list[str]] = None,
    warning_mints: Optional[list[str]] = None,
) -> Decimal:
    del rpc_url
    return _value_mint_amount(
        mint,
        amount_raw,
        degraded_mints if degraded_mints is not None else [],
        warning_mints if warning_mints is not None else [],
        suppress_immaterial_warning=True,
    )


def _value_mint_amount(
    mint: str,
    amount_raw: Decimal,
    degraded_mints: list[str],
    warnings: list[str],
    *,
    suppress_immaterial_warning: bool,
) -> Decimal:
    if mint == SOL_MINT:
        return amount_raw / Decimal(LAMPORTS)
    if amount_raw <= 0:
        return _ZERO

    amount_raw_int = int(amount_raw)
    logging.debug("pricing mint %s amount_raw=%s", mint[:8], amount_raw_int)

    if amount_raw_int > U64_MAX:
        warning = (
            f"Jupiter amount exceeds u64 for {mint} amount_raw={amount_raw_int}; "
            "likely decode artifact"
        )
        logging.warning(warning)
        _add_degraded(degraded_mints, mint)
        _add_warning(warnings, warning)
        return _ZERO

    quote = _quote_jupiter_to_sol(mint, amount_raw_int)
    if quote.reason == "reference-immaterial":
        _add_immaterial_fallback(warnings, mint, quote.value_sol)
        if _immaterial_fallback_total(warnings) >= IMMATERIAL_FALLBACK_SUM_THRESHOLD_SOL:
            _add_degraded(degraded_mints, "immaterial-sum")
        log = logging.debug if suppress_immaterial_warning else logging.warning
        log(
            "Jupiter quote failed for %s amount_raw=%s; reference quote estimates %s SOL",
            mint,
            amount_raw_int,
            quote.value_sol,
        )
    elif quote.warning:
        _add_warning(warnings, quote.warning)
        log = logging.debug if suppress_immaterial_warning and not quote.degraded else logging.warning
        log("%s", quote.warning)

    if quote.degraded:
        _add_degraded(degraded_mints, mint)
        if not quote.warning:
            _add_warning(
                warnings,
                f"degraded Jupiter valuation for {mint} amount_raw={amount_raw_int}",
            )
        return quote.value_sol

    return quote.value_sol


def _add_degraded(degraded_mints: list[str], mint: str) -> None:
    if mint not in degraded_mints:
        degraded_mints.append(mint)


def _add_warning(warnings: list[str], warning: str) -> None:
    if warning not in warnings:
        warnings.append(warning)


def _add_immaterial_fallback(warnings: list[str], mint: str, value_sol: Decimal) -> None:
    warnings.append(
        f"immaterial reference-priced mint {mint} value={value_sol.normalize()} SOL"
    )


def _immaterial_fallback_total(warnings: list[str]) -> Decimal:
    total = _ZERO
    for warning in warnings:
        if not warning.startswith("immaterial reference-priced mint "):
            continue
        try:
            value = warning.rsplit(" value=", 1)[1].removesuffix(" SOL")
            total += Decimal(value)
        except TransientPricingError:
            raise
        except Exception:
            continue
    return total


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
            if _is_transient_http_code(exc.code):
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise TransientPricingError(
                    f"RPC {method} failed: HTTP {exc.code}"
                ) from exc
            raise RuntimeError(f"RPC {method} failed: HTTP {exc.code}") from exc
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise TransientPricingError(
                f"RPC {method} transient error after {retries} retries: {exc}"
            ) from exc
    raise TransientPricingError(f"RPC {method} failed after {retries} retries")


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
    except TransientPricingError:
        raise
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
    except TransientPricingError:
        raise
    except Exception as exc:
        logging.warning("On-chain LbPair fetch failed for %s: %s", lb_pair, exc)
        return None, None

    if len(data) >= LBPAIR_MINT_Y + 32:
        mint_x = data[LBPAIR_MINT_X : LBPAIR_MINT_X + 32]
        mint_y = data[LBPAIR_MINT_Y : LBPAIR_MINT_Y + 32]
        if any(byte != 0 for byte in mint_x) and any(byte != 0 for byte in mint_y):
            return base58.b58encode(mint_x).decode(), base58.b58encode(mint_y).decode()

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
    except TransientPricingError:
        raise
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
    result = _quote_jupiter_to_sol(mint, amount_raw)
    return result.value_sol, result.degraded


def _quote_jupiter_to_sol(mint: str, amount_raw: int) -> _MintQuoteResult:
    if mint == SOL_MINT:
        return _MintQuoteResult(Decimal(amount_raw) / Decimal(LAMPORTS), False)
    if amount_raw <= 0:
        return _MintQuoteResult(_ZERO, False)
    # Reuse price from earlier in this same NAV run (SOL per 1 raw unit).
    if mint in _jupiter_price_cache:
        return _MintQuoteResult(
            _jupiter_price_cache[mint] * Decimal(amount_raw), False, "direct-cache"
        )
    if mint in _load_jupiter_skip_cache():
        return _MintQuoteResult(
            _ZERO, False, "no_route", f"no-route treated as 0: {mint}"
        )

    retries = 4
    for attempt in range(retries):
        time.sleep(JUPITER_DELAY)
        try:
            out_sol = _jupiter_quote_to_sol(mint, amount_raw)
            _jupiter_price_cache[mint] = out_sol / Decimal(amount_raw)
            return _MintQuoteResult(out_sol, False, "direct")
        except urllib.error.HTTPError as exc:
            if _is_transient_http_code(exc.code):
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise TransientPricingError(f"Jupiter HTTP {exc.code} for {mint}") from exc
            try:
                body = _read_http_error_body(exc)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as body_exc:
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise TransientPricingError(
                    f"Jupiter HTTP error body read failed for {mint}: {body_exc}"
                ) from body_exc
            if _is_jupiter_no_route(body):
                _cache_jupiter_no_route(mint)
                return _MintQuoteResult(
                    _ZERO, False, "no_route", f"no-route treated as 0: {mint}"
                )
            return _jupiter_reference_fallback_to_sol(mint, amount_raw)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise TransientPricingError(f"Jupiter transient error for {mint}: {exc}") from exc
        except TransientPricingError:
            raise
        except Exception as exc:
            return _jupiter_reference_fallback_to_sol(mint, amount_raw)
    raise TransientPricingError(f"Jupiter quote retries exhausted for {mint}")


def _jupiter_quote_to_sol(mint: str, amount_raw: int) -> Decimal:
    url = (
        "https://api.jup.ag/swap/v1/quote"
        f"?inputMint={mint}&outputMint={SOL_MINT}"
        f"&amount={amount_raw}&slippageBps=50"
    )
    data = _http_get(url)
    return Decimal(int(data["outAmount"])) / Decimal(LAMPORTS)


def _jupiter_reference_fallback_to_sol(mint: str, amount_raw: int) -> _MintQuoteResult:
    if amount_raw <= 0:
        return _MintQuoteResult(_ZERO, False)
    retries = 5
    for attempt in range(retries):
        time.sleep(JUPITER_DELAY)
        try:
            reference_sol = _jupiter_quote_to_sol(mint, JUPITER_REFERENCE_AMOUNT_RAW)
            break
        except urllib.error.HTTPError as exc:
            if _is_transient_http_code(exc.code):
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise TransientPricingError(
                    f"Jupiter reference HTTP {exc.code} for {mint}"
                ) from exc
            try:
                body = _read_http_error_body(exc)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as body_exc:
                if attempt < retries - 1:
                    time.sleep(2**attempt)
                    continue
                raise TransientPricingError(
                    f"Jupiter reference HTTP error body read failed for {mint}: {body_exc}"
                ) from body_exc
            if _is_jupiter_no_route(body):
                return _MintQuoteResult(
                    _ZERO, False, "no_route", f"no-route treated as 0: {mint}"
                )
            return _material_unpriceable(mint)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            if attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            raise TransientPricingError(
                f"Jupiter reference transient error for {mint}: {exc}"
            ) from exc
        except TransientPricingError:
            raise
        except Exception:
            return _material_unpriceable(mint)
    else:
        raise TransientPricingError(f"Jupiter reference retries exhausted for {mint}")

    price = reference_sol / Decimal(JUPITER_REFERENCE_AMOUNT_RAW)
    value = price * Decimal(amount_raw)
    if value >= IMMATERIAL_NAV_THRESHOLD_SOL:
        return _material_unpriceable(mint, value)
    return _MintQuoteResult(value, False, "reference-immaterial")


def _material_unpriceable(mint: str, estimate: Optional[Decimal] = None) -> _MintQuoteResult:
    suffix = f" estimated={estimate} SOL" if estimate is not None else ""
    return _MintQuoteResult(
        _ZERO, True, "material-unpriceable", f"material unpriceable mint {mint}{suffix}"
    )


def _is_jupiter_no_route(body: str) -> bool:
    return any(marker in body for marker in NO_ROUTE_MARKERS)


def _is_transient_http_code(code: int) -> bool:
    return code == 429 or 500 <= code < 600


def _read_http_error_body(exc: urllib.error.HTTPError) -> str:
    return exc.read().decode(errors="replace")


def _load_jupiter_skip_cache() -> set[str]:
    global _jupiter_skip_cache
    if _jupiter_skip_cache is not None:
        return _jupiter_skip_cache

    try:
        data = json.loads(JUPITER_SKIP_CACHE_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        _jupiter_skip_cache = set()
        return _jupiter_skip_cache
    except TransientPricingError:
        raise
    except Exception as exc:
        logging.warning("Failed to read Jupiter skip cache: %s", exc)
        _jupiter_skip_cache = set()
        return _jupiter_skip_cache

    if isinstance(data, dict):
        raw_mints = data.get("no_route_mints", [])
    elif isinstance(data, list):
        raw_mints = data
    else:
        raw_mints = []
    _jupiter_skip_cache = {
        str(mint) for mint in raw_mints if _is_probable_solana_mint(str(mint))
    }
    return _jupiter_skip_cache


def _cache_jupiter_no_route(mint: str) -> None:
    if not _is_probable_solana_mint(mint):
        return
    cache = _load_jupiter_skip_cache()
    if mint in cache:
        return
    cache.add(mint)
    payload = {
        "updated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "no_route_mints": sorted(cache),
    }
    try:
        JUPITER_SKIP_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        JUPITER_SKIP_CACHE_PATH.write_text(
            json.dumps(payload, indent=2) + "\n", encoding="utf-8"
        )
    except TransientPricingError:
        raise
    except Exception as exc:
        logging.warning("Failed to write Jupiter skip cache: %s", exc)


def _is_probable_solana_mint(value: str) -> bool:
    return 32 <= len(value) <= 44 and value.isalnum()
