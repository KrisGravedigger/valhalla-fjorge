#!/usr/bin/env python3
"""Inspect bytes trailing after the fixed PositionV2 payload."""

from __future__ import annotations

import base64
import json
import os
import struct
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


PROGRAM_ID = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
POSITION_V2_FIXED_LEN = 8120
POSITION_LB_PAIR_OFF = 8
POSITION_OWNER_OFF = 40
POSITION_V2_DISC = bytes([117, 176, 212, 199, 245, 180, 133, 182])

LIMIT_ORDER_SIZE = 112
LIMIT_ORDER_LB_PAIR_OFF = 0
LIMIT_ORDER_OWNER_OFF = 32
LIMIT_ORDER_BIN_COUNT_OFF = 64

LIMIT_ORDER_BIN_DATA_SIZE = 32
BIN_DATA_AMOUNT_OFF = 0
BIN_DATA_BIN_ID_OFF = 16
BIN_DATA_IS_ASK_OFF = 20

POSITION_BIN_DATA_SIZE = 112
POSITION_BIN_LIQUIDITY_SHARE_OFF = 0
POSITION_BIN_REWARD_INFO_OFF = 16
POSITION_BIN_FEE_INFO_OFF = 64
POSITION_BIN_FEE_X_PENDING_OFF = POSITION_BIN_FEE_INFO_OFF + 32
POSITION_BIN_FEE_Y_PENDING_OFF = POSITION_BIN_FEE_INFO_OFF + 40

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {ch: i for i, ch in enumerate(BASE58_ALPHABET)}


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def b58decode(value: str) -> bytes:
    num = 0
    for ch in value:
        try:
            num = num * 58 + BASE58_INDEX[ch]
        except KeyError as exc:
            raise ValueError(f"invalid base58 character: {ch!r}") from exc
    leading_zeroes = len(value) - len(value.lstrip("1"))
    raw = b"" if num == 0 else num.to_bytes((num.bit_length() + 7) // 8, "big")
    return b"\x00" * leading_zeroes + raw


def b58encode(raw: bytes) -> str:
    num = int.from_bytes(raw, "big")
    chars: list[str] = []
    while num:
        num, rem = divmod(num, 58)
        chars.append(BASE58_ALPHABET[rem])
    leading_zeroes = len(raw) - len(raw.lstrip(b"\x00"))
    encoded = "".join(reversed(chars)) if chars else ""
    return "1" * leading_zeroes + encoded


def rpc_call(url: str, method: str, params: list[Any], retries: int = 3) -> Any:
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    body = json.dumps(payload).encode("utf-8")
    for attempt in range(retries):
        req = urllib.request.Request(
            url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as resp:
                response = json.loads(resp.read())
            if "error" in response:
                raise RuntimeError(f"RPC {method} error: {response['error']}")
            return response["result"]
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < retries - 1:
                time.sleep(2**attempt)
                continue
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def account_data_from_value(value: dict[str, Any]) -> bytes:
    encoded = value["data"][0]
    return base64.b64decode(encoded)


def get_account_len(account: dict[str, Any]) -> int:
    reported_space = account.get("space")
    if isinstance(reported_space, int):
        return reported_space
    data = account.get("data")
    if isinstance(data, list) and data:
        return len(base64.b64decode(data[0]))
    raise ValueError("account length unavailable")


def hexdump_line(data: bytes, offset: int, length: int) -> str:
    chunk = data[offset : offset + length]
    return f"{offset}: {' '.join(f'{byte:02X}' for byte in chunk)}"


def pubkey_at(data: bytes, offset: int) -> str:
    return b58encode(data[offset : offset + 32])


def plausibility_label(amount: int, bin_id: int, is_ask: int) -> str:
    checks = [
        amount >= 0,
        -(2**23) <= bin_id <= 2**23,
        is_ask in (0, 1),
    ]
    return "plausible" if all(checks) else "not_plausible"


def unpack_u128(data: bytes, offset: int) -> int:
    lo, hi = struct.unpack_from("<QQ", data, offset)
    return lo + (hi << 64)


def main() -> int:
    repo_root = Path(__file__).resolve().parents[1]
    load_env(repo_root / ".env")

    helius_api_key = os.environ.get("HELIUS_API_KEY", "").strip()
    wallet = os.environ.get("LPAGENT_WALLET", "").strip()
    if not helius_api_key:
        print("ERROR: HELIUS_API_KEY is missing", file=sys.stderr)
        return 2
    if not wallet:
        print("ERROR: LPAGENT_WALLET is missing", file=sys.stderr)
        return 2
    wallet_bytes = b58decode(wallet)
    if len(wallet_bytes) != 32:
        print(
            f"ERROR: LPAGENT_WALLET decoded to {len(wallet_bytes)} bytes, expected 32",
            file=sys.stderr,
        )
        return 2

    rpc_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
    disc_b58 = b58encode(POSITION_V2_DISC)
    filters = [
        {"memcmp": {"offset": 0, "bytes": disc_b58}},
        {"memcmp": {"offset": POSITION_OWNER_OFF, "bytes": wallet}},
    ]

    print("PositionV2 trailing bytes inspection")
    print(f"program_id={PROGRAM_ID}")
    print(f"wallet={wallet}")
    print(f"position_v2_fixed_len={POSITION_V2_FIXED_LEN}")
    print(f"position_v2_discriminator_bytes={list(POSITION_V2_DISC)}")
    print(f"position_v2_discriminator_b58={disc_b58}")
    print(f"filters={json.dumps(filters, separators=(',', ':'))}")

    accounts = rpc_call(
        rpc_url,
        "getProgramAccounts",
        [
            PROGRAM_ID,
            {
                "encoding": "base64",
                "filters": filters,
            },
        ],
    )
    print(f"matched_accounts={len(accounts)}")

    oversized: list[tuple[str, int]] = []
    for item in accounts:
        account_len = get_account_len(item["account"])
        if account_len > POSITION_V2_FIXED_LEN:
            oversized.append((item["pubkey"], account_len))
    print(f"accounts_len_gt_{POSITION_V2_FIXED_LEN}={len(oversized)}")
    if not oversized:
        print("No PositionV2 accounts with trailing bytes found.")
        return 1

    selected_pubkey, selected_len = oversized[0]
    print(f"selected_pubkey={selected_pubkey}")
    print(f"selected_reported_len={selected_len}")

    account_info = rpc_call(
        rpc_url,
        "getAccountInfo",
        [selected_pubkey, {"encoding": "base64"}],
    )
    if account_info["value"] is None:
        print(f"ERROR: selected account disappeared: {selected_pubkey}", file=sys.stderr)
        return 1

    data = account_data_from_value(account_info["value"])
    raw_len = len(data)
    trailing_len = raw_len - POSITION_V2_FIXED_LEN
    position_lb_pair = pubkey_at(data, POSITION_LB_PAIR_OFF)
    position_owner = pubkey_at(data, POSITION_OWNER_OFF)

    print(f"raw_len={raw_len}")
    print(f"trailing_len={trailing_len}")
    print(f"trailing_as_112_byte_count={trailing_len / POSITION_BIN_DATA_SIZE:.6g}")
    print(f"trailing_as_limit_order_count={trailing_len / LIMIT_ORDER_SIZE:.6g}")
    print(f"trailing_as_bin_data_count={trailing_len / LIMIT_ORDER_BIN_DATA_SIZE:.6g}")
    print(f"position_lb_pair_offset_8={position_lb_pair}")
    print(f"position_owner_offset_40={position_owner}")
    print(f"bytes[{POSITION_V2_FIXED_LEN}:{POSITION_V2_FIXED_LEN + 112}]")
    print(hexdump_line(data, POSITION_V2_FIXED_LEN, 112))

    lo_base = POSITION_V2_FIXED_LEN
    header_lb_pair = pubkey_at(data, lo_base + LIMIT_ORDER_LB_PAIR_OFF)
    header_owner = pubkey_at(data, lo_base + LIMIT_ORDER_OWNER_OFF)
    bin_count = struct.unpack_from("<H", data, lo_base + LIMIT_ORDER_BIN_COUNT_OFF)[0]
    print("\nDecode as LimitOrder header @8120")
    print(f"lb_pair={header_lb_pair}")
    print(f"owner={header_owner}")
    print(f"bin_count={bin_count}")
    print(f"lb_pair_matches_position={header_lb_pair == position_lb_pair}")
    print(f"owner_matches_position_owner={header_owner == position_owner}")

    amount = struct.unpack_from("<Q", data, lo_base + BIN_DATA_AMOUNT_OFF)[0]
    bin_id = struct.unpack_from("<i", data, lo_base + BIN_DATA_BIN_ID_OFF)[0]
    is_ask = data[lo_base + BIN_DATA_IS_ASK_OFF]
    print("\nDecode as LimitOrderBinData @8120")
    print(f"amount={amount}")
    print(f"bin_id={bin_id}")
    print(f"is_ask={is_ask}")
    print(f"plausibility={plausibility_label(amount, bin_id, is_ask)}")

    liquidity_share = unpack_u128(data, lo_base + POSITION_BIN_LIQUIDITY_SHARE_OFF)
    fee_x_pending = struct.unpack_from("<Q", data, lo_base + POSITION_BIN_FEE_X_PENDING_OFF)[0]
    fee_y_pending = struct.unpack_from("<Q", data, lo_base + POSITION_BIN_FEE_Y_PENDING_OFF)[0]
    print("\nDecode as PositionBinData extension @8120")
    print(f"liquidity_share={liquidity_share}")
    print(f"fee_x_pending={fee_x_pending}")
    print(f"fee_y_pending={fee_y_pending}")
    print(
        "interpretation_hint=PositionV2 dynamic extension stores extra 112B "
        "PositionBinData slots after the fixed account body"
    )

    if trailing_len % LIMIT_ORDER_SIZE == 0:
        print("\nLimitOrder header scan")
        for idx in range(trailing_len // LIMIT_ORDER_SIZE):
            base = POSITION_V2_FIXED_LEN + idx * LIMIT_ORDER_SIZE
            scan_lb_pair = pubkey_at(data, base)
            scan_owner = pubkey_at(data, base + 32)
            scan_bin_count = struct.unpack_from("<H", data, base + 64)[0]
            print(
                f"slot={idx} offset={base} "
                f"lb_pair_match={scan_lb_pair == position_lb_pair} "
                f"owner_match={scan_owner == position_owner} "
                f"bin_count={scan_bin_count}"
            )

    if trailing_len % LIMIT_ORDER_BIN_DATA_SIZE == 0:
        print("\nLimitOrderBinData scan")
        plausible = 0
        for idx in range(trailing_len // LIMIT_ORDER_BIN_DATA_SIZE):
            base = POSITION_V2_FIXED_LEN + idx * LIMIT_ORDER_BIN_DATA_SIZE
            scan_amount = struct.unpack_from("<Q", data, base)[0]
            scan_bin_id = struct.unpack_from("<i", data, base + 16)[0]
            scan_is_ask = data[base + 20]
            label = plausibility_label(scan_amount, scan_bin_id, scan_is_ask)
            if label == "plausible":
                plausible += 1
            print(
                f"slot={idx} offset={base} amount={scan_amount} "
                f"bin_id={scan_bin_id} is_ask={scan_is_ask} {label}"
            )
        print(f"plausible_bin_data_slots={plausible}")

    if trailing_len % POSITION_BIN_DATA_SIZE == 0:
        print("\nPositionBinData extension scan")
        nonzero_slots = 0
        for idx in range(trailing_len // POSITION_BIN_DATA_SIZE):
            base = POSITION_V2_FIXED_LEN + idx * POSITION_BIN_DATA_SIZE
            scan_liquidity_share = unpack_u128(data, base)
            scan_fee_x_pending = struct.unpack_from("<Q", data, base + POSITION_BIN_FEE_X_PENDING_OFF)[0]
            scan_fee_y_pending = struct.unpack_from("<Q", data, base + POSITION_BIN_FEE_Y_PENDING_OFF)[0]
            if scan_liquidity_share or scan_fee_x_pending or scan_fee_y_pending:
                nonzero_slots += 1
            print(
                f"slot={idx} offset={base} liquidity_share={scan_liquidity_share} "
                f"fee_x_pending={scan_fee_x_pending} fee_y_pending={scan_fee_y_pending}"
            )
        print(f"nonzero_position_bin_data_slots={nonzero_slots}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
