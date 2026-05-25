#!/usr/bin/env python3
"""Audit Meteora DLMM PositionV2 fee_infos bytes without touching spike code."""

from __future__ import annotations

import base64
import hashlib
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
FEE_DATA_OFF = 4552
FEE_INFO_SIZE = 48
FEE_INFOS_LEN = 70 * FEE_INFO_SIZE
FEE_INFOS_END = FEE_DATA_OFF + FEE_INFOS_LEN
LAST_REGION_START = 7860
LAST_REGION_END = 7920
FETCH_LEN = LAST_REGION_END
FEE_X_PENDING_OFF = 32
FEE_Y_PENDING_OFF = 40
MAX_ACCOUNTS = 3
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
    chars = []
    while num:
        num, rem = divmod(num, 58)
        chars.append(BASE58_ALPHABET[rem])
    leading_zeroes = len(raw) - len(raw.lstrip(b"\x00"))
    return "1" * leading_zeroes + ("".join(reversed(chars)) if chars else "")


def account_discriminator(name: str) -> str:
    return b58encode(hashlib.sha256(f"account:{name}".encode()).digest()[:8])


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


def get_program_accounts(
    rpc_url: str, label: str, filters: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    config: dict[str, Any] = {
        "encoding": "base64",
        "filters": filters,
        "dataSlice": {"offset": 0, "length": FETCH_LEN},
    }
    print(f"\n=== Query: {label} ===")
    print(f"filters={json.dumps(filters, separators=(',', ':'))}")
    result = rpc_call(rpc_url, "getProgramAccounts", [PROGRAM_ID, config])
    print(f"returned_accounts={len(result)}")
    return result[:MAX_ACCOUNTS]


def hexdump(data: bytes, base_offset: int) -> str:
    lines = []
    for idx in range(0, len(data), 16):
        chunk = data[idx : idx + 16]
        hex_bytes = " ".join(f"{byte:02x}" for byte in chunk)
        ascii_bytes = "".join(chr(byte) if 32 <= byte <= 126 else "." for byte in chunk)
        lines.append(f"{base_offset + idx:08x}  {hex_bytes:<47}  {ascii_bytes}")
    return "\n".join(lines)


def unpack_u64_le(data: bytes, offset: int) -> int | None:
    if offset + 8 > len(data):
        return None
    return struct.unpack_from("<Q", data, offset)[0]


def audit_account(index: int, account: dict[str, Any]) -> None:
    pubkey = account["pubkey"]
    acct = account["account"]
    encoded = acct["data"][0]
    data = base64.b64decode(encoded)
    space = acct.get("space")

    print(f"\n--- Account {index}: {pubkey} ---")
    print(f"owner={acct.get('owner')}")
    print(f"lamports={acct.get('lamports')}")
    print(f"reported_space={space}")
    print(f"decoded_slice_len={len(data)}")

    if len(data) < LAST_REGION_END:
        print(f"WARNING: decoded data shorter than {LAST_REGION_END}; dumps may be truncated")

    first_region = data[FEE_DATA_OFF : min(FEE_DATA_OFF + FEE_INFO_SIZE, len(data))]
    last_region = data[LAST_REGION_START : min(LAST_REGION_END, len(data))]
    fee_region = data[FEE_DATA_OFF : min(FEE_INFOS_END, len(data))]

    print(f"\nbytes[4552:4600] first FeeInfo ({len(first_region)} bytes):")
    print(hexdump(first_region, FEE_DATA_OFF) if first_region else "<empty>")

    print(f"\nbytes[7860:7920] last FeeInfo region ({len(last_region)} bytes):")
    print(hexdump(last_region, LAST_REGION_START) if last_region else "<empty>")

    zero_count = fee_region.count(0)
    non_zero_count = len(fee_region) - zero_count
    print(f"\nfee_infos bytes[4552:7912] inspected_len={len(fee_region)}")
    print(f"zero_count={zero_count}")
    print(f"non_zero_count={non_zero_count}")
    if non_zero_count:
        for rel_offset, value in enumerate(fee_region):
            if value:
                print(
                    f"first_non_zero_byte_abs_offset={FEE_DATA_OFF + rel_offset} "
                    f"rel_offset={rel_offset} value=0x{value:02x}"
                )
                break
    else:
        print("first_non_zero_byte=<none>")

    fee_x_total = 0
    fee_y_total = 0
    pending_offsets_nonzero: list[str] = []
    for slot in range(70):
        base = FEE_DATA_OFF + slot * FEE_INFO_SIZE
        fee_x = unpack_u64_le(data, base + FEE_X_PENDING_OFF)
        fee_y = unpack_u64_le(data, base + FEE_Y_PENDING_OFF)
        if fee_x is None or fee_y is None:
            continue
        fee_x_total += fee_x
        fee_y_total += fee_y
        if fee_x or fee_y:
            pending_offsets_nonzero.append(
                f"slot={slot} fee_x@{base + FEE_X_PENDING_OFF}={fee_x} "
                f"fee_y@{base + FEE_Y_PENDING_OFF}={fee_y}"
            )

    print(f"assumed_fee_x_pending_raw_sum={fee_x_total}")
    print(f"assumed_fee_y_pending_raw_sum={fee_y_total}")
    if pending_offsets_nonzero:
        print("nonzero_pending_slots:")
        for line in pending_offsets_nonzero:
            print(f"  {line}")
    else:
        print("nonzero_pending_slots=<none>")


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
        print(f"ERROR: LPAGENT_WALLET decoded to {len(wallet_bytes)} bytes, expected 32", file=sys.stderr)
        return 2

    rpc_url = f"https://mainnet.helius-rpc.com/?api-key={helius_api_key}"
    position_v2_disc = account_discriminator("PositionV2")
    position_disc = account_discriminator("Position")

    print("Meteora DLMM fee_infos audit")
    print(f"program_id={PROGRAM_ID}")
    print(f"wallet={wallet}")
    print(f"fee_infos_range=[{FEE_DATA_OFF}:{FEE_INFOS_END}] bytes={FEE_INFOS_LEN}")
    print(f"first_fee_info_range=[{FEE_DATA_OFF}:{FEE_DATA_OFF + FEE_INFO_SIZE}]")
    print(f"last_region_range=[{LAST_REGION_START}:{LAST_REGION_END}]")
    print(f"position_v2_discriminator_b58={position_v2_disc}")
    print(f"position_discriminator_b58={position_disc}")

    selected: list[dict[str, Any]] = []
    selected_label = ""

    requested_filters = [{"memcmp": {"offset": 8, "bytes": wallet}}]
    requested = get_program_accounts(rpc_url, "requested_wallet_memcmp_offset_8", requested_filters)
    if requested:
        selected = requested
        selected_label = "requested_wallet_memcmp_offset_8"

    if not selected:
        spike_filters = [
            {"memcmp": {"offset": 0, "bytes": position_v2_disc}},
            {"memcmp": {"offset": 40, "bytes": wallet}},
        ]
        spike_matches = get_program_accounts(
            rpc_url, "spike_style_position_v2_owner_offset_40", spike_filters
        )
        if spike_matches:
            selected = spike_matches
            selected_label = "spike_style_position_v2_owner_offset_40"

    if not selected:
        fallback_filters = [{"memcmp": {"offset": 0, "bytes": position_v2_disc}}]
        selected = get_program_accounts(
            rpc_url, "fallback_first_position_v2_without_wallet_memcmp", fallback_filters
        )
        selected_label = "fallback_first_position_v2_without_wallet_memcmp"

    if not selected:
        fallback_filters = [{"memcmp": {"offset": 0, "bytes": position_disc}}]
        selected = get_program_accounts(
            rpc_url, "fallback_first_position_without_wallet_memcmp", fallback_filters
        )
        selected_label = "fallback_first_position_without_wallet_memcmp"

    print(f"\n=== Auditing selected accounts from {selected_label} ===")
    if not selected:
        print("No accounts available to audit.")
        return 1

    for index, account in enumerate(selected, 1):
        audit_account(index, account)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
