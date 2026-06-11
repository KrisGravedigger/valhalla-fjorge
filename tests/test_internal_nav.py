from __future__ import annotations

import csv
import importlib.util
import io
import logging
import struct
import urllib.error
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from valhalla import internal_nav
from valhalla.internal_nav import NavResult


def _load_record_tool() -> Any:
    path = Path(__file__).resolve().parents[1] / "tools" / "record_internal_nav.py"
    spec = importlib.util.spec_from_file_location("record_internal_nav", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _http_error(code: int, body: bytes) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="https://api.jup.ag/swap/v1/quote",
        code=code,
        msg="error",
        hdrs={},
        fp=io.BytesIO(body),
    )


def _nav_result(
    value: Decimal = Decimal("50"),
    degraded: bool = False,
    degraded_mints: list[str] | None = None,
) -> NavResult:
    return NavResult(
        wallet="WALLET",
        timestamp=datetime(2026, 5, 24, tzinfo=timezone.utc),
        positions_nav_sol=value,
        fees_sol=Decimal("0"),
        rewards_sol=Decimal("0"),
        free_sol=Decimal("0"),
        idle_spl_sol=Decimal("0"),
        total_nav_sol=value,
        n_positions=1,
        degraded=degraded,
        degraded_mints=degraded_mints or [],
    )


def _tmp_dir() -> Path:
    path = Path("_temp") / f"internal_nav_tests_{uuid.uuid4().hex}"
    path.mkdir(parents=True, exist_ok=False)
    return path


def test_decode_position_fixed() -> None:
    data = bytearray(internal_nav.POSV2_FIXED)
    data[internal_nav.POS_LB_PAIR : internal_nav.POS_LB_PAIR + 32] = bytes([1]) * 32
    struct.pack_into("<i", data, 7912, -12)
    struct.pack_into("<i", data, 7916, 57)

    decoded = internal_nav._decode_position(bytes(data))

    assert decoded["lower_bin_id"] == -12
    assert decoded["upper_bin_id"] == 57
    assert decoded["width"] == 70
    assert decoded["ext_count"] == 0


def test_decode_position_extended() -> None:
    data = bytearray(internal_nav.POSV2_FIXED + 3 * internal_nav.POSBIN_SIZE)
    data[internal_nav.POS_LB_PAIR : internal_nav.POS_LB_PAIR + 32] = bytes([2]) * 32
    struct.pack_into("<i", data, 7912, 1)
    struct.pack_into("<i", data, 7916, 70)
    for idx, value in enumerate((11, 22, 33)):
        struct.pack_into(
            "<QQ",
            data,
            internal_nav.POSV2_FIXED + idx * internal_nav.POSBIN_SIZE,
            value,
            0,
        )

    decoded = internal_nav._decode_position(bytes(data))

    assert decoded["ext_count"] == 3
    assert decoded["ext_liq_shares"] == [11, 22, 33]


def test_decode_bin_array() -> None:
    data = bytearray(internal_nav.BA_HEADER + internal_nav.N_BINS * internal_nav.BIN_SIZE)
    slot = 2
    offset = internal_nav.BA_HEADER + slot * internal_nav.BIN_SIZE
    struct.pack_into("<Q", data, offset + internal_nav.BIN_AMOUNT_X, 123)
    struct.pack_into("<Q", data, offset + internal_nav.BIN_AMOUNT_Y, 456)
    struct.pack_into("<QQ", data, offset + internal_nav.BIN_LIQ_SUPPLY, 789, 0)

    decoded = internal_nav._decode_bin_array(bytes(data), array_idx=3)

    assert decoded[212]["amount_x"] == 123
    assert decoded[212]["amount_y"] == 456
    assert decoded[212]["liquidity_supply"] == 789


def test_jupiter_no_route_treated_as_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)

    def fail(_: str) -> dict[str, Any]:
        raise _http_error(400, b'{"error":"NO_ROUTES_FOUND"}')

    monkeypatch.setattr(internal_nav, "_http_get", fail)

    assert internal_nav._jupiter_to_sol("MINT", 1000) == (Decimal("0"), False)


def test_jupiter_no_route_persistent_skip_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    mint = "A" * 32
    cache_path = _tmp_dir() / "skipped_mints.json"
    calls = 0

    def fail(_: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise _http_error(400, b'{"error":"NO_ROUTES_FOUND"}')

    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "JUPITER_SKIP_CACHE_PATH", cache_path)
    monkeypatch.setattr(internal_nav, "_jupiter_skip_cache", None)
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_http_get", fail)

    assert internal_nav._jupiter_to_sol(mint, 1000) == (Decimal("0"), False)
    assert calls == 1
    assert mint in cache_path.read_text(encoding="utf-8")

    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(AssertionError("HTTP should be skipped")),
    )

    assert internal_nav._jupiter_to_sol(mint, 2000) == (Decimal("0"), False)
    assert calls == 1


def test_jupiter_reference_no_route_does_not_persist_skip_cache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mint = "B" * 32
    cache_path = _tmp_dir() / "skipped_mints.json"
    calls: list[str] = []

    def fail_primary_then_no_route_reference(url: str) -> dict[str, Any]:
        calls.append(url)
        if "amount=1000&" in url:
            raise _http_error(400, b'{"error":"primary amount failed"}')
        if "amount=1000000000&" in url:
            raise _http_error(400, b'{"error":"NO_ROUTES_FOUND"}')
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "JUPITER_SKIP_CACHE_PATH", cache_path)
    monkeypatch.setattr(internal_nav, "_jupiter_skip_cache", None)
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_http_get", fail_primary_then_no_route_reference)

    assert internal_nav._jupiter_to_sol(mint, 1000) == (Decimal("0"), True)
    assert len(calls) == 2
    assert not cache_path.exists()

    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(internal_nav, "_jupiter_skip_cache", None)

    assert internal_nav._jupiter_to_sol(mint, 1000) == (Decimal("0"), True)
    assert len(calls) == 4
    assert not cache_path.exists()


def test_jupiter_degraded_429_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    calls = 0

    def fail(_: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise _http_error(429, b"rate limited")

    monkeypatch.setattr(internal_nav, "_http_get", fail)

    assert internal_nav._jupiter_to_sol("MINT", 1000) == (Decimal("0"), True)
    assert calls == 4


def test_jupiter_failed_cache_remains_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", {"MINT"})
    monkeypatch.setattr(internal_nav, "_jupiter_skip_cache", set())
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(AssertionError("HTTP should be skipped")),
    )

    assert internal_nav._jupiter_to_sol("MINT", 1000) == (Decimal("0"), True)


def test_jupiter_u64_guard_skips_http(monkeypatch: pytest.MonkeyPatch) -> None:
    degraded: list[str] = []
    warnings: list[str] = []
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(AssertionError("HTTP should be skipped")),
    )

    assert (
        internal_nav._value_mint_amount(
            "MINT",
            Decimal(internal_nav.U64_MAX + 1),
            degraded,
            warnings,
            suppress_immaterial_warning=False,
        )
        == Decimal("0")
    )
    assert degraded == ["MINT"]
    assert warnings == [
        f"Jupiter amount exceeds u64 for MINT amount_raw={internal_nav.U64_MAX + 1}; likely decode artifact"
    ]


def test_jupiter_reference_prices_tiny_failed_full_quote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mint = "MINT"
    calls: list[str] = []
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    degraded: list[str] = []
    warnings: list[str] = []

    def http_get(url: str) -> dict[str, Any]:
        calls.append(url)
        if "amount=271&" in url:
            raise _http_error(
                400,
                b'{"error":"Cannot compute other amount threshold, with amount 1"}',
            )
        if "amount=1000000000&" in url:
            return {"outAmount": "1000000000"}
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav, "_http_get", http_get)

    sol_value = internal_nav._value_mint_amount(
        mint,
        Decimal("271"),
        degraded,
        warnings,
        suppress_immaterial_warning=False,
    )

    assert sol_value == Decimal("2.71E-7")
    assert degraded == []
    assert mint not in internal_nav._jupiter_price_cache
    assert warnings == ["immaterial reference-priced mint MINT value<0.01 SOL"]
    assert len(calls) == 2


def test_jupiter_reference_does_not_cleanly_value_material_failed_full_quote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mint = "MINT"
    calls: list[str] = []
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())

    def http_get(url: str) -> dict[str, Any]:
        calls.append(url)
        if "amount=500000000&" in url:
            raise _http_error(400, b'{"error":"full quote failed"}')
        if "amount=1000000000&" in url:
            return {"outAmount": "100000000"}
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav, "_http_get", http_get)

    sol_value, degraded = internal_nav._jupiter_to_sol(mint, 500_000_000)

    assert sol_value == Decimal("0")
    assert degraded is True
    assert mint not in internal_nav._jupiter_price_cache
    assert len(calls) == 2


def test_material_reference_failure_blocks_snapshot_without_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    degraded: list[str] = []
    warnings: list[str] = []
    calls: list[str] = []
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())

    def http_get(url: str) -> dict[str, Any]:
        calls.append(url)
        if "amount=500000000&" in url:
            raise _http_error(400, b'{"error":"full quote failed"}')
        if "amount=1000000000&" in url:
            return {"outAmount": "100000000"}
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav, "_http_get", http_get)
    value = internal_nav._convert_amount(
        "RPC", "MINT", Decimal("500000000"), degraded, warnings
    )
    assert value == Decimal("0")
    assert degraded == ["MINT"]
    assert calls == [
        "https://api.jup.ag/swap/v1/quote?inputMint=MINT&outputMint=So11111111111111111111111111111111111111112&amount=500000000&slippageBps=50",
        "https://api.jup.ag/swap/v1/quote?inputMint=MINT&outputMint=So11111111111111111111111111111111111111112&amount=1000000000&slippageBps=50",
    ]

    result = _nav_result(degraded=True, degraded_mints=degraded)
    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
        ]
    )

    assert code == 1
    assert not path.exists()


def test_small_reference_fallback_does_not_cache_clean_price_for_large_amount(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mint = "MINT"
    calls: list[str] = []
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    degraded: list[str] = []
    warnings: list[str] = []

    def http_get(url: str) -> dict[str, Any]:
        calls.append(url)
        if "amount=271&" in url:
            raise _http_error(400, b'{"error":"tiny full quote failed"}')
        if "amount=500000000&" in url:
            raise _http_error(400, b'{"error":"large full quote failed"}')
        if "amount=1000000000&" in url:
            if len([call for call in calls if "amount=1000000000&" in call]) == 1:
                return {"outAmount": "1000"}
            return {"outAmount": "100000000"}
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav, "_http_get", http_get)

    small_value = internal_nav._value_mint_amount(
        mint,
        Decimal("271"),
        degraded,
        warnings,
        suppress_immaterial_warning=False,
    )
    large_value, large_degraded = internal_nav._jupiter_to_sol(mint, 500_000_000)

    assert small_value == Decimal("2.71E-13")
    assert degraded == []
    assert warnings == ["immaterial reference-priced mint MINT value<0.01 SOL"]
    assert large_value == Decimal("0")
    assert large_degraded is True
    assert mint not in internal_nav._jupiter_price_cache
    assert any("amount=500000000&" in call for call in calls)


def test_suspicious_material_amount_is_degraded_with_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    amount = internal_nav.SUSPICIOUS_SPL_RAW_AMOUNT + 1
    degraded: list[str] = []
    warnings: list[str] = []
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: {"outAmount": "1000000000"},
    )

    value = internal_nav._convert_amount(
        "RPC", "MINT", Decimal(amount), degraded, warnings
    )

    assert value == Decimal("1")
    assert degraded == ["MINT"]
    assert warnings == [f"suspicious large raw amount MINT amount_raw={amount}"]


def test_jupiter_sol_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(_: str) -> dict[str, Any]:
        raise AssertionError("HTTP should not be called for SOL")

    monkeypatch.setattr(internal_nav, "_http_get", unexpected)

    assert internal_nav._jupiter_to_sol(internal_nav.SOL_MINT, 1_000_000_000) == (
        Decimal("1"),
        False,
    )


def test_convert_amount_keeps_immaterial_reference_warning_non_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    degraded: list[str] = []
    warnings: list[str] = []
    monkeypatch.setattr(
        internal_nav,
        "_quote_jupiter_to_sol",
        lambda mint, _amount: internal_nav._MintQuoteResult(
            Decimal("0.000001"),
            False,
            "reference-immaterial",
            f"immaterial reference-priced mint {mint} value<0.01 SOL",
        ),
    )

    value = internal_nav._convert_amount(
        "RPC", "MINT", Decimal("271.0206025944224600182639848"), degraded, warnings
    )

    assert value == Decimal("0.000001")
    assert degraded == []
    assert warnings == ["immaterial reference-priced mint MINT value<0.01 SOL"]


def test_compute_nav_zero_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _rpc, _wallet: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _rpc, _pubkeys: [])
    monkeypatch.setattr(
        internal_nav, "_rpc_call", lambda _rpc, _method, _params: {"value": 0}
    )

    with pytest.raises(RuntimeError, match="zero NAV result"):
        internal_nav.compute_nav("RPC", "WALLET")


def test_null_position_account_sets_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    address = "POSITION1"

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {"value": []}
        raise AssertionError(method)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [address])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [None])
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.degraded is True
    assert address in result.degraded_mints


def test_missing_bin_array_sets_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    lb_pair_bytes = bytes(internal_nav.METEORA_PROGRAM)
    position = {
        "address": "POS",
        "lb_pair": "LBPAIR",
        "lb_pair_bytes": lb_pair_bytes,
        "lower_bin_id": 0,
        "upper_bin_id": 0,
        "width": 1,
        "n_slots": internal_nav.N_BINS,
        "liquidity_shares": [1000] + [0] * (internal_nav.N_BINS - 1),
        "ext_liq_shares": [],
        "ext_count": 0,
        "fee_x_pending_raw": 0,
        "fee_y_pending_raw": 0,
        "reward0_raw": 0,
        "reward1_raw": 0,
    }
    fetch_calls = 0

    def fetch_accounts(_rpc: str, _pubkeys: list[str]) -> list[bytes | None]:
        nonlocal fetch_calls
        fetch_calls += 1
        return [b"position"] if fetch_calls == 1 else [None]

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {"value": []}
        raise AssertionError(method)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: ["POS"])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", fetch_accounts)
    monkeypatch.setattr(internal_nav, "_decode_position", lambda _raw: position)
    monkeypatch.setattr(
        internal_nav,
        "_get_pool_mints",
        lambda _rpc, _lb_pair: {
            "mint_x": internal_nav.SOL_MINT,
            "mint_y": internal_nav.SOL_MINT,
        },
    )
    monkeypatch.setattr(internal_nav, "_get_reward_mints", lambda _rpc, _lb: [None, None])
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.degraded is True
    assert "binarray:LBPAIR:0" in result.degraded_mints


def test_compute_nav_degraded_propagates(monkeypatch: pytest.MonkeyPatch) -> None:
    lb_pair_bytes = bytes(internal_nav.METEORA_PROGRAM)
    position = {
        "address": "POS",
        "lb_pair": "LBPAIR",
        "lb_pair_bytes": lb_pair_bytes,
        "lower_bin_id": 0,
        "upper_bin_id": 0,
        "width": 1,
        "n_slots": internal_nav.N_BINS,
        "liquidity_shares": [1000] + [0] * (internal_nav.N_BINS - 1),
        "ext_liq_shares": [],
        "ext_count": 0,
        "fee_x_pending_raw": 0,
        "fee_y_pending_raw": 0,
        "reward0_raw": 0,
        "reward1_raw": 0,
    }
    fetch_calls = 0

    def fetch_accounts(_rpc: str, _pubkeys: list[str]) -> list[bytes | None]:
        nonlocal fetch_calls
        fetch_calls += 1
        return [b"position"] if fetch_calls == 1 else [b"binarray"]

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {"value": []}
        raise AssertionError(method)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: ["POS"])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", fetch_accounts)
    monkeypatch.setattr(internal_nav, "_decode_position", lambda _raw: position)
    monkeypatch.setattr(
        internal_nav,
        "_get_pool_mints",
        lambda _rpc, _lb_pair: {"mint_x": "MINTX", "mint_y": internal_nav.SOL_MINT},
    )
    monkeypatch.setattr(
        internal_nav,
        "_decode_bin_array",
        lambda _raw, _idx, _lb: {
            0: {"amount_x": 1000, "amount_y": 0, "liquidity_supply": 1000}
        },
    )
    monkeypatch.setattr(internal_nav, "_get_reward_mints", lambda _rpc, _lb: [None, None])
    monkeypatch.setattr(
        internal_nav,
        "_quote_jupiter_to_sol",
        lambda _mint, _amount: internal_nav._MintQuoteResult(Decimal("0"), True),
    )
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.degraded is True
    assert result.degraded_mints == ["MINTX"]


def test_unknown_positive_reward_mint_degrades_and_blocks_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    lb_pair_bytes = bytes(internal_nav.METEORA_PROGRAM)
    lb_pair = str(internal_nav.METEORA_PROGRAM)
    position = {
        "address": "POS",
        "lb_pair": lb_pair,
        "lb_pair_bytes": lb_pair_bytes,
        "lower_bin_id": 0,
        "upper_bin_id": 0,
        "width": 1,
        "n_slots": internal_nav.N_BINS,
        "liquidity_shares": [0] * internal_nav.N_BINS,
        "ext_liq_shares": [],
        "ext_count": 0,
        "fee_x_pending_raw": 0,
        "fee_y_pending_raw": 0,
        "reward0_raw": 123,
        "reward1_raw": 0,
    }
    degraded: list[str] = []
    warnings: list[str] = []

    monkeypatch.setattr(
        internal_nav,
        "_get_pool_mints",
        lambda _rpc, _lb_pair: {
            "mint_x": internal_nav.SOL_MINT,
            "mint_y": internal_nav.SOL_MINT,
        },
    )
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [b"binarray"])
    monkeypatch.setattr(internal_nav, "_decode_bin_array", lambda _raw, _idx, _lb: {})
    monkeypatch.setattr(internal_nav, "_get_reward_mints", lambda _rpc, _lb: [None, None])

    _pos_nav, _fees, rewards = internal_nav._compute_position_nav(
        "RPC", position, degraded, warnings
    )

    assert rewards == Decimal("0")
    assert degraded == [f"reward-mint:{lb_pair}:0"]
    assert warnings == [
        f"reward0 raw=123 for {lb_pair} but reward mint unknown"
    ]

    result = _nav_result(degraded=True, degraded_mints=degraded)
    result.warnings = warnings
    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
        ]
    )

    assert code == 1
    assert not path.exists()


def test_idle_spl_no_route_is_visible_and_snapshot_writes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "mint": "IDLEMINT",
                                        "tokenAmount": {"amount": "1000"},
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        raise AssertionError(method)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [])
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(
            _http_error(400, b'{"error":"NO_ROUTES_FOUND"}')
        ),
    )
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.idle_spl_sol == Decimal("0")
    assert result.total_nav_sol == Decimal("1")
    assert result.degraded is False
    assert result.degraded_mints == []
    assert result.warnings == ["no-route treated as 0: IDLEMINT"]

    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
        ]
    )

    assert code == 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["notes"] == "no-route treated as 0: 1 mints (IDLEMINT)"


def test_idle_spl_http_error_degrades_and_blocks_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "mint": "IDLEMINT",
                                        "tokenAmount": {"amount": "500000000"},
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        raise AssertionError(method)

    def http_get(url: str) -> dict[str, Any]:
        if "amount=500000000&" in url:
            raise _http_error(500, b'{"error":"server error"}')
        if "amount=1000000000&" in url:
            return {"outAmount": "100000000"}
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [])
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(internal_nav, "_http_get", http_get)
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.idle_spl_sol == Decimal("0")
    assert result.degraded is True
    assert result.degraded_mints == ["IDLEMINT"]
    assert result.warnings == [
        "degraded Jupiter valuation for IDLEMINT amount_raw=500000000"
    ]

    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
        ]
    )

    assert code == 1
    assert not path.exists()


def test_idle_spl_reference_immaterial_passes_clean_without_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    calls: list[str] = []

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "mint": "IDLEMINT",
                                        "tokenAmount": {"amount": "1000"},
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        raise AssertionError(method)

    def http_get(url: str) -> dict[str, Any]:
        calls.append(url)
        if "amount=1000&" in url:
            raise _http_error(
                400,
                b'{"error":"Cannot compute other amount threshold, with amount 1"}',
            )
        if "amount=1000000000&" in url:
            return {"outAmount": "1000"}
        raise AssertionError(url)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [])
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(internal_nav, "_http_get", http_get)
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.idle_spl_sol == Decimal("1.000E-12")
    assert result.degraded is False
    assert result.degraded_mints == []
    assert result.warnings == []
    assert len(calls) == 2

    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
            "--timestamp",
            "2026-05-24T12:00:00Z",
        ]
    )

    assert code == 0


def test_idle_spl_u64_overflow_degrades_with_warning(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    amount = internal_nav.U64_MAX + 1

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "mint": "IDLEMINT",
                                        "tokenAmount": {"amount": str(amount)},
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        raise AssertionError(method)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [])
    monkeypatch.setattr(
        internal_nav,
        "_http_get",
        lambda _url: (_ for _ in ()).throw(AssertionError("HTTP should be skipped")),
    )
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.idle_spl_sol == Decimal("0")
    assert result.total_nav_sol == Decimal("1")
    assert result.degraded is True
    assert result.degraded_mints == ["IDLEMINT"]
    assert result.warnings == [
        f"Jupiter amount exceeds u64 for IDLEMINT amount_raw={amount}; likely decode artifact"
    ]


def test_idle_spl_suspicious_material_amount_degrades_and_blocks_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    amount = internal_nav.SUSPICIOUS_SPL_RAW_AMOUNT + 1

    def rpc_call(_rpc: str, method: str, _params: list[Any]) -> dict[str, Any]:
        if method == "getBalance":
            return {"value": 1_000_000_000}
        if method == "getTokenAccountsByOwner":
            return {
                "value": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "mint": "IDLEMINT",
                                        "tokenAmount": {"amount": str(amount)},
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        raise AssertionError(method)

    monkeypatch.setattr(internal_nav, "_get_position_addresses", lambda _r, _w: [])
    monkeypatch.setattr(internal_nav, "_fetch_accounts", lambda _r, _p: [])
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)
    monkeypatch.setattr(internal_nav, "_jupiter_price_cache", {})
    monkeypatch.setattr(internal_nav, "_jupiter_failed_cache", set())
    monkeypatch.setattr(internal_nav, "_http_get", lambda _url: {"outAmount": "1000000000"})
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.idle_spl_sol == Decimal("1")
    assert result.degraded is True
    assert result.degraded_mints == ["IDLEMINT"]
    assert result.warnings == [
        f"suspicious large raw amount IDLEMINT amount_raw={amount}"
    ]

    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
        ]
    )

    assert code == 1
    assert not path.exists()


def test_record_tool_degraded_notes() -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    row = record_tool.build_snapshot_row(
        result=_nav_result(degraded=True, degraded_mints=["MINT1"]),
        path=tmp_path / "snapshots.csv",
        timestamp_arg="2026-05-24T12:00:00Z",
        net_contribution_arg="44.6",
    )

    assert row["notes"] == "degraded: MINT1"


def test_record_tool_warning_notes() -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    result = _nav_result()
    result.warnings = ["immaterial reference-priced mint MINT1 value<0.01 SOL"]

    row = record_tool.build_snapshot_row(
        result=result,
        path=tmp_path / "snapshots.csv",
        timestamp_arg="2026-05-24T12:00:00Z",
        net_contribution_arg="44.6",
    )

    assert row["notes"] == (
        "warnings: immaterial reference-priced mint MINT1 value<0.01 SOL"
    )


def test_record_tool_zero_nav_exits(monkeypatch: pytest.MonkeyPatch) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(
        record_tool,
        "compute_nav",
        lambda _rpc, _wallet: (_ for _ in ()).throw(
            RuntimeError("zero NAV result: 0 positions and 0 free SOL")
        ),
    )
    path = tmp_path / "snapshots.csv"

    code = record_tool.main(
        ["--rpc-url", "RPC", "--wallet", "WALLET", "--path", str(path)]
    )

    assert code == 1
    assert not path.exists()


def test_record_tool_degraded_exits_without_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(
        record_tool,
        "compute_nav",
        lambda _rpc, _wallet: _nav_result(
            degraded=True, degraded_mints=["MINT1", "MINT2"]
        ),
    )

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
        ]
    )

    assert code == 1
    assert not path.exists()


def test_record_tool_degraded_writes_with_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(
        record_tool,
        "compute_nav",
        lambda _rpc, _wallet: _nav_result(degraded=True, degraded_mints=["MINT1"]),
    )

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
            "--timestamp",
            "2026-05-24T12:00:00Z",
            "--allow-degraded",
        ]
    )

    assert code == 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["notes"] == "degraded: MINT1"


def test_record_tool_writes_with_warnings_without_degraded_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    result = _nav_result()
    result.warnings = ["immaterial reference-priced mint MINT1 value<0.01 SOL"]
    monkeypatch.setattr(record_tool, "_load_env_file", lambda: None)
    monkeypatch.setattr(record_tool, "compute_nav", lambda _rpc, _wallet: result)

    code = record_tool.main(
        [
            "--rpc-url",
            "RPC",
            "--wallet",
            "WALLET",
            "--path",
            str(path),
            "--net-contribution-sol",
            "44.6",
            "--timestamp",
            "2026-05-24T12:00:00Z",
        ]
    )

    assert code == 0
    with path.open("r", newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    assert rows[0]["notes"] == (
        "warnings: immaterial reference-priced mint MINT1 value<0.01 SOL"
    )


def test_snapshot_net_contribution_from_flows(monkeypatch: pytest.MonkeyPatch) -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    (tmp_path / "capital_flows.csv").write_text("placeholder", encoding="utf-8")
    monkeypatch.setattr(
        record_tool, "read_flows", lambda _path, _asof: Decimal("44.6")
    )

    row = record_tool.build_snapshot_row(
        result=_nav_result(),
        path=path,
        timestamp_arg="2026-05-24T12:00:00Z",
    )

    assert row["net_contribution_sol"] == "44.600000"


def test_snapshot_net_contribution_carryforward() -> None:
    record_tool = _load_record_tool()
    tmp_path = _tmp_dir()
    path = tmp_path / "snapshots.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=record_tool.FIELDS)
        writer.writeheader()
        writer.writerow(
            {
                "timestamp": "2026-05-23T12:00:00Z",
                "source": "internal",
                "value_sol": "49.000000",
                "value_usd": "",
                "sol_usd": "",
                "net_contribution_sol": "44.6",
                "total_pnl_sol": "4.400000",
                "total_pnl_pct": "9.8655",
                "period_pnl_sol": "",
                "notes": "",
            }
        )

    row = record_tool.build_snapshot_row(
        result=_nav_result(),
        path=path,
        timestamp_arg="2026-05-24T12:00:00Z",
    )

    assert row["net_contribution_sol"] == "44.600000"


def test_bin_math_fraction() -> None:
    total_x_raw, total_y_raw = internal_nav._accumulate_bin_reserves(
        [(0, 500)],
        {0: {0: {"amount_x": 200, "amount_y": 100, "liquidity_supply": 1000}}},
    )

    assert total_x_raw == Decimal("100.0")
    assert total_y_raw == Decimal("50.0")
