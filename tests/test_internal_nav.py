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


def test_jupiter_degraded_no_route(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(internal_nav.time, "sleep", lambda _: None)

    def fail(_: str) -> dict[str, Any]:
        raise _http_error(400, b'{"error":"NO_ROUTES_FOUND"}')

    monkeypatch.setattr(internal_nav, "_http_get", fail)

    assert internal_nav._jupiter_to_sol("MINT", 1000) == (Decimal("0"), True)


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


def test_jupiter_sol_passthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    def unexpected(_: str) -> dict[str, Any]:
        raise AssertionError("HTTP should not be called for SOL")

    monkeypatch.setattr(internal_nav, "_http_get", unexpected)

    assert internal_nav._jupiter_to_sol(internal_nav.SOL_MINT, 1_000_000_000) == (
        Decimal("1"),
        False,
    )


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
        internal_nav, "_jupiter_to_sol", lambda _mint, _amount: (Decimal("0"), True)
    )
    monkeypatch.setattr(internal_nav, "_rpc_call", rpc_call)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.degraded is True
    assert result.degraded_mints == ["MINTX"]


def test_idle_spl_jupiter_degraded_does_not_mark_nav_degraded(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
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
    caplog.set_level(logging.DEBUG)

    result = internal_nav.compute_nav("RPC", "WALLET")

    assert result.idle_spl_sol == Decimal("0")
    assert result.total_nav_sol == Decimal("1")
    assert result.degraded is False
    assert result.degraded_mints == []
    assert "Idle SPL mint IDLEMINT has no reliable Jupiter value; using 0" in caplog.text
    assert "No Jupiter route for IDLEMINT" not in caplog.text


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
