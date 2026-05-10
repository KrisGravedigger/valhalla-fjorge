import argparse
import csv
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from tools.record_portfolio_snapshot import append_snapshot, build_snapshot
from valhalla.scoreboard import build_scoreboard, render_console, render_markdown


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _case_dir(name: str) -> Path:
    path = PROJECT_ROOT / "_temp" / "test_scoreboard" / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _args(
    path: Path,
    source: str = "lpagent",
    value_sol: str = "63.84",
    net_contribution_sol: str | None = None,
    timestamp: str | None = "2026-05-10T12:00:00Z",
) -> argparse.Namespace:
    return argparse.Namespace(
        source=source,
        value_sol=value_sol,
        value_usd=None,
        sol_usd=None,
        net_contribution_sol=net_contribution_sol,
        timestamp=timestamp,
        notes="",
        path=str(path),
    )


def _read_snapshot_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_build_scoreboard_latest_per_source() -> None:
    rows = [
        {
            "timestamp": "2026-05-09T12:00:00Z",
            "source": "lpagent",
            "value_sol": "62.000000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "17.400000",
            "total_pnl_pct": "39.0135",
        },
        {
            "timestamp": "2026-05-10T12:00:00Z",
            "source": "lpagent",
            "value_sol": "63.840000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "19.240000",
            "total_pnl_pct": "43.1390",
        },
        {
            "timestamp": "2026-05-10T10:00:00Z",
            "source": "fabriq",
            "value_sol": "60.140000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "15.540000",
            "total_pnl_pct": "34.8430",
        },
    ]

    table = build_scoreboard(rows)

    assert [row["source"] for row in table] == ["fabriq", "lpagent"]
    assert table[1]["value_sol"] == "63.840000"


def test_build_scoreboard_same_second_tiebreak_uses_last_appended() -> None:
    rows = [
        {
            "timestamp": "2026-05-10T12:00:00Z",
            "source": "lpagent",
            "value_sol": "63.000000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "18.400000",
            "total_pnl_pct": "41.2556",
        },
        {
            "timestamp": "2026-05-10T12:00:00Z",
            "source": "lpagent",
            "value_sol": "64.000000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "19.400000",
            "total_pnl_pct": "43.4978",
        },
    ]

    table = build_scoreboard(rows)

    assert table[0]["value_sol"] == "64.000000"


def test_build_scoreboard_empty() -> None:
    assert build_scoreboard([]) == []


def test_render_console_skips_malformed_numeric_row(capsys: pytest.CaptureFixture[str]) -> None:
    table = [
        {
            "timestamp": "2026-05-10T12:00:00Z",
            "source": "lpagent",
            "value_sol": "not-a-number",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "19.240000",
            "total_pnl_pct": "43.1390",
        },
        {
            "timestamp": "2026-05-10T10:00:00Z",
            "source": "fabriq",
            "value_sol": "60.140000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "15.540000",
            "total_pnl_pct": "34.8430",
        },
    ]

    render_console(table, "2026-05-10")

    captured = capsys.readouterr()
    assert "Warning: skipping malformed snapshot row" in captured.err
    assert "lpagent" not in captured.out
    assert "not-a-number" not in captured.out
    assert "fabriq" in captured.out


def test_render_markdown_creates_file() -> None:
    path = _case_dir("markdown") / "portfolio_scoreboard.md"
    table = [
        {
            "timestamp": "2026-05-10T12:00:00Z",
            "source": "lpagent",
            "value_sol": "63.840000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "19.240000",
            "total_pnl_pct": "43.1390",
        },
        {
            "timestamp": "2026-05-10T10:00:00Z",
            "source": "fabriq",
            "value_sol": "60.140000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "15.540000",
            "total_pnl_pct": "34.8430",
        },
    ]

    render_markdown(table, "2026-05-10", path)

    text = path.read_text(encoding="utf-8")
    assert "# Portfolio Scoreboard - 2026-05-10" in text
    assert "lpagent" in text
    assert "fabriq" in text
    assert "_NAV PnL = current portfolio value - external net contributions." in text


def test_render_markdown_skips_malformed_numeric_row(capsys: pytest.CaptureFixture[str]) -> None:
    path = _case_dir("markdown-malformed") / "portfolio_scoreboard.md"
    table = [
        {
            "timestamp": "2026-05-10T12:00:00Z",
            "source": "lpagent",
            "value_sol": "not-a-number",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "19.240000",
            "total_pnl_pct": "43.1390",
        },
        {
            "timestamp": "2026-05-10T10:00:00Z",
            "source": "fabriq",
            "value_sol": "60.140000",
            "net_contribution_sol": "44.600000",
            "total_pnl_sol": "15.540000",
            "total_pnl_pct": "34.8430",
        },
    ]

    render_markdown(table, "2026-05-10", path)

    captured = capsys.readouterr()
    text = path.read_text(encoding="utf-8")
    assert "Warning: skipping malformed snapshot row" in captured.err
    assert "lpagent" not in text
    assert "not-a-number" not in text
    assert "fabriq" in text


def test_snapshot_auto_reads_capital_flows(monkeypatch: pytest.MonkeyPatch) -> None:
    import valhalla.capital_flow as cf_mod

    tmp_path = _case_dir("auto-read")
    path = tmp_path / "portfolio_snapshots.csv"
    (tmp_path / "capital_flows.csv").write_text("timestamp_utc,wallet,type,sol_amount,tx_signature,notes\n")
    monkeypatch.setattr(cf_mod, "read_flows", lambda ledger_path, asof: Decimal("44.6"))

    row = build_snapshot(_args(path), previous=None)

    assert row["net_contribution_sol"] == "44.600000"
    assert row["total_pnl_sol"] == "19.240000"


def test_snapshot_auto_reads_zero_capital_flows(monkeypatch: pytest.MonkeyPatch) -> None:
    import valhalla.capital_flow as cf_mod

    tmp_path = _case_dir("zero-flows")
    path = tmp_path / "portfolio_snapshots.csv"
    (tmp_path / "capital_flows.csv").write_text("timestamp_utc,wallet,type,sol_amount,tx_signature,notes\n")
    monkeypatch.setattr(cf_mod, "read_flows", lambda ledger_path, asof: Decimal("0.000000"))

    row = build_snapshot(_args(path), previous=None)

    assert row["net_contribution_sol"] == "0.000000"
    assert row["total_pnl_sol"] == "63.840000"
    assert row["total_pnl_pct"] == ""


def test_snapshot_fallback_cli_arg() -> None:
    tmp_path = _case_dir("cli-fallback")
    path = tmp_path / "portfolio_snapshots.csv"

    row = build_snapshot(_args(path, net_contribution_sol="44.6"), previous=None)

    assert row["net_contribution_sol"] == "44.600000"
    assert row["total_pnl_sol"] == "19.240000"


def test_snapshot_carries_forward_previous_contribution() -> None:
    tmp_path = _case_dir("carry-forward")
    path = tmp_path / "portfolio_snapshots.csv"
    previous = {
        "timestamp": "2026-05-09T12:00:00Z",
        "source": "lpagent",
        "value_sol": "63.000000",
        "net_contribution_sol": "44.600000",
    }

    row = build_snapshot(_args(path), previous=previous)

    assert row["net_contribution_sol"] == "44.600000"
    assert row["total_pnl_sol"] == "19.240000"


def test_snapshot_no_contribution_source_exits() -> None:
    tmp_path = _case_dir("missing-contribution")
    path = tmp_path / "portfolio_snapshots.csv"

    with pytest.raises(SystemExit) as excinfo:
        build_snapshot(_args(path), previous=None)

    assert str(excinfo.value) == (
        "Error: No capital_flows.csv found and --net-contribution-sol not provided. "
        "Provide one of these to record the first snapshot."
    )


def test_snapshot_negative_pnl() -> None:
    tmp_path = _case_dir("negative-pnl")
    path = tmp_path / "portfolio_snapshots.csv"

    row = build_snapshot(
        _args(path, value_sol="40.0", net_contribution_sol="44.6"),
        previous=None,
    )

    assert row["total_pnl_sol"] == "-4.600000"


def test_snapshot_two_same_day_both_append() -> None:
    tmp_path = _case_dir("same-day")
    path = tmp_path / "portfolio_snapshots.csv"
    first = build_snapshot(
        _args(path, value_sol="63.84", net_contribution_sol="44.6", timestamp="2026-05-10T12:00:00Z"),
        previous=None,
    )
    second = build_snapshot(
        _args(path, value_sol="63.84", net_contribution_sol="44.6", timestamp="2026-05-10T13:00:00Z"),
        previous=first,
    )

    append_snapshot(path, first)
    append_snapshot(path, second)

    rows = _read_snapshot_rows(path)
    assert len(rows) == 2
    assert rows[1]["period_pnl_sol"] == "0.000000"
