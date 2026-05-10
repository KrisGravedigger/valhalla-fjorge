import csv
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from valhalla.capital_flow import FIELDS, check_stale_flows, read_flows


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "tools" / "record_capital_flow.py"


def _write_rows(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _row(
    timestamp_utc: str,
    flow_type: str,
    sol_amount: str,
    tx_signature: str = "",
    wallet: str = "",
    notes: str = "",
) -> dict[str, str]:
    return {
        "timestamp_utc": timestamp_utc,
        "wallet": wallet,
        "type": flow_type,
        "sol_amount": sol_amount,
        "tx_signature": tx_signature,
        "notes": notes,
    }


def _run_record_flow(
    path: Path,
    tx_signature: str,
    timestamp_utc: str = "2026-03-01T10:00:00Z",
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--timestamp-utc",
            timestamp_utc,
            "--wallet",
            "J4tkG",
            "--type",
            "deposit",
            "--sol-amount",
            "5.0",
            "--tx-signature",
            tx_signature,
            "--notes",
            "top-up",
            "--path",
            str(path),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )


def test_record_capital_flow_normal_append_formats_fields(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    result = _run_record_flow(path, "ABC123")

    assert result.returncode == 0
    with path.open("r", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows == [
        {
            "timestamp_utc": "2026-03-01T10:00:00Z",
            "wallet": "J4tkG",
            "type": "deposit",
            "sol_amount": "5.000000",
            "tx_signature": "ABC123",
            "notes": "top-up",
        }
    ]


def test_record_capital_flow_invalid_timestamp_format(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    result = _run_record_flow(path, "ABC123", "2026-03-01T10:00:00")

    assert result.returncode != 0
    assert (
        "Invalid --timestamp-utc: must be YYYY-MM-DDTHH:MM:SSZ (UTC, Z suffix)."
    ) in result.stderr
    assert not path.exists()


def test_record_capital_flow_invalid_timestamp_no_time(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    result = _run_record_flow(path, "ABC123", "2026-03-01")

    assert result.returncode != 0
    assert (
        "Invalid --timestamp-utc: must be YYYY-MM-DDTHH:MM:SSZ (UTC, Z suffix)."
    ) in result.stderr
    assert not path.exists()


def test_record_capital_flow_valid_timestamp(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    result = _run_record_flow(path, "ABC123", "2026-03-01T10:00:00Z")

    assert result.returncode == 0
    with path.open("r", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 1
    assert rows[0]["timestamp_utc"] == "2026-03-01T10:00:00Z"


def test_record_capital_flow_internal_transfer_dry_run_reminder(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--timestamp-utc",
            "2026-03-01T10:00:00Z",
            "--wallet",
            "J4tkG",
            "--type",
            "internal_transfer",
            "--sol-amount",
            "3",
            "--tx-signature",
            "",
            "--notes",
            "rebalance",
            "--path",
            str(path),
            "--dry-run",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "J4tkG,internal_transfer,3.000000,,rebalance" in result.stdout
    assert (
        "Reminder: record the matching leg of this internal transfer "
        "(withdrawal on the source wallet / deposit on the destination wallet) "
        "to keep portfolio-level net contribution correct."
    ) in result.stdout
    assert not path.exists()


def test_read_flows_basic(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    _write_rows(
        path,
        [
            _row("2026-02-11T00:00:00Z", "deposit", "10.000000", "SIG001"),
            _row("2026-03-01T10:00:00Z", "deposit", "5.000000", "SIG002"),
            _row("2026-04-01T12:00:00Z", "withdrawal", "2.000000", "SIG003"),
        ],
    )

    assert read_flows(path, "2026-04-15") == Decimal("13.000000")


def test_read_flows_asof_cutoff(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    _write_rows(
        path,
        [
            _row("2026-02-11T00:00:00Z", "deposit", "10.000000", "SIG001"),
            _row("2026-04-01T12:00:00Z", "withdrawal", "2.000000", "SIG002"),
        ],
    )

    assert read_flows(path, "2026-02-28") == Decimal("10.000000")


def test_read_flows_missing_file(tmp_path: Path) -> None:
    assert read_flows(tmp_path / "missing.csv", "2026-04-15") is None


def test_read_flows_internal_transfer_ignored(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    _write_rows(
        path,
        [
            _row("2026-02-11T00:00:00Z", "deposit", "10.000000", "SIG001"),
            _row("2026-03-01T10:00:00Z", "internal_transfer", "3.000000", "SIG002"),
        ],
    )

    assert read_flows(path, "2026-12-31") == Decimal("10.000000")


def test_read_flows_only_internal_transfers_returns_zero(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    _write_rows(
        path,
        [
            _row("2026-03-01T10:00:00Z", "internal_transfer", "3.000000", "SIG001"),
            _row("2026-03-01T10:05:00Z", "internal_transfer", "3.000000", "SIG002"),
        ],
    )

    assert read_flows(path, "2026-12-31") == Decimal("0.000000")


def test_check_stale_flows_fresh(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    recent = datetime.now(timezone.utc).date() - timedelta(days=5)
    _write_rows(path, [_row(f"{recent}T00:00:00Z", "deposit", "1.000000")])

    assert check_stale_flows(path) is None


def test_check_stale_flows_stale(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    stale = datetime.now(timezone.utc).date() - timedelta(days=20)
    _write_rows(path, [_row(f"{stale}T00:00:00Z", "deposit", "1.000000")])

    assert check_stale_flows(path) == (
        "Warning: capital_flows.csv has not been updated in 20 days. "
        "Record any recent deposits/withdrawals to keep the scoreboard accurate."
    )


def test_record_capital_flow_duplicate_sig(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    first = _run_record_flow(path, "ABC123")
    second = _run_record_flow(path, "ABC123")

    assert first.returncode == 0
    assert second.returncode == 1
    assert (
        "Error: tx_signature ABC123 already exists in capital_flows.csv "
        "— duplicate entry rejected."
    ) in second.stdout
    with path.open("r", newline="", encoding="utf-8") as fh:
        assert len(list(csv.DictReader(fh))) == 1


def test_record_capital_flow_null_sig_allowed(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"

    first = _run_record_flow(path, "")
    second = _run_record_flow(path, "")

    assert first.returncode == 0
    assert second.returncode == 0
    with path.open("r", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert len(rows) == 2
    assert rows[0]["tx_signature"] == ""
    assert rows[1]["tx_signature"] == ""


def test_bootstrap_fixture_uses_confirmed_baseline(tmp_path: Path) -> None:
    path = tmp_path / "capital_flows.csv"
    _write_rows(
        path,
        [
            _row(
                "2026-02-11T00:00:00Z",
                "deposit",
                "44.600000",
                "BOOTSTRAP-2026-02-11",
                "",
                "Initial portfolio capital baseline (PITCH.md 2026-04-30)",
            )
        ],
    )

    assert read_flows(path, "2026-12-31") == Decimal("44.600000")
