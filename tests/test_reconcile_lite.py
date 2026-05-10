import csv
import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from valhalla import reconcile


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "reconcile_lite"


def _case_dir(name: str) -> Path:
    path = PROJECT_ROOT / "_temp" / "test_reconcile_lite" / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_fixture_result() -> reconcile._ReconcileResult:
    lpagent = reconcile._load_legacy_cache(FIXTURES / "cache", "2026-04-30", "2026-04-30")
    ours = reconcile._load_positions_csv(FIXTURES / "sample_positions.csv")
    return reconcile._reconcile(lpagent, ours, "2026-04-01")


def _write_positions_csv(path: Path, rows: list[dict[str, str]]) -> None:
    header = (FIXTURES / "sample_positions.csv").read_text(encoding="utf-8").splitlines()[0].split(",")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def test_golden_file_match_counts() -> None:
    result = _load_fixture_result()

    assert len(result.matched) == 10
    assert len(result.lpagent_only) == 5
    assert len(result.ours_only) == 5


def test_dedup_legacy_cache_last_seen_file_wins() -> None:
    lpagent = reconcile._load_legacy_cache(
        FIXTURES / "cache_overlap",
        "2026-04-29",
        "2026-04-30",
    )

    assert sorted(lpagent) == ["TOKEN_MATCH_0001", "TOKEN_MATCH_0002"]
    assert lpagent["TOKEN_MATCH_0001"]["token0Info"]["token_symbol"] == "NEW01"


def test_dedup_legacy_cache_duplicate_within_file() -> None:
    lpagent = reconcile._load_legacy_cache(FIXTURES / "cache", "2026-04-30", "2026-04-30")

    assert len(lpagent) == 15
    assert lpagent["TOKEN_MATCH_0010"]["pnlNative"] == "0.000000"


def test_skip_empty_and_missing_tokenid(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        lpagent = reconcile._load_legacy_cache(FIXTURES / "cache", "2026-04-30", "2026-04-30")

    assert "" not in lpagent
    assert all("EMPTY" != row.get("token") for row in lpagent.values())
    assert "empty tokenId" in caplog.text
    assert len(lpagent) == 15


def test_dedup_positions_csv_prefer_discord(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level("WARNING"):
        ours = reconcile._load_positions_csv(FIXTURES / "sample_positions.csv")

    assert len(ours) == 15
    assert ours["TOKEN_MATCH_0001"]["pnl_source"] == "discord"
    assert ours["TOKEN_MATCH_0001"]["pnl_sol"] == "0.050000"
    assert "Duplicate full_address TOKEN_MATCH_0001" in caplog.text


def test_dedup_positions_csv_two_discord_rows_keep_first(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows = [
        {
            "datetime_open": "2026-04-30T10:00:00",
            "datetime_close": "2026-04-30T10:30:00",
            "token": "FIRST",
            "full_address": "DUPLICATE_TOKEN",
            "pnl_source": "discord",
            "pnl_sol": "0.010000",
        },
        {
            "datetime_open": "2026-04-30T11:00:00",
            "datetime_close": "2026-04-30T11:30:00",
            "token": "SECOND",
            "full_address": "DUPLICATE_TOKEN",
            "pnl_source": "discord",
            "pnl_sol": "0.020000",
        },
    ]
    path = _case_dir("duplicate-discord") / "positions.csv"
    _write_positions_csv(path, rows)

    with caplog.at_level("WARNING"):
        ours = reconcile._load_positions_csv(path)

    assert ours["DUPLICATE_TOKEN"]["token"] == "FIRST"
    assert "Duplicate full_address DUPLICATE_TOKEN" in caplog.text


def test_pnl_diff_math() -> None:
    result = _load_fixture_result()
    row = next(item for item in result.matched if item.full_address == "TOKEN_MATCH_0001")

    assert row.pnl_ours == Decimal("0.050000")
    assert row.pnl_lpagent == Decimal("0.048000")
    assert row.diff_sol == Decimal("0.002000")
    assert row.diff_pct == "+4.17%"


def test_pnl_diff_zero_lpagent() -> None:
    result = _load_fixture_result()
    row = next(item for item in result.matched if item.full_address == "TOKEN_MATCH_0010")

    assert row.pnl_lpagent == Decimal("0.000000")
    assert row.diff_pct == "N/A"


def test_malformed_numeric_is_skipped_without_crash(caplog: pytest.LogCaptureFixture) -> None:
    lpagent = {
        "BAD_TOKEN": {
            "tokenId": "BAD_TOKEN",
            "token0Info": {"token_symbol": "BAD"},
            "pnlNative": "not-a-number",
        }
    }
    ours = {"BAD_TOKEN": {"full_address": "BAD_TOKEN", "token": "BAD", "pnl_sol": "0.1"}}

    with caplog.at_level("WARNING"):
        result = reconcile._reconcile(lpagent, ours, "2026-04-01")

    assert result.matched == []
    assert "malformed pnlNative" in caplog.text


def test_legacy_cache_required(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        reconcile.main(["--from", "2026-04-01", "--to", "2026-04-30"])

    captured = capsys.readouterr()
    assert excinfo.value.code == 1
    assert (
        "Error: --legacy-cache is required. "
        "Non-legacy mode (sub-project D) is not yet implemented."
    ) in captured.err


def test_invalid_date_exits_before_loading(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as excinfo:
        reconcile.main(["--from", "2026-4-1", "--to", "2026-04-30", "--legacy-cache"])

    captured = capsys.readouterr()
    assert excinfo.value.code == 1
    assert "Invalid --from: must be YYYY-MM-DD." in captured.err


def test_warning_banner_is_first_console_line(capsys: pytest.CaptureFixture[str]) -> None:
    result = _load_fixture_result()

    reconcile._render_console(result, "2026-04-01", "2026-04-30")

    captured = capsys.readouterr()
    assert captured.out.splitlines()[0] == reconcile._LEGACY_WARNING.splitlines()[0]


def test_warning_banner_in_markdown() -> None:
    result = _load_fixture_result()

    path = reconcile._render_markdown(
        result,
        "2026-04-01",
        "2026-04-30",
        _case_dir("markdown"),
    )

    first_lines = path.read_text(encoding="utf-8").splitlines()[:10]
    assert reconcile._LEGACY_WARNING.splitlines()[0] in "\n".join(first_lines)
    assert reconcile._LEGACY_WARNING.splitlines()[1] in "\n".join(first_lines)


def test_warning_banner_in_all_csvs() -> None:
    result = _load_fixture_result()

    paths = reconcile._render_csvs(
        result,
        "2026-04-01",
        "2026-04-30",
        _case_dir("csv"),
    )

    assert len(paths) == 3
    for path in paths:
        assert path.read_text(encoding="utf-8").splitlines()[0].startswith("# WARNING:")


def test_date_window_loads_only_existing_files_and_reports_missing() -> None:
    cache_dir = _case_dir("date-window") / "lpagent_cache"
    cache_dir.mkdir()
    for day in ["2026-05-01", "2026-05-02", "2026-05-03", "2026-05-04"]:
        (cache_dir / f"{day}.json").write_text(
            json.dumps([{"tokenId": f"TOKEN_{day}", "pnlNative": "0.0"}]),
            encoding="utf-8",
        )

    lpagent = reconcile._load_legacy_cache(cache_dir, "2026-05-01", "2026-05-10")
    missing = reconcile._missing_cache_dates(cache_dir, "2026-05-01", "2026-05-10")
    notice = reconcile._missing_notice(missing)

    assert len(lpagent) == 4
    assert notice == (
        "Note: cache files for 2026-05-05 through 2026-05-10 not found - "
        "those days may have incomplete coverage."
    )


def test_cli_smoke_with_fixtures() -> None:
    output_dir = _case_dir("cli-smoke") / "output"
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir(parents=True)
    (output_dir / "positions.csv").write_text(
        (FIXTURES / "sample_positions.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (cache_dir / "2026-04-30.json").write_text(
        (FIXTURES / "cache" / "2026-04-30.json").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "valhalla.reconcile",
            "--from",
            "2026-04-30",
            "--to",
            "2026-04-30",
            "--legacy-cache",
            "--output-dir",
            str(output_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert result.stdout.splitlines()[0] == reconcile._LEGACY_WARNING.splitlines()[0]
    assert "Matched:       10 positions" in result.stdout
    assert (output_dir / "reconciliation_2026-04-30_2026-04-30.md").exists()
