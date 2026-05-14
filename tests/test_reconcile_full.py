"""Tests for D-full JSONL reconciliation mode (doc 029)."""

from __future__ import annotations

import json
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from valhalla import reconcile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "reconcile_full"
WALLET = reconcile.DEFAULT_WALLET
FROM_DATE = "2026-04-01"
TO_DATE = "2026-04-30"


def _case_dir(name: str) -> Path:
    path = PROJECT_ROOT / "_temp" / "test_reconcile_full" / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _load_fixture_result() -> reconcile._ReconcileResult:
    cache_dir = FIXTURES
    lpagent = reconcile._load_jsonl_cache(cache_dir, WALLET, FROM_DATE, TO_DATE)
    ours = reconcile._load_positions_csv(FIXTURES / "sample_positions.csv")
    archive_dir = FIXTURES / "archive"
    return reconcile._reconcile_jsonl(
        lpagent, ours, FROM_DATE, TO_DATE, WALLET, archive_dir
    )


# ---------------------------------------------------------------------------
# AC-5: Matching counts
# ---------------------------------------------------------------------------


def test_reconcile_counts() -> None:
    result = _load_fixture_result()

    assert len(result.matched) == 10
    assert len(result.lpagent_only) == 5
    assert len(result.ours_only) == 5


# ---------------------------------------------------------------------------
# AC-6: Sub-categorisation — ours-only
# ---------------------------------------------------------------------------


def test_ours_only_subcategories() -> None:
    result = _load_fixture_result()
    reasons = {row.full_address: row.reason for row in result.ours_only}

    assert reasons["TOKEN_OURS_OLDER"] == "older_than_retention"
    assert reasons["TOKEN_OURS_ZERO_PNL"] == "lpagent_dropped"
    assert reasons["TOKEN_OURS_OTHER_WALLET"] == "wallet_not_tracked"
    assert reasons["TOKEN_OURS_NOT_IN_LP1"] == "not_in_lpagent"
    assert reasons["TOKEN_OURS_NOT_IN_LP2"] == "not_in_lpagent"


def test_ours_only_reason_older_than_retention() -> None:
    row = {
        "datetime_close": "2026-03-31T23:59:59",
        "pnl_sol": "0.01",
        "source_wallet": WALLET,
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", WALLET)
    assert reason == "older_than_retention"


def test_ours_only_reason_lpagent_dropped_zero() -> None:
    row = {
        "datetime_close": "2026-04-15T10:00:00",
        "pnl_sol": "0",
        "source_wallet": WALLET,
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", WALLET)
    assert reason == "lpagent_dropped"


def test_ours_only_reason_lpagent_dropped_empty() -> None:
    row = {
        "datetime_close": "2026-04-15T10:00:00",
        "pnl_sol": "",
        "source_wallet": WALLET,
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", WALLET)
    assert reason == "lpagent_dropped"


def test_ours_only_reason_lpagent_dropped_eight_decimal_zeros() -> None:
    """pnl_sol = '0.00000000' (8 decimal places) must be treated as zero."""
    row = {
        "datetime_close": "2026-04-15T10:00:00",
        "pnl_sol": "0.00000000",
        "source_wallet": WALLET,
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", WALLET)
    assert reason == "lpagent_dropped"


def test_ours_only_reason_wallet_not_tracked() -> None:
    row = {
        "datetime_close": "2026-04-15T10:00:00",
        "pnl_sol": "0.05",
        "source_wallet": "OTHERWALLET",
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", WALLET)
    assert reason == "wallet_not_tracked"


def test_ours_only_reason_not_in_lpagent_residual() -> None:
    row = {
        "datetime_close": "2026-04-15T10:00:00",
        "pnl_sol": "0.05",
        "source_wallet": WALLET,
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", WALLET)
    assert reason == "not_in_lpagent"


# ---------------------------------------------------------------------------
# AC-6: Sub-categorisation — lpagent-only
# ---------------------------------------------------------------------------


def test_lpagent_only_subcategories() -> None:
    result = _load_fixture_result()
    hints = {row.token_id: row.hint for row in result.lpagent_only}

    assert hints["TOKEN_LP_ARCHIVE"] == "in_archive"
    assert hints["TOKEN_LP_OTHER_WALLET"] == "outside_wallet_set"
    assert hints["TOKEN_LP_TRULY_MISSING"] == "truly_missing"
    assert hints["TOKEN_LP_OUTSIDE2"] == "outside_wallet_set"
    assert hints["TOKEN_LP_TRULY_MISSING2"] == "truly_missing"


def test_lpagent_only_hint_outside_wallet_set() -> None:
    record = {"owner": "OTHERWALLET", "tokenId": "TOKEN_X"}
    archive_ids: frozenset[str] = frozenset()
    hint = reconcile._lpagent_only_hint_jsonl("TOKEN_X", record, WALLET, archive_ids)
    assert hint == "outside_wallet_set"


def test_lpagent_only_hint_in_archive() -> None:
    record = {"owner": WALLET, "tokenId": "TOKEN_X"}
    archive_ids: frozenset[str] = frozenset(["TOKEN_X"])
    hint = reconcile._lpagent_only_hint_jsonl("TOKEN_X", record, WALLET, archive_ids)
    assert hint == "in_archive"


def test_lpagent_only_hint_truly_missing() -> None:
    record = {"owner": WALLET, "tokenId": "TOKEN_X"}
    archive_ids: frozenset[str] = frozenset()
    hint = reconcile._lpagent_only_hint_jsonl("TOKEN_X", record, WALLET, archive_ids)
    assert hint == "truly_missing"


# ---------------------------------------------------------------------------
# AC-7: Aggregates
# ---------------------------------------------------------------------------


def test_aggregates() -> None:
    """Two wallets, two days — verify aggregate grouping."""
    wallet_a = "WALLET_AAAA"
    wallet_b = "WALLET_BBBB"

    # Build minimal lpagent and positions dicts directly
    lpagent: dict[str, dict] = {
        "TK_A_D1": {
            "tokenId": "TK_A_D1",
            "pnlNative": "0.100000",
            "updatedAt": "2026-04-01T10:00:00Z",
            "owner": wallet_a,
        },
        "TK_A_D2": {
            "tokenId": "TK_A_D2",
            "pnlNative": "0.200000",
            "updatedAt": "2026-04-02T10:00:00Z",
            "owner": wallet_a,
        },
        "TK_B_D1": {
            "tokenId": "TK_B_D1",
            "pnlNative": "0.300000",
            "updatedAt": "2026-04-01T10:00:00Z",
            "owner": wallet_b,
        },
        "TK_B_D2": {
            "tokenId": "TK_B_D2",
            "pnlNative": "0.400000",
            "updatedAt": "2026-04-02T10:00:00Z",
            "owner": wallet_b,
        },
    }
    ours: dict[str, dict[str, str]] = {
        "TK_A_D1": {
            "full_address": "TK_A_D1",
            "token": "A1",
            "pnl_sol": "0.110000",
            "datetime_close": "2026-04-01T11:00:00",
            "source_wallet": wallet_a,
        },
        "TK_A_D2": {
            "full_address": "TK_A_D2",
            "token": "A2",
            "pnl_sol": "0.210000",
            "datetime_close": "2026-04-02T11:00:00",
            "source_wallet": wallet_a,
        },
        "TK_B_D1": {
            "full_address": "TK_B_D1",
            "token": "B1",
            "pnl_sol": "0.310000",
            "datetime_close": "2026-04-01T11:00:00",
            "source_wallet": wallet_b,
        },
        "TK_B_D2": {
            "full_address": "TK_B_D2",
            "token": "B2",
            "pnl_sol": "0.410000",
            "datetime_close": "2026-04-02T11:00:00",
            "source_wallet": wallet_b,
        },
    }
    archive_dir = FIXTURES / "archive"

    result = reconcile._reconcile_jsonl(
        lpagent, ours, "2026-04-01", "2026-04-02", wallet_a, archive_dir
    )

    assert len(result.matched) == 4
    assert len(result.wallet_aggregates) == 2
    assert len(result.day_aggregates) == 2

    wa_map = {wa.wallet: wa for wa in result.wallet_aggregates}
    assert wallet_a in wa_map
    assert wallet_b in wa_map
    assert wa_map[wallet_a].matched_count == 2
    assert wa_map[wallet_b].matched_count == 2
    assert wa_map[wallet_a].pnl_ours_sol == Decimal("0.110000") + Decimal("0.210000")
    assert wa_map[wallet_b].pnl_ours_sol == Decimal("0.310000") + Decimal("0.410000")

    da_map = {da.day: da for da in result.day_aggregates}
    assert "2026-04-01" in da_map
    assert "2026-04-02" in da_map
    assert da_map["2026-04-01"].matched_count == 2
    assert da_map["2026-04-02"].matched_count == 2


# ---------------------------------------------------------------------------
# AC-1 / AC-2: JSONL load
# ---------------------------------------------------------------------------


def test_load_jsonl_cache_filters_by_date() -> None:
    """Only records with updatedAt in [from, to] are included."""
    lpagent = reconcile._load_jsonl_cache(FIXTURES, WALLET, FROM_DATE, TO_DATE)
    # TOKEN_LP_* have updatedAt 2026-04-15 — in range
    assert "TOKEN_LP_ARCHIVE" in lpagent
    # All 15 records in the fixture have updatedAt within [2026-04-01, 2026-04-30]
    assert len(lpagent) == 15


def test_load_jsonl_cache_dedup_newer_wins() -> None:
    """Dedup: newer updatedAt wins for same tokenId."""
    cache_dir = _case_dir("dedup-newer-wins")
    jsonl_path = cache_dir / "positions_J4tkG.jsonl"
    jsonl_path.write_text(
        '{"tokenId": "TK1", "updatedAt": "2026-04-10T10:00:00Z", "pnlNative": "0.1"}\n'
        '{"tokenId": "TK1", "updatedAt": "2026-04-20T10:00:00Z", "pnlNative": "0.9"}\n',
        encoding="utf-8",
    )
    result = reconcile._load_jsonl_cache(cache_dir, WALLET, "2026-04-01", "2026-04-30")
    assert len(result) == 1
    assert result["TK1"]["pnlNative"] == "0.9"


def test_load_jsonl_cache_missing_file_raises() -> None:
    cache_dir = _case_dir("missing-jsonl")
    with pytest.raises(FileNotFoundError, match="JSONL cache not found"):
        reconcile._load_jsonl_cache(cache_dir, WALLET, FROM_DATE, TO_DATE)


def test_load_jsonl_cache_empty_file_raises() -> None:
    cache_dir = _case_dir("empty-jsonl")
    (cache_dir / f"positions_{WALLET[:5]}.jsonl").write_text("", encoding="utf-8")
    with pytest.raises(FileNotFoundError, match="JSONL cache not found"):
        reconcile._load_jsonl_cache(cache_dir, WALLET, FROM_DATE, TO_DATE)


def test_load_jsonl_cache_skips_empty_tokenid(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC-10: Empty tokenId in JSONL record is skipped with warning."""
    cache_dir = _case_dir("skip-empty-tokenid")
    jsonl_path = cache_dir / f"positions_{WALLET[:5]}.jsonl"
    jsonl_path.write_text(
        '{"tokenId": "", "updatedAt": "2026-04-10T10:00:00Z", "pnlNative": "0.1"}\n'
        '{"updatedAt": "2026-04-10T10:00:00Z", "pnlNative": "0.2"}\n'
        '{"tokenId": "GOOD_TOKEN",'
        ' "updatedAt": "2026-04-10T10:00:00Z", "pnlNative": "0.3"}\n',
        encoding="utf-8",
    )
    with caplog.at_level("WARNING"):
        result = reconcile._load_jsonl_cache(cache_dir, WALLET, FROM_DATE, TO_DATE)
    assert len(result) == 1
    assert "GOOD_TOKEN" in result
    assert "empty tokenId" in caplog.text


def test_load_jsonl_cache_skips_malformed_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cache_dir = _case_dir("malformed-line")
    jsonl_path = cache_dir / f"positions_{WALLET[:5]}.jsonl"
    jsonl_path.write_text(
        "NOT JSON\n"
        '{"tokenId": "GOOD_TOKEN",'
        ' "updatedAt": "2026-04-10T10:00:00Z", "pnlNative": "0.3"}\n',
        encoding="utf-8",
    )
    with caplog.at_level("WARNING"):
        result = reconcile._load_jsonl_cache(cache_dir, WALLET, FROM_DATE, TO_DATE)
    assert len(result) == 1
    assert "GOOD_TOKEN" in result


# ---------------------------------------------------------------------------
# AC-3: Watermark loading
# ---------------------------------------------------------------------------


def test_load_watermark_returns_dict() -> None:
    wm_dir = _case_dir("watermark-present")
    wm = {"min_safe_open_date": "2026-03-01", "wallet": WALLET}
    (wm_dir / "watermark.json").write_text(json.dumps(wm), encoding="utf-8")
    result = reconcile._load_watermark(wm_dir)
    assert result is not None
    assert result["min_safe_open_date"] == "2026-03-01"


def test_load_watermark_missing_returns_none() -> None:
    wm_dir = _case_dir("watermark-missing")
    result = reconcile._load_watermark(wm_dir)
    assert result is None


def test_load_watermark_malformed_returns_none() -> None:
    wm_dir = _case_dir("watermark-malformed")
    (wm_dir / "watermark.json").write_text("NOT JSON", encoding="utf-8")
    result = reconcile._load_watermark(wm_dir)
    assert result is None


# ---------------------------------------------------------------------------
# Archive loading
# ---------------------------------------------------------------------------


def test_load_archive_token_ids() -> None:
    ids = reconcile._load_archive_token_ids(FIXTURES / "archive")
    assert "TOKEN_LP_ARCHIVE" in ids
    assert "TOKEN_OLD_ARCHIVED" in ids


def test_load_archive_missing_dir_returns_empty() -> None:
    ids = reconcile._load_archive_token_ids(Path("/nonexistent/path"))
    assert ids == frozenset()


def test_load_archive_skips_corrupt_file(caplog: pytest.LogCaptureFixture) -> None:
    archive_dir = _case_dir("archive-corrupt") / "archive"
    archive_dir.mkdir(parents=True)
    (archive_dir / "good.json").write_text('[{"tokenId": "GOOD"}]', encoding="utf-8")
    (archive_dir / "bad.json").write_text("NOT JSON", encoding="utf-8")
    with caplog.at_level("WARNING"):
        ids = reconcile._load_archive_token_ids(archive_dir)
    assert "GOOD" in ids
    assert "corrupt archive file" in caplog.text


# ---------------------------------------------------------------------------
# AC-9: JSONL mode CSVs have no # WARNING: prefix
# ---------------------------------------------------------------------------


def test_jsonl_csvs_have_no_warning_prefix() -> None:
    result = _load_fixture_result()
    out_dir = _case_dir("csv-no-warning")

    paths = reconcile._render_csvs_jsonl(result, FROM_DATE, TO_DATE, out_dir)

    assert len(paths) == 3
    for path in paths:
        first_line = path.read_text(encoding="utf-8").splitlines()[0]
        assert not first_line.startswith("# WARNING:")


# ---------------------------------------------------------------------------
# AC-1: Console output header
# ---------------------------------------------------------------------------


def test_render_console_jsonl_header(capsys: pytest.CaptureFixture[str]) -> None:
    result = _load_fixture_result()
    reconcile._render_console_jsonl(result, FROM_DATE, TO_DATE)
    output = capsys.readouterr().out
    assert "JSONL cache" in output
    assert "legacy cache" not in output
    assert f"updatedAt in [{FROM_DATE}, {TO_DATE}]" in output
    assert f"datetime_close in [{FROM_DATE}, {TO_DATE}]" in output


# ---------------------------------------------------------------------------
# AC-3: Coverage warning in console AND markdown
# ---------------------------------------------------------------------------


def test_coverage_warning_in_console(capsys: pytest.CaptureFixture[str]) -> None:
    result = reconcile._ReconcileResult(
        matched=[],
        lpagent_only=[],
        ours_only=[],
        coverage_warning=(
            "Warning: JSONL coverage starts 2026-03-01;"
            " requested window starts 2026-02-11."
        ),
    )
    reconcile._render_console_jsonl(result, "2026-02-11", TO_DATE)
    output = capsys.readouterr().out
    assert "Warning: JSONL coverage starts 2026-03-01" in output


def test_coverage_warning_in_markdown() -> None:
    warning = (
        "Warning: JSONL coverage starts 2026-03-01;"
        " requested window starts 2026-02-11."
    )
    result = reconcile._ReconcileResult(
        matched=[],
        lpagent_only=[],
        ours_only=[],
        coverage_warning=warning,
    )
    out_dir = _case_dir("markdown-warning")
    path = reconcile._render_markdown_jsonl(result, "2026-02-11", TO_DATE, out_dir)
    content = path.read_text(encoding="utf-8")
    assert "Warning: JSONL coverage starts 2026-03-01" in content


# ---------------------------------------------------------------------------
# AC-8: --wallet CLI flag and env fallback
# ---------------------------------------------------------------------------


def test_wallet_prefix_from_default() -> None:
    """Default wallet prefix is first 5 chars of DEFAULT_WALLET."""
    assert reconcile.DEFAULT_WALLET[:5] == "J4tkG"


def test_load_jsonl_cache_wallet_prefix() -> None:
    """_load_jsonl_cache uses wallet[:5] as filename prefix."""
    cache_dir = _case_dir("wallet-prefix")
    wallet = "ABCDE12345FGHIJ"
    (cache_dir / "positions_ABCDE.jsonl").write_text(
        '{"tokenId": "TK", "updatedAt": "2026-04-10T10:00:00Z", "pnlNative": "0.1"}\n',
        encoding="utf-8",
    )
    result = reconcile._load_jsonl_cache(cache_dir, wallet, "2026-04-01", "2026-04-30")
    assert "TK" in result


# ---------------------------------------------------------------------------
# CLI smoke: JSONL mode (AC-1)
# ---------------------------------------------------------------------------


def test_cli_jsonl_smoke() -> None:
    """CLI smoke test: JSONL mode produces output with correct header."""
    out_dir = _case_dir("cli-jsonl-smoke")
    cache_dir = out_dir / "lpagent_cache"
    cache_dir.mkdir(parents=True)
    (cache_dir / f"positions_{WALLET[:5]}.jsonl").write_text(
        (FIXTURES / f"positions_{WALLET[:5]}.jsonl").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (out_dir / "positions.csv").write_text(
        (FIXTURES / "sample_positions.csv").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "valhalla.reconcile",
            "--from",
            FROM_DATE,
            "--to",
            TO_DATE,
            "--output-dir",
            str(out_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, f"stderr: {result.stderr}"
    assert "JSONL cache" in result.stdout
    assert "legacy cache" not in result.stdout
    assert "Error: --legacy-cache is required" not in result.stderr
    md_path = out_dir / f"reconciliation_{FROM_DATE}_{TO_DATE}.md"
    assert md_path.exists()


def test_cli_jsonl_missing_file_exits_1() -> None:
    """AC-2: Missing JSONL file exits 1 with clear error."""
    out_dir = _case_dir("cli-jsonl-missing")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "valhalla.reconcile",
            "--from",
            FROM_DATE,
            "--to",
            TO_DATE,
            "--output-dir",
            str(out_dir),
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "JSONL cache not found" in result.stderr
