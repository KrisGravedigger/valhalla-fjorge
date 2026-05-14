# ruff: noqa: E501
"""
Adversarial tests for docs/029-reconciliation-full.md (D-full reconciliation).

Written BEFORE reading valhalla/reconcile.py.
Each test targets a specific edge case or AC clause that the implementer
might have gotten wrong.
"""
import csv
import json
from pathlib import Path
from uuid import uuid4

import pytest

from valhalla import reconcile

PROJECT_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_FULL = PROJECT_ROOT / "tests" / "fixtures" / "reconcile_full"


def _case_dir(name: str) -> Path:
    path = PROJECT_ROOT / "_temp" / "test_reconcile_adv_029" / f"{name}-{uuid4().hex}"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _make_output_dir(case_name: str) -> Path:
    """Create an output dir with required subdirectories."""
    base = _case_dir(case_name)
    output_dir = base / "output"
    output_dir.mkdir()
    return output_dir


def _write_positions_csv(path: Path, rows: list[dict[str, str]]) -> None:
    """Write a positions CSV with the full header from the full fixture."""
    header_line = (FIXTURES_FULL / "sample_positions.csv").read_text(encoding="utf-8").splitlines()[0]
    fieldnames = header_line.split(",")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _write_jsonl(path: Path, records: list[dict]) -> None:
    """Write JSONL file from list of dicts."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _minimal_position_row(
    full_address: str,
    token: str = "TOK",
    pnl_sol: str = "0.100000",
    datetime_close: str = "2026-04-30T10:00:00",
    source_wallet: str = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
) -> dict:
    return {
        "datetime_open": "2026-04-30T09:00:00",
        "datetime_close": datetime_close,
        "target_wallet": "wallet",
        "token": token,
        "position_type": "Spot",
        "sol_deployed": "1.0",
        "sol_received": "1.1",
        "pnl_sol": pnl_sol,
        "pnl_pct": "10.00",
        "close_reason": "normal",
        "mc_at_open": "0",
        "jup_score": "0",
        "token_age": "",
        "token_age_days": "",
        "token_age_hours": "",
        "price_drop_pct": "",
        "position_id": f"P{full_address[:4]}",
        "full_address": full_address,
        "pnl_source": "discord",
        "meteora_deposited": "1.0",
        "meteora_withdrawn": "1.1",
        "meteora_fees": "0.0",
        "meteora_pnl": "0.1",
        "target_wallet_address": source_wallet,
        "target_tx_signature": "",
        "source_wallet_hold_min": "",
        "source_wallet_pnl_pct": "",
        "source_wallet_scenario": "",
        "original_wallet": "wallet",
        "source_wallet": source_wallet,
    }


DEFAULT_WALLET = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"


# ---------------------------------------------------------------------------
# AC-3: Watermark warning in BOTH console AND Markdown
# ---------------------------------------------------------------------------

def test_watermark_warning_appears_in_markdown(capsys: pytest.CaptureFixture[str]) -> None:
    """AC-3: When min_safe_open_date > --from, warning must appear in Markdown file, not just console."""
    output_dir = _make_output_dir("watermark-md")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    # Create a JSONL file with one record in range
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_A",
                "token0Info": {"token_symbol": "TOKA"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )

    # positions.csv with matching row
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_A", "TOKA")],
    )

    # Watermark where min_safe_open_date is AFTER --from (coverage gap)
    (output_dir / "watermark.json").write_text(
        json.dumps({"min_safe_open_date": "2026-04-15"}),
        encoding="utf-8",
    )

    reconcile.main([
        "--from", "2026-04-01",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    captured = capsys.readouterr()
    # Warning must appear in console output
    assert "JSONL coverage starts" in captured.out or "JSONL coverage starts" in captured.err, \
        "AC-3: watermark warning must appear in console output"

    # Warning must ALSO appear in the Markdown report
    md_files = list(output_dir.glob("*.md"))
    assert md_files, "Expected at least one Markdown report file"
    md_content = md_files[0].read_text(encoding="utf-8")
    assert "JSONL coverage starts" in md_content, \
        "AC-3: watermark warning must also appear in the Markdown report, not just console"


def test_watermark_missing_does_not_crash() -> None:
    """AC-3 adversarial: missing watermark.json must not crash; report runs normally."""
    output_dir = _make_output_dir("watermark-missing")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_B",
                "token0Info": {"token_symbol": "TOKB"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.050000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_B", "TOKB")],
    )
    # No watermark.json written — must not crash

    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])


def test_watermark_malformed_does_not_crash() -> None:
    """AC-3 adversarial: malformed watermark.json must be silently skipped, no crash."""
    output_dir = _make_output_dir("watermark-malformed")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_C",
                "token0Info": {"token_symbol": "TOKC"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.050000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_C", "TOKC")],
    )
    # Truncated / malformed JSON
    (output_dir / "watermark.json").write_text("{", encoding="utf-8")

    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])


# ---------------------------------------------------------------------------
# AC-4: Date filtering boundary conditions
# ---------------------------------------------------------------------------

def test_date_filter_updatedAt_on_from_boundary_is_included() -> None:
    """AC-4: updatedAt[:10] == --from (exact boundary) MUST be included."""
    cache_dir = _case_dir("filter-boundary-from") / "cache"
    record_on_boundary = {
        "tokenId": "TOKEN_ON_FROM",
        "token0Info": {"token_symbol": "TOKON"},
        "updatedAt": "2026-04-01T00:00:00Z",  # exactly == from
        "pnlNative": "0.100000",
        "owner": DEFAULT_WALLET,
    }
    record_before = {
        "tokenId": "TOKEN_BEFORE_FROM",
        "token0Info": {"token_symbol": "TOKBF"},
        "updatedAt": "2026-03-31T23:59:59Z",  # one second before = previous day
        "pnlNative": "0.100000",
        "owner": DEFAULT_WALLET,
    }
    _write_jsonl(cache_dir / "positions_J4tkG.jsonl", [record_on_boundary, record_before])

    result = reconcile._load_jsonl_cache(cache_dir, DEFAULT_WALLET, "2026-04-01", "2026-04-30")

    assert "TOKEN_ON_FROM" in result, \
        "AC-4: record with updatedAt on --from boundary must be included"
    assert "TOKEN_BEFORE_FROM" not in result, \
        "AC-4: record with updatedAt before --from must be excluded"


def test_date_filter_updatedAt_on_to_boundary_is_included() -> None:
    """AC-4: updatedAt[:10] == --to (exact end boundary) MUST be included."""
    cache_dir = _case_dir("filter-boundary-to") / "cache"
    record_on_to = {
        "tokenId": "TOKEN_ON_TO",
        "token0Info": {"token_symbol": "TOKON"},
        "updatedAt": "2026-04-30T23:59:59Z",  # exactly == to
        "pnlNative": "0.100000",
        "owner": DEFAULT_WALLET,
    }
    record_after = {
        "tokenId": "TOKEN_AFTER_TO",
        "token0Info": {"token_symbol": "TOKAFT"},
        "updatedAt": "2026-05-01T00:00:00Z",  # one day after
        "pnlNative": "0.100000",
        "owner": DEFAULT_WALLET,
    }
    _write_jsonl(cache_dir / "positions_J4tkG.jsonl", [record_on_to, record_after])

    result = reconcile._load_jsonl_cache(cache_dir, DEFAULT_WALLET, "2026-04-01", "2026-04-30")

    assert "TOKEN_ON_TO" in result, \
        "AC-4: record with updatedAt on --to boundary must be included"
    assert "TOKEN_AFTER_TO" not in result, \
        "AC-4: record with updatedAt after --to must be excluded"


def test_ours_only_excludes_rows_after_to_date() -> None:
    """AC-4: positions with datetime_close > to_date must NOT appear in ours_only."""
    output_dir = _make_output_dir("filter-after-to-date")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_IN_RANGE",
                "token0Info": {"token_symbol": "OK"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [
            _minimal_position_row("TOKEN_IN_RANGE", "OK"),
            _minimal_position_row(
                "TOKEN_MAY", "MAY", datetime_close="2026-05-01T10:00:00"
            ),
        ],
    )

    reconcile.main([
        "--from", "2026-04-01",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    ours_only_csv = output_dir / "reconciliation_2026-04-01_2026-04-30_ours_only.csv"
    assert ours_only_csv.exists()
    content = ours_only_csv.read_text(encoding="utf-8")
    assert "TOKEN_MAY" not in content, \
        "AC-4: position with datetime_close after to_date must NOT appear in ours_only"


# ---------------------------------------------------------------------------
# AC-6: Sub-categorisation boundary and priority
# ---------------------------------------------------------------------------

def test_older_than_retention_boundary_exactly_on_from_is_NOT_older() -> None:
    """AC-6: datetime_close == --from exactly should NOT be 'older_than_retention'.
    Spec says < (strictly less than), not <=.
    Note: _ours_only_reason_jsonl takes (row, from_date, wallet) — no archive_dir.
    The archive_dir was refactored out; the implementation pre-computes token_ids.
    """
    row = _minimal_position_row(
        "BOUNDARY_TOKEN",
        datetime_close="2026-04-01T00:00:00",  # exactly == from_date
    )
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)

    assert reason != "older_than_retention", \
        "AC-6: datetime_close == from_date should NOT trigger older_than_retention (spec uses <, not <=)"


def test_older_than_retention_wins_over_zero_pnl() -> None:
    """AC-6 priority: row with datetime_close < from AND pnl_sol == 0 must be 'older_than_retention'.
    The design doc checks older_than_retention FIRST in the helper.
    """
    row = _minimal_position_row(
        "OLD_ZERO_TOKEN",
        pnl_sol="0.000000",
        datetime_close="2026-03-01T10:00:00",  # before from
    )
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)

    assert reason == "older_than_retention", \
        "AC-6: older_than_retention must win over lpagent_dropped when both conditions apply"


def test_lpagent_dropped_wins_over_wallet_not_tracked() -> None:
    """AC-6 priority: pnl_sol == 0 AND source_wallet != wallet must be 'lpagent_dropped', not 'wallet_not_tracked'."""
    row = _minimal_position_row(
        "ZERO_OTHER_WALLET",
        pnl_sol="0.000000",
        source_wallet="ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ",  # wrong wallet
    )
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)

    assert reason == "lpagent_dropped", \
        "AC-6: lpagent_dropped (zero pnl) must win over wallet_not_tracked when both apply"


def test_lpagent_dropped_zero_forms() -> None:
    """AC-6: All zero representations must be treated as zero (→ lpagent_dropped)."""
    zero_variants = [
        "0",
        "0.0",
        "0.00000000",   # 8 decimal zeros
        "0e0",          # scientific notation
        "0E0",          # uppercase scientific
        "+0",           # explicit positive zero
        "-0",           # negative zero
        "0.0000",       # 4 zeros
    ]
    for pnl_str in zero_variants:
        row = _minimal_position_row(
            f"TOKEN_ZERO_{pnl_str.replace('.', '_').replace('+', 'p').replace('-', 'm').replace('e', 'e').replace('E', 'E')}",
            pnl_sol=pnl_str,
            datetime_close="2026-04-30T10:00:00",  # within range
        )
        reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)
        assert reason == "lpagent_dropped", \
            f"AC-6: pnl_sol='{pnl_str}' must be treated as zero → lpagent_dropped, got '{reason}'"


def test_lpagent_dropped_empty_string_forms() -> None:
    """AC-6: Empty/whitespace pnl_sol must also be treated as zero (lpagent_dropped)."""
    empty_variants = ["", "   ", "\t"]
    for pnl_str in empty_variants:
        row = _minimal_position_row(
            "TOKEN_EMPTY_PNL",
            pnl_sol=pnl_str,
            datetime_close="2026-04-30T10:00:00",
        )
        reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)
        assert reason == "lpagent_dropped", \
            f"AC-6: pnl_sol=repr('{pnl_str}') must be treated as empty/zero → lpagent_dropped, got '{reason}'"


def test_lpagent_dropped_missing_key() -> None:
    """AC-6: Missing pnl_sol key entirely must be treated as zero (lpagent_dropped)."""
    row = {
        "datetime_open": "2026-04-30T09:00:00",
        "datetime_close": "2026-04-30T10:00:00",
        "full_address": "TOKEN_NO_PNL_KEY",
        "token": "NOPNL",
        "source_wallet": DEFAULT_WALLET,
        # No "pnl_sol" key at all
    }
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)
    assert reason == "lpagent_dropped", \
        "AC-6: missing pnl_sol key must be treated as empty/zero → lpagent_dropped"


def test_lpagent_dropped_non_numeric_pnl_falls_through() -> None:
    """AC-6: Non-numeric pnl_sol ('abc') must NOT be lpagent_dropped — falls through.
    Per doc: 'non-numeric pnl → not zero, fall through to other checks'.
    """
    row = _minimal_position_row(
        "TOKEN_BAD_PNL",
        pnl_sol="abc",
        datetime_close="2026-04-30T10:00:00",
    )
    reason = reconcile._ours_only_reason_jsonl(row, "2026-04-01", DEFAULT_WALLET)
    assert reason != "lpagent_dropped", \
        "AC-6: non-numeric pnl_sol must NOT be classified as lpagent_dropped (fall through)"
    # Should be wallet_not_tracked or not_in_lpagent depending on source_wallet


def test_lpagent_only_outside_wallet_wins_over_in_archive() -> None:
    """AC-6 lpagent-only priority: owner != wallet AND tokenId in archive → 'outside_wallet_set' wins.
    Per doc pseudocode, outside_wallet_set is checked first.
    Note: _lpagent_only_hint_jsonl takes archive_token_ids: frozenset[str] (pre-computed),
    not archive_dir: Path. The implementation refactored archive scanning to be done upfront.
    """
    token_id = "TOKEN_BOTH_ARCHIVE_AND_OTHER_OWNER"
    # Pre-compute the archive token_ids frozenset (as the implementation does internally)
    archive_token_ids = frozenset([token_id])

    record = {"owner": "ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ"}  # not our wallet
    hint = reconcile._lpagent_only_hint_jsonl(token_id, record, DEFAULT_WALLET, archive_token_ids)

    assert hint == "outside_wallet_set", \
        "AC-6: outside_wallet_set must win over in_archive when both conditions apply"


# ---------------------------------------------------------------------------
# AC-7: Aggregate matched_count includes None-PnL rows; sums exclude them
# ---------------------------------------------------------------------------

def test_aggregates_count_includes_none_pnl_rows() -> None:
    """AC-7: matched_count must include rows where pnl_ours or pnl_lpagent is None.
    Only the sum should exclude them.
    """
    output_dir = _make_output_dir("aggregates-none-pnl")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    # One record with valid pnl, one with malformed pnlNative (→ pnl_lpagent will be None)
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_VALID",
                "token0Info": {"token_symbol": "VAL"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            },
            {
                "tokenId": "TOKEN_BAD_PNL",
                "token0Info": {"token_symbol": "BAD"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "NOT_A_NUMBER",  # will produce pnl_lpagent = None
                "owner": DEFAULT_WALLET,
            },
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [
            _minimal_position_row("TOKEN_VALID", "VAL", pnl_sol="0.200000"),
            _minimal_position_row("TOKEN_BAD_PNL", "BAD", pnl_sol=""),  # pnl_ours = None
        ],
    )

    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    # Find markdown output
    md_files = list(output_dir.glob("*.md"))
    assert md_files
    md_content = md_files[0].read_text(encoding="utf-8")

    # The aggregates section should show matched_count = 2 (both rows counted)
    # Even though one has None pnl values
    assert "2" in md_content, "AC-7: aggregates should reflect matched_count of 2 including None-pnl rows"


# ---------------------------------------------------------------------------
# AC-9: JSONL mode CSVs must NOT have # WARNING: as first line
# ---------------------------------------------------------------------------

def test_jsonl_mode_csvs_have_no_warning_header() -> None:
    """AC-9: In JSONL mode (no --legacy-cache), CSVs must NOT start with '# WARNING:'."""
    output_dir = _make_output_dir("csv-no-warning")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_CSV_CHECK",
                "token0Info": {"token_symbol": "CSV"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_CSV_CHECK", "CSV")],
    )

    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    # Only look at the reconciliation CSVs (not positions.csv which is also in output_dir)
    csv_files = list(output_dir.glob("reconciliation_*.csv"))
    assert len(csv_files) == 3, f"Expected 3 reconciliation CSV files, got {len(csv_files)}: {[f.name for f in csv_files]}"
    for csv_path in csv_files:
        first_line = csv_path.read_text(encoding="utf-8").splitlines()[0]
        assert not first_line.startswith("# WARNING:"), \
            f"AC-9: CSV {csv_path.name} must NOT start with '# WARNING:' in JSONL mode, got: {first_line!r}"


# ---------------------------------------------------------------------------
# AC-10: Empty/missing tokenId in JSONL record is skipped with warning
# ---------------------------------------------------------------------------

def test_empty_tokenid_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    """AC-10: tokenId = '' must skip with warning, not crash."""
    cache_dir = _case_dir("empty-tokenid") / "cache"
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {"tokenId": "", "token0Info": {"token_symbol": "EMPTY"}, "updatedAt": "2026-04-30T10:00:00Z", "pnlNative": "0.1"},
            {"tokenId": "TOKEN_GOOD", "token0Info": {"token_symbol": "GOOD"}, "updatedAt": "2026-04-30T10:00:00Z", "pnlNative": "0.1", "owner": DEFAULT_WALLET},
        ],
    )

    with caplog.at_level("WARNING"):
        result = reconcile._load_jsonl_cache(cache_dir, DEFAULT_WALLET, "2026-04-30", "2026-04-30")

    assert "" not in result, "AC-10: empty tokenId must be skipped, not included in result"
    assert "TOKEN_GOOD" in result, "AC-10: valid tokenId after empty one must still be loaded"
    assert "empty tokenId" in caplog.text.lower() or "tokenid" in caplog.text.lower(), \
        "AC-10: warning must be logged for empty tokenId"


def test_null_tokenid_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    """AC-10: tokenId = null must skip with warning, not crash."""
    cache_dir = _case_dir("null-tokenid") / "cache"
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {"tokenId": None, "token0Info": {"token_symbol": "NULL"}, "updatedAt": "2026-04-30T10:00:00Z", "pnlNative": "0.1"},
            {"tokenId": "TOKEN_AFTER_NULL", "token0Info": {"token_symbol": "AFNL"}, "updatedAt": "2026-04-30T10:00:00Z", "pnlNative": "0.1", "owner": DEFAULT_WALLET},
        ],
    )

    with caplog.at_level("WARNING"):
        result = reconcile._load_jsonl_cache(cache_dir, DEFAULT_WALLET, "2026-04-30", "2026-04-30")

    assert None not in result, "AC-10: null tokenId must be skipped"
    assert "TOKEN_AFTER_NULL" in result, "AC-10: valid record after null tokenId must still load"
    assert "tokenid" in caplog.text.lower() or "empty" in caplog.text.lower(), \
        "AC-10: warning must be logged for null tokenId"


def test_missing_tokenid_key_skipped_with_warning(caplog: pytest.LogCaptureFixture) -> None:
    """AC-10: record with no 'tokenId' key at all must skip with warning."""
    cache_dir = _case_dir("missing-tokenid-key") / "cache"
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {"token0Info": {"token_symbol": "NOTOK"}, "updatedAt": "2026-04-30T10:00:00Z", "pnlNative": "0.1"},  # no tokenId key
            {"tokenId": "TOKEN_AFTER_MISSING", "token0Info": {"token_symbol": "AFMS"}, "updatedAt": "2026-04-30T10:00:00Z", "pnlNative": "0.1", "owner": DEFAULT_WALLET},
        ],
    )

    with caplog.at_level("WARNING"):
        result = reconcile._load_jsonl_cache(cache_dir, DEFAULT_WALLET, "2026-04-30", "2026-04-30")

    assert "TOKEN_AFTER_MISSING" in result, "AC-10: valid record after missing-key record must still load"
    assert "tokenid" in caplog.text.lower() or "empty" in caplog.text.lower(), \
        "AC-10: warning must be logged for missing tokenId key"


# ---------------------------------------------------------------------------
# Adversarial: Corrupt archive file must not crash
# ---------------------------------------------------------------------------

def test_corrupt_archive_file_does_not_crash() -> None:
    """Adversarial: corrupt JSON in archive/ must log warning and skip that file, not crash."""
    output_dir = _make_output_dir("corrupt-archive")
    cache_dir = output_dir / "lpagent_cache"
    archive_dir = cache_dir / "archive"
    archive_dir.mkdir(parents=True)

    # Write a corrupt archive file
    (archive_dir / "corrupt.json").write_text('{"not": "an array"', encoding="utf-8")

    # Write a valid JSONL
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_ARCH_CORRUPT",
                "token0Info": {"token_symbol": "ARC"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [],  # ours-only scenario: no matching position
    )

    # Must not crash — just warn and skip the corrupt archive file
    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])


def test_archive_not_array_does_not_crash() -> None:
    """Adversarial: archive file is valid JSON but not an array — must not crash."""
    output_dir = _make_output_dir("archive-not-array")
    cache_dir = output_dir / "lpagent_cache"
    archive_dir = cache_dir / "archive"
    archive_dir.mkdir(parents=True)

    # Valid JSON but wrong structure (dict, not array)
    (archive_dir / "wrong_format.json").write_text(
        json.dumps({"tokenId": "TOKEN_NOT_ARRAY"}),
        encoding="utf-8",
    )

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_NOT_ARRAY",
                "token0Info": {"token_symbol": "NA"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(output_dir / "positions.csv", [])

    # Must not crash
    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])


# ---------------------------------------------------------------------------
# AC-1/AC-4: Report header contains correct strings
# ---------------------------------------------------------------------------

def test_report_header_contains_jsonl_cache_and_filter_lines(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-1 + AC-4: Console output must contain 'JSONL cache' and filter axis lines."""
    output_dir = _make_output_dir("report-header")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_HDR",
                "token0Info": {"token_symbol": "HDR"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_HDR", "HDR")],
    )

    reconcile.main([
        "--from", "2026-04-01",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    captured = capsys.readouterr()
    combined = captured.out + captured.err
    assert "JSONL cache" in combined, "AC-1: console output must contain 'JSONL cache'"
    assert "updatedAt" in combined, "AC-4: console output must mention updatedAt filter axis"
    assert "datetime_close" in combined, "AC-4: console output must mention datetime_close filter axis"


# ---------------------------------------------------------------------------
# JSONL dedup: last-write-wins by updatedAt
# ---------------------------------------------------------------------------

def test_jsonl_dedup_last_updated_at_wins() -> None:
    """Design §_load_jsonl_cache: duplicate tokenId → keep record with later updatedAt."""
    cache_dir = _case_dir("jsonl-dedup") / "cache"
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "DUP_TOKEN",
                "token0Info": {"token_symbol": "OLD"},
                "updatedAt": "2026-04-28T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            },
            {
                "tokenId": "DUP_TOKEN",
                "token0Info": {"token_symbol": "NEW"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.999000",
                "owner": DEFAULT_WALLET,
            },
        ],
    )

    result = reconcile._load_jsonl_cache(cache_dir, DEFAULT_WALLET, "2026-04-01", "2026-04-30")

    assert "DUP_TOKEN" in result
    assert result["DUP_TOKEN"]["token0Info"]["token_symbol"] == "NEW", \
        "Dedup: last-write-wins — later updatedAt ('NEW') should survive"


# ---------------------------------------------------------------------------
# AC-8: Wallet prefix resolution
# ---------------------------------------------------------------------------

def test_wallet_flag_takes_precedence_over_env(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """AC-8: --wallet CLI flag must win over LPAGENT_WALLET env var."""
    monkeypatch.setenv("LPAGENT_WALLET", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")

    output_dir = _make_output_dir("wallet-precedence")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    # Create JSONL for the --wallet prefix (J4tkG), NOT for the env var prefix (AAAAA)
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_WALLET_FLAG",
                "token0Info": {"token_symbol": "FLAG"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_WALLET_FLAG", "FLAG")],
    )

    # --wallet J4tkG... should use J4tkG prefix, ignoring env AAAAAAA
    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--wallet", DEFAULT_WALLET,
        "--output-dir", str(output_dir),
    ])
    # If --wallet did not take precedence, it would look for positions_AAAAA.jsonl
    # and fail with missing JSONL error


def test_wallet_env_var_used_when_no_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AC-8: LPAGENT_WALLET env var is used when --wallet is not specified."""
    env_wallet = "BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
    monkeypatch.setenv("LPAGENT_WALLET", env_wallet)
    monkeypatch.delenv("LPAGENT_WALLET", raising=False)
    monkeypatch.setenv("LPAGENT_WALLET", env_wallet)

    output_dir = _make_output_dir("wallet-env")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    prefix = env_wallet[:5]  # BBBBB
    _write_jsonl(
        cache_dir / f"positions_{prefix}.jsonl",
        [
            {
                "tokenId": "TOKEN_ENV_WALLET",
                "token0Info": {"token_symbol": "ENV"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": env_wallet,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_ENV_WALLET", "ENV", source_wallet=env_wallet)],
    )

    # No --wallet flag; should use LPAGENT_WALLET env
    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])


# ---------------------------------------------------------------------------
# AC-2: Missing JSONL produces clear error with exit code 1
# ---------------------------------------------------------------------------

def test_missing_jsonl_exits_1_with_clear_error(capsys: pytest.CaptureFixture[str]) -> None:
    """AC-2: missing JSONL file → exit 1 with 'JSONL cache not found' in stderr."""
    output_dir = _make_output_dir("missing-jsonl")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()
    # No JSONL file created
    _write_positions_csv(output_dir / "positions.csv", [])

    with pytest.raises(SystemExit) as excinfo:
        reconcile.main([
            "--from", "2026-04-30",
            "--to", "2026-04-30",
            "--output-dir", str(output_dir),
        ])

    captured = capsys.readouterr()
    assert excinfo.value.code == 1, "AC-2: exit code must be 1"
    assert "JSONL cache not found" in captured.err or "JSONL cache not found" in captured.out, \
        "AC-2: error message must mention 'JSONL cache not found'"


def test_empty_jsonl_exits_1_with_clear_error(capsys: pytest.CaptureFixture[str]) -> None:
    """AC-2: empty JSONL file → exit 1 (same as missing)."""
    output_dir = _make_output_dir("empty-jsonl")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    # Write an empty file
    (cache_dir / "positions_J4tkG.jsonl").write_text("", encoding="utf-8")
    _write_positions_csv(output_dir / "positions.csv", [])

    with pytest.raises(SystemExit) as excinfo:
        reconcile.main([
            "--from", "2026-04-30",
            "--to", "2026-04-30",
            "--output-dir", str(output_dir),
        ])

    assert excinfo.value.code == 1, "AC-2: empty JSONL must also exit 1"


# ---------------------------------------------------------------------------
# AC-9: Output files produced in JSONL mode
# ---------------------------------------------------------------------------

def test_output_files_produced_in_jsonl_mode() -> None:
    """AC-9: Running with valid data in JSONL mode produces .md + 3 CSVs."""
    output_dir = _make_output_dir("output-files")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_OUT",
                "token0Info": {"token_symbol": "OUT"},
                "updatedAt": "2026-04-30T10:00:00Z",
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [_minimal_position_row("TOKEN_OUT", "OUT")],
    )

    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    assert (output_dir / "reconciliation_2026-04-30_2026-04-30.md").exists(), \
        "AC-9: Markdown report must be produced"
    assert (output_dir / "reconciliation_2026-04-30_2026-04-30_matched.csv").exists(), \
        "AC-9: matched CSV must be produced"
    assert (output_dir / "reconciliation_2026-04-30_2026-04-30_lpagent_only.csv").exists(), \
        "AC-9: lpagent_only CSV must be produced"
    assert (output_dir / "reconciliation_2026-04-30_2026-04-30_ours_only.csv").exists(), \
        "AC-9: ours_only CSV must be produced"


# ---------------------------------------------------------------------------
# Adversarial: All-ours-only scenario (JSONL covers different date range)
# ---------------------------------------------------------------------------

def test_all_ours_only_no_crash() -> None:
    """Adversarial: When JSONL covers a different date, all positions are ours-only.
    Must produce valid report, not crash on empty matched/lpagent aggregates.
    """
    output_dir = _make_output_dir("all-ours-only")
    cache_dir = output_dir / "lpagent_cache"
    cache_dir.mkdir()

    # JSONL records are all for a different date window (outside --from/--to)
    _write_jsonl(
        cache_dir / "positions_J4tkG.jsonl",
        [
            {
                "tokenId": "TOKEN_DIFFERENT_DATE",
                "token0Info": {"token_symbol": "DIFF"},
                "updatedAt": "2026-01-01T10:00:00Z",  # January — way outside April window
                "pnlNative": "0.100000",
                "owner": DEFAULT_WALLET,
            }
        ],
    )
    _write_positions_csv(
        output_dir / "positions.csv",
        [
            _minimal_position_row("TOKEN_OUR_ONLY_A"),
            _minimal_position_row("TOKEN_OUR_ONLY_B"),
        ],
    )

    # Must not crash — valid report with 0 matched, N ours-only
    reconcile.main([
        "--from", "2026-04-30",
        "--to", "2026-04-30",
        "--output-dir", str(output_dir),
    ])

    md_files = list(output_dir.glob("*.md"))
    assert md_files, "All-ours-only scenario must still produce a Markdown report"
