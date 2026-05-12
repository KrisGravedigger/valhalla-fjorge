"""Tests for tools/migrate_lpagent_cache.py (doc 027).

All file-based tests use tmp_path copies of fixtures — never real output/ files.
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

import pytest

# Ensure tools/ is importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "tools"))

import migrate_lpagent_cache as mig  # noqa: E402

FIXTURES = PROJECT_ROOT / "tests" / "fixtures" / "migrate_lpagent_cache"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _copy_fixtures(src_dir: Path, dest_dir: Path, names: List[str]) -> None:
    """Copy selected fixture files into dest_dir."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    for name in names:
        (dest_dir / name).write_bytes((src_dir / name).read_bytes())


def _write_daily_file(dest_dir: Path, date_str: str, positions: List[dict]) -> None:
    (dest_dir / f"{date_str}.json").write_text(
        json.dumps(positions, ensure_ascii=False), encoding="utf-8"
    )


def _write_jsonl_file(dest_dir: Path, wallet: str, records: List[dict]) -> None:
    prefix = wallet[:5]
    path = dest_dir / f"positions_{prefix}.jsonl"
    lines = [json.dumps(r, ensure_ascii=False) for r in records]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# AC-1: Audit report content
# ---------------------------------------------------------------------------

def test_audit_finds_overlaps_and_bug_signatures(tmp_path: Path) -> None:
    """Given fixture files, audit detects overlapping tokenIds and bug-signature records."""
    _copy_fixtures(FIXTURES, tmp_path, ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"])

    daily_files, _ = mig._load_daily_files(tmp_path)
    report = mig._audit(daily_files)

    # TOKEN_D and TOKEN_E appear in 2026-04-28 and 2026-04-29
    assert "TOKEN_D" in report.overlapping_token_ids
    assert "TOKEN_E" in report.overlapping_token_ids
    assert len(report.overlapping_token_ids) == 2

    # TOKEN_I in 2026-04-30.json has updatedAt=2026-05-03 (3 days after file date)
    bug_token_ids = [r["tokenId"] for r in report.bug_signature_records]
    assert "TOKEN_I" in bug_token_ids
    assert len(report.bug_signature_records) == 1


def test_audit_empty_cache_dir(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Empty cache dir prints 'No legacy daily files found' and exits 0."""
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--audit", "--cache-dir", str(tmp_path)])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "No legacy daily files found. Nothing to audit." in captured.out


def test_audit_exits_zero(tmp_path: Path) -> None:
    """Audit mode exits 0 even when overlaps are present."""
    _copy_fixtures(FIXTURES, tmp_path, ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"])

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--audit", "--cache-dir", str(tmp_path)])

    assert exc_info.value.code == 0


def test_audit_report_sections_printed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Audit prints the three required report sections."""
    _copy_fixtures(FIXTURES, tmp_path, ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"])

    with pytest.raises(SystemExit):
        mig.main(["--audit", "--cache-dir", str(tmp_path)])

    out = capsys.readouterr().out
    assert "Overlapping tokenIds" in out
    assert "Empty/zero-position files" in out
    assert "Bug-signature records" in out


# ---------------------------------------------------------------------------
# AC-2: Migration produces correct JSONL
# ---------------------------------------------------------------------------

def test_migrate_dedup_newer_updatedAt_wins(tmp_path: Path) -> None:
    """Newer updatedAt wins when the same tokenId appears in multiple files."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )

    daily_files, _ = mig._load_daily_files(tmp_path)
    jsonl_path = tmp_path / "positions_J4tkG.jsonl"
    merged = mig._merge_to_jsonl(daily_files, jsonl_path)

    # TOKEN_D: 2026-04-29 has updatedAt=2026-04-29T09:00:00.000Z (newer)
    assert "TOKEN_D" in merged
    assert merged["TOKEN_D"]["updatedAt"] == "2026-04-29T09:00:00.000Z"
    assert merged["TOKEN_D"]["pnlNative"] == 0.41

    # TOKEN_E: 2026-04-29 has updatedAt=2026-04-29T10:00:00.000Z (newer)
    assert "TOKEN_E" in merged
    assert merged["TOKEN_E"]["updatedAt"] == "2026-04-29T10:00:00.000Z"
    assert merged["TOKEN_E"]["pnlNative"] == 0.51

    # All 12 unique tokens present
    assert len(merged) == 12


def test_migrate_produces_correct_line_count(tmp_path: Path) -> None:
    """AC-2 happy path: 14 total records (2 duplicates) → 12 unique tokenIds in JSONL."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )

    wallet = "J4tkGAbcde"
    with pytest.raises(SystemExit) as exc_info:
        mig.main([
            "--migrate",
            "--cache-dir", str(tmp_path),
            "--wallet", wallet,
        ])

    assert exc_info.value.code == 0

    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    assert jsonl_path.exists()
    lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 12


def test_migrate_preserves_existing_jsonl_records(tmp_path: Path) -> None:
    """AC-2 adversarial: existing JSONL records with newer updatedAt survive merge."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )

    wallet = "J4tkGAbcde"
    # Pre-write existing JSONL with 3 records having newer updatedAt than daily files
    existing_records = [
        {
            "tokenId": "TOKEN_A",
            "pnlNative": 999.0,
            "createdAt": "2026-04-28T08:00:00.000Z",
            "updatedAt": "2026-05-01T00:00:00Z",  # newer than anything in daily files
            "fetched_at_utc": "2026-05-01T12:00:00Z",
        },
        {
            "tokenId": "TOKEN_B",
            "pnlNative": 888.0,
            "createdAt": "2026-04-28T09:00:00.000Z",
            "updatedAt": "2026-05-01T00:00:00Z",
            "fetched_at_utc": "2026-05-01T12:00:00Z",
        },
        {
            "tokenId": "TOKEN_C",
            "pnlNative": 777.0,
            "createdAt": "2026-04-28T10:00:00.000Z",
            "updatedAt": "2026-05-01T00:00:00Z",
            "fetched_at_utc": "2026-05-01T12:00:00Z",
        },
    ]
    _write_jsonl_file(tmp_path, wallet, existing_records)

    daily_files, _ = mig._load_daily_files(tmp_path)
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    merged = mig._merge_to_jsonl(daily_files, jsonl_path)

    # Doc-026 records with newer updatedAt must survive
    assert merged["TOKEN_A"]["pnlNative"] == 999.0
    assert merged["TOKEN_B"]["pnlNative"] == 888.0
    assert merged["TOKEN_C"]["pnlNative"] == 777.0

    # All 12 unique tokens still present
    assert len(merged) == 12


# ---------------------------------------------------------------------------
# AC-3: Idempotency
# ---------------------------------------------------------------------------

def test_idempotency(tmp_path: Path) -> None:
    """Running --migrate twice produces same JSONL and 0 new archive moves."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"

    # First run
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])
    assert exc_info.value.code == 0
    content_first = jsonl_path.read_text(encoding="utf-8")

    # Second run — daily files now in archive, JSONL is the sole source
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])
    assert exc_info.value.code == 0
    content_second = jsonl_path.read_text(encoding="utf-8")

    # JSONL content must be equivalent (same tokenIds and records)
    records_first = {
        json.loads(ln)["tokenId"]: json.loads(ln)
        for ln in content_first.splitlines() if ln.strip()
    }
    records_second = {
        json.loads(ln)["tokenId"]: json.loads(ln)
        for ln in content_second.splitlines() if ln.strip()
    }
    assert records_first == records_second

    # Archive still has exactly 3 files (no double-move)
    archive_dir = tmp_path / "archive"
    assert archive_dir.exists()
    archived = list(archive_dir.glob("*.json"))
    assert len(archived) == 3


def test_idempotency_no_double_move(tmp_path: Path) -> None:
    """AC-3 adversarial: already-archived files are not re-processed."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    # First run — archives all 3 files
    with pytest.raises(SystemExit):
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    archive_dir = tmp_path / "archive"
    mtime_before = {f.name: f.stat().st_mtime for f in archive_dir.glob("*.json")}

    # Second run — no error, no re-archive
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])
    assert exc_info.value.code == 0

    mtime_after = {f.name: f.stat().st_mtime for f in archive_dir.glob("*.json")}
    assert mtime_before == mtime_after  # files untouched


# ---------------------------------------------------------------------------
# AC-4: Integrity check
# ---------------------------------------------------------------------------

def test_integrity_check_passes(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """After migration the integrity check prints OK."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    assert "Integrity check:" in captured.out
    assert "OK." in captured.out


def test_integrity_check_fails_exits_1(tmp_path: Path) -> None:
    """AC-4 adversarial: merged count mismatch → exit 1, no archive move."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    original_merge = mig._merge_to_jsonl

    def _merge_drops_one(
        daily_files: Dict[str, List[dict]],
        existing_jsonl_path: Path,
    ) -> Dict[str, dict]:
        result = original_merge(daily_files, existing_jsonl_path)
        # Drop one record to simulate a migration bug
        first_key = next(iter(result))
        del result[first_key]
        return result

    with patch.object(mig, "_merge_to_jsonl", side_effect=_merge_drops_one):
        with pytest.raises(SystemExit) as exc_info:
            mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 1
    # No files should have moved to archive
    archive_dir = tmp_path / "archive"
    assert not archive_dir.exists() or len(list(archive_dir.glob("*.json"))) == 0


# ---------------------------------------------------------------------------
# AC-5: Archive is read-only
# ---------------------------------------------------------------------------

@pytest.mark.skipif(sys.platform == "win32", reason="Windows chmod semantics differ from POSIX")
def test_read_only_set_on_archive(tmp_path: Path) -> None:
    """After migration on POSIX, archived files are read-only."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0
    archive_dir = tmp_path / "archive"
    for f in archive_dir.glob("*.json"):
        assert not os.access(str(f), os.W_OK), f"Expected {f} to be read-only"


def test_chmod_failure_does_not_abort(tmp_path: Path) -> None:
    """AC-5 adversarial: chmod failure is warned but migration still succeeds."""
    _copy_fixtures(FIXTURES, tmp_path, ["2026-04-28.json"])
    wallet = "J4tkGAbcde"

    def _failing_chmod(path: Path) -> None:
        raise OSError("permission denied (simulated)")

    with patch.object(mig, "_set_read_only", side_effect=_failing_chmod):
        with pytest.raises(SystemExit) as exc_info:
            mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0
    # File still archived despite chmod failure
    archive_dir = tmp_path / "archive"
    assert (archive_dir / "2026-04-28.json").exists()


# ---------------------------------------------------------------------------
# AC-6: Bug-signature detection and malformed file handling
# ---------------------------------------------------------------------------

def test_bug_signature_detected_in_audit(tmp_path: Path) -> None:
    """AC-6 happy path: TOKEN_I in 2026-04-30 has updatedAt 3 days after file date."""
    _copy_fixtures(FIXTURES, tmp_path, ["2026-04-30.json"])

    daily_files, _ = mig._load_daily_files(tmp_path)
    report = mig._audit(daily_files)

    assert len(report.bug_signature_records) == 1
    rec = report.bug_signature_records[0]
    assert rec["tokenId"] == "TOKEN_I"
    assert rec["file"] == "2026-04-30.json"
    assert rec["createdAt"] == "2026-04-30T10:00:00.000Z"
    assert rec["updatedAt"] == "2026-05-03T10:00:00.000Z"


def test_malformed_file_skipped(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """AC-6 adversarial: malformed.json is logged and skipped; other files processed."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json", "malformed.json"],
    )

    with caplog.at_level("WARNING"):
        daily_files, loaded_names = mig._load_daily_files(tmp_path)

    # Warning logged for malformed.json
    assert any("malformed.json" in msg for msg in caplog.messages)

    # Malformed file not in result dict
    assert "malformed" not in daily_files

    # 3 valid date files loaded
    assert len(daily_files) == 3
    assert "malformed.json" not in loaded_names


def test_malformed_file_not_archived(tmp_path: Path) -> None:
    """Malformed files are NOT moved to archive/ during migration."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "malformed.json"],
    )
    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0
    # malformed.json stays in cache_dir
    assert (tmp_path / "malformed.json").exists()
    archive_dir = tmp_path / "archive"
    assert not (archive_dir / "malformed.json").exists()


# ---------------------------------------------------------------------------
# Dry-run
# ---------------------------------------------------------------------------

def test_dry_run_does_not_move_files(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--dry-run writes JSONL but does NOT move daily files to archive/."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--dry-run", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0

    # JSONL written with correct number of records
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    assert jsonl_path.exists()
    lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 12

    # Daily files still in place
    assert (tmp_path / "2026-04-28.json").exists()
    assert (tmp_path / "2026-04-29.json").exists()
    assert (tmp_path / "2026-04-30.json").exists()

    # Output mentions [dry-run]
    captured = capsys.readouterr()
    assert "[dry-run]" in captured.out


def test_dry_run_with_audit_is_invalid(capsys: pytest.CaptureFixture[str]) -> None:
    """--dry-run combined with --audit must produce a non-zero exit (usage error)."""
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--audit", "--dry-run"])

    assert exc_info.value.code != 0


# ---------------------------------------------------------------------------
# fetched_at_utc injection
# ---------------------------------------------------------------------------

def test_fetched_at_utc_injected_for_legacy_records(tmp_path: Path) -> None:
    """Legacy daily records that lack fetched_at_utc get it set to {file_date}T00:00:00Z."""
    _write_daily_file(tmp_path, "2026-04-28", [
        {
            "tokenId": "TOKEN_X",
            "pnlNative": 1.0,
            "createdAt": "2026-04-28T08:00:00Z",
            "updatedAt": "2026-04-28T10:00:00Z",
        }
    ])

    daily_files, _ = mig._load_daily_files(tmp_path)
    merged = mig._merge_to_jsonl(daily_files, tmp_path / "positions_J4tkG.jsonl")

    assert merged["TOKEN_X"]["fetched_at_utc"] == "2026-04-28T00:00:00Z"


def test_existing_fetched_at_utc_preserved(tmp_path: Path) -> None:
    """Records that already have fetched_at_utc keep their original value."""
    _write_daily_file(tmp_path, "2026-04-28", [
        {
            "tokenId": "TOKEN_Y",
            "pnlNative": 2.0,
            "createdAt": "2026-04-28T08:00:00Z",
            "updatedAt": "2026-04-28T10:00:00Z",
            "fetched_at_utc": "2026-04-27T15:00:00Z",
        }
    ])

    daily_files, _ = mig._load_daily_files(tmp_path)
    merged = mig._merge_to_jsonl(daily_files, tmp_path / "positions_J4tkG.jsonl")

    assert merged["TOKEN_Y"]["fetched_at_utc"] == "2026-04-27T15:00:00Z"


# ---------------------------------------------------------------------------
# JSONL format compatibility
# ---------------------------------------------------------------------------

def test_output_jsonl_is_loadable_per_line(tmp_path: Path) -> None:
    """Every line in the output JSONL is valid JSON with a non-empty tokenId."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        assert "tokenId" in rec
        assert rec["tokenId"]


def test_expected_merged_matches_fixtures(tmp_path: Path) -> None:
    """The migration output matches the expected_merged.jsonl fixture (pnlNative + updatedAt)."""
    _copy_fixtures(
        FIXTURES, tmp_path,
        ["2026-04-28.json", "2026-04-29.json", "2026-04-30.json"],
    )
    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"

    actual_records = {
        json.loads(ln)["tokenId"]: json.loads(ln)
        for ln in jsonl_path.read_text(encoding="utf-8").splitlines()
        if ln.strip()
    }
    expected_records = {
        json.loads(ln)["tokenId"]: json.loads(ln)
        for ln in (FIXTURES / "expected_merged.jsonl").read_text(encoding="utf-8").splitlines()
        if ln.strip()
    }

    assert set(actual_records.keys()) == set(expected_records.keys()), (
        f"tokenId sets differ: actual={set(actual_records.keys())}, "
        f"expected={set(expected_records.keys())}"
    )
    for tid in expected_records:
        assert actual_records[tid]["pnlNative"] == expected_records[tid]["pnlNative"], (
            f"pnlNative mismatch for {tid}"
        )
        assert actual_records[tid]["updatedAt"] == expected_records[tid]["updatedAt"], (
            f"updatedAt mismatch for {tid}"
        )
