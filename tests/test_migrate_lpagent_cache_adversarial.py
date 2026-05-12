"""Adversarial tests for tools/migrate_lpagent_cache.py (doc 027).

These tests target edge cases NOT covered by the existing test suite.
They are written from the CONTRACT (design doc), not from reading the implementation.

Attack surface:
  ADV-01: Position with NO tokenId field (missing key, not just empty)
  ADV-02: All daily files are empty arrays []
  ADV-03: Same tokenId, same updatedAt in two files — tie-break must be later-dated file
  ADV-04: archive/ already exists with files from a previous partial migration
  ADV-05: Existing JSONL tokenId vs daily file with NEWER updatedAt — which wins?
  ADV-06: --wallet prefix doesn't match existing JSONL entries
  ADV-07: output/lpagent_cache/ directory doesn't exist at all
  ADV-08: Non-date filename in cache dir (backup.json, positions_J4tkG.jsonl)
  ADV-09: .tmp file left from a crashed doc-026 write
  ADV-10: _load_daily_files skip archive/ even if archive/ is the first glob match
  ADV-11: Integrity assertion fires BEFORE archiving — if count mismatch → exit(1),
          no files should be in archive/ afterwards
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
# Helpers (mirror conventions from test_migrate_lpagent_cache.py)
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
# ADV-01: Position with NO tokenId field (missing key, not just empty string)
# ---------------------------------------------------------------------------

def test_missing_tokenid_field_skipped_consistently(tmp_path: Path) -> None:
    """ADV-01: A position dict with NO tokenId key must be skipped.

    The contract (Open Questions section) says rows with absent/empty tokenId
    are skipped — consistent with cross_check.py behaviour.
    Critical risk: integrity check uses one counting policy (skipping rows
    without tokenId), but merge logic might count it differently.
    """
    _write_daily_file(tmp_path, "2026-05-01", [
        # Valid position
        {
            "tokenId": "TOKEN_VALID",
            "pnlNative": 1.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
        },
        # Missing tokenId entirely (not empty string — key does not exist)
        {
            "pnlNative": 0.5,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
        },
    ])

    wallet = "J4tkGAbcde"

    # Migration should succeed with exit 0 (integrity check must pass)
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0, (
        "Migration should succeed: row with missing tokenId is skipped, "
        "but integrity check should still pass (consistent counting on both sides)"
    )

    # JSONL must contain exactly 1 record (the valid one)
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    assert jsonl_path.exists()
    lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 1, (
        f"Expected 1 line (row without tokenId should be skipped), got {len(lines)}"
    )
    rec = json.loads(lines[0])
    assert rec["tokenId"] == "TOKEN_VALID"


def test_missing_tokenid_not_emitted_in_audit(tmp_path: Path) -> None:
    """ADV-01b: Audit should handle positions without tokenId gracefully (no crash)."""
    _write_daily_file(tmp_path, "2026-05-01", [
        {"pnlNative": 0.5, "createdAt": "2026-05-01T08:00:00Z", "updatedAt": "2026-05-01T09:00:00Z"},
        {"tokenId": "TOKEN_OK", "pnlNative": 1.0, "createdAt": "2026-05-01T08:00:00Z", "updatedAt": "2026-05-01T09:00:00Z"},
    ])

    # Should not crash
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--audit", "--cache-dir", str(tmp_path)])
    assert exc_info.value.code == 0


# ---------------------------------------------------------------------------
# ADV-02: All daily files are empty arrays []
# ---------------------------------------------------------------------------

def test_all_empty_files_no_crash(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """ADV-02: All daily files contain empty arrays []; migration must not crash.

    The doc specifies "empty_date_files" in AuditReport for files with zero positions.
    With --migrate, zero positions → JSONL with 0 lines, integrity check: 0 == 0, OK.
    """
    _write_daily_file(tmp_path, "2026-05-01", [])
    _write_daily_file(tmp_path, "2026-05-02", [])

    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0, (
        "Migration of all-empty files should exit 0: 0 unique tokenIds, 0 JSONL lines — counts match"
    )

    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    # JSONL may or may not exist, but if it exists, it must have 0 non-empty lines
    if jsonl_path.exists():
        lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        assert len(lines) == 0


def test_all_empty_files_audit_reports_gaps(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """ADV-02b: Audit on empty files should list them in 'empty_date_files'."""
    _write_daily_file(tmp_path, "2026-05-01", [])
    _write_daily_file(tmp_path, "2026-05-02", [])

    daily_files, _ = mig._load_daily_files(tmp_path)
    report = mig._audit(daily_files)

    assert len(report.empty_date_files) == 2, (
        f"Expected 2 empty date files in report, got {report.empty_date_files}"
    )


# ---------------------------------------------------------------------------
# ADV-03: Same tokenId, same updatedAt in two files — tie-break by later-dated file
# ---------------------------------------------------------------------------

def test_same_updatedAt_tiebreak_later_file_wins(tmp_path: Path) -> None:
    """ADV-03: When updatedAt is identical, the record from the later-dated file wins.

    Design doc Dedup rule 2: "Tie-break (same updatedAt): prefer the record from
    the later-dated file (i.e., more recent snapshot)."

    We distinguish the two records by pnlNative so we know which was kept.
    """
    TOKEN = "TOKEN_TIE"
    SAME_UPDATED_AT = "2026-05-01T12:00:00.000Z"

    # Earlier file (2026-05-01): same tokenId, same updatedAt, different pnlNative
    _write_daily_file(tmp_path, "2026-05-01", [
        {
            "tokenId": TOKEN,
            "pnlNative": 1.11,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": SAME_UPDATED_AT,
        }
    ])

    # Later file (2026-05-02): same tokenId, same updatedAt, different pnlNative
    _write_daily_file(tmp_path, "2026-05-02", [
        {
            "tokenId": TOKEN,
            "pnlNative": 2.22,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": SAME_UPDATED_AT,
        }
    ])

    daily_files, _ = mig._load_daily_files(tmp_path)
    merged = mig._merge_to_jsonl(daily_files, tmp_path / "positions_J4tkG.jsonl")

    assert TOKEN in merged
    assert merged[TOKEN]["pnlNative"] == 2.22, (
        f"Tie-break: later-dated file (2026-05-02, pnlNative=2.22) should win, "
        f"got pnlNative={merged[TOKEN]['pnlNative']}"
    )


def test_same_updatedAt_tiebreak_three_files(tmp_path: Path) -> None:
    """ADV-03b: Three-way tie — the latest file date wins."""
    TOKEN = "TOKEN_3TIE"
    SAME_UPDATED_AT = "2026-05-02T06:00:00.000Z"

    for date, pnl in [("2026-05-01", 1.0), ("2026-05-02", 2.0), ("2026-05-03", 3.0)]:
        _write_daily_file(tmp_path, date, [
            {
                "tokenId": TOKEN,
                "pnlNative": pnl,
                "createdAt": "2026-05-01T00:00:00Z",
                "updatedAt": SAME_UPDATED_AT,
            }
        ])

    daily_files, _ = mig._load_daily_files(tmp_path)
    merged = mig._merge_to_jsonl(daily_files, tmp_path / "positions_J4tkG.jsonl")

    assert merged[TOKEN]["pnlNative"] == 3.0, (
        "Three-way tie: file 2026-05-03 (latest date) must win"
    )


# ---------------------------------------------------------------------------
# ADV-04: archive/ already exists with files from a previous partial migration
# ---------------------------------------------------------------------------

def test_preexisting_archive_with_partial_migration(tmp_path: Path) -> None:
    """ADV-04: archive/ exists and already contains some files; migration handles gracefully.

    Scenario: A previous migration moved 2026-05-01.json to archive/ but crashed
    before moving 2026-05-02.json. On retry, only 2026-05-02.json should be moved.
    The JSONL must contain all tokenIds from both files.
    """
    # Simulate partial migration: archive/ exists, 2026-05-01 is already there
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    # Pre-place 2026-05-01.json in archive (already migrated)
    token_a_record = [
        {
            "tokenId": "TOKEN_PARTIAL_A",
            "pnlNative": 10.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T10:00:00Z",
        }
    ]
    (archive_dir / "2026-05-01.json").write_text(
        json.dumps(token_a_record), encoding="utf-8"
    )

    # 2026-05-02 still in cache dir (not yet migrated)
    _write_daily_file(tmp_path, "2026-05-02", [
        {
            "tokenId": "TOKEN_PARTIAL_B",
            "pnlNative": 20.0,
            "createdAt": "2026-05-02T08:00:00Z",
            "updatedAt": "2026-05-02T10:00:00Z",
        }
    ])

    wallet = "J4tkGAbcde"
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    # Migration should not crash
    assert exc_info.value.code == 0, (
        "Migration with pre-existing partial archive should succeed"
    )

    # 2026-05-02.json should now be in archive
    assert (archive_dir / "2026-05-02.json").exists(), (
        "2026-05-02.json should have been moved to archive"
    )

    # The JSONL file should be written
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    assert jsonl_path.exists()


def test_preexisting_archive_file_not_overwritten(tmp_path: Path) -> None:
    """ADV-04b: If archive already has 2026-05-01.json, the file must not be overwritten.

    AC-3 adversarial: 'already archived — skip'.
    """
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()

    ORIGINAL_CONTENT = json.dumps([{"tokenId": "TOKEN_ORIG", "pnlNative": 99.0}])
    archived_file = archive_dir / "2026-05-01.json"
    archived_file.write_text(ORIGINAL_CONTENT, encoding="utf-8")
    original_mtime = archived_file.stat().st_mtime

    # Same filename also sitting in cache_dir
    _write_daily_file(tmp_path, "2026-05-01", [
        {"tokenId": "TOKEN_NEW", "pnlNative": 1.0, "createdAt": "2026-05-01T08:00:00Z",
         "updatedAt": "2026-05-01T09:00:00Z"}
    ])

    wallet = "J4tkGAbcde"
    with pytest.raises(SystemExit):
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    # The archive file must NOT be overwritten
    assert archived_file.stat().st_mtime == original_mtime, (
        "Pre-existing archive file must not be overwritten (idempotency guarantee)"
    )
    assert archived_file.read_text(encoding="utf-8") == ORIGINAL_CONTENT, (
        "Archive file content must remain unchanged"
    )


# ---------------------------------------------------------------------------
# ADV-05: Existing JSONL has tokenId X; daily file has SAME tokenId with NEWER updatedAt
# ---------------------------------------------------------------------------

def test_daily_file_newer_updatedAt_beats_existing_jsonl(tmp_path: Path) -> None:
    """ADV-05: Daily file has a NEWER updatedAt than existing JSONL record.

    Contract ambiguity note (Goals section vs. Design section):
      - Goals: "newer updatedAt always wins"
      - Design rule 3: "Existing JSONL records with fetched_at_utc set are freshest;
        they beat daily-file records on updatedAt or tie-break."
    These conflict. The Goals section ("newer updatedAt ALWAYS wins") is the primary
    contract promise. We test the Goals interpretation.
    If the implementation picks Design rule 3 instead, this is classified as WARNING
    (contract ambiguity), not CRITICAL.
    """
    TOKEN = "TOKEN_CONFLICT"

    # Existing JSONL from doc 026 — older updatedAt
    existing_record = {
        "tokenId": TOKEN,
        "pnlNative": 50.0,
        "createdAt": "2026-05-01T08:00:00Z",
        "updatedAt": "2026-05-01T09:00:00Z",   # OLDER
        "fetched_at_utc": "2026-05-01T12:00:00Z",
    }
    wallet = "J4tkGAbcde"
    _write_jsonl_file(tmp_path, wallet, [existing_record])

    # Daily file — NEWER updatedAt for the same tokenId
    _write_daily_file(tmp_path, "2026-05-02", [
        {
            "tokenId": TOKEN,
            "pnlNative": 75.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-03T00:00:00Z",   # NEWER
        }
    ])

    daily_files, _ = mig._load_daily_files(tmp_path)
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    merged = mig._merge_to_jsonl(daily_files, jsonl_path)

    assert TOKEN in merged
    # Per Goals: newer updatedAt wins → daily file version (pnlNative=75.0)
    actual_pnl = merged[TOKEN]["pnlNative"]
    assert actual_pnl == 75.0, (
        f"Goals say newer updatedAt wins: expected pnlNative=75.0 (daily file), "
        f"got {actual_pnl}. If impl keeps JSONL record (50.0), this is a WARNING "
        f"(Goals vs. Design rule 3 ambiguity)."
    )


# ---------------------------------------------------------------------------
# ADV-06: --wallet that doesn't match prefix of existing JSONL entries
# ---------------------------------------------------------------------------

def test_wallet_prefix_mismatch_no_crash(tmp_path: Path) -> None:
    """ADV-06: --wallet prefix doesn't match existing JSONL file's prefix.

    If doc 026 wrote positions_J4tkG.jsonl but we run migration with --wallet XXXXX,
    the migration should either:
    (a) ignore the existing JSONL (no cross-wallet contamination), OR
    (b) crash with a clear error.
    It must NOT silently merge data from a different wallet's JSONL.
    """
    # Write JSONL for wallet J4tkGAbcde (prefix J4tkG)
    existing_records = [
        {
            "tokenId": "TOKEN_WALLET_A",
            "pnlNative": 100.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
            "fetched_at_utc": "2026-05-01T12:00:00Z",
        }
    ]
    _write_jsonl_file(tmp_path, "J4tkGAbcde", existing_records)

    # Daily file for different content
    _write_daily_file(tmp_path, "2026-05-01", [
        {
            "tokenId": "TOKEN_DAILY_X",
            "pnlNative": 5.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T10:00:00Z",
        }
    ])

    # Run migration with a DIFFERENT wallet prefix
    other_wallet = "XXXXX12345"
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", other_wallet])

    exit_code = exc_info.value.code

    # Either exit 0 (no cross-wallet contamination) or non-zero (explicit error) is acceptable
    # What is NOT acceptable: the output JSONL for XXXXX12345 contains TOKEN_WALLET_A
    if exit_code == 0:
        jsonl_path = tmp_path / f"positions_{other_wallet[:5]}.jsonl"
        if jsonl_path.exists():
            lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
            token_ids_in_output = {json.loads(ln)["tokenId"] for ln in lines}
            assert "TOKEN_WALLET_A" not in token_ids_in_output, (
                "Cross-wallet contamination: TOKEN_WALLET_A from J4tkG JSONL must not "
                "appear in XXXXX12345 migration output"
            )


# ---------------------------------------------------------------------------
# ADV-07: output/lpagent_cache/ directory doesn't exist at all
# ---------------------------------------------------------------------------

def test_nonexistent_cache_dir_audit(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """ADV-07: Passing a non-existent --cache-dir to --audit should either:
    (a) exit 0 with "No legacy daily files found." message, OR
    (b) exit non-zero with a clear error.
    It must NOT raise an unhandled exception (traceback).
    """
    nonexistent = tmp_path / "does_not_exist"
    assert not nonexistent.exists()

    # Should not raise unhandled exception
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--audit", "--cache-dir", str(nonexistent)])

    # Any clean exit is acceptable; no unhandled Python traceback
    # If exit 0: must print "No legacy daily files" or similar
    # If exit 1: must not print a raw traceback
    captured = capsys.readouterr()
    if exc_info.value.code == 0:
        assert "No legacy daily files" in captured.out or "Nothing to audit" in captured.out, (
            "Exit 0 with non-existent dir must say 'No legacy daily files found.'"
        )


def test_nonexistent_cache_dir_migrate(tmp_path: Path) -> None:
    """ADV-07b: --migrate with non-existent cache dir must not crash with traceback."""
    nonexistent = tmp_path / "does_not_exist"
    assert not nonexistent.exists()

    wallet = "J4tkGAbcde"

    # Must not raise unhandled exception (SystemExit is fine)
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(nonexistent), "--wallet", wallet])

    # Any clean exit code is acceptable here (0 = empty migration, non-0 = error)
    # The critical invariant is: no Python traceback / AttributeError from NoneType
    _ = exc_info.value.code  # just confirm it is a SystemExit


# ---------------------------------------------------------------------------
# ADV-08: Non-date filename in cache dir (backup.json, positions_J4tkG.jsonl)
# ---------------------------------------------------------------------------

def test_non_date_json_file_in_cache_dir(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """ADV-08: backup.json in the cache dir must NOT be loaded as a date file.

    The loader uses path.stem as date_str. For 'backup.json', stem = 'backup'.
    This is not a YYYY-MM-DD date string. The loader MUST skip non-date files
    or treat them as malformed, NOT include their tokens in the JSONL.

    Critical property: backup.json must NOT be moved to archive/ by _archive_daily_files.
    It is not a legitimate daily cache file — archiving it would be data corruption.
    """
    # Valid date file
    _write_daily_file(tmp_path, "2026-05-01", [
        {
            "tokenId": "TOKEN_VALID",
            "pnlNative": 1.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
        }
    ])

    # Non-date JSON file with valid JSON array content (but not a YYYY-MM-DD named file)
    (tmp_path / "backup.json").write_text(
        json.dumps([{"tokenId": "TOKEN_BACKUP", "pnlNative": 99.0}]),
        encoding="utf-8"
    )

    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0

    # CRITICAL: backup.json must NOT be in archive/ — it's not a daily cache file
    archive_dir = tmp_path / "archive"
    assert not (archive_dir / "backup.json").exists(), (
        "backup.json is not a YYYY-MM-DD daily file — it must NOT be moved to archive/. "
        "Archiving it would corrupt non-cache files left in the same directory."
    )

    # CRITICAL: backup.json must still exist in the cache root (not destroyed)
    assert (tmp_path / "backup.json").exists(), (
        "backup.json must remain in place — the migration must not touch non-date files"
    )

    # CRITICAL: TOKEN_BACKUP must not appear in the JSONL
    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    if jsonl_path.exists():
        lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
        token_ids = {json.loads(ln)["tokenId"] for ln in lines}
        assert "TOKEN_BACKUP" not in token_ids, (
            "TOKEN_BACKUP is from backup.json (non-date file) — must not appear in JSONL"
        )
        # Only TOKEN_VALID from the real date file should be present
        assert len(lines) == 1, (
            f"Expected 1 line (only TOKEN_VALID from 2026-05-01.json), got {len(lines)}"
        )


def test_non_date_json_file_does_not_poison_audit(tmp_path: Path) -> None:
    """ADV-08b: Non-date JSON file must not cause a crash in _audit() (date parsing).

    _audit() calls datetime.strptime on the dict key from _load_daily_files.
    If _load_daily_files includes 'backup' as a key (non-date stem), _audit calls
    strptime('backup', '%Y-%m-%d') which raises ValueError.
    _audit wraps this in try/except and sets file_date = None — so it won't crash.
    But the audit should also NOT include backup.json tokens in overlap/gap reporting.
    """
    _write_daily_file(tmp_path, "2026-05-01", [
        {
            "tokenId": "TOKEN_OK",
            "pnlNative": 1.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
        }
    ])
    (tmp_path / "backup.json").write_text(
        json.dumps([{"tokenId": "TOKEN_BACKUP", "pnlNative": 99.0}]),
        encoding="utf-8"
    )

    # This must not crash regardless of loader/audit behavior
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--audit", "--cache-dir", str(tmp_path)])

    assert exc_info.value.code == 0, (
        "--audit should exit 0 even with backup.json present; must not crash on date parse"
    )


def test_jsonl_file_in_cache_dir_ignored(tmp_path: Path) -> None:
    """ADV-08c: A .jsonl file (e.g., positions_J4tkG.jsonl) must be ignored by the loader.

    The glob pattern *.json should exclude *.jsonl naturally. Verify no crash
    and no contamination of daily_files dict.
    """
    # Write a .jsonl file in the cache dir
    _write_daily_file(tmp_path, "2026-05-01", [
        {"tokenId": "TOKEN_DAILY", "pnlNative": 1.0,
         "createdAt": "2026-05-01T08:00:00Z", "updatedAt": "2026-05-01T09:00:00Z"}
    ])
    (tmp_path / "positions_J4tkG.jsonl").write_text(
        '{"tokenId": "TOKEN_JSONL", "pnlNative": 5.0}\n', encoding="utf-8"
    )

    daily_files, loaded_names = mig._load_daily_files(tmp_path)

    # .jsonl file must not appear in daily_files
    assert "positions_J4tkG" not in daily_files
    assert all(name.endswith(".json") for name in loaded_names), (
        "loaded_names should contain only .json files, not .jsonl"
    )


# ---------------------------------------------------------------------------
# ADV-09: .tmp file left from a crashed doc-026 write
# ---------------------------------------------------------------------------

def test_tmp_file_in_cache_dir_ignored(tmp_path: Path) -> None:
    """ADV-09: A .tmp file in the cache dir must be ignored.

    doc-026 writes JSONL atomically: write to .tmp, then os.replace.
    A crash between write and replace leaves a .tmp file.
    The migration's *.json glob should exclude *.tmp naturally.
    """
    _write_daily_file(tmp_path, "2026-05-01", [
        {"tokenId": "TOKEN_REAL", "pnlNative": 1.0,
         "createdAt": "2026-05-01T08:00:00Z", "updatedAt": "2026-05-01T09:00:00Z"}
    ])

    # Simulate crashed doc-026 write
    (tmp_path / "positions_J4tkG.jsonl.tmp").write_text(
        '{"tokenId": "TOKEN_TMP", "pnlNative": 99.0}\n', encoding="utf-8"
    )

    wallet = "J4tkGAbcde"

    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0, (
        "Migration should succeed when .tmp file is present; *.json glob must exclude it"
    )

    # .tmp file must remain untouched (not archived)
    assert (tmp_path / "positions_J4tkG.jsonl.tmp").exists(), (
        ".tmp file should remain in place (not moved to archive)"
    )
    archive_dir = tmp_path / "archive"
    assert not (archive_dir / "positions_J4tkG.jsonl.tmp").exists()


# ---------------------------------------------------------------------------
# ADV-10: _load_daily_files correctly skips archive/ even if it contains .json files
# ---------------------------------------------------------------------------

def test_load_daily_files_skips_archive_subdirectory(tmp_path: Path) -> None:
    """ADV-10: _load_daily_files must skip archive/ even if glob hits it.

    Note: cache_dir.glob('*.json') does NOT recurse, so archive/ files won't
    appear in the glob results anyway. The design doc includes an explicit
    `if path.parent == archive_dir: continue` guard. We verify both:
    1. Archive files don't appear in daily_files
    2. The archive dir itself is not treated as a JSON file
    """
    # Normal file in cache root
    _write_daily_file(tmp_path, "2026-05-01", [
        {"tokenId": "TOKEN_ROOT", "pnlNative": 1.0,
         "createdAt": "2026-05-01T08:00:00Z", "updatedAt": "2026-05-01T09:00:00Z"}
    ])

    # File in archive/
    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    (archive_dir / "2026-04-01.json").write_text(
        json.dumps([{"tokenId": "TOKEN_ARCHIVED", "pnlNative": 50.0,
                     "createdAt": "2026-04-01T08:00:00Z", "updatedAt": "2026-04-01T09:00:00Z"}]),
        encoding="utf-8"
    )

    daily_files, loaded_names = mig._load_daily_files(tmp_path)

    assert "2026-04-01" not in daily_files, (
        "Archive file 2026-04-01.json must not appear in daily_files"
    )
    assert "TOKEN_ARCHIVED" not in str(daily_files), (
        "Token from archive file must not appear in loaded daily_files"
    )
    assert "2026-05-01" in daily_files, (
        "Root-level 2026-05-01.json should be loaded"
    )


def test_archive_subdir_tokens_not_in_merge(tmp_path: Path) -> None:
    """ADV-10b: Tokens from archive/ must not pollute the merged output.

    If _load_daily_files incorrectly reads archive/ files, those tokens
    would appear in the JSONL, inflating the line count.
    """
    _write_daily_file(tmp_path, "2026-05-01", [
        {"tokenId": "TOKEN_DAILY", "pnlNative": 1.0,
         "createdAt": "2026-05-01T08:00:00Z", "updatedAt": "2026-05-01T09:00:00Z"}
    ])

    archive_dir = tmp_path / "archive"
    archive_dir.mkdir()
    (archive_dir / "2026-04-01.json").write_text(
        json.dumps([{"tokenId": "TOKEN_ARCHIVE_ONLY", "pnlNative": 77.0,
                     "createdAt": "2026-04-01T08:00:00Z", "updatedAt": "2026-04-01T09:00:00Z"}]),
        encoding="utf-8"
    )

    wallet = "J4tkGAbcde"
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0

    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    token_ids = {json.loads(ln)["tokenId"] for ln in lines}

    assert "TOKEN_ARCHIVE_ONLY" not in token_ids, (
        "TOKEN_ARCHIVE_ONLY is in archive/, not in cache root — must not appear in JSONL"
    )
    assert len(lines) == 1, (
        "Only TOKEN_DAILY (from cache root) should be in JSONL"
    )


# ---------------------------------------------------------------------------
# ADV-11: Integrity assertion fires BEFORE archiving
# ---------------------------------------------------------------------------

def test_integrity_failure_leaves_no_archive_files(tmp_path: Path) -> None:
    """ADV-11: When integrity check fails, NO files should be in archive/ afterwards.

    AC-4 adversarial: "tool prints error and exits with code 1 WITHOUT moving any
    files to archive. The daily files remain in place."

    This is the strongest ordering guarantee: integrity check → archive.
    Not: archive → integrity check.
    """
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
        first_key = next(iter(result))
        del result[first_key]
        return result

    with patch.object(mig, "_merge_to_jsonl", side_effect=_merge_drops_one):
        with pytest.raises(SystemExit) as exc_info:
            mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 1, "Integrity failure must exit 1"

    # CRITICAL: No files should have been moved to archive/
    archive_dir = tmp_path / "archive"
    archived_files = list(archive_dir.glob("*.json")) if archive_dir.exists() else []
    assert len(archived_files) == 0, (
        f"Integrity failure must block archive move. "
        f"Found {len(archived_files)} files in archive/: {[f.name for f in archived_files]}"
    )

    # Daily files must still be in cache root (recoverable state)
    assert (tmp_path / "2026-04-28.json").exists(), "Daily file must remain in cache root"
    assert (tmp_path / "2026-04-29.json").exists(), "Daily file must remain in cache root"
    assert (tmp_path / "2026-04-30.json").exists(), "Daily file must remain in cache root"


def test_integrity_failure_no_jsonl_written_or_truncated(tmp_path: Path) -> None:
    """ADV-11b: When integrity check fails, the JSONL written by _write_jsonl before
    the check is present (the check happens after writing), but this is acceptable —
    the important thing is that daily files are NOT archived (recovery is possible).

    The design doc says exit(1) prevents archiving. The JSONL may be in a
    partially-migrated state; that's recoverable (re-run migration).

    This test verifies the ordering: write JSONL → verify → archive.
    If archive is skipped on failure, the contract is satisfied.
    """
    _copy_fixtures(FIXTURES, tmp_path, ["2026-04-28.json"])
    wallet = "J4tkGAbcde"

    original_merge = mig._merge_to_jsonl

    def _merge_drops_all(
        daily_files: Dict[str, List[dict]],
        existing_jsonl_path: Path,
    ) -> Dict[str, dict]:
        # Return empty dict to guarantee mismatch
        return {}

    with patch.object(mig, "_merge_to_jsonl", side_effect=_merge_drops_all):
        with pytest.raises(SystemExit) as exc_info:
            mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 1

    # Daily file must still be in place (not archived)
    assert (tmp_path / "2026-04-28.json").exists(), (
        "2026-04-28.json must remain in cache root after integrity failure"
    )


# ---------------------------------------------------------------------------
# ADV-BONUS: Empty string tokenId (not the same as missing key)
# ---------------------------------------------------------------------------

def test_empty_string_tokenid_skipped(tmp_path: Path) -> None:
    """BONUS: A position with tokenId="" (empty string) must be skipped.

    Design doc: 'skip such rows (consistent with cross_check.py behavior)'.
    Integrity check uses `if tid:` — empty string is falsy, so it's excluded.
    Merge logic must also exclude it to keep counting consistent.
    """
    _write_daily_file(tmp_path, "2026-05-01", [
        # Valid
        {
            "tokenId": "TOKEN_VALID_2",
            "pnlNative": 1.0,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
        },
        # Empty string tokenId
        {
            "tokenId": "",
            "pnlNative": 0.5,
            "createdAt": "2026-05-01T08:00:00Z",
            "updatedAt": "2026-05-01T09:00:00Z",
        },
    ])

    wallet = "J4tkGAbcde"
    with pytest.raises(SystemExit) as exc_info:
        mig.main(["--migrate", "--cache-dir", str(tmp_path), "--wallet", wallet])

    assert exc_info.value.code == 0, (
        "Empty-string tokenId should be skipped; integrity check should pass (1 == 1)"
    )

    jsonl_path = tmp_path / f"positions_{wallet[:5]}.jsonl"
    lines = [ln for ln in jsonl_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["tokenId"] == "TOKEN_VALID_2"
