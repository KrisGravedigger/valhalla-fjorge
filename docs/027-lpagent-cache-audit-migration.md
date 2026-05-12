---
critical: true
---

# [027] lpagent Cache Integrity Audit + Migration (C.3)

## Intent
Build a one-shot migration tool `tools/migrate_lpagent_cache.py` that audits the
existing daily `output/lpagent_cache/YYYY-MM-DD.json` files for overlaps, gaps, and
the "closed-after-watermark" bug signature, then merges all positions into the new
flat JSONL format introduced by doc 026. After migration the daily files are frozen
as read-only in `output/lpagent_cache/archive/`. Nothing is deleted. The migration is
idempotent: running it twice produces the same JSONL.

## Overview
This doc covers `tools/migrate_lpagent_cache.py` with two modes: `--audit` (read-only
report of cache quality) and `--migrate` (full audit + JSONL production + archive
move). The resulting JSONL is the canonical input for all future lpagent client runs
(doc 026) and the full reconciliation tool (D-full, future doc). This doc also
surfaces an open question about `valhalla/reconcile.py` (doc 025), which currently
reads the daily files directly and will need updating after migration.

## Context
After doc 026 ships, the system has two cache representations in parallel:
- **Legacy**: `output/lpagent_cache/YYYY-MM-DD.json` files covering (roughly)
  2026-02-11 through the last pipeline run. These have documented bugs: overlapping
  date ranges, `to_date` ignored, and the closed-after-watermark invisibility.
- **New**: `output/lpagent_cache/positions_{wallet_prefix}.jsonl` built and maintained
  by the rewritten `LpAgentClient` (doc 026).

The migration must produce a JSONL that is at least as complete as the union of all
daily files, with duplicates collapsed. It must not corrupt the new JSONL if doc 026
has already run and written records there.

`output/` is `.gitignore`d (confirmed in root `.gitignore`), so neither the daily
files, the JSONL, nor the archive directory will be committed to the repo.

## Goals
- `--audit` mode: walk all daily JSON files, report overlaps (same `tokenId` in
  multiple files), gaps (dates with zero positions), and bug-signature records
  (`updatedAt > file_date + 1d`), without writing anything.
- `--migrate` mode: produce `positions_{wallet_prefix}.jsonl` from the union of all
  daily files, deduped by `tokenId` (latest `updatedAt` wins). Then move daily files
  into `output/lpagent_cache/archive/` as read-only. Existing JSONL records (if doc
  026 already ran) are preserved and merged in — newer `updatedAt` always wins.
- Migration is idempotent: running `--migrate` a second time on already-migrated data
  produces the same JSONL and reports 0 new moves to archive.
- Roundtrip integrity: every `tokenId` that appeared in any daily file appears exactly
  once in the output JSONL. No `pnlNative` value is lost or altered.

## Non-Goals
- Deleting any files (daily files are archived, not deleted).
- Fetching from the live API (this is a pure local-file operation).
- Fixing the reconcile-lite tool (`valhalla/reconcile.py` in `--legacy-cache` mode)
  to look in `archive/` — that is an open question for the PM (see Open Questions).
- Updating `valhalla/lpagent_client.py` to ignore archive files (doc 026 already
  ignores `*.json` daily files and only reads `*.jsonl`).
- Backfilling any gaps found during audit (those require a live API fetch — that is
  doc 026's `fetch_since` responsibility).

## Acceptance Criteria

### AC-1: Audit report content
- **Happy path**: Given a cache directory with 5 daily JSON files, some with overlapping
  `tokenId` values and at least one with a position where `updatedAt > file_date + 1d`,
  the `--audit` mode prints a report with three sections: "Overlaps",
  "Gaps (dates with zero positions)", and "Bug-signature records
  (updatedAt > file_date + 24h)". Each section lists affected `tokenId`s or date
  strings. The command exits with code 0.
- **Adversarial**: Given an empty `output/lpagent_cache/` directory (no daily files),
  `--audit` prints "No legacy daily files found. Nothing to audit." and exits 0.

### AC-2: Migration produces correct JSONL
- **Happy path**: Given 3 daily JSON files with a total of 25 records (7 unique
  `tokenId`s repeated across files), `--migrate` produces a JSONL with exactly 7
  lines, one per unique `tokenId`. The `pnlNative` value for each record matches the
  version with the latest `updatedAt` across all files.
- **Adversarial**: Given the same 3 files plus an existing JSONL from doc 026 that
  already contains 3 of those `tokenId`s (with newer `updatedAt` values),
  `--migrate` keeps the doc-026 versions for those 3 records (since they have newer
  `updatedAt`) and merges in the remaining 4 from the daily files. Final JSONL: 7
  lines, no truncation of the doc-026 records.

### AC-3: Idempotency
- **Happy path**: Run `--migrate` once → JSONL has N records, archive has M files.
  Run `--migrate` again → JSONL still has N records (byte-for-byte equivalent content
  modulo sort order), archive still has M files (no double-move, no error).
- **Adversarial**: A daily file that was already moved to `archive/` should not be
  re-processed. The migration must check both `output/lpagent_cache/` and
  `output/lpagent_cache/archive/` to determine "already migrated" status and skip
  re-archiving.

### AC-4: Roundtrip integrity assertion
- **Happy path**: After `--migrate`, the tool itself counts `tokenId`s from all
  original daily files (including those now in archive/), counts lines in the JSONL,
  and asserts the deduped count matches. Prints:
  `"Integrity check: 1234 unique tokenIds in source files, 1234 lines in JSONL. OK."`.
- **Adversarial**: If the post-migration JSONL count does not match the deduped source
  count (indicates a bug in the migration logic), the tool prints an error and exits
  with code 1 WITHOUT moving any files to archive. The daily files remain in place.
  This guarantees the user can always retry.

### AC-5: Archive is read-only
- **Happy path**: After `--migrate`, all `.json` files in `output/lpagent_cache/archive/`
  have file mode 0o444 (read-only) on POSIX. On Windows (NTFS), the read-only
  attribute is set via `os.chmod(path, 0o444)` which sets the read-only flag.
- **Adversarial**: If `os.chmod` fails (e.g., insufficient permissions), the migration
  logs a warning but does NOT abort. The files are still in archive/; they just may
  not be read-only. The user is told to set permissions manually if needed.

### AC-6: Bug-signature detection
- **Happy path**: Given a position with `createdAt=2026-04-30T10:00Z` and
  `updatedAt=2026-05-03T10:00Z` in file `2026-04-30.json` (updatedAt is 3 days after
  the file date), the `--audit` report labels it as a bug-signature record with the
  exact `tokenId`, `createdAt`, `updatedAt`, and the file it was found in.
- **Adversarial**: Given a file with malformed JSON (not a valid JSON array), the
  migration logs `"Skipping malformed file 2026-04-XX.json: <error>"` and continues.
  The file is NOT moved to archive. Post-migration integrity check will fail if this
  file contained unique positions, alerting the user.

## Touchable Files
- `tools/migrate_lpagent_cache.py` — new script
- `tests/test_migrate_lpagent_cache.py` — new test file
- `tests/fixtures/migrate_lpagent_cache/` — fixture directory with synthetic daily
  JSON files and expected JSONL output

Do NOT touch: `valhalla/lpagent_client.py`, `valhalla/lpagent_pipeline.py`,
`valhalla/reconcile.py`, `valhalla/cross_check.py`,
`output/lpagent_cache/*.json` (the tool manages these; the test harness uses
`tmp_path` copies only).

## Verification Contract

```bash
# Unit tests — all must pass without modifying any real output/ files
pytest tests/test_migrate_lpagent_cache.py -v

# Type checking
mypy tools/migrate_lpagent_cache.py --ignore-missing-imports

# Lint
ruff check tools/migrate_lpagent_cache.py

# Smoke test — audit-only mode against real cache (read-only, safe to run anytime)
python tools/migrate_lpagent_cache.py --audit --cache-dir output/lpagent_cache
# Expected: report with overlaps/gaps/bug-signatures; exit 0; no files modified

# Full migration dry-run check (do NOT run without PM approval — destructive to legacy cache)
# python tools/migrate_lpagent_cache.py --migrate --cache-dir output/lpagent_cache
# Expected: integrity check passes; daily files moved to archive/; JSONL written
```

The migration smoke test (`--migrate`) is listed for documentation but must NOT be
run as part of automated CI — it is a one-shot destructive operation on real data.
The PM runs it manually after reviewing the `--audit` output.

## Design

### CLI

```
python tools/migrate_lpagent_cache.py
  (--audit | --migrate)
  [--cache-dir PATH]        (default: output/lpagent_cache)
  [--wallet WALLET_ADDR]    (default: from LPAGENT_WALLET env or hardcoded default)
  [--dry-run]               (with --migrate: produce JSONL but do NOT move files to archive)
```

- `--audit`: read-only. Parses all daily files, emits report. Exits 0.
- `--migrate`: full operation. Parses, deduplicates, writes JSONL, verifies integrity,
  moves files to archive/, sets read-only.
- `--dry-run` (only valid with `--migrate`): perform all steps up to and including
  writing the JSONL, then print what would be moved but do not move anything. Useful
  for a final pre-flight check.

### Module Structure

```
tools/migrate_lpagent_cache.py
├── _load_daily_files(cache_dir) → Dict[str, List[dict]]
│     # Returns {date_str: [positions]} for all *.json files in cache_dir
│     # Skips archive/ subdirectory
│     # Logs warning and skips malformed files
├── _audit(daily_files) → AuditReport
│     # Returns dataclass with overlaps, gaps, bug_signatures
├── _print_audit_report(report, daily_files)
├── _merge_to_jsonl(daily_files, existing_jsonl_path) → Dict[str, dict]
│     # Returns merged dict keyed by tokenId; newer updatedAt wins
├── _write_jsonl(records, path) → None
│     # Write to .tmp, then os.replace (atomic)
├── _verify_integrity(records, daily_files) -> bool
│     # Compares deduped source count vs output line count; prints result
├── _archive_daily_files(cache_dir, dry_run) -> int
│     # Moves *.json files to archive/; sets read-only; returns count moved
├── _set_read_only(path) -> None
│     # os.chmod(path, 0o444); log warning if fails
└── main()  # argparse entry point
```

### Daily File Loading

```python
def _load_daily_files(cache_dir: Path) -> Dict[str, List[dict]]:
    """
    Walk cache_dir for YYYY-MM-DD.json files. Skip archive/ subdirectory.
    Return dict mapping date_str → list of position dicts.
    Malformed JSON: log warning, skip file (do not raise).
    """
    result = {}
    archive_dir = cache_dir / "archive"
    for path in sorted(cache_dir.glob("*.json")):
        if path.parent == archive_dir:
            continue
        date_str = path.stem  # "2026-04-30"
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                raise ValueError("Expected JSON array")
            result[date_str] = data
        except (json.JSONDecodeError, ValueError, OSError) as e:
            logger.warning("Skipping malformed file %s: %s", path.name, e)
    return result
```

### Deduplication Logic

Priority order for keeping a record when the same `tokenId` appears in multiple files:
1. Record with the **latest `updatedAt`** wins.
2. Tie-break (same `updatedAt`): prefer the record from the **later-dated file**
   (i.e., more recent snapshot).
3. Existing JSONL records from doc 026 with `fetched_at_utc` set: these are the
   freshest; they beat daily-file records on `updatedAt` or tie-break.

Each merged record has `fetched_at_utc` set to the file date of the source file
(as `{file_date}T00:00:00Z`) if it comes from a legacy daily file and does not
already have a `fetched_at_utc` field.

### Integrity Verification

After `_merge_to_jsonl` produces the merged dict:
```python
all_token_ids = set()
for positions in daily_files.values():
    for pos in positions:
        tid = pos.get("tokenId", "")
        if tid:
            all_token_ids.add(tid)

# Also include any tokenIds from the existing JSONL (if present)
if existing_jsonl_path.exists():
    for line in existing_jsonl_path.read_bytes().splitlines():
        try:
            rec = json.loads(line)
            tid = rec.get("tokenId", "")
            if tid:
                all_token_ids.add(tid)
        except json.JSONDecodeError:
            pass  # truncated line; already handled by doc 026

expected = len(all_token_ids)
actual = len(merged_records)

if actual != expected:
    print(f"ERROR: Integrity check FAILED: expected {expected}, got {actual}.")
    sys.exit(1)
print(f"Integrity check: {expected} unique tokenIds in source files, {actual} in JSONL. OK.")
```

### Archive Move

```python
def _archive_daily_files(cache_dir: Path, dry_run: bool) -> int:
    archive_dir = cache_dir / "archive"
    archive_dir.mkdir(exist_ok=True)
    moved = 0
    for path in sorted(cache_dir.glob("*.json")):
        dest = archive_dir / path.name
        if dest.exists():
            logger.info("Already archived: %s — skipping", path.name)
            continue
        if dry_run:
            print(f"  [dry-run] Would move {path.name} → archive/")
        else:
            path.rename(dest)
            _set_read_only(dest)
            moved += 1
    return moved
```

### Audit Report Structure

```python
@dataclass
class AuditReport:
    overlapping_token_ids: Dict[str, List[str]]   # tokenId → list of file dates
    empty_date_files: List[str]                    # date strings with zero positions
    bug_signature_records: List[dict]              # {tokenId, createdAt, updatedAt, file}
```

Console output format:
```
=== lpagent Cache Audit ===

Daily files found: 34 (2026-02-11 → 2026-05-04)

Overlapping tokenIds (same position in multiple files): 157
  Example: AbCd1234 found in 2026-04-28.json, 2026-04-29.json, 2026-04-30.json

Empty/zero-position files: 2
  2026-03-15.json, 2026-03-22.json

Bug-signature records (updatedAt > file_date + 24h): 23
  AbCd1234 | createdAt=2026-04-30T10:00Z | updatedAt=2026-05-03T10:00Z | file=2026-04-30.json
  ...

Total unique tokenIds across all files: 1234
Estimated post-migration JSONL size: 1234 records
```

### Fixture Files for Tests

Place in `tests/fixtures/migrate_lpagent_cache/`:

- `2026-04-28.json` — 5 positions (3 unique tokenIds + 2 that repeat in later files)
- `2026-04-29.json` — 5 positions (the same 2 repeating tokenIds with newer `updatedAt`,
  plus 3 new ones)
- `2026-04-30.json` — 4 positions (1 bug-signature record where `updatedAt` is 3 days
  after the file date, 3 new unique tokenIds)
- `malformed.json` — not a valid JSON array (to test AC-6 adversarial)
- `expected_merged.jsonl` — expected output: 12 unique tokenIds, one per line, with
  latest `updatedAt` values from the fixture files

The fixture `tokenId` values must be clearly synthetic (e.g., `TOKEN_A`, `TOKEN_B`).

## Implementation Plan

1. **Create `tools/migrate_lpagent_cache.py`** with argparse `main()` and the module
   structure above. Wire `--audit` to `_audit + _print_audit_report` only.

2. **Implement `_load_daily_files`**: glob pattern, archive/ skip, malformed-file
   handling with logged warning.

3. **Implement `_audit`**: collect overlapping `tokenId`s (dict from tokenId to list
   of file dates), empty-file list, bug-signature records (`updatedAt > file_date +
   timedelta(hours=24)`).

4. **Implement `_print_audit_report`**: format as shown in Design section.

5. **Implement `_merge_to_jsonl`**: iterate all files in sorted order; build dict with
   dedup logic (newer `updatedAt` wins); then load existing JSONL if present and merge
   (doc-026 records win on same `updatedAt`). Add `fetched_at_utc` field to legacy
   records.

6. **Implement `_write_jsonl`**: write to `.tmp`, then `os.replace` for atomicity.

7. **Implement `_verify_integrity`**: count union of all source tokenIds; compare to
   merged dict size; exit 1 if mismatch.

8. **Implement `_archive_daily_files` and `_set_read_only`**: move with
   `path.rename(dest)` (same filesystem — no copy overhead); set read-only with
   `os.chmod`; log warning if chmod fails.

9. **Create fixtures** in `tests/fixtures/migrate_lpagent_cache/` as specified above.

10. **Write `tests/test_migrate_lpagent_cache.py`**:
    - `test_audit_finds_overlaps_and_bug_signatures` — use fixtures; assert overlap
      count, bug-signature count.
    - `test_audit_empty_cache_dir` — empty tmp_path; assert "No legacy daily files"
      message; exit 0.
    - `test_migrate_dedup_newer_updatedAt_wins` — two fixture files; assert merged
      JSONL has correct record per tokenId.
    - `test_migrate_preserves_existing_jsonl_records` — pre-write an "existing" JSONL
      with newer timestamps; assert those records survive merge.
    - `test_integrity_check_passes` — normal migration; assert "OK" message.
    - `test_integrity_check_fails_exits_1` — mock `_merge_to_jsonl` to return wrong
      count; assert exit code 1 and no archive move.
    - `test_idempotency` — run migration twice on same fixtures; assert JSONL content
      identical on second run; assert archive count unchanged.
    - `test_malformed_file_skipped` — include `malformed.json` in fixture dir; assert
      logged warning; other files still processed.
    - `test_dry_run_does_not_move_files` — run with `--dry-run`; assert no files in
      archive/; assert JSONL written.
    - `test_read_only_set_on_archive` — after migration, assert archived files have
      read-only attribute (check `os.access(path, os.W_OK) == False` on POSIX;
      skip on Windows if insufficient privilege).

## Dependencies
- **Requires doc 026**: the target JSONL format (`positions_{wallet_prefix}.jsonl`)
  and the wallet-prefix naming convention are defined in doc 026. The migration
  script must produce a file that `LpAgentClient.load_cache()` (doc 026) can read
  without errors.
- External: Python stdlib only (`json`, `os`, `logging`, `pathlib`, `datetime`,
  `argparse`, `dataclasses`).

## Testing

All file-based tests use `tmp_path` copies of fixtures — never real `output/`
files. The migration smoke test (`--migrate` against real data) is run manually
by the PM after inspecting the `--audit` output.

## Alternatives Considered
- **Delete daily files after migration**: Rejected — non-destructive archive is safer.
  If the migration has a bug, the user retains the source data.
- **Merge into SQLite instead of JSONL**: Rejected — consistent with doc 026 decision;
  JSONL is human-inspectable, requires no dependency.
- **Auto-run migration on first `LpAgentClient` init**: Rejected — migration should be
  an explicit PM decision, not a silent side effect. The `--audit` → review → `--migrate`
  workflow preserves oversight.
- **Separate audit script from migration script**: Rejected — the same data-loading
  code underlies both modes; two scripts would duplicate this logic. The `--audit` /
  `--migrate` flags on one script are cleaner.

## Open Questions
- **`valhalla/reconcile.py` (doc 025) compatibility after migration**: The
  `--legacy-cache` mode in `reconcile.py` currently reads daily files from
  `output/lpagent_cache/YYYY-MM-DD.json`. After `--migrate` runs, those files live in
  `output/lpagent_cache/archive/`. The reconcile tool will find zero cache files and
  print the "no files found" notice for every date in the range.
  **Decision (2026-05-12)**: `--legacy-cache` is considered superseded. After
  `--migrate` runs, do not use `--legacy-cache`. D-full will implement `--jsonl-cache`
  against the new flat JSONL. No change to `reconcile.py` is planned.
- **`tokenId` absent in legacy daily files**: Some edge-case positions (e.g., the
  DSc936vC-style zero-token positions noted in `PLAN-portfolio-truth.md`) may have
  empty or absent `tokenId`. The migration skips such rows (consistent with
  `cross_check.py` behavior). Confirm this is acceptable — these positions are likely
  already excluded from `positions.csv` matching.
