"""
tools/migrate_lpagent_cache.py

One-shot migration tool for lpagent cache (doc 027).

Modes:
  --audit   Read-only: audit daily JSON files for overlaps, gaps,
            and "closed-after-watermark" bug signatures.
  --migrate Full operation: audit + merge to JSONL + archive daily files.

See docs/027-lpagent-cache-audit-migration.md for full design.
"""

import argparse
import json
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

logger = logging.getLogger(__name__)

DEFAULT_WALLET = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"
DEFAULT_CACHE_DIR = "output/lpagent_cache"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class AuditReport:
    overlapping_token_ids: Dict[str, List[str]] = field(default_factory=dict)
    """tokenId → list of file dates where it appears"""

    empty_date_files: List[str] = field(default_factory=list)
    """date strings (stems) of files with zero positions"""

    bug_signature_records: List[dict] = field(default_factory=list)
    """list of {tokenId, createdAt, updatedAt, file} for updatedAt > file_date + 24h"""


# ---------------------------------------------------------------------------
# Core helpers
# ---------------------------------------------------------------------------

def _load_daily_files(
    cache_dir: Path,
) -> "tuple[Dict[str, List[dict]], List[str]]":
    """Walk cache_dir for YYYY-MM-DD.json files. Skip archive/ subdirectory.

    Returns:
        (date_str_to_positions, successfully_loaded_filenames)

    Malformed JSON: log warning, skip file (do not raise).
    Only successfully-loaded files are included in either return value.
    """
    result: Dict[str, List[dict]] = {}
    loaded_names: List[str] = []
    archive_dir = cache_dir / "archive"

    for path in sorted(cache_dir.glob("*.json")):
        # Skip anything physically inside archive/ (shouldn't happen via glob("*.json")
        # since glob is not recursive, but be explicit)
        if path.resolve().parent == archive_dir.resolve():
            continue
        date_str = path.stem  # "2026-04-30"
        if not _DATE_RE.match(date_str):
            logger.warning("Skipping non-date file %s", path.name)
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                raise ValueError("Expected JSON array at top level")
            result[date_str] = data
            loaded_names.append(path.name)
        except (json.JSONDecodeError, ValueError, OSError) as exc:
            logger.warning("Skipping malformed file %s: %s", path.name, exc)

    return result, loaded_names


def _audit(daily_files: Dict[str, List[dict]]) -> AuditReport:
    """Analyse loaded daily files and produce an AuditReport.

    Detects:
    - overlapping tokenIds (same tokenId in multiple files)
    - empty files (zero positions)
    - bug-signature records (updatedAt > file_date + 24h)
    """
    report = AuditReport()

    # Map tokenId → [file_date, ...]
    token_to_files: Dict[str, List[str]] = {}

    for date_str, positions in daily_files.items():
        if not positions:
            report.empty_date_files.append(date_str)
            continue

        # Parse file date for bug-signature check
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            file_date = None  # not a date-named file — skip bug check

        for pos in positions:
            tid = pos.get("tokenId", "")
            if not tid:
                continue

            # Track which files contain each tokenId
            if tid not in token_to_files:
                token_to_files[tid] = []
            token_to_files[tid].append(date_str)

            # Bug-signature: updatedAt more than 24h after the file date
            if file_date is not None:
                updated_str = pos.get("updatedAt", "")
                if updated_str:
                    try:
                        # Handle both "Z" and "+00:00" suffixes
                        updated_dt = datetime.fromisoformat(
                            updated_str.replace("Z", "+00:00")
                        )
                        if updated_dt > file_date + timedelta(hours=24):
                            report.bug_signature_records.append(
                                {
                                    "tokenId": tid,
                                    "createdAt": pos.get("createdAt", ""),
                                    "updatedAt": updated_str,
                                    "file": f"{date_str}.json",
                                }
                            )
                    except ValueError:
                        pass  # unparseable timestamp — skip

    # Build overlapping_token_ids: only those appearing in more than one file
    for tid, dates in token_to_files.items():
        if len(dates) > 1:
            report.overlapping_token_ids[tid] = dates

    return report


def _print_audit_report(
    report: AuditReport,
    daily_files: Dict[str, List[dict]],
) -> None:
    """Print formatted audit report to stdout."""
    date_strs = sorted(daily_files.keys())
    file_count = len(daily_files)

    date_range = ""
    if date_strs:
        date_range = f" ({date_strs[0]} to {date_strs[-1]})"

    print("=== lpagent Cache Audit ===")
    print()
    print(f"Daily files found: {file_count}{date_range}")
    print()

    # Overlaps
    overlap_count = len(report.overlapping_token_ids)
    print(f"Overlapping tokenIds (same position in multiple files): {overlap_count}")
    if overlap_count:
        # Print a few examples (up to 3)
        for tid, dates in list(report.overlapping_token_ids.items())[:3]:
            file_list = ", ".join(f"{d}.json" for d in sorted(dates))
            print(f"  Example: {tid} found in {file_list}")
    print()

    # Empty files
    empty_count = len(report.empty_date_files)
    print(f"Empty/zero-position files: {empty_count}")
    if empty_count:
        file_list = ", ".join(f"{d}.json" for d in sorted(report.empty_date_files))
        print(f"  {file_list}")
    print()

    # Bug-signature records
    bug_count = len(report.bug_signature_records)
    print(f"Bug-signature records (updatedAt > file_date + 24h): {bug_count}")
    for rec in report.bug_signature_records[:10]:
        print(
            f"  {rec['tokenId']} | createdAt={rec['createdAt']} | "
            f"updatedAt={rec['updatedAt']} | file={rec['file']}"
        )
    print()

    # Summary
    all_token_ids: set = set()
    for positions in daily_files.values():
        for pos in positions:
            tid = pos.get("tokenId", "")
            if tid:
                all_token_ids.add(tid)
    unique_count = len(all_token_ids)
    print(f"Total unique tokenIds across all files: {unique_count}")
    print(f"Estimated post-migration JSONL size: {unique_count} records")


def _merge_to_jsonl(
    daily_files: Dict[str, List[dict]],
    existing_jsonl_path: Path,
) -> Dict[str, dict]:
    """Merge all daily-file records + existing JSONL into one dict keyed by tokenId.

    Deduplication priority:
    1. Newer updatedAt wins.
    2. Same updatedAt: later-dated source file wins (process in ascending order).
    3. Existing JSONL records (have fetched_at_utc from doc 026) beat daily-file records
       on same updatedAt (process existing JSONL last).

    Legacy records get fetched_at_utc injected as "{file_date}T00:00:00Z" if absent.
    Records with empty/missing tokenId are skipped.
    """
    merged: Dict[str, dict] = {}

    # Process in sorted ascending file-date order (earlier → later overwrites on tie)
    for date_str in sorted(daily_files.keys()):
        positions = daily_files[date_str]
        for pos in positions:
            tid = pos.get("tokenId", "")
            if not tid:
                continue

            # Inject fetched_at_utc if missing
            if "fetched_at_utc" not in pos:
                pos = dict(pos)  # don't mutate the original
                pos["fetched_at_utc"] = f"{date_str}T00:00:00Z"

            updated = pos.get("updatedAt", "")

            if tid not in merged:
                merged[tid] = pos
            else:
                existing_updated = merged[tid].get("updatedAt", "")
                if updated > existing_updated:
                    merged[tid] = pos
                elif updated == existing_updated:
                    # Same updatedAt: later-dated file wins (we process in ascending
                    # order so simply overwrite — later iteration overwrites earlier)
                    merged[tid] = pos

    # Now load existing JSONL and merge last (existing records win on same updatedAt)
    if existing_jsonl_path.exists():
        try:
            raw = existing_jsonl_path.read_bytes()
            for line in raw.splitlines():
                line_stripped = line.strip()
                if not line_stripped:
                    continue
                try:
                    rec = json.loads(line_stripped)
                    tid = rec.get("tokenId", "")
                    if not tid:
                        continue
                    updated = rec.get("updatedAt", "")
                    if tid not in merged:
                        merged[tid] = rec
                    else:
                        existing_updated = merged[tid].get("updatedAt", "")
                        if updated >= existing_updated:
                            # Existing JSONL beats or ties (>= gives priority to JSONL)
                            merged[tid] = rec
                except json.JSONDecodeError:
                    pass  # truncated line; skip
        except OSError as exc:
            logger.warning("Could not read existing JSONL %s: %s", existing_jsonl_path, exc)

    return merged


def _write_jsonl(records: Dict[str, dict], path: Path) -> None:
    """Write records dict to JSONL via atomic tmp → os.replace."""
    tmp = path.with_suffix(".tmp")

    lines = [json.dumps(rec, ensure_ascii=False) for rec in records.values()]
    content = "\n".join(lines)
    if content:
        content += "\n"

    tmp.write_text(content, encoding="utf-8")
    os.replace(str(tmp), str(path))
    logger.info("JSONL written: %d records to %s", len(records), path)


def _verify_integrity(
    merged_records: Dict[str, dict],
    daily_files: Dict[str, List[dict]],
    existing_jsonl_path: Path,
) -> bool:
    """Verify that all source tokenIds are represented in the merged output.

    Returns True on success, prints error and returns False on mismatch.
    Does NOT call sys.exit — caller decides.
    """
    all_token_ids: set = set()

    # Collect from daily files
    for positions in daily_files.values():
        for pos in positions:
            tid = pos.get("tokenId", "")
            if tid:
                all_token_ids.add(tid)

    # Collect from existing JSONL
    if existing_jsonl_path.exists():
        try:
            for line in existing_jsonl_path.read_bytes().splitlines():
                line_stripped = line.strip()
                if not line_stripped:
                    continue
                try:
                    rec = json.loads(line_stripped)
                    tid = rec.get("tokenId", "")
                    if tid:
                        all_token_ids.add(tid)
                except json.JSONDecodeError:
                    pass
        except OSError:
            pass

    expected = len(all_token_ids)
    actual = len(merged_records)

    if actual != expected:
        print(
            f"ERROR: Integrity check FAILED: expected {expected} unique tokenIds, "
            f"got {actual} in JSONL."
        )
        return False

    print(
        f"Integrity check: {expected} unique tokenIds in source files, "
        f"{actual} lines in JSONL. OK."
    )
    return True


def _set_read_only(path: Path) -> None:
    """Set file to read-only (0o444). Logs warning on failure."""
    try:
        os.chmod(path, 0o444)
    except OSError as exc:
        logger.warning(
            "Could not set read-only on %s: %s — set permissions manually if needed",
            path,
            exc,
        )


def _archive_daily_files(
    cache_dir: Path,
    dry_run: bool,
    loaded_date_stems: Optional[set] = None,
) -> int:
    """Move successfully-loaded *.json files to archive/; set read-only.

    Args:
        cache_dir: The cache directory containing daily JSON files.
        dry_run: If True, print what would be moved but do not move anything.
        loaded_date_stems: Set of date stems (e.g. {"2026-04-28"}) that were
            successfully loaded by _load_daily_files. Files NOT in this set
            (e.g. malformed files) are skipped.

    Returns:
        Number of files moved (0 in dry-run mode).
    """
    archive_dir = cache_dir / "archive"
    archive_dir.mkdir(exist_ok=True)
    moved = 0

    for path in sorted(cache_dir.glob("*.json")):
        # Skip files that failed to load (e.g., malformed JSON)
        if loaded_date_stems is not None and path.stem not in loaded_date_stems:
            logger.info("Skipping non-loaded file %s (malformed or not a date file)", path.name)
            continue

        dest = archive_dir / path.name
        if dest.exists():
            logger.info("Already archived: %s — skipping", path.name)
            continue

        if dry_run:
            print(f"  [dry-run] Would move {path.name} to archive/")
        else:
            path.rename(dest)
            try:
                _set_read_only(dest)
            except OSError as exc:
                logger.warning(
                    "Could not set read-only on %s: %s — set permissions manually if needed",
                    dest,
                    exc,
                )
            moved += 1

    return moved


def _jsonl_path_for_wallet(cache_dir: Path, wallet: str) -> Path:
    """Return the JSONL path for the given wallet (matches LpAgentClient convention)."""
    prefix = wallet[:5]
    return cache_dir / f"positions_{prefix}.jsonl"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv: Optional[List[str]] = None) -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Audit and migrate lpagent daily cache to flat JSONL format."
    )

    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--audit",
        action="store_true",
        help="Read-only audit: report overlaps, gaps, and bug-signature records.",
    )
    mode_group.add_argument(
        "--migrate",
        action="store_true",
        help=(
            "Full migration: merge daily files to JSONL, verify integrity, "
            "archive daily files."
        ),
    )

    parser.add_argument(
        "--cache-dir",
        default=DEFAULT_CACHE_DIR,
        help=f"Path to lpagent cache directory (default: {DEFAULT_CACHE_DIR})",
    )
    parser.add_argument(
        "--wallet",
        default=os.environ.get("LPAGENT_WALLET", DEFAULT_WALLET),
        help="Wallet address (default: LPAGENT_WALLET env var or hardcoded default)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="With --migrate: write JSONL but do NOT move files to archive/.",
    )

    args = parser.parse_args(argv)

    if args.dry_run and args.audit:
        parser.error("--dry-run is only valid with --migrate")

    cache_dir = Path(args.cache_dir)

    if not cache_dir.exists():
        print("No legacy daily files found. Nothing to audit.")
        sys.exit(0)

    # Load daily files (returns dict + list of successfully-loaded filenames)
    daily_files, loaded_names = _load_daily_files(cache_dir)

    if not daily_files:
        if args.audit:
            print("No legacy daily files found. Nothing to audit.")
            sys.exit(0)
        else:
            # --migrate with no remaining daily files: already migrated, nothing to do
            print("No legacy daily files found in cache directory. Nothing to migrate.")
            sys.exit(0)

    # --- Audit mode ---
    if args.audit:
        report = _audit(daily_files)
        _print_audit_report(report, daily_files)
        sys.exit(0)

    # --- Migrate mode ---
    jsonl_path = _jsonl_path_for_wallet(cache_dir, args.wallet)

    # Load existing JSONL before writing (for integrity check)
    existing_jsonl_path = jsonl_path

    # Merge records
    merged = _merge_to_jsonl(daily_files, existing_jsonl_path)

    # Verify integrity BEFORE moving any files
    ok = _verify_integrity(merged, daily_files, existing_jsonl_path)
    if not ok:
        print("Aborting migration — no files moved to archive.")
        sys.exit(1)

    # Write merged JSONL (atomic)
    _write_jsonl(merged, jsonl_path)
    print(f"JSONL written: {len(merged)} records to {jsonl_path}")

    # Archive daily files (or dry-run preview)
    # Use loaded_names (filenames) so only successfully-loaded files are archived;
    # malformed files that were skipped during loading are left in place.
    loaded_stems = {Path(n).stem for n in loaded_names}
    moved = _archive_daily_files(cache_dir, dry_run=args.dry_run, loaded_date_stems=loaded_stems)

    if args.dry_run:
        print(
            f"[dry-run] Migration complete. JSONL written ({len(merged)} records). "
            f"Files NOT moved (dry-run mode)."
        )
    else:
        print(f"Migration complete. {moved} daily file(s) moved to archive/.")

    sys.exit(0)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )
    main()
