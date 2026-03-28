"""
Backfill target_wallet_address and target_tx_signature in positions.csv
by re-scanning all archive files.

Run from project root:
    python tools/backfill_target_data.py
"""

import csv
import sys
from pathlib import Path

# Allow importing valhalla package from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from valhalla.readers import PlainTextReader
from valhalla.event_parser import EventParser


def _build_index_from_archive(archive_dir: Path) -> dict:
    """
    Scan all archive files, parse open events, and return a dict:
        position_id -> (target_wallet_address, target_tx_signature)

    Only entries where at least one of the two fields is non-empty are included.
    """
    index: dict[str, tuple] = {}

    txt_files = sorted(archive_dir.glob("*.txt")) + sorted(archive_dir.glob("*.html"))
    print(f"Scanning {len(txt_files)} archive file(s)...")

    for filepath in txt_files:
        try:
            reader = PlainTextReader(str(filepath))
            messages = reader.read()
        except Exception as e:
            print(f"  WARNING: could not read {filepath.name}: {e}")
            continue

        parser = EventParser()
        parser.parse_messages(messages)

        for event in parser.open_events:
            pid = event.position_id
            if not pid:
                continue
            addr = event.target_wallet_address or ""
            sig = event.target_tx_signatures[0] if event.target_tx_signatures else ""
            if addr or sig:
                # Keep the best (non-empty) values seen for this position
                existing = index.get(pid, ("", ""))
                index[pid] = (addr or existing[0], sig or existing[1])

    print(f"Found open-event data for {len(index)} unique position ID(s).")
    return index


def backfill(positions_csv: Path, archive_dir: Path) -> None:
    if not positions_csv.exists():
        print(f"ERROR: {positions_csv} not found")
        return
    if not archive_dir.exists():
        print(f"ERROR: {archive_dir} not found")
        return

    index = _build_index_from_archive(archive_dir)

    rows = []
    with open(positions_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    updated = 0
    for row in rows:
        pid = row.get("position_id", "")
        if not pid:
            continue
        if row.get("target_wallet_address") and row.get("target_tx_signature"):
            continue  # already complete
        if pid not in index:
            continue

        addr, sig = index[pid]
        changed = False
        if not row.get("target_wallet_address") and addr:
            row["target_wallet_address"] = addr
            changed = True
        if not row.get("target_tx_signature") and sig:
            row["target_tx_signature"] = sig
            changed = True
        if changed:
            updated += 1

    print(f"Backfilling {updated} position(s)...")

    with open(positions_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Done. Updated {updated} row(s) in {positions_csv}.")


if __name__ == "__main__":
    root = Path(__file__).parent.parent
    backfill(root / "output" / "positions.csv", root / "archive")
