#!/usr/bin/env python3
"""One-shot migration: strip timestamp_open, date_open, timestamp_close, date_close from positions.csv"""

import csv
import shutil
from pathlib import Path

CSV_PATH = Path("output/positions.csv")
BACKUP_PATH = CSV_PATH.with_suffix(".csv.bak")
COLUMNS_TO_DROP = {"timestamp_open", "date_open", "timestamp_close", "date_close"}

if not CSV_PATH.exists():
    print("No output/positions.csv found, nothing to migrate.")
    exit(0)

# Read
with open(CSV_PATH, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    if not COLUMNS_TO_DROP.intersection(reader.fieldnames or []):
        print("CSV already migrated (old columns not found). Nothing to do.")
        exit(0)
    new_fields = [f for f in reader.fieldnames if f not in COLUMNS_TO_DROP]
    rows = list(reader)

# Backup
shutil.copy2(CSV_PATH, BACKUP_PATH)
print(f"Backup: {BACKUP_PATH}")

# Write
with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=new_fields, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)

print(f"Migrated {len(rows)} rows. Removed columns: {', '.join(sorted(COLUMNS_TO_DROP))}")
