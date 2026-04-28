"""Detect gaps in position timestamp coverage and report to stdout."""

import csv
import re
import statistics
from datetime import datetime
from pathlib import Path


def detect_coverage_gaps(positions_csv_path: str) -> None:
    """Detect and report coverage gaps in position timestamps.

    Args:
        positions_csv_path: Path to positions.csv file
    """
    # Check if CSV exists
    csv_path = Path(positions_csv_path)
    if not csv_path.exists():
        return

    # Read all timestamps from CSV
    timestamps = []
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Parse datetime_open
                dt_open = row.get('datetime_open', '').strip()
                if dt_open:
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M'):
                        try:
                            timestamps.append(datetime.strptime(dt_open, fmt))
                            break
                        except ValueError:
                            continue

                # Parse datetime_close
                dt_close = row.get('datetime_close', '').strip()
                if dt_close:
                    for fmt in ('%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M', '%Y-%m-%d %H:%M'):
                        try:
                            timestamps.append(datetime.strptime(dt_close, fmt))
                            break
                        except ValueError:
                            continue
    except Exception:
        return  # Silently fail on any CSV read error

    # Need at least 2 timestamps to detect gaps
    if len(timestamps) < 2:
        return

    # Sort timestamps
    timestamps.sort()

    # Calculate consecutive gaps in minutes
    gaps = []
    for i in range(len(timestamps) - 1):
        gap_minutes = (timestamps[i + 1] - timestamps[i]).total_seconds() / 60
        gaps.append((gap_minutes, timestamps[i], timestamps[i + 1]))

    # Calculate median gap
    gap_values = [g[0] for g in gaps]
    median_gap = statistics.median(gap_values)

    # Dynamic threshold: max(180, min(median * 20, 360))
    threshold_minutes = max(180, min(median_gap * 20, 360))

    # Filter gaps above threshold
    significant_gaps = [(gap_min, start, end) for gap_min, start, end in gaps if gap_min > threshold_minutes]

    # Format time values (hours if >= 60, else minutes)
    def format_time(minutes):
        if minutes >= 60:
            return f"{minutes / 60:.1f}h"
        else:
            return f"{int(minutes)}m"

    # Load allowlist (paste full gap lines or just "MM-DD HH:MM -> MM-DD HH:MM")
    allowlist = set()
    allowlist_path = csv_path.parent / 'gap_allowlist.txt'
    if allowlist_path.exists():
        for line in allowlist_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # Extract date range: "MM-DD HH:MM -> MM-DD HH:MM" from anywhere in line
            m = re.search(r'(\d{2}-\d{2} \d{2}:\d{2}) -> (\d{2}-\d{2} \d{2}:\d{2})', line)
            if m:
                allowlist.add(f"{m.group(1)} -> {m.group(2)}")

    # Print results
    threshold_str = format_time(threshold_minutes)
    median_str = format_time(median_gap)

    if significant_gaps:
        # Sort chronologically (oldest first)
        significant_gaps.sort(key=lambda x: x[1])

        new_gaps = []
        allowed_count = 0
        for gap_min, start, end in significant_gaps:
            start_str = start.strftime('%m-%d %H:%M')
            end_str = end.strftime('%m-%d %H:%M')
            range_key = f"{start_str} -> {end_str}"
            if range_key in allowlist:
                allowed_count += 1
            else:
                new_gaps.append((gap_min, start_str, end_str))

        if new_gaps:
            print(f"\nCoverage Gaps (threshold: {threshold_str}, median: {median_str})")
            for gap_min, start_str, end_str in new_gaps:
                gap_str = format_time(gap_min)
                print(f"  ! {gap_str} gap: {start_str} -> {end_str}")
            if allowed_count:
                print(f"  ({allowed_count} allowed gap(s) hidden - see gap_allowlist.txt)")
            print(f"  {len(new_gaps)} potential gap(s) detected")
        elif allowed_count:
            print(f"\nNo new coverage gaps (threshold: {threshold_str}, {allowed_count} allowed gap(s) hidden)")
    else:
        print(f"\nNo coverage gaps detected (threshold: {threshold_str}, median: {median_str})")
