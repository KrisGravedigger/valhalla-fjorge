# [013] LpAgent Pipeline Integration

## Overview
This doc covers wiring the lpagent cross-check system into `valhalla_parser_v2.py`: a new `--cross-check` CLI flag for manual date-range queries, an auto-run hook at the end of every normal pipeline run, and a watermark file (`output/lpagent_sync.json`) that tracks the last synced date so the auto-run never re-queries the same days.

## Context
After docs 011 and 012 are in place, the cross-check logic exists but has no entry point. This doc adds two ways to trigger it:

1. **Manual**: `python valhalla_parser_v2.py --cross-check [from_date [to_date]]` — fetch and backfill a specific date or range
2. **Auto**: at the end of every normal pipeline run, fetch from `last_synced_date + 1` to yesterday, silently skip if 0 gaps

The existing parser already has `--no-input` mode (skip log processing, re-run analysis only). The cross-check auto-run should happen in both normal and `--no-input` modes.

## Goals
- Add `--cross-check [from_date [to_date]]` argument to the parser
- When `--cross-check` is used: skip normal log processing, run cross-check only, write backfill rows to `positions.csv`, print the report
- Auto-run cross-check at end of every pipeline execution (after positions.csv is written), using watermark to determine date range
- Watermark file `output/lpagent_sync.json`: read on start, write on success
- Missing watermark: default `last_synced_date` to `2026-02-11` (first day of our tracking)
- If auto-run finds 0 missing positions: print nothing (fully silent)
- If auto-run finds gaps: print the cross-check report and update watermark
- Load `LPAGENT_API_KEY` and `LPAGENT_WALLET` from `.env` (via `python-dotenv` or `os.environ`)

## Non-Goals
- Automated scheduling (cron, background tasks)
- Interactive prompts during cross-check
- Modifying the `--merge` workflow (merging multiple CSVs — separate path)

## Design

### New CLI Argument

```python
parser.add_argument(
    '--cross-check',
    nargs='*',         # 0, 1, or 2 positional values after the flag
    metavar='DATE',
    help='Run lpagent cross-check. Optional: FROM_DATE [TO_DATE] in YYYY-MM-DD format. '
         'If no dates given, uses watermark to yesterday.'
)
```

Usage examples:
- `--cross-check` → use watermark to yesterday
- `--cross-check 2026-03-31` → single day (from=to=2026-03-31)
- `--cross-check 2026-02-11 2026-03-31` → date range

When `--cross-check` is present, the pipeline should skip normal log processing but still load and write `positions.csv`.

### Watermark File

Path: `output/lpagent_sync.json`

Format:
```json
{ "last_synced_date": "2026-04-01" }
```

Read at pipeline start. Write after a successful cross-check run (whether manual or auto). The value written is `to_date` of the completed sync.

If file does not exist: `last_synced_date = "2026-02-11"` (hardcoded default — first day of tracking).

### Helper Functions

Add these functions to `valhalla_parser_v2.py` (or a new `valhalla/lpagent_integration.py` module if the main file gets too long):

```python
def _read_watermark(output_dir: str) -> str:
    """Read last_synced_date from lpagent_sync.json. Returns YYYY-MM-DD string."""
    ...

def _write_watermark(output_dir: str, date: str) -> None:
    """Write last_synced_date to lpagent_sync.json."""
    ...

def _run_cross_check(
    from_date: str,
    to_date: str,
    positions_csv_path: str,
    output_dir: str,
    silent_if_empty: bool = False
) -> int:
    """Run full cross-check: fetch, compare, append missing rows, return count of missing."""
    ...
```

### `_run_cross_check` Logic

```
1. Load LPAGENT_API_KEY from os.environ (raise ValueError if missing)
2. Load LPAGENT_WALLET from os.environ (fallback to hardcoded default)
3. Instantiate LpAgentClient(api_key, wallet, cache_dir=output_dir+"/lpagent_cache")
4. Call client.fetch_range(from_date, to_date) → raw list
5. Instantiate CrossChecker(positions_csv_path)
6. Call checker.find_missing(raw_list) → missing list
7. If missing is empty and silent_if_empty: return 0
8. Call checker.report(missing)
9. If missing not empty:
   a. Read existing positions.csv
   b. Append missing rows (convert to dicts via existing CsvWriter field order)
   c. Write positions.csv back
   d. Print "  Added N backfill rows to positions.csv"
10. Return len(missing)
```

For step 9b-c: read the existing CSV rows as dicts, append the missing `MatchedPosition` objects serialized as dicts (same fieldnames as the rest of the file), write back. Use the same fieldnames list already defined in `merge.py`'s `merge_positions_csvs` function.

### Auto-Run Hook Placement

In the `main()` function, identify where `positions.csv` is finalized (after CsvWriter writes it). Add the auto-run after that point, but only when not in `--merge` mode:

```python
# Auto-run lpagent cross-check (after positions.csv is written)
if not args.merge and LPAGENT_API_KEY_available:
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    watermark = _read_watermark(output_dir)
    next_day = (datetime.strptime(watermark, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    if next_day <= yesterday:
        print(f"\n[Cross-check] Syncing {next_day} → {yesterday}...")
        count = _run_cross_check(next_day, yesterday, positions_csv, output_dir, silent_if_empty=True)
        if count == 0:
            pass  # silent
        else:
            _write_watermark(output_dir, yesterday)
    # If watermark is already yesterday: fully silent, no print
```

If `LPAGENT_API_KEY` is not set in the environment: skip auto-run entirely (don't print any warning — not everyone has an API key).

### `--cross-check` Mode Flow

When `args.cross_check is not None` (the flag is present):

```python
# Parse date arguments
if len(args.cross_check) == 0:
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    watermark = _read_watermark(output_dir)
    from_date = (datetime.strptime(watermark, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    to_date = yesterday
elif len(args.cross_check) == 1:
    from_date = to_date = args.cross_check[0]
else:
    from_date, to_date = args.cross_check[0], args.cross_check[1]

print(f"[Cross-check] {from_date} → {to_date}")
count = _run_cross_check(from_date, to_date, positions_csv, output_dir, silent_if_empty=False)
if count > 0:
    _write_watermark(output_dir, to_date)
```

In `--cross-check` mode: skip normal log parsing, skip Meteora API calls, skip report generation. Load `positions.csv` if it exists (for the `CrossChecker`), write it back if backfill rows were added. Then exit.

### Environment Variable Loading

The project uses `python-dotenv` (check `requirements.txt` or existing usage). Check if `.env` is already loaded at startup. If yes: `os.environ.get('LPAGENT_API_KEY')` will work. If not loaded yet: add `load_dotenv()` call near top of `main()` (check existing pattern in codebase first — do not add a second `load_dotenv()` if one already exists).

### .env Updates

`.env` (actual file, not committed): add two lines:
```
LPAGENT_API_KEY=lpagent_xxx
LPAGENT_WALLET=J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF
```

`.env.example` (committed): add:
```
# lpagent API for cross-checking positions (optional)
LPAGENT_API_KEY=lpagent_xxx
LPAGENT_WALLET=J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bSSVArF
```

## Implementation Plan

1. Check how `.env` is loaded in `valhalla_parser_v2.py` (grep for `load_dotenv` or `dotenv`) — note the pattern
2. Add `--cross-check` argument to the `argparse` block in `main()`
3. Add `_read_watermark` and `_write_watermark` helper functions
4. Add `_run_cross_check` helper function (imports `LpAgentClient` and `CrossChecker`)
5. Add `--cross-check` mode handling in `main()`: parse dates, call `_run_cross_check`, write watermark, exit early
6. Add auto-run hook at the end of the normal pipeline (after CSV write, before exit)
7. Update `.env.example` with the two new variables
8. Manual test: `python valhalla_parser_v2.py --cross-check 2026-03-31` with a real API key

## Dependencies
- **Requires [011-lpagent-client]**: `LpAgentClient` must exist
- **Requires [012-lpagent-cross-check]**: `CrossChecker` must exist
- **Existing**: `valhalla_parser_v2.py` main pipeline structure

## Testing
- `--cross-check 2026-04-01` with valid API key: should print report and update `lpagent_sync.json`
- Re-run same command: should use cache (0 API requests), print same report
- `--cross-check` without dates: should compute from watermark to yesterday automatically
- Normal pipeline run (with log files): should silently auto-run cross-check at end
- Normal pipeline run with no `LPAGENT_API_KEY`: should complete normally, no cross-check, no error

## Alternatives Considered
- **Separate entrypoint script (`cross_check.py` at root)**: Rejected — keeps CLI surface unified; users already know `valhalla_parser_v2.py`
- **Auto-run before writing positions.csv (pre-write hook)**: Rejected — cross-check appends to the already-written CSV; post-write is simpler and avoids interfering with the main pipeline write

## Open Questions
- Does `valhalla_parser_v2.py` already call `load_dotenv()`? If not, need to add it. Implementer should check before adding.
- The auto-run watermark write only happens when `count > 0` (gaps found). Should it also update when `count == 0` to avoid re-querying clean days? Recommendation: yes — always update watermark on successful sync, regardless of gap count. Implementer should apply this.
