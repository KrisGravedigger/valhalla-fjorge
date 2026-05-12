---
critical: true
---

# [026] lpagent Client JSONL Rewrite + Watermark Redesign (C.1 + C.2)

## Intent
Rewrite `valhalla/lpagent_client.py` to replace the broken per-day JSON cache with a
single flat JSONL file keyed on `tokenId`, add a sliding-window refresh that catches
positions opened before the watermark and closed after it, and replace the
`last_synced_date` scalar watermark with a structured JSON object that makes the cache
semantics explicit. This closes the silent-stale-data and silent-truncation bugs
identified in `PLAN-portfolio-truth.md` and documented in `notes/lpagent_api_semantics.md`.

## Overview
This doc covers rewriting `LpAgentClient` in `valhalla/lpagent_client.py` and
`read_watermark` / `write_watermark` in `valhalla/lpagent_pipeline.py`. The new client
stores all positions in `output/lpagent_cache/positions_{wallet_prefix}.jsonl`,
paginates via a `fetch_since(from_date_utc)` API, asserts `totalCount` on every fetch,
deduplicates by `tokenId`, and uses a 5-day sliding refresh window. Existing callers
(`lpagent_pipeline.run_cross_check`, `cross_check.CrossChecker`) must continue to work
unchanged via a back-compat shim on `fetch_range`. C.2 and C.1 are bundled because the
new watermark format is both written and consumed by the same client.

## Context
The current `LpAgentClient.fetch_day(date_str)` caches in `{cache_dir}/{date}.json`
with no wallet component in the name. The empirical findings in
`PLAN-portfolio-truth.md` (lines 63-76) show:
- `to_date` is silently ignored by the API — every file contains positions from
  `from_date` through "now-at-time-of-fetch".
- Adjacent daily files overlap; re-reading them returns stale snapshots.
- A position opened before the watermark and closed after it is invisible to all
  future fetches (max observed hold time ~87 hours).
- `totalCount` is returned in pagination but never asserted, so silent truncation can
  occur undetected.

The current watermark in `output/lpagent_sync.json` is a single key
`{"last_synced_date": "YYYY-MM-DD"}` that does not encode the wallet, the refresh
window, or the last fetch timestamp.

`valhalla/lpagent_pipeline.py` and `valhalla/cross_check.py` are the only callers of
`fetch_day` / `fetch_range`. Both will continue to work via the back-compat shim
described below.

## Goals
- New cache format: `output/lpagent_cache/positions_{wallet_prefix}.jsonl` — one
  JSON-encoded object per line, keyed/deduped by `tokenId`.
- New watermark format: structured JSON with `wallet`, `min_safe_open_date`,
  `last_full_refresh_at`, and `refresh_window_hours`.
- Every paginated fetch asserts `len(retrieved) == totalCount`; raises `AssertionError`
  with diagnostic context on mismatch.
- Sliding-window refresh of `refresh_window_hours` (default 120 h = 5 days) always
  re-fetches the tail regardless of watermark.
- On startup, client refuses to silently merge data if `LPAGENT_WALLET` does not match
  the cached wallet prefix.
- Partial JSONL (truncated last line from crash mid-write) is detected on open and
  automatically recovered (re-fetch the refresh window).
- Concurrent writes use `os.replace` for atomic rename (safe on NTFS / Windows).
- `fetch_range` is kept as a back-compat shim so `run_cross_check` in
  `lpagent_pipeline.py` needs no changes in this doc.

## Non-Goals
- Migrating or deleting existing daily `{date}.json` cache files — that is sub-project
  C.3 (doc 027).
- Changing `CrossChecker.find_missing` or `backfill` logic (doc 025 scope or later).
- The full reconciliation tool (`valhalla/reconcile.py` non-legacy mode — that is
  doc D-full, future).
- Updating `valhalla/cli.py` new commands — not in scope for this doc.
- Writing `notes/lpagent_api_semantics.md` — out of scope (PM does this manually or
  it is picked up in a future doc).

## Acceptance Criteria

### AC-1: JSONL cache file per wallet
- **Happy path**: Given `LPAGENT_WALLET=J4tkG...` and a successful fetch, the client
  writes to `output/lpagent_cache/positions_J4tkG.jsonl` (first 5 chars of wallet
  address as prefix). Each line of the file is valid JSON with at minimum the keys
  `tokenId`, `createdAt`, `updatedAt`, `fetched_at_utc`, and all raw API fields.
- **Adversarial**: Given `LPAGENT_WALLET=J4tkG...` at first run (new JSONL created),
  then `LPAGENT_WALLET=XXXXX...` on a second run pointing to the same `cache_dir`,
  the client raises `ValueError` with message "Wallet mismatch: cache was built for
  J4tkG, current wallet is XXXXX. Clear the cache or use a separate cache_dir."

### AC-2: `totalCount` assertion
- **Happy path**: Given a mock API that returns `totalCount=20` across two pages of 10
  each, the client retrieves all 20 records and asserts successfully.
- **Adversarial**: Given a mock API that reports `totalCount=50` but only returns 40
  rows (pagination ends early), the client raises `AssertionError` with message
  including `totalCount=50`, `retrieved=40`, and the query parameters used. It does
  NOT write any partial results to the JSONL file.

### AC-3: Sliding-window refresh
- **Happy path**: Given a JSONL with `last_full_refresh_at` 48 hours ago and
  `refresh_window_hours=120`, the next sync re-fetches the last 120 hours of positions
  (`fetch_since(now - 120h)`). Existing records outside the refresh window are
  preserved; records inside the window are re-merged (last-write-wins on `updatedAt`).
- **Adversarial**: Given `last_full_refresh_at` 23 hours ago (within the 24-hour skip
  threshold), the client skips the network fetch entirely and logs
  "Refresh skipped: last full refresh was N hours ago (threshold: 24h)".

### AC-4: Partial JSONL recovery
- **Happy path**: A clean JSONL with 100 complete lines opens normally and all 100
  records are loaded into the in-memory dedup map.
- **Adversarial**: A JSONL whose last line is `{"tokenId":"abc","pnlNative":0.` (no
  closing brace — simulates crash mid-write) is detected on open. The client logs a
  warning, truncates the file at the last complete line (99 records), then re-fetches
  the full refresh window to recover potentially lost data. Test asserts 99 records
  loaded before re-fetch and that the file on disk no longer contains the truncated
  line.

### AC-5: Atomic writes
- **Happy path**: A normal write cycle builds a new JSONL to a `.tmp` file in the same
  directory, then calls `os.replace` to atomically rename it over the live file.
  No half-written JSONL is ever visible to readers.
- **Adversarial**: Given a crash simulation (exception raised between writing `.tmp`
  and `os.replace`), the live JSONL remains intact at its last known-good state. The
  stale `.tmp` file is cleaned up on the next startup (if present, delete before
  writing new `.tmp`).

### AC-6: Back-compat `fetch_range` shim
- **Happy path**: A call to `client.fetch_range("2026-04-01", "2026-04-30")` still
  returns a `List[dict]` of raw position dicts (the new JSONL-loaded data filtered to
  records with `createdAt` in the requested range), so `run_cross_check` in
  `lpagent_pipeline.py` needs no modification.
- **Adversarial**: Given an empty JSONL (no cached data) and a call to `fetch_range`,
  the shim triggers `fetch_since(from_date)` to populate the cache, then filters and
  returns. The caller receives data, not an empty list.

### AC-7: Watermark format
- **Happy path**: After a successful full refresh, `output/lpagent_sync.json` contains:
  ```json
  {
    "wallet": "J4tkG...",
    "min_safe_open_date": "2026-02-11",
    "last_full_refresh_at": "2026-05-09T14:22:00Z",
    "refresh_window_hours": 120
  }
  ```
  `read_watermark()` returns a dict with these four keys. Legacy callers that only
  need `last_synced_date` receive it derived from `min_safe_open_date` for backwards
  compatibility.
- **Adversarial**: Given an `lpagent_sync.json` that still has the old format
  `{"last_synced_date": "2026-04-30"}`, `read_watermark()` parses it and promotes it
  to the new format (setting `min_safe_open_date` from `last_synced_date`, all other
  fields to defaults), logs a migration notice, and writes the new format back.
  The client does not crash on legacy watermarks.

## Touchable Files
- `valhalla/lpagent_client.py` — complete rewrite of `LpAgentClient`
- `valhalla/lpagent_pipeline.py` — `read_watermark` / `write_watermark` redesign; `run_cross_check` shim kept intact
- `valhalla/cli.py` — update 5 call sites that use old watermark signatures (see note below)
- `tests/test_lpagent_client.py` — new test file (replaces or extends existing if any)
- `tests/fixtures/lpagent_client/` — new fixture directory for mock API responses

**cli.py call site migration (added 2026-05-11):** `valhalla/cli.py` has 5 places using old watermark API:
- Two `read_watermark` call sites that do `datetime.strptime(watermark, "%Y-%m-%d")` — update to extract `watermark["min_safe_open_date"]` before strptime.
- Three `write_watermark(output_dir, to_date_string)` calls — update to pass the full dict form: `write_watermark(output_dir, {"wallet": wallet, "min_safe_open_date": to_date, "last_full_refresh_at": ..., "refresh_window_hours": REFRESH_WINDOW_HOURS})`.

Do NOT touch: `valhalla/cross_check.py`, `valhalla/reconcile.py`,
`output/lpagent_cache/*.json` (legacy daily files — left for doc 027).

## Verification Contract

```bash
# Unit tests — all must pass without network access
pytest tests/test_lpagent_client.py -v

# Type checking
mypy valhalla/lpagent_client.py valhalla/lpagent_pipeline.py --ignore-missing-imports

# Lint
ruff check valhalla/lpagent_client.py valhalla/lpagent_pipeline.py

# Back-compat smoke test: run_cross_check still works (no network; uses JSONL cache
# pre-populated by test fixtures — requires LPAGENT_API_KEY=fake in env and mocked
# urlopen to skip live calls)
# This test is part of test_lpagent_client.py (see AC-6 test case).

# Verify no existing tests are broken by the refactor
pytest tests/ -v --ignore=tests/test_lpagent_client.py -x
```

All tests must pass without `LPAGENT_API_KEY` set in the environment (mock it inside
tests). No `time.sleep` calls execute during tests — use monkeypatching.

## Design

### JSONL Cache Format

Each line in `positions_{wallet_prefix}.jsonl` is a JSON object:

```json
{
  "tokenId": "AbCd...",
  "fetched_at_utc": "2026-05-09T14:22:00Z",
  "createdAt": "2026-05-07T10:00:00Z",
  "updatedAt": "2026-05-09T12:00:00Z",
  "pnlNative": 0.045,
  "inputNative": 1.0,
  "outputNative": 1.045,
  ... (all other raw API fields preserved verbatim)
}
```

The file is not pretty-printed (one object per line, no indentation). This keeps it
scannable with `grep` and allows line-by-line parsing for truncation detection.

**Wallet prefix in filename**: first 5 characters of the wallet address.
`J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF` → `positions_J4tkG.jsonl`.

**Deduplication key**: `tokenId`. Last-write-wins on `updatedAt`. When merging new
fetch results into the existing JSONL, newer `updatedAt` always overwrites older.

### Constants

```python
DEFAULT_WALLET = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"
API_BASE = "https://api.lpagent.io/open-api/v1"
PAGE_SIZE = 10
RATE_LIMIT_SLEEP = 12          # seconds between API requests (5 RPM free tier)
REFRESH_WINDOW_HOURS = 120     # 5 days; max observed hold time ~87h + 33h buffer
REFRESH_THRESHOLD_HOURS = 24   # skip network fetch if last refresh was this recent
WATERMARK_DEFAULT_DATE = "2026-02-11"
```

### Class Structure: `LpAgentClient`

```python
class LpAgentClient:
    def __init__(self, api_key, wallet, cache_dir) -> None: ...

    # --- Public ---
    def fetch_since(self, from_date_utc: str) -> List[dict]:
        """Fetch all positions with createdAt >= from_date_utc, paginating fully.
        Asserts len(retrieved) == totalCount. Returns raw position list.
        Writes results to JSONL (merge + deduplicate)."""

    def fetch_range(self, from_date: str, to_date: str) -> List[dict]:
        """Back-compat shim. Triggers fetch_since(from_date) if cache is empty or
        stale, then returns records with createdAt in [from_date, to_date]."""

    def load_cache(self) -> Dict[str, dict]:
        """Load all records from the JSONL into a dict keyed by tokenId.
        Detects truncated last line and recovers (see AC-4)."""

    # --- Internal ---
    def _jsonl_path(self) -> Path: ...
    def _wallet_prefix(self) -> str: ...
    def _check_wallet_match(self, loaded_records: Dict[str, dict]) -> None: ...
    def _write_jsonl_atomic(self, records: Dict[str, dict]) -> None: ...
    def _fetch_all_pages(self, from_date_utc: str) -> List[dict]:
        """Paginates from page 1 until totalPages, asserts totalCount."""
    def _lpagent_get(self, url: str) -> dict: ...  # unchanged from current
```

### Watermark Schema

New `output/lpagent_sync.json`:
```json
{
  "wallet": "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF",
  "min_safe_open_date": "2026-02-11",
  "last_full_refresh_at": "2026-05-09T14:22:00Z",
  "refresh_window_hours": 120
}
```

**Semantics**: "We have all positions where `createdAt >= min_safe_open_date`, as
observed at `last_full_refresh_at`. Positions in the last `refresh_window_hours` are
re-fetched on every sync regardless."

**`read_watermark(output_dir) -> dict`** in `lpagent_pipeline.py`:
- If file missing: return defaults (`min_safe_open_date=WATERMARK_DEFAULT_DATE`,
  `refresh_window_hours=REFRESH_WINDOW_HOURS`, `last_full_refresh_at=None`).
- If file has old format (`last_synced_date` key, no `wallet` key): auto-promote to
  new format, log migration notice, write back. Return promoted dict.
- If file has new format: return as-is.

**`write_watermark(output_dir, watermark_dict) -> None`**: writes the full dict.
Existing `run_cross_check` calls `write_watermark` with the date string — update
`run_cross_check` to build the dict from the new structure and call the new signature.
The old string-based `write_watermark(output_dir, date_str)` signature must be removed
or overloaded gracefully (raise TypeError with a clear message if called with a string).

### Fetch Logic: Sliding Window Refresh

```
on sync():
    watermark = read_watermark()
    if watermark.last_full_refresh_at is not None:
        hours_ago = (now_utc - last_full_refresh_at).total_seconds() / 3600
        if hours_ago < REFRESH_THRESHOLD_HOURS:
            log "Refresh skipped: last full refresh was N hours ago"
            return load_cache()

    # Always re-fetch the sliding window tail
    refresh_from = now_utc - timedelta(hours=REFRESH_WINDOW_HOURS)
    new_records = fetch_since(max(min_safe_open_date, refresh_from_str))

    # Merge into existing JSONL (deduplicate by tokenId, newer updatedAt wins)
    existing = load_cache()
    merged = {**existing}
    for rec in new_records:
        tid = rec["tokenId"]
        if tid not in merged or rec["updatedAt"] > merged[tid]["updatedAt"]:
            merged[tid] = {**rec, "fetched_at_utc": now_utc_iso}
    write_jsonl_atomic(merged)
    write_watermark(output_dir, new_watermark_dict)
    return list(merged.values())
```

### `totalCount` Assertion

After the final page is retrieved:
```python
if len(all_positions) != total_count:
    raise AssertionError(
        f"lpagent API totalCount mismatch: "
        f"expected {total_count}, retrieved {len(all_positions)} "
        f"(from_date={from_date_utc}, pages={total_pages})"
    )
```
This check fires before any write to the JSONL — partial data is never persisted.

### Truncation Detection

On `load_cache()`:
```python
lines = path.read_bytes().splitlines()
valid_lines = []
for line in lines:
    try:
        json.loads(line)
        valid_lines.append(line)
    except json.JSONDecodeError:
        logger.warning("Truncated line detected at record %d — truncating file and scheduling re-fetch", len(valid_lines))
        break  # stop at first bad line
if len(valid_lines) < len(lines):
    # Rewrite the file with only valid lines
    path.write_bytes(b"\n".join(valid_lines) + b"\n")
    # Signal caller to re-fetch refresh window
    self._needs_refresh = True
```

### Error Handling

| Error condition | Behavior |
|---|---|
| `LPAGENT_API_KEY` not set | `ValueError` at `__init__` (unchanged from current) |
| HTTP non-200 from API | `RuntimeError` with status code + body snippet (unchanged) |
| JSON decode failure from API | `RuntimeError` with response preview (unchanged) |
| `totalCount` mismatch | `AssertionError` with context; no JSONL write |
| Wallet mismatch on cache | `ValueError` with clear message; no data merged |
| JSONL truncation detected | Log warning, truncate file, set re-fetch flag |
| Stale `.tmp` file on startup | Delete before writing new `.tmp` |

### Fixture Files for Tests

Place in `tests/fixtures/lpagent_client/`:

- `page1_of_2.json` — mock API response: `totalCount=20`, `totalPages=2`, `data=[10 positions]`
- `page2_of_2.json` — mock API response: `totalPages=2`, `data=[10 positions]`
- `page1_truncated.json` — mock API response: `totalCount=50`, `totalPages=5`, `data=[10]`
  (pagination will "end early" after page 1 to simulate totalCount mismatch)
- `sample_positions.jsonl` — 5 complete JSON lines as a known-good cache state for
  load tests
- `truncated_positions.jsonl` — 4 complete JSON lines + 1 truncated line (no closing
  brace) for AC-4 testing

## Implementation Plan

1. **Add constants** to `valhalla/lpagent_client.py`: `REFRESH_WINDOW_HOURS`,
   `REFRESH_THRESHOLD_HOURS`, `WATERMARK_DEFAULT_DATE`. Remove the concept of a
   "today" skip (the new model has no per-day files).

2. **Rewrite `LpAgentClient.__init__`**: accept same parameters; compute
   `_wallet_prefix`; check for stale `.tmp` file and delete it; do NOT read JSONL yet
   (lazy load).

3. **Implement `_fetch_all_pages(from_date_utc)`**: paginate from page 1, accumulate
   positions, assert `totalCount` after last page. Raise `AssertionError` on mismatch.
   Do not write to JSONL here.

4. **Implement `load_cache()`**: read JSONL line by line, detect truncated last line,
   recover file, set `_needs_refresh`. Return `Dict[str, dict]` keyed by `tokenId`.

5. **Implement `_write_jsonl_atomic(records)`**: write to `.tmp` → `os.replace`.

6. **Implement `_check_wallet_match(loaded_records)`**: read `wallet` from the
   watermark dict; if set and does not match `self._wallet`, raise `ValueError`.

7. **Implement `fetch_since(from_date_utc)`**: call `_fetch_all_pages`, merge into
   `load_cache()` result (newer `updatedAt` wins), call `_write_jsonl_atomic`. Add
   `fetched_at_utc` to each merged record.

8. **Implement `fetch_range(from_date, to_date)` shim**: if JSONL missing or
   `_needs_refresh`, call `fetch_since(from_date)`. Load cache, filter by
   `createdAt in [from_date, to_date]`, return as `List[dict]`.

9. **Rewrite `read_watermark` and `write_watermark` in `lpagent_pipeline.py`**:
   new schema, legacy auto-promotion, new `write_watermark(output_dir, dict)` signature.
   Update `run_cross_check` to pass the dict form.

10. **Write `tests/test_lpagent_client.py`**: one test per AC, using monkeypatched
    `urllib.request.urlopen` and `time.sleep`, `tmp_path` for all file I/O.

## Dependencies
- **Independent**: can be implemented in any order after docs 023-025 are shipped.
- **Required before doc 027**: the new JSONL format must exist before the migration
  script can target it.
- External: Python stdlib only (`json`, `os`, `logging`, `pathlib`, `urllib`,
  `datetime`).

## Testing

All tests in `tests/test_lpagent_client.py`. Key test cases:

- `test_pagination_two_pages_asserts_totalcount` — mock two pages, assert count.
- `test_totalcount_mismatch_raises_assertionerror` — truncated page stream raises.
- `test_truncated_jsonl_recovery` — uses `truncated_positions.jsonl` fixture; asserts
  file repaired and `_needs_refresh=True`.
- `test_wallet_mismatch_raises` — watermark has different wallet; assert `ValueError`.
- `test_atomic_write_leaves_live_file_intact` — simulate crash before `os.replace`;
  live file unchanged.
- `test_fetch_range_shim_filters_by_date` — pre-loaded JSONL with 20 records; shim
  filters to requested window.
- `test_read_watermark_legacy_format_promoted` — old format auto-promoted; new format
  written back.
- `test_refresh_skipped_within_threshold` — watermark `last_full_refresh_at` is 10h
  ago; no `urlopen` called.
- `test_no_sleep_in_tests` — monkeypatch `time.sleep`; assert it is not called with
  value > 0 during any test.

## Alternatives Considered
- **Keep per-day JSON, fix `to_date` handling**: Rejected — `to_date` is ignored by
  the API regardless of what we send (empirically verified). A per-day model is
  structurally mismatched with how the API actually works.
- **SQLite instead of JSONL**: Rejected — adds a dependency, harder to inspect
  manually, and the data volume (thousands of rows) does not justify it.
- **Split C.1 and C.2 into separate docs**: Rejected — the watermark schema is both
  written and consumed in the same file (`lpagent_client.py` calls
  `read_watermark`/`write_watermark`). Splitting would create a temporary broken state
  where the new client writes a format the old watermark reader cannot parse.

## Open Questions
- **`fetch_range` shim date semantics**: the shim filters by `createdAt in [from_date, to_date]`. This matches the current behavior (positions opened in that range). Confirm with PM if `cross_check.CrossChecker` downstream callers need any other date field.
- **Rate limit**: the current `RATE_LIMIT_SLEEP=12s` (5 RPM). If lpagent upgrades the tier, this constant should be tunable. For now, keep it hardcoded but named.
