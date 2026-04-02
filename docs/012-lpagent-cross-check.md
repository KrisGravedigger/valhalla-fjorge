# [012] LpAgent Cross-Check and Backfill Logic

## Overview
This doc covers building `valhalla/cross_check.py` — the comparison engine that diffs lpagent positions against the local `positions.csv`, identifies missing entries, converts them into backfill rows, and reports the gap. It also extends `valhalla/merge.py` to handle replacement of lpagent backfill rows when Discord logs later provide the full data.

## Context
After `011-lpagent-client` is in place, we have raw lpagent position data. This doc turns that data into actionable output: a list of `MatchedPosition` objects (using the existing model) that can be written directly to `positions.csv` via the existing `CsvWriter`. The key dedup field is `full_address` (= lpagent `tokenId`).

The existing `merge.py` has a `merge_with_existing_csv` function with merge rules keyed on `pnl_source`. Currently the rules are: `meteora` = never overwrite, `pending`/`discord` = can upgrade. We need to add a rule for `pnl_source="lpagent"` rows: they are placeholders that should be replaced when Discord data arrives.

## Goals
- Compare lpagent positions against local CSV by `full_address` (exact match, no fuzzy)
- Produce a list of missing `MatchedPosition` objects with `pnl_source="lpagent"` and `target_wallet="lpagent_backfill"`
- Print a human-readable report: count missing, sum of missing PnL in SOL
- Append missing rows to `positions.csv` (or produce a merged list for the caller)
- Extend `merge_with_existing_csv` so that an existing lpagent row is replaced by incoming Discord-sourced data for the same `full_address`

## Non-Goals
- Fetching data from API (handled by doc 011)
- Writing to CSV directly (caller uses existing `CsvWriter`)
- Deduplication within a single lpagent API response
- Handling positions where `full_address` is empty in CSV

## Design

### CrossChecker Class

File: `valhalla/cross_check.py`

```python
class CrossChecker:
    def __init__(self, positions_csv_path: str):
        ...

    def find_missing(self, lpagent_positions: List[dict]) -> List[MatchedPosition]:
        """Compare lpagent_positions against CSV. Return list of MatchedPosition
        objects for positions missing from CSV."""
        ...

    def report(self, missing: List[MatchedPosition]) -> None:
        """Print summary report to stdout."""
        ...
```

### find_missing Logic

1. Read `positions.csv`, collect all `full_address` values into a set (ignore empty strings)
2. For each lpagent position in `lpagent_positions`:
   - Extract `tokenId` as the key
   - If `tokenId` not in the set → it's missing
   - Convert to `MatchedPosition` using the field mapping below
3. Return the list of missing `MatchedPosition` objects

### Field Mapping: lpagent → MatchedPosition

| `MatchedPosition` field | lpagent field | Notes |
|---|---|---|
| `datetime_open` | `createdAt` | Parse ISO string, format as `YYYY-MM-DDTHH:MM` |
| `datetime_close` | `updatedAt` | Same formatting |
| `target_wallet` | — | Hardcoded `"lpagent_backfill"` |
| `token` | `token0Info.token_symbol` | May be absent → `""` |
| `position_type` | — | Hardcoded `"Spot"` |
| `sol_deployed` | `inputNative` | `Decimal(str(value))` or `None` if absent |
| `sol_received` | `outputNative` | Same |
| `pnl_sol` | `pnlNative` | Same |
| `pnl_pct` | `pnl.percentNative` | `pnl` is a nested object; `.get("pnl", {}).get("percentNative")` |
| `position_id` | `tokenId[:8]` | First 8 characters |
| `full_address` | `tokenId` | Full string |
| `pnl_source` | — | Hardcoded `"lpagent"` |
| `meteora_deposited` | `inputNative` | Same as `sol_deployed` |
| `meteora_withdrawn` | `outputNative` | Same as `sol_received` |
| `meteora_fees` | `collectedFeeNative` | `Decimal` or `None` |
| `meteora_pnl` | `pnlNative` | Same as `pnl_sol` |
| `close_reason` | — | `"normal"` (best guess; no Discord data) |
| All other fields | — | `None` or `0` or `""` per field type |

Datetime formatting: strip to minute precision. Input is ISO 8601 like `"2026-03-31T14:23:45.000Z"` — parse with `datetime.fromisoformat` after stripping trailing `Z`, then format as `"%Y-%m-%dT%H:%M"`.

### report() Output

```
  Cross-check results:
    - lpagent positions checked: 47
    - Missing from positions.csv: 3
    - Estimated missing PnL: +0.0842 SOL
    - Missing positions:
        ABCD1234 | TOKEN | 2026-03-31T14:23 | +0.042 SOL
        EFGH5678 | TOKEN2 | 2026-03-30T09:11 | -0.012 SOL
        ...
```

If 0 missing: print `"  Cross-check: 0 missing positions — all clear."` and return.

### merge.py Extension: lpagent Row Replacement

In `merge_with_existing_csv`, the existing merge rules process `existing_by_id` keyed on `position_id`. However, lpagent rows use `tokenId[:8]` as `position_id`, which is the same key as Discord-sourced rows for the same position. This means the replacement happens naturally through the existing merge logic — BUT only if the Discord data wins over the lpagent data.

Add a new rule after Rule 3 (but before Rule 4) in the existing if/elif chain:

**Rule 3.5: `pnl_source="lpagent"` — treat as replaceable placeholder**

```python
# Rule 3.5: lpagent backfill row — replace with Discord data if available
if existing_pos.pnl_source == "lpagent":
    if new_matched_pos:
        # Discord data arrived — replace entirely, keep lpagent financial fields
        # as fallback only if Discord doesn't have them
        new_matched_pos.meteora_deposited = new_matched_pos.meteora_deposited or existing_pos.meteora_deposited
        new_matched_pos.meteora_withdrawn = new_matched_pos.meteora_withdrawn or existing_pos.meteora_withdrawn
        new_matched_pos.meteora_fees = new_matched_pos.meteora_fees or existing_pos.meteora_fees
        new_matched_pos.meteora_pnl = new_matched_pos.meteora_pnl or existing_pos.meteora_pnl
        merged_matched.append(new_matched_pos)
        upgraded_count += 1
    else:
        # No Discord data yet — keep as-is
        merged_matched.append(existing_pos)
        kept_from_existing_count += 1
    continue
```

This rule must be placed before Rule 4 (the generic pending/discord upgrade rule) so lpagent rows are handled explicitly.

Also update the merge stats print to include a counter for lpagent replacements.

### Deduplication Within lpagent Response

lpagent may return the same `tokenId` twice if a position spans date boundaries (edge case). Apply `{pos["tokenId"]: pos for pos in lpagent_positions}.values()` dedup in `find_missing` before comparing.

## Implementation Plan

1. Create `valhalla/cross_check.py` with `CrossChecker` class
2. Implement `find_missing`: read CSV `full_address` set, iterate lpagent data, convert missing
3. Implement field mapping helper `_lpagent_to_position(raw: dict) -> MatchedPosition`
4. Implement datetime parsing helper `_parse_lpagent_datetime(s: str) -> str`
5. Implement `report()` with formatted output
6. In `valhalla/merge.py`: add Rule 3.5 in `merge_with_existing_csv` for `pnl_source="lpagent"`
7. Add `lpagent_replaced_count` counter and include it in the merge stats print block

## Dependencies
- **Requires [011-lpagent-client]**: `CrossChecker` receives `List[dict]` from `LpAgentClient`, so doc 011 must be built first (or implemented in parallel — `cross_check.py` only depends on the dict format, not the client class itself)
- **Existing**: `valhalla/models.py` (`MatchedPosition`), `valhalla/merge.py`

## Testing
- Unit test `find_missing` with a handcrafted list of 3 lpagent dicts and a CSV that has 1 of the 3 → should return 2 missing
- Verify `pnl_source="lpagent"` on all returned objects
- Verify `full_address` equals `tokenId` exactly
- Verify `position_id` equals `tokenId[:8]`
- Merge test: write a `positions.csv` with one lpagent row, then run a parse that includes a Discord log for the same position → verify the row is replaced with `pnl_source` from Discord, not `"lpagent"`, and the row count stays the same

## Alternatives Considered
- **Match by `position_id` (first 8 chars) instead of `full_address`**: Rejected — `position_id` has collision risk with 8-char prefix; `full_address` is the authoritative unique key
- **Separate CSV file for backfill rows**: Rejected — complicates all downstream analysis; single `positions.csv` is the source of truth

## Open Questions
- None
