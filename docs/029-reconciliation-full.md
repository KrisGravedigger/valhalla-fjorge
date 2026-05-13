---
critical: true
---

# [029] Full Reconciliation against JSONL Cache (D-full)

## Intent

Extend `valhalla/reconcile.py` (which already implements D-lite legacy mode) to support
a non-legacy JSONL mode: read from `output/lpagent_cache/positions_{wallet_prefix}.jsonl`
(produced by sub-project C), filter both sides by date window, and emit a categorised
reconciliation report with per-wallet and per-day aggregates. The CLI, module layout,
matching key, and existing legacy path are all unchanged.

## Overview

D-lite (doc 025) is a read-only sanity check against the known-broken legacy cache.
D-full is the authoritative reconciliation: it reads from the clean JSONL produced by
the redesigned lpagent client (C/026+027), and adds richer sub-categorisation plus
aggregate breakdowns. Both modes live in the same `valhalla/reconcile.py` module.

When `--legacy-cache` is not supplied, the command uses the JSONL path. A missing or
empty JSONL exits with a clear error message — it never silently produces an empty
report. A watermark check warns if `min_safe_open_date` from the watermark is after
`--from`, indicating partial JSONL coverage.

**Matching key:** `full_address` (positions.csv) == `tokenId` (lpagent JSONL). This is
the existing D-lite convention — preserved unchanged.

## Goals

1. `python -m valhalla.reconcile --from YYYY-MM-DD --to YYYY-MM-DD` (no `--legacy-cache`)
   reads from JSONL, filters by `updatedAt` (lpagent) and `datetime_close` (ours), and
   produces a Markdown report + three CSVs analogous to D-lite.
2. `--wallet ADDR` selects which wallet prefix to use for the JSONL filename
   (`positions_{addr[:5]}.jsonl`). Defaults to `LPAGENT_WALLET` env var, then the
   hardcoded default (`J4tkG…`).
3. Sub-categorise ours-only:
   - `older_than_retention` — `datetime_close < --from`
   - `lpagent_dropped` — position has `pnl_sol == 0` or `pnl_sol` is empty/missing
   - `wallet_not_tracked` — `source_wallet` (positions.csv field) is not `--wallet`
   - `not_in_lpagent` — residual: present in our CSV, absent from JSONL, none of the above
4. Sub-categorise lpagent-only:
   - `in_archive` — `tokenId` is present in any `output/lpagent_cache/archive/*.json` file
   - `outside_wallet_set` — lpagent record's `owner` field ≠ `--wallet`
   - `truly_missing` — residual: present in JSONL, absent from our CSV, none of the above
5. Aggregates: per-wallet sums (both sides), per-day sums (both sides), total drift.
6. Report header explicitly states which date axis each side was filtered on.

## Non-Goals

- Modifying the JSONL file or watermark state. This is read-only.
- Changing D-lite legacy behaviour (`--legacy-cache` path).
- Writing to `notes/lpagent_retention.md` — that is a manual step after running D-full.
- Auto-fixing discrepancies or enriching positions.csv.

## Acceptance Criteria

### AC-1: JSONL mode activates when `--legacy-cache` is absent

```
python -m valhalla.reconcile --from 2026-04-01 --to 2026-05-13
```
Exits 0, produces console output with "JSONL cache" in the header (not "legacy cache").
No "Error: --legacy-cache is required" message.

### AC-2: Missing JSONL produces a clear error

When the JSONL file for the resolved wallet does not exist or is empty:

```
Error: JSONL cache not found: output/lpagent_cache/positions_J4tkG.jsonl
Run the lpagent pipeline first to populate it.
```
Exit code 1.

### AC-3: Watermark partial-coverage warning

When `output/watermark.json` exists and `min_safe_open_date > --from`:

```
Warning: JSONL coverage starts 2026-03-01; requested window starts 2026-02-11.
Positions opened before 2026-03-01 may be missing from lpagent data.
```
Report still runs; warning appears in console and in Markdown.

### AC-4: Date filtering is correct on both sides

- lpagent JSONL: records where `updatedAt[:10]` is in `[--from, --to]` are included.
- positions.csv: rows where `datetime_close[:10]` is in `[--from, --to]` are included.
- Report header reads:
  ```
  lpagent filter: updatedAt in [YYYY-MM-DD, YYYY-MM-DD]
  positions filter: datetime_close in [YYYY-MM-DD, YYYY-MM-DD]
  ```

### AC-5: Matching produces correct counts

With a synthetic fixture of 10 matched / 5 ours-only / 5 lpagent-only tokenIds:
- `matched` count == 10
- `ours_only` count == 5
- `lpagent_only` count == 5

(Test: `tests/test_reconcile_full.py::test_reconcile_counts`)

### AC-6: Sub-categorisation is correct

Given synthetic rows crafted to exercise each bucket:

| Category | Trigger condition |
|---|---|
| `older_than_retention` | `datetime_close < --from` |
| `lpagent_dropped` | `pnl_sol` is empty/zero |
| `wallet_not_tracked` | `source_wallet != --wallet` |
| `not_in_lpagent` | residual (none of above) |
| `in_archive` | tokenId present in archive/*.json |
| `outside_wallet_set` | lpagent record `owner != --wallet` |
| `truly_missing` | residual |

Each bucket has at least one test assertion.

(Test: `tests/test_reconcile_full.py::test_ours_only_subcategories`,
`tests/test_reconcile_full.py::test_lpagent_only_subcategories`)

### AC-7: Per-wallet and per-day aggregates are correct

With a synthetic fixture spanning 2 wallets and 2 days, the aggregates section contains:
- `Per-wallet` table with one row per distinct wallet in positions.csv (ours side)
- `Per-day` table with one row per distinct close date appearing in matched positions

Values: sum of `pnl_ours` and `pnl_lpagent` per wallet/day.

(Test: `tests/test_reconcile_full.py::test_aggregates`)

### AC-8: `--wallet` CLI flag and env fallback work

`--wallet J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF` uses prefix `J4tkG`.
Without `--wallet`, falls back to `LPAGENT_WALLET` env var; then to the hardcoded default.

### AC-9: Output files produced

Running with valid data produces:
- `output/reconciliation_{from}_{to}.md` (Markdown report)
- `output/reconciliation_{from}_{to}_matched.csv`
- `output/reconciliation_{from}_{to}_lpagent_only.csv`
- `output/reconciliation_{from}_{to}_ours_only.csv`

CSVs do NOT carry the `# WARNING: legacy cache` comment line — that is legacy-mode only.

### AC-10: tokenId absent in JSONL record does not crash

If a JSONL line has no `tokenId` or `tokenId` is null/empty, the record is skipped with
`WARNING: skipping lpagent record with empty tokenId`. The rest of the report continues.

### AC-11: Duplicate `full_address` in positions.csv preserved

Existing D-lite behaviour: keep the row with `pnl_source != 'lpagent'`. Log duplicates.
This behaviour is unchanged; the test in `tests/test_reconcile_lite.py` continues to pass.

### AC-12: Lint passes

```
ruff check valhalla/reconcile.py tests/test_reconcile_full.py --select E,F,I
```
Exit 0.

## Touchable Files

- `valhalla/reconcile.py` — extend with JSONL mode (D-full)
- `tests/test_reconcile_full.py` — new test module
- `docs/029-reconciliation-full.md` — this file

Do NOT touch: other `valhalla/` modules, `tools/`, `output/`, `PLAN-portfolio-truth.md`,
`tests/test_reconcile.py` (D-lite tests must keep passing).

## Verification Contract

```bash
# Run new D-full tests
python -m pytest tests/test_reconcile_full.py -v

# Verify D-lite tests still pass
python -m pytest tests/test_reconcile_lite.py -v

# Lint
ruff check valhalla/reconcile.py tests/test_reconcile_full.py --select E,F,I

# Smoke: JSONL mode (requires populated output/lpagent_cache/positions_J4tkG.jsonl)
python -m valhalla.reconcile --from 2026-04-01 --to 2026-05-13

# Smoke: --legacy-cache still works
python -m valhalla.reconcile --from 2026-04-01 --to 2026-05-13 --legacy-cache
```

## Design

### Data structures (new/changed)

```python
# Reuse for both modes; hint field is sub-category label
@dataclass(frozen=True)
class _LpAgentOnlyRow:
    token_id: str
    token: str
    opened: str
    pnl_native: str
    hint: str          # "in_archive" | "outside_wallet_set" | "truly_missing"
                       # | "possible duplicate (cache overlap)" (legacy)

@dataclass(frozen=True)
class _OursOnlyRow:
    full_address: str
    token: str
    datetime_close: str
    pnl_sol: str
    reason: str        # "older_than_retention" | "lpagent_dropped"
                       # | "wallet_not_tracked" | "not_in_lpagent"
                       # | "older than query window" | "not in lpagent cache for this range" (legacy)

@dataclass(frozen=True)
class _WalletAggregate:
    wallet: str
    matched_count: int
    pnl_ours_sol: Decimal
    pnl_lpagent_sol: Decimal
    drift_sol: Decimal

@dataclass(frozen=True)
class _DayAggregate:
    day: str           # YYYY-MM-DD
    matched_count: int
    pnl_ours_sol: Decimal
    pnl_lpagent_sol: Decimal
    drift_sol: Decimal

@dataclass(frozen=True)
class _ReconcileResult:
    matched: list[_MatchedRow]
    lpagent_only: list[_LpAgentOnlyRow]
    ours_only: list[_OursOnlyRow]
    wallet_aggregates: list[_WalletAggregate] = field(default_factory=list)  # empty in legacy mode
    day_aggregates: list[_DayAggregate] = field(default_factory=list)        # empty in legacy mode
```

### `_load_jsonl_cache(cache_dir, wallet, from_date, to_date)`

1. Resolve JSONL path: `cache_dir / f"positions_{wallet[:5]}.jsonl"`.
2. Raise `FileNotFoundError` with the expected path if file absent or empty.
3. Read line-by-line; skip blank lines. For each line:
   - If `json.JSONDecodeError`: log warning, skip.
   - If `tokenId` absent/empty: log warning, skip.
   - Apply date filter: include if `updatedAt[:10]` is in `[from_date, to_date]`.
4. Dedup by tokenId (last-write-wins by `updatedAt`) — same as client behaviour.
5. Return `dict[str, dict]` keyed by tokenId.

### `_load_watermark(output_dir)` → `dict | None`

Read `output_dir / "watermark.json"`. Return `None` on any error (missing, malformed).
Caller checks `result.get("min_safe_open_date")` for the coverage warning (AC-3).

### Sub-categorisation helpers

```python
def _ours_only_reason_jsonl(
    row: dict[str, str],
    from_date: str,
    wallet: str,
    archive_dir: Path,
) -> str:
    closed = (row.get("datetime_close") or "")[:10]
    pnl_raw = (row.get("pnl_sol") or "").strip()
    source_wallet = (row.get("source_wallet") or "").strip()

    if closed and closed < from_date:
        return "older_than_retention"
    # Normalise to Decimal to catch all zero representations ("0", "0.0", "0.00000000", etc.)
    try:
        pnl_is_zero = not pnl_raw or Decimal(pnl_raw) == 0
    except InvalidOperation:
        pnl_is_zero = False  # non-numeric pnl → not zero, fall through to other checks
    if pnl_is_zero:
        return "lpagent_dropped"
    if source_wallet and source_wallet != wallet:
        return "wallet_not_tracked"
    return "not_in_lpagent"


def _lpagent_only_hint_jsonl(
    token_id: str,
    record: dict,
    wallet: str,
    archive_dir: Path,
) -> str:
    owner = (record.get("owner") or "").strip()
    if owner and owner != wallet:
        return "outside_wallet_set"
    if _token_in_archive(token_id, archive_dir):
        return "in_archive"
    return "truly_missing"
```

`_token_in_archive(token_id, archive_dir)`: walk `archive_dir/*.json`, load each as a
JSON array, check if any element has `tokenId == token_id`. Cache results for the run
(scan archive once, build a set of all known tokenIds). If `archive_dir` does not exist,
always return False.

### Aggregate computation

`_reconcile_jsonl()` returns matched rows as `_MatchedRow` objects plus a parallel
`list[tuple[_MatchedRow, dict[str, str]]]` — each matched row paired with the original
positions.csv dict so that `source_wallet` and `datetime_close` are available for grouping.
The caller unpacks this to build aggregates; the `_MatchedRow` dataclass itself is not
changed (keeps D-lite compatibility).

After reconcile, compute wallet and day aggregates from matched rows only:

- **Per-wallet**: group by `source_wallet` from the original positions.csv row.
  If `source_wallet` absent/empty, group as `unknown`.
- **Per-day**: group by `datetime_close[:10]` from the original positions.csv row.
  If `datetime_close` absent, group as `unknown`.

Sums: `pnl_ours_sol` = sum of matched row `pnl_ours`; `pnl_lpagent_sol` = sum of
`pnl_lpagent`; both Decimal (rows with `None` values excluded from sums, still counted).

### `main()` changes

```python
parser.add_argument("--wallet", dest="wallet", default=None,
    help="Wallet address (default: LPAGENT_WALLET env, then J4tkG...)")
```

Mode dispatch in `main()`:

```python
if args.legacy_cache:
    # existing D-lite code path — unchanged
    ...
else:
    wallet = args.wallet or os.environ.get("LPAGENT_WALLET", DEFAULT_WALLET)
    watermark = _load_watermark(output_dir)
    if watermark:
        min_safe = watermark.get("min_safe_open_date")
        if min_safe and min_safe > args.from_date:
            print(f"Warning: JSONL coverage starts {min_safe}; requested window starts {args.from_date}.")
            print(f"Positions opened before {min_safe} may be missing from lpagent data.")
            print()
    cache_dir = output_dir / "lpagent_cache"
    archive_dir = cache_dir / "archive"
    try:
        lpagent_positions = _load_jsonl_cache(cache_dir, wallet, args.from_date, args.to_date)
        our_positions = _load_positions_csv(positions_path)
    except FileNotFoundError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)
    result = _reconcile_jsonl(lpagent_positions, our_positions, args.from_date, wallet, archive_dir)
    _render_console_jsonl(result, args.from_date, args.to_date)
    ...
```

### `_reconcile_jsonl()` vs `_reconcile()`

A separate function (not overloading `_reconcile`) to avoid coupling D-lite and D-full
codepaths. Shares the same `_compute_pnl_diff()` helper, `_MatchedRow`, and
`_load_positions_csv()`. Does not share the legacy `_ours_only_reason()`.

### Report header (JSONL mode)

```
=== Reconciliation Report: YYYY-MM-DD -> YYYY-MM-DD (JSONL cache) ===
lpagent filter:   updatedAt in [YYYY-MM-DD, YYYY-MM-DD]
positions filter: datetime_close in [YYYY-MM-DD, YYYY-MM-DD]
```

## Adversarial Scenarios

- **Empty tokenId in JSONL**: skip + warn. No crash. (AC-10)
- **Duplicate full_address in positions.csv**: keep non-lpagent row. (AC-11)
- **JSONL absent or empty**: clear error with expected path. (AC-2)
- **Watermark missing or malformed**: silently skip the coverage warning; don't crash. (AC-3)
- **Archive scan fails** (e.g., archive file is corrupt JSON): log warning, skip that file;
  do not let it crash the archive lookup or abort the report.
- **`pnl_sol` is `"0.00000000"` (8 decimal places)**: treated as zero via `Decimal(pnl_raw) == 0`,
  which catches all zero representations. Non-numeric values fall through to `not_in_lpagent`.
- **All positions are ours-only** (JSONL covers a different date range entirely):
  produces a valid report with 0 matched, 0 lpagent-only, N ours-only. Not an error.
- **`datetime_close` missing from positions.csv row**: treated as empty string for date
  comparison, which means the row passes the `< from_date` check only if the empty string
  compares less (which it does: `"" < "2026-..."` is True in Python). Sub-category will
  be `older_than_retention` unless `pnl_sol` is zero. This is acceptable — a row with no
  close date is indeterminate; treating it as outside the window is safe and conservative.

## Open Questions

None — spec is fully derived from PLAN-portfolio-truth.md § D-full and the existing
D-lite implementation as reference. Archive scan logic is new; implementation must
verify that `archive/` files are JSON arrays (same format as legacy daily files).
