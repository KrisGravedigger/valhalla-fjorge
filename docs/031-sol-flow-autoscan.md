# [031] SOL Flow Auto-Scanner (Part F)

## Intent

Automatically scan the main wallet's on-chain transaction history to detect pure SOL
transfers from/to external wallets and maintain `output/capital_flows.csv` without
manual entry. Generate a daily SOL-flow chart for visual tracking.

**Scope limitation (explicit):**
- Only pure SOL transfers (System Program only — no Jupiter, no Meteora, no swaps)
- Only external wallet direction (wallet ↔ outside the portfolio)
- No SPL token flows, no fee-claim events, no swap proceeds

## Overview

Three deliverables:

1. **`valhalla/sol_flow_scanner.py`** — `SolFlowScanner` class. Paginates
   `getSignaturesForAddress`, filters pure SOL transfers, returns new flow rows.

2. **`tools/autoscan_sol_flows.py`** — CLI tool: runs scanner, appends new rows to
   `output/capital_flows.csv`, updates `output/sol_flow_scan.json` watermark.
   Runnable standalone; also invoked from `run_pipeline.py` Step 5.

3. **`tools/chart_sol_flows.py`** — generates `output/sol_flows.png`: dual-axis chart
   (daily bars + cumulative line) from full `capital_flows.csv` history.

`run_pipeline.py` gets a new Step 5 calling `autoscan_sol_flows.py`. New flag
`--skip-flow-scan` bypasses the step.

## Context

`output/capital_flows.csv` has 5 manual rows (`HIST-*` pseudo-signatures, deposits only,
range Sep 2025 – Mar 2026). These are historical estimates and stay unchanged; the scanner
only appends rows with real `tx_signature` values.

`valhalla/solana_rpc.py` already has a `SolanaRpcClient` with `getTransaction(jsonParsed)`
and adaptive rate-limit handling. `SolFlowScanner` reuses this client.

`KNOWN_PROGRAMS` in `valhalla/models.py` lists System Program and Compute Budget — the
exact set allowed in a pure SOL transfer transaction.

## Technical Design

### SOL Transfer Detection

A transaction is a **pure SOL transfer** if and only if all instruction program IDs belong
to the allow-set:

```
PURE_SOL_ALLOW = {
    "11111111111111111111111111111111",            # System Program
    "ComputeBudget111111111111111111111111111111", # Compute Budget
}
```

Detection algorithm (per transaction):

0. If `meta.err is not None` → skip (failed transaction paid fee but moved no net value).
1. Extract all `programId` values from `transaction.message.instructions`.
2. If `program_ids ⊄ PURE_SOL_ALLOW` → skip (swap/LP operation).
3. Find our wallet's index in `transaction.message.accountKeys`.
4. `delta_lamports = meta.postBalances[our_idx] - meta.preBalances[our_idx]`
5. If `delta_lamports == 0` → skip (fee-only, no net change).
6. `flow_type = "deposit" if delta_lamports > 0 else "withdrawal"`
7. `sol_amount = abs(delta_lamports) / 1_000_000_000` (Decimal, 6 dp)

Note: withdrawal delta is negative and includes the transaction fee paid by our wallet.
We do **not** subtract the fee — the full signed delta is the recorded amount.

**Single-wallet assumption**: the scanner treats every external address as truly external.
Own multi-wallet transfers (e.g., cold wallet → hot wallet to the same owner) would be
classified as deposits/withdrawals. Out of scope for this doc — handled manually if needed.

### SolanaRpcClient extension (`valhalla/solana_rpc.py`)

Add one method to the existing class:

```python
def get_signatures_for_address(
    self,
    address: str,
    limit: int = 1000,
    before: str | None = None,
    until: str | None = None,
) -> list[dict]:
    """
    Wraps getSignaturesForAddress RPC method.
    Returns list of signature-info objects (newest first):
      {"signature": str, "blockTime": int | None, "err": any}
    Returns [] on error.
    """
```

Reuses the same urllib + adaptive-delay + retry pattern as `get_transaction()`.

### SolFlowScanner Class (`valhalla/sol_flow_scanner.py`)

```python
@dataclass
class FlowRow:
    timestamp_utc: str   # ISO 8601, e.g. "2026-04-01T14:23:11Z"
    wallet: str          # always "portfolio"
    type: str            # "deposit" or "withdrawal"
    sol_amount: Decimal  # absolute value, 6 dp
    tx_signature: str    # base-58 signature
    notes: str           # always "autoscan"

class SolFlowScanner:
    def __init__(self, rpc_client: SolanaRpcClient, our_wallet: str): ...

    def get_existing_signatures(self, flows_path: Path) -> set[str]:
        """Read tx_signature column from capital_flows.csv."""

    def scan_new(
        self,
        flows_path: Path,
        watermark_path: Path,
        start_date: str | None = None,   # YYYY-MM-DD; used only on first run
    ) -> list[FlowRow]:
        """
        Paginate getSignaturesForAddress using watermark.
        Returns list of new FlowRow objects not already in flows_path.
        """
```

Pagination logic:

- Load watermark from `watermark_path` if it exists: `{"last_signature": "...", "scan_utc": "..."}`
- If watermark exists: call `getSignaturesForAddress(wallet, limit=1000, until=last_signature)`
  to fetch only signatures newer than the watermark.
- If no watermark: call without `until`, paginate backward via `before=last_sig_seen`
  until `blockTime` of fetched signature is older than `start_date`.
- Skip signatures already in `existing_signatures` (dedup guard).
- For each remaining signature: call `getTransaction(jsonParsed)` → classify → collect FlowRow.
- After processing: save watermark = most recently seen signature (first result of newest page).

### autoscan_sol_flows.py (CLI)

```
python tools/autoscan_sol_flows.py [--dry-run] [--start-date YYYY-MM-DD]
```

- Reads `WALLET_ADDRESS` and `RPC_URL` from `.env` (python-dotenv).
- Default `start_date`: date of most recent row in `capital_flows.csv` (2026-03-03).
- `--dry-run`: print found rows, do not write CSV or watermark.
- On success: appends new rows, prints summary (`N new flows found, appended`).
- Watermark written only after successful CSV append (atomic: update both or neither).
- If 0 new transactions found, watermark is **not** updated (no new "last_signature" to save).

### chart_sol_flows.py

```
python tools/chart_sol_flows.py [--output PATH]
```

Chart: `output/sol_flows.png` — dual-axis matplotlib figure.

**Left axis (bars):**
- X: dates from capital_flows.csv (UTC day)
- Y: net SOL per day (deposits positive, withdrawals negative)
- Green bars for positive days, red bars for negative days

**Right axis (line):**
- Cumulative net SOL from all rows in capital_flows.csv (HIST- + on-chain)
- Blue line, labeled "Cumulative net SOL"

Title: "SOL Capital Flows". Legend: "Daily net" + "Cumulative".

### run_pipeline.py Integration

New Step 5 inserted after Step 4 (record_internal_nav):

```python
# ── Step 5: SOL flow autoscan ─────────────────────────────────────────────
skip_flow = "--skip-flow-scan" in args
if skip_flow:
    print("\n[pipeline] --skip-flow-scan: skipping.")
else:
    _run(
        "SOL flow autoscan",
        [sys.executable, str(ROOT / "tools" / "autoscan_sol_flows.py")],
    )
```

Non-zero exit from autoscan is logged but does **not** abort the pipeline
(flows are informational — NAV snapshot already written).

New Step 6 — chart regeneration:

```python
# ── Step 6: SOL flows chart ───────────────────────────────────────────────
if not skip_flow:
    _run(
        "SOL flows chart",
        [sys.executable, str(ROOT / "tools" / "chart_sol_flows.py")],
    )
```

Step 6 runs only when Step 5 ran (same `skip_flow` guard). Non-zero exit does not abort.

## File Inventory

| Path | Action | Notes |
|---|---|---|
| `valhalla/sol_flow_scanner.py` | **create** | SolFlowScanner, FlowRow |
| `valhalla/solana_rpc.py` | **edit** | add `get_signatures_for_address()` method to SolanaRpcClient |
| `tools/autoscan_sol_flows.py` | **create** | CLI, CSV append, watermark |
| `tools/chart_sol_flows.py` | **create** | matplotlib chart |
| `run_pipeline.py` | **edit** | add Steps 5 + 6 + `--skip-flow-scan` |
| `output/sol_flow_scan.json` | runtime | watermark (not tracked in git) |
| `output/sol_flows.png` | runtime | chart output (not tracked in git) |
| `tests/test_sol_flow_scanner.py` | **create** | unit tests |

**Do not touch:** `capital_flow.py`, `capital_flows.csv`, `models.py`.

## Acceptance Criteria

1. `autoscan_sol_flows.py` appends rows only for transactions where
   all instruction programs ⊆ `{SystemProgram, ComputeBudget}`.
2. Running the tool twice with the same RPC state produces no duplicate rows.
3. Watermark advances after each run; subsequent run fetches zero new rows
   if no new transactions exist.
4. `chart_sol_flows.py` generates `output/sol_flows.png` with daily bars and
   cumulative line using all rows in capital_flows.csv.
5. `run_pipeline.py` runs autoscan in Step 5 by default; `--skip-flow-scan` bypasses it.
6. Failure in Step 5 does not abort the pipeline (pipeline exits 0 if Steps 1–4 succeed).

## Verification Contract

```bash
# 1. Unit: pure SOL transfer detected correctly
python -m pytest tests/test_sol_flow_scanner.py::test_classify_pure_sol_transfer -v

# 2. Unit: swap transaction rejected
python -m pytest tests/test_sol_flow_scanner.py::test_classify_swap_skipped -v

# 3. Unit: deposit vs withdrawal direction
python -m pytest tests/test_sol_flow_scanner.py::test_deposit_direction -v
python -m pytest tests/test_sol_flow_scanner.py::test_withdrawal_direction -v

# 4. Unit: dedup — existing signature not re-appended
python -m pytest tests/test_sol_flow_scanner.py::test_dedup_skips_known_signature -v

# 5. Integration: dry-run produces output, CSV unchanged
python tools/autoscan_sol_flows.py --dry-run
# Expected: prints summary, capital_flows.csv mtime unchanged

# 6. Chart generation
python tools/chart_sol_flows.py
# Expected: output/sol_flows.png exists, non-zero size

# 7. Pipeline integration
python run_pipeline.py --skip-pull --nav-dry-run --skip-flow-scan
# Expected: "Step 5" line appears in output, exits 0

# 8. Full test suite green
python -m pytest tests/test_sol_flow_scanner.py -v
```
