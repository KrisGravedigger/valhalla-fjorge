# [004] Phase C — Source Wallet Analyzer

## Overview

Create a new module `valhalla/source_wallet_analyzer.py` that, for each loss position with a target transaction signature, resolves the source wallet's DLMM position and fetches its PnL via the Meteora API. The result classifies the scenario: did the source wallet hold longer and do better, exit first (causing our bot's delay to hurt us), or did both sides lose? Results are saved as three new columns at the end of positions.csv.

This phase is optional and a separate implementation iteration. It requires Phase A (target_tx_signature must be populated in MatchedPosition).

## Context

After Phase A, loss positions in `positions.csv` may have:
- `target_wallet_address`: full Solana address of the wallet being copied
- `target_tx_signature`: the first Target Tx signature (the transaction that opened/closed the position on the source wallet)

The existing infrastructure provides:
- `PositionResolver` in `valhalla/solana_rpc.py` — resolves tx signatures to DLMM position addresses via Solana RPC
- `MeteoraPnlCalculator` in `valhalla/meteora.py` — fetches PnL for a position address
- `AddressCache` in `valhalla/solana_rpc.py` — caches short_id -> full_address (reused here)
- Rate limiting convention: 0.3s sleep between API calls; exponential backoff with 5 attempts

The `PositionResolver.resolve(short_id, [sig])` method returns a full position address from a transaction. For the target tx, the short_id is not known in advance — we use a special resolution path: inspect the tx for non-system program accounts and look for the DLMM position (filter using `KNOWN_PROGRAMS` from models.py).

## Goals

- Implement `SourceWalletAnalyzer` class that:
  1. Takes a `MatchedPosition` with a target_tx_signature
  2. Resolves it to a DLMM position address
  3. Fetches PnL from Meteora API
  4. Computes hold duration and PnL% for the source wallet
  5. Classifies the scenario
- Add three new columns to positions.csv: `source_wallet_hold_min`, `source_wallet_pnl_pct`, `source_wallet_scenario`
- Handle all failure modes gracefully: log warnings, write empty values, continue
- Integrate into `valhalla_parser_v2.py` via a new `--analyze-source` flag (does not run automatically by default)

## Non-Goals

- No changes to positions parsed without `target_tx_signature` (Phase A must have run)
- No re-analysis of already-classified source positions (skip if `source_wallet_scenario` already populated)
- No backfilling of historical positions that predate Phase A

## Design

### Scenario Classification

```python
SCENARIO_HELD_LONGER = "held_longer"
SCENARIO_EXITED_FIRST = "exited_first"
SCENARIO_BOTH_LOSS = "both_loss"
SCENARIO_UNKNOWN = "unknown"
```

Classification logic:
```
source_close_time = datetime_close of source wallet position

if source_pnl_pct > bot_pnl_pct + 5:     # source did significantly better
    if source_close_time > bot_close_time:
        scenario = "held_longer"
    else:
        scenario = "exited_first"   # source was out before bot closed
elif source_pnl_pct <= 0 and bot_pnl_pct <= 0:
    scenario = "both_loss"
else:
    scenario = "unknown"
```

The 5% tolerance prevents noise from fee differences from triggering misclassification.

### SourceWalletResult Dataclass

```python
@dataclass
class SourceWalletResult:
    position_id: str
    source_position_address: Optional[str]
    source_open_time: Optional[datetime]
    source_close_time: Optional[datetime]
    source_hold_min: Optional[int]       # hold duration in minutes
    source_pnl_pct: Optional[Decimal]
    source_pnl_sol: Optional[Decimal]
    scenario: str                         # see SCENARIO_* constants
    error: Optional[str]                  # None if successful
```

### Class: SourceWalletAnalyzer

```python
class SourceWalletAnalyzer:
    def __init__(self, rpc_client: SolanaRpcClient, cache: AddressCache):
        self.rpc_client = rpc_client
        self.cache = cache
        self.meteora = MeteoraPnlCalculator()

    def analyze_position(self, position: MatchedPosition) -> SourceWalletResult:
        """
        Analyze source wallet for a single loss position.
        Returns SourceWalletResult with error field set on failure.
        """
        ...

    def analyze_batch(self, positions: List[MatchedPosition],
                      max_positions: Optional[int] = None
                      ) -> List[SourceWalletResult]:
        """
        Run analyze_position() for all eligible positions (has target_tx_signature,
        close_reason in LOSS_REASONS, source_wallet_scenario not already set).
        Prints progress. Returns list of results.
        """
        ...
```

### Resolution Path for Target Tx

`PositionResolver.resolve(short_id, signatures)` is designed to resolve the BOT's own position. For target tx, the logic is the same (inspect the transaction accounts, filter known programs) but we don't have a pre-known short_id.

Approach: call `rpc_client.get_transaction(target_tx_signature)` directly, then scan the account keys list for the DLMM position address — same logic as `PositionResolver` but without the short_id matching step.

```python
def _resolve_target_position_address(self, tx_signature: str) -> Optional[str]:
    """Resolve a target tx signature to its DLMM position address."""
    account_keys = self.rpc_client.get_transaction(tx_signature)
    if not account_keys:
        return None
    # Filter out known programs, return the first remaining non-SOL account
    # that looks like a position (44-character base58 address)
    for addr in account_keys:
        if addr not in KNOWN_PROGRAMS and len(addr) >= 32:
            return addr
    return None
```

Note: The DLMM position address is deterministic from the transaction. In practice, the Meteora DLMM program creates a dedicated account for each position. The filtering approach mirrors `PositionResolver._extract_position_from_accounts()` logic in solana_rpc.py. Implementer should review that method and reuse/refactor it.

### Hold Duration Calculation

The Meteora API response for a position includes transaction history. `MeteoraPnlResult` currently contains deposited/withdrawn amounts but not timestamps. The Meteora DLMM API (`/position/{address}`) includes an array of transactions with timestamps.

Two options:
1. **Parse timestamps from the Meteora API response** — the `/position/{address}` endpoint returns `transactions` array with `onchain_timestamp` per event. Use min (first deposit) and max (last withdrawal) as open/close times.
2. **Use Solana RPC `getTransaction` for open/close tx signatures** — fetch blockTime from the tx itself.

Option 1 is preferred because it reuses the existing Meteora call. The `MeteoraPnlCalculator.calculate_pnl()` method currently does not return timestamps. Two approaches:
- Extend `MeteoraPnlResult` to include `open_timestamp` and `close_timestamp`
- Add a separate `get_position_timestamps(address)` method to `MeteoraPnlCalculator`

The separate method approach is less invasive (does not change existing `MeteoraPnlResult`).

```python
# In MeteoraPnlCalculator (extension):
def get_position_timestamps(self, address: str) -> Optional[Tuple[datetime, datetime]]:
    """Return (open_time, close_time) from position transaction history."""
    pos_info = self._meteora_get(f"/position/{address}")
    if not pos_info:
        return None
    transactions = pos_info.get('transactions', [])
    if not transactions:
        return None
    timestamps = [t.get('onchain_timestamp') for t in transactions if t.get('onchain_timestamp')]
    if not timestamps:
        return None
    open_ts = datetime.fromtimestamp(min(timestamps))
    close_ts = datetime.fromtimestamp(max(timestamps))
    return open_ts, close_ts
```

If the Meteora API does not expose timestamps in the `/position` endpoint, fall back to the Solana RPC `getTransaction` approach.

### New columns in positions.csv

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/csv_writer.py`

Add to header (after `target_tx_signature` from Phase A, or after `meteora_pnl` if Phase A not present):
```python
'source_wallet_hold_min', 'source_wallet_pnl_pct', 'source_wallet_scenario'
```

Add to each matched-position row:
```python
str(pos.source_wallet_hold_min) if pos.source_wallet_hold_min is not None else "",
f"{pos.source_wallet_pnl_pct:.2f}" if pos.source_wallet_pnl_pct is not None else "",
pos.source_wallet_scenario if pos.source_wallet_scenario else "",
```

### New fields on MatchedPosition

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/models.py`

Add at the end with defaults:
```python
source_wallet_hold_min: Optional[int] = field(default=None)
source_wallet_pnl_pct: Optional[Decimal] = field(default=None)
source_wallet_scenario: Optional[str] = field(default=None)
```

### json_io.py — persist and restore new fields

File: `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/json_io.py`

Add to `export_to_json()` serialization dict:
```python
"source_wallet_hold_min": pos.source_wallet_hold_min,
"source_wallet_pnl_pct": str(pos.source_wallet_pnl_pct) if pos.source_wallet_pnl_pct is not None else None,
"source_wallet_scenario": pos.source_wallet_scenario,
```

Add to `import_from_json()` deserialization:
```python
source_wallet_hold_min=pos_dict.get('source_wallet_hold_min'),
source_wallet_pnl_pct=parse_optional_decimal(pos_dict.get('source_wallet_pnl_pct')),
source_wallet_scenario=pos_dict.get('source_wallet_scenario'),
```

### Integration into valhalla_parser_v2.py

New CLI flag:
```python
parser.add_argument('--analyze-source', action='store_true',
    help='Analyze source wallet PnL for stop-loss positions (requires Phase A data)')
```

Call site: after Step 5.6 (CSV merge), before loss report generation:

```python
if args.analyze_source:
    print(f"\nAnalyzing source wallet positions...")
    from valhalla.source_wallet_analyzer import SourceWalletAnalyzer
    rpc_client = SolanaRpcClient(args.rpc_url)
    analyzer = SourceWalletAnalyzer(rpc_client, cache)
    source_results = analyzer.analyze_batch(matched_positions)
    # Apply results back to matched_positions
    results_by_id = {r.position_id: r for r in source_results}
    for pos in matched_positions:
        result = results_by_id.get(pos.position_id)
        if result and not result.error:
            pos.source_wallet_hold_min = result.source_hold_min
            pos.source_wallet_pnl_pct = result.source_pnl_pct
            pos.source_wallet_scenario = result.scenario
    cache.save()
    # Regenerate CSVs with new data
    csv_writer = CsvWriter()
    csv_writer.generate_positions_csv(matched_positions, unmatched_opens, str(positions_csv))
    print(f"  Updated {positions_csv} with source wallet data")
```

### Rate Limiting

Follow the existing convention from `meteora.py`:
```python
import time
time.sleep(0.3)   # between Meteora API calls
```

For Solana RPC calls, `SolanaRpcClient` already has built-in rate-limit backoff (increases `_delay` on 429 responses).

### Error Handling

Every step that can fail (RPC call, Meteora call, timestamp parsing) is wrapped in try/except. On failure:
- Log: `print(f"  Warning: source wallet analysis failed for {pos.position_id}: {e}")`
- Return `SourceWalletResult` with `error=str(e)` and all data fields as `None`
- `scenario = SCENARIO_UNKNOWN`
- Continue to next position

If the Meteora API returns no data (position too old, API change): gracefully skip. Log: `"  Meteora returned no data for {address}, skipping"`.

## Implementation Plan

1. **`valhalla/models.py`** — add three new Optional fields to `MatchedPosition`

2. **`valhalla/csv_writer.py`** — add three new columns to `generate_positions_csv()` header and matched-position rows

3. **`valhalla/json_io.py`** — add new fields to export and import

4. **`valhalla/meteora.py`** — add `get_position_timestamps()` method to `MeteoraPnlCalculator`

5. **Create `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/source_wallet_analyzer.py`**
   - Add `SCENARIO_*` constants
   - Add `SourceWalletResult` dataclass
   - Implement `SourceWalletAnalyzer` class

6. **`valhalla_parser_v2.py`** — add `--analyze-source` argument and call site in `main()`

## Dependencies

- **Requires [001] Phase A** — `target_tx_signature` field must be populated in `MatchedPosition`
- External: Solana RPC (existing infrastructure), Meteora API (existing infrastructure)
- No new Python packages required

## Testing

1. Pick one known stop-loss position from `positions.csv` that has a `target_tx_signature` value
2. `python valhalla_parser_v2.py --analyze-source --skip-meteora` (skip main Meteora run to save time)
3. Verify `positions.csv` now has `source_wallet_hold_min`, `source_wallet_pnl_pct`, `source_wallet_scenario` populated for that position
4. Manually check `source_wallet_pnl_pct` against Solscan/Metlex for the target tx — approximate match expected
5. Verify positions without `target_tx_signature` have empty values in the new columns (regression)
6. Run twice — second run should skip already-classified positions (idempotent)
7. Test error case: corrupt/invalid `target_tx_signature` → position should show `"unknown"` scenario, no crash

## Alternatives Considered

- **Fetching source wallet timestamps via Solana RPC instead of Meteora API**: Would require additional RPC calls for each position open/close tx. Meteora API is more reliable here since it already has the full transaction history per position.
- **Running source analysis automatically (not behind a flag)**: Rejected — this phase makes ~2 Solana RPC calls and ~2 Meteora API calls per analyzed position. For large datasets this could take minutes. The `--analyze-source` flag keeps the normal run fast.
- **Storing source wallet results in a separate file**: Rejected — adding columns to positions.csv keeps all position data in one place, consistent with the existing design.

## Open Questions

- Does the Meteora `/position/{address}` endpoint include `onchain_timestamp` per transaction? This needs to be verified against the actual API response. If not, the implementation must fall back to fetching transaction blockTime from Solana RPC.
- The `_resolve_target_position_address()` method assumes the DLMM position account is the first non-program account in the tx. This needs validation against real target tx data — it may require the same heuristics used in `PositionResolver` (e.g., filtering by address length or known prefixes).
