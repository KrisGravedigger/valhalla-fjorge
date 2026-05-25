---
critical: true
---

# [030] Internal NAV Computation (Sub-project E)

## Intent

Implement portfolio NAV computation directly from on-chain Solana data — independent of
lpagent, Fabriq, or any other third-party UI. The result is a `source=internal` snapshot
row in `output/portfolio_snapshots.csv`, produced by a cron-friendly tool.

E-spike (doc 028) confirmed feasibility at **0.014% diff** on the correct comparison
baseline (LP positions + free SOL). This doc translates the spike into production code.

## Overview

Two deliverables:

1. **`valhalla/internal_nav.py`** — core computation module. Public surface: one function
   `compute_nav(rpc_url, wallet) -> NavResult`. All Meteora DLMM decoding, BinArray math,
   Jupiter pricing, and error handling live here.

2. **`tools/record_internal_nav.py`** — cron-friendly CLI tool. Calls `compute_nav`,
   builds a snapshot row, appends it to `output/portfolio_snapshots.csv`. Same role as
   `tools/record_portfolio_snapshot.py` but fully automated: no manual NAV input needed.

No changes to `valhalla/cli.py`, `record_portfolio_snapshot.py`, or the CSV schema.

## Context

### Verified struct layouts (source of truth: `tools/spike_internal_nav.py`)

**CRITICAL:** These constants are verified empirically on live mainnet data. Do NOT
derive them from Meteora IDL docs or TypeScript SDK — the IDL led Codex astray during
the spike (wrong `N`, wrong `BA_HEADER`, wrong `POSV2_FIXED`). Always cite the spike
as the proof source.

```
PositionV2 fixed layout (8120 bytes total):
  offset 8:    lb_pair        pubkey (32B)
  offset 40:   owner          pubkey (32B)
  offset 72:   liq_shares[70] u128×70 (1120B)   # N=70 always, regardless of account size
  offset 1192: reward_infos[70] UserRewardInfo×70 (3360B)
  offset 4552: fee_infos[70]   FeeInfo×70 (3360B)
  offset 7912: lower_bin_id   i32 (4B)
  offset 7916: upper_bin_id   i32 (4B)
  offset 7920: metadata (timestamps, claimed totals, operator, fee_owner, _reserved)
  offset 8120: end of fixed layout

  N = 70  (MAX_BIN_PER_POSITION — constant, not derived from account size)
  POSV2_FIXED = 8120  (NOT 7920 — metadata block after upper_bin_id is 200B)

PositionBinData extension (accounts > 8120B):
  ext_count = (raw_len - 8120) // 112  (each extension bin is 112B)
  Per bin (112B):
    offset 0:   liquidity_share     u128 (16B)
    offset 16:  reward_per_token_completes  u128×2 (32B)
    offset 48:  reward_pendings[0]  u64 (8B)
    offset 56:  reward_pendings[1]  u64 (8B)
    offset 64:  fee_x_per_token_complete  u128 (16B)
    offset 80:  fee_y_per_token_complete  u128 (16B)
    offset 96:  fee_x_pending       u64 (8B)
    offset 104: fee_y_pending       u64 (8B)

UserRewardInfo (48B, at base + REWARD_INFO_OFF + j*48):
  offset 0:  reward_per_token_completes  u128×2 (32B)
  offset 32: reward_pendings[0]   u64 (8B)
  offset 40: reward_pendings[1]   u64 (8B)
  REWARD_INFO_OFF = 1192

FeeInfo (48B, at base + FEE_DATA_OFF + j*48):
  offset 0:  fee_x_per_token_complete  u128 (16B)
  offset 16: fee_y_per_token_complete  u128 (16B)
  offset 32: fee_x_pending       u64 (8B)
  offset 40: fee_y_pending       u64 (8B)
  FEE_DATA_OFF = 4552

BinArray layout:
  BA_HEADER = 56  (8 disc + 8 index + 1 version + 7 padding + 32 lb_pair)
  BIN_SIZE   = 144  (each Bin struct)
  Per bin:
    offset 0:  amount_x         u64 (8B)
    offset 8:  amount_y         u64 (8B)
    offset 32: liquidity_supply u128 (16B)
  BinArray PDA seeds: [b"bin_array", bytes(lb_pair_pk), struct.pack("<q", array_idx)]
```

### Comparison baseline

Correct comparison: `internal_nav_sol` (LP + free SOL + idle SPL) vs lpagent
`LP positions + free SOL`. The lpagent portfolio widget (~5 SOL higher) includes
rent reserves and unmanaged assets — these are intentionally excluded from this NAV
definition. See `notes/internal_nav_feasibility.md` for full justification.

### Existing infrastructure

`valhalla/solana_rpc.py` (`SolanaRpcClient`) covers address resolution and
`get_sol_balance` only — it does not support `getProgramAccounts` with filters or
`getMultipleAccounts` batching. The new module brings its own thin RPC layer
(3 functions, same as the spike). Do not extend `SolanaRpcClient`.

## Goals

- Enumerate all PositionV2 accounts for the configured wallet via `getProgramAccounts`
- Decode fixed bins (N=70) and extended PositionBinData bins from each position account
- Compute per-position token reserves: `frac = liq_share / liq_supply`,
  `token_x += frac × bin.amount_x` for each active bin
- Collect fee pendings (FeeInfo) and reward pendings (UserRewardInfo + extended) per position
- Fetch BinArrays via PDA derivation; decode bin data
- Get pool mint addresses from Meteora REST API with on-chain fallback (scan LbPair bytes
  for SOL_MINT pattern)
- Convert all non-SOL token amounts to SOL via Jupiter `/swap/v1/quote`
  with 0.15s throttle and exponential backoff on 429 (up to 4 attempts)
- Collect free SOL balance (`getBalance`) and idle SPL token balances
  (`getTokenAccountsByOwner`)
- Return a `NavResult` dataclass capturing all components plus degraded flag
- Write snapshot row to `output/portfolio_snapshots.csv` with `source=internal`;
  if degraded, record failed mints in the `notes` field
- Never write a zero NAV snapshot — exit with error if positions query returns 0 positions
  AND free SOL is 0 (indicates RPC failure, not an empty wallet)

## Non-Goals

- Integration with the main `valhalla/cli.py` pipeline (separate cron tool only)
- Staked SOL (mSOL, jitoSOL) — excluded per spike findings; tracked separately
- Rent reserve SOL in position accounts — intentionally excluded (see spike findings)
- `--amend` or snapshot deduplication — always append, consistent with sub-project B
- Fabriq or other source automation (separate concern)
- On-chain fallback for reward mints (Meteora API is sufficient; rewards were zero in spike)
- Scoreboard display of degraded snapshots — degraded state is only visible in the CSV
  `notes` field; the scoreboard does not surface it specially and is not modified by this doc

## Acceptance Criteria

### AC-1: Positions enumerated
Given a wallet with at least one active Meteora DLMM PositionV2 account,
`get_position_addresses(rpc_url, wallet)` returns a non-empty list. The result set
matches what lpagent reports as open positions (within ±1 for recently opened/closed).

### AC-2: Position decode — fixed and extended bins
Given a PositionV2 account with `raw_len > 8120` (extended bins):
`decode_position(data)` returns `ext_count = (raw_len - 8120) // 112` with correct
`ext_liq_shares`, `ext_fee_x_pending`, `ext_reward0_raw`, etc.
Given a standard 8120B account: `ext_count == 0`.

### AC-3: NAV accuracy on live data
If `--lpagent-nav N` is passed (optional float argument), `record_internal_nav.py` prints
a diff line and asserts `abs(total_nav_sol - N) / N < 0.02`, exiting 1 if the check fails.
If `--lpagent-nav` is not passed, AC-3 is verified manually at implementation time and
documented in the PR (compare `--dry-run` output against lpagent LP+free read simultaneously).

### AC-4: Degraded snapshot — partial write
Given a Jupiter call that returns a permanent failure for one token mint (no route):
- `NavResult.degraded == True` and the mint address appears in `NavResult.degraded_mints`
- The snapshot is still written with the available NAV value
- The `notes` column contains `degraded: <MINT1>` (base58 addresses, comma-separated)
- The snapshot is never silently zero — all non-degraded tokens are included

### AC-5: Zero NAV guard
Given `getProgramAccounts` returns 0 positions AND `getBalance` returns 0 lamports
(clear RPC failure for a wallet known to be active), `record_internal_nav.py` exits
with code 1 and prints `ERROR: zero NAV result — RPC failure suspected`.
No snapshot is written.

**Scope of the guard (narrow by design):** The guard triggers only on
`n_positions_enumerated == 0 AND free_sol == 0` (i.e., both RPC calls returned
nothing). If positions are found but all Jupiter calls fail, `degraded=True` is set
and the snapshot is still written (with partial NAV). This is intentional: a partial
snapshot is better than a gap in the history, and the degraded flag signals the anomaly.
A fully-degraded snapshot with `total_nav_sol ~= 0` is distinguishable from a healthy
zero by `degraded=True` and the mints in `notes`.

### AC-6: Snapshot row format
The written row passes `csv.DictReader` with the exact `FIELDS` list from
`record_portfolio_snapshot.py`:
`["timestamp", "source", "value_sol", "value_usd", "sol_usd", "net_contribution_sol",
"total_pnl_sol", "total_pnl_pct", "period_pnl_sol", "notes"]`.
`source == "internal"`. `value_usd`, `sol_usd` are empty (not queried). `value_sol`
uses 6-decimal precision.

### AC-7: net_contribution_sol resolution
The tool reads `net_contribution_sol` from `capital_flows.csv` via
`valhalla.capital_flow.read_flows()` if the file exists. If absent, it tries the
previous `source=internal` snapshot row (carry-forward). If neither available and
`--net-contribution-sol` is not passed, exits with a clear error.

### AC-8: Idempotency
Two consecutive invocations on the same calendar day both write rows. The second row's
`period_pnl_sol` reflects the change between the two runs (near-zero if NAV barely moved).
No deduplication logic.

### AC-9: Cron exit codes
- Exit 0: snapshot written (including degraded-but-written case)
- Exit 1: fatal error — no snapshot written (zero NAV guard, RPC timeout, missing config)

## Touchable Files

- `valhalla/internal_nav.py` — new module
- `tools/record_internal_nav.py` — new tool
- `tests/test_internal_nav.py` — new test file

**Do NOT touch:** `valhalla/cli.py`, `tools/record_portfolio_snapshot.py`,
`valhalla/solana_rpc.py`, `output/portfolio_snapshots.csv` schema,
any other existing file.

## Verification Contract

```bash
# Unit tests — all must pass
pytest tests/test_internal_nav.py -v

# Type checking
mypy valhalla/internal_nav.py tools/record_internal_nav.py --ignore-missing-imports

# Lint
ruff check valhalla/internal_nav.py tools/record_internal_nav.py

# Smoke test — requires .env with HELIUS_API_KEY (or HELIUS_RPC_URL) + LPAGENT_WALLET
python tools/record_internal_nav.py --dry-run
# Expected: prints a row with source=internal, value_sol > 0, no crash

# Degraded path smoke test (manual)
python tools/record_internal_nav.py --dry-run
# While running, note any "WARN: no Jupiter route for..." lines
# If any: NavResult.degraded should be True and notes should contain those mints
```

## Design

### `valhalla/internal_nav.py`

#### `NavResult` dataclass

```python
from dataclasses import dataclass, field
from decimal import Decimal
from datetime import datetime

@dataclass
class NavResult:
    wallet: str
    timestamp: datetime
    positions_nav_sol: Decimal     # LP reserves (all positions)
    fees_sol: Decimal              # unclaimed fees (all positions)
    rewards_sol: Decimal           # unclaimed rewards (all positions)
    free_sol: Decimal              # getBalance result
    idle_spl_sol: Decimal          # non-position SPL tokens
    total_nav_sol: Decimal         # sum of all above
    n_positions: int               # active positions counted
    degraded: bool                 # True if any token conversion failed
    degraded_mints: list[str] = field(default_factory=list)  # mints that failed
```

#### Public function

```python
def compute_nav(rpc_url: str, wallet: str) -> NavResult:
    """Compute portfolio NAV from on-chain Solana state.

    Raises RuntimeError if RPC call for positions fails entirely.
    Returns NavResult with degraded=True if any token conversion was skipped.
    """
```

#### RPC helpers (internal, not exported)

Extracted from spike with minimal changes:

- `_rpc_call(url, method, params, retries=3) -> dict`
  Same retry-on-429 logic as spike. Raises `RuntimeError` after exhausting retries.

- `_http_get(url) -> dict`
  Plain GET with `User-Agent: internal-nav/1.0`. No retry (callers handle it).

- `_fetch_accounts(rpc_url, pubkeys) -> list[Optional[bytes]]`
  Chunked at 100. Returns `None` for missing accounts.

#### Decoding functions (internal)

Extracted 1:1 from spike with these changes:
- Replace all `print(...)` with `logging.debug(...)` / `logging.warning(...)`
- Replace `float` intermediate results with `Decimal`
- `_jupiter_to_sol(mint, amount_raw) -> tuple[Decimal, bool]`: returns
  `(sol_value, was_degraded)`. `was_degraded=True` when no route found or
  all retries exhausted. Never returns `(0.0, False)` for an amount > 100 — if
  the call fails definitively, return `(Decimal("0"), True)`.

Functions to implement (all internal, prefixed `_`):
- `_get_position_addresses(rpc_url, wallet) -> list[str]`
- `_decode_position(data: bytes) -> dict`
- `_bin_array_address(lb_pair_pk: Pubkey, array_idx: int) -> Pubkey`
- `_decode_bin_array(data, array_idx, expected_lb_pair=None) -> dict[int, dict]`
- `_get_pool_mints(rpc_url, lb_pair) -> dict` (Meteora API → on-chain fallback)
- `_lbpair_mints_onchain(rpc_url, lb_pair) -> tuple[Optional[str], Optional[str]]`
- `_get_reward_mints(rpc_url, lb_pair) -> list[Optional[str]]`
- `_get_decimals(rpc_url, mint) -> int`
- `_jupiter_to_sol(mint, amount_raw) -> tuple[Decimal, bool]`

Module-level caches (same as spike):
- `_decimals_cache: dict[str, int] = {}`
- `_reward_mints_cache: dict[str, list[Optional[str]]] = {}`

#### Struct constants (top of module, locked)

```python
# Verified constants — DO NOT derive from Meteora IDL docs.
# Source: tools/spike_internal_nav.py (2026-05-24, mainnet, 0.014% diff)
METEORA_PROGRAM = Pubkey.from_string("LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo")
POSITION_V2_DISC = bytes([117, 176, 212, 199, 245, 180, 133, 182])
SOL_MINT = "So11111111111111111111111111111111111111112"
TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
LAMPORTS = 1_000_000_000
JUPITER_DELAY = 0.15  # seconds between Jupiter quote calls

# PositionV2 layout constants
POS_LB_PAIR = 8
POS_OWNER = 40
POS_LIQ_SHARES = 72
N_BINS = 70            # MAX_BIN_PER_POSITION — always 70, regardless of account size
POSV2_FIXED = 8120     # total fixed layout size (NOT 7920 — 200B metadata after upper_bin_id)
POSBIN_SIZE = 112      # PositionBinData extension bin size
REWARD_INFO_OFF = 1192
REWARD_INFO_SIZE = 48
REWARD_PENDING_OFF = 32
FEE_DATA_OFF = 4552
FEE_INFO_SIZE = 48
FEE_X_PENDING_OFF = 32
FEE_Y_PENDING_OFF = 40

# BinArray layout constants
BA_HEADER = 56         # 8 disc + 8 index + 1 version + 7 padding + 32 lb_pair
BIN_SIZE = 144
BIN_AMOUNT_X = 0
BIN_AMOUNT_Y = 8
BIN_LIQ_SUPPLY = 32
```

#### `compute_nav` implementation sketch

```python
def compute_nav(rpc_url: str, wallet: str) -> NavResult:
    degraded_mints: list[str] = []

    # Step 1: enumerate positions
    pos_addrs = _get_position_addresses(rpc_url, wallet)
    # decode + filter empty (all liq_shares == 0)
    # ... (same as spike Steps 2-5)

    # Step 6: free SOL
    free_sol = Decimal(rpc_call_balance(rpc_url, wallet)) / LAMPORTS

    # Zero-NAV guard — caller raises RuntimeError
    if not pos_addrs and free_sol == 0:
        raise RuntimeError(
            "zero NAV result: 0 positions and 0 free SOL — RPC failure suspected"
        )

    # Step 6b: idle SPL tokens
    # ... (same as spike)

    total_nav = positions_nav + fees + rewards + free_sol + idle_spl

    return NavResult(
        wallet=wallet,
        timestamp=datetime.now(timezone.utc),
        positions_nav_sol=positions_nav,
        fees_sol=fees,
        rewards_sol=rewards,
        free_sol=free_sol,
        idle_spl_sol=idle_spl,
        total_nav_sol=total_nav,
        n_positions=len(active_positions),
        degraded=bool(degraded_mints),
        degraded_mints=degraded_mints,
    )
```

### `tools/record_internal_nav.py`

```
Usage:
  python tools/record_internal_nav.py [--dry-run] [--wallet W] [--rpc-url URL]
                                       [--path CSV] [--net-contribution-sol N]
                                       [--timestamp TS] [--lpagent-nav N]

Env vars read (via .env):
  HELIUS_API_KEY or HELIUS_RPC_URL   — Solana RPC
  LPAGENT_WALLET                     — wallet address
```

Logic:
1. `load_dotenv()`
2. Resolve `rpc_url` and `wallet` from args or env (error if missing)
3. Call `compute_nav(rpc_url, wallet)` — propagate `RuntimeError` as exit code 1
4. Zero-NAV guard: if `result.total_nav_sol == 0`, exit 1 with message
5. Resolve `net_contribution_sol`:
   a. CLI `--net-contribution-sol` → use directly
   b. Read `capital_flows.csv` via `valhalla.capital_flow.read_flows(path, asof)` if exists
   c. Read previous `source=internal` row from CSV (carry-forward)
   d. Otherwise: exit 1 with message
6. Compute `total_pnl`, `period_pnl` (same arithmetic as `record_portfolio_snapshot.build_snapshot`)
7. Build notes: `""` normally; `"degraded: MINT1,MINT2"` if degraded
8. If `--lpagent-nav N` passed: print diff line and assert < 2% (AC-3 check)
9. Write row by inlining the `append_snapshot` logic (≈10 lines, open CSV in append mode,
   write header if new file, write DictWriter row) directly in `record_internal_nav.py`.
   Add a comment: `# mirrors tools/record_portfolio_snapshot.py:append_snapshot`.
   Do NOT import from `record_portfolio_snapshot` — that creates a circular path in tools/.

The `timestamp` defaults to `datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")`.

### Tests (`tests/test_internal_nav.py`)

All tests use mocks — no live RPC calls.

Required test cases:

**`test_decode_position_fixed`**
Given 8120-byte buffer with known bytes at known offsets:
assert `decode_position` returns correct `lower_bin_id`, `upper_bin_id`, `ext_count=0`.

**`test_decode_position_extended`**
Given `(8120 + 3*112)`-byte buffer (3 extension bins):
assert `ext_count=3`, `ext_liq_shares` has 3 entries.

**`test_decode_bin_array`**
Given a synthetic BinArray buffer with BA_HEADER=56, BIN_SIZE=144, known `amount_x`/`amount_y`:
assert decoded bin dict has correct values.

**`test_jupiter_degraded_no_route`**
Mock `_http_get` to raise `urllib.error.HTTPError` with body containing `"NO_ROUTES_FOUND"`:
assert `_jupiter_to_sol(mint, 1000)` returns `(Decimal("0"), True)`.

**`test_jupiter_degraded_429_exhausted`**
Mock `_http_get` to always raise 429:
assert after 4 attempts returns `(Decimal("0"), True)`.

**`test_jupiter_sol_passthrough`**
`_jupiter_to_sol(SOL_MINT, 1_000_000_000)` returns `(Decimal("1"), False)` without HTTP call.

**`test_compute_nav_zero_guard`**
Mock `_get_position_addresses` to return `[]` and free SOL to return 0:
assert `compute_nav(...)` raises `RuntimeError` matching "zero NAV result".

**`test_compute_nav_degraded_propagates`**
Mock one position with one non-SOL token where Jupiter returns `(Decimal("0"), True)`:
assert `NavResult.degraded == True` and the mint is in `degraded_mints`.

**`test_record_tool_degraded_notes`**
Mock `compute_nav` returning `NavResult(..., degraded=True, degraded_mints=["MINT1"])`:
assert the snapshot row's `notes` contains `"degraded: MINT1"`.

**`test_record_tool_zero_nav_exits`**
Mock `compute_nav` raising `RuntimeError("zero NAV result: ...")`:
assert the tool exits with code 1 and no snapshot is written.

**`test_snapshot_net_contribution_from_flows`**
Mock `read_flows` returning `Decimal("44.6")` and `compute_nav` returning a valid result:
assert the snapshot row has `net_contribution_sol="44.600000"`.

**`test_snapshot_net_contribution_carryforward`**
Mock `read_flows` raising FileNotFoundError (no capital_flows.csv);
existing CSV has a previous `source=internal` row with `net_contribution_sol="44.6"`:
assert new row carries forward `net_contribution_sol="44.600000"`.

**`test_bin_math_fraction`**
Unit test for the bin math formula:
given `liq_share=500`, `liq_supply=1000`, `amount_x=200`, `amount_y=100`:
assert `total_x_raw` accumulates `Decimal("100")`, `total_y_raw = Decimal("50")`.

### Jupiter throttle contract

The 0.15s delay in `JUPITER_DELAY` applies **between** all Jupiter calls. The module
uses a module-level `time.sleep(JUPITER_DELAY)` at the start of each `_jupiter_to_sol`
call (same as spike). Retry on 429 uses exponential backoff: wait `2**attempt` seconds
before each retry (attempt 0 → 0 wait, attempt 1 → 2s, attempt 2 → 4s).

### Decimal precision

All SOL amounts: `Decimal`, quantized to 6 decimal places before writing to CSV
(`str(value.quantize(Decimal("0.000001")))`). Match `record_portfolio_snapshot._fmt()`.

### Error handling philosophy

Match plan section E adversarial scenarios:
- Jupiter 429 exhausted → mark token degraded, include in `degraded_mints`, continue
- Token no route → same as above
- `getProgramAccounts` fails → raise RuntimeError → tool exits 1, no partial write
- Single BinArray not found → skip that bin (log warning), continue with other bins
- Single position decode fails → log warning, skip position, continue

## Implementation Plan

1. Create `valhalla/internal_nav.py`:
   - Define all constants and `NavResult` dataclass
   - Implement RPC helpers (lifted from spike with logging substitution)
   - Implement decoding functions (`_decode_position`, `_decode_bin_array`)
   - Implement pricing functions (`_get_pool_mints`, `_get_decimals`, `_jupiter_to_sol`)
   - Implement `compute_nav()` orchestrator

2. Create `tools/record_internal_nav.py`:
   - CLI with `argparse`
   - `.env` loading and config resolution
   - Call `compute_nav()`, handle RuntimeError → exit 1
   - `net_contribution_sol` resolution (flows → carry-forward → error)
   - Build and write snapshot row

3. Create `tests/test_internal_nav.py` with all test cases above.

4. Run Verification Contract (all tests, mypy, ruff, smoke test).

## Dependencies

- `solders` (Pubkey + find_program_address) — already in requirements
- `base58` — already in requirements
- `python-dotenv` — already in requirements
- `valhalla.capital_flow.read_flows` — from doc 023 (already implemented)
- `record_portfolio_snapshot.append_snapshot` — imported at call site to avoid circular

## Alternatives Considered

- **Reuse `SolanaRpcClient`**: Rejected — it lacks `getProgramAccounts` with filters
  and `getMultipleAccounts`; adding them would expand its scope beyond address resolution.
- **Split into 030 core + 031 pipeline**: Rejected — the orchestration is 50 lines;
  the coupling is tight; one context window is sufficient.
- **`notes` column for degraded vs `source=internal-degraded`**: Notes wins — avoids
  breaking scoreboard source filtering (`source=internal` stays stable).
- **Skip snapshot when degraded**: Rejected — partial data is better than a gap in
  history; the degraded flag lets downstream tooling filter if needed.
