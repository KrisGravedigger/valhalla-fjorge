---
critical: true
---

# [032] NAV Pricing: Materiality Contract (de-brittling degraded)

## Intent

Stop the recurring "degraded NAV" fire drills. Swarga (sister project) hard-fails on a
degraded internal NAV snapshot, and today a single dust-sized token hitting a *new*
Jupiter error variant can degrade the whole NAV and block the snapshot. Each new Jupiter
error string has cost a multi-attempt fix session (CANNOT_COMPUTE_OTHER_AMOUNT_THRESHOLD
was only the latest).

Root cause of the brittleness: `degraded` is decided by **cause classification**
(which HTTP code / which error string Jupiter returned), and Jupiter's error vocabulary
is open-ended. This doc replaces cause-based classification with a **materiality-based
contract**: the only question that decides `degraded` is *"how much SOL value might we
be missing?"* — not *"which error did the API return?"*.

Secondary goals: separate transient infrastructure failures (retry later) from data-quality
failures (degraded), and reduce the heuristic surface added during the previous
6-iteration hardening.

**This is a contract change + simplification, not a rewrite.** Net LOC in
`valhalla/internal_nav.py` must go DOWN. If an implementation step requires adding a new
heuristic or a new error-string branch, stop and re-read this Intent.

## Context

- `valhalla/internal_nav.py` — `compute_nav` walks Meteora positions, fees, rewards,
  free SOL and idle SPL; all token valuation already flows through one chokepoint
  `_value_mint_amount` (commit 7eb9e2d). Keep the chokepoint; change its decision logic.
- `tools/record_internal_nav.py` — writes `output/portfolio_snapshots.csv`; refuses to
  write when `result.degraded` unless `--allow-degraded`; exit 1 for everything that fails.
- Swarga reads the latest `source=internal` CSV row (`swarga/src/config.ts`); it throws when
  `notes` starts with `"degraded:"` and enforces snapshot freshness (`maxAgeSeconds`).
  → A degraded-and-not-written snapshot eventually also fails Swarga via staleness, so
  "don't write" is acceptable only for *transient* problems that cron can retry.
- `output/internal_nav_skipped_mints.json` — durable no-route skip cache. Keep.
- 35 tests exist in `tests/test_internal_nav.py`, several of them lock in the current
  cause-based heuristics. **Adapt/delete those tests; do not preserve behavior this doc
  removes.**

### Known holes in HEAD that this doc closes

1. **429 storm degrades NAV.** Persistent Jupiter 429 after retries → mint enters
   `_jupiter_failed_cache` → degraded → exit 1. Rate limiting is a known recurring issue;
   this WILL fire again.
2. **Asymmetric reference fallback.** `_jupiter_immaterial_reference_to_sol` returns
   `None` when `amount_raw > 1e9`, so a large raw amount of a junk token failing with any
   4xx degrades NAV. The asymmetry is unnecessary: linear extrapolation *upward*
   overestimates value (ignores price impact), which is conservative for a materiality
   test.
3. **Unknown future error strings degrade.** Any 4xx body not matching the three no-route
   markers is treated as a genuine failure. New Jupiter error variants land here by
   default — this is the whack-a-mole generator.
4. **Transient vs data-quality conflated.** Exit 1 means both "Jupiter is down, retry in
   10 minutes" and "the data is wrong". Cron can't tell them apart.

## Goals

1. A new Jupiter error variant (unknown 4xx string) on an immaterial amount must NOT
   degrade NAV and must NOT block the snapshot — without any code change.
2. Jupiter-wide transient failure (429 storm, 5xx, network) must NOT produce a degraded
   snapshot; it aborts the run with a distinct retryable exit code and writes nothing.
3. `degraded` fires only for: structural unknowns (missing/undecodable position or
   bin-array, u64 decode artifact, unknown reward mint with pending amount) and
   **material** unpriceable value.
4. Heuristic surface shrinks: `SUSPICIOUS_SPL_RAW_AMOUNT` and the root-logger warning
   filter are removed.

## Non-Goals

- No new price sources (Birdeye/DexScreener). Jupiter stays the only oracle.
- No change to the on-chain decoding layer (struct constants stay locked).
- No change to Swarga; the CSV contract (`notes` prefix `degraded:`) is preserved.
- No parallel agent competition. `critical: true` means stronger post-implementation
  verification only.

## Technical Design

### The decision table (single source of truth)

`_value_mint_amount` / `_quote_jupiter_to_sol` must implement exactly this, in this order:

| # | Situation | Value used | Degraded? | Note in snapshot |
|---|-----------|-----------|-----------|------------------|
| 1 | `mint == SOL_MINT` | amount/1e9 | no | — |
| 2 | `amount_raw <= 0` | 0 | no | — |
| 3 | `amount_raw > U64_MAX` (decode artifact) | 0 | **YES** | u64 overflow |
| 4 | mint in durable skip cache | 0 | no | aggregated no-route count |
| 5 | direct quote OK | quote | no | — |
| 6 | direct quote no-route (markers) | 0 | no | aggregated no-route count; mint → skip cache |
| 7 | direct quote fails for ANY other non-transient reason (any 4xx, any unknown body) → reference quote OK, `price × amount < MATERIALITY` | `price × amount` | no | aggregated immaterial-fallback count |
| 8 | same as 7 but `price × amount ≥ MATERIALITY` | 0 | **YES** | material unpriceable mint |
| 9 | direct quote fails non-transiently → reference quote no-route | 0 | no | aggregated no-route count |
| 10 | transient failure (429/5xx/network) persists after retries — on direct OR reference quote | — | — | **abort run: raise `TransientPricingError`** |
| 11 | reward pending > 0 but reward mint unknown | 0 | **YES** | unknown reward mint |

- `MATERIALITY = IMMATERIAL_NAV_THRESHOLD_SOL = Decimal("0.01")` (unchanged).
- **Aggregate cap:** track the summed estimated value of all rows that hit case 7.
  If the sum ≥ `Decimal("0.05")` SOL → set degraded once (marker `immaterial-sum`).
  Rationale: death by a thousand dusts. (No-route rows don't count toward the sum —
  untradable means ~0 realizable value.)
- Error-string matching survives ONLY for detecting no-route
  (`NO_ROUTES_FOUND`, `TOKEN_NOT_TRADABLE`, `COULD_NOT_FIND_ANY_ROUTE`). It is no longer
  load-bearing for degraded: an unmatched string falls into case 7/8 (materiality
  decides), never directly into degraded.
- Reference quote (`JUPITER_REFERENCE_AMOUNT_RAW = 1e9` raw): remove the
  `amount_raw > reference → None` restriction. Extrapolate linearly in both directions.
- Transient classification: HTTP 429, HTTP 5xx, `URLError`/timeout/connection errors.
  Retry with the existing backoff; if still failing → case 10. Per-run circuit breaker:
  after the **first** `TransientPricingError`, abort the whole run (don't grind through
  remaining mints against a dead API).
- **The transient contract covers Solana RPC too, not only Jupiter.** `_rpc_call` must
  retry `TimeoutError`/`URLError`/`ConnectionError` (including read timeouts raised from
  `response.read()`) with the same backoff as HTTP 429, and raise `TransientPricingError`
  after exhaustion. A raw stdlib traceback escaping `compute_nav` for a network hiccup is
  a contract violation. (Found live 2026-06-12: Helius read timeout on
  `getMultipleAccounts` crashed the tool with exit 1 instead of exit 2.)

### Exit codes (`tools/record_internal_nav.py`)

| Exit | Meaning | Cron action |
|------|---------|-------------|
| 0 | snapshot written (warnings allowed) | nothing |
| 1 | degraded data or validation error (`--allow-degraded` overrides degraded) | alert human |
| 2 | transient infrastructure failure, nothing written | retry later, alert only after N consecutive |

`TransientPricingError` propagates out of `compute_nav`; the tool catches it, prints
`TRANSIENT: <reason> - retry later`, returns 2.

### Removals (explicit)

1. `SUSPICIOUS_SPL_RAW_AMOUNT` heuristic and its branches in `_value_mint_amount`.
   Replacement at snapshot level: in `build_snapshot_row`, if a previous internal
   snapshot exists and `|value_sol - prev| / prev > 0.5`, append note
   `nav-jump: +NN.N% vs previous` (warning-only, still writes, exit 0).
2. `_IdleJupiterWarningFilter` and the root-logger add/remove dance. The
   `suppress_immaterial_warning` flag on the chokepoint decides log level
   (idle-SPL noise → `logging.debug`, material paths → `logging.warning`).
3. Vestigial `rpc_url` parameters (`del rpc_url`) in `_convert_amount` /
   `_convert_idle_amount` — fold both wrappers into direct chokepoint calls if that
   reads cleaner.
4. `_jupiter_failed_cache` as a degraded source: a mint may stay cached per-run to avoid
   re-querying, but the cached outcome must be one of the table's cases, not a blanket
   `degraded=True`.

### Unchanged

- Durable skip cache read/write semantics (incl. "reference fallback does not persist
  no-route into the durable cache").
- Per-run price cache (`_jupiter_price_cache`).
- Zero-NAV guard, `--lpagent-nav` cross-check, CSV row format, `notes` field semantics
  (`degraded:` prefix first, then aggregated counts).
- u64 guard (case 3).

## Touchable Files

- `valhalla/internal_nav.py`
- `tools/record_internal_nav.py`
- `tests/test_internal_nav.py`
- `tests/fixtures/jupiter_errors/` (new — recorded error bodies)

Nothing else. Swarga repo is read-only context.

## Acceptance Criteria

### AC-1: Unknown future error string is harmless when immaterial
Fixture: direct quote → HTTP 400 with body `{"error":"SOME_BRAND_NEW_ERROR_CODE"}` (a
string that appears nowhere in the codebase); reference quote OK; extrapolated value
0.0004 SOL. → NAV not degraded, value included, note counts the mint, snapshot writes
with exit 0.

### AC-2: Material unpriceable value still degrades
Same fixture but extrapolated value 0.4 SOL → degraded, exit 1 without
`--allow-degraded`.

### AC-3: 429 storm is transient, not degraded
Fixture: every Jupiter call → HTTP 429, retries exhausted. → `TransientPricingError`,
exit 2, CSV untouched, no mint added to the durable skip cache, output contains
`TRANSIENT`.

### AC-4: Large-amount dust no longer degrades
Fixture: `amount_raw = 5e9` (> reference 1e9), direct quote 400 (non-no-route), reference
price puts the value at 0.002 SOL → not degraded (closes the asymmetry hole).

### AC-5: Aggregate dust cap
Fixture: 12 mints each hitting case 7 at 0.006 SOL → sum 0.072 ≥ 0.05 → degraded with
`immaterial-sum` marker.

### AC-6: nav-jump warning is non-blocking
Previous internal snapshot 10 SOL, new NAV 16 SOL → row written, exit 0, note contains
`nav-jump`.

### AC-7: Structural failures still degrade
Missing bin array / undecodable position / unknown reward mint with pending raw → degraded
(existing behavior preserved; existing tests for these keep passing).

### AC-8: RPC timeout is transient, not a crash
Fixture: `_rpc_call`'s HTTP layer raises `TimeoutError` on every attempt (e.g. patched
`urllib.request.urlopen` or a response whose `read()` times out). → retries with backoff,
then `TransientPricingError`, exit 2, no traceback on stdout/stderr, CSV untouched.

### AC-9: Heuristics gone
`SUSPICIOUS_SPL_RAW_AMOUNT` and `_IdleJupiterWarningFilter` no longer exist in the module;
`git diff --stat` shows net negative LOC for `valhalla/internal_nav.py`.

## Verification Contract

```bash
# 1. Full suite (adapted, not just appended)
python -m pytest tests/test_internal_nav.py -q

# 2. The whole repo still green
python -m pytest -q

# 3. Smoke: live dry-run (requires .env)
python tools/record_internal_nav.py --dry-run
# Expected: exit 0, row with source=internal, value_sol > 0

# 4. Grep gates
grep -c "SUSPICIOUS_SPL_RAW_AMOUNT" valhalla/internal_nav.py   # expected: 0 (grep exits 1)
grep -c "_IdleJupiterWarningFilter" valhalla/internal_nav.py   # expected: 0 (grep exits 1)
```

## Review guidance (for adversarial review — read this)

Verify the implementation against the decision table, case by case. The previous
hardening round degenerated into 6 iterations of added heuristics; that failure mode is
explicitly out of bounds here. A finding is valid only if it shows a deviation from the
table or a value-correctness bug. "What if Jupiter returns X?" is answered by the table
(case 7/8/10) — proposing a new error-specific branch is not an acceptable fix.

## Alternatives Considered

- **Second price oracle (Birdeye) as fallback** — rejected for now: another API key,
  another rate limit, another error vocabulary. The materiality contract makes the single
  oracle tolerable; revisit only if case 8 (material unpriceable) actually occurs in
  practice.
- **On-chain decode of reward mints from LbPair** (removes the dlmm-api.meteora.ag
  dependency for case 11) — deferred: requires offset verification against the spike
  methodology. Candidate for a follow-up doc if unknown-reward-mint degradation ever
  fires in production.
- **Valuing no-route mints at reference price instead of 0** — rejected: no route means
  the value is not realizable; 0 is the honest number.
