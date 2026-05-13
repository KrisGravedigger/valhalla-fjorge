# Portfolio Truth — PnL/NAV Reliability Plan

**Date:** 2026-05-10
**Status:** A/B/D-lite shipped (docs 023/024/025, merged 2026-05-10). C shipped (docs 026/027, merged 2026-05-12). E-spike doc written (doc 028, 2026-05-13). D-full, E-full, F pending.
**Supersedes (in spirit):** parts of `PLAN.md` (2026-04-02), all of `notes/portfolio_nav_pnl_model.md`

---

## TL;DR

We currently confuse **closed-position PnL** (attribution) with **portfolio NAV PnL** (scoreboard). The two answer different questions and will never agree as long as positions are still open. On top of that, the lpagent client has a broken cache contract that silently produces stale, partially-overlapping data. This plan splits the work into six sub-projects and sequences them so the user gets a working scoreboard quickly and the deeper fixes follow.

**Decisions confirmed by user (do not relitigate):**
- **Daily bucket timezone: UTC** (consistent with lpagent native, avoids DST surprises).
- **NAV source: track all sources in parallel** (lpagent, Fabriq, eventual internal) — pick a "primary" later.
- **lpagent's "2.29 SOL all-time" view is not authoritative.** Empirical comparison shows our closed PnL on 2026-05-02→09 ≈ 2.73 SOL, on 2026-05-07→09 ≈ 2.37 SOL — the short-window value is close; lpagent likely has limited retention and/or different valuation. Reconciliation will determine this empirically.
- **Attribution drift of ~1.18 SOL** (our closed-PnL vs lpagent on 1196 matched positions) is explicitly out of scope for this plan. It lives in attribution-land and is acceptable for scorecard/wallet-ranking purposes. Do not attempt to close it here.
- **Capital flow bootstrap:** single row on `2026-02-11` (start of tracking) with `sol_amount=44.6` (canonical figure per `PITCH.md` 2026-04-30). Period PnL before that date is undefined — acceptable.
- **A-B sequencing:** loose. B may ship with `--net-contribution-sol` fallback while A is being built; not a strict prerequisite.
- **Scoreboard reporting surface:** both console block at end of pipeline run + `output/portfolio_scoreboard.md` durable artifact.

---

## Problem statement

Numbers we have today (all "all-time"):

| Source | Value | Meaning |
|---|---:|---|
| `output/positions.csv` (closed PnL sum) | +31.79 SOL | Closed positions only |
| Old `portfolio_cumulative.png` (before fix) | +9.99 SOL | Closed PnL minus retired wallets — chart artifact |
| lpagent UI (today) | +2.29 SOL | Last ~7-14 days, subject to lpagent retention |
| Fabriq (USD-equivalent) | ~60.14 SOL | NAV including open positions, USD-priced |
| lpagent NAV widget | ~63.84 SOL | NAV including open positions |
| User-stated net contribution | 44.4 SOL | Deposits minus withdrawals |

Implied "real" portfolio PnL = NAV − net contribution = ~16-19 SOL. None of the per-position views give this number, because **the +69.3 SOL of still-open deployed capital sits between closed-PnL and NAV.**

This is **not a bug** — it is a category mismatch. Codex's chart fix correctly restored +31.79 to the cumulative chart, but that only re-confirms the closed-PnL view; it does not solve the scoreboard problem.

---

## Conceptual framework

Two metrics, two purposes — keep them lexically distinct in code, charts, and reports:

### Closed-position PnL (attribution)
- Sum of realized PnL over closed positions in `output/positions.csv`.
- **Use:** wallet/strategy/source-wallet attribution, scorecard inputs, loss autopsy.
- **Authoritative source:** Discord events parsed into positions.csv, with lpagent backfill for gaps.
- **Known drift:** ~1.18 SOL on a 1196-position sample vs lpagent valuation (token-side methodology differences). Acceptable for attribution; **out of scope for this plan**.

### Portfolio NAV PnL (scoreboard)
- `nav_today_sol − net_contribution_sol` (cumulative), or `nav_today − nav_yesterday − net_flow_today` (daily).
- **Use:** answer "am I making money on this portfolio?".
- **Authoritative source:** time series of NAV snapshots × time series of capital flows. Today only manual snapshots exist; both sides need infrastructure.

**Rule:** every chart, report, and CLI message must label which metric it shows. No more "PnL" alone.

---

## Empirical findings (verified locally on 2026-05-09)

### lpagent API semantics
- Endpoint `/lp-positions/historical` returns only `status=Close` positions.
- `from_date` filters on `createdAt` (open date). All sampled cache files have zero positions with `createdAt < from_date`.
- **`to_date` is ignored** — every cache contains positions opened from `from_date` through "now-at-time-of-fetch". Examples:
  - `2026-04-30.json` covers `createdAt` 2026-04-30 → 2026-05-07 (8 days, 871 positions).
  - `2026-04-15.json` covers `createdAt` 2026-04-15 → 2026-04-16 (cache written next day).
- Pagination: `pageSize=10` fixed. `totalCount` and `totalPages` are returned — **but the current client never asserts `len(retrieved) == totalCount`**, so silent truncation can occur undetected.

### Cache contract is broken
- Path is `{cache_dir}/{YYYY-MM-DD}.json` with no wallet component in the name — corrupts silently if `LPAGENT_WALLET` ever changes.
- File "for date X" actually means "snapshot of all positions opened ≥ X, frozen at write time". Re-reading later returns stale data; adjacent files overlap.
- Watermark `last_synced_date` advances to `to_date` after each successful run — but a position **opened before watermark and closed after watermark** is structurally invisible to all future fetches. Max observed hold time in sample: ~87 hours.

### Timezone reality
- lpagent: ISO timestamps with `Z` suffix → UTC.
- Discord exporter: depends on parser; mostly local time (CET/CEST) per existing `event_parser` logic.
- Decided: daily bucket boundary = UTC midnight. Discord timestamps already converted to UTC via existing parsing chain — confirm in implementation.

---

## Sub-projects

Six tracks. Sequencing prioritizes "scoreboard works fast" over "everything fixed perfectly".

### A. Capital flow ledger (Phase 1)
**Goal:** External capital deposits/withdrawals tracked in `output/capital_flows.csv` so `net_contribution_sol` is a ledger, not a typed-in number that drifts.

- New CSV: `timestamp_utc`, `wallet`, `type` (`deposit` / `withdrawal` / `internal_transfer`), `sol_amount`, `tx_signature` (optional), `notes`.
- `internal_transfer` = wallet-to-wallet inside the portfolio; nets to zero portfolio-wide.
- Helper script `tools/record_capital_flow.py` for manual entry.
- **Bootstrap (decided):** single row, `timestamp_utc=2026-02-11T00:00:00Z`, `wallet=portfolio_bootstrap`, `type=deposit`, `sol_amount=44.6`, `tx_signature=` (empty), `notes="bootstrap row — net contribution at start of tracking; replace with per-deposit breakdown if available later"`.
- **A.2 — Stale-flow guard:** end-of-pipeline warning if `max(timestamp_utc)` in `capital_flows.csv` is older than 14 days. Five lines of code; catches forgotten entries before they corrupt scoreboard math.
- Phase 2: on-chain auto-detection (sub-project F).

**Effort:** S (~half day). **Risk:** low. **Dependencies:** none.

**Dependency relation to B:** A is not a hard blocker for B. `record_portfolio_snapshot.py` already accepts `--net-contribution-sol` as a manual override. B can ship immediately with that fallback; A makes the value automatic and auditable. Recommended: implement A first in the same session (it is smaller), but do not block B if A is delayed.

**Adversarial scenarios:**
- Duplicate `tx_signature` entry: ledger reads the same on-chain deposit twice — net contribution is overstated, total_pnl is understated. Mitigation: `record_capital_flow.py` checks for duplicate `tx_signature` before appending (NULL signatures always allowed through, as they cover manual entries without an on-chain TX).
- One-sided `internal_transfer`: user records wallet A → B as a deposit on B, forgets to record the withdrawal on A. Portfolio-level net contribution becomes inflated. Mitigation: when `type=internal_transfer` is recorded, tool prints a reminder to record the matching leg. Phase 2 (on-chain detection) closes this automatically.

---

### B. Portfolio snapshot system (Phase 1)
**Goal:** Daily NAV snapshots from each tracked source, stored in one time series per source.

- Extend `tools/record_portfolio_snapshot.py` to:
  - Read `net_contribution_sol` from `capital_flows.csv` (sum up to `--asof` date, default today) when `capital_flows.csv` exists. Fall back to `--net-contribution-sol` CLI arg when it does not (backwards-compatible during bootstrap).
  - Always store `source` = one of `lpagent`, `fabriq`, `internal` (or free-form `manual` for one-offs).
  - Compute `total_pnl_sol = value_sol − net_contribution_sol` and `period_pnl_sol = (value_t − value_t-1) − (contribution_t − contribution_t-1)`.
- **Reporting surface (both):**
  1. Console block printed at end of pipeline run: "Portfolio scoreboard — [date]" with per-source table.
  2. `output/portfolio_scoreboard.md` — regenerated each run. The markdown file is the durable artifact; the console block is for interactive use.
- New chart: `portfolio_nav_pnl.png` — multi-line, one line per source. Legend must clearly distinguish this from `portfolio_cumulative.png` (closed-PnL chart). Both charts must carry a subtitle stating which metric they show.

**Effort:** S-M (~1 implementation session). **Risk:** low. **Dependencies:** A (soft — see note above).

**Adversarial scenarios:**
- Two snapshots for the same source on the same day (e.g., user runs script twice): `period_pnl_sol` on the second call is near-zero (value barely changed) but the calculation is still valid. No deduplication needed — the time series is append-only; latest-per-source is used for reporting. However if the user records a wrong value and re-records the correct one, both rows persist. Acceptable for now; add `--amend` flag in Phase 2 if needed.
- `total_pnl_sol` is negative when contribution > value (portfolio underwater). No special handling needed — display as-is with a negative sign. The math is correct; the user should see the truth.
- `capital_flows.csv` missing or empty during B run: fall back to `--net-contribution-sol` if provided, else error clearly: "No capital_flows.csv found and --net-contribution-sol not provided."

---

### C. lpagent client redesign + cache integrity audit
**Goal:** Correct API semantics, no silent stale data, idempotent re-runs.

**C.1 — Client rewrite:**
- Replace per-day cache with a single flat file `output/lpagent_cache/positions_{wallet_prefix}.jsonl` keyed on `tokenId`. Each line: `createdAt`, `updatedAt`, `fetched_at_utc`, full raw payload (JSON-encoded).
- API contract: `fetch_since(from_date_utc)` paginates from `from_date` to current end. **Asserts `len(retrieved) == totalCount` after each full pagination.** Deduplicates by `tokenId` (last-write-wins on `updatedAt`).
- Sliding-window refresh: always re-fetch positions where `now − createdAt < refresh_window` (default: 5 days = max observed hold time ~87h + 33h buffer), even if already cached. This catches positions opened-before-watermark-closed-after-watermark.
- Wallet component in cache filename; on startup, verify `LPAGENT_WALLET` matches the cached wallet. Refuse to silently merge data from a different wallet.

**C.2 — Watermark redesign:**
- Replace single `last_synced_date` with structured JSON:
  ```json
  {
    "wallet": "J4tkG...",
    "min_safe_open_date": "2026-02-11",
    "last_full_refresh_at": "2026-05-09T14:22:00Z",
    "refresh_window_hours": 120
  }
  ```
- Semantics: "we have all positions where `createdAt ≥ min_safe_open_date`, as seen at `last_full_refresh_at`."
- Refresh policy: if `now − last_full_refresh_at > 24h`, re-pull `[now − refresh_window, now]`.

**C.3 — Cache integrity audit + migration:**
- Audit script walks existing daily `{date}.json` files; identifies overlaps, gaps, and any `tokenId` where `updatedAt > cache_file_date + 1d` (the bug signature).
- Produces a one-shot migration to the new flat JSONL format. After migration, old daily files are **frozen as read-only archive** in `output/lpagent_cache/archive/` — not deleted, not re-read.
- Migration is idempotent: running twice produces the same flat file.

**Effort:** M (~3 implementation sessions). **Risk:** medium (changes semantics of all downstream lpagent consumers; the cross-check and reconciliation tools depend on this). **Dependencies:** none, but deliver A and B first.

**Adversarial scenarios:**
- `totalCount` reported as 50 but only 40 rows retrieved (API bug or early pagination cutoff): client raises `AssertionError` with context rather than silently returning incomplete data.
- Partially-written JSONL from a crash mid-write: on next run, client detects the last line is not valid JSON (truncated), truncates the file at the last valid record, and re-fetches the refresh window.
- Two pipeline runs racing on the same JSONL file (e.g., cron + manual): last writer wins, but intermediate state is always a valid JSONL (writes are append then rename). Windows atomic-rename caveat: use `os.replace` which is atomic on NTFS.

---

### D. Reconciliation report
**Goal:** A focused tool comparing `positions.csv` vs lpagent for any date window/wallet scope, with categorized diffs. Also the tool that empirically answers "what retention window does lpagent's UI use?"

**D-lite (ships before C, using existing broken cache):**
A read-only script against the current daily cache files. Clearly labeled in output:

```
WARNING: lpagent data sourced from legacy daily cache files (known stale/overlapping).
Results are approximate. Re-run after C (client redesign) for authoritative numbers.
```

D-lite output: matched positions with PnL diff, lpagent-only (may be duplicates due to cache overlap), ours-only categorized by age. This is enough to empirically test candidate retention windows and see if any 7-day or 14-day window sums to 2.29 SOL.

**Full D (after C):**
- New CLI: `python -m valhalla.reconcile --from YYYY-MM-DD --to YYYY-MM-DD [--wallet X] [--legacy-cache]`.
- Inputs: `positions.csv` rows with `datetime_close` in window; lpagent flat JSONL with `updatedAt` in window.
- Outputs (one Markdown report + one CSV per category):
  - **Matched:** per-position row with `pnl_ours`, `pnl_lpagent`, `diff_sol`, `diff_pct`, `valuation_method_hint`.
  - **lpagent-only:** present in lpagent, absent from our CSV. Sub-categorized: "in archive/" / "truly missing" / "outside our wallet set".
  - **Ours-only:** in our CSV, not in lpagent. Sub-categorized: "older than lpagent retention" / "wallet not tracked by lpagent" / "lpagent dropped (e.g. zero-token positions like DSc936vC)".
  - **Aggregates:** per-wallet sums for both sides; per-day sums; total drift.
- Date semantic is explicit in report header: whether the window filters on `open_date` or `close_date`.
- Document findings in `notes/lpagent_retention.md` once retention window is confirmed.

**Effort:** D-lite = XS (~2-3 hours). Full D = M (~1.5 sessions). **Risk:** low (read-only). **Dependencies:** D-lite has no dependencies. Full D depends on C.

**Adversarial scenarios:**
- `tokenId` absent or empty in lpagent response: skip row, log a warning with the raw dict. Do not crash — API has returned empty tokenIds in edge cases (e.g., DSc936vC-style positions).
- Duplicate `full_address` in `positions.csv` (two rows, same position, e.g. one from Discord, one from lpagent backfill): dedup before matching, keep the row where `pnl_source != 'lpagent'` (prefer Discord-derived). Log the duplicate.

---

### E. Internal NAV computation (Phase 2) — feasibility-gated
**Goal:** Derive portfolio NAV from on-chain state, independent of any third-party UI.

**This sub-project is explicitly feasibility-gated.** Before committing implementation sessions, verify the following endpoints return enough data to reconstruct active DLMM position value:

| Endpoint | What we need from it | Kill criterion |
|---|---|---|
| Solana RPC `getBalance` | Free SOL per wallet | Works, no risk |
| Solana RPC `getTokenAccountsByOwner` | Idle SPL token balances | Works, no risk |
| `getProgramAccounts` filtered by Meteora DLMM program ID + owner | List of active position accounts | If RPC node rate-limits or returns empty for active positions, E is dead |
| Meteora `/position/{addr}` or DLMM SDK `getPositionsByUserAndLbPair` | Current bin reserves per active position (token-X and token-Y amounts withdrawable now) | **This is the hard part.** If the API/SDK does not expose per-bin current reserves without an RPC simulation, E is dead |
| Unclaimed fees per position | Meteora `getClaimableFee` or equivalent | Risk: medium |
| Jupiter v6 `/quote` for non-SOL → SOL | Convert all token balances to SOL at snapshot time | Fails if token has no route (illiquid); fallback: last known DLMM bin price, mark snapshot "degraded" |

**Kill criterion (explicit):** if active-bin reserves per position cannot be reconstructed without a simulated withdrawal transaction, sub-project E is permanently deferred and the scoreboard will depend on lpagent/Fabriq indefinitely. This is a legitimate outcome — document it in `notes/internal_nav_feasibility.md` after the spike.

**Verification spike (separate, ~2 hours, before any E implementation):** write a throwaway script that fetches the current state of one known active position and checks whether the per-bin token amounts match the lpagent-reported current value within 5%. If yes, proceed. If no, kill E.

**If feasibility confirmed:**
- For each tracked wallet: free SOL + active DLMM reserves + unclaimed fees + idle SPL tokens → convert non-SOL to SOL via Jupiter quote → `internal_nav_sol`.
- Snapshot row format identical to B (same CSV columns, `source=internal`).
- Cron-friendly: emit once per UTC day boundary.

**Effort (if feasible):** L (~4-6 sessions). **Risk:** medium-high. **Dependencies:** A, B, feasibility spike passing.

**Adversarial scenarios:**
- Jupiter returns HTTP 429 mid-snapshot (rate limit): retry with exponential backoff up to 3 attempts. If all fail, mark the token's SOL value as `null`, mark snapshot as `degraded=True`, write partial snapshot. Never write a snapshot that silently drops tokens.
- Token with no Jupiter route at all (e.g., illiquid meme token): fall back to last known DLMM bin price × amount. Document the fallback in the snapshot row (`nav_method=bin_price_fallback`).
- `getProgramAccounts` RPC throttle: if the node refuses, the script should exit cleanly with a message rather than returning zero NAV — a zero NAV snapshot is worse than no snapshot.

---

### F. Capital flow ledger (Phase 2) — on-chain auto-detection
**Goal:** Detect deposits/withdrawals automatically rather than relying on manual entry.

- For each tracked wallet, scan SOL and SPL token transfers where the counterparty is **not** in a known internal set: Meteora program, Jupiter program, wrapped-SOL system, position program rent, internal wallet-to-wallet, fee accounts.
- Anything else = proposed external flow. Tool prints candidate rows for `capital_flows.csv` with confidence score; user confirms before commit.
- Lower priority — Phase 1 manual ledger (A) covers the immediate need.

**Effort:** L (~4 sessions). **Risk:** medium. **Dependencies:** A, RPC access established.

---

## Sequencing — implementation sessions, not calendar weeks

Calendar weeks are misleading for a user who works iteratively. Use sessions instead.

```
Session 1:    [A] capital_flows.csv + record_capital_flow.py
              [B] snapshot tool update + scoreboard console + scoreboard.md

Session 2:    [D-lite] legacy-cache reconciliation (quick, no C needed)
              → user can run "what does 2.29 mean?" exploration immediately

Session 3-5:  [C] lpagent client redesign (C.1 + C.2 + C.3)

Session 6:    [D] full reconciliation against clean lpagent data

Session 7+:   [E] feasibility spike → decision → (implement or kill)
              [F] on-chain capital flow detection (if F is prioritized)
```

**Milestone 1 (after session 1):** User can record a NAV snapshot, see portfolio PnL vs net contribution in the console and a markdown scoreboard. lpagent client still buggy but quarantined.

**Milestone 2 (after session 2):** User can run D-lite and empirically test candidate retention windows against the 2.29 SOL figure.

**Milestone 3 (after session 5):** lpagent data is trustworthy; cache is clean; full reconciliation is available.

**Milestone 4 (later):** Internal NAV is the authoritative scoreboard source, if E passes feasibility.

---

## Test strategy

These are the minimum tests needed so that design-doc implementers can verify correctness without running the live API.

### Scoreboard math (sub-project B)
- Unit tests for `build_snapshot()` in `record_portfolio_snapshot.py` with synthetic inputs: Decimal precision, period_pnl when previous snapshot exists vs. not, negative total_pnl, fallback to `--net-contribution-sol`.
- Fixture: synthetic `capital_flows.csv` with 3 rows (two deposits, one withdrawal) → assert sum equals expected net contribution.

### Reconciliation golden-file test (sub-project D)
- Freeze a small slice of `output/lpagent_cache/2026-04-30.json` (e.g., 20 positions) as a test fixture.
- Construct a synthetic `positions.csv` with: 10 matching positions, 5 ours-only, 5 lpagent-only.
- Assert: matched count = 10, ours-only = 5, lpagent-only = 5, aggregate diff in expected range.

### Cache migration roundtrip (sub-project C)
- Run the audit/migration script against a copy of the real daily cache files.
- Assert: all `tokenId` values in the daily files appear exactly once in the JSONL output (deduped), all `pnlNative` values preserved (no silent truncation).

### lpagent client mock (sub-project C)
- Mock `urllib.request.urlopen` to return fixture JSON with known `totalCount` and two pages.
- Assert: pagination stops at `totalPages`, `totalCount` assertion fires if page data is short.
- This avoids the 12-second rate-limit sleep in CI.

---

## Open items / things to verify during implementation

1. **Discord timestamp → UTC conversion.** Confirm `event_parser.py` produces UTC ISO strings (or document the offset). Daily bucket math depends on this.
2. **lpagent retention window.** D-lite will surface this empirically; document in `notes/lpagent_retention.md` once confirmed.
3. **Fabriq data ingestion.** Currently manual USD values + SOL/USD price. Future API/scrape is ad-hoc and out of scope for this plan.
4. **DSc936vC-style zero-token positions.** Already tracked: lpagent excludes, we keep at PnL=0. Reconciliation should auto-classify these as "lpagent dropped — known".
5. **Wallet retirement filter.** NAV scoreboard is wallet-agnostic by design; no retirement filter needed.
6. **README/docs.** After B ships, update project README with a section explaining the two metrics (closed-PnL vs NAV-PnL) and the source-confidence ladder: lpagent < Fabriq < internal (once built). Include a one-line example for each metric. This is cheap and prevents the confusion from recurring.

---

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| lpagent API changes semantics again silently | Medium | C.1 `totalCount` assertions and daily JSONL audit catches drift; D-lite also catches it visually |
| Meteora can't expose active-position reserves (E) | Medium | Explicit feasibility spike before any E implementation; kill criterion defined; scoreboard works without E |
| Jupiter quote unavailable for illiquid tokens (E) | Medium | Fallback to bin price; mark snapshot as `degraded`; never write a zero-NAV snapshot |
| User forgets to record capital flows (A) | Medium | A.2 stale-flow guard warns at pipeline end if ledger is >14 days stale; F closes the loop automatically in Phase 2 |
| User picks scoreboard source later → historical snapshots in wrong source | Low | All sources tracked in parallel from day one; switching primary is a label change, not a backfill |
| Cache migration loses positions (C) | Low | Migration is non-destructive (old files frozen in archive/); roundtrip test required before deletion |
| Two concurrent pipeline runs corrupt JSONL (C) | Low | Atomic write via `os.replace`; detect last-line truncation on open |

---

## Recommended next steps

1. **Plan approved by user 2026-05-10.** All open questions resolved (see "Decisions confirmed" in TL;DR).
2. Invoke `design-doc-writer` to split sessions 1-2 (sub-projects A, B, D-lite) into `docs/023-*.md` design docs. Defer C, D-full, E docs until those ship.
3. Run `/verify-docs`, then `/implement` per doc.
4. **Constraint for design-doc-writer:** keep A, B, and D-lite as **three separate design docs**, not bundled. Each must be `/implement`-able and reviewable independently. Suggested file numbering: `docs/023-capital-flow-ledger.md` (A), `docs/024-portfolio-nav-scoreboard.md` (B), `docs/025-reconciliation-lite.md` (D-lite).

---

## Files this plan will produce / change

- New: `output/capital_flows.csv` (data, .gitignored)
- New: `output/portfolio_snapshots.csv` (already exists as Codex stub; column contract unchanged, now fed from capital_flows.csv)
- New: `output/portfolio_scoreboard.md` (regenerated each pipeline run)
- New: `tools/record_capital_flow.py`
- Changed: `tools/record_portfolio_snapshot.py`
- New: `valhalla/scoreboard.py` (NAV reporting — console + markdown)
- New: `valhalla/reconcile.py`
- Changed: `valhalla/lpagent_client.py` (redesign)
- Changed: `valhalla/lpagent_pipeline.py` (watermark redesign)
- Changed: `valhalla/cli.py` (new commands, scoreboard integration, stale-flow warning)
- Changed: `valhalla/charts.py` (new NAV chart, explicit labels on closed-PnL chart)
- New: `output/lpagent_cache/positions_{wallet}.jsonl` (new flat cache format)
- New: `output/lpagent_cache/archive/` (frozen legacy daily files post-migration)
- New: `notes/lpagent_api_semantics.md` (canonical reference for empirical API findings)
- New: `notes/lpagent_retention.md` (populated after D-lite runs)
- New: `notes/internal_nav_feasibility.md` (populated after E feasibility spike)
