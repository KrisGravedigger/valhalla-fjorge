# PnL Discrepancy Analysis: valhalla-fjorge vs lpagent

## TL;DR

After full cross-reference of 1196 matched positions (Feb 15-27, 2026):

- **Fees: MATCH** - 0.1% difference (23.45 vs 23.42 SOL)
- **PnL after bug fixes**: 1.18 SOL remaining difference (excl. DSc936vC)
- **Root cause**: different token-to-SOL valuation methodology (not yet fully understood)
- **79% of positions match within 0.001 SOL** per-position

---

## What We Know

### Our calculation method

We use the **Meteora DLMM API** (`/position/{addr}/deposits`, `/withdraws`, `/claim_fees`).
Each transaction returns `token_x_usd_amount` and `token_y_usd_amount` - USD values
presumably based on the DLMM bin price at time of transaction.

We derive SOL price from the SOL-side USD (`sol_price = sol_usd / sol_amount`), then
convert the token-side USD to SOL equivalent (`token_sol_equiv = token_usd / sol_price`).

PnL = withdrawn_sol_equiv + fees_sol_equiv - deposited_sol_equiv

### What lpagent uses

Unknown. Their PnL values are **stable** (confirmed by observation - they do not change
on page refresh or over time). So they use a fixed price, but from a different source
or methodology than the DLMM API USD amounts we use.

### On-chain swap verification (Feb 21)

For 2 positions, we fetched the actual swap TX from Solana RPC:

| Position | Our price (DLMM API) | Actual swap price | Ratio |
|---|---|---|---|
| 3KSCw7vD (automaton) | 1.064e-05 | 1.047e-05 | our is 1.6% higher |
| 5QYhYU6GD5 (Punch) | 3.547e-04 | 3.496e-04 | our is 1.5% higher |

Our DLMM-API-derived prices are slightly above actual swap prices (slippage).

---

## Full Cross-Reference (2026-02-27)

### Dataset

| | Count | Notes |
|---|---|---|
| lpagent CSV | 1212 | Extracted via browser script, Feb 15 - Feb 27 |
| Our positions.csv | 1461 | Feb 11 - Feb 27 |
| Matched | 1196 | By position address prefix+suffix |
| lpagent only | 16 | Very recent, not yet in our data |
| Our data only | 243 | Feb 11-14 (older than lpagent CSV retention) |

### Final Numbers (1196 matched positions)

| Metric | lpagent (from %) | Ours | Diff |
|---|---|---|---|
| **Fees** | 23.42 SOL | 23.45 SOL | +0.03 SOL (0.1%) |
| **PnL (excl DSc936vC)** | 5.40 SOL | 6.58 SOL | **+1.18 SOL** |

### Per-Position Accuracy

| Difference range | Count | % |
|---|---|---|
| < 0.001 SOL | 941 | 79% |
| 0.001 - 0.01 SOL | 189 | 16% |
| 0.01 - 0.05 SOL | 54 | 5% |
| > 0.05 SOL | 12 | 1% |

Direction: 184 positions we're higher, 70 lower, 942 close match.

---

## Key Position Comparisons

All positions have matching fees but divergent PnL. The difference is in how
the token side is valued in SOL equivalents.

### Positions where we are HIGHER than lpagent

| Position | Token | Our PnL | lp PnL | Diff | Notes |
|---|---|---|---|---|---|
| CL6N3Ve3 | Tastecoin | +0.3335 | +0.1294 | +0.2041 | Even deposits differ (5.35 vs 5.46) |
| BvapUjjU | Lobstar | -0.4192 | -0.5164 | +0.0972 | Token-only withdrawal |
| 3KSCw7vD | automaton | -0.5991 | -0.6708 | +0.0717 | On-chain verified |
| 5etffR8f | automaton | -0.0275 | -0.0781 | +0.0506 | BidAsk, mixed withdrawal |
| ABZp12yw | TOTO | -0.0696 | -0.1145 | +0.0449 | Spot, token+SOL withdrawal |

### Positions where we are LOWER than lpagent

| Position | Token | Our PnL | lp PnL | Diff | Notes |
|---|---|---|---|---|---|
| 9Q3Tk7PW | MUSHU | -0.4070 | -0.3157 | -0.0913 | BidAsk, token-only withdrawal |
| Azsxa6HU | Jellycat | -0.3584 | -0.2797 | -0.0787 | BidAsk, token-only withdrawal |

Notably, positions where we're LOWER are both BidAsk strategy with token-only
withdrawals. Our valuation of those tokens comes out lower than lpagent's.

---

## Bugs Found and Fixed

### Bug #1: Phantom Deposit (token-only first deposit)

When the first deposit is token-only (0 SOL), `running_sol_price` starts at 0, so
the token USD can't be converted to SOL. Result: deposit = 0, inflating PnL.

**Fix**: Pre-scan all transactions to find initial SOL price before processing.

After fix + recalculation of 14 pending positions: our total dropped from 8.39 to 6.58 SOL.
3 positions remain unfixable (manually overridden with lpagent estimates,
`pnl_source=lpagent_estimate`).

### DSc936vC (Lobstar, 3.0 SOL)

Meteora API: 3.0 SOL in, 3.0 SOL out, 0 tokens, PnL = 0.
lpagent shows -3.0 (100% loss) but excludes it from displayed total.
Both tools skip this position. Not a real discrepancy.

---

## Open Questions

1. **What price source does lpagent use?** Not DLMM API bin price (we'd match).
   Possibly Jupiter aggregator, CoinGecko, or actual swap quotes.

2. **Why is CL6N3Ve3 deposit different?** The third deposit includes tokens.
   Our token valuation = 1.539 SOL, lpagent implies 1.654 SOL (+7.5%).
   Same tokens, different SOL equivalent.

3. **Why are some positions lower?** 9Q3Tk7PW and Azsxa6HU show us LOWER than
   lpagent, meaning our token valuation is sometimes below lpagent's.
   Both are BidAsk with token-only withdrawals.

4. **Is the DLMM API bin price the right price?** On-chain verification shows
   actual swap prices are 1-2% below DLMM bin prices (slippage), but the
   per-position PnL differences often exceed 1-2%.

---

## Archived: Earlier Hypotheses

### Mark-to-market hypothesis (Feb 21, first session)

Initially concluded that lpagent uses current market prices (mark-to-market) that
change daily. This was inferred from 4 positions where tokens had appreciated since
close, and lpagent showed higher PnL. **Disproven**: user confirmed lpagent values
are stable and do not change over time. The explanation was circumstantial.

### Revised mark-to-market (Feb 27, second session)

Relabeled the remaining difference as "mark-to-market" without verifying. This was
incorrect - "mark-to-market" implies fluctuating values, which contradicts user
observation. The correct label is "different token valuation methodology" - both
tools use fixed prices, just from different sources.
