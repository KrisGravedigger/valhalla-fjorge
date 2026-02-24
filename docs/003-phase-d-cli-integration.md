# [003] Phase D — CLI Integration and loss_analysis.md Report

## Overview

Wire the loss analysis classes (Phase B) into `valhalla_parser_v2.py` so that every normal run automatically generates `output/loss_analysis.md`. Add a `--backtest` CLI flag for on-demand filter sweeps with custom thresholds. Add a `--no-loss-analysis` flag to skip the report. The report is generated from already-computed `MatchedPosition` data — no new API calls.

## Context

After Phases A and B:
- `valhalla/loss_analyzer.py` exists with `LossAnalyzer`, `FilterBacktester`, `StopLossLevelAnalyzer`, `WalletTrendAnalyzer`, and their result dataclasses
- `valhalla_parser_v2.py` already has a `main()` function that, at the end of a run, holds `matched_positions` and `unmatched_opens`
- The report must be generated after Step 5.6 (CSV merge) so it uses the complete merged dataset

The report format is Markdown readable in a terminal. Tables use `|---|---|` separators. The file is always created, even if there are zero stop-loss positions (it will contain a "no stop-loss positions found" note).

## Goals

- Generate `output/loss_analysis.md` automatically on every normal parse run
- `--backtest` flag: accepts `param=value` pairs, runs FilterBacktester with those thresholds, outputs to terminal (does not write file separately)
- `--no-loss-analysis` flag: skips report generation
- Report sections: Overview, Risk Profile, Filter Backtest, SL Distribution, Wallet Flags, Source Wallet section placeholder (if Phase A data available)
- `--backtest` can optionally be combined with `--wallet WALLET_ID` to filter to one wallet

## Non-Goals

- No changes to the loss_analyzer.py module (that is Phase B)
- No source wallet analysis execution (that is Phase C) — only a placeholder section in the report if `target_tx_signature` data is present (from Phase A)
- No new API calls or file reads during report generation

## Design

### New CLI Arguments

```python
parser.add_argument('--backtest', nargs='+', metavar='PARAM=VALUE',
    help='Run filter backtest with custom thresholds. '
         'E.g.: --backtest jup_score=80 mc=5000000 age=1')
parser.add_argument('--wallet', metavar='WALLET_ID',
    help='Filter --backtest to a specific wallet alias')
parser.add_argument('--no-loss-analysis', action='store_true',
    help='Skip loss analysis report generation')
```

`--backtest` parsing: each item is `"param=value"`. Supported param names:
- `jup_score` → maps to `FilterBacktester.sweep(param="jup_score", thresholds=[value])`
- `mc` or `mc_at_open` → maps to `param="mc_at_open"`
- `age` or `token_age` → maps to `param="token_age_days"`

Multiple params produce multiple separate sweep tables in terminal output.

### Report Generation: new function

Add a new function in `valhalla_parser_v2.py`:

```python
def _generate_loss_report(
    positions: List[MatchedPosition],
    output_path: str,
    backtest_wallet: Optional[str] = None
) -> None:
    """Generate loss_analysis.md from matched positions."""
    from valhalla.loss_analyzer import (
        LossAnalyzer, FilterBacktester, StopLossLevelAnalyzer,
        WalletTrendAnalyzer
    )
    ...
```

This function is called after the CSV merge step (Step 5.6), before chart generation.

### Report Structure

```
# Loss Analysis Report
Generated: YYYY-MM-DD HH:MM

## Overview

| Metric | Value |
|--------|-------|
| Total positions (closed) | N |
| Stop-loss exits | N |
| Rug / failsafe | N |
| Total loss PnL | -X.XXXX SOL |
| Loss rate | XX.X% |

## Risk Profile: Stop-Loss vs All Trades

[section header explaining what this shows]

| Metric | Stop-Loss Avg | All Trades Avg | Difference |
|--------|---------------|----------------|------------|
| jup_score | 72 | 84 | -14.3% |
| mc_at_open | $6.5M | $18.2M | -64.3% |
| token_age_days | 1.2d | 5.8d | -79.3% |

[note if insufficient data]

## Filter Backtest

[3 tables: one per parameter (jup_score, mc_at_open, token_age_days)]

### jup_score (minimum threshold)

| Threshold | Wins Kept | Wins Excl. | Losses Avoided | Losses Kept | Net SOL Impact |
|-----------|-----------|-----------|----------------|-------------|----------------|
| >= 70 | ... | ... | ... | ... | ... |
| >= 75 | ... | ... | ... | ... | ← sweet spot |
...

[sweet spot row marked with "← sweet spot"]

### mc_at_open (minimum threshold)
[same table structure]

### token_age_days (minimum threshold)
[same table structure]

## Stop-Loss Level Distribution

If your stop-loss had been set tighter:

| SL Level | Positions Below | SOL Saved vs Actual |
|----------|----------------|---------------------|
| -3% | N | X.XXX SOL |
...

## Wallet Stop-Loss Flags

[if no flags]
No wallets flagged (all within normal stop-loss rates).

[if flags exist]
| Wallet | Overall SL Rate | Recent 7d SL Rate | Recent Positions | Flag |
|--------|----------------|-------------------|-----------------|------|
| 20260125_... | 18% | 45% | 8 | deteriorating |

## Source Wallet Comparison

[if Phase A data not available — target_tx_signature column empty]
No source wallet data available. Run Phase C (source_wallet_analyzer) to enable this section.

[if Phase A data available but Phase C not run]
N positions have target transaction signatures available.
Run `python valhalla_parser_v2.py --analyze-source` to populate source wallet comparison.
```

### Formatting Helpers

Add private formatting functions inside `valhalla_parser_v2.py`:

```python
def _fmt_sol(val: Optional[Decimal]) -> str:
    """Format SOL value or return 'N/A'"""
    return f"{val:.4f} SOL" if val is not None else "N/A"

def _fmt_pct(val: Optional[float]) -> str:
    return f"{val:+.1f}%" if val is not None else "N/A"

def _fmt_mc(val: float) -> str:
    """Format market cap: 1_500_000 -> '$1.5M'"""
    if val >= 1_000_000:
        return f"${val/1_000_000:.1f}M"
    elif val >= 1_000:
        return f"${val/1_000:.0f}K"
    return f"${val:.0f}"

def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    """Render a markdown table string."""
    ...
```

### Backtest mode (--backtest flag)

When `--backtest` is passed, run FilterBacktester with the given thresholds and print results to terminal. Do NOT skip normal parse and report generation — `--backtest` is additive.

```python
if args.backtest:
    _run_custom_backtest(matched_positions, args.backtest, args.wallet)
```

```python
def _run_custom_backtest(positions, param_value_strs, wallet=None):
    from valhalla.loss_analyzer import FilterBacktester
    backtester = FilterBacktester()
    PARAM_ALIASES = {
        'mc': 'mc_at_open',
        'age': 'token_age_days',
        'token_age': 'token_age_days',
        'jup': 'jup_score',
    }
    for pv in param_value_strs:
        try:
            param_raw, value_str = pv.split('=', 1)
            param = PARAM_ALIASES.get(param_raw, param_raw)
            threshold = float(value_str)
        except ValueError:
            print(f"  Warning: could not parse --backtest argument '{pv}', expected param=value")
            continue
        rows = backtester.sweep(positions, param, thresholds=[threshold], wallet=wallet)
        _print_backtest_table(param, rows)
```

### Integration into main() flow

The call site in `main()` goes after Step 5.6 (CSV merge) and before chart generation:

```python
# Step 6.5b: Generate loss analysis report
if not args.no_loss_analysis:
    loss_report_path = output_dir / 'loss_analysis.md'
    print(f"\nGenerating loss analysis report...")
    try:
        _generate_loss_report(matched_positions, str(loss_report_path))
        print(f"  {loss_report_path}")
    except Exception as e:
        print(f"  Warning: loss analysis failed: {e}")

# --backtest custom run (additive, prints to terminal)
if hasattr(args, 'backtest') and args.backtest:
    _run_custom_backtest(matched_positions, args.backtest,
                         getattr(args, 'wallet', None))
```

The `try/except` wrapper ensures that any bug in the analysis code does not break the main parse run.

### mc_at_open formatting in report

The `mc_at_open` values in positions.csv are raw floats (e.g. `1047047.227`). The report uses `_fmt_mc()` to display them as `$1.0M` etc.

The backtest table for `mc_at_open` thresholds shows the threshold itself formatted the same way.

### Source Wallet placeholder logic

Check if any `MatchedPosition` in the dataset has a non-empty `target_tx_signature` (Phase A field). This check is safe regardless of whether Phase A has been run — if the field doesn't exist on older MatchedPosition objects, use `getattr(pos, 'target_tx_signature', None)`.

```python
has_target_tx = any(
    getattr(pos, 'target_tx_signature', None)
    for pos in positions
    if pos.close_reason in LOSS_REASONS
)
```

## Implementation Plan

1. **`valhalla_parser_v2.py`** — add `--backtest`, `--wallet`, `--no-loss-analysis` arguments to `argparse` section

2. **`valhalla_parser_v2.py`** — add helper functions: `_fmt_sol`, `_fmt_pct`, `_fmt_mc`, `_md_table`

3. **`valhalla_parser_v2.py`** — implement `_generate_loss_report()` function: instantiate all four analyzer classes, run them, format each section, write file

4. **`valhalla_parser_v2.py`** — implement `_run_custom_backtest()` and `_print_backtest_table()`

5. **`valhalla_parser_v2.py`** — insert the two call sites in `main()` at the correct position (after step 5.6, before charts)

## Dependencies

- **Requires [002] Phase B** — `valhalla/loss_analyzer.py` must exist
- Phase A (001) is NOT required for this phase, but the report will include Phase A placeholder section only if Phase A columns are present

## Testing

1. `python valhalla_parser_v2.py` → `cat output/loss_analysis.md` → check all 5 sections are present, tables are valid Markdown
2. If no stop-loss positions: report generates without error, shows "0 stop-loss exits" in overview
3. `python valhalla_parser_v2.py --backtest jup_score=85` → terminal shows a backtest table; `wins_excluded + wins_kept == total_wins` (sanity check)
4. `python valhalla_parser_v2.py --backtest jup_score=80 --wallet 20260125_C5JXfmK` → table shows only that wallet's positions
5. `python valhalla_parser_v2.py --no-loss-analysis` → no `output/loss_analysis.md` created; normal CSV generation completes
6. Regression: `output/positions.csv` and `output/summary.csv` are not changed by adding `--backtest`

## Alternatives Considered

- **Separate script `loss_report.py`**: Rejected — PLAN.md specifies auto-generation on every run, which means integration into `valhalla_parser_v2.py` main flow.
- **Writing backtest output to a separate file**: Rejected — the `--backtest` flag is intended for interactive use, terminal output is more useful.

## Open Questions

- Should the report be regenerated during the Meteora retry loop (end of main)? Probably yes — positions may get updated PnL. The current design wraps `_generate_loss_report` in a try/except so a second call at the end of the retry block is safe.
- Should `--wallet` also filter the main report (not just `--backtest`)? Current design: no — it only filters `--backtest`. The main report always covers all wallets.
