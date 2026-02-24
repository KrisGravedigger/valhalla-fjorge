# [002] Phase B — Loss Analyzer

## Overview

Create a new module `valhalla/loss_analyzer.py` with four analysis classes that work entirely on the already-parsed `List[MatchedPosition]` data. The module produces structured analysis objects (not formatted text). It covers: risk profiling of stop-loss positions vs all trades, filter backtesting (what if we had raised the jup_score / mc / age threshold?), stop-loss level distribution (what if SL had been set tighter?), and per-wallet stop-loss trend flags.

This phase is independent of Phase A — it works on the existing columns in positions.csv / MatchedPosition. It is a prerequisite for Phase D (which calls these classes and formats the output into `loss_analysis.md`).

## Context

The existing `MatchedPosition` dataclass (in `valhalla/models.py`) already contains all fields needed:
- `close_reason`: `"stop_loss"`, `"rug"`, `"failsafe"`, `"take_profit"`, `"normal"`, `"still_open"`, `"unknown_open"`, etc.
- `pnl_pct`: `Decimal` or `None`
- `pnl_sol`: `Decimal` or `None`
- `jup_score`: `int` (0 = missing)
- `mc_at_open`: `float` (0 = missing)
- `token_age_days`: `Optional[int]`
- `target_wallet`: `str`
- `datetime_close`: `str` (ISO 8601)

`positions.csv` is already read by `valhalla/merge.py` into `MatchedPosition` objects. Phase D will pass the full list into the analyzer.

## Goals

- Implement `LossAnalyzer` — compares metric averages for stop-loss positions vs all closed positions
- Implement `FilterBacktester` — sweeps filter thresholds, calculates SOL impact of each threshold
- Implement `StopLossLevelAnalyzer` — shows distribution of losses by depth bucket, calculates SOL saved per tighter SL level
- Implement `WalletTrendAnalyzer` — per-wallet stop_loss_rate overall vs last 7 days, flags deteriorating wallets
- All classes return data objects (dicts/dataclasses), not formatted strings — formatting is Phase D's job
- No external API calls, no file I/O — pure computation on the in-memory position list

## Non-Goals

- No markdown formatting or file writing (that is Phase D)
- No source wallet comparison (that is Phase C)
- No CLI argument parsing (that is Phase D)
- No changes to existing files

## Design

### Loss Classification

Throughout this module, a position counts as a "loss" if:
```python
LOSS_REASONS = {"stop_loss", "rug", "rug_unknown_open", "failsafe", "failsafe_unknown_open"}
```

Positions with `close_reason == "still_open"` are excluded from all analysis.

Positions with `pnl_sol is None` or `pnl_pct is None` are excluded from PnL-dependent calculations but may be counted in ratios.

### Data Model for Results

```python
@dataclass
class RiskProfileRow:
    metric: str            # "jup_score", "mc_at_open", "token_age_days"
    sl_avg: Optional[float]
    all_avg: Optional[float]
    diff_pct: Optional[float]  # (sl_avg - all_avg) / all_avg * 100
    sl_count: int          # positions with valid data in stop-loss group
    all_count: int         # positions with valid data in all group

@dataclass
class BacktestRow:
    threshold: float       # e.g. 80 for jup_score >= 80
    wins_kept: int
    wins_excluded: int
    losses_avoided: int
    losses_kept: int
    net_sol_impact: Decimal   # losses_avoided_sol - wins_missed_sol
    trade_off_ratio: Optional[float]  # losses_avoided_sol / wins_missed_sol, None if wins_missed=0

@dataclass
class SLBucketRow:
    bucket_label: str      # e.g. "-5%"
    count: int             # positions at or worse than this bucket
    sol_saved: Decimal     # extra SOL saved vs current SL level
    description: str       # human-readable, e.g. "saved X SOL on Y positions"

@dataclass
class WalletFlag:
    wallet: str
    overall_sl_rate_pct: float
    recent_sl_rate_pct: float   # last 7 days
    recent_position_count: int
    flag: str   # "deteriorating" | "ok" | "insufficient_data"
    message: str  # e.g. "stop-loss rate 7d = 45% vs avg 18%"

@dataclass
class LossAnalysisResult:
    total_positions: int
    closed_positions: int      # excludes still_open
    loss_positions: int        # by LOSS_REASONS
    stop_loss_positions: int   # close_reason == "stop_loss" only
    total_pnl_sol: Decimal
    loss_pnl_sol: Decimal
    risk_profile: List[RiskProfileRow]
    backtest_results: Dict[str, List[BacktestRow]]  # param_name -> rows
    sl_buckets: List[SLBucketRow]
    wallet_flags: List[WalletFlag]
```

### Class: LossAnalyzer

```python
class LossAnalyzer:
    def analyze(self, positions: List[MatchedPosition]) -> LossAnalysisResult:
        ...

    def _risk_profile(self, stop_loss_positions: List[MatchedPosition],
                      all_closed: List[MatchedPosition]) -> List[RiskProfileRow]:
        ...
```

**Logic for risk_profile:**

For each metric in `["jup_score", "mc_at_open", "token_age_days"]`:
- Filter positions where the metric value is valid (non-zero, non-None)
  - `jup_score`: exclude if `== 0`
  - `mc_at_open`: exclude if `== 0.0`
  - `token_age_days`: exclude if `is None`
- Calculate average for stop-loss group and all-closed group
- Calculate `diff_pct = (sl_avg - all_avg) / all_avg * 100` if `all_avg != 0`

**stop_loss_positions** for risk profile = positions where `close_reason == "stop_loss"` (not rug/failsafe — those have different failure modes).

### Class: FilterBacktester

```python
class FilterBacktester:
    # Default thresholds to sweep for each parameter
    DEFAULT_THRESHOLDS = {
        "jup_score": [70, 75, 80, 85, 90],
        "mc_at_open": [1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000],
        "token_age_days": [0, 1, 2, 3, 7],
    }

    def sweep(self, positions: List[MatchedPosition],
              param: str,
              thresholds: Optional[List[float]] = None,
              direction: str = "min",   # "min" = threshold is minimum required value
              wallet: Optional[str] = None
              ) -> List[BacktestRow]:
        ...

    def sweep_all(self, positions: List[MatchedPosition],
                  wallet: Optional[str] = None
                  ) -> Dict[str, List[BacktestRow]]:
        """Run sweep() for all three default parameters."""
        ...
```

**Logic for sweep():**

1. Filter out `still_open` and `unknown_open` positions. Optionally filter by `wallet`.
2. For each threshold:
   - A position "passes" the filter if its metric value meets the threshold (and has a valid value).
   - Positions with invalid/missing metric values (jup_score=0, mc_at_open=0, token_age_days=None) always fail the filter — they would be excluded.
   - Classify each remaining position:
     - `win` = `pnl_sol > 0` and `close_reason not in LOSS_REASONS`
     - `loss` = `close_reason in LOSS_REASONS` or `pnl_sol <= 0`
   - Count: `wins_kept` (pass + win), `wins_excluded` (fail + win), `losses_avoided` (fail + loss), `losses_kept` (pass + loss)
   - `wins_missed_sol = sum(pnl_sol for wins_excluded with pnl_sol not None)`
   - `losses_avoided_sol = sum(abs(pnl_sol) for losses_avoided with pnl_sol not None)`
   - `net_sol_impact = losses_avoided_sol - wins_missed_sol`
   - `trade_off_ratio = losses_avoided_sol / wins_missed_sol` if `wins_missed_sol > 0` else `None`

**Sweet spot identification** (for Phase D to use): the row with maximum `net_sol_impact`.

### Class: StopLossLevelAnalyzer

```python
class StopLossLevelAnalyzer:
    BUCKETS = [-3, -5, -8, -10, -12, -15, -20]  # percentages (negative)

    def analyze(self, positions: List[MatchedPosition]) -> List[SLBucketRow]:
        ...
```

**Logic:**

Input: positions where `close_reason in LOSS_REASONS` and `pnl_pct is not None`.

For each bucket level B (e.g. -8%):
- "Would be saved" = positions where `pnl_pct < B` (loss was worse than B)
  - e.g., for B = -8: positions with pnl_pct < -8 (they lost more than 8%)
- `sol_saved = sum(abs(pnl_sol) - abs(pnl_sol * (B/100 / pnl_pct * 100)))`
  - Simplified: assume if SL was at B%, we exit at B% instead of pnl_pct
  - `saved_per_position = abs(pnl_sol) * (1 - B / float(pnl_pct))` — how much less we'd lose
  - `sol_saved = sum(saved_per_position for qualifying positions)`
- `description = f"If SL at {B}%: saved {sol_saved:.3f} SOL on {count} position(s)"`

Note: The current SL level is whatever the bot uses (unknown from logs). The analysis is relative: "if you had exited at B%, you would have saved X compared to actual outcome."

### Class: WalletTrendAnalyzer

```python
class WalletTrendAnalyzer:
    RECENT_DAYS = 7
    MIN_POSITIONS_FOR_FLAG = 3   # minimum recent positions to issue a flag

    def analyze(self, positions: List[MatchedPosition],
                reference_date: Optional[datetime] = None
                ) -> List[WalletFlag]:
        ...
```

**Logic:**

1. If `reference_date` is None, derive it as the max `datetime_close` across all positions.
2. Group closed positions by `target_wallet`.
3. For each wallet:
   - `overall_sl_rate = count(loss) / count(closed)` (using `LOSS_REASONS`)
   - `recent_positions` = those with `datetime_close` within last 7 days of reference_date
   - `recent_sl_rate = count(loss in recent) / count(recent)` if `len(recent) >= MIN_POSITIONS_FOR_FLAG`
   - Flag logic:
     - `"deteriorating"` if `recent_sl_rate > overall_sl_rate * 1.5` and `recent_sl_rate > 0.3` (>30% recent loss rate) and enough recent data
     - `"insufficient_data"` if fewer than `MIN_POSITIONS_FOR_FLAG` recent positions
     - `"ok"` otherwise
4. Return only wallets with `flag == "deteriorating"` unless called with `include_all=True` (for summary stats).

### Edge Cases

- **`pnl_sol is None`**: Do not include in SOL-based calculations. Count the position in ratios if `close_reason` is known.
- **`jup_score == 0`**: Treat as missing. Do not include in jup_score averages or filter sweeps for jup_score parameter.
- **`mc_at_open == 0.0`**: Treat as missing. Same handling as jup_score.
- **`token_age_days is None`**: Treat as missing.
- **Zero losses**: If there are no stop-loss positions, `LossAnalyzer.analyze()` still returns a valid `LossAnalysisResult` with zeroed counters and empty lists.
- **`close_reason == "rug"` and `"failsafe"`**: These are counted as losses in all calculations (per PLAN.md: "traktuj jako loss").
- **`close_reason == "stop_loss_unknown_open"`**: Also count as loss (stop_loss is in the reason string).
- **Decimal arithmetic**: All SOL sums use `Decimal`. Averages for float metrics (jup_score, mc, age) can use plain `float` — these are for display/comparison only, not financial calculations.

## Implementation Plan

1. **Create `/c/nju/ai/claude/projects/IaaS/valhalla-fjorge/valhalla/loss_analyzer.py`**
   - Add all result dataclasses at the top
   - Implement `LossAnalyzer`
   - Implement `FilterBacktester`
   - Implement `StopLossLevelAnalyzer`
   - Implement `WalletTrendAnalyzer`
   - Add module-level constant `LOSS_REASONS`

2. No changes to any existing files in this phase.

## Dependencies

- Independent of Phase A. Works on existing `MatchedPosition` fields.
- External: no new libraries — stdlib only (`dataclasses`, `decimal`, `datetime`, `typing`, `statistics`).

## Testing

1. Unit test with a synthetic list of positions covering:
   - Mix of stop_loss, rug, take_profit, normal, still_open close_reasons
   - Some positions with `pnl_sol = None`
   - Some positions with `jup_score = 0`
2. `LossAnalyzer`: verify that `stop_loss_positions` count matches expected, `risk_profile` diff_pct has correct sign
3. `FilterBacktester.sweep()`: at threshold=0 (nothing excluded), `wins_excluded + losses_avoided == 0`; at threshold=999 (everything excluded), `wins_kept + losses_kept == 0`
4. `StopLossLevelAnalyzer`: for a position with `pnl_pct = -15`, bucket `-8` should show SOL saved; bucket `-20` should not include it
5. `WalletTrendAnalyzer`: wallet with 4 recent losses out of 4 recent positions and overall rate 20% should be flagged as "deteriorating"
6. Run against real `output/positions.csv` data via a quick script:
   ```python
   from valhalla.merge import load_positions_from_csv  # or read via csv.DictReader
   from valhalla.loss_analyzer import LossAnalyzer
   result = LossAnalyzer().analyze(positions)
   print(result.total_positions, result.loss_positions)
   ```

## Alternatives Considered

- **Returning formatted strings from the analyzer**: Rejected — separating computation from presentation makes Phase D easier to test and modify independently.
- **Single monolithic `LossAnalyzer` class with all methods**: The PLAN.md explicitly names four separate classes. Separate classes make each unit independently testable.
- **Using pandas for analysis**: Rejected — the project uses only stdlib. The data volumes are small (hundreds of positions) and do not justify a new dependency.

## Open Questions

- Should `stop_loss_unknown_open` and similar compound reasons be included in the stop-loss group for risk profiling? They lack `jup_score`/`mc_at_open` data (those fields are 0 for unknown_open). They will naturally be excluded from metric averages due to the zero-value filtering. Suggested: include in loss counts, exclude from metric averages.
