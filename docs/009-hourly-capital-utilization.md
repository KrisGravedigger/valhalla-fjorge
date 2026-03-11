# [009] Hourly Capital Utilization

## Overview
Add a new module `valhalla/utilization.py` that computes how much SOL is locked
in active positions hour by hour over the last 72 hours. The result is rendered
as `output/hourly_utilization.png` and used to emit an action-item suggestion
when capital sits idle for too long. This gives the operator a quick visual
answer to: "Is my money working, or sitting unused?"

## Context
The pipeline already produces `output/positions.csv` and several PNG charts via
`valhalla/charts.py`. The constant `PORTFOLIO_TOTAL_SOL` (currently `54.0`) is
defined in `valhalla/analysis_config.py`. The chart pipeline is wired in
`valhalla_parser_v2.py` at line ~2293:

```python
generate_charts(matched_positions, str(output_dir))
generate_insufficient_balance_chart(str(insuf_csv), str(output_dir))
```

The action-item system lives in `_build_action_items()` (~line 662) which
already checks wallet scorecards. `InsufficientBalanceEvent` is parsed from
Discord logs and passed to `_build_action_items()` as
`insufficient_balance_events`.

Relevant existing code:
- `valhalla/models.py`: `MatchedPosition`, `InsufficientBalanceEvent`,
  `parse_iso_datetime()`
- `valhalla/loss_analyzer.py`: `WalletScorecard` (status field, including
  `"increase_capital"`)
- `valhalla/analysis_config.py`: `PORTFOLIO_TOTAL_SOL`
- `valhalla/charts.py`: `HAS_MATPLOTLIB`, `generate_charts()` — established
  pattern for new chart functions

## Goals
- Implement `valhalla/utilization.py` with a pure function that computes hourly
  SOL utilization from a list of `MatchedPosition` objects.
- Generate `output/hourly_utilization.png` — a single aggregate line chart with
  a reference line at `PORTFOLIO_TOTAL_SOL`, 72-hour lookback.
- Add a new suggestion to action items when: utilization < 80% for 3
  consecutive days AND <= 10 insufficient-balance events in the last 24 hours.
- The suggestion names only wallets whose scorecard status is
  `"increase_capital"`.
- Add 4 new constants to `analysis_config.py`.
- Wire the chart and the action-item check into `valhalla_parser_v2.py`.

## Non-Goals
- Per-wallet utilization breakdown (single aggregate line only).
- Real-time / streaming updates (batch pipeline only).
- Changing how existing charts are generated.
- Changing `positions.csv` format.

## Design

### Algorithm: `compute_hourly_utilization()`

A position is "active" at hour H when:
```
parse_iso_datetime(datetime_open) <= H
AND (parse_iso_datetime(datetime_close) > H  OR  close_reason == "still_open")
```

Steps:
1. Build a list of 72 hour-buckets: `now - 71h, now - 70h, ..., now` (i.e.
   the start of each hour in UTC-local time, floored to the hour).
2. For each hour H, sum `sol_deployed` (as `Decimal`) over all positions that
   are active at H. Skip positions where `sol_deployed is None`.
3. Return a list of `(datetime, Decimal)` tuples sorted oldest to newest.

Positions to include: ALL positions in the input list, including
`close_reason == "still_open"`. Positions with no parseable `datetime_open`
are skipped.

### Data Model

```python
# valhalla/utilization.py

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Tuple

@dataclass
class HourlyUtilizationPoint:
    hour: datetime          # start of the hour (floored)
    sol_active: Decimal     # sum of sol_deployed for active positions at this hour
```

### Pure computation function

```python
def compute_hourly_utilization(
    positions: List["MatchedPosition"],
    lookback_hours: int = 72,
    reference_time: Optional[datetime] = None,
) -> List[HourlyUtilizationPoint]:
    """
    Compute SOL locked in active positions for each hour in the lookback window.

    Args:
        positions: All MatchedPosition objects (including still_open).
        lookback_hours: Number of hours to look back (default 72).
        reference_time: End of the window. Defaults to datetime.now() floored
                        to the current hour.

    Returns:
        List of HourlyUtilizationPoint sorted oldest-first.
    """
```

Implementation detail: "floor to hour" means
`dt.replace(minute=0, second=0, microsecond=0)`.

### Three-day utilization check

A helper that answers: "Was utilization < 80% of PORTFOLIO_TOTAL_SOL for 3
consecutive full days?"

```python
def check_low_utilization_days(
    points: List[HourlyUtilizationPoint],
    portfolio_sol: Decimal,
    threshold_pct: float,      # e.g. 0.80
    consecutive_days: int,     # e.g. 3
) -> bool:
    """
    Returns True if the last `consecutive_days` full calendar days each had
    average hourly utilization below threshold_pct * portfolio_sol.

    A "full calendar day" = a day for which all 24 hours appear in points.
    If fewer than `consecutive_days` full days exist, returns False.
    """
```

Steps inside:
1. Group points by calendar date.
2. Keep only dates with exactly 24 data points.
3. For each such date, compute `avg_sol = mean(point.sol_active)`.
4. Collect the most recent `consecutive_days` dates.
5. Return `True` if all have `avg_sol < threshold_pct * portfolio_sol`.

### Chart: `generate_utilization_chart()`

```python
def generate_utilization_chart(
    points: List[HourlyUtilizationPoint],
    portfolio_sol: Decimal,
    output_dir: str,
) -> None:
    """
    Save hourly_utilization.png to output_dir.
    No-ops if matplotlib is not installed or points is empty.
    """
```

Visual spec:
- X-axis: datetime labels formatted as `"MM-DD HH:00"`, 72 ticks (one per
  hour), rotated 45 degrees, thinned to show one label every 6 hours to avoid
  crowding.
- Y-axis: SOL amount, label `"SOL in Active Positions"`.
- Main line: thick (`linewidth=2.5`), solid, color `"#1f77b4"` (same blue as
  first wallet in tab10 palette).
- Area fill: `fill_between()` with `alpha=0.15`, `color="lightblue"`.
- Reference line: red dashed (`color="red"`, `linestyle="--"`,
  `linewidth=1.5`), horizontal at `float(portfolio_sol)`. Label:
  `f"Portfolio total: {portfolio_sol:.1f} SOL"`.
- Title: `"Hourly Capital Utilization (last 72h)"`.
- Grid: `alpha=0.3`, `linestyle=":"`.
- Save as `{output_dir}/hourly_utilization.png`, `dpi=150`, `bbox_inches="tight"`.

### New constants in `analysis_config.py`

Add a new section after `LOSS_DETAIL_LOOKBACK_DAYS`:

```python
# ---------------------------------------------------------------------------
# Hourly Capital Utilization (Doc 009)
# ---------------------------------------------------------------------------

# Lookback window for the utilization chart (hours).
UTILIZATION_LOOKBACK_HOURS: int = 72

# Utilization below this fraction of PORTFOLIO_TOTAL_SOL triggers a suggestion.
# 0.80 = 80% of portfolio must be deployed to be considered "well utilized".
UTILIZATION_LOW_THRESHOLD: float = 0.80

# Number of consecutive days below threshold before triggering the suggestion.
UTILIZATION_CONSECUTIVE_DAYS: int = 3

# Maximum insufficient-balance events in the last 24 hours before suppressing
# the "increase capital" suggestion (if SOL is thin, don't suggest deploying more).
UTILIZATION_MAX_INSUF_EVENTS_24H: int = 10
```

### Action-item suggestion

Inside `_build_action_items()` in `valhalla_parser_v2.py`, after the existing
`insuf_items` block, add a utilization check block:

```python
# Utilization-based suggestion (Doc 009)
utilization_items: List[str] = []
if PORTFOLIO_TOTAL_SOL > 0:
    from valhalla.utilization import (
        compute_hourly_utilization, check_low_utilization_days,
    )
    util_points = compute_hourly_utilization(positions, UTILIZATION_LOOKBACK_HOURS)
    low_util = check_low_utilization_days(
        util_points,
        Decimal(str(PORTFOLIO_TOTAL_SOL)),
        UTILIZATION_LOW_THRESHOLD,
        UTILIZATION_CONSECUTIVE_DAYS,
    )
    if low_util:
        # Count insuf-balance events in last 24h
        insuf_24h = 0
        if insufficient_balance_events:
            cutoff = datetime.now() - timedelta(hours=24)
            for ev in insufficient_balance_events:
                ev_dt = parse_iso_datetime(
                    make_iso_datetime(ev.date, ev.timestamp) if ev.date else ev.timestamp
                )
                if ev_dt and ev_dt >= cutoff:
                    insuf_24h += 1
        if insuf_24h <= UTILIZATION_MAX_INSUF_EVENTS_24H:
            # Find wallets with status "increase_capital"
            ic_wallets = [
                sc.wallet for sc in result.wallet_scorecards
                if sc.status == "increase_capital"
            ]
            if ic_wallets:
                utilization_items.append(
                    f"Portfolio: capital utilization below "
                    f"{UTILIZATION_LOW_THRESHOLD*100:.0f}% for "
                    f"{UTILIZATION_CONSECUTIVE_DAYS} consecutive days — "
                    f"consider increasing capital per position for: "
                    + ", ".join(ic_wallets)
                )
```

Insert `utilization_items` into the `all_items` assembly line, after
`insuf_items` and before `filter_recs`:

```python
all_items = (
    size_guard_items + replacing + increasing + reduce_capital_items
    + insuf_items + utilization_items + filter_recs
    + inactive_items + deteriorating_recs + other_recs
)
```

Also update `_scorecard_action_hints()` to recognize the new text:
```python
elif "capital utilization below" in il:
    if "↑ capital (util)" not in hints:
        hints.append("↑ capital (util)")
```

### Wiring into `valhalla_parser_v2.py` chart pipeline

At line ~2293, after the existing `generate_insufficient_balance_chart()` call:

```python
# Doc 009: Hourly capital utilization chart
if PORTFOLIO_TOTAL_SOL > 0:
    from valhalla.utilization import (
        compute_hourly_utilization, generate_utilization_chart,
    )
    util_points = compute_hourly_utilization(matched_positions, UTILIZATION_LOOKBACK_HOURS)
    generate_utilization_chart(util_points, Decimal(str(PORTFOLIO_TOTAL_SOL)), str(output_dir))
    print(f"  Saved: {output_dir}/hourly_utilization.png")
```

Add the import at the top of the `if not args.skip_charts` block rather than
at module level (consistent with local-import pattern used for
`InsufficientBalanceAnalyzer`).

Also add the new config imports to the existing import block at the top of
`valhalla_parser_v2.py` (~line 21):

```python
from valhalla.analysis_config import (
    ...
    UTILIZATION_LOOKBACK_HOURS,
    UTILIZATION_LOW_THRESHOLD,
    UTILIZATION_CONSECUTIVE_DAYS,
    UTILIZATION_MAX_INSUF_EVENTS_24H,
)
```

## Implementation Plan

1. **`valhalla/analysis_config.py`** — Add the 4 new constants under a new
   `# Hourly Capital Utilization` section at the bottom of the file.

2. **`valhalla/utilization.py`** (new file) — Create with:
   - `HourlyUtilizationPoint` dataclass
   - `compute_hourly_utilization()` function
   - `check_low_utilization_days()` function
   - `generate_utilization_chart()` function (with `HAS_MATPLOTLIB` guard,
     same pattern as `valhalla/charts.py`)

3. **`valhalla_parser_v2.py`** — Add 4 new config imports to the import block
   at the top of the file.

4. **`valhalla_parser_v2.py`** — Inside `_build_action_items()`: add the
   utilization check block after `insuf_items` and update `all_items` assembly.

5. **`valhalla_parser_v2.py`** — Inside `_scorecard_action_hints()`: add
   recognition of `"capital utilization below"` text.

6. **`valhalla_parser_v2.py`** — Inside the `if not args.skip_charts` block
   (~line 2291): add the chart generation call after
   `generate_insufficient_balance_chart()`.

7. **Manual test** — Run `python valhalla_parser_v2.py --report` and verify:
   - `output/hourly_utilization.png` is created.
   - No crash when `PORTFOLIO_TOTAL_SOL == 0` (chart skipped, action item
     skipped).
   - Utilization suggestion appears in `output/loss_analysis.md` if conditions
     are met.

## Dependencies
- **Requires [008-loss-analysis-enhancements]**: `PORTFOLIO_TOTAL_SOL` constant
  already added by doc 008, so the constant exists. Doc 009 adds 4 more
  constants but does NOT change any existing ones.
- **Independent from [007-report-restructure]**: the action-item suggestion
  just appends to the `utilization_items` list before `all_items` is assembled;
  the report structure itself is not changed by this doc.

## Testing

Manual verification steps:
1. Run parser with `PORTFOLIO_TOTAL_SOL = 54.0` (already set):
   `python valhalla_parser_v2.py --report`
   Check that `output/hourly_utilization.png` exists and is not empty.

2. Open the chart. Confirm:
   - X-axis has date-hour labels.
   - Red dashed line at 54.0.
   - Blue line with light blue fill.

3. Set `PORTFOLIO_TOTAL_SOL = 0.0` temporarily, re-run. Confirm chart is NOT
   generated and no crash.

4. To test the action-item suggestion: temporarily lower
   `UTILIZATION_LOW_THRESHOLD` to `1.10` (above any realistic value) so the
   condition always triggers. Confirm the suggestion appears in
   `output/loss_analysis.md` for wallets with `increase_capital` status.

5. To test the insuf-balance suppression: set
   `UTILIZATION_MAX_INSUF_EVENTS_24H = 0`. Confirm no suggestion even when
   utilization is low.

## Alternatives Considered
- **Per-wallet lines instead of aggregate**: Rejected — 19 wallets on one chart
  would be unreadable. Aggregate answers the actual question (total idle SOL).
- **Daily average instead of hourly**: Rejected — loses intraday patterns;
  hourly is cheap to compute and gives richer chart.
- **Separate script instead of module**: Rejected — adding to the existing
  pipeline keeps a single entry point and avoids duplicate file-loading logic.

## Open Questions
- None. All design decisions resolved.
