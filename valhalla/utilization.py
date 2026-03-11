"""
Hourly capital utilization module for Valhalla parser (Doc 009).

Computes how much SOL is locked in active positions hour by hour
over a configurable lookback window and generates a chart.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional

# Optional matplotlib for chart generation (same pattern as charts.py)
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


@dataclass
class HourlyUtilizationPoint:
    hour: datetime      # start of the hour (floored to minute=0, second=0, microsecond=0)
    sol_active: Decimal # sum of sol_deployed for active positions at this hour


def compute_hourly_utilization(
    positions: List,
    lookback_hours: int = 72,
    reference_time: Optional[datetime] = None,
) -> List[HourlyUtilizationPoint]:
    """
    Compute SOL locked in active positions for each hour in the lookback window.

    A position is "active" at hour H when:
        parse_iso_datetime(datetime_open) <= H
        AND (parse_iso_datetime(datetime_close) > H  OR  close_reason == "still_open")

    Args:
        positions: All MatchedPosition objects (including still_open).
        lookback_hours: Number of hours to look back (default 72).
        reference_time: End of the window. Defaults to datetime.now() floored
                        to the current hour.

    Returns:
        List of HourlyUtilizationPoint sorted oldest-first.
    """
    from valhalla.models import parse_iso_datetime

    if reference_time is None:
        now = datetime.now()
        reference_time = now.replace(minute=0, second=0, microsecond=0)
    else:
        reference_time = reference_time.replace(minute=0, second=0, microsecond=0)

    # Build list of hour-bucket start times: oldest to newest
    hours = [
        reference_time - timedelta(hours=(lookback_hours - 1 - i))
        for i in range(lookback_hours)
    ]

    results: List[HourlyUtilizationPoint] = []

    for hour in hours:
        total_sol = Decimal("0")
        for pos in positions:
            # Skip positions with no parseable open time
            dt_open = parse_iso_datetime(getattr(pos, "datetime_open", "") or "")
            if dt_open is None:
                continue

            # Skip positions where sol_deployed is missing
            sol_deployed = getattr(pos, "sol_deployed", None)
            if sol_deployed is None:
                continue

            # Check if position is active at this hour
            if dt_open > hour:
                continue  # Not yet open

            close_reason = getattr(pos, "close_reason", "") or ""
            if close_reason == "still_open":
                # Still open: active at every hour after open
                total_sol += sol_deployed
            else:
                dt_close = parse_iso_datetime(getattr(pos, "datetime_close", "") or "")
                if dt_close is not None and dt_close > hour:
                    total_sol += sol_deployed
                # If dt_close is None or <= hour, position was already closed at this hour

        results.append(HourlyUtilizationPoint(hour=hour, sol_active=total_sol))

    return results


def check_low_utilization_days(
    points: List[HourlyUtilizationPoint],
    portfolio_sol: Decimal,
    threshold_pct: float,
    consecutive_days: int,
) -> bool:
    """
    Returns True if the last `consecutive_days` full calendar days each had
    average hourly utilization below threshold_pct * portfolio_sol.

    A "full calendar day" = a day for which at least 20 hours appear in points
    (allows partial first/last day of the lookback window).
    If fewer than `consecutive_days` such days exist, returns False.

    Args:
        points: List of HourlyUtilizationPoint (sorted oldest-first).
        portfolio_sol: Total portfolio SOL (Decimal).
        threshold_pct: Fraction threshold, e.g. 0.80 for 80%.
        consecutive_days: Number of consecutive full days required.

    Returns:
        True if all of the last `consecutive_days` full days are below threshold.
    """
    from collections import defaultdict

    threshold_sol = portfolio_sol * Decimal(str(threshold_pct))

    # Group points by calendar date
    by_date = defaultdict(list)
    for pt in points:
        by_date[pt.hour.date()].append(pt)

    # Keep only sufficiently complete days (>= 20 data points out of 24)
    full_days = {d: pts for d, pts in by_date.items() if len(pts) >= 20}

    if len(full_days) < consecutive_days:
        return False

    # Sort dates and take the most recent `consecutive_days`
    sorted_dates = sorted(full_days.keys())
    recent_dates = sorted_dates[-consecutive_days:]

    for d in recent_dates:
        pts = full_days[d]
        avg_sol = sum((pt.sol_active for pt in pts), Decimal("0")) / Decimal(str(len(pts)))
        if avg_sol >= threshold_sol:
            return False

    return True


def generate_utilization_chart(
    points: List[HourlyUtilizationPoint],
    portfolio_sol: Decimal,
    output_dir: str,
) -> None:
    """
    Save hourly_utilization.png to output_dir.
    No-ops if matplotlib is not installed or points is empty.

    Visual spec:
    - X-axis: "MM-DD HH:00" labels, one per hour, rotated 45 deg, thinned to 1 per 6 hours.
    - Y-axis: SOL amount, label "SOL in Active Positions".
    - Main line: thick (linewidth=2.5), solid, color "#1f77b4".
    - Area fill: fill_between() with alpha=0.15, color="lightblue".
    - Reference line: red dashed at float(portfolio_sol).
    - Title: "Hourly Capital Utilization (last 72h)".
    - Grid: alpha=0.3, linestyle=":".
    - Saved as {output_dir}/hourly_utilization.png, dpi=150, bbox_inches="tight".

    Args:
        points: List of HourlyUtilizationPoint sorted oldest-first.
        portfolio_sol: Total portfolio SOL for the reference line.
        output_dir: Directory to save the chart.
    """
    if not HAS_MATPLOTLIB:
        print("  matplotlib not installed, skipping utilization chart")
        return

    if not points:
        print("  No utilization data, skipping utilization chart")
        return

    from pathlib import Path

    x_hours = [pt.hour for pt in points]
    y_sol = [float(pt.sol_active) for pt in points]

    # Build x-tick labels: "MM-DD HH:00"
    x_labels = [h.strftime("%m-%d %H:00") for h in x_hours]

    fig, ax = plt.subplots(figsize=(14, 5))

    # Main line
    ax.plot(
        range(len(x_hours)),
        y_sol,
        color="#1f77b4",
        linewidth=2.5,
        linestyle="-",
    )

    # Area fill under the line
    ax.fill_between(
        range(len(x_hours)),
        y_sol,
        alpha=0.15,
        color="lightblue",
    )

    # Reference line at portfolio total
    ax.axhline(
        y=float(portfolio_sol),
        color="red",
        linestyle="--",
        linewidth=1.5,
        label=f"Portfolio total: {portfolio_sol:.1f} SOL",
    )

    # X-axis ticks: show one label every 6 hours
    tick_positions = list(range(0, len(x_hours), 6))
    ax.set_xticks(tick_positions)
    ax.set_xticklabels([x_labels[i] for i in tick_positions], rotation=45, ha="right")

    ax.set_ylabel("SOL in Active Positions", fontsize=11)
    ax.set_title("Hourly Capital Utilization (last 72h)", fontsize=14, fontweight="bold")
    ax.grid(True, alpha=0.3, linestyle=":")
    ax.legend()

    fig.tight_layout()

    out_path = Path(output_dir) / "hourly_utilization.png"
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
