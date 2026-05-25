#!/usr/bin/env python3
"""Generate a combined portfolio NAV + SOL capital-flow chart."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FLOWS_PATH = PROJECT_ROOT / "output" / "capital_flows.csv"
DEFAULT_SNAPSHOTS_PATH = PROJECT_ROOT / "output" / "portfolio_snapshots.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output" / "sol_flows.png"
WINDOW_DAYS = 30


def _read_daily_net(path: Path) -> dict[str, Decimal]:
    daily: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            day = (row.get("timestamp_utc") or "")[:10]
            flow_type = row.get("type", "")
            if not day or flow_type not in {"deposit", "withdrawal"}:
                continue
            amount = Decimal(row.get("sol_amount") or "0")
            daily[day] += amount if flow_type == "deposit" else -amount
    return dict(daily)


def _read_nav_snapshots(path: Path) -> list[tuple[datetime, Decimal]]:
    if not path.exists():
        return []
    points = []
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            ts_str = (row.get("timestamp") or "").strip()
            value_sol = (row.get("value_sol") or "").strip()
            if not ts_str or not value_sol:
                continue
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                points.append((ts, Decimal(value_sol)))
            except (ValueError, Exception):
                continue
    return sorted(points, key=lambda x: x[0])


def generate_chart(flows_path: Path, snapshots_path: Path, output_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    daily = _read_daily_net(flows_path)
    all_days = sorted(daily)

    all_cumulative: dict[str, Decimal] = {}
    running = Decimal("0")
    for day_str in all_days:
        running += daily[day_str]
        all_cumulative[day_str] = running

    today = date.today()
    cutoff = today - timedelta(days=WINDOW_DAYS)

    cum_at_cutoff = Decimal("0")
    for day_str in all_days:
        if datetime.strptime(day_str, "%Y-%m-%d").date() < cutoff:
            cum_at_cutoff = all_cumulative[day_str]

    recent_days = [d for d in all_days if datetime.strptime(d, "%Y-%m-%d").date() >= cutoff]

    step_dates = [cutoff]
    step_values = [cum_at_cutoff]
    level = cum_at_cutoff
    for day_str in recent_days:
        level += daily[day_str]
        step_dates.append(datetime.strptime(day_str, "%Y-%m-%d").date())
        step_values.append(level)
    step_dates.append(today)
    step_values.append(level)

    nav_points = _read_nav_snapshots(snapshots_path)
    nav_in_window = [
        (p[0].date(), p[1])
        for p in nav_points
        if p[0].date() >= cutoff
    ]

    fig, ax = plt.subplots(figsize=(12, 6))

    ax.step(
        step_dates,
        [float(v) for v in step_values],
        color="#1f5fbf",
        linewidth=2,
        where="post",
        label="Net SOL invested",
    )

    if nav_in_window:
        nav_dates = [p[0] for p in nav_in_window]
        nav_values = [float(p[1]) for p in nav_in_window]
        ax.plot(
            nav_dates,
            nav_values,
            color="#16803a",
            linewidth=2,
            marker="o",
            markersize=6,
            label="Portfolio NAV (total SOL)",
        )

    ax.set_ylabel("SOL")
    ax.set_xlabel("Date")
    ax.set_title(f"Portfolio NAV vs Investment — last {WINDOW_DAYS} days")
    ax.set_xlim(cutoff, today)
    ax.legend(loc="best")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")
    ax.grid(True, alpha=0.3, axis="y")
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Chart portfolio NAV vs SOL investment.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="PNG output path.")
    parser.add_argument("--flows-path", default=str(DEFAULT_FLOWS_PATH), help=argparse.SUPPRESS)
    parser.add_argument("--snapshots-path", default=str(DEFAULT_SNAPSHOTS_PATH), help=argparse.SUPPRESS)
    args = parser.parse_args()

    generate_chart(Path(args.flows_path), Path(args.snapshots_path), Path(args.output))
    print(f"Wrote {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
