#!/usr/bin/env python3
"""Generate a daily SOL capital-flow chart."""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_FLOWS_PATH = PROJECT_ROOT / "output" / "capital_flows.csv"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "output" / "sol_flows.png"


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


def generate_chart(flows_path: Path, output_path: Path) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    daily = _read_daily_net(flows_path)
    days = sorted(daily)
    values = [daily[day] for day in days]
    cumulative: list[Decimal] = []
    running = Decimal("0")
    for value in values:
        running += value
        cumulative.append(running)

    x_values = [datetime.strptime(day, "%Y-%m-%d").date() for day in days]
    fig, ax_daily = plt.subplots(figsize=(11, 6))
    colors = ["#16803a" if value >= 0 else "#c43333" for value in values]

    ax_daily.bar(x_values, [float(value) for value in values], color=colors, label="Daily net")
    ax_daily.axhline(0, color="#555555", linewidth=0.8)
    ax_daily.set_ylabel("Daily net SOL")
    ax_daily.set_xlabel("Date")

    ax_cumulative = ax_daily.twinx()
    ax_cumulative.plot(
        x_values,
        [float(value) for value in cumulative],
        color="#1f5fbf",
        linewidth=2,
        marker="o",
        label="Cumulative net SOL",
    )
    ax_cumulative.set_ylabel("Cumulative net SOL")

    ax_daily.set_title("SOL Capital Flows")
    handles_1, labels_1 = ax_daily.get_legend_handles_labels()
    handles_2, labels_2 = ax_cumulative.get_legend_handles_labels()
    ax_daily.legend(handles_1 + handles_2, labels_1 + labels_2, loc="best")
    fig.autofmt_xdate()
    fig.tight_layout()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)


def main() -> int:
    parser = argparse.ArgumentParser(description="Chart SOL capital flows.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="PNG output path.")
    parser.add_argument("--flows-path", default=str(DEFAULT_FLOWS_PATH), help=argparse.SUPPRESS)
    args = parser.parse_args()

    output_path = Path(args.output)
    generate_chart(Path(args.flows_path), output_path)
    print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
