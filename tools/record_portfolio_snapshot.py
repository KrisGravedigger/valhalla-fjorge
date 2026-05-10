#!/usr/bin/env python3
"""Record a portfolio NAV snapshot and compute contribution-adjusted PnL.

This is intentionally separate from per-position PnL. It answers:
current portfolio value - external net contributions = real portfolio PnL.

Single-process tool: do not run concurrent invocations against the same snapshot CSV.
"""

import argparse
import csv
import sys
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional


FIELDS = [
    "timestamp",
    "source",
    "value_sol",
    "value_usd",
    "sol_usd",
    "net_contribution_sol",
    "total_pnl_sol",
    "total_pnl_pct",
    "period_pnl_sol",
    "notes",
]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _decimal(value: Optional[str], name: str) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise SystemExit(f"Invalid decimal for {name}: {value}") from exc


def _fmt(value: Optional[Decimal], places: str = "0.000001") -> str:
    if value is None:
        return ""
    return str(value.quantize(Decimal(places)))


def _read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _latest_for_source(rows: List[Dict[str, str]], source: str) -> Optional[Dict[str, str]]:
    matches = [row for row in rows if row.get("source") == source]
    if not matches:
        return None
    return sorted(matches, key=lambda row: row.get("timestamp", ""))[-1]


def _validate_timestamp_utc(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise SystemExit(
            "Invalid --timestamp: must be YYYY-MM-DDTHH:MM:SSZ (UTC, Z suffix)."
        ) from exc

    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise SystemExit(
            "Invalid --timestamp: must be YYYY-MM-DDTHH:MM:SSZ (UTC, Z suffix)."
        )

    return value


def build_snapshot(args: argparse.Namespace, previous: Optional[Dict[str, str]]) -> Dict[str, str]:
    timestamp_arg = _validate_timestamp_utc(args.timestamp) if args.timestamp else None
    value_sol = _decimal(args.value_sol, "value_sol")
    value_usd = _decimal(args.value_usd, "value_usd")
    sol_usd = _decimal(args.sol_usd, "sol_usd")

    if value_sol is None:
        if value_usd is None or sol_usd is None or sol_usd <= 0:
            raise SystemExit("Provide --value-sol, or both --value-usd and --sol-usd.")
        value_sol = value_usd / sol_usd

    net_contribution = _decimal(args.net_contribution_sol, "net_contribution_sol")
    if net_contribution is None:
        flows_path = Path(args.path).parent / "capital_flows.csv"
        if flows_path.exists():
            from valhalla.capital_flow import read_flows

            asof = (timestamp_arg or datetime.now().strftime("%Y-%m-%d"))[:10]
            net_from_ledger = read_flows(flows_path, asof)
            if net_from_ledger is not None:
                net_contribution = net_from_ledger

    if net_contribution is None:
        if previous and previous.get("net_contribution_sol"):
            net_contribution = _decimal(previous["net_contribution_sol"], "net_contribution_sol")
        else:
            raise SystemExit(
                "Error: No capital_flows.csv found and --net-contribution-sol not provided. "
                "Provide one of these to record the first snapshot."
            )

    assert value_sol is not None
    assert net_contribution is not None
    total_pnl = value_sol - net_contribution
    total_pnl_pct = (total_pnl / net_contribution * Decimal("100")) if net_contribution else None

    period_pnl = None
    if previous:
        prev_value = _decimal(previous.get("value_sol"), "previous.value_sol")
        prev_contribution = _decimal(previous.get("net_contribution_sol"), "previous.net_contribution_sol")
        if prev_value is not None and prev_contribution is not None:
            period_pnl = (value_sol - prev_value) - (net_contribution - prev_contribution)

    timestamp = timestamp_arg or datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    return {
        "timestamp": timestamp,
        "source": args.source,
        "value_sol": _fmt(value_sol),
        "value_usd": _fmt(value_usd),
        "sol_usd": _fmt(sol_usd),
        "net_contribution_sol": _fmt(net_contribution),
        "total_pnl_sol": _fmt(total_pnl),
        "total_pnl_pct": _fmt(total_pnl_pct, "0.0001"),
        "period_pnl_sol": _fmt(period_pnl),
        "notes": args.notes or "",
    }


def append_snapshot(path: Path, row: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Record a portfolio NAV snapshot.")
    parser.add_argument("--source", required=True, help="Valuation source, e.g. lpagent, fabriq, manual.")
    parser.add_argument("--value-sol", help="Portfolio NAV in SOL.")
    parser.add_argument("--value-usd", help="Portfolio NAV in USD; requires --sol-usd.")
    parser.add_argument("--sol-usd", help="SOL/USD price used to convert --value-usd.")
    parser.add_argument(
        "--net-contribution-sol",
        help="External deposits minus withdrawals in SOL. Required for the first snapshot per source.",
    )
    parser.add_argument("--timestamp", help="Snapshot timestamp, default: local now, ISO format.")
    parser.add_argument("--notes", default="", help="Optional free-form notes.")
    parser.add_argument("--path", default="output/portfolio_snapshots.csv", help="Output CSV path.")
    parser.add_argument("--dry-run", action="store_true", help="Print the row without writing.")
    args = parser.parse_args()

    path = Path(args.path)
    rows = _read_rows(path)
    previous = _latest_for_source(rows, args.source)
    row = build_snapshot(args, previous)

    if args.dry_run:
        print(",".join(FIELDS))
        print(",".join(row[field] for field in FIELDS))
        return

    append_snapshot(path, row)
    print(f"Recorded snapshot in {path}")
    print(f"  value: {row['value_sol']} SOL")
    print(f"  total PnL: {Decimal(row['total_pnl_sol']):+.6f} SOL ({row['total_pnl_pct']}%)")
    if row["period_pnl_sol"]:
        print(f"  period PnL vs previous {args.source}: {Decimal(row['period_pnl_sol']):+.6f} SOL")


if __name__ == "__main__":
    main()
