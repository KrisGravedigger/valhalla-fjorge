#!/usr/bin/env python3
"""Record an internally computed portfolio NAV snapshot."""

from __future__ import annotations

import argparse
import csv
import inspect
import os
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Optional


def _load_env_file() -> None:
    try:
        import dotenv
    except ImportError:  # pragma: no cover - production fallback
        return
    dotenv.load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from valhalla.capital_flow import read_flows  # noqa: E402
from valhalla.internal_nav import NavResult, compute_nav  # noqa: E402


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


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Record an internal NAV snapshot.")
    parser.add_argument("--dry-run", action="store_true", help="Print row without writing.")
    parser.add_argument("--wallet", help="Wallet address; defaults to LPAGENT_WALLET.")
    parser.add_argument("--rpc-url", help="Solana RPC URL.")
    parser.add_argument(
        "--path", default="output/portfolio_snapshots.csv", help="Output CSV path."
    )
    parser.add_argument("--net-contribution-sol", help="External net contribution in SOL.")
    parser.add_argument(
        "--timestamp", help="UTC timestamp, YYYY-MM-DDTHH:MM:SSZ. Defaults to now."
    )
    parser.add_argument("--lpagent-nav", help="Optional live lpagent NAV comparison.")
    parser.add_argument(
        "--allow-degraded",
        action="store_true",
        help="Write snapshot even when internal NAV is degraded.",
    )
    args = parser.parse_args(argv)

    _load_env_file()

    rpc_url = _resolve_rpc_url(args.rpc_url)
    wallet = args.wallet or os.getenv("LPAGENT_WALLET")
    if not rpc_url:
        print("ERROR: set HELIUS_API_KEY or HELIUS_RPC_URL in .env or pass --rpc-url")
        return 1
    if not wallet:
        print("ERROR: set LPAGENT_WALLET in .env or pass --wallet")
        return 1

    def progress(message: str) -> None:
        print(f"[nav] {message}", flush=True)

    try:
        if "progress" in inspect.signature(compute_nav).parameters:
            result = compute_nav(rpc_url, wallet, progress=progress)
        else:
            result = compute_nav(rpc_url, wallet)
    except RuntimeError as exc:
        if "zero NAV result" in str(exc):
            print("ERROR: zero NAV result - RPC failure suspected")
        else:
            print(f"ERROR: {exc}")
        return 1

    if result.total_nav_sol == 0:
        print("ERROR: zero NAV result - RPC failure suspected")
        return 1
    if result.degraded and not args.allow_degraded:
        print(
            "WARNING: degraded internal NAV; pass --allow-degraded to write snapshot. "
            f"Degraded mints: {','.join(result.degraded_mints)}"
        )
        return 1

    path = Path(args.path)
    try:
        row = build_snapshot_row(
            result=result,
            path=path,
            timestamp_arg=args.timestamp,
            net_contribution_arg=args.net_contribution_sol,
        )
        _check_lpagent_nav(result.total_nav_sol, args.lpagent_nav)
    except SystemExit as exc:
        print(str(exc))
        return 1

    if args.dry_run:
        print(",".join(FIELDS))
        print(",".join(row[field] for field in FIELDS))
        return 0

    _append_snapshot(path, row)
    print(f"Recorded internal snapshot in {path}")
    print(f"  value: {row['value_sol']} SOL")
    print(f"  total PnL: {Decimal(row['total_pnl_sol']):+.6f} SOL ({row['total_pnl_pct']}%)")
    if row["period_pnl_sol"]:
        print(f"  period PnL vs previous internal: {Decimal(row['period_pnl_sol']):+.6f} SOL")
    if row["notes"]:
        print(f"  notes: {row['notes']}")
    return 0


def build_snapshot_row(
    result: NavResult,
    path: Path,
    timestamp_arg: Optional[str] = None,
    net_contribution_arg: Optional[str] = None,
) -> dict[str, str]:
    timestamp = _validate_timestamp_utc(timestamp_arg) if timestamp_arg else _utc_now()
    rows = _read_rows(path)
    previous = _latest_for_source(rows, "internal")
    net_contribution = _resolve_net_contribution(
        net_contribution_arg, path, timestamp[:10], previous
    )
    value_sol = result.total_nav_sol
    total_pnl = value_sol - net_contribution
    total_pnl_pct = (
        total_pnl / net_contribution * Decimal("100") if net_contribution else None
    )
    period_pnl = None
    if previous:
        prev_value = _decimal(previous.get("value_sol"), "previous.value_sol")
        prev_contribution = _decimal(
            previous.get("net_contribution_sol"), "previous.net_contribution_sol"
        )
        if prev_value is not None and prev_contribution is not None:
            period_pnl = (value_sol - prev_value) - (net_contribution - prev_contribution)

    notes = ""
    if result.degraded:
        notes = "degraded: " + ",".join(result.degraded_mints)

    return {
        "timestamp": timestamp,
        "source": "internal",
        "value_sol": _fmt(value_sol),
        "value_usd": "",
        "sol_usd": "",
        "net_contribution_sol": _fmt(net_contribution),
        "total_pnl_sol": _fmt(total_pnl),
        "total_pnl_pct": _fmt(total_pnl_pct, "0.0001"),
        "period_pnl_sol": _fmt(period_pnl),
        "notes": notes,
    }


def _resolve_rpc_url(arg_value: Optional[str]) -> Optional[str]:
    if arg_value:
        return arg_value
    env_url = os.getenv("HELIUS_RPC_URL")
    if env_url:
        return env_url
    api_key = os.getenv("HELIUS_API_KEY")
    if api_key:
        return f"https://mainnet.helius-rpc.com/?api-key={api_key}"
    return None


def _resolve_net_contribution(
    arg_value: Optional[str],
    snapshot_path: Path,
    asof_date: str,
    previous: Optional[dict[str, str]],
) -> Decimal:
    explicit = _decimal(arg_value, "net_contribution_sol")
    if explicit is not None:
        return explicit

    flows_path = snapshot_path.parent / "capital_flows.csv"
    if flows_path.exists():
        try:
            from_ledger = read_flows(flows_path, asof_date)
        except FileNotFoundError:
            from_ledger = None
        if from_ledger is not None:
            return from_ledger

    if previous and previous.get("net_contribution_sol"):
        previous_value = _decimal(
            previous["net_contribution_sol"], "previous.net_contribution_sol"
        )
        if previous_value is not None:
            return previous_value

    raise SystemExit(
        "Error: No capital_flows.csv found and --net-contribution-sol not provided. "
        "Provide one of these to record the first internal snapshot."
    )


def _check_lpagent_nav(value_sol: Decimal, lpagent_nav_arg: Optional[str]) -> None:
    lpagent_nav = _decimal(lpagent_nav_arg, "lpagent_nav")
    if lpagent_nav is None:
        return
    if lpagent_nav == 0:
        raise SystemExit("Invalid --lpagent-nav: must be non-zero.")
    diff = abs(value_sol - lpagent_nav) / lpagent_nav
    print(f"lpagent diff: {diff * Decimal('100'):.4f}%")
    if diff >= Decimal("0.02"):
        raise SystemExit("Internal NAV differs from lpagent by >= 2%.")


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


def _read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def _latest_for_source(
    rows: list[dict[str, str]], source: str
) -> Optional[dict[str, str]]:
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


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _append_snapshot(path: Path, row: dict[str, str]) -> None:
    # mirrors tools/record_portfolio_snapshot.py:append_snapshot
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


if __name__ == "__main__":
    raise SystemExit(main())
