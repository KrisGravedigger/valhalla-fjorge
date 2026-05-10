#!/usr/bin/env python3
"""Append a manual capital-flow entry to output/capital_flows.csv."""

import argparse
import csv
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List


FIELDS = [
    "timestamp_utc",
    "wallet",
    "type",
    "sol_amount",
    "tx_signature",
    "notes",
]

FLOW_TYPES = ("deposit", "withdrawal", "internal_transfer")
INTERNAL_TRANSFER_REMINDER = (
    "Reminder: record the matching leg of this internal transfer "
    "(withdrawal on the source wallet / deposit on the destination wallet) "
    "to keep portfolio-level net contribution correct."
)


def _fmt_sol(value: Decimal) -> str:
    return str(value.quantize(Decimal("0.000001")))


def _parse_positive_decimal(value: str) -> Decimal:
    try:
        amount = Decimal(str(value))
    except InvalidOperation as exc:
        raise SystemExit(f"Invalid decimal for sol_amount: {value}") from exc

    if amount <= 0:
        raise SystemExit("sol_amount must be positive.")

    return amount


def _read_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _reject_duplicate_signature(path: Path, tx_signature: str) -> None:
    if not tx_signature:
        return

    for row in _read_rows(path):
        if row.get("tx_signature") == tx_signature:
            print(
                f"Error: tx_signature {tx_signature} already exists in "
                "capital_flows.csv — duplicate entry rejected."
            )
            sys.exit(1)


def _default_timestamp_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_row(args: argparse.Namespace) -> Dict[str, str]:
    amount = _parse_positive_decimal(args.sol_amount)
    return {
        "timestamp_utc": args.timestamp_utc or _default_timestamp_utc(),
        "wallet": args.wallet,
        "type": args.type,
        "sol_amount": _fmt_sol(amount),
        "tx_signature": args.tx_signature or "",
        "notes": args.notes or "",
    }


def append_flow(path: Path, row: Dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def _print_row(row: Dict[str, str]) -> None:
    writer = csv.DictWriter(sys.stdout, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Record a portfolio capital flow.")
    parser.add_argument("--timestamp-utc", help="UTC timestamp, YYYY-MM-DDTHH:MM:SSZ.")
    parser.add_argument("--wallet", required=True, help="Wallet address or portfolio_bootstrap.")
    parser.add_argument("--type", required=True, choices=FLOW_TYPES, help="Capital-flow type.")
    parser.add_argument("--sol-amount", required=True, help="Positive SOL amount.")
    parser.add_argument("--tx-signature", default="", help="On-chain transaction signature.")
    parser.add_argument("--notes", default="", help="Optional free-form notes.")
    parser.add_argument("--path", default="output/capital_flows.csv", help="Output CSV path.")
    parser.add_argument("--dry-run", action="store_true", help="Print the row without writing.")
    args = parser.parse_args()

    path = Path(args.path)
    row = _build_row(args)
    _reject_duplicate_signature(path, row["tx_signature"])

    if args.dry_run:
        _print_row(row)
    else:
        append_flow(path, row)
        print(f"Recorded capital flow in {path}")

    if row["type"] == "internal_transfer":
        print(INTERNAL_TRANSFER_REMINDER)


if __name__ == "__main__":
    main()
