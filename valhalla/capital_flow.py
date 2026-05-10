"""Helpers for reading the manual capital-flow ledger."""

import csv
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional


FIELDS = [
    "timestamp_utc",
    "wallet",
    "type",
    "sol_amount",
    "tx_signature",
    "notes",
]

_ZERO_SOL = Decimal("0.000000")
_SOL_PLACES = Decimal("0.000001")


def _fmt_sol(value: Decimal) -> Decimal:
    return value.quantize(_SOL_PLACES)


def read_flows(path: str | Path, asof_date: str) -> Optional[Decimal]:
    """Sum net external capital flows up to and including asof_date.

    Deposits add to net contribution, withdrawals subtract from it, and
    internal transfers are ignored because they stay inside the portfolio.
    """
    ledger_path = Path(path)
    if not ledger_path.exists():
        return None

    total = _ZERO_SOL
    with ledger_path.open("r", newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("timestamp_utc", "")[:10] > asof_date:
                continue

            flow_type = row.get("type", "")
            if flow_type == "internal_transfer":
                continue

            amount = Decimal(row["sol_amount"])
            if flow_type == "deposit":
                total += amount
            elif flow_type == "withdrawal":
                total -= amount

    return _fmt_sol(total)


def _today_utc_date() -> date:
    return datetime.now(timezone.utc).date()


def check_stale_flows(path: str | Path, warn_after_days: int = 14) -> Optional[str]:
    """Return a warning if the ledger's most recent entry is too old."""
    ledger_path = Path(path)
    if not ledger_path.exists():
        return None

    dates = []
    with ledger_path.open("r", newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            timestamp = row.get("timestamp_utc", "")
            if timestamp:
                dates.append(datetime.strptime(timestamp[:10], "%Y-%m-%d").date())

    if not dates:
        return None

    elapsed_days = (_today_utc_date() - max(dates)).days
    if elapsed_days > warn_after_days:
        return (
            f"Warning: capital_flows.csv has not been updated in {elapsed_days} days. "
            "Record any recent deposits/withdrawals to keep the scoreboard accurate."
        )

    return None
