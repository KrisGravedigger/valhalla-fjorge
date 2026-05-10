"""Portfolio NAV scoreboard - build and render the per-source summary table."""

import csv
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, List, Optional


TABLE_FIELDS = [
    "source",
    "value_sol",
    "net_contribution_sol",
    "total_pnl_sol",
    "total_pnl_pct",
    "timestamp",
]


def _decimal(value: Optional[str]) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _warn_malformed(row: Dict[str, str], error: InvalidOperation) -> None:
    source = row.get("source", "")
    value = row.get("value_sol")
    print(
        f"Warning: skipping malformed snapshot row (source={source}, value_sol={value!r}): {error}",
        file=sys.stderr,
    )


def _is_valid_display_row(row: Dict[str, str]) -> bool:
    try:
        _decimal(row.get("value_sol"))
        _decimal(row.get("net_contribution_sol"))
        _decimal(row.get("total_pnl_sol"))
        _decimal(row.get("total_pnl_pct"))
    except InvalidOperation as error:
        _warn_malformed(row, error)
        return False
    return True


def _fmt(value: Optional[Decimal], places: str = "0.000001", signed: bool = False) -> str:
    if value is None:
        return ""
    quantized = value.quantize(Decimal(places))
    if signed:
        return f"{quantized:+f}"
    return str(quantized)


def _fmt_pct(value: Optional[Decimal]) -> str:
    if value is None:
        return ""
    return f"{value.quantize(Decimal('0.01')):+f}%"


def build_scoreboard(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    """Return the latest portfolio snapshot row per source, sorted by source."""
    latest_by_source: Dict[str, Dict[str, str]] = {}
    for row in rows:
        source = row.get("source", "").strip()
        if not source:
            continue
        if not _is_valid_display_row(row):
            continue
        previous = latest_by_source.get(source)
        # Tie-break by append order: on equal timestamps, last-appended wins.
        if previous is None or row.get("timestamp", "") >= previous.get("timestamp", ""):
            latest_by_source[source] = row

    return [
        {field: latest.get(field, "") for field in TABLE_FIELDS}
        for _, latest in sorted(latest_by_source.items())
    ]


def _display_row(row: Dict[str, str]) -> Dict[str, str]:
    return {
        "source": row.get("source", ""),
        "value_sol": _fmt(_decimal(row.get("value_sol"))),
        "net_contribution_sol": _fmt(_decimal(row.get("net_contribution_sol"))),
        "total_pnl_sol": _fmt(_decimal(row.get("total_pnl_sol")), signed=True),
        "total_pnl_pct": _fmt_pct(_decimal(row.get("total_pnl_pct"))),
    }


def render_console(table: List[Dict[str, str]], date: str) -> None:
    """Print the scoreboard block to stdout."""
    if not table:
        return

    display_rows = [_display_row(row) for row in table if _is_valid_display_row(row)]
    if not display_rows:
        return

    print(f"\n=== Portfolio Scoreboard - {date} ===\n")
    print("Source    | Value (SOL) | Net Contribution | Total PnL    | Total PnL %")
    print("----------|-------------|------------------|--------------|-------------")
    for row in display_rows:
        print(
            f"{row['source']:<9} | "
            f"{row['value_sol']:>11} | "
            f"{row['net_contribution_sol']:>16} | "
            f"{row['total_pnl_sol']:>12} | "
            f"{row['total_pnl_pct']:>11}"
        )
    print("\n(NAV PnL = portfolio value minus net external contributions)")


def render_markdown(table: List[Dict[str, str]], date: str, path: Path) -> None:
    """Write the scoreboard to output/portfolio_scoreboard.md."""
    if not table:
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    updated = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [
        f"# Portfolio Scoreboard - {date}",
        "",
        "| Source | Value (SOL) | Net Contribution | Total PnL | Total PnL % |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in table:
        if not _is_valid_display_row(row):
            continue
        display = _display_row(row)
        lines.append(
            "| {source} | {value_sol} | {net_contribution_sol} | {total_pnl_sol} | {total_pnl_pct} |".format(
                **display
            )
        )

    lines.extend(
        [
            "",
            (
                "_NAV PnL = current portfolio value - external net contributions. "
                f"Updated: {updated}_"
            ),
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")


def run_scoreboard(snapshots_path: Path, output_dir: Path) -> None:
    """Read portfolio snapshots and render the current scoreboard."""
    if not snapshots_path.exists():
        return

    with snapshots_path.open("r", newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))

    table = build_scoreboard(rows)
    if not table:
        return

    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    render_console(table, date)
    render_markdown(table, date, output_dir / "portfolio_scoreboard.md")
