"""
Wallet Trend Report generator.

Produces output/wallet_trend.md — a compact 7/3/1-day rolling scorecard
covering ALL wallets (not filtered to SCORECARD_RECENT_DAYS), enabling the
user to spot wallets that have died or degraded over time.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from .loss_analyzer import WalletScorecard, WalletScorecardAnalyzer
from .models import MatchedPosition, parse_iso_datetime


def _fmt_wr(rate: Optional[float]) -> str:
    """Format a win-rate percentage as an integer string, or '—' for None."""
    return f"{rate:.0f}" if rate is not None else "—"


def _fmt_rug(rate: Optional[float]) -> str:
    """Format a rug rate as an integer string, or '—' for None."""
    return f"{rate:.0f}" if rate is not None else "—"


def _fmt_pnl(value: Decimal) -> str:
    """Format a SOL PnL value with sign and 4 decimal places."""
    return f"{value:+.4f}"


def _fmt_per_day(value: Decimal, days: int) -> str:
    """Format SOL per day (value / days), signed, 4 decimal places."""
    per_day = value / Decimal(str(days))
    return f"{per_day:+.4f}"


def _fmt_pos_per_day(count: int, days: int) -> str:
    """Format positions per day with one decimal place."""
    return f"{count / days:.1f}"


def _fmt_last_seen(days: Optional[int]) -> str:
    """Format days_since_last_position into a human-readable string."""
    if days is None:
        return "—"
    if days == 0:
        return "today"
    return f"{days}d"


def _fmt_exposure(exposure_sol: Decimal, portfolio_total_sol: float) -> str:
    """Format current exposure as a percentage of portfolio."""
    if portfolio_total_sol <= 0:
        return "—"
    pct = float(exposure_sol) / portfolio_total_sol * 100.0
    return f"{pct:.1f}%"


def _md_table(headers: List[str], rows: List[List[str]]) -> str:
    """Render a markdown table string with padded columns."""
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(cell)))

    def _pad(text: str, width: int) -> str:
        return str(text).ljust(width)

    header_line = "| " + " | ".join(_pad(h, col_widths[i]) for i, h in enumerate(headers)) + " |"
    sep_line = "| " + " | ".join("-" * w for w in col_widths) + " |"
    data_lines = [
        "| " + " | ".join(_pad(str(cell), w) for cell, w in zip(row, col_widths)) + " |"
        for row in rows
    ]
    return "\n".join([header_line, sep_line] + data_lines)


def _build_row(sc: WalletScorecard, portfolio_total_sol: float) -> List[str]:
    """Build a single table row for one WalletScorecard."""
    pos_per_day = (
        f"{_fmt_pos_per_day(sc.positions_7d, 7)}/"
        f"{_fmt_pos_per_day(sc.positions_3d, 3)}/"
        f"{_fmt_pos_per_day(sc.positions_1d, 1)}"
    )

    wr = (
        f"{_fmt_wr(sc.win_rate_7d_pct)}/"
        f"{_fmt_wr(sc.win_rate_72h_pct)}/"
        f"{_fmt_wr(sc.win_rate_24h_pct)}%"
    )

    pnl = (
        f"{_fmt_pnl(sc.pnl_7d_sol)}/"
        f"{_fmt_pnl(sc.pnl_3d_sol)}/"
        f"{_fmt_pnl(sc.pnl_1d_sol)}"
    )

    sol_per_day = (
        f"{_fmt_per_day(sc.pnl_7d_sol, 7)}/"
        f"{_fmt_per_day(sc.pnl_3d_sol, 3)}/"
        f"{_fmt_per_day(sc.pnl_1d_sol, 1)}"
    )

    rug = (
        f"{_fmt_rug(sc.rug_rate_7d_pct)}/"
        f"{_fmt_rug(sc.rug_rate_3d_pct)}/"
        f"{_fmt_rug(sc.rug_rate_1d_pct)}%"
    )

    med_pnl = _fmt_pnl(sc.median_pnl_sol) if sc.median_pnl_sol is not None else "—"

    return [
        sc.wallet,
        pos_per_day,
        wr,
        pnl,
        sol_per_day,
        rug,
        med_pnl,
        _fmt_last_seen(sc.days_since_last_position),
        _fmt_exposure(sc.current_exposure_sol, portfolio_total_sol),
    ]


HEADERS = [
    "Wallet",
    "Pos/day 7/3/1",
    "WR% 7/3/1",
    "PnL 7/3/1 (SOL)",
    "SOL/day 7/3/1",
    "Rug% 7/3/1",
    "Med PnL",
    "Last seen",
    "% portf",
]


def generate_wallet_trend_report(
    scorecards: List[WalletScorecard],
    matched_positions: List[MatchedPosition],
    output_path: str,
    portfolio_total_sol: float,
    reference_date: Optional[datetime] = None,
) -> None:
    """
    Write output/wallet_trend.md with the compact 7/3/1 rolling-window scorecard.

    Covers ALL wallets (no recency filter). Wallets are grouped into:
    - Dormant: active in last 7d but silent in last 1d (died recently)
    - Active: had at least one position in last 1d
    - Long-dormant: no activity in last 7d

    Args:
        scorecards: Pre-computed WalletScorecard list from WalletScorecardAnalyzer.
        matched_positions: Full position list (used to resolve reference_date if needed).
        output_path: Absolute path for the output .md file.
        portfolio_total_sol: Portfolio size in SOL for exposure % calculation.
        reference_date: Reference point for windows; defaults to max datetime_close.
    """
    # Resolve reference_date from positions if not provided
    if reference_date is None:
        closed = [p for p in matched_positions if p.close_reason != "still_open"]
        dates = [
            parse_iso_datetime(p.datetime_close)
            for p in closed
            if parse_iso_datetime(p.datetime_close) is not None
        ]
        reference_date = max(dates) if dates else datetime.utcnow()

    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M")
    ref_str = reference_date.strftime("%Y-%m-%d %H:%M")
    date_str = datetime.utcnow().strftime("%Y-%m-%d")

    # Partition wallets into three groups
    dormant: List[WalletScorecard] = []    # active in 7d, silent in 1d
    active: List[WalletScorecard] = []     # had activity in last 1d
    long_dormant: List[WalletScorecard] = []  # no activity in last 7d

    for sc in scorecards:
        if sc.positions_1d > 0:
            active.append(sc)
        elif sc.positions_7d > 0:
            dormant.append(sc)
        else:
            long_dormant.append(sc)

    # Sort active by SOL/day (7d) descending (best performers first)
    active.sort(key=lambda s: s.pnl_per_day_sol, reverse=True)
    # Sort dormant by last-seen (most recently died first, i.e. fewest days)
    dormant.sort(key=lambda s: (s.days_since_last_position is None, s.days_since_last_position))
    # Sort long-dormant by last-seen as well
    long_dormant.sort(key=lambda s: (s.days_since_last_position is None, s.days_since_last_position))

    lines: List[str] = []
    lines.append(f"# Wallet Trend Report — {date_str}")
    lines.append(f"Generated: {now_str} UTC")
    lines.append("")
    lines.append(f"Reference date (latest activity, open or close): {ref_str} UTC")
    lines.append("Windows: 7d / 3d / 1d rolling from reference date.")
    lines.append("")
    lines.append(
        "Columns: **Pos/day** = all positions per day opened in window (includes still-open). "
        "**WR%** = win rate on closed positions. "
        "**PnL** = SOL earned from closes in window. "
        "**SOL/day** = PnL normalised per day. "
        "**Rug%** = rug/rug_unknown_open as % of window closes (— = no closes). "
        "**Med PnL** = median pnl_sol per trade across all time. "
        "**Last seen** = days since most recent activity (open or close). "
        "**% portf** = sum of your_sol on still-open positions / PORTFOLIO_TOTAL_SOL."
    )
    lines.append("")

    def _append_group(title: str, group: List[WalletScorecard]) -> None:
        lines.append(f"### {title}")
        lines.append("")
        if not group:
            lines.append("_None._")
        else:
            rows = [_build_row(sc, portfolio_total_sol) for sc in group]
            lines.append(_md_table(HEADERS, rows))
        lines.append("")

    _append_group(
        f"Dormant — active in last 7d but no positions in last 1d ({len(dormant)})",
        dormant,
    )
    _append_group(
        f"Active ({len(active)} wallet{'s' if len(active) != 1 else ''}, sorted by SOL/day 7d)",
        active,
    )
    _append_group(
        f"Long-dormant — no positions in last 7d ({len(long_dormant)})",
        long_dormant,
    )

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
        f.write("\n")
