from decimal import Decimal
from typing import List

from valhalla.analysis_config import (
    LOSS_DETAIL_MIN_SOL,
    LOSS_DETAIL_LOOKBACK_DAYS,
    PORTFOLIO_TOTAL_SOL,
)
from valhalla.models import parse_iso_datetime

from .formatters import scenario_label


def md_table(headers: List[str], rows: List[List[str]]) -> str:
    """Render a markdown table string."""
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

def build_loss_detail_table(positions: List) -> str:
    """
    Build a markdown table of recent large losses.

    Filters: pnl_sol <= -LOSS_DETAIL_MIN_SOL (any close_reason except still_open),
    datetime_close within last LOSS_DETAIL_LOOKBACK_DAYS days.
    Sorted by pnl_sol ascending (largest loss first).
    """
    from datetime import timedelta

    header = "## 3. Recent Large Losses {#large-losses}"
    min_sol = Decimal(str(LOSS_DETAIL_MIN_SOL))

    # Collect qualifying positions: any closed position with loss >= threshold
    candidates = []
    for p in positions:
        if p.close_reason == "still_open":
            continue
        pnl = getattr(p, "pnl_sol", None)
        if pnl is None or pnl > -min_sol:
            continue
        dt_close = getattr(p, "datetime_close", None)
        if not dt_close:
            continue
        candidates.append(p)

    empty_msg = (
        f"{header}\n\n"
        f"_No losses above {LOSS_DETAIL_MIN_SOL:.2f} SOL in the last "
        f"{LOSS_DETAIL_LOOKBACK_DAYS} days._\n"
    )

    if not candidates:
        return empty_msg

    # Filter by datetime_close within last LOSS_DETAIL_LOOKBACK_DAYS days
    close_dates = []
    for p in candidates:
        dt = parse_iso_datetime(p.datetime_close)
        close_dates.append(dt)

    valid_close = [(p, d) for p, d in zip(candidates, close_dates) if d is not None]
    if not valid_close:
        return empty_msg

    ref_date = max(d for _, d in valid_close)
    cutoff = ref_date - timedelta(days=LOSS_DETAIL_LOOKBACK_DAYS)
    recent_losses = [(p, d) for p, d in valid_close if d >= cutoff]

    if not recent_losses:
        return empty_msg

    # Sort by datetime_open ascending (chronological order)
    recent_losses.sort(key=lambda x: getattr(x[0], "datetime_open", "") or "")

    include_portfolio_pct = PORTFOLIO_TOTAL_SOL > 0
    portfolio_dec = Decimal(str(PORTFOLIO_TOTAL_SOL)) if include_portfolio_pct else None

    headers = ["Open", "Close", "Wallet", "Token", "ID", "Reason", "Loss (SOL)", "Loss (%)", "Source PnL (%)", "Source hold (min)", "Source action"]
    if include_portfolio_pct:
        headers.append("% portfolio")

    rows = []
    for p, _ in recent_losses:
        pnl = p.pnl_sol
        pnl_pct = getattr(p, "pnl_pct", None)
        source_pnl_pct = getattr(p, "source_wallet_pnl_pct", None)
        source_hold = getattr(p, "source_wallet_hold_min", None)
        scenario = getattr(p, "source_wallet_scenario", None)

        open_str = getattr(p, "datetime_open", None) or "N/A"
        close_str = p.datetime_close if p.datetime_close else "N/A"
        token_pair_str = p.token if p.token else "N/A"
        position_id_str = p.position_id[:8] if getattr(p, "position_id", None) else "N/A"
        loss_sol_str = f"-{abs(pnl):.3f}"
        loss_pct_str = f"{float(pnl_pct):.1f}%" if pnl_pct is not None else "N/A"
        src_pnl_str = f"{float(source_pnl_pct):.1f}%" if source_pnl_pct is not None else "N/A"
        src_hold_str = str(source_hold) if source_hold is not None else "N/A"

        row = [
            open_str,
            close_str,
            p.target_wallet,
            token_pair_str,
            position_id_str,
            p.close_reason,
            loss_sol_str,
            loss_pct_str,
            src_pnl_str,
            src_hold_str,
            scenario_label(scenario),
        ]
        if include_portfolio_pct:
            portfolio_pct_val = abs(pnl) / portfolio_dec * 100
            row.append(f"{float(portfolio_pct_val):.1f}%")
        rows.append(row)

    table_str = md_table(headers, rows)
    return f"{header}\n\n{table_str}\n"
