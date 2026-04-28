"""Terminal-only filter backtest helpers."""

from typing import List, Optional

from valhalla.loss_report.formatters import fmt_mc
from valhalla.loss_report.tables import md_table


def print_backtest_table(param: str, rows: List) -> None:
    """Print a backtest sweep result table to terminal."""
    PARAM_LABELS = {
        "jup_score": "jup_score",
        "mc_at_open": "mc_at_open",
        "token_age_hours": "token_age_hours",
    }
    print(f"\n--- Backtest: {PARAM_LABELS.get(param, param)} ---")

    if not rows:
        print("  No data.")
        return

    def _fmt_age_threshold_t(threshold: float) -> str:
        if threshold < 24:
            return f"{threshold:.0f}h"
        return f"{threshold / 24:.0f}d"

    headers = ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"]
    table_rows = []
    for brow in rows:
        if param == "mc_at_open":
            threshold_str = fmt_mc(brow.threshold)
        elif param == "token_age_hours":
            threshold_str = _fmt_age_threshold_t(brow.threshold)
        else:
            threshold_str = f"{brow.threshold:.0f}" if brow.threshold == int(brow.threshold) else f"{brow.threshold}"

        net_str = f"{brow.net_sol_impact:+.4f} SOL"
        table_rows.append([
            f">= {threshold_str}",
            str(brow.wins_kept),
            str(brow.wins_excluded),
            str(brow.losses_avoided),
            str(brow.losses_kept),
            net_str,
        ])

    print(md_table(headers, table_rows))


def run_custom_backtest(
    positions: List,
    param_value_strs: List[str],
    wallet: Optional[str] = None,
) -> None:
    """Parse --backtest arguments and run FilterBacktester sweeps, printing to terminal."""
    from valhalla.loss_analyzer import FilterBacktester

    backtester = FilterBacktester()
    PARAM_ALIASES = {
        'mc': 'mc_at_open',
        'mc_at_open': 'mc_at_open',
        'age': 'token_age_hours',
        'token_age': 'token_age_hours',
        'token_age_days': 'token_age_hours',
        'token_age_hours': 'token_age_hours',
        'jup': 'jup_score',
        'jup_score': 'jup_score',
    }

    for pv in param_value_strs:
        try:
            param_raw, value_str = pv.split('=', 1)
            param = PARAM_ALIASES.get(param_raw, param_raw)
            threshold = float(value_str)
        except ValueError:
            print(f"  Warning: could not parse --backtest argument '{pv}', expected param=value")
            continue

        rows = backtester.sweep(positions, param, thresholds=[threshold], wallet=wallet)
        print_backtest_table(param, rows)
