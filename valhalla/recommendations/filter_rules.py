from decimal import Decimal
from typing import List

from valhalla.analysis_config import (
    MIN_FILTER_GAIN_SOL,
    MIN_POSITIONS_FOR_FILTER_REC,
)


def _fmt_threshold_d(param: str, threshold: float) -> str:
    """Format threshold for Rule D messages."""
    if param == "mc_at_open":
        if threshold >= 1_000_000:
            return f"${threshold / 1_000_000:.1f}M"
        elif threshold >= 1_000:
            return f"${threshold / 1_000:.0f}K"
        return f"${threshold:.0f}"
    elif param == "token_age_hours":
        if threshold < 24:
            return f"{threshold:.0f}h"
        return f"{int(threshold // 24)}d"
    else:
        return f"{threshold:.0f}" if threshold == int(threshold) else str(threshold)


PARAM_DISPLAY_D = {
    "jup_score": "jup_score",
    "mc_at_open": "mc_at_open",
    "token_age_hours": "token_age_hours",
}


def run_rule_d(label: str, rule_d_positions: List) -> List[str]:
    """Run Rule D for a given set of positions; label is wallet name or 'Portfolio'."""
    from valhalla.loss_analyzer import FilterBacktester as _FilterBacktester

    rule_d_recs: List[str] = []
    bt = _FilterBacktester()
    bt_results = bt.sweep_all(rule_d_positions)
    for param, bt_rows in bt_results.items():
        if not bt_rows or len(bt_rows) < 2:
            continue
        # Skip params where all net_sol_impact <= 0
        if all(r.net_sol_impact <= Decimal("0") for r in bt_rows):
            continue
        # Find sweet spot (highest positive net_sol_impact)
        best_idx = None
        best_impact = Decimal("0")
        for i, brow in enumerate(bt_rows):
            if brow.net_sol_impact > best_impact:
                best_impact = brow.net_sol_impact
                best_idx = i
        if best_idx is None or best_idx == 0:
            continue  # sweet spot is already at lowest threshold - no recommendation
        if best_impact < Decimal(str(MIN_FILTER_GAIN_SOL)):
            continue  # net gain too small to be material
        sweet_threshold = bt_rows[best_idx].threshold
        min_threshold = bt_rows[0].threshold
        param_display = PARAM_DISPLAY_D.get(param, param)
        rule_d_recs.append(
            f"{label}: Consider tightening {param_display} filter — "
            f"sweet spot at >= {_fmt_threshold_d(param, sweet_threshold)}, "
            f"not the minimum (>= {_fmt_threshold_d(param, min_threshold)}). "
            f"Net gain: +{best_impact:.3f} SOL"
        )
    return rule_d_recs
