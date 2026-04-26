from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional

from valhalla.analysis_config import (
    REDUCE_CAPITAL_CONSECUTIVE_DAYS,
    MIN_POSITIONS_FOR_FILTER_REC,
)
from valhalla.models import parse_iso_datetime

from .filter_rules import run_rule_d


def generate_wallet_recommendations(positions: List) -> List[str]:
    """
    Analyze per-wallet daily data and return recommendation lines.

    Rules:
      A - "Verify or change tracked wallet":
          avg positions/day < 3 OR any single day has >= 2 loss events (rug/SL/failsafe)
      B - "Investigate underperformance":
          wallet has Daily PnL% ROI < 0.02% for >= 3 consecutive days
      C - "Consider increasing tracking level":
          wallet has >= 3 consecutive days where daily PnL SOL < portfolio avg SOL
          AND daily PnL% ROI > portfolio avg % ROI

    Args:
        positions: Full list of positions (all close_reasons).

    Returns:
        List of human-readable recommendation strings.
        Empty list if no recommendations.
    """
    LOSS_R = {
        "stop_loss", "rug", "rug_unknown_open",
        "failsafe", "failsafe_unknown_open",
        "stop_loss_unknown_open",
    }

    # Only closed positions with valid datetime_close and pnl_sol
    closed = [
        p for p in positions
        if p.close_reason != "still_open"
        and p.pnl_sol is not None
        and getattr(p, 'datetime_close', None)
        and p.datetime_close
    ]

    if not closed:
        return []

    # Compute reference date (most recent close) and 3-day cutoff
    all_close_dates = []
    for pos in closed:
        dt = parse_iso_datetime(pos.datetime_close)
        if dt is not None:
            all_close_dates.append(dt.date())

    if not all_close_dates:
        return []

    reference_date = max(all_close_dates)
    cutoff_3d = reference_date - timedelta(days=3)

    # Build: wallet -> date -> list of positions
    daily_by_wallet: Dict[str, Dict[date, List]] = defaultdict(lambda: defaultdict(list))
    for pos in closed:
        if getattr(pos, 'target_wallet', None) in (None, 'unknown'):
            continue
        dt = parse_iso_datetime(pos.datetime_close)
        if dt is None:
            continue
        daily_by_wallet[pos.target_wallet][dt.date()].append(pos)

    if not daily_by_wallet:
        return []

    # Compute portfolio averages per date
    # portfolio_avg_sol[d] = mean of all wallets' daily pnl_sol on date d
    # portfolio_avg_pct[d] = mean of all wallets' daily pnl_pct on date d (wallets with deployed > 0)
    all_dates: set = set()
    for wallet_days in daily_by_wallet.values():
        all_dates.update(wallet_days.keys())

    portfolio_avg_sol: Dict[date, float] = {}
    portfolio_avg_pct: Dict[date, float] = {}

    for d in all_dates:
        sol_vals = []
        pct_vals = []
        for wallet_days in daily_by_wallet.values():
            day_positions = wallet_days.get(d, [])
            if not day_positions:
                continue
            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            sol_vals.append(day_sol)
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )
            if day_deployed > 0:
                day_pct = day_sol / day_deployed * 100.0
                pct_vals.append(day_pct)
        if sol_vals:
            portfolio_avg_sol[d] = sum(sol_vals) / len(sol_vals)
        if pct_vals:
            portfolio_avg_pct[d] = sum(pct_vals) / len(pct_vals)

    recommendations: List[str] = []

    for wallet, wallet_days in daily_by_wallet.items():
        sorted_dates = sorted(wallet_days.keys())
        wallet_recs: List[str] = []

        # ----------------------------------------------------------------
        # Rule A: avg positions/day < 3 (over all history) OR any day
        #         within the recent 3-day window with >= 2 loss events
        # ----------------------------------------------------------------
        total_positions_count = sum(len(wallet_days[d]) for d in sorted_dates)
        num_days = len(sorted_dates)
        avg_per_day = total_positions_count / num_days if num_days > 0 else 0.0

        rule_a_reasons = []
        if avg_per_day < 3:
            rule_a_reasons.append(f"avg {avg_per_day:.1f} pos/day")

        # Only flag days within the 3-day window
        for d in sorted_dates:
            if d < cutoff_3d:
                continue
            loss_count_day = sum(
                1 for p in wallet_days[d] if p.close_reason in LOSS_R
            )
            if loss_count_day >= 2:
                rule_a_reasons.append(f"2+ losses on {d}")

        if rule_a_reasons:
            reason_str = ", ".join(rule_a_reasons)
            wallet_recs.append(
                f"{wallet}: Verify or change tracked wallet ({reason_str})"
            )

        # ----------------------------------------------------------------
        # Rule B: Daily PnL% ROI < 0.02% for >= 3 consecutive days
        # ----------------------------------------------------------------
        consec_b = 0
        b_start: Optional[date] = None
        b_end: Optional[date] = None
        best_b_streak = 0
        best_b_start: Optional[date] = None
        best_b_end: Optional[date] = None

        for d in sorted_dates:
            day_positions = wallet_days[d]
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )
            if day_deployed <= 0:
                # Reset streak - no deployed means can't compute ROI
                consec_b = 0
                b_start = None
                b_end = None
                continue

            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            day_pct = day_sol / day_deployed * 100.0

            if day_pct < 0.02:
                if consec_b == 0:
                    b_start = d
                b_end = d
                consec_b += 1
                if consec_b > best_b_streak:
                    best_b_streak = consec_b
                    best_b_start = b_start
                    best_b_end = b_end
            else:
                consec_b = 0
                b_start = None
                b_end = None

        # Only flag if the streak ends within the 3-day window
        if best_b_streak >= 3 and best_b_start and best_b_end and best_b_end >= cutoff_3d:
            wallet_recs.append(
                f"{wallet}: Investigate underperformance "
                f"(Daily ROI < 0.02% for {best_b_streak} consecutive days: "
                f"{best_b_start} to {best_b_end})"
            )

        # ----------------------------------------------------------------
        # Rule C: >= 3 consecutive days where daily PnL SOL < portfolio avg SOL
        #         AND daily PnL% ROI > portfolio avg % ROI
        # ----------------------------------------------------------------
        consec_c = 0
        c_start: Optional[date] = None
        c_end: Optional[date] = None
        best_c_streak = 0
        best_c_start: Optional[date] = None
        best_c_end: Optional[date] = None

        for d in sorted_dates:
            if d not in portfolio_avg_sol or d not in portfolio_avg_pct:
                consec_c = 0
                c_start = None
                c_end = None
                continue

            day_positions = wallet_days[d]
            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )

            if day_deployed <= 0:
                consec_c = 0
                c_start = None
                c_end = None
                continue

            day_pct = day_sol / day_deployed * 100.0
            port_sol = portfolio_avg_sol[d]
            port_pct = portfolio_avg_pct[d]

            if day_sol < port_sol and day_pct > port_pct:
                if consec_c == 0:
                    c_start = d
                c_end = d
                consec_c += 1
                if consec_c > best_c_streak:
                    best_c_streak = consec_c
                    best_c_start = c_start
                    best_c_end = c_end
            else:
                consec_c = 0
                c_start = None
                c_end = None

        # Only flag if the streak ends within the 3-day window
        if best_c_streak >= 3 and best_c_start and best_c_end and best_c_end >= cutoff_3d:
            wallet_recs.append(
                f"{wallet}: Consider increasing tracking level "
                f"(good ROI% but low absolute SOL for {best_c_streak} consecutive days: "
                f"{best_c_start} to {best_c_end})"
            )

        # ----------------------------------------------------------------
        # Rule F: wallet's daily pnl_pct < portfolio_avg_pct for N
        #         consecutive days -> reduce capital
        # ----------------------------------------------------------------
        consec_f = 0
        f_start: Optional[date] = None
        f_end: Optional[date] = None
        best_f_streak = 0
        best_f_start: Optional[date] = None
        best_f_end: Optional[date] = None

        for d in sorted_dates:
            # Only count days where BOTH wallet AND portfolio have pnl_pct data
            if d not in portfolio_avg_pct:
                consec_f = 0
                f_start = None
                f_end = None
                continue

            day_positions = wallet_days[d]
            day_sol = float(sum(p.pnl_sol for p in day_positions if p.pnl_sol is not None))
            day_deployed = sum(
                float(p.sol_deployed) for p in day_positions
                if getattr(p, 'sol_deployed', None) is not None
            )

            if day_deployed <= 0:
                # Skip days where we can't compute wallet pnl_pct
                consec_f = 0
                f_start = None
                f_end = None
                continue

            day_pct = day_sol / day_deployed * 100.0
            port_pct = portfolio_avg_pct[d]

            if day_pct < port_pct:
                if consec_f == 0:
                    f_start = d
                f_end = d
                consec_f += 1
                if consec_f > best_f_streak:
                    best_f_streak = consec_f
                    best_f_start = f_start
                    best_f_end = f_end
            else:
                consec_f = 0
                f_start = None
                f_end = None

        # Flag if streak >= threshold and ends within the 3-day window
        if (best_f_streak >= REDUCE_CAPITAL_CONSECUTIVE_DAYS
                and best_f_start and best_f_end
                and best_f_end >= cutoff_3d):
            wallet_recs.append(
                f"[REDUCE] {wallet}: underperforming (PnL%) for {best_f_streak} consecutive days "
                f"vs. portfolio avg — consider reducing capital"
            )

        recommendations.extend(wallet_recs)

    # ----------------------------------------------------------------
    # Rule D: sweet spot not at lowest threshold (per wallet + aggregate)
    # ----------------------------------------------------------------
    # Per-wallet Rule D (MIN_POSITIONS_FOR_FILTER_REC+ closed positions)
    for wallet in daily_by_wallet:
        wallet_all_positions = [
            p for p in positions
            if getattr(p, 'target_wallet', None) == wallet
        ]
        closed_wallet_d = [
            p for p in wallet_all_positions
            if p.close_reason not in ("still_open", "unknown_open")
        ]
        if len(closed_wallet_d) < MIN_POSITIONS_FOR_FILTER_REC:
            continue
        recommendations.extend(run_rule_d(wallet, wallet_all_positions))

    # Aggregate Rule D (Portfolio)
    all_closed_d = [
        p for p in positions
        if p.close_reason not in ("still_open", "unknown_open")
    ]
    if len(all_closed_d) >= 10:
        recommendations.extend(run_rule_d("Portfolio", positions))

    return recommendations
