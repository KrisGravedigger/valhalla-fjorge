from datetime import datetime, timedelta
from decimal import Decimal
from typing import List

from valhalla.analysis_config import (
    MAX_POSITION_FRACTION,
    PORTFOLIO_TOTAL_SOL,
    RECOMMENDATION_LOOKBACK_DAYS,
    UTILIZATION_CONSECUTIVE_DAYS,
    UTILIZATION_LOW_THRESHOLD,
    UTILIZATION_MAX_INSUF_EVENTS_24H,
)
from valhalla.models import make_iso_datetime, parse_iso_datetime
from valhalla.recommendations import (
    check_position_size_guard,
    filter_recent_positions,
    generate_wallet_recommendations,
)


def build_action_items(
    result: object,
    positions: List,
    wallet_recs: object = None,
    insufficient_balance_events: List = None,
    util_points: List = None,
) -> List[str]:
    """
    Build a prioritized list of action item strings for the report.

    Combines scorecard-based triggers with the existing Rules A-D from
    generate_wallet_recommendations(), plus Rule E (insufficient balance).

    Priority order in output:
      0. position size guard warnings (Feature 1)
      1. consider_replacing wallets
      2. increase_capital wallets
      3. Rule F: consecutive underperformance → reduce capital
      4. insufficient balance warnings (Rule E)
      5. filter sweet-spot recommendations (Rule D)
      6. inactive wallets
      7. deteriorating wallets (WalletTrendAnalyzer flags)
      8. remaining A-B-C rules

    Args:
        result: LossAnalysisResult from LossAnalyzer.analyze().
        positions: Full list of MatchedPosition (passed to generate_wallet_recommendations).
        wallet_recs: Optional pre-computed wallet recommendations (reserved for doc 007).
        insufficient_balance_events: List[InsufficientBalanceEvent] from event parser.

    Returns:
        List of recommendation strings, each starting with a wallet name or
        "Portfolio:".
    """
    replacing: List[str] = []
    increasing: List[str] = []
    inactive_items: List[str] = []

    for sc in result.wallet_scorecards:
        # consider_replacing triggers
        if sc.status == "consider_replacing":
            if sc.pnl_7d_sol < Decimal("0"):
                replacing.append(
                    f"{sc.wallet}: negative 7d PnL ({sc.pnl_7d_sol:+.3f} SOL) "
                    f"across {sc.closed_positions} positions — candidate for replacement"
                )
            elif sc.win_rate_7d_pct is not None and sc.win_rate_7d_pct < 45.0:
                replacing.append(
                    f"{sc.wallet}: 7d win rate dropped to {sc.win_rate_7d_pct:.0f}% "
                    f"(overall: {sc.win_rate_pct:.0f}%) — consider replacing"
                )
            else:
                # fallback if neither sub-condition is specifically True
                wr_str = f"{sc.win_rate_7d_pct:.0f}%" if sc.win_rate_7d_pct is not None else "N/A"
                replacing.append(
                    f"{sc.wallet}: poor performance — candidate for replacement "
                    f"(PnL: {sc.total_pnl_sol:+.3f} SOL, WR 7d: {wr_str})"
                )

        # Win rate decline trigger (separate bullet, regardless of status)
        if sc.win_rate_trend_pp is not None and sc.win_rate_trend_pp < -15.0:
            replacing.append(
                f"{sc.wallet}: win rate declining — {sc.win_rate_7d_pct:.0f}% (7d) "
                f"vs {sc.win_rate_pct:.0f}% (overall)"
            )

        # High rug rate trigger (separate bullet, regardless of status)
        if sc.rug_rate_pct > 15.0:
            replacing.append(
                f"{sc.wallet}: high rug rate ({sc.rug_rate_pct:.0f}%) — "
                f"wallet trades riskier tokens"
            )

        # increase_capital trigger
        if sc.status == "increase_capital":
            wr_7d = sc.win_rate_7d_pct if sc.win_rate_7d_pct is not None else sc.win_rate_pct
            increasing.append(
                f"{sc.wallet}: 7d win rate {wr_7d:.0f}% across {sc.closed_positions} positions "
                f"— consider increasing capital"
            )

        # inactive trigger
        if sc.status == "inactive" and sc.days_since_last_position is not None:
            inactive_items.append(
                f"{sc.wallet}: no activity for {sc.days_since_last_position}+ days "
                f"— verify wallet is still active"
            )

    # Filter positions to the recommendation lookback window (applies to Rules A-D and Rule E)
    recent_positions = filter_recent_positions(positions, RECOMMENDATION_LOOKBACK_DAYS)

    # Rule E: Insufficient balance events
    insuf_items: List[str] = []
    if insufficient_balance_events:
        from valhalla.loss_analyzer import InsufficientBalanceAnalyzer
        ib_results = InsufficientBalanceAnalyzer().analyze(
            insufficient_balance_events, recent_positions
        )
        for ib in ib_results:
            rate_pct = ib.rate * 100
            insuf_items.append(
                f"{ib.wallet}: {ib.total_events} insufficient balance events "
                f"({rate_pct:.0f}% of {ib.total_positions} positions, "
                f"avg. required {ib.avg_required_sol:.2f} SOL) "
                f"— consider increasing SOL balance or decreasing position size"
            )

    # Utilization-based suggestion (Doc 009)
    utilization_items: List[str] = []
    if PORTFOLIO_TOTAL_SOL > 0 and util_points is not None:
        from valhalla.utilization import check_low_utilization_days
        low_util = check_low_utilization_days(
            util_points,
            Decimal(str(PORTFOLIO_TOTAL_SOL)),
            UTILIZATION_LOW_THRESHOLD,
            UTILIZATION_CONSECUTIVE_DAYS,
        )
        if low_util:
            # Count insuf-balance events in last 24h
            insuf_24h = 0
            if insufficient_balance_events:
                cutoff_dt = datetime.now() - timedelta(hours=24)
                cutoff_date = cutoff_dt.date()
                for ev in insufficient_balance_events:
                    # Support both InsufficientBalanceEvent (.date/.timestamp)
                    # and _InsuFEvent (.event_date) from CSV loader
                    ev_date = getattr(ev, "event_date", None)
                    if ev_date is not None:
                        # _InsuFEvent: event_date is datetime.date
                        if ev_date >= cutoff_date:
                            insuf_24h += 1
                    else:
                        # InsufficientBalanceEvent: has .date and .timestamp
                        ev_dt = parse_iso_datetime(
                            make_iso_datetime(ev.date, ev.timestamp) if ev.date else ev.timestamp
                        )
                        if ev_dt and ev_dt >= cutoff_dt:
                            insuf_24h += 1
            if insuf_24h <= UTILIZATION_MAX_INSUF_EVENTS_24H:
                # Find wallets with status "increase_capital"
                ic_wallets = [
                    sc.wallet for sc in result.wallet_scorecards
                    if sc.status == "increase_capital"
                ]
                for w in ic_wallets:
                    utilization_items.append(
                        f"{w}: capital utilization below "
                        f"{UTILIZATION_LOW_THRESHOLD*100:.0f}% for "
                        f"{UTILIZATION_CONSECUTIVE_DAYS} consecutive days — "
                        f"consider increasing capital per position"
                    )

    # Position size guard warnings (Feature 1) — highest priority
    size_guard_items = check_position_size_guard(positions, PORTFOLIO_TOTAL_SOL, MAX_POSITION_FRACTION)

    # Existing Rules A-D and Rule F (filtered to the same lookback window)
    existing_recs = generate_wallet_recommendations(recent_positions)

    # Extract Rule F items (prefixed with "[REDUCE] ") from recommendations
    reduce_capital_items = [
        r[len("[REDUCE] "):] for r in existing_recs if r.startswith("[REDUCE] ")
    ]
    non_reduce_recs = [r for r in existing_recs if not r.startswith("[REDUCE] ")]

    filter_recs = [r for r in non_reduce_recs if "sweet spot" in r.lower() or "tightening" in r.lower()]
    other_recs = [r for r in non_reduce_recs if r not in filter_recs]

    # Deteriorating flag from result.wallet_flags
    deteriorating_recs = [
        f"{wf.wallet}: deteriorating stop-loss rate — {wf.message}"
        for wf in result.wallet_flags
        if wf.flag == "deteriorating"
    ]

    all_items = (
        size_guard_items + replacing + increasing + reduce_capital_items
        + insuf_items + utilization_items + filter_recs
        + inactive_items + deteriorating_recs + other_recs
    )
    return all_items

def load_insuf_balance_csv(csv_path: str) -> List:
    """Load insufficient balance events from CSV.

    Returns simple objects with .target, .required_amount, .event_date (date | None).
    """
    import csv as _csv
    from datetime import date as _date
    from pathlib import Path

    class _InsuFEvent:
        __slots__ = ("target", "required_amount", "event_date")
        def __init__(self, target: str, required_amount: float, event_date):
            self.target = target
            self.required_amount = required_amount
            self.event_date = event_date  # datetime.date or None

    path = Path(csv_path)
    if not path.exists():
        return []
    events = []
    try:
        with open(path, newline="", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                target = row.get("target_wallet", "").strip()
                if not target:
                    continue
                try:
                    req = float(row.get("required_amount", 0))
                except ValueError:
                    req = 0.0
                # Parse event_date from the "datetime" column (ISO format: YYYY-MM-DDTHH:MM:SS)
                event_date = None
                raw_dt = row.get("datetime", "").strip()
                if raw_dt:
                    try:
                        event_date = _date.fromisoformat(raw_dt[:10])
                    except ValueError:
                        pass
                events.append(_InsuFEvent(target, req, event_date))
    except Exception:
        pass
    return events
