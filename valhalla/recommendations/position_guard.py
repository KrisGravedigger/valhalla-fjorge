from collections import defaultdict
from datetime import timedelta
from decimal import Decimal
from typing import List

from valhalla.analysis_config import RECOMMENDATION_LOOKBACK_DAYS
from valhalla.models import parse_iso_datetime


def filter_recent_positions(positions: List, days: int) -> List:
    """Return only positions whose datetime_open falls within the last `days` days.
    If days <= 0, returns the full list unchanged.
    """
    if days <= 0 or not positions:
        return positions
    dates = [parse_iso_datetime(getattr(p, "datetime_open", None) or "") for p in positions]
    valid_dates = [d for d in dates if d is not None]
    if not valid_dates:
        return positions
    ref = max(valid_dates)
    cutoff = ref - timedelta(days=days)
    return [
        p for p, d in zip(positions, dates)
        if d is not None and d >= cutoff
    ]


def check_position_size_guard(
    positions: List,
    portfolio_sol: float,
    max_fraction: float,
) -> List[str]:
    """
    Check if any position exceeds max_fraction of portfolio_sol.

    Returns list of action item strings (warnings + recommendations).
    Empty list if portfolio_sol <= 0 (feature disabled).
    """
    if portfolio_sol <= 0:
        return []

    max_sol = Decimal(str(portfolio_sol)) * Decimal(str(max_fraction))

    # Find positions exceeding the limit (use RECOMMENDATION_LOOKBACK_DAYS window)
    recent = filter_recent_positions(positions, RECOMMENDATION_LOOKBACK_DAYS)

    oversized_by_wallet: dict = defaultdict(list)
    for pos in recent:
        deployed = getattr(pos, "sol_deployed", None)
        if deployed is not None and deployed > max_sol:
            oversized_by_wallet[pos.target_wallet].append(deployed)

    items: List[str] = []
    for wallet, sizes in oversized_by_wallet.items():
        largest = max(sizes)
        items.append(
            f"WARN {wallet}: position {largest:.3f} SOL exceeds "
            f"1/{round(1/max_fraction):.0f} portfolio limit ({max_sol:.2f} SOL) "
            f"— consider reducing position size"
        )
    return items
