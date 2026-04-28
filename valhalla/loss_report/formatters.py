from decimal import Decimal
from typing import List, Optional


def scenario_label(scenario: Optional[str]) -> str:
    """Map source wallet scenario to a human-readable label."""
    return {
        "source_held_longer": "Held longer (SL too tight)",
        "source_exited_early": "Exited before us (copy lag)",
        "source_recovered": "Recovered (unclear mechanism)",
        "both_loss": "Both lost",
        "comparable": "Comparable outcome",
    }.get(scenario or "", "No data")

def fmt_sol(val: Optional[Decimal]) -> str:
    """Format SOL value or return 'N/A'."""
    return f"{val:.4f} SOL" if val is not None else "N/A"

def fmt_pct(val: Optional[float]) -> str:
    """Format percentage with sign or return 'N/A'."""
    return f"{val:+.1f}%" if val is not None else "N/A"

def fmt_mc(val: float) -> str:
    """Format market cap: 1_500_000 -> '$1.5M'."""
    if val >= 1_000_000:
        return f"${val / 1_000_000:.1f}M"
    elif val >= 1_000:
        return f"${val / 1_000:.0f}K"
    return f"${val:.0f}"

def scorecard_action_hints(wallet: str, action_items: List[str]) -> str:
    """Return brief comma-separated action hints for a wallet from the action items list."""
    hints: List[str] = []
    for item in action_items:
        if not (item.startswith(f"{wallet}:") or item.startswith(f"WARN {wallet}:")):
            continue
        il = item.lower()
        if "portfolio limit" in il:
            if "↓ size" not in hints:
                hints.append("↓ size")
        elif "candidate for replacement" in il or "consider replacing" in il or "poor performance" in il:
            if "replace" not in hints:
                hints.append("replace")
        elif "win rate declining" in il:
            if "↓ WR" not in hints:
                hints.append("↓ WR")
        elif "high rug rate" in il:
            if "high rug" not in hints:
                hints.append("high rug")
        elif "capital utilization below" in il:
            if "↑ capital (util)" not in hints:
                hints.append("↑ capital (util)")
        elif "increasing capital" in il or "increase capital" in il:
            if "↑ capital" not in hints:
                hints.append("↑ capital")
        elif "reduce capital" in il or "reducing capital" in il:
            if "↓ capital" not in hints:
                hints.append("↓ capital")
        elif "insufficient balance" in il:
            if "↑ SOL" not in hints:
                hints.append("↑ SOL")
        elif "sweet spot" in il or "tightening" in il:
            if "tighten filter" not in hints:
                hints.append("tighten filter")
        elif "verify or change" in il or "no activity" in il:
            if "verify" not in hints:
                hints.append("verify")
        elif "pos/day" in il:
            if "low activity" not in hints:
                hints.append("low activity")
        elif "deteriorating" in il:
            if "↑ SL" not in hints:
                hints.append("↑ SL")
    # Resolve contradictory capital signals
    if "↑ capital" in hints and "↓ capital" in hints:
        hints = [h for h in hints if h not in ("↑ capital", "↓ capital")]
        hints.append("⚠️ mixed capital signal")

    return ", ".join(hints) if hints else "—"
