"""
Persistent state tracking for loss_analysis recommendations.

Provides stable ID generation, JSON state persistence, and an interactive
CLI for marking recommendations as done or ignored across report runs.
"""

import hashlib
import json
import re
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple

STATE_VERSION = 1


# ---------------------------------------------------------------------------
# ID generation
# ---------------------------------------------------------------------------

def _normalize_key(item: str) -> str:
    """
    Extract a stable, category-specific key from a recommendation string.

    The key is independent of volatile parts (position counts, SOL amounts)
    so that the same logical recommendation gets the same ID across runs.
    """
    m = re.match(r'^(?:WARN\s+)?([^:]+):', item)
    wallet = m.group(1).strip() if m else ""

    il = item.lower()

    if "sweet spot" in il or "tightening" in il:
        param_m = re.search(r'(jup_score|mc_at_open|token_age_hours)', il)
        spot_m = re.search(r'sweet spot at >= ([^\s,\.]+)', il)
        param = param_m.group(1) if param_m else ""
        spot = spot_m.group(1) if spot_m else ""
        return f"{wallet}|filter|{param}|{spot}"

    if "increase capital" in il or "increasing capital" in il:
        return f"{wallet}|capital_increase"
    if "reduce capital" in il or "reducing capital" in il:
        return f"{wallet}|capital_reduce"
    if "insufficient balance" in il:
        return f"{wallet}|insuf_balance"
    if "high rug rate" in il:
        return f"{wallet}|high_rug"
    if "candidate for replacement" in il or "consider replacing" in il or "poor performance" in il:
        return f"{wallet}|replace"
    if "negative 7d pnl" in il:
        return f"{wallet}|negative_pnl"
    if "win rate declining" in il or "win rate dropped" in il:
        return f"{wallet}|wr_decline"
    if "deteriorating stop-loss" in il:
        return f"{wallet}|deteriorating_sl"
    if "verify or change" in il:
        return f"{wallet}|verify"
    if "no activity" in il:
        return f"{wallet}|inactive"
    if "pos/day" in il:
        return f"{wallet}|low_activity"
    if "portfolio limit" in il:
        return f"{wallet}|size_guard"

    # Fallback: hash first 80 chars of item
    return f"{wallet}|other|{item[:80]}"


def generate_id(item: str) -> str:
    """Return a stable 8-char hex ID for a recommendation item string."""
    key = _normalize_key(item)
    return hashlib.sha256(key.encode()).hexdigest()[:8]


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def load_state(path: str) -> Dict[str, Dict]:
    """
    Load recommendation state from a JSON file.

    Returns an empty dict if the file does not exist or is unreadable.
    Keys are 8-char recommendation IDs; values are dicts with 'status', etc.
    """
    p = Path(path)
    if not p.exists():
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("items", {})
    except (json.JSONDecodeError, KeyError, OSError):
        return {}


def save_state(path: str, state: Dict[str, Dict]) -> None:
    """Persist recommendation state to a JSON file."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump({"version": STATE_VERSION, "items": state}, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------

STATUS_NEW = "new"
STATUS_DONE = "done"
STATUS_IGNORED = "ignored"

VALID_STATUSES = {STATUS_NEW, STATUS_DONE, STATUS_IGNORED}

STATUS_BADGE = {
    STATUS_NEW: "[new]",
    STATUS_DONE: "[done]",
    STATUS_IGNORED: "[ignored]",
}


def get_item_status(item: str, state: Dict[str, Dict]) -> str:
    """Return the current status for an item string ('new', 'done', or 'ignored')."""
    rec_id = generate_id(item)
    entry = state.get(rec_id, {})
    status = entry.get("status", STATUS_NEW)
    return status if status in VALID_STATUSES else STATUS_NEW


def annotate_items(
    items: List[str],
    state: Dict[str, Dict],
) -> List[Tuple[str, str, str]]:
    """
    Return a list of (item_text, rec_id, status) for each item.

    Status is looked up from state; defaults to 'new' if not found.
    """
    result = []
    for item in items:
        rec_id = generate_id(item)
        status = get_item_status(item, state)
        result.append((item, rec_id, status))
    return result


# ---------------------------------------------------------------------------
# Interactive tracker CLI
# ---------------------------------------------------------------------------

def run_interactive_tracker(items: List[str], state_path: str) -> None:
    """
    Run an interactive CLI session for marking recommendation statuses.

    Displays all items with their current status and allows the user to
    mark each as done (d), ignored (i), or reset to new (n).

    Args:
        items: List of recommendation strings (e.g., from _build_action_items).
        state_path: Path to the JSON state file.
    """
    state = load_state(state_path)
    annotated = annotate_items(items, state)

    if not annotated:
        print("No recommendations to track.")
        return

    _print_tracker_list(annotated)

    print()
    print("Commands: NUMBER + d (done) | i (ignore) | n (new)   — e.g. 3d")
    print("Press Enter or q to quit without changes to that item.")
    print()

    updates = 0
    while True:
        try:
            inp = input("> ").strip().lower()
        except (KeyboardInterrupt, EOFError):
            print()
            break

        if inp in ("q", "quit", ""):
            break

        m = re.match(r"^(\d+)([din])$", inp)
        if not m:
            print("  Invalid command. Use NUMBER+d/i/n (e.g. 3d). Enter or q to quit.")
            continue

        idx = int(m.group(1)) - 1
        action = m.group(2)

        if idx < 0 or idx >= len(annotated):
            print(f"  No item #{idx + 1}.")
            continue

        item, rec_id, _ = annotated[idx]
        status_map = {"d": STATUS_DONE, "i": STATUS_IGNORED, "n": STATUS_NEW}
        new_status = status_map[action]

        state[rec_id] = {
            "status": new_status,
            "updated": str(date.today()),
            "text_preview": item[:120],
        }
        annotated[idx] = (item, rec_id, new_status)
        badge = STATUS_BADGE[new_status]
        print(f"  ✓ {rec_id} → {badge}")
        updates += 1

    if updates > 0:
        save_state(state_path, state)
        print(f"\nSaved. {updates} item(s) updated.")
    else:
        print("\nNo changes made.")


def _print_tracker_list(annotated: List[Tuple[str, str, str]]) -> None:
    """Print the numbered list of recommendations with IDs and status badges."""
    MAX_WIDTH = 90
    print()
    print("Recommendation Tracker")
    print("=" * 70)
    for idx, (item, rec_id, status) in enumerate(annotated, 1):
        badge = STATUS_BADGE.get(status, "[?]")
        truncated = item[:MAX_WIDTH] + "…" if len(item) > MAX_WIDTH else item
        print(f"[{idx:2d}] {rec_id}  {badge}  {truncated}")
    print()
