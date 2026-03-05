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

def run_interactive_tracker(items: List[str], state_path: str) -> int:
    """
    Run an interactive CLI session for marking recommendation statuses.

    Default view shows only 'new' items. Type 'done' or 'ignore' to switch
    to those views; 'new' to return to the default view.

    Commands:
        NUMBER + d/i/n  — mark item in current view (e.g. 3d = mark #3 as done)
        done            — switch to 'done' view
        ignore          — switch to 'ignored' view
        new             — switch to 'new' view
        q / Enter       — quit

    Returns:
        Number of items whose status was updated (0 if no changes).
    """
    state = load_state(state_path)
    annotated = annotate_items(items, state)

    if not annotated:
        print("No recommendations to track.")
        return 0

    current_view = STATUS_NEW
    _print_tracker_list(annotated, current_view)

    print("Commands: NUMBER + d (done) | i (ignore) | n (new)  — e.g. 3d")
    print("Views:    'done' | 'ignore' | 'new'  — switch filter")
    print("'q' or Enter to quit.")
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

        # View switching
        if inp in ("new", "done", "ignore", "ignored"):
            if inp == "done":
                current_view = STATUS_DONE
            elif inp in ("ignore", "ignored"):
                current_view = STATUS_IGNORED
            else:
                current_view = STATUS_NEW
            _print_tracker_list(annotated, current_view)
            continue

        # Action command: Nd, Ni, Nn — operate on currently visible items
        m = re.match(r"^(\d+)([din])$", inp)
        if not m:
            print("  Invalid: use NUMBER+d/i/n (e.g. 3d) or 'done'/'ignore'/'new' to switch view.")
            continue

        visible = [
            (orig_i, item, rec_id, status)
            for orig_i, (item, rec_id, status) in enumerate(annotated)
            if status == current_view
        ]

        idx_in_view = int(m.group(1)) - 1
        action = m.group(2)

        if idx_in_view < 0 or idx_in_view >= len(visible):
            print(f"  No item #{idx_in_view + 1} in current view.")
            continue

        orig_i, item, rec_id, _ = visible[idx_in_view]
        status_map = {"d": STATUS_DONE, "i": STATUS_IGNORED, "n": STATUS_NEW}
        new_status = status_map[action]

        state[rec_id] = {
            "status": new_status,
            "updated": str(date.today()),
            "text_preview": item[:120],
        }
        annotated[orig_i] = (item, rec_id, new_status)
        badge = STATUS_BADGE[new_status]
        print(f"  ✓ {rec_id} → {badge}")
        updates += 1

        # Auto-refresh the current view (item may have left it)
        _print_tracker_list(annotated, current_view)

    if updates > 0:
        save_state(state_path, state)
        print(f"\nSaved. {updates} item(s) updated.")
    else:
        print("\nNo changes made.")

    return updates


def _print_tracker_list(
    annotated: List[Tuple[str, str, str]],
    view_filter: str = STATUS_NEW,
) -> None:
    """Print the filtered list of recommendations for the given status view."""
    MAX_WIDTH = 90
    visible = [t for t in annotated if t[2] == view_filter]
    badge_label = STATUS_BADGE.get(view_filter, view_filter)
    print()
    print(f"Recommendation Tracker — {badge_label} ({len(visible)} items)")
    print("=" * 70)
    for idx, (item, rec_id, status) in enumerate(visible, 1):
        badge = STATUS_BADGE.get(status, "[?]")
        truncated = item[:MAX_WIDTH] + "…" if len(item) > MAX_WIDTH else item
        print(f"[{idx:2d}] {rec_id}  {badge}  {truncated}")
    if not visible:
        print("  (none)")
    print()
