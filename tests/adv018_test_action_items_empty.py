"""
Adversarial test for doc 018 AC-2 build_action_items empty input behavior.

Uses inspect.signature for the public function and calls it with empty
collections. The result should be an empty list, not an exception or a
placeholder action.
"""
import inspect
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class EmptyResult:
    total_loss_sol = Decimal("0")
    total_pnl_sol = Decimal("0")
    total_positions = 0
    closed_positions = 0
    loss_positions = 0
    wins = 0
    losses = 0
    by_wallet = {}
    by_reason = {}
    wallet_stats = {}
    reason_stats = {}
    loss_reasons = {}
    filter_backtests = []

    def __getattr__(self, name):
        if name.endswith("_sol") or name.endswith("_pct") or name.startswith("total_"):
            return Decimal("0")
        if name.endswith("_count") or name.endswith("count"):
            return 0
        return {}


def main():
    from valhalla.loss_report import build_action_items

    signature = inspect.signature(build_action_items)
    expected = ["result", "positions", "wallet_recs", "insufficient_balance_events", "util_points"]
    actual = list(signature.parameters)
    if actual != expected:
        print(f"FAIL: build_action_items signature changed. expected {expected}, got {actual}")
        sys.exit(1)

    try:
        items = build_action_items(EmptyResult(), [], [], [], [])
    except Exception as exc:
        print(f"FAIL: build_action_items crashed on empty inputs: {type(exc).__name__}: {exc}")
        sys.exit(1)

    if items != []:
        print(f"FAIL: expected empty list for empty inputs, got {items!r}")
        sys.exit(1)

    print("PASS: build_action_items returns [] for empty inputs")
    sys.exit(0)


if __name__ == "__main__":
    main()
