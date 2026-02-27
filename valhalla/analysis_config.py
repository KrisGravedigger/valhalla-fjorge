"""
Analysis thresholds for Valhalla Fjorge.

Edit this file to tune recommendations without touching analyzer code.
All values can be adjusted to match your trading setup and risk tolerance.
"""

# ---------------------------------------------------------------------------
# Wallet Scorecard (WalletScorecardAnalyzer)
# ---------------------------------------------------------------------------

# Minimum closed positions required for non-trivial wallet classification.
# Wallets below this count are marked as insufficient_data.
SCORECARD_MIN_POSITIONS: int = 30

# Days without a closed position to mark a wallet as inactive.
SCORECARD_INACTIVE_DAYS: int = 7

# Thresholds for "increase_capital" status (all must be met):
SCORECARD_INCREASE_WR_ALL: float = 60.0    # minimum overall win rate (%)
SCORECARD_INCREASE_WR_7D: float = 65.0     # minimum 7-day win rate (%)
SCORECARD_INCREASE_MAX_RUG: float = 8.0    # maximum rug rate (%)

# Threshold for "consider_replacing" status:
SCORECARD_REPLACE_WR_7D: float = 45.0      # 7-day win rate below this => flag for replacement

# ---------------------------------------------------------------------------
# Action Items (triggers in _build_action_items)
# ---------------------------------------------------------------------------

# Win rate decline (7d vs overall) that triggers a warning (percentage points).
ACTION_WIN_RATE_DECLINE_PP: float = 15.0

# Rug rate above this value triggers a high-rug warning (%).
ACTION_HIGH_RUG_RATE_PCT: float = 15.0

# ---------------------------------------------------------------------------
# Insufficient Balance recommendation (Rule E)
# ---------------------------------------------------------------------------

# Flag a wallet when its insufficient-balance events exceed this fraction
# of the wallet's total opened positions.
# Example: 0.10 means "more than 1 missed trade per 10 executed trades".
INSUF_BALANCE_RATE_THRESHOLD: float = 0.10
