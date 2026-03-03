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

# Days lookback for Wallet Scorecard table: only show wallets that opened
# at least one position within this window (counting back from the most
# recent datetime_open across all positions).
SCORECARD_RECENT_DAYS: int = 2

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

# Lookback window (in days) applied to ALL action-item recommendations:
#   - insufficient balance events and positions (Rule E)
#   - filter sweet-spot backtest (Rule D)
#   - wallet status classification (increase_capital / consider_replacing)
# Set to 0 to use all historical data.
RECOMMENDATION_LOOKBACK_DAYS: int = 7

# ---------------------------------------------------------------------------
# Portfolio-level position size guard (Feature 1)
# ---------------------------------------------------------------------------

# Your total portfolio value in SOL.
# Set to 0.0 to disable position size checking.
PORTFOLIO_TOTAL_SOL: float = 54.0

# Automatic SOL balance fetching is currently disabled because the on-chain balance
# does not include SOL locked in open Meteora positions.
# PORTFOLIO_WALLET_ADDRESS: str = "J4tkGDbTUVtAkcziKruadhRkP3A9HquvmBXK6bsSVArF"

# Maximum allowed fraction of portfolio in a single position.
# 0.125 = 1/8 of portfolio. Configurable.
MAX_POSITION_FRACTION: float = 0.125

# ---------------------------------------------------------------------------
# Rule F: consecutive underperformance → reduce capital (Feature 2)
# ---------------------------------------------------------------------------

# Number of consecutive days of worse PnL% than portfolio average that
# triggers a Rule F "reduce capital" recommendation.
REDUCE_CAPITAL_CONSECUTIVE_DAYS: int = 3

# ---------------------------------------------------------------------------
# Loss Detail Table (Feature 3)
# ---------------------------------------------------------------------------

# Minimum loss (in SOL) to include in the loss detail table.
LOSS_DETAIL_MIN_SOL: float = 0.1

# Lookback window (in days) for the loss detail table.
LOSS_DETAIL_LOOKBACK_DAYS: int = 3
