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
# Also controls the chart retirement filter (wallets inactive > this many days are hidden).
SCORECARD_INACTIVE_DAYS: int = 3

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
RECOMMENDATION_LOOKBACK_DAYS: int = 5

# ---------------------------------------------------------------------------
# Recommendation quality filters
# ---------------------------------------------------------------------------

# Minimum net gain (SOL) for a filter tightening recommendation to appear.
# Recommendations below this threshold are omitted as not material enough.
MIN_FILTER_GAIN_SOL: float = 0.02

# Minimum number of closed positions a wallet must have for filter
# tightening recommendations (Rule D) to be generated.
# Small samples produce unreliable backtest results.
MIN_POSITIONS_FOR_FILTER_REC: int = 100

# ---------------------------------------------------------------------------
# Portfolio-level position size guard (Feature 1)
# ---------------------------------------------------------------------------

# Your total portfolio value in SOL.
# Set to 0.0 to disable position size checking.
PORTFOLIO_TOTAL_SOL: float = 58.0

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

# ---------------------------------------------------------------------------
# Daily PnL Breakdown chart (stacked bar per wallet per day)
# ---------------------------------------------------------------------------

# Number of days to show in the daily_pnl_breakdown.png chart.
PNL_BREAKDOWN_LOOKBACK_DAYS: int = 3

# ---------------------------------------------------------------------------
# Hourly Capital Utilization (Doc 009)
# ---------------------------------------------------------------------------

# Lookback window for the utilization chart (hours).
UTILIZATION_LOOKBACK_HOURS: int = 72

# Utilization below this fraction of PORTFOLIO_TOTAL_SOL triggers a suggestion.
# 0.80 = 80% of portfolio must be deployed to be considered "well utilized".
UTILIZATION_LOW_THRESHOLD: float = 0.80

# Number of consecutive days below threshold before triggering the suggestion.
UTILIZATION_CONSECUTIVE_DAYS: int = 3

# Maximum insufficient-balance events in the last 24 hours before suppressing
# the "increase capital" suggestion (if SOL is thin, don't suggest deploying more).
UTILIZATION_MAX_INSUF_EVENTS_24H: int = 10

# ---------------------------------------------------------------------------
# Source Wallet Analysis
# ---------------------------------------------------------------------------

# Minimum loss (%) for a position to be eligible for source wallet analysis.
# Only positions with pnl_pct <= this value are analyzed (e.g. -5.0 means 5%+ loss).
# Set to 0.0 to analyze all losing positions, or None to analyze everything.
SOURCE_WALLET_MIN_LOSS_PCT: float = -5.0

# ---------------------------------------------------------------------------
# Filter Impact Analysis Chart
# ---------------------------------------------------------------------------
FILTER_IMPACT_LOOKBACK_DAYS: int = 3
