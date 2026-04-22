"""
Loss analysis module for Valhalla position data.

Provides six analysis classes that work on List[MatchedPosition]:
- LossAnalyzer: risk profiling of stop-loss positions vs all trades
- FilterBacktester: sweeps filter thresholds, calculates SOL impact
- StopLossLevelAnalyzer: distribution of losses by depth bucket
- WalletTrendAnalyzer: per-wallet stop-loss trend flags
- WalletScorecardAnalyzer: per-wallet performance metrics and actionable classification
- InsufficientBalanceAnalyzer: detects wallets with too many missed trades due to low SOL

All classes return structured data objects. No file I/O, no external calls.
Thresholds are read from valhalla.analysis_config (edit that file to tune behaviour).
"""

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

from .analysis_config import (
    SCORECARD_MIN_POSITIONS,
    SCORECARD_INACTIVE_DAYS,
    SCORECARD_INCREASE_WR_ALL,
    SCORECARD_INCREASE_WR_7D,
    SCORECARD_INCREASE_MAX_RUG,
    SCORECARD_REPLACE_WR_7D,
    INSUF_BALANCE_RATE_THRESHOLD,
    RECOMMENDATION_LOOKBACK_DAYS,
)
from .models import MatchedPosition, parse_iso_datetime


# Positions with these close reasons count as a "loss".
# stop_loss_unknown_open can persist in CSV if the open event was never matched.
LOSS_REASONS = {
    "stop_loss", "rug", "rug_unknown_open",
    "failsafe", "failsafe_unknown_open",
    "stop_loss_unknown_open",
}

# Only pure stop-loss exits (no rug/failsafe).
SL_ONLY_REASONS = {"stop_loss", "stop_loss_unknown_open"}


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RiskProfileRow:
    """Metric comparison: stop-loss group vs all closed positions."""
    metric: str                 # "jup_score", "mc_at_open", "token_age_hours"
    sl_avg: Optional[float]
    all_avg: Optional[float]
    diff_pct: Optional[float]   # (sl_avg - all_avg) / all_avg * 100
    sl_count: int               # positions with valid data in stop-loss group
    all_count: int              # positions with valid data in all group
    sl_rug_avg: Optional[float]             # avg for SL+Rug/Failsafe combined group
    sl_rug_count: int                       # positions with valid data in combined group
    sl_rug_diff_pct: Optional[float]        # (sl_rug_avg - all_avg) / all_avg * 100


@dataclass
class BacktestRow:
    """Result of a single threshold sweep step."""
    threshold: float            # e.g. 80 for jup_score >= 80
    wins_kept: int
    wins_excluded: int
    losses_avoided: int
    losses_kept: int
    net_sol_impact: Decimal     # losses_avoided_sol - wins_missed_sol
    trade_off_ratio: Optional[float]  # losses_avoided_sol / wins_missed_sol; None if no missed wins


@dataclass
class SLBucketRow:
    """Stop-loss level bucket result."""
    bucket_label: str           # e.g. "-5%"
    count: int                  # positions that would be saved at this bucket level
    sol_saved: Decimal          # extra SOL saved vs actual outcome
    description: str            # human-readable summary


@dataclass
class WalletFlag:
    """Per-wallet stop-loss trend flag."""
    wallet: str
    overall_sl_rate_pct: float
    recent_sl_rate_pct: float       # last 7 days (SL+Rug/Failsafe combined)
    recent_position_count: int
    flag: str                       # "deteriorating" | "ok" | "insufficient_data"
    message: str                    # e.g. "stop-loss rate 7d = 45% vs avg 18%"
    overall_sl_only_rate_pct: float = 0.0   # SL-only (no rug/failsafe)
    recent_sl_only_rate_pct: float = 0.0    # SL-only in last 7 days


@dataclass
class WalletScorecard:
    """Per-wallet scorecard with performance metrics and classification."""
    wallet: str
    total_positions: int
    closed_positions: int
    win_rate_pct: float
    win_rate_7d_pct: Optional[float]    # None if no closed positions in last 7d
    win_rate_24h_pct: Optional[float]   # None if no closed positions in last 24h
    win_rate_72h_pct: Optional[float]   # None if no closed positions in last 72h
    total_pnl_sol: Decimal
    pnl_7d_sol: Decimal
    pnl_per_day_sol: Decimal            # pnl_7d_sol / 7
    rug_rate_pct: float
    avg_hold_minutes: Optional[float]   # None if datetime fields missing/unparseable
    capital_efficiency: Optional[float] # total_pnl_sol / sum(sol_deployed); None if no sol_deployed
    consistency_score: Optional[float]  # max(|WR_24h - WR_all|, |WR_72h - WR_all|, |WR_7d - WR_all|) in pp; None if all sub-rates are None
    win_rate_trend_pp: Optional[float]  # win_rate_7d_pct - win_rate_pct (pp); None if win_rate_7d_pct is None
    status: str                         # see status classification below
    days_since_last_position: Optional[int]  # None if no datetime_close available
    # Rolling-window position counts (closed positions per window)
    positions_7d: int = 0
    positions_3d: int = 0
    positions_1d: int = 0
    # Rolling-window PnL
    pnl_3d_sol: Decimal = field(default_factory=lambda: Decimal("0"))
    pnl_1d_sol: Decimal = field(default_factory=lambda: Decimal("0"))
    # Rolling-window rug rates; None when window has 0 positions (not 0%)
    rug_rate_7d_pct: Optional[float] = None
    rug_rate_3d_pct: Optional[float] = None
    rug_rate_1d_pct: Optional[float] = None
    # Median pnl_sol per trade across all closed positions; None if no closed positions
    median_pnl_sol: Optional[Decimal] = None
    # Sum of sol_deployed across currently open positions
    current_exposure_sol: Decimal = field(default_factory=lambda: Decimal("0"))


@dataclass
class LossAnalysisResult:
    """Top-level result returned by LossAnalyzer.analyze()."""
    total_positions: int
    closed_positions: int               # excludes still_open
    loss_positions: int                 # by LOSS_REASONS
    stop_loss_positions: int            # close_reason == "stop_loss" only
    total_pnl_sol: Decimal
    loss_pnl_sol: Decimal
    risk_profile: List[RiskProfileRow]
    backtest_results: Dict[str, List[BacktestRow]]   # param_name -> rows
    sl_buckets: List[SLBucketRow]                    # all losses (SL+Rug/Failsafe)
    sl_buckets_sl_only: List[SLBucketRow]            # SL-only exits (without rugs)
    wallet_flags: List[WalletFlag]
    wallet_scorecards: List[WalletScorecard] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helper: metric value extractor
# ---------------------------------------------------------------------------

def _get_metric_value(pos: MatchedPosition, metric: str) -> Optional[float]:
    """
    Return the metric value for a position, or None if missing/invalid.

    Missing is defined as:
      - jup_score == 0
      - mc_at_open == 0.0
      - token_age_hours: token_age_hours is None
    """
    if metric == "jup_score":
        return float(pos.jup_score) if pos.jup_score != 0 else None
    elif metric == "mc_at_open":
        return pos.mc_at_open if pos.mc_at_open != 0.0 else None
    elif metric == "token_age_hours":
        return float(pos.token_age_hours) if pos.token_age_hours is not None else None
    return None


# ---------------------------------------------------------------------------
# LossAnalyzer
# ---------------------------------------------------------------------------

class LossAnalyzer:
    """
    Compare metric averages for stop-loss positions vs all closed positions
    and produce a full LossAnalysisResult.
    """

    def analyze(self, positions: List[MatchedPosition]) -> LossAnalysisResult:
        """
        Analyze a list of MatchedPosition objects and return structured results.

        Args:
            positions: Full list of positions (all close_reasons, including still_open).

        Returns:
            LossAnalysisResult with all sub-analysis populated.
        """
        total = len(positions)

        # Exclude still_open from closed analysis
        closed = [p for p in positions if p.close_reason != "still_open"]

        # Loss positions (by LOSS_REASONS = SL+Rug/Failsafe combined)
        loss_positions = [p for p in closed if p.close_reason in LOSS_REASONS]

        # Stop-loss only (no rug/failsafe)
        stop_loss_only = [p for p in closed if p.close_reason in SL_ONLY_REASONS]

        # SL+Rug/Failsafe combined group (= LOSS_REASONS)
        sl_rug_positions = [p for p in closed if p.close_reason in LOSS_REASONS]

        # PnL sums (only where pnl_sol is not None)
        total_pnl_sol = sum(
            (p.pnl_sol for p in closed if p.pnl_sol is not None),
            Decimal("0")
        )
        loss_pnl_sol = sum(
            (p.pnl_sol for p in loss_positions if p.pnl_sol is not None),
            Decimal("0")
        )

        # Sub-analyses
        risk_profile = self._risk_profile(stop_loss_only, sl_rug_positions, closed)
        backtest_results = FilterBacktester().sweep_all(positions)
        sl_buckets = StopLossLevelAnalyzer().analyze(positions)
        sl_buckets_sl_only = StopLossLevelAnalyzer().analyze(positions, reasons=SL_ONLY_REASONS)
        wallet_flags = WalletTrendAnalyzer().analyze(positions)
        wallet_scorecards = WalletScorecardAnalyzer().analyze(positions)

        return LossAnalysisResult(
            total_positions=total,
            closed_positions=len(closed),
            loss_positions=len(loss_positions),
            stop_loss_positions=len(stop_loss_only),
            total_pnl_sol=total_pnl_sol,
            loss_pnl_sol=loss_pnl_sol,
            risk_profile=risk_profile,
            backtest_results=backtest_results,
            sl_buckets=sl_buckets,
            sl_buckets_sl_only=sl_buckets_sl_only,
            wallet_flags=wallet_flags,
            wallet_scorecards=wallet_scorecards,
        )

    def _risk_profile(
        self,
        stop_loss_positions: List[MatchedPosition],
        sl_rug_positions: List[MatchedPosition],
        all_closed: List[MatchedPosition],
    ) -> List[RiskProfileRow]:
        """
        For each metric, compute averages for the SL-only group, SL+Rug/Failsafe group,
        and profitable trades only (pnl_sol > 0 and close_reason not in LOSS_REASONS).

        stop_loss_positions: positions where close_reason is in SL_ONLY_REASONS.
        sl_rug_positions: positions where close_reason is in LOSS_REASONS (combined).
        all_closed: all non-still_open positions. Used only to derive profitable subset.

        The comparison baseline is profitable positions only, not all closed positions.
        all_count and all_avg in RiskProfileRow refer to profitable positions.
        """
        # Compute profitable-only baseline (pnl_sol > 0 and not a loss)
        profitable = [
            p for p in all_closed
            if p.pnl_sol is not None
            and p.pnl_sol > Decimal("0")
            and p.close_reason not in LOSS_REASONS
        ]

        rows: List[RiskProfileRow] = []

        for metric in ["jup_score", "mc_at_open", "token_age_hours"]:
            sl_values = [
                v for p in stop_loss_positions
                if (v := _get_metric_value(p, metric)) is not None
            ]
            sl_rug_values = [
                v for p in sl_rug_positions
                if (v := _get_metric_value(p, metric)) is not None
            ]
            all_values = [
                v for p in profitable
                if (v := _get_metric_value(p, metric)) is not None
            ]

            sl_avg: Optional[float] = sum(sl_values) / len(sl_values) if sl_values else None
            sl_rug_avg: Optional[float] = sum(sl_rug_values) / len(sl_rug_values) if sl_rug_values else None
            all_avg: Optional[float] = sum(all_values) / len(all_values) if all_values else None

            diff_pct: Optional[float] = None
            if sl_avg is not None and all_avg is not None and all_avg != 0.0:
                diff_pct = (sl_avg - all_avg) / all_avg * 100.0

            sl_rug_diff_pct: Optional[float] = None
            if sl_rug_avg is not None and all_avg is not None and all_avg != 0.0:
                sl_rug_diff_pct = (sl_rug_avg - all_avg) / all_avg * 100.0

            rows.append(RiskProfileRow(
                metric=metric,
                sl_avg=sl_avg,
                all_avg=all_avg,
                diff_pct=diff_pct,
                sl_count=len(sl_values),
                all_count=len(all_values),
                sl_rug_avg=sl_rug_avg,
                sl_rug_count=len(sl_rug_values),
                sl_rug_diff_pct=sl_rug_diff_pct,
            ))

        return rows


# ---------------------------------------------------------------------------
# FilterBacktester
# ---------------------------------------------------------------------------

class FilterBacktester:
    """
    Sweep filter thresholds and calculate the SOL impact of each threshold.

    For a given parameter (e.g., jup_score), answers: "What if we had only
    taken trades where jup_score >= X?" and quantifies the net SOL impact.
    """

    DEFAULT_THRESHOLDS: Dict[str, List[float]] = {
        "jup_score": [70, 75, 80, 85, 90],
        "mc_at_open": [300_000, 500_000, 1_000_000, 3_000_000, 5_000_000],
        "token_age_hours": [1, 2, 3, 4, 5, 6, 8, 12, 24, 48],
    }

    def sweep(
        self,
        positions: List[MatchedPosition],
        param: str,
        thresholds: Optional[List[float]] = None,
        direction: str = "min",   # "min" means threshold is minimum required value
        wallet: Optional[str] = None,
    ) -> List[BacktestRow]:
        """
        Sweep thresholds for a single parameter and return a BacktestRow per threshold.

        Args:
            positions: Full position list.
            param: One of "jup_score", "mc_at_open", "token_age_hours".
            thresholds: List of threshold values to test. Defaults to DEFAULT_THRESHOLDS[param].
            direction: "min" (value >= threshold passes) or "max" (value <= threshold passes).
            wallet: If given, restrict analysis to this wallet only.

        Returns:
            List of BacktestRow, one per threshold.
        """
        if thresholds is None:
            thresholds = self.DEFAULT_THRESHOLDS.get(param, [])

        # Filter: exclude still_open and unknown_open; optionally restrict by wallet
        filtered = [
            p for p in positions
            if p.close_reason not in ("still_open", "unknown_open")
            and (wallet is None or p.target_wallet == wallet)
        ]

        rows: List[BacktestRow] = []

        for threshold in thresholds:
            wins_kept = 0
            wins_excluded = 0
            losses_avoided = 0
            losses_kept = 0
            wins_missed_sol = Decimal("0")
            losses_avoided_sol = Decimal("0")

            for pos in filtered:
                metric_val = _get_metric_value(pos, param)

                # Positions with invalid/missing metric always fail the filter
                if metric_val is None:
                    passes = False
                elif direction == "min":
                    passes = metric_val >= threshold
                else:  # "max"
                    passes = metric_val <= threshold

                is_win = (
                    pos.pnl_sol is not None
                    and pos.pnl_sol > Decimal("0")
                    and pos.close_reason not in LOSS_REASONS
                )
                is_loss = (
                    pos.close_reason in LOSS_REASONS
                    or (pos.pnl_sol is not None and pos.pnl_sol <= Decimal("0"))
                )

                if passes:
                    if is_win:
                        wins_kept += 1
                    elif is_loss:
                        losses_kept += 1
                    # else: neutral (pnl_sol is None and not a loss_reason) — not counted
                else:
                    if is_win:
                        wins_excluded += 1
                        if pos.pnl_sol is not None:
                            wins_missed_sol += pos.pnl_sol
                    elif is_loss:
                        losses_avoided += 1
                        if pos.pnl_sol is not None:
                            losses_avoided_sol += abs(pos.pnl_sol)

            net_sol_impact = losses_avoided_sol - wins_missed_sol
            trade_off_ratio: Optional[float] = (
                float(losses_avoided_sol) / float(wins_missed_sol)
                if wins_missed_sol > Decimal("0")
                else None
            )

            rows.append(BacktestRow(
                threshold=float(threshold),
                wins_kept=wins_kept,
                wins_excluded=wins_excluded,
                losses_avoided=losses_avoided,
                losses_kept=losses_kept,
                net_sol_impact=net_sol_impact,
                trade_off_ratio=trade_off_ratio,
            ))

        return rows

    def sweep_all(
        self,
        positions: List[MatchedPosition],
        wallet: Optional[str] = None,
    ) -> Dict[str, List[BacktestRow]]:
        """
        Run sweep() for all three default parameters.

        jup_score thresholds are computed dynamically from data (ceil5(min) to 100, step 5).
        Other parameters use DEFAULT_THRESHOLDS as-is.

        Returns:
            Dict mapping param name to list of BacktestRow.
        """
        results = {}
        for param in self.DEFAULT_THRESHOLDS:
            if param == "jup_score":
                thresholds = self._jup_score_thresholds(positions, wallet)
            else:
                thresholds = self.DEFAULT_THRESHOLDS[param]
            results[param] = self.sweep(positions, param, thresholds=thresholds, wallet=wallet)
        return results

    def _jup_score_thresholds(
        self,
        positions: List[MatchedPosition],
        wallet: Optional[str] = None,
    ) -> List[float]:
        """
        Compute jup_score thresholds dynamically: from ceil5(min) to 100, step 5.

        Falls back to DEFAULT_THRESHOLDS["jup_score"] if no valid scores are found.
        """
        import math
        filtered = [
            p for p in positions
            if p.close_reason not in ("still_open", "unknown_open")
            and (wallet is None or p.target_wallet == wallet)
        ]
        scores = [p.jup_score for p in filtered if p.jup_score and p.jup_score > 0]
        if not scores:
            return self.DEFAULT_THRESHOLDS["jup_score"]
        min_score = min(scores)
        # Round up to nearest 5
        start = math.ceil(min_score / 5) * 5
        return list(range(start, 101, 5))


# ---------------------------------------------------------------------------
# StopLossLevelAnalyzer
# ---------------------------------------------------------------------------

class StopLossLevelAnalyzer:
    """
    Show the distribution of loss depth and quantify SOL saved per tighter SL level.

    For each bucket B (e.g. -8%): calculates how much SOL would have been saved
    if the bot had exited at B% instead of the actual (deeper) loss.
    """

    BUCKETS = [-3, -5, -8, -10, -12, -15, -20]  # percentages (negative)

    def analyze(self, positions: List[MatchedPosition], reasons=None) -> List[SLBucketRow]:
        """
        Analyze stop-loss depth distribution.

        Args:
            positions: Full position list.
            reasons: Set of close_reason values to include. Defaults to LOSS_REASONS.

        Returns:
            One SLBucketRow per bucket level.
        """
        if reasons is None:
            reasons = LOSS_REASONS

        # Only loss positions with pnl_pct available
        loss_with_pct = [
            p for p in positions
            if p.close_reason in reasons
            and p.pnl_pct is not None
        ]

        rows: List[SLBucketRow] = []

        for bucket in self.BUCKETS:
            bucket_decimal = Decimal(str(bucket))

            # Positions where the actual loss was WORSE than bucket
            # (pnl_pct < bucket, e.g. pnl_pct=-15 < -8 => would have been saved)
            qualifying = [
                p for p in loss_with_pct
                if p.pnl_pct < bucket_decimal
            ]

            sol_saved = Decimal("0")
            for pos in qualifying:
                if pos.pnl_sol is None or pos.pnl_pct == Decimal("0"):
                    continue
                # How much LESS we'd have lost if we exited at bucket% instead of pnl_pct
                # saved = |pnl_sol| * (1 - bucket / pnl_pct)
                saved = abs(pos.pnl_sol) * (
                    Decimal("1") - bucket_decimal / pos.pnl_pct
                )
                sol_saved += saved

            count = len(qualifying)
            description = (
                f"If SL at {bucket}%: saved {sol_saved:.3f} SOL on {count} position(s)"
            )

            rows.append(SLBucketRow(
                bucket_label=f"{bucket}%",
                count=count,
                sol_saved=sol_saved,
                description=description,
            ))

        return rows


# ---------------------------------------------------------------------------
# WalletTrendAnalyzer
# ---------------------------------------------------------------------------

class WalletTrendAnalyzer:
    """
    Per-wallet stop-loss trend analysis.

    Flags wallets whose recent (7-day) stop-loss rate has deteriorated
    significantly compared to their overall historical rate.
    """

    RECENT_DAYS = 7
    MIN_POSITIONS_FOR_FLAG = 3  # minimum recent positions to issue a flag

    def analyze(
        self,
        positions: List[MatchedPosition],
        reference_date: Optional[datetime] = None,
        include_all: bool = False,
    ) -> List[WalletFlag]:
        """
        Analyze per-wallet stop-loss trends.

        Args:
            positions: Full position list.
            reference_date: Cutoff for "recent" window. Defaults to max datetime_close.
            include_all: If True, return all wallets. If False (default), return only
                         wallets flagged as "deteriorating".

        Returns:
            List of WalletFlag objects.
        """
        # Only closed positions are relevant
        closed = [p for p in positions if p.close_reason != "still_open"]

        if not closed:
            return []

        # Determine reference date
        if reference_date is None:
            reference_date = self._max_close_date(closed)

        if reference_date is None:
            return []

        cutoff = reference_date - timedelta(days=self.RECENT_DAYS)

        # Group by wallet
        wallets: Dict[str, List[MatchedPosition]] = {}
        for pos in closed:
            wallets.setdefault(pos.target_wallet, []).append(pos)

        flags: List[WalletFlag] = []

        for wallet, wallet_positions in wallets.items():
            total_count = len(wallet_positions)
            loss_count = sum(1 for p in wallet_positions if p.close_reason in LOSS_REASONS)
            overall_sl_rate = loss_count / total_count if total_count > 0 else 0.0

            # SL-only rate (no rug/failsafe)
            sl_only_count = sum(1 for p in wallet_positions if p.close_reason in SL_ONLY_REASONS)
            overall_sl_only_rate = sl_only_count / total_count if total_count > 0 else 0.0

            # Recent positions: datetime_close within RECENT_DAYS of reference_date
            recent_positions = [
                p for p in wallet_positions
                if self._is_recent(p.datetime_close, cutoff)
            ]
            recent_count = len(recent_positions)
            recent_loss_count = sum(
                1 for p in recent_positions if p.close_reason in LOSS_REASONS
            )
            recent_sl_rate = (
                recent_loss_count / recent_count if recent_count > 0 else 0.0
            )

            # Recent SL-only rate
            recent_sl_only_count = sum(
                1 for p in recent_positions if p.close_reason in SL_ONLY_REASONS
            )
            recent_sl_only_rate = (
                recent_sl_only_count / recent_count if recent_count > 0 else 0.0
            )

            # Determine flag
            if recent_count < self.MIN_POSITIONS_FOR_FLAG:
                flag = "insufficient_data"
                message = (
                    f"only {recent_count} recent position(s), need {self.MIN_POSITIONS_FOR_FLAG}"
                )
            elif (
                recent_sl_rate > overall_sl_rate * 1.5
                and recent_sl_rate > 0.3
            ):
                flag = "deteriorating"
                message = (
                    f"stop-loss rate 7d = {recent_sl_rate * 100:.0f}% "
                    f"vs avg {overall_sl_rate * 100:.0f}%"
                )
            else:
                flag = "ok"
                message = (
                    f"stop-loss rate 7d = {recent_sl_rate * 100:.0f}% "
                    f"vs avg {overall_sl_rate * 100:.0f}%"
                )

            wallet_flag = WalletFlag(
                wallet=wallet,
                overall_sl_rate_pct=overall_sl_rate * 100.0,
                recent_sl_rate_pct=recent_sl_rate * 100.0,
                recent_position_count=recent_count,
                flag=flag,
                message=message,
                overall_sl_only_rate_pct=overall_sl_only_rate * 100.0,
                recent_sl_only_rate_pct=recent_sl_only_rate * 100.0,
            )

            if include_all or flag == "deteriorating":
                flags.append(wallet_flag)

        return flags

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _max_close_date(closed: List[MatchedPosition]) -> Optional[datetime]:
        """Return the latest datetime_close across all closed positions, or None."""
        latest: Optional[datetime] = None
        for pos in closed:
            dt = parse_iso_datetime(pos.datetime_close)
            if dt is not None:
                if latest is None or dt > latest:
                    latest = dt
        return latest

    @staticmethod
    def _is_recent(datetime_close: str, cutoff: datetime) -> bool:
        """Return True if datetime_close parses to a datetime >= cutoff."""
        dt = parse_iso_datetime(datetime_close)
        if dt is None:
            return False
        return dt >= cutoff


# ---------------------------------------------------------------------------
# WalletScorecardAnalyzer
# ---------------------------------------------------------------------------

class WalletScorecardAnalyzer:
    """
    Per-wallet scorecard: performance metrics and actionable classification.

    Minimum data for non-trivial classification: >= 30 closed positions.
    """

    MIN_POSITIONS = SCORECARD_MIN_POSITIONS
    INACTIVE_DAYS = SCORECARD_INACTIVE_DAYS
    WIN_RATE_INCREASE_THRESHOLD = SCORECARD_INCREASE_WR_ALL
    WIN_RATE_7D_INCREASE_THRESHOLD = SCORECARD_INCREASE_WR_7D
    MAX_RUG_RATE_FOR_INCREASE = SCORECARD_INCREASE_MAX_RUG
    WIN_RATE_7D_REPLACE_THRESHOLD = SCORECARD_REPLACE_WR_7D

    def analyze(
        self,
        positions: List[MatchedPosition],
        reference_date: Optional[datetime] = None,
    ) -> List[WalletScorecard]:
        """
        Compute per-wallet scorecards from a list of positions.

        Args:
            positions: Full list of positions (all close_reasons, including still_open).
            reference_date: Reference point for recency windows. Defaults to the
                            maximum datetime_close found in closed positions.

        Returns:
            List of WalletScorecard, sorted by pnl_per_day_sol descending.
        """
        RUG_REASONS = {"rug", "rug_unknown_open"}

        # 1. Filter to closed positions
        closed = [p for p in positions if p.close_reason != "still_open"]

        # 2. Determine reference_date
        if reference_date is None:
            dates = [
                parse_iso_datetime(p.datetime_close)
                for p in closed
                if parse_iso_datetime(p.datetime_close) is not None
            ]
            reference_date = max(dates) if dates else datetime.utcnow()

        # 3. Compute time cutoffs
        cutoff_7d = reference_date - timedelta(days=7)
        cutoff_72h = reference_date - timedelta(hours=72)
        cutoff_24h = reference_date - timedelta(hours=24)
        cutoff_3d = reference_date - timedelta(days=3)

        # 4. Group all positions by wallet (including still_open for total_positions)
        all_by_wallet: Dict[str, List[MatchedPosition]] = {}
        for pos in positions:
            all_by_wallet.setdefault(pos.target_wallet, []).append(pos)

        # Also group closed positions by wallet
        closed_by_wallet: Dict[str, List[MatchedPosition]] = {}
        for pos in closed:
            closed_by_wallet.setdefault(pos.target_wallet, []).append(pos)

        # Union of wallets seen in either group
        all_wallets = set(all_by_wallet.keys()) | set(closed_by_wallet.keys())

        # 5. Compute scorecard per wallet
        scorecards: List[WalletScorecard] = []

        for wallet in all_wallets:
            wallet_all = all_by_wallet.get(wallet, [])
            wallet_closed = closed_by_wallet.get(wallet, [])

            total_positions = len(wallet_all)
            closed_positions = len(wallet_closed)

            # Win definition: pnl_sol > 0 AND close_reason NOT in LOSS_REASONS
            def is_win(p: MatchedPosition) -> bool:
                return (
                    p.pnl_sol is not None
                    and p.pnl_sol > Decimal("0")
                    and p.close_reason not in LOSS_REASONS
                )

            # win_rate_pct (all closed)
            wins = sum(1 for p in wallet_closed if is_win(p))
            win_rate_pct = wins / closed_positions * 100.0 if closed_positions > 0 else 0.0

            # Helper: win_rate for a recent subset
            def _win_rate_recent(cutoff: datetime) -> Optional[float]:
                recent = [
                    p for p in wallet_closed
                    if parse_iso_datetime(p.datetime_close) is not None
                    and parse_iso_datetime(p.datetime_close) >= cutoff
                ]
                if not recent:
                    return None
                return sum(1 for p in recent if is_win(p)) / len(recent) * 100.0

            win_rate_7d_pct = _win_rate_recent(cutoff_7d)
            win_rate_72h_pct = _win_rate_recent(cutoff_72h)
            win_rate_24h_pct = _win_rate_recent(cutoff_24h)

            # total_pnl_sol
            total_pnl_sol = sum(
                (p.pnl_sol for p in wallet_closed if p.pnl_sol is not None),
                Decimal("0"),
            )

            # pnl_7d_sol
            pnl_7d_sol = sum(
                (
                    p.pnl_sol
                    for p in wallet_closed
                    if p.pnl_sol is not None
                    and parse_iso_datetime(p.datetime_close) is not None
                    and parse_iso_datetime(p.datetime_close) >= cutoff_7d
                ),
                Decimal("0"),
            )

            # pnl_per_day_sol
            pnl_per_day_sol = pnl_7d_sol / Decimal("7")

            # rug_rate_pct
            rugs = sum(1 for p in wallet_closed if p.close_reason in RUG_REASONS)
            rug_rate_pct = rugs / closed_positions * 100.0 if closed_positions > 0 else 0.0

            # avg_hold_minutes
            hold_times = []
            for p in wallet_closed:
                dt_open = parse_iso_datetime(p.datetime_open)
                dt_close = parse_iso_datetime(p.datetime_close)
                if dt_open is not None and dt_close is not None and dt_close > dt_open:
                    hold_times.append((dt_close - dt_open).total_seconds() / 60.0)
            avg_hold_minutes = sum(hold_times) / len(hold_times) if hold_times else None

            # capital_efficiency
            deployed_vals = [
                p.sol_deployed
                for p in wallet_closed
                if p.sol_deployed is not None and p.sol_deployed > Decimal("0")
            ]
            if not deployed_vals:
                capital_efficiency = None
            else:
                sum_deployed = sum(deployed_vals, Decimal("0"))
                capital_efficiency = (
                    float(total_pnl_sol / sum_deployed)
                    if sum_deployed > Decimal("0")
                    else None
                )

            # consistency_score
            deviations = []
            for rate in [win_rate_24h_pct, win_rate_72h_pct, win_rate_7d_pct]:
                if rate is not None:
                    deviations.append(abs(rate - win_rate_pct))
            consistency_score = max(deviations) if deviations else None

            # win_rate_trend_pp
            win_rate_trend_pp = (
                win_rate_7d_pct - win_rate_pct if win_rate_7d_pct is not None else None
            )

            # days_since_last_position
            close_dates = [
                parse_iso_datetime(p.datetime_close)
                for p in wallet_closed
                if parse_iso_datetime(p.datetime_close) is not None
            ]
            if close_dates:
                last_close = max(close_dates)
                days_since_last_position = (reference_date - last_close).days
            else:
                days_since_last_position = None

            # positions_Xd: count of closed positions within each rolling window
            def _window_closed(cutoff: datetime) -> List:
                return [
                    p for p in wallet_closed
                    if parse_iso_datetime(p.datetime_close) is not None
                    and parse_iso_datetime(p.datetime_close) >= cutoff
                ]

            window_7d = _window_closed(cutoff_7d)
            window_3d = _window_closed(cutoff_3d)
            window_1d = _window_closed(cutoff_24h)

            positions_7d = len(window_7d)
            positions_3d = len(window_3d)
            positions_1d = len(window_1d)

            # pnl_3d_sol, pnl_1d_sol
            pnl_3d_sol = sum(
                (p.pnl_sol for p in window_3d if p.pnl_sol is not None),
                Decimal("0"),
            )
            pnl_1d_sol = sum(
                (p.pnl_sol for p in window_1d if p.pnl_sol is not None),
                Decimal("0"),
            )

            # rug_rate_Xd_pct: None when window has 0 positions
            def _rug_rate(window: List) -> Optional[float]:
                if not window:
                    return None
                rugs_w = sum(1 for p in window if p.close_reason in RUG_REASONS)
                return rugs_w / len(window) * 100.0

            rug_rate_7d_pct = _rug_rate(window_7d)
            rug_rate_3d_pct = _rug_rate(window_3d)
            rug_rate_1d_pct = _rug_rate(window_1d)

            # median_pnl_sol across all closed positions with a pnl value
            pnl_values = [p.pnl_sol for p in wallet_closed if p.pnl_sol is not None]
            if pnl_values:
                sorted_vals = sorted(pnl_values)
                n = len(sorted_vals)
                mid = n // 2
                median_pnl_sol = sorted_vals[mid] if n % 2 != 0 else (sorted_vals[mid - 1] + sorted_vals[mid]) / Decimal("2")
            else:
                median_pnl_sol = None

            # current_exposure_sol: sum of sol_deployed for open positions
            current_exposure_sol = sum(
                (p.sol_deployed for p in wallet_all
                 if p.close_reason == "still_open" and p.sol_deployed is not None),
                Decimal("0"),
            )

            # Status classification (priority order, first match wins)
            if (
                days_since_last_position is not None
                and days_since_last_position >= self.INACTIVE_DAYS
            ):
                status = "inactive"
            elif closed_positions < self.MIN_POSITIONS:
                status = "insufficient_data"
            elif (
                closed_positions >= self.MIN_POSITIONS
                and win_rate_7d_pct is not None
                and win_rate_7d_pct >= self.WIN_RATE_7D_INCREASE_THRESHOLD
                and win_rate_pct >= self.WIN_RATE_INCREASE_THRESHOLD
                and pnl_7d_sol > Decimal("0")
                and rug_rate_pct < self.MAX_RUG_RATE_FOR_INCREASE
            ):
                status = "increase_capital"
            elif closed_positions >= self.MIN_POSITIONS and (
                pnl_7d_sol < Decimal("0")
                or (
                    win_rate_7d_pct is not None
                    and win_rate_7d_pct < self.WIN_RATE_7D_REPLACE_THRESHOLD
                )
            ):
                status = "consider_replacing"
            else:
                status = "monitor"

            scorecards.append(WalletScorecard(
                wallet=wallet,
                total_positions=total_positions,
                closed_positions=closed_positions,
                win_rate_pct=win_rate_pct,
                win_rate_7d_pct=win_rate_7d_pct,
                win_rate_24h_pct=win_rate_24h_pct,
                win_rate_72h_pct=win_rate_72h_pct,
                total_pnl_sol=total_pnl_sol,
                pnl_7d_sol=pnl_7d_sol,
                pnl_per_day_sol=pnl_per_day_sol,
                rug_rate_pct=rug_rate_pct,
                avg_hold_minutes=avg_hold_minutes,
                capital_efficiency=capital_efficiency,
                consistency_score=consistency_score,
                win_rate_trend_pp=win_rate_trend_pp,
                status=status,
                days_since_last_position=days_since_last_position,
                positions_7d=positions_7d,
                positions_3d=positions_3d,
                positions_1d=positions_1d,
                pnl_3d_sol=pnl_3d_sol,
                pnl_1d_sol=pnl_1d_sol,
                rug_rate_7d_pct=rug_rate_7d_pct,
                rug_rate_3d_pct=rug_rate_3d_pct,
                rug_rate_1d_pct=rug_rate_1d_pct,
                median_pnl_sol=median_pnl_sol,
                current_exposure_sol=current_exposure_sol,
            ))

        # 6. Sort by pnl_per_day_sol descending (Decimal comparison)
        scorecards.sort(key=lambda s: s.pnl_per_day_sol, reverse=True)

        return scorecards


# ---------------------------------------------------------------------------
# InsufficientBalanceAnalyzer
# ---------------------------------------------------------------------------

@dataclass
class InsufficientBalanceResult:
    """Analysis result for a wallet with excessive insufficient-balance events."""
    wallet: str
    total_events: int           # total insufficient-balance events for this wallet
    total_positions: int        # total opened positions for this wallet
    rate: float                 # total_events / total_positions
    avg_required_sol: float     # average SOL required per missed trade


class InsufficientBalanceAnalyzer:
    """
    Detects wallets where insufficient-balance events are excessive relative
    to the number of positions opened.

    The rate is computed as: insuf-balance events / positions within the
    lookback window. Both are filtered to the same time window so the
    comparison is apples-to-apples.
    """

    def analyze(
        self,
        events: list,           # List with .target, .required_amount, .event_date (date)
        positions: list,        # List[MatchedPosition]
        threshold: float = INSUF_BALANCE_RATE_THRESHOLD,
        lookback_days: int = RECOMMENDATION_LOOKBACK_DAYS,
    ) -> List["InsufficientBalanceResult"]:
        """
        Return flagged wallets sorted by rate descending.

        If lookback_days > 0, only events and positions within the last
        lookback_days days (relative to the most recent event date) are
        considered. Set lookback_days=0 to use all historical data.

        Only wallets with at least one position in the window are considered.
        """
        from collections import defaultdict
        from datetime import date as _date

        # Determine reference date and cutoff
        cutoff: Optional[_date] = None
        if lookback_days > 0:
            event_dates = [
                getattr(ev, "event_date", None)
                for ev in events
                if getattr(ev, "event_date", None) is not None
            ]
            if event_dates:
                ref_date = max(event_dates)
                cutoff = ref_date - timedelta(days=lookback_days - 1)

        # Filter events by window
        filtered_events = [
            ev for ev in events
            if cutoff is None or (
                getattr(ev, "event_date", None) is not None
                and ev.event_date >= cutoff
            )
        ]

        # Filter positions by window (using datetime_open)
        def _pos_date(pos) -> Optional[_date]:
            dt = parse_iso_datetime(getattr(pos, "datetime_open", None) or "")
            return dt.date() if dt else None

        filtered_positions = [
            pos for pos in positions
            if cutoff is None or (
                _pos_date(pos) is not None and _pos_date(pos) >= cutoff
            )
        ]

        events_per_wallet: Dict[str, list] = defaultdict(list)
        for ev in filtered_events:
            target = getattr(ev, "target", None)
            if target:
                events_per_wallet[target].append(ev)

        positions_per_wallet: Dict[str, int] = defaultdict(int)
        for pos in filtered_positions:
            wallet = getattr(pos, "target_wallet", None)
            if wallet:
                positions_per_wallet[wallet] += 1

        results: List[InsufficientBalanceResult] = []
        for wallet, wallet_events in events_per_wallet.items():
            total_pos = positions_per_wallet.get(wallet, 0)
            if total_pos == 0:
                continue  # can't compute a meaningful ratio
            rate = len(wallet_events) / total_pos
            if rate > threshold:
                avg_req = sum(
                    getattr(ev, "required_amount", 0.0) for ev in wallet_events
                ) / len(wallet_events)
                results.append(InsufficientBalanceResult(
                    wallet=wallet,
                    total_events=len(wallet_events),
                    total_positions=total_pos,
                    rate=rate,
                    avg_required_sol=avg_req,
                ))

        results.sort(key=lambda r: r.rate, reverse=True)
        return results
