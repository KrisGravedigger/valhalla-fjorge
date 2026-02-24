"""
Loss analysis module for Valhalla position data.

Provides four analysis classes that work on List[MatchedPosition]:
- LossAnalyzer: risk profiling of stop-loss positions vs all trades
- FilterBacktester: sweeps filter thresholds, calculates SOL impact
- StopLossLevelAnalyzer: distribution of losses by depth bucket
- WalletTrendAnalyzer: per-wallet stop-loss trend flags

All classes return structured data objects. No file I/O, no external calls.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

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
    metric: str                 # "jup_score", "mc_at_open", "token_age_days"
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


# ---------------------------------------------------------------------------
# Helper: metric value extractor
# ---------------------------------------------------------------------------

def _get_metric_value(pos: MatchedPosition, metric: str) -> Optional[float]:
    """
    Return the metric value for a position, or None if missing/invalid.

    Missing is defined as:
      - jup_score == 0
      - mc_at_open == 0.0
      - token_age_days is None
    """
    if metric == "jup_score":
        return float(pos.jup_score) if pos.jup_score != 0 else None
    elif metric == "mc_at_open":
        return pos.mc_at_open if pos.mc_at_open != 0.0 else None
    elif metric == "token_age_days":
        return float(pos.token_age_days) if pos.token_age_days is not None else None
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
        )

    def _risk_profile(
        self,
        stop_loss_positions: List[MatchedPosition],
        sl_rug_positions: List[MatchedPosition],
        all_closed: List[MatchedPosition],
    ) -> List[RiskProfileRow]:
        """
        For each metric, compute averages for the SL-only group, SL+Rug/Failsafe group,
        and all closed positions.

        stop_loss_positions: positions where close_reason is in SL_ONLY_REASONS.
        sl_rug_positions: positions where close_reason is in LOSS_REASONS (combined).
        all_closed: all non-still_open positions.
        """
        rows: List[RiskProfileRow] = []

        for metric in ["jup_score", "mc_at_open", "token_age_days"]:
            sl_values = [
                v for p in stop_loss_positions
                if (v := _get_metric_value(p, metric)) is not None
            ]
            sl_rug_values = [
                v for p in sl_rug_positions
                if (v := _get_metric_value(p, metric)) is not None
            ]
            all_values = [
                v for p in all_closed
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
        "mc_at_open": [1_000_000, 2_000_000, 5_000_000, 10_000_000, 20_000_000],
        "token_age_days": [0, 1, 2, 3, 7],
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
            param: One of "jup_score", "mc_at_open", "token_age_days".
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

        Returns:
            Dict mapping param name to list of BacktestRow.
        """
        return {
            param: self.sweep(positions, param, wallet=wallet)
            for param in self.DEFAULT_THRESHOLDS
        }


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
