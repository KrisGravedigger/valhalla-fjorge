from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Dict, List

import valhalla.analysis_config as _cfg
from valhalla import recommendations_tracker as _tracker
from valhalla.analysis_config import (
    MAX_POSITION_FRACTION,
    PORTFOLIO_TOTAL_SOL,
    SCORECARD_RECENT_DAYS,
    SOURCE_WALLET_MIN_LOSS_PCT,
    UTILIZATION_LOOKBACK_HOURS,
)
from valhalla.loss_analyzer import FilterBacktester, LOSS_REASONS, LossAnalyzer
from valhalla.models import parse_iso_datetime
from valhalla.recommendations import generate_wallet_recommendations

from .action_items import build_action_items, load_insuf_balance_csv
from .formatters import fmt_mc, fmt_pct, scorecard_action_hints, scenario_label
from .tables import build_loss_detail_table, md_table


def generate_loss_report(
    positions: List,
    output_path: str,
    insufficient_balance_csv: str = None,
) -> None:
    """Generate loss_analysis.md from matched positions."""
    analyzer = LossAnalyzer()
    result = analyzer.analyze(positions)
    inactive_wallets = {sc.wallet for sc in result.wallet_scorecards if sc.status == "inactive" and sc.wallet}

    # Load persistent recommendation state
    state_path = str(Path(output_path).parent / ".recommendations_state.json")
    rec_state = _tracker.load_state(state_path)
    wallet_recs = generate_wallet_recommendations(positions)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    date_str = datetime.now().strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # Local helpers (preserved from original function)
    # ------------------------------------------------------------------
    def _fmt_age_hours(hours: float) -> str:
        return f"{hours:.0f}h" if hours < 24 else f"{hours / 24:.0f}d"

    def _fmt_age_threshold(threshold: float) -> str:
        """Format token_age_hours threshold: hours < 24 as Xh, hours >= 24 as Xd."""
        if threshold < 24:
            return f"{threshold:.0f}h"
        return f"{threshold / 24:.0f}d"

    # PARAM_LABELS: used in Section 7 (Filter Backtest) and Section 8 (per-wallet loop)
    PARAM_LABELS = {
        "jup_score": "jup_score (minimum threshold)",
        "mc_at_open": "mc_at_open (minimum threshold)",
        "token_age_hours": "token_age_hours (minimum threshold)",
    }

    lines: List[str] = []

    # ------------------------------------------------------------------
    # Report header
    # ------------------------------------------------------------------
    lines.append(f"# Loss Analysis Report — {date_str}")
    lines.append(f"Generated: {now_str}")
    lines.append("")

    # ------------------------------------------------------------------
    # Table of Contents
    # ------------------------------------------------------------------
    lines.append("## Table of Contents")
    lines.append("")
    lines.append("- [1. Executive Summary](#executive-summary)")
    lines.append("- [2. Action Items](#action-items)")
    lines.append("- [3. Recent Large Losses](#large-losses)")
    lines.append("- [4. Wallet Scorecard](#wallet-scorecard)")
    lines.append("- [5. Filter Recommendations](#filter-recommendations)")
    lines.append("- [6. Loss Analysis](#loss-analysis)")
    lines.append("- [7. Global Filter Backtest](#filter-backtest)")
    lines.append("- [8. Per-Wallet Details](#per-wallet-details)")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 1: Executive Summary
    # ------------------------------------------------------------------
    lines.append("## 1. Executive Summary {#executive-summary}")
    lines.append("")

    loss_rate = (
        result.loss_positions / result.closed_positions * 100.0
        if result.closed_positions > 0 else 0.0
    )

    active_scorecards = [
        sc for sc in result.wallet_scorecards
        if sc.status not in ("inactive", "insufficient_data")
    ]
    best_wallet = max(
        active_scorecards,
        key=lambda sc: sc.pnl_per_day_sol,
        default=None,
    )
    replacing_wallets = [
        sc for sc in result.wallet_scorecards
        if sc.status == "consider_replacing"
    ]

    lines.append(
        f"> Portfolio closed {result.closed_positions} positions"
        f" with total PnL {result.total_pnl_sol:+.4f} SOL."
    )
    lines.append(
        f"> Loss rate (SL+Rug+Failsafe): {loss_rate:.1f}%"
        f" ({result.loss_positions} positions)."
    )
    if best_wallet is not None:
        lines.append(
            f"> Top wallet: {best_wallet.wallet}"
            f" ({best_wallet.pnl_per_day_sol:+.4f} SOL/day,"
            f" WR {best_wallet.win_rate_pct:.0f}%)."
        )
    if replacing_wallets:
        lines.append(f"> {len(replacing_wallets)} wallet(s) flagged for replacement.")
    elif active_scorecards:
        lines.append("> All wallets within normal range — no urgent actions.")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 2: Pilne działania
    # ------------------------------------------------------------------
    lines.append("## 2. Action Items {#action-items}")
    lines.append("")

    # Portfolio size info line
    if PORTFOLIO_TOTAL_SOL > 0:
        max_pos_sol = PORTFOLIO_TOTAL_SOL * MAX_POSITION_FRACTION
        lines.append(
            f"**Portfolio:** {PORTFOLIO_TOTAL_SOL:.1f} SOL total, "
            f"max position {max_pos_sol:.2f} SOL "
            f"(1/{round(1/MAX_POSITION_FRACTION):.0f})"
        )
    else:
        lines.append(
            "**Portfolio size:** not configured — position size guard disabled "
            "(set PORTFOLIO_TOTAL_SOL in analysis_config.py to enable)"
        )
    lines.append("")

    insuf_events = load_insuf_balance_csv(insufficient_balance_csv) if insufficient_balance_csv else []
    # Compute utilization once for both chart and action items
    _util_points = None
    if PORTFOLIO_TOTAL_SOL > 0:
        from valhalla.utilization import compute_hourly_utilization
        _util_points = compute_hourly_utilization(positions, UTILIZATION_LOOKBACK_HOURS)
    action_items = build_action_items(result, positions, wallet_recs, insuf_events, _util_points)
    action_items = [item for item in action_items if not any(item.startswith(w) for w in inactive_wallets)]

    # Annotate each action item with its persistent status
    annotated_items = _tracker.annotate_items(action_items, rec_state)
    # Items considered "active" (not yet done/ignored) drive the Scorecard Action column
    active_action_items = [item for item, _id, status in annotated_items
                           if status == _tracker.STATUS_NEW]

    if not action_items:
        lines.append("_No urgent actions._")
    else:
        for idx, (item, rec_id, status) in enumerate(annotated_items, start=1):
            badge = _tracker.STATUS_BADGE[status]
            lines.append(f"{idx}. `{rec_id}` {badge} {item}")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 3: Recent Large Losses
    # ------------------------------------------------------------------
    loss_detail_section = build_loss_detail_table(positions)
    lines.append(loss_detail_section)

    # ------------------------------------------------------------------
    # Section 4: Wallet Scorecard
    # ------------------------------------------------------------------
    lines.append("## 4. Wallet Scorecard {#wallet-scorecard}")
    lines.append("")

    if not result.wallet_scorecards:
        lines.append("_No scorecard data (no closed positions)._")
    else:
        from datetime import timedelta

        # Determine which wallets had any position opened within SCORECARD_RECENT_DAYS
        open_dates_by_wallet: Dict[str, datetime] = {}
        for pos in positions:
            dt_open = parse_iso_datetime(pos.datetime_open)
            if dt_open is not None:
                w = pos.target_wallet
                if w not in open_dates_by_wallet or dt_open > open_dates_by_wallet[w]:
                    open_dates_by_wallet[w] = dt_open

        if open_dates_by_wallet:
            reference_open = max(open_dates_by_wallet.values())
            cutoff_open = reference_open - timedelta(days=SCORECARD_RECENT_DAYS)
            recent_wallets = {w for w, d in open_dates_by_wallet.items() if d >= cutoff_open}
        else:
            recent_wallets = None  # no date info — show all

        sc_rows = []
        for sc in result.wallet_scorecards:
            if sc.wallet in inactive_wallets:
                continue
            if recent_wallets is not None and sc.wallet not in recent_wallets:
                continue
            wr_7d_str = f"{sc.win_rate_7d_pct:.0f}%" if sc.win_rate_7d_pct is not None else "N/A"
            hold_str = f"{sc.avg_hold_minutes:.0f}m" if sc.avg_hold_minutes is not None else "N/A"
            trend_str = f"{sc.win_rate_trend_pp:+.0f}pp" if sc.win_rate_trend_pp is not None else "N/A"
            hint_str = scorecard_action_hints(sc.wallet, active_action_items)
            sc_rows.append([
                sc.wallet,
                str(sc.closed_positions),
                f"{sc.win_rate_pct:.0f}%",
                wr_7d_str,
                f"{sc.total_pnl_sol:+.4f}",
                f"{sc.pnl_per_day_sol:+.4f}",
                f"{sc.rug_rate_pct:.0f}%",
                hold_str,
                trend_str,
                sc.status,
                hint_str,
            ])

        ref_date_str = reference_open.strftime("%Y-%m-%d %H:%M") if open_dates_by_wallet else "N/A"
        lines.append(
            f"_Showing wallets active in last {SCORECARD_RECENT_DAYS}d "
            f"(last open: {ref_date_str}). "
            f"Change `SCORECARD_RECENT_DAYS` in analysis_config.py to adjust._"
        )
        lines.append("")
        if sc_rows:
            lines.append(md_table(
                ["Wallet", "Pos.", "WR%", "WR 7d%", "PnL (SOL)", "SOL/day",
                 "Rug Rate", "Avg Hold", "Trend", "Status", "Action"],
                sc_rows,
            ))
        else:
            lines.append("_No wallets with recent activity._")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 5: Rekomendacje filtrów
    # ------------------------------------------------------------------
    lines.append("## 5. Filter Recommendations {#filter-recommendations}")
    lines.append("")

    filter_recs = [
        r for r in wallet_recs
        if ("sweet spot" in r.lower() or "tightening" in r.lower())
        and not any(r.startswith(w) for w in inactive_wallets)
    ]

    if not filter_recs:
        lines.append("_No actionable filter recommendations._")
    else:
        for rec in filter_recs:
            lines.append(f"- {rec.strip()}")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 6: Analiza strat
    # ------------------------------------------------------------------
    lines.append("## 6. Loss Analysis {#loss-analysis}")
    lines.append("")

    # ---- 5a. Risk Profile ----
    lines.append("### 5a. Risk Profile: Stop-Loss vs Profitable Trades")
    lines.append("")
    lines.append("Compares average token quality metrics for loss groups vs profitable trades only.")
    lines.append("Lower quality metrics in the stop-loss group may indicate avoidable entries.")
    lines.append("")

    if result.stop_loss_positions == 0:
        lines.append("_No stop-loss positions found — section not applicable._")
    else:
        rp_rows = []
        for row in result.risk_profile:
            metric_label = {
                "jup_score": "jup_score",
                "mc_at_open": "mc_at_open",
                "token_age_hours": "token_age_hours",
            }.get(row.metric, row.metric)

            if row.metric == "mc_at_open":
                sl_val = fmt_mc(row.sl_avg) if row.sl_avg is not None else "N/A"
                sl_rug_val = fmt_mc(row.sl_rug_avg) if row.sl_rug_avg is not None else "N/A"
                all_val = fmt_mc(row.all_avg) if row.all_avg is not None else "N/A"
            elif row.metric == "token_age_hours":
                sl_val = _fmt_age_hours(row.sl_avg) if row.sl_avg is not None else "N/A"
                sl_rug_val = _fmt_age_hours(row.sl_rug_avg) if row.sl_rug_avg is not None else "N/A"
                all_val = _fmt_age_hours(row.all_avg) if row.all_avg is not None else "N/A"
            else:
                sl_val = f"{row.sl_avg:.0f}" if row.sl_avg is not None else "N/A"
                sl_rug_val = f"{row.sl_rug_avg:.0f}" if row.sl_rug_avg is not None else "N/A"
                all_val = f"{row.all_avg:.0f}" if row.all_avg is not None else "N/A"

            sl_note = ""
            if row.sl_count < 3:
                sl_note = f" (n={row.sl_count}, insufficient)"
            sl_rug_note = ""
            if row.sl_rug_count < 3:
                sl_rug_note = f" (n={row.sl_rug_count}, insufficient)"

            rp_rows.append([
                metric_label,
                sl_val + sl_note,
                sl_rug_val + sl_rug_note,
                all_val,
                fmt_pct(row.diff_pct),
                fmt_pct(row.sl_rug_diff_pct),
            ])

        lines.append(md_table(
            ["Metric", "SL Only Avg", "SL+Rug/FS Avg", "Profitable Avg", "SL Diff", "SL+Rug Diff"],
            rp_rows,
        ))
    lines.append("")

    # ---- 5b. Stop-Loss Level Distribution ----
    lines.append("### 5b. Stop-Loss Level Distribution")
    lines.append("")
    lines.append("If your stop-loss had been set tighter, how many positions would have been saved?")
    lines.append("")

    # Sub-table A: SL exits only
    lines.append("**SL exits only** (stop_loss / stop_loss_unknown_open):")
    lines.append("")
    if not result.sl_buckets_sl_only or all(b.count == 0 for b in result.sl_buckets_sl_only):
        lines.append("_No SL-only positions with PnL percentage data available._")
    else:
        sl_only_rows = [
            [b.bucket_label, str(b.count), f"{b.sol_saved:.3f} SOL"]
            for b in result.sl_buckets_sl_only
        ]
        lines.append(md_table(
            ["SL Level", "Positions Below", "SOL Saved vs Actual"],
            sl_only_rows,
        ))
    lines.append("")

    # Sub-table B: All losses (SL + Rug/Failsafe)
    lines.append("**All losses** (SL + Rug/Failsafe):")
    lines.append("")
    if not result.sl_buckets or all(b.count == 0 for b in result.sl_buckets):
        lines.append("_No loss positions with PnL percentage data available._")
    else:
        sl_rows = [
            [b.bucket_label, str(b.count), f"{b.sol_saved:.3f} SOL"]
            for b in result.sl_buckets
        ]
        lines.append(md_table(
            ["SL Level", "Positions Below", "SOL Saved vs Actual"],
            sl_rows,
        ))
    lines.append("")

    # ---- 5c. Source Wallet Comparison ----
    lines.append("### 5c. Source Wallet Comparison")
    lines.append("")

    # Consider positions eligible for source wallet analysis (by PnL threshold)
    if SOURCE_WALLET_MIN_LOSS_PCT is not None:
        sw_threshold = Decimal(str(SOURCE_WALLET_MIN_LOSS_PCT))
        sw_eligible_all = [
            p for p in positions
            if p.close_reason != "still_open"
            and p.pnl_pct is not None and p.pnl_pct <= sw_threshold
        ]
        threshold_label = f"positions with loss > {abs(SOURCE_WALLET_MIN_LOSS_PCT):.0f}%"
    else:
        sw_eligible_all = [p for p in positions if p.close_reason != "still_open"]
        threshold_label = "closed positions"
    sw_eligible_total = len(sw_eligible_all)

    # Positions with source_wallet_scenario populated (excluding failed attempts)
    with_scenario = [
        p for p in sw_eligible_all
        if getattr(p, 'source_wallet_scenario', None)
        and p.source_wallet_scenario != "no_data"
    ]

    if not with_scenario:
        lines.append("_No source wallet data available yet._")
    else:
        scenario_count = len(with_scenario)
        lines.append(
            f"Source wallet data available for {scenario_count} of {sw_eligible_total} {threshold_label}."
        )
        lines.append("")

        # Scenario distribution
        from collections import Counter
        scenario_counts: Counter = Counter()
        for p in with_scenario:
            scenario_counts[p.source_wallet_scenario] += 1

        # Avg pnl_pct per scenario
        scenario_pnl_pcts: dict = {}
        for scenario in scenario_counts:
            pcts = [
                float(p.source_wallet_pnl_pct)
                for p in with_scenario
                if p.source_wallet_scenario == scenario
                and getattr(p, 'source_wallet_pnl_pct', None) is not None
            ]
            if pcts:
                scenario_pnl_pcts[scenario] = sum(pcts) / len(pcts)

        scenario_order = [
            "source_held_longer", "source_exited_early", "source_recovered",
            "held_longer", "exited_first",  # legacy labels from old CSV
            "both_loss", "comparable",
            "unknown",  # legacy label from old CSV
            "no_data", "error",
        ]
        dist_rows = []
        for sc in scenario_order:
            count = scenario_counts.get(sc, 0)
            pct_of_total = count / scenario_count * 100.0 if scenario_count > 0 else 0.0
            avg_pnl = scenario_pnl_pcts.get(sc)
            avg_pnl_str = f"{avg_pnl:+.1f}%" if avg_pnl is not None else "N/A"
            dist_rows.append([sc, str(count), f"{pct_of_total:.0f}%", avg_pnl_str])

        lines.append(md_table(
            ["Scenario", "Count", "% of Source Data", "Avg Source PnL%"],
            dist_rows,
        ))
        lines.append("")
        lines.append("**Scenario guide:**")
        lines.append("- `source_held_longer` / `held_longer`: source wallet stayed in the position longer and recovered — consider widening stop-loss margin or enabling non-SOL token top-up")
        lines.append("- `source_exited_early` / `exited_first`: source wallet exited before the drop — copy lag or entry speed issue")
        lines.append("- `source_recovered`: source wallet ended positive while bot lost — timing data unavailable to determine mechanism")
        lines.append("- `both_loss`: both source and bot lost — likely bad luck or market conditions, not a strategy issue")
        lines.append("- `comparable`: similar outcomes on both sides — no clear lesson from this position")
        lines.append("- `no_data` / `no_data`: API error or missing transaction data — could not analyze")
    lines.append("")

    # ------------------------------------------------------------------
    # Section 7: Filter Backtest (globalny)
    # ------------------------------------------------------------------
    lines.append("## 7. Global Filter Backtest {#filter-backtest}")
    lines.append("")
    lines.append("For each parameter: what if only trades meeting the threshold were taken?")
    lines.append("")

    for param, bt_rows in result.backtest_results.items():
        lines.append(f"### {PARAM_LABELS.get(param, param)}")
        lines.append("")
        if not bt_rows:
            lines.append("_No data._")
            lines.append("")
            continue

        # Find sweet spot: row with highest net_sol_impact > 0
        best_idx = None
        best_impact = Decimal("0")
        for i, brow in enumerate(bt_rows):
            if brow.net_sol_impact > best_impact:
                best_impact = brow.net_sol_impact
                best_idx = i

        table_rows = []
        for i, brow in enumerate(bt_rows):
            if param == "mc_at_open":
                threshold_str = fmt_mc(brow.threshold)
            elif param == "token_age_hours":
                threshold_str = _fmt_age_threshold(brow.threshold)
            else:
                threshold_str = f"{brow.threshold:.0f}" if brow.threshold == int(brow.threshold) else f"{brow.threshold}"

            net_str = f"{brow.net_sol_impact:+.4f} SOL"
            marker = " <- sweet spot" if i == best_idx else ""
            table_rows.append([
                f">= {threshold_str}",
                str(brow.wins_kept),
                str(brow.wins_excluded),
                str(brow.losses_avoided),
                str(brow.losses_kept),
                net_str + marker,
            ])

        lines.append(md_table(
            ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"],
            table_rows,
        ))
        lines.append("")

    # ------------------------------------------------------------------
    # Section 8: Szczegóły per wallet
    # ------------------------------------------------------------------
    lines.append("## 8. Per-Wallet Details {#per-wallet-details}")
    lines.append("")

    # Collect unique wallet names from all positions
    all_wallets = sorted(set(
        p.target_wallet for p in positions
        if getattr(p, 'target_wallet', None) and p.target_wallet != "unknown"
    ))

    RUG_FAILSAFE_REASONS = {"rug", "rug_unknown_open", "failsafe", "failsafe_unknown_open"}

    wallet_sections_written = 0

    for wallet_name in all_wallets:
        if wallet_name in inactive_wallets:
            continue
        # Filter to this wallet's positions only
        wallet_positions = [p for p in positions if p.target_wallet == wallet_name]

        # Only include wallets with at least 3 closed positions (not still_open, not unknown_open)
        closed_wallet = [
            p for p in wallet_positions
            if p.close_reason not in ("still_open", "unknown_open")
        ]
        if len(closed_wallet) < 3:
            continue

        # Skip backtest if fewer than 10 closed positions (too little data)
        run_backtest = len(closed_wallet) >= 10

        # Compute header stats
        WIN_THRESHOLD = Decimal("0.01")
        LOSS_THRESHOLD = Decimal("-0.01")

        TP_REASONS = {"take_profit"}
        SL_REASONS = {"stop_loss", "stop_loss_unknown_open"}
        RF_REASONS = {"rug", "rug_unknown_open", "failsafe", "failsafe_unknown_open"}

        wins = sum(1 for p in closed_wallet if p.pnl_sol is not None and p.pnl_sol > WIN_THRESHOLD)
        neutral = sum(1 for p in closed_wallet if p.pnl_sol is not None and LOSS_THRESHOLD <= p.pnl_sol <= WIN_THRESHOLD)
        losses = sum(1 for p in closed_wallet if p.pnl_sol is not None and p.pnl_sol < LOSS_THRESHOLD)
        no_pnl = sum(1 for p in closed_wallet if p.pnl_sol is None)
        tp_count = sum(1 for p in closed_wallet if p.close_reason in TP_REASONS)
        sl_count = sum(1 for p in closed_wallet if p.close_reason in SL_REASONS)
        rf_count = sum(1 for p in closed_wallet if p.close_reason in RF_REASONS)
        wallet_pnl = sum((p.pnl_sol for p in closed_wallet if p.pnl_sol is not None), Decimal("0"))

        lines.append(f"### Wallet: {wallet_name}")
        lines.append("")
        lines.append(
            f"{len(closed_wallet)} total positions | "
            f"{wins} wins (>{WIN_THRESHOLD} SOL) | {neutral} neutral | {losses} losses (<{LOSS_THRESHOLD} SOL)"
            f" | [TP: {tp_count} | SL: {sl_count} | Rug/FS: {rf_count}]"
            f" | PnL: {wallet_pnl:.4f} SOL"
        )
        lines.append("")

        if run_backtest:
            lines.append("#### Filter Backtest")
            lines.append("")

            wallet_bt_results = FilterBacktester().sweep_all(wallet_positions)

            for param, bt_rows in wallet_bt_results.items():
                lines.append(f"**{PARAM_LABELS.get(param, param)}**")
                lines.append("")
                if not bt_rows:
                    lines.append("_No data._")
                    lines.append("")
                    continue

                # Find sweet spot: row with highest net_sol_impact > 0
                best_idx = None
                best_impact = Decimal("0")
                for i, brow in enumerate(bt_rows):
                    if brow.net_sol_impact > best_impact:
                        best_impact = brow.net_sol_impact
                        best_idx = i

                table_rows = []
                for i, brow in enumerate(bt_rows):
                    if param == "mc_at_open":
                        threshold_str = fmt_mc(brow.threshold)
                    elif param == "token_age_hours":
                        threshold_str = _fmt_age_threshold(brow.threshold)
                    else:
                        threshold_str = (
                            f"{brow.threshold:.0f}"
                            if brow.threshold == int(brow.threshold)
                            else f"{brow.threshold}"
                        )

                    net_str = f"{brow.net_sol_impact:+.4f} SOL"
                    marker = " <- sweet spot" if i == best_idx else ""
                    table_rows.append([
                        f">= {threshold_str}",
                        str(brow.wins_kept),
                        str(brow.wins_excluded),
                        str(brow.losses_avoided),
                        str(brow.losses_kept),
                        net_str + marker,
                    ])

                lines.append(md_table(
                    ["Threshold", "Wins Kept", "Wins Excl.", "Losses Avoided", "Losses Kept", "Net SOL Impact"],
                    table_rows,
                ))
                lines.append("")
        else:
            lines.append(
                f"_Filter backtest requires at least 10 closed positions "
                f"(this wallet has {len(closed_wallet)})._"
            )
            lines.append("")

        wallet_sections_written += 1

    if wallet_sections_written == 0:
        lines.append("_No wallets with sufficient data for per-wallet analysis._")
        lines.append("")

    # Write file
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(lines))
