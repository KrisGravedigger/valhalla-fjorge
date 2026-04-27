"""Interactive recommendation tracker mode."""

import csv as _csv
from decimal import Decimal
from pathlib import Path

from valhalla import recommendations_tracker as _tracker
from valhalla.analysis_config import PORTFOLIO_TOTAL_SOL, UTILIZATION_LOOKBACK_HOURS
from valhalla.loss_report import build_action_items, load_insuf_balance_csv
from valhalla.models import MatchedPosition
from valhalla.recommendations import generate_wallet_recommendations


def run_track_mode(output_dir: str) -> None:
    """
    Load positions from existing positions.csv, rebuild action items, and
    run the interactive recommendation tracker CLI.

    Called when --track flag is passed. Exits after tracker session ends.
    """
    from valhalla.loss_analyzer import LossAnalyzer

    output_path = Path(output_dir)
    positions_csv = output_path / "positions.csv"
    insuf_csv = output_path / "insufficient_balance.csv"
    state_path = str(output_path / ".recommendations_state.json")

    if not positions_csv.exists():
        print(f"Error: {positions_csv} not found. Run the parser first to generate it.")
        return

    print(f"Loading positions from {positions_csv}...")
    try:
        matched_positions = []
        with open(positions_csv, "r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            for row in reader:
                def _dec(key):
                    v = row.get(key, "").strip()
                    return Decimal(v) if v else None

                def _float(key):
                    v = row.get(key, "").strip()
                    return float(v) if v else None

                pos = MatchedPosition(
                    target_wallet=row.get("target_wallet", ""),
                    token=row.get("token", ""),
                    position_type=row.get("position_type", ""),
                    sol_deployed=_dec("sol_deployed"),
                    sol_received=_dec("sol_received"),
                    pnl_sol=_dec("pnl_sol"),
                    pnl_pct=_dec("pnl_pct"),
                    close_reason=row.get("close_reason", ""),
                    mc_at_open=float(row["mc_at_open"]) if row.get("mc_at_open", "").strip() else 0.0,
                    jup_score=int(float(row["jup_score"])) if row.get("jup_score", "").strip() else 0,
                    token_age=row.get("token_age", ""),
                    token_age_days=int(float(row["token_age_days"])) if row.get("token_age_days", "").strip() else None,
                    token_age_hours=int(float(row["token_age_hours"])) if row.get("token_age_hours", "").strip() else None,
                    price_drop_pct=_float("price_drop_pct"),
                    position_id=row.get("position_id", ""),
                    full_address=row.get("full_address", ""),
                    pnl_source=row.get("pnl_source", "pending"),
                    meteora_deposited=_dec("meteora_deposited"),
                    meteora_withdrawn=_dec("meteora_withdrawn"),
                    meteora_fees=_dec("meteora_fees"),
                    meteora_pnl=_dec("meteora_pnl"),
                    datetime_open=row.get("datetime_open", ""),
                    datetime_close=row.get("datetime_close", ""),
                    target_wallet_address=row.get("target_wallet_address") or None,
                    target_tx_signature=row.get("target_tx_signature") or None,
                    source_wallet_hold_min=int(float(row["source_wallet_hold_min"])) if row.get("source_wallet_hold_min", "").strip() else None,
                    source_wallet_pnl_pct=_dec("source_wallet_pnl_pct"),
                    source_wallet_scenario=row.get("source_wallet_scenario") or None,
                )
                matched_positions.append(pos)
    except Exception as e:
        print(f"Error loading positions CSV: {e}")
        return

    print(f"  Loaded {len(matched_positions)} positions.")

    result = LossAnalyzer().analyze(matched_positions)
    inactive_wallets = {sc.wallet for sc in result.wallet_scorecards if sc.status == "inactive" and sc.wallet}
    wallet_recs = generate_wallet_recommendations(matched_positions)
    insuf_events = load_insuf_balance_csv(str(insuf_csv)) if insuf_csv.exists() else []
    _util_points = None
    if PORTFOLIO_TOTAL_SOL > 0:
        from valhalla.utilization import compute_hourly_utilization
        _util_points = compute_hourly_utilization(matched_positions, UTILIZATION_LOOKBACK_HOURS)
    action_items = build_action_items(result, matched_positions, wallet_recs, insuf_events, _util_points)
    action_items = [item for item in action_items if not any(item.startswith(w) for w in inactive_wallets)]

    if not action_items:
        print("No recommendations to track.")
        return

    updates = _tracker.run_interactive_tracker(action_items, state_path)

    if updates > 0:
        from valhalla.loss_report import generate_loss_report

        print("\nRegenerating loss_analysis.md...")
        insuf_csv_str = str(insuf_csv) if insuf_csv.exists() else None
        output_md = str(output_path / "loss_analysis.md")
        generate_loss_report(matched_positions, output_md, insuf_csv_str)
        print(f"Report updated: {output_md}")
