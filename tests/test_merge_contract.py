"""Characterization tests pinning ``merge_with_existing_csv`` upgrade paths.

S2b (PLAN-refactor §4 phase 5): minimal contract tests around the merge
policy / upgrade-decision seams *before* the in-file extraction in S6.

These tests assert what the code currently does (including any quirks) — the
goal is to catch S6 changing behavior, not to judge correctness. PnL
correctness (#146) is explicitly out of scope.
"""

from decimal import Decimal

from valhalla.merge import merge_with_existing_csv
from valhalla.models import MatchedPosition, OpenEvent


DATE = "2026-04-26"

# Full positions.csv header as written by the pipeline.
FIELDNAMES = [
    "datetime_open", "datetime_close",
    "target_wallet", "token", "position_type",
    "sol_deployed", "sol_received", "pnl_sol", "pnl_pct", "close_reason",
    "mc_at_open", "jup_score", "token_age", "token_age_days", "token_age_hours",
    "price_drop_pct", "position_id",
    "full_address", "pnl_source", "meteora_deposited", "meteora_withdrawn",
    "meteora_fees", "meteora_pnl",
    "target_wallet_address", "target_tx_signature",
    "source_wallet_hold_min", "source_wallet_pnl_pct", "source_wallet_scenario",
    "original_wallet",
]


def _write_csv(tmp_path, *rows):
    """Write one positions.csv with the given row dicts; return its path."""
    import csv

    csv_path = tmp_path / "positions.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            full = {name: "" for name in FIELDNAMES}
            full.update(row)
            writer.writerow(full)
    return str(csv_path)


def _matched(**kwargs):
    """Build a MatchedPosition with sensible defaults for required fields."""
    base = dict(
        target_wallet="wallet",
        token="TOKEN",
        position_type="BidAsk",
        sol_deployed=None,
        sol_received=None,
        pnl_sol=None,
        pnl_pct=None,
        close_reason="normal",
        mc_at_open=0.0,
        jup_score=0,
        token_age="",
    )
    base.update(kwargs)
    return MatchedPosition(**base)


def _open_event(**kwargs):
    base = dict(
        timestamp="[20:45]",
        position_type="BidAsk",
        token_name="TOKEN",
        token_pair="TOKEN-SOL",
        target="wallet",
        market_cap=100.0,
        token_age="5h ago",
        jup_score=81,
        target_sol=1.0,
        your_sol=2.0,
        position_id="PID",
        date=DATE,
    )
    base.update(kwargs)
    return OpenEvent(**base)


# ---------------------------------------------------------------------------
# Rule 1 — fully complete (open + close + meteora)
# ---------------------------------------------------------------------------

def test_rule1_complete_kept_unchanged_when_no_new_data(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "token": "HENRY", "position_type": "BidAsk", "target_wallet": "w1",
        "close_reason": "normal", "pnl_source": "meteora",
        "sol_deployed": "2.5", "pnl_sol": "0.7", "position_id": "PID",
        "meteora_deposited": "2.5", "meteora_pnl": "0.7",
    })

    merged, still_open = merge_with_existing_csv([], [], csv_path)

    assert still_open == []
    assert len(merged) == 1
    assert merged[0].close_reason == "normal"
    assert merged[0].pnl_source == "meteora"
    assert merged[0].pnl_sol == Decimal("0.7")


def test_rule1_upgrades_normal_to_take_profit_keeping_meteora_pnl(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "normal", "pnl_source": "meteora",
        "pnl_sol": "0.7", "position_id": "PID",
        "meteora_deposited": "2.5", "meteora_pnl": "0.7",
    })
    new_pos = _matched(
        position_id="PID", close_reason="take_profit",
        datetime_open="2026-04-26T20:45:00",
        datetime_close="2026-04-26T21:25:00",
        pnl_source="pending",
    )

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].close_reason == "take_profit"
    # Meteora financials are never overwritten by the upgrade.
    assert merged[0].pnl_source == "meteora"
    assert merged[0].pnl_sol == Decimal("0.7")
    # datetime_close is taken from the new (more specific) data.
    assert merged[0].datetime_close == "2026-04-26T21:25:00"


def test_rule1_does_not_downgrade_specific_to_normal(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "take_profit", "pnl_source": "meteora",
        "pnl_sol": "0.7", "position_id": "PID",
        "meteora_deposited": "2.5",
    })
    new_pos = _matched(position_id="PID", close_reason="normal",
                       datetime_open="2026-04-26T20:45:00")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    # close_reason "take_profit" is not in the upgradeable set, so it stays.
    assert merged[0].close_reason == "take_profit"


def test_rule1_recovers_unknown_target_wallet_from_new(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "target_wallet": "unknown", "close_reason": "normal",
        "pnl_source": "meteora", "position_id": "PID",
        "meteora_deposited": "2.5",
    })
    new_pos = _matched(position_id="PID", target_wallet="realwallet",
                       datetime_open="2026-04-26T20:45:00")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].target_wallet == "realwallet"


def test_rule1_backfills_address_and_tx_signature(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "normal", "pnl_source": "meteora", "position_id": "PID",
        "meteora_deposited": "2.5",
    })
    new_pos = _matched(position_id="PID", datetime_open="2026-04-26T20:45:00",
                       target_wallet_address="FULLADDR", target_tx_signature="SIG")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].target_wallet_address == "FULLADDR"
    assert merged[0].target_tx_signature == "SIG"


# ---------------------------------------------------------------------------
# Rule 2 — meteora PnL but missing open data (unknown_open variants)
# ---------------------------------------------------------------------------

def test_rule2_enriches_meteora_unknown_open_from_new_matched(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "rug_unknown_open", "pnl_source": "meteora",
        "pnl_sol": "-0.7", "position_id": "PID", "meteora_deposited": "2.5",
        "token": "unknown", "position_type": "unknown",
    })
    new_pos = _matched(
        position_id="PID", token="HENRY", position_type="BidAsk",
        close_reason="rug", datetime_open="2026-04-26T20:45:00",
        datetime_close="2026-04-26T21:25:00", mc_at_open=2500.0, jup_score=91,
    )

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    # Open-side data enriched; close_reason de-suffixed; meteora PnL preserved.
    assert merged[0].token == "HENRY"
    assert merged[0].datetime_open == "2026-04-26T20:45:00"
    assert merged[0].close_reason == "rug"
    assert merged[0].pnl_source == "meteora"
    assert merged[0].pnl_sol == Decimal("-0.7")
    assert merged[0].datetime_close == "2026-04-26T21:25:00"


def test_rule2_enriches_meteora_unknown_open_from_still_open_event(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "unknown_open", "pnl_source": "meteora",
        "position_id": "PID", "meteora_deposited": "2.5",
        "token": "unknown", "position_type": "unknown",
    })
    open_event = _open_event(position_id="PID", token_name="HENRY")

    merged, still_open = merge_with_existing_csv([], [open_event], csv_path)

    assert still_open == []
    assert merged[0].token == "HENRY"
    assert merged[0].datetime_open == "2026-04-26T20:45:00"
    assert merged[0].close_reason == "normal"  # unknown_open -> normal
    assert merged[0].pnl_source == "meteora"


def test_rule2_kept_as_is_when_no_open_data(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "rug_unknown_open", "pnl_source": "meteora",
        "position_id": "PID", "meteora_deposited": "2.5",
    })

    merged, _ = merge_with_existing_csv([], [], csv_path)

    assert merged[0].close_reason == "rug_unknown_open"
    assert merged[0].pnl_source == "meteora"


# ---------------------------------------------------------------------------
# Rule 3.5 — lpagent backfill rows (replaceable placeholder)
# ---------------------------------------------------------------------------

ADDR = "ABCD12345678WXYZ"  # addr[:8]="ABCD1234"; addr[:4]+addr[-4:]="ABCDWXYZ"


def test_rule35_lpagent_replaced_by_discord_keeping_financials_fallback(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "close_reason": "normal", "pnl_source": "lpagent",
        "position_id": "PIDLP", "full_address": "FULLADDRLP",
        "sol_deployed": "2.5", "pnl_sol": "0.7",
        "meteora_deposited": "2.5", "meteora_pnl": "0.7",
    })
    # Discord arrives without financials -> lpagent fields used as fallback,
    # and pnl_source promoted to meteora because lpagent had meteora_deposited.
    new_pos = _matched(position_id="PIDLP", token="HENRY", close_reason="normal",
                       datetime_open="2026-04-26T20:45:00", pnl_source="pending")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].token == "HENRY"
    assert merged[0].sol_deployed == Decimal("2.5")
    assert merged[0].pnl_sol == Decimal("0.7")
    assert merged[0].pnl_source == "meteora"


def test_rule35_lpagent_old_id_format_fallback(tmp_path):
    # lpagent row stored under old addr[:8] id; Discord uses addr[:4]+addr[-4:].
    # The fallback id-match correctly replaces the lpagent placeholder with the
    # Discord row (financials kept as fallback, pnl_source promoted to meteora).
    #
    # QUIRK pinned here: because the Discord row's position_id (addr[:4]+addr[-4:])
    # is NOT a key in existing_by_id (keyed by addr[:8]), the same row is *also*
    # appended again by the trailing "truly new positions" loop. So the merged
    # output contains the Discord row twice (double-count). If S6 changes this,
    # the test fires — this characterizes current behavior, not desired behavior.
    csv_path = _write_csv(tmp_path, {
        "close_reason": "normal", "pnl_source": "lpagent",
        "position_id": ADDR[:8], "full_address": ADDR,
        "meteora_deposited": "2.5", "pnl_sol": "0.7",
    })
    new_pos = _matched(position_id=ADDR[:4] + ADDR[-4:], token="HENRY",
                       close_reason="normal", pnl_source="pending")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert len(merged) == 2
    assert all(p.token == "HENRY" for p in merged)
    assert all(p.pnl_source == "meteora" for p in merged)
    assert {id(p) for p in merged} == {id(new_pos)}  # same object appended twice


def test_rule35_lpagent_enriched_by_still_open_marks_pending_need_meteora(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "close_reason": "normal", "pnl_source": "lpagent",
        "position_id": "PIDLP", "full_address": "FULLADDRLP",
        "meteora_deposited": "2.5",
    })
    open_event = _open_event(position_id="PIDLP", token_name="HENRY")

    merged, still_open = merge_with_existing_csv([], [open_event], csv_path)

    assert still_open == []
    assert merged[0].token == "HENRY"
    assert merged[0].pnl_source == "pending_need_meteora_close"


def test_rule35_lpagent_kept_when_no_new_data(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "close_reason": "normal", "pnl_source": "lpagent",
        "position_id": "PIDLP", "full_address": "FULLADDRLP",
        "meteora_deposited": "2.5",
    })

    merged, _ = merge_with_existing_csv([], [], csv_path)

    assert merged[0].pnl_source == "lpagent"


# ---------------------------------------------------------------------------
# Rule 4 — pending/discord (no meteora) upgrade paths
# ---------------------------------------------------------------------------

def test_rule4_upgrade_preserves_existing_open_side_data(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "token": "HENRY", "position_type": "BidAsk", "mc_at_open": "2500",
        "jup_score": "91", "token_age": "16h ago",
        "close_reason": "unknown_open", "pnl_source": "pending",
        "position_id": "PID",
    })
    # New close arrives without open-side metadata.
    new_pos = _matched(position_id="PID", token="unknown", position_type="unknown",
                       close_reason="normal", mc_at_open=0.0, jup_score=0,
                       pnl_source="discord", pnl_sol=Decimal("0.5"))

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].token == "HENRY"
    assert merged[0].position_type == "BidAsk"
    assert merged[0].mc_at_open == 2500.0
    assert merged[0].jup_score == 91
    assert merged[0].datetime_open == "2026-04-26T20:45:00"
    assert merged[0].pnl_source == "discord"


def test_rule4_fixes_close_reason_when_new_has_open(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "close_reason": "unknown_open", "pnl_source": "pending",
        "position_id": "PID",
    })
    new_pos = _matched(position_id="PID", close_reason="rug_unknown_open",
                       datetime_open="2026-04-26T20:45:00", pnl_source="pending")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].close_reason == "rug"


def test_rule4_still_open_does_not_downgrade_existing_close(tmp_path):
    # Existing has close data (failsafe_unknown_open, pending) — a new still_open
    # event must enrich, not downgrade to still_open.
    csv_path = _write_csv(tmp_path, {
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "failsafe_unknown_open", "pnl_source": "pending",
        "position_id": "PID",
    })
    open_event = _open_event(position_id="PID", token_name="HENRY")

    merged, still_open = merge_with_existing_csv([], [open_event], csv_path)

    assert still_open == []
    assert len(merged) == 1
    assert merged[0].close_reason == "failsafe"
    assert merged[0].token == "HENRY"
    assert merged[0].datetime_open == "2026-04-26T20:45:00"


def test_rule4_still_open_existing_with_still_open_event_stays_open(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "close_reason": "still_open", "pnl_source": "pending",
        "position_id": "PID",
    })
    open_event = _open_event(position_id="PID", token_name="HENRY")

    merged, still_open = merge_with_existing_csv([], [open_event], csv_path)

    assert merged == []
    assert [e.position_id for e in still_open] == ["PID"]


def test_rule4_still_open_existing_no_new_data_rebuilds_open_event(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "token": "HENRY", "position_type": "BidAsk",
        "close_reason": "still_open", "pnl_source": "pending",
        "position_id": "PID",
    })

    merged, still_open = merge_with_existing_csv([], [], csv_path)

    assert merged == []
    assert len(still_open) == 1
    assert still_open[0].position_id == "PID"
    assert still_open[0].token_name == "HENRY"


def test_rule4_pending_matched_kept_when_no_new_data(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "normal", "pnl_source": "pending",
        "position_id": "PID",
    })

    merged, still_open = merge_with_existing_csv([], [], csv_path)

    assert still_open == []
    assert merged[0].close_reason == "normal"
    assert merged[0].pnl_source == "pending"


# ---------------------------------------------------------------------------
# New positions (not in existing CSV)
# ---------------------------------------------------------------------------

def test_new_matched_and_still_open_positions_added(tmp_path):
    csv_path = _write_csv(tmp_path, {
        "close_reason": "normal", "pnl_source": "meteora",
        "position_id": "EXISTING", "meteora_deposited": "2.5",
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
    })
    new_matched = _matched(position_id="NEWCLOSED", token="NEW",
                           close_reason="normal", pnl_source="discord")
    new_open = _open_event(position_id="NEWOPEN", token_name="NEWO")

    merged, still_open = merge_with_existing_csv([new_matched], [new_open], csv_path)

    matched_ids = {p.position_id for p in merged}
    assert matched_ids == {"EXISTING", "NEWCLOSED"}
    assert [e.position_id for e in still_open] == ["NEWOPEN"]


# ---------------------------------------------------------------------------
# is_fully_complete asymmetry: already_closed_unknown_open
# ---------------------------------------------------------------------------

def test_asymmetry_already_closed_unknown_open_meteora_is_rule1(tmp_path):
    # With meteora, already_closed_unknown_open is treated as fully complete:
    # an incoming specific close_reason upgrades it in place, keeping meteora PnL.
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "already_closed_unknown_open", "pnl_source": "meteora",
        "pnl_sol": "0.7", "position_id": "PID", "meteora_deposited": "2.5",
    })
    new_pos = _matched(position_id="PID", close_reason="take_profit",
                       datetime_open="2026-04-26T20:45:00", pnl_source="pending")

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].close_reason == "take_profit"
    assert merged[0].pnl_source == "meteora"
    assert merged[0].pnl_sol == Decimal("0.7")


def test_asymmetry_already_closed_unknown_open_pending_is_rule4(tmp_path):
    # Without meteora, the same close_reason falls through to Rule 4: the new
    # matched position replaces it wholesale (pnl_source from the new row).
    csv_path = _write_csv(tmp_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "already_closed_unknown_open", "pnl_source": "pending",
        "position_id": "PID",
    })
    new_pos = _matched(position_id="PID", close_reason="take_profit",
                       datetime_open="2026-04-26T20:45:00", pnl_source="discord",
                       pnl_sol=Decimal("0.5"))

    merged, _ = merge_with_existing_csv([new_pos], [], csv_path)

    assert merged[0].close_reason == "take_profit"
    assert merged[0].pnl_source == "discord"
    assert merged[0].pnl_sol == Decimal("0.5")
