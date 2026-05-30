from decimal import Decimal

from valhalla.event_parser import EventParser
from valhalla.matcher import PositionMatcher
from valhalla.models import (
    AddLiquidityEvent,
    AlreadyClosedEvent,
    CloseEvent,
    FailsafeEvent,
    MeteoraPnlResult,
    OpenEvent,
    RugEvent,
)


DATE = "2026-04-26"
PID = "PID00001"


def _open_event(pid=PID):
    return OpenEvent(
        timestamp="[10:00]",
        position_type="BidAsk",
        token_name="TOKEN",
        token_pair="TOKEN-SOL",
        target="wallet",
        market_cap=123.45,
        token_age="5h ago",
        jup_score=81,
        target_sol=1.0,
        your_sol=2.0,
        position_id=pid,
        date=DATE,
    )


def _close_event(pid=PID, close_type="normal"):
    return CloseEvent(
        timestamp="[10:30]",
        target="wallet",
        starting_sol=10.0,
        starting_usd=100.0,
        ending_sol=13.0,
        ending_usd=130.0,
        position_id=pid,
        close_type=close_type,
        date=DATE,
    )


def _rug_event(pid=PID):
    return RugEvent(
        timestamp="[10:45]",
        target="wallet",
        token_pair="TOKEN-SOL",
        position_address="ADDR",
        price_drop=25.0,
        threshold=20.0,
        position_id=pid,
        date=DATE,
    )


def _failsafe_event(pid=PID):
    return FailsafeEvent(timestamp="[10:50]", position_id=pid, date=DATE)


def _already_closed_event(pid=PID):
    return AlreadyClosedEvent(
        timestamp="[11:00]",
        position_id=pid,
        position_address="ADDR",
        target="wallet",
        date=DATE,
    )


def _meteora_result():
    return MeteoraPnlResult(
        deposited_sol=Decimal("2.50"),
        withdrawn_sol=Decimal("3.10"),
        fees_sol=Decimal("0.20"),
        deposited_usd=Decimal("100"),
        withdrawn_usd=Decimal("120"),
        fees_usd=Decimal("8"),
        pnl_usd=Decimal("28"),
        pnl_sol=Decimal("0.70"),
    )


def _match(
    *,
    opens=(),
    closes=(),
    rugs=(),
    failsafes=(),
    already_closed=(),
    add_liquidity=(),
    meteora_results=None,
    use_discord_pnl=False,
):
    parser = EventParser(base_date=DATE)
    parser.open_events = list(opens)
    parser.close_events = list(closes)
    parser.rug_events = list(rugs)
    parser.failsafe_events = list(failsafes)
    parser.already_closed_events = list(already_closed)
    parser.add_liquidity_events = list(add_liquidity)

    return PositionMatcher(parser).match_positions(
        meteora_results or {},
        {PID: "RESOLVED"},
        use_discord_pnl=use_discord_pnl,
    )


def _single_position(**kwargs):
    matched, unmatched = _match(**kwargs)

    assert unmatched == []
    assert len(matched) == 1
    return matched[0]


def test_close_reason_normal():
    position = _single_position(opens=[_open_event()], closes=[_close_event()])

    assert position.close_reason == "normal"


def test_close_reason_take_profit():
    position = _single_position(
        opens=[_open_event()],
        closes=[_close_event(close_type="take_profit")],
    )

    assert position.close_reason == "take_profit"


def test_close_reason_stop_loss():
    position = _single_position(
        opens=[_open_event()],
        closes=[_close_event(close_type="stop_loss")],
    )

    assert position.close_reason == "stop_loss"


def test_close_reason_failsafe_close_event_override():
    position = _single_position(
        opens=[_open_event()],
        closes=[_close_event()],
        failsafes=[_failsafe_event()],
    )

    assert position.close_reason == "failsafe"
    assert position.datetime_close == "2026-04-26T10:30:00"


def test_close_reason_unknown_open():
    position = _single_position(closes=[_close_event()])

    assert position.close_reason == "unknown_open"
    assert position.token == "unknown"
    assert position.position_type == "unknown"
    assert position.datetime_open == ""
    assert position.datetime_close == "2026-04-26T10:30:00"


def test_close_reason_take_profit_unknown_open():
    position = _single_position(closes=[_close_event(close_type="take_profit")])

    assert position.close_reason == "take_profit_unknown_open"


def test_close_reason_stop_loss_unknown_open():
    position = _single_position(closes=[_close_event(close_type="stop_loss")])

    assert position.close_reason == "stop_loss_unknown_open"


def test_close_reason_rug():
    position = _single_position(opens=[_open_event()], rugs=[_rug_event()])

    assert position.close_reason == "rug"
    assert position.price_drop_pct == 25.0


def test_close_reason_rug_unknown_open():
    position = _single_position(rugs=[_rug_event()])

    assert position.close_reason == "rug_unknown_open"
    assert position.token == "unknown"
    assert position.datetime_open == ""


def test_close_reason_rug_unknown_open_without_position_id():
    position = _single_position(rugs=[_rug_event(pid=None)])

    assert position.close_reason == "rug_unknown_open"
    assert position.position_id == ""


def test_close_reason_standalone_failsafe():
    position = _single_position(opens=[_open_event()], failsafes=[_failsafe_event()])

    assert position.close_reason == "failsafe"
    assert position.datetime_close == "2026-04-26T10:50:00"


def test_close_reason_failsafe_unknown_open():
    position = _single_position(failsafes=[_failsafe_event()])

    assert position.close_reason == "failsafe_unknown_open"
    assert position.target_wallet == "unknown"
    assert position.datetime_open == ""


def test_close_reason_already_closed():
    position = _single_position(
        opens=[_open_event()],
        already_closed=[_already_closed_event()],
    )

    assert position.close_reason == "already_closed"


def test_close_reason_already_closed_unknown_open():
    position = _single_position(already_closed=[_already_closed_event()])

    assert position.close_reason == "already_closed_unknown_open"
    assert position.full_address == "RESOLVED"
    assert position.datetime_open == ""


def test_unmatched_open_is_returned_for_still_open_csv_handling():
    matched, unmatched = _match(opens=[_open_event()])

    assert matched == []
    assert [event.position_id for event in unmatched] == [PID]


def test_pnl_source_meteora_takes_precedence_over_discord_flag():
    position = _single_position(
        opens=[_open_event()],
        closes=[_close_event()],
        meteora_results={PID: _meteora_result()},
        use_discord_pnl=True,
    )

    assert position.pnl_source == "meteora"
    assert position.sol_deployed == Decimal("2.50")
    assert position.sol_received == Decimal("3.10")
    assert position.pnl_sol == Decimal("0.70")
    assert position.pnl_pct == Decimal("28.00")
    assert position.meteora_pnl == Decimal("0.70")


def test_pnl_source_discord_uses_discord_balances_and_liquidity():
    position = _single_position(
        opens=[_open_event()],
        closes=[_close_event()],
        add_liquidity=[
            AddLiquidityEvent("[10:10]", PID, "wallet", amount_sol=0.5, date=DATE)
        ],
        use_discord_pnl=True,
    )

    assert position.pnl_source == "discord"
    assert position.sol_deployed == Decimal("2.5")
    assert position.sol_received == Decimal("3.0")
    assert position.pnl_sol == Decimal("0.5")
    assert position.pnl_pct == Decimal("20.0")


def test_pnl_source_pending_leaves_pnl_empty_without_meteora_or_flag():
    position = _single_position(opens=[_open_event()], closes=[_close_event()])

    assert position.pnl_source == "pending"
    assert position.sol_deployed is None
    assert position.sol_received is None
    assert position.pnl_sol is None
    assert position.pnl_pct is None
