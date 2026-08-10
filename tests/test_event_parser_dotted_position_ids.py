import pytest

from valhalla.event_parser import EventParser


NORMALIZED_ID = "Ak8JyTfR"
DOTTED_ID = "Ak8J...yTfR"
POSITION_ADDRESS = "Ak8J111111111111111111111111111111111111yTfR"


def _open_message(position_id: str) -> str:
    return "\n".join([
        f"Opened New DLMM Position! ({position_id})",
        "Target: target_wallet",
        "Spot 1-Sided Position | TOKEN-SOL",
        "MC: $1,234",
        "Age: 2h",
        "Jup Score: 42",
        "Your Pos: copy | SOL: 1.5",
        "Target Pos: target | SOL: 2.5",
    ])


def _close_message(position_id: str) -> str:
    return "\n".join([
        f"Closed DLMM Position! ({position_id})",
        "Target: target_wallet",
        "Starting SOL balance: 1 SOL ($100 USD)",
        "Ending SOL balance: 2 SOL ($200 USD)",
    ])


def _close_successful_message(position_id: str) -> str:
    return f"Position Closed Successfully (DLMM) ({position_id})"


def _failsafe_header_message(position_id: str) -> str:
    return f"Failsafe Activated (DLMM) ({position_id})"


def _add_liquidity_message(position_id: str) -> str:
    return "\n".join([
        f"Added DLMM Liquidity ({position_id})",
        "Target: target_wallet",
        "Amount: 1.25 SOL",
    ])


def _rug_message(position_id: str) -> str:
    return "\n".join([
        f"Rug Check Stop Loss Executed (DLMM) ({position_id}) (Copied From: target_wallet)",
        "Pair: TOKEN-SOL",
        f"Position: {POSITION_ADDRESS}",
        "Price Drop: 27.86%",
        "Rug Check Threshold: 20%",
    ])


def _already_closed_message(position_id: str) -> str:
    return f"Your position {POSITION_ADDRESS} was already closed ({position_id})\nTarget: target_wallet"


def _take_profit_message(position_id: str) -> str:
    return "\n".join([
        f"Take Profit Executed (DLMM) ({position_id}) (Copied From: target_wallet)",
        "Entry Value: 2.4 SOL",
        "Exit Value: 3.1 SOL",
    ])


def _stop_loss_message(position_id: str) -> str:
    return "\n".join([
        f"Stop Loss Executed (DLMM) ({position_id}) (Copied From: target_wallet)",
        "Entry Value: 2.4 SOL",
        "Exit Value: 1.9 SOL",
    ])


@pytest.mark.parametrize(
    ("message_factory", "event_collection"),
    [
        (_open_message, "open_events"),
        (_close_message, "close_events"),
        (_close_successful_message, "close_events"),
        (_failsafe_header_message, "failsafe_events"),
        (_add_liquidity_message, "add_liquidity_events"),
        (_rug_message, "rug_events"),
        (_already_closed_message, "already_closed_events"),
        (_take_profit_message, "close_events"),
        (_stop_loss_message, "close_events"),
    ],
)
@pytest.mark.parametrize("header_position_id", [NORMALIZED_ID, DOTTED_ID])
def test_all_position_event_headers_normalize_dotted_and_legacy_ids(
    message_factory, event_collection, header_position_id
):
    parser = EventParser(base_date="2026-08-05")

    parser._classify_and_parse_message("[13:00]", message_factory(header_position_id), [])

    events = getattr(parser, event_collection)
    assert len(events) == 1
    assert events[0].position_id == NORMALIZED_ID
    assert not parser.unparsed_counts


def test_failsafe_without_header_id_uses_bold_your_position_not_target_position():
    parser = EventParser(base_date="2026-08-05")
    message = "\n".join([
        "Failsafe Activated (DLMM)",
        "Target Wallet: Targ...Wall",
        "Target Position: Targ...Posi",
        "**Your Position:** Ak8J...yTfR",
        "Pool: TOKEN/SOL",
    ])

    parser._classify_and_parse_message("[13:00]", message, [])

    assert len(parser.failsafe_events) == 1
    assert parser.failsafe_events[0].position_id == NORMALIZED_ID
    assert not parser.unparsed_counts


def test_known_header_that_fails_to_parse_is_counted_but_clean_batch_is_not():
    parser = EventParser(base_date="2026-08-05")
    parser._classify_and_parse_message("[13:00]", f"Opened New DLMM Position! ({DOTTED_ID})", [])

    assert parser.unparsed_counts == {"Opened New DLMM Position": 1}

    clean_parser = EventParser(base_date="2026-08-05")
    clean_parser._classify_and_parse_message("[13:01]", _close_successful_message(DOTTED_ID), [])
    clean_parser._classify_and_parse_message("[13:02]", _failsafe_header_message(NORMALIZED_ID), [])

    assert not clean_parser.unparsed_counts


def test_dotted_rug_id_does_not_emit_position_id_mismatch_warning(capsys):
    parser = EventParser(base_date="2026-08-05")

    parser._classify_and_parse_message("[13:00]", _rug_message(DOTTED_ID), [])

    assert len(parser.rug_events) == 1
    assert parser.rug_events[0].position_id == NORMALIZED_ID
    assert "position_id mismatch" not in capsys.readouterr().out
