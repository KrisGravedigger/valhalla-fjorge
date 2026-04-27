from valhalla.event_parser import EventParser


def test_stop_loss_parser_accepts_discord_bold_header():
    parser = EventParser(base_date="2026-04-26")
    message = "\n".join(
        [
            "🚨 **Stop Loss Executed (DLMM)** (FLoFBG1d) (Copied From: 20260424_7q3uZx)",
            "Entry Value: 2.4 SOL",
            "Exit Value: 1.9 SOL",
        ]
    )

    parser._classify_and_parse_message("[10:41]", message, [])

    assert len(parser.close_events) == 1
    event = parser.close_events[0]
    assert event.position_id == "FLoFBG1d"
    assert event.close_type == "stop_loss"
    assert event.target == "20260424_7q3uZx"


def test_rug_parser_accepts_discord_bold_header_position_id():
    parser = EventParser(base_date="2026-04-26")
    message = "\n".join(
        [
            "🚨 **Rug Check Stop Loss Executed (DLMM)** (7zfQwEox) (Copied From: 20260420_3CPwnjLS)",
            "Pair: HENRY-SOL",
            "Position: 7zfQH8rscwc1aokGpEQiwgVjXJChuKsjREw4V81WwEox",
            "Price Drop: 27.86%",
            "Rug Check Threshold: 20%",
        ]
    )

    parser._classify_and_parse_message("[22:11]", message, [])

    assert len(parser.rug_events) == 1
    event = parser.rug_events[0]
    assert event.position_id == "7zfQwEox"
    assert event.position_address == "7zfQH8rscwc1aokGpEQiwgVjXJChuKsjREw4V81WwEox"
    assert event.target == "20260420_3CPwnjLS"
