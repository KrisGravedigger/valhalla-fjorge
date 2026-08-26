from pathlib import Path
import re

from dce_to_input import convert_dce_json
from valhalla.event_parser import EventParser
from valhalla.readers import PlainTextReader


ROOT = Path(__file__).resolve().parents[1]
FIXTURE = ROOT / "tests" / "fixtures" / "discord_embeds" / "valhalla_format_variants.json"
ARCHIVE = ROOT / "archive" / "20260819T2255-20260823T0611_dce_20260819_213443_discord.txt"


def _parse_fixture(tmp_path):
    text, _ = convert_dce_json(FIXTURE)
    path = tmp_path / "variants.txt"
    path.write_text(text, encoding="utf-8")
    messages = PlainTextReader(str(path)).read()
    parser = EventParser()
    parser.parse_messages(messages)
    return messages, parser


def test_all_fixture_variants_parse_and_every_supported_bucket_is_populated(tmp_path):
    messages, parser = _parse_fixture(tmp_path)

    assert len(messages) == 45
    assert not parser.unparsed_counts
    assert all((
        parser.open_events, parser.close_events, parser.swap_events, parser.rug_events,
        parser.skip_events, parser.already_closed_events,
        parser.insufficient_balance_events, parser.failsafe_events,
    ))


def test_open_close_swap_already_closed_and_skip_values_match_all_dialects(tmp_path):
    _, parser = _parse_fixture(tmp_path)

    opens = {event.position_id: event for event in parser.open_events}
    assert (opens["3mXLJHz6"].token_pair, opens["3mXLJHz6"].target_sol) == ("MANIFEST-SOL", 100.0)
    assert (opens["4jbUhYhE"].token_pair, opens["4jbUhYhE"].your_sol) == ("EYE-SOL", 0.6583)
    assert (opens["HGh5nXn9"].token_pair, opens["HGh5nXn9"].target_sol) == ("STONK-SOL", 45.04)
    stable_pair = opens["GcWNhNc8"]
    assert (
        stable_pair.token_pair, stable_pair.position_type, stable_pair.your_sol,
        stable_pair.target_sol, stable_pair.target, stable_pair.market_cap,
        stable_pair.token_age, stable_pair.jup_score,
    ) == ("SOL-USDC", "Spot", 1.0, 60.0, "20260618_gh7gW8Ft", 0.0, "", 0)

    closes = {event.position_id: event for event in parser.close_events}
    assert (closes["8HBv2wgG"].starting_sol, closes["8HBv2wgG"].ending_usd) == (0.42, 141.68)
    assert (closes["E1hGhBX7"].starting_sol, closes["E1hGhBX7"].active_positions) == (2.80, 63)
    assert (closes["HDQASnvm"].ending_sol, closes["HDQASnvm"].total_sol) == (1.28, 68.43)

    swaps = {(event.amount, event.token_name, event.token_address) for event in parser.swap_events}
    assert ("1273", "Jimothy The Raccoon", "Ge87EtsjwRQbHaqQmKRno69RFTwh9bfSsm99XNxTpump") in swaps
    assert ("3809", "BULLS'S EYE", "RmtMAYVTTFv2iK9muMrXEoAnSSsZPPgRPbqZCKwNDYk") in swaps
    assert ("15", "Cate", "Ai66LHZG9MCzg1WKdawwqduVAXpNDUuV8M3uyq5ppump") in swaps

    already_closed = {event.position_id: event.target for event in parser.already_closed_events}
    assert already_closed["9ExtJUud"] == "20260821_8bHpPjob"
    assert already_closed["3h8EN83f"] == "20260618_gh7gW8Ft"
    assert already_closed["51QH4qRJ"] == "20260713_FVGQRHDD5"
    assert {event.reason for event in parser.skip_events} >= {
        "low market cap", "token age restriction", "low Jupiter organic score",
    }


def test_line_scoped_transaction_attribution_does_not_cross_lists(tmp_path):
    messages, _ = _parse_fixture(tmp_path)
    by_position = {
        message.clean_text[message.clean_text.index("Closed DLMM Position!"):].splitlines()[0]: message
        for message in messages
        if "Closed DLMM Position!" in message.clean_text
    }

    for prefix in (
        "Closed DLMM Position! (8HBv...2wgG)",
        "Closed DLMM Position! (E1hG...hBX7)",
        "Closed DLMM Position! (HDQA...Snvm)",
    ):
        message = by_position[prefix]
        assert message.bot_tx_signatures
        assert message.target_tx_signatures
        assert not set(message.bot_tx_signatures) & set(message.target_tx_signatures)
        all_signatures = message.bot_tx_signatures + message.target_tx_signatures
        assert all(
            all(domain not in item for domain in ("orbmarkets.io", "metlex.io", "dexscreener.com", "jup.ag", "valhalla-bot.app"))
            for item in all_signatures
        )

    assert len(by_position["Closed DLMM Position! (HDQA...Snvm)"].bot_tx_signatures) == 2
    assert len(by_position["Closed DLMM Position! (HDQA...Snvm)"].target_tx_signatures) == 2



def test_already_closed_and_failsafe_transaction_attribution(tmp_path):
    messages, _ = _parse_fixture(tmp_path)

    expected_already_closed = {
        "9Ext...JUud": [
            "61ctABvsbfuYFGDCyrUs7rbsYpF1Uq7PG9stNuivZs3qsK5hPcedTFbRLTaB99D9jRFCmvHjjwxBvAVNXxDC82g7",
            "BhdxLj8JJpw1AxzUvC7ucKUcBGArTyvAtQCjNzKTZes8gShB8MZdhtcfZfUTgMAM5GVS3icCmvRbuXLpAokSSBg",
        ],
        "3h8E...N83f": [
            "2rhb2RGktkoFueC6aTD22mE4cv6fzRgnWKccjtpzEkj2WbirF39DR8CotXAFZWM9V7kRCNKaABCTYrDzrzgm347E",
            "MBdJsRWNgJW2kjLrMGJYTsczNdj4YHno8G1WXJo3BETLTas89BfbFfjQE65qFfaWGFTZKVtCuo7oQdwjULucTPC",
        ],
        "51QH...4qRJ": [
            "65z43gTN2tC9T2BS7uySpGqy6QnJiMNFSpbwDftCf1G92ikz8DegJhUFFVnfpDGnxrjCmkinWQxu9jLn6itQ6dzu",
            "5AUisCNUudx8NBpQ1Rfrc2y6FZiASicpKsCidv2hw1j5KfCKReozRsZhYrjkxbss2g1Xfwb9dY6DBqzXxQVAP1Z9",
        ],
    }
    for position_id, expected_signatures in expected_already_closed.items():
        message = next(
            item for item in messages
            if 'was already closed' in item.clean_text and position_id in item.clean_text
        )
        assert message.bot_tx_signatures == []
        assert message.target_tx_signatures == expected_signatures
        assert not set(message.bot_tx_signatures) & set(message.target_tx_signatures)

    failsafe = next(item for item in messages if "Failsafe Activated" in item.clean_text)
    assert failsafe.bot_tx_signatures == [
        "4FXLEGeYZhWZF66HKM17Sg2G5AnKwjbN29EeB22eP1ThzD9R5CsMQwwRPyMLmRnGUhFkVm6sBZjzptRiCEiBK8UP",
        "2bJASEbwEdy3pKXAjGFvixmpBLWhdB8N536ELPbqp53GynM4mYFs3KXNUYmLmTJMhKbx2XsxAa7RRVRnDJ2A8XFc",
    ]
    assert failsafe.target_tx_signatures == []
def test_gen3_footer_position_id_and_missing_footer_id_are_safe():
    parser = EventParser()
    template = "\n".join([
        "Opened \u00b7 DLMM \u00b7 BidAsk 1-Sided",
        "LOOKSMAX-SOL",
        "### 0.50 SOL",
        "7% of target's 7.14 \u00b7 **MC** $1,574,702.576 \u00b7 **Age** 1w \u00b7 **Jup** 75",
        "**Target** 20260713_5iB13i7i",
        "Valhalla \u00b7 CV1C...BoBG",
    ])
    parser._classify_and_parse_message("[2026-08-23T15:31]", template, [])
    assert parser.open_events[0].position_id == "CV1CBoBG"

    for footer in ("Valhalla", "Valhalla \u00b7 No action needed"):
        clean = EventParser()
        clean._classify_and_parse_message("[2026-08-23T15:31]", template.replace("Valhalla \u00b7 CV1C...BoBG", footer), [])
        assert not clean.open_events
        assert clean.unparsed_counts

def test_gen1_open_without_market_cap_remains_unparsed(tmp_path):
    messages, _ = _parse_fixture(tmp_path)
    gen1_open = next(
        item.clean_text for item in messages
        if "Opened New DLMM Position! (3mXL...JHz6)" in item.clean_text
    )
    damaged_open = re.sub(r'(?<!\w)MC:', 'Market Cap:', gen1_open, count=1)

    parser = EventParser()
    parser._classify_and_parse_message("[2026-08-23T06:00]", damaged_open, [])

    assert not parser.open_events
    assert parser.unparsed_counts["Opened New DLMM Position"] == 1



def test_unknown_embed_author_is_counted_loudly():
    parser = EventParser()
    parser._classify_and_parse_message("[2026-08-23T15:31]", "Teleported \u00b7 DLMM\nunrecognised body", [])

    assert any("Teleported" in key for key in parser.unparsed_counts)


def test_archived_gen1_counts_and_open_close_records_match_head_baseline():
    messages = PlainTextReader(str(ARCHIVE)).read()
    parser = EventParser()
    parser.parse_messages(messages)

    assert len(messages) == 1801
    assert len(parser.open_events) == 569
    assert len(parser.close_events) == 525
    # Hashes cover every open/close field, including IDs and tx signatures.
    from dataclasses import asdict
    from hashlib import sha256
    from json import dumps
    assert sha256(dumps([asdict(event) for event in parser.open_events], sort_keys=True, default=str).encode()).hexdigest() == "c9366d1c0559f0e7d8db8a494ea8eca9d65fad99a6c583de59832c8606488c60"
    assert sha256(dumps([asdict(event) for event in parser.close_events], sort_keys=True, default=str).encode()).hexdigest() == "ecd47a51bfa7c70d63436f347c3a3c6943d07eac583a251e884411bebba33d7a"
