import csv
from decimal import Decimal

from valhalla.merge import merge_with_existing_csv
from valhalla.models import MatchedPosition


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


def test_merge_upgrades_already_closed_unknown_open_to_rug(tmp_path):
    csv_path = tmp_path / "positions.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerow({
            "datetime_open": "2026-04-26T20:45:00",
            "datetime_close": "2026-04-26T22:11:00",
            "target_wallet": "20260420_3CPwnjLS",
            "token": "HENRY",
            "position_type": "BidAsk",
            "sol_deployed": "2.5000",
            "pnl_sol": "-0.6964",
            "pnl_pct": "-27.86",
            "close_reason": "already_closed_unknown_open",
            "mc_at_open": "2513039.59",
            "jup_score": "91",
            "position_id": "7zfQwEox",
            "full_address": "7zfQH8rscwc1aokGpEQiwgVjXJChuKsjREw4V81WwEox",
            "pnl_source": "meteora",
            "meteora_deposited": "2.5000",
            "meteora_withdrawn": "1.6832",
            "meteora_fees": "0.1203",
            "meteora_pnl": "-0.6964",
        })

    new_pos = MatchedPosition(
        datetime_open="2026-04-26T20:45:00",
        datetime_close="2026-04-26T21:20:00",
        target_wallet="20260420_3CPwnjLS",
        token="HENRY",
        position_type="BidAsk",
        sol_deployed=Decimal("2.5"),
        sol_received=None,
        pnl_sol=None,
        pnl_pct=None,
        close_reason="rug",
        mc_at_open=2513039.59,
        jup_score=91,
        token_age="16h ago",
        token_age_days=0,
        token_age_hours=16,
        price_drop_pct=21.68,
        position_id="7zfQwEox",
        full_address="7zfQH8rscwc1aokGpEQiwgVjXJChuKsjREw4V81WwEox",
        pnl_source="pending",
    )

    merged, still_open = merge_with_existing_csv([new_pos], [], str(csv_path))

    assert still_open == []
    assert len(merged) == 1
    assert merged[0].close_reason == "rug"
    assert merged[0].price_drop_pct == 21.68
    assert merged[0].pnl_source == "meteora"
    assert merged[0].pnl_sol == Decimal("-0.6964")
