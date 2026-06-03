"""
Targeted characterization net for merge.py, ahead of the S6 in-file extraction.

Scope rationale (advisor-reviewed): the baseline gate already diffs the real
positions.csv (S1.6), so for merge_with_existing_csv it effectively checks all
28 reconstruction fields across ~13k rows. A full S5-style net would mostly
duplicate that. Only two genuine gaps remain, and this file closes exactly them:

1. Fields that are uniformly empty in the baseline data (e.g. source_wallet_*,
   original_wallet) — a drop there would not move the baseline CSV. One
   round-trip test with ALL 28 fields non-empty pins them.

2. merge_positions_csvs() is fully dark: reachable only via `--merge` (cli.py),
   which neither --report nor --parse exercise, and it has no other test. It is
   also exactly where the position_id reconstruction differs from
   merge_with_existing_csv (stripped vs unstripped — see S6 notes), so a shared
   _row_to_matched_position helper must NOT be folded into it blindly. This net
   is written BEFORE any extraction (same "net first, independently" rule as the
   S5 matcher net) so sharing a helper into the dark function is safe.

The S6 extraction removes cross-function duplication only (parse helpers,
_row_to_matched_position, open-event stub). The Rules 1-4 merge loop is left
intact (high-risk financial branching with pinned quirks — same call as S5-3).
"""

import csv
from dataclasses import asdict
from decimal import Decimal

from valhalla.merge import merge_with_existing_csv, merge_positions_csvs


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


def _write_csv(path, *rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in rows:
            full = {name: "" for name in FIELDNAMES}
            full.update(row)
            writer.writerow(full)


# --------------------------------------------------------------------------- #
# Gap 1: full 28-field round-trip through the CSV-row -> MatchedPosition
# reconstruction. Uses a Rule-1 "kept complete, no new data" path so the
# reconstructed object passes through merge_with_existing_csv unchanged.
# --------------------------------------------------------------------------- #
def test_row_reconstruction_round_trip_all_fields(tmp_path):
    csv_path = str(tmp_path / "positions.csv")
    _write_csv(csv_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "target_wallet": "w1",
        "token": "HENRY",
        "position_type": "BidAsk",
        "sol_deployed": "2.5",
        "sol_received": "3.1",
        "pnl_sol": "0.6",
        "pnl_pct": "24.0",
        "close_reason": "normal",
        "mc_at_open": "2500.5",
        "jup_score": "91",
        "token_age": "16h ago",
        "token_age_days": "0",
        "token_age_hours": "16",
        "price_drop_pct": "12.5",
        "position_id": "PID",
        "full_address": "FULLADDR",
        "pnl_source": "meteora",
        "meteora_deposited": "2.5",
        "meteora_withdrawn": "3.1",
        "meteora_fees": "0.2",
        "meteora_pnl": "0.6",
        "target_wallet_address": "TWADDR",
        "target_tx_signature": "TXSIG",
        "source_wallet_hold_min": "120",
        "source_wallet_pnl_pct": "15.5",
        "source_wallet_scenario": "scenario_a",
        "original_wallet": "OW1",
    })

    merged, still_open = merge_with_existing_csv([], [], csv_path)

    assert still_open == []
    assert len(merged) == 1
    # Full-object assertion: every field of the reconstruction is pinned, so an
    # extracted _row_to_matched_position dropping/swapping any field fires here.
    assert asdict(merged[0]) == {
        "target_wallet": "w1",
        "token": "HENRY",
        "position_type": "BidAsk",
        "sol_deployed": Decimal("2.5"),
        "sol_received": Decimal("3.1"),
        "pnl_sol": Decimal("0.6"),
        "pnl_pct": Decimal("24.0"),
        "close_reason": "normal",
        "mc_at_open": 2500.5,
        "jup_score": 91,
        "token_age": "16h ago",
        "token_age_days": 0,
        "token_age_hours": 16,
        "price_drop_pct": 12.5,
        "position_id": "PID",
        "full_address": "FULLADDR",
        "pnl_source": "meteora",
        "meteora_deposited": Decimal("2.5"),
        "meteora_withdrawn": Decimal("3.1"),
        "meteora_fees": Decimal("0.2"),
        "meteora_pnl": Decimal("0.6"),
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "target_wallet_address": "TWADDR",
        "target_tx_signature": "TXSIG",
        "source_wallet_hold_min": 120,
        "source_wallet_pnl_pct": Decimal("15.5"),
        "source_wallet_scenario": "scenario_a",
        "original_wallet": "OW1",
    }


def test_row_reconstruction_strips_position_id(tmp_path):
    """merge_with_existing_csv strips position_id (line 77) before reconstruction.
    Pins this so an extracted helper keeps the stripped value on this path."""
    csv_path = str(tmp_path / "positions.csv")
    _write_csv(csv_path, {
        "datetime_open": "2026-04-26T20:45:00",
        "datetime_close": "2026-04-26T21:20:00",
        "close_reason": "normal", "pnl_source": "meteora",
        "meteora_deposited": "2.5", "position_id": "  PID  ",
    })

    merged, _ = merge_with_existing_csv([], [], csv_path)

    assert merged[0].position_id == "PID"


# --------------------------------------------------------------------------- #
# Gap 2: merge_positions_csvs (dark function). Pins dedup behavior + the
# reconstruction-fed summary (Total PnL is computed from reconstructed rows,
# so a pnl_sol reconstruction drop would change it). CSV in -> CSV out, no
# network, fully deterministic.
# --------------------------------------------------------------------------- #
def test_merge_positions_csvs_dedup_and_summary(tmp_path, capsys):
    file1 = str(tmp_path / "file1.csv")
    file2 = str(tmp_path / "file2.csv")
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    _write_csv(
        file1,
        {  # P1 older version
            "close_reason": "normal", "pnl_source": "meteora",
            "position_id": "P1", "pnl_sol": "0.5",
            "datetime_open": "2026-04-26T20:00:00",
            "datetime_close": "2026-04-26T20:30:00",
        },
        {  # P2 still open (excluded from summary matched set)
            "close_reason": "still_open", "pnl_source": "pending",
            "position_id": "P2", "datetime_open": "2026-04-26T20:10:00",
        },
    )
    _write_csv(
        file2,
        {  # P1 newer version — must win the dedup (later file)
            "close_reason": "normal", "pnl_source": "meteora",
            "position_id": "P1", "pnl_sol": "0.9",
            "datetime_open": "2026-04-26T20:00:00",
            "datetime_close": "2026-04-26T20:35:00",
        },
        {  # empty position_id — kept (treated as unique)
            "close_reason": "normal", "pnl_source": "discord",
            "position_id": "", "pnl_sol": "0.3",
            "datetime_open": "2026-04-26T21:00:00",
            "datetime_close": "2026-04-26T21:30:00",
        },
    )

    merge_positions_csvs([file1, file2], str(out_dir))

    # Output positions.csv: dedup keeps later P1, the still_open P2, and the
    # empty-id row -> 3 rows; P1 resolves to the file2 version (pnl_sol 0.9).
    with open(out_dir / "positions.csv", encoding="utf-8") as f:
        out_rows = list(csv.DictReader(f))
    by_id = {r["position_id"]: r for r in out_rows if r["position_id"]}
    assert len(out_rows) == 3
    assert by_id["P1"]["pnl_sol"] == "0.9"
    assert by_id["P1"]["datetime_close"] == "2026-04-26T20:35:00"
    assert "P2" in by_id  # still_open preserved
    assert sum(1 for r in out_rows if r["position_id"] == "") == 1  # empty kept

    # summary.csv generated from reconstructed matched rows (P1=0.9, empty=0.3;
    # P2 still_open excluded) -> Total PnL 1.2 SOL. Reconstruction-sensitive.
    assert (out_dir / "summary.csv").exists()
    captured = capsys.readouterr().out
    assert "Total PnL: 1.2000 SOL" in captured
