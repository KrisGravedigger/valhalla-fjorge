from __future__ import annotations

import csv
import shutil
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

from valhalla.sol_flow_scanner import SolFlowScanner


WALLET = "Wallet111111111111111111111111111111111"
OTHER = "Other1111111111111111111111111111111111"
SYSTEM = "11111111111111111111111111111111"
COMPUTE = "ComputeBudget111111111111111111111111111111"
JUPITER = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"


class FakeRpc:
    def __init__(self, signatures: list[dict[str, Any]], txs: dict[str, dict[str, Any]]):
        self.signatures = signatures
        self.txs = txs

    def get_signatures_for_address(
        self,
        _address: str,
        limit: int = 1000,
        before: str | None = None,
        until: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = self.signatures
        if until:
            rows = [row for row in rows if row["signature"] != until]
        if before:
            idx = next(i for i, row in enumerate(rows) if row["signature"] == before)
            rows = rows[idx + 1 :]
        return rows[:limit]

    def get_transaction_json(self, signature: str) -> dict[str, Any] | None:
        return self.txs.get(signature)


def _tx(
    *,
    pre: int,
    post: int,
    programs: list[str] | None = None,
    block_time: int = 1_775_000_000,
    err: Any = None,
) -> dict[str, Any]:
    return {
        "blockTime": block_time,
        "meta": {
            "err": err,
            "preBalances": [pre, 5_000_000_000],
            "postBalances": [post, 5_000_000_000],
        },
        "transaction": {
            "message": {
                "accountKeys": [{"pubkey": WALLET}, {"pubkey": OTHER}],
                "instructions": [
                    {"programId": program_id} for program_id in (programs or [SYSTEM])
                ],
            }
        },
    }


def _scanner_for(tx: dict[str, Any]) -> SolFlowScanner:
    return SolFlowScanner(
        FakeRpc([{"signature": "SIG1", "blockTime": tx["blockTime"], "err": None}], {"SIG1": tx}),
        WALLET,
    )


def test_classify_pure_sol_transfer() -> None:
    scanner = _scanner_for(
        _tx(pre=1_000_000_000, post=3_500_000_000, programs=[COMPUTE, SYSTEM])
    )

    row = scanner._classify_transaction("SIG1", scanner.rpc_client.txs["SIG1"])

    assert row is not None
    assert row.type == "deposit"
    assert row.sol_amount == Decimal("2.500000")


def test_classify_swap_skipped() -> None:
    scanner = _scanner_for(_tx(pre=1_000_000_000, post=2_000_000_000, programs=[JUPITER]))

    row = scanner._classify_transaction("SIG1", scanner.rpc_client.txs["SIG1"])

    assert row is None


def test_deposit_direction() -> None:
    scanner = _scanner_for(_tx(pre=1_000_000_000, post=2_000_000_000))

    row = scanner._classify_transaction("SIG1", scanner.rpc_client.txs["SIG1"])

    assert row is not None
    assert row.type == "deposit"
    assert row.sol_amount == Decimal("1.000000")


def test_withdrawal_direction() -> None:
    scanner = _scanner_for(_tx(pre=2_000_000_000, post=750_000_000))

    row = scanner._classify_transaction("SIG1", scanner.rpc_client.txs["SIG1"])

    assert row is not None
    assert row.type == "withdrawal"
    assert row.sol_amount == Decimal("1.250000")


def test_dedup_skips_known_signature() -> None:
    tmp_path = Path("_temp") / f"sol_flow_scanner_{uuid.uuid4().hex}"
    tmp_path.mkdir(parents=True, exist_ok=False)
    flows_path = tmp_path / "capital_flows.csv"
    watermark_path = tmp_path / "sol_flow_scan.json"
    with flows_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "timestamp_utc",
                "wallet",
                "type",
                "sol_amount",
                "tx_signature",
                "notes",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "timestamp_utc": "2026-03-01T00:00:00Z",
                "wallet": "portfolio",
                "type": "deposit",
                "sol_amount": "1.000000",
                "tx_signature": "SIG1",
                "notes": "autoscan",
            }
        )
    scanner = _scanner_for(_tx(pre=1_000_000_000, post=2_000_000_000))

    try:
        rows = scanner.scan_new(flows_path, watermark_path, start_date="2026-01-01")
    finally:
        shutil.rmtree(tmp_path, ignore_errors=True)

    assert rows == []
