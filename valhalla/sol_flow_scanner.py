"""Scan Solana history for pure SOL portfolio capital flows."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from .solana_rpc import SolanaRpcClient


LAMPORTS_PER_SOL = Decimal("1000000000")
SOL_QUANT = Decimal("0.000001")
PURE_SOL_ALLOW = {
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
}


@dataclass(frozen=True)
class FlowRow:
    timestamp_utc: str
    wallet: str
    type: str
    sol_amount: Decimal
    tx_signature: str
    notes: str

    def as_csv_row(self) -> dict[str, str]:
        return {
            "timestamp_utc": self.timestamp_utc,
            "wallet": self.wallet,
            "type": self.type,
            "sol_amount": str(self.sol_amount.quantize(SOL_QUANT)),
            "tx_signature": self.tx_signature,
            "notes": self.notes,
        }


class SolFlowScanner:
    def __init__(self, rpc_client: SolanaRpcClient, our_wallet: str):
        self.rpc_client = rpc_client
        self.our_wallet = our_wallet
        self.watermark_to_write: Optional[dict[str, str]] = None

    def get_existing_signatures(self, flows_path: Path) -> set[str]:
        """Read non-empty tx_signature values from capital_flows.csv."""
        if not flows_path.exists():
            return set()

        with flows_path.open("r", newline="", encoding="utf-8") as handle:
            return {
                row.get("tx_signature", "")
                for row in csv.DictReader(handle)
                if row.get("tx_signature")
            }

    def scan_new(
        self,
        flows_path: Path,
        watermark_path: Path,
        start_date: str | None = None,
    ) -> list[FlowRow]:
        """Return new pure-SOL flow rows that are not already in flows_path."""
        self.watermark_to_write = None
        existing_signatures = self.get_existing_signatures(flows_path)
        watermark = _load_watermark(watermark_path)
        until = watermark.get("last_signature") if watermark else None
        cutoff_ts = _start_date_timestamp(start_date) if not until and start_date else None

        signatures = self._collect_signatures(until=until, cutoff_ts=cutoff_ts)
        if not signatures:
            return []

        self.watermark_to_write = {
            "last_signature": signatures[0]["signature"],
            "scan_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

        rows: list[FlowRow] = []
        for info in signatures:
            signature = info.get("signature")
            if not signature or signature in existing_signatures:
                continue

            tx = self._get_transaction_json(signature)
            if not tx:
                continue

            row = self._classify_transaction(
                signature=signature,
                tx=tx,
                fallback_block_time=info.get("blockTime"),
            )
            if row is not None:
                rows.append(row)

        return rows

    def _collect_signatures(
        self,
        until: str | None,
        cutoff_ts: int | None,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        before: str | None = None

        while True:
            page = self.rpc_client.get_signatures_for_address(
                self.our_wallet,
                limit=1000,
                before=before,
                until=until,
            )
            if not page:
                break

            stop_after_page = False
            for info in page:
                block_time = info.get("blockTime")
                if cutoff_ts is not None and block_time is not None and block_time < cutoff_ts:
                    stop_after_page = True
                    continue
                collected.append(info)

            last_signature = page[-1].get("signature")
            if stop_after_page or len(page) < 1000 or not last_signature:
                break
            before = last_signature

        return collected

    def _get_transaction_json(self, signature: str) -> Optional[dict[str, Any]]:
        if hasattr(self.rpc_client, "get_transaction_json"):
            return self.rpc_client.get_transaction_json(signature)
        if hasattr(self.rpc_client, "get_transaction"):
            result = self.rpc_client.get_transaction(signature)
            if isinstance(result, dict):
                return result
        return None

    def _classify_transaction(
        self,
        signature: str,
        tx: dict[str, Any],
        fallback_block_time: int | None = None,
    ) -> FlowRow | None:
        meta = tx.get("meta") or {}
        if meta.get("err") is not None:
            return None

        message = (tx.get("transaction") or {}).get("message") or {}
        instructions = message.get("instructions") or []
        program_ids = set()
        for instruction in instructions:
            if not isinstance(instruction, dict):
                return None
            program_id = instruction.get("programId")
            if not program_id:
                return None
            program_ids.add(program_id)

        if not program_ids or not program_ids.issubset(PURE_SOL_ALLOW):
            return None

        account_keys = [_account_key(key) for key in message.get("accountKeys") or []]
        try:
            wallet_idx = account_keys.index(self.our_wallet)
        except ValueError:
            return None

        pre_balances = meta.get("preBalances") or []
        post_balances = meta.get("postBalances") or []
        if wallet_idx >= len(pre_balances) or wallet_idx >= len(post_balances):
            return None

        delta_lamports = int(post_balances[wallet_idx]) - int(pre_balances[wallet_idx])
        if delta_lamports == 0:
            return None

        block_time = tx.get("blockTime")
        if block_time is None:
            block_time = fallback_block_time
        if block_time is None:
            return None

        amount = (Decimal(abs(delta_lamports)) / LAMPORTS_PER_SOL).quantize(SOL_QUANT)
        return FlowRow(
            timestamp_utc=datetime.fromtimestamp(
                int(block_time), tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            wallet="portfolio",
            type="deposit" if delta_lamports > 0 else "withdrawal",
            sol_amount=amount,
            tx_signature=signature,
            notes="autoscan",
        )


def _account_key(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        return str(value.get("pubkey", ""))
    return ""


def _load_watermark(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if isinstance(data, dict) and isinstance(data.get("last_signature"), str):
        return {"last_signature": data["last_signature"]}
    return {}


def _start_date_timestamp(value: str | None) -> int | None:
    if not value:
        return None
    dt = datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp())
