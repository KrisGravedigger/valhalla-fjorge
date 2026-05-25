"""Scan Solana history for pure SOL portfolio capital flows.

Two backends:
- Helius enhanced API (default when HELIUS_API_KEY / helius-rpc.com URL):
    GET /v0/addresses/{wallet}/transactions?type=TRANSFER
    Returns only transfer-type transactions server-side; no per-tx fetch needed.
- Standard RPC fallback: getSignaturesForAddress + getTransaction per tx.
    Very slow for active wallets; prints a warning.
"""

from __future__ import annotations

import csv
import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from .solana_rpc import SolanaRpcClient


LAMPORTS_PER_SOL = Decimal("1000000000")
SOL_QUANT = Decimal("0.000001")

# Programs allowed in a pure SOL transfer (standard RPC fallback path)
PURE_SOL_ALLOW = {
    "11111111111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
}

_HELIUS_ENHANCED_BASE = "https://api.helius.xyz"
_HELIUS_PAGE_SIZE = 100


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
        """Return new pure-SOL flow rows not already in flows_path."""
        self.watermark_to_write = None
        existing_sigs = self.get_existing_signatures(flows_path)
        watermark = _load_watermark(watermark_path)
        last_sig = watermark.get("last_signature")
        cutoff_ts = _start_date_timestamp(start_date) if not last_sig and start_date else None

        rpc_url = getattr(self.rpc_client, "rpc_url", "")
        helius_key = _extract_helius_key(rpc_url)
        if helius_key:
            raw_txs = self._helius_fetch_transfers(
                helius_key, last_sig=last_sig, cutoff_ts=cutoff_ts
            )
            parser = self._parse_helius_tx
        else:
            if rpc_url:
                print(
                    "WARNING: no Helius API key found; falling back to standard RPC scan. "
                    "This is very slow for active wallets. Set HELIUS_API_KEY in .env."
                )
            raw_txs = self._rpc_fetch_transfers(last_sig=last_sig, cutoff_ts=cutoff_ts)
            # _rpc_fetch_transfers embeds "signature" key into each tx dict
            parser = lambda tx: self._classify_transaction(tx.get("signature", ""), tx)

        if not raw_txs:
            return []

        # Watermark = most recent signature (first element, Helius returns newest first)
        newest_sig = raw_txs[0].get("signature") if raw_txs else None
        if newest_sig:
            self.watermark_to_write = {
                "last_signature": newest_sig,
                "scan_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }

        rows: list[FlowRow] = []
        for tx_data in raw_txs:
            sig = tx_data.get("signature")
            if not sig or sig in existing_sigs:
                continue
            row = parser(tx_data)
            if row is not None:
                rows.append(row)

        return rows

    # ── Helius enhanced API path ──────────────────────────────────────────────

    def _helius_fetch_transfers(
        self,
        api_key: str,
        last_sig: str | None,
        cutoff_ts: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch TRANSFER-type transactions from Helius enhanced API.

        Paginates newest-first. Stops when last_sig is encountered (watermark)
        or when blockTime drops below cutoff_ts (start_date).
        Returns newest-first list of raw Helius transaction objects.
        """
        url_base = (
            f"{_HELIUS_ENHANCED_BASE}/v0/addresses/{self.our_wallet}/transactions"
            f"?api-key={api_key}&type=TRANSFER"
        )
        collected: list[dict[str, Any]] = []
        before: str | None = None

        while True:
            url = url_base
            if before:
                url += f"&before={urllib.parse.quote(before)}"

            page = _http_get_json(url)
            if not page:
                break

            stop = False
            for tx in page:
                sig = tx.get("signature", "")
                if last_sig and sig == last_sig:
                    stop = True
                    break
                ts = tx.get("timestamp")
                if cutoff_ts is not None and ts is not None and ts < cutoff_ts:
                    stop = True
                    break
                collected.append(tx)

            if stop or len(page) < _HELIUS_PAGE_SIZE:
                break
            before = page[-1].get("signature")
            if not before:
                break

        return collected

    def _parse_helius_tx(self, tx_data: dict[str, Any]) -> FlowRow | None:
        """Extract FlowRow from a Helius enhanced transaction object.

        Accepts only pure wallet-to-wallet SOL transfers:
        - source=SYSTEM_PROGRAM (no DeFi program as primary driver)
        - tokenTransfers=[] (empty — any token movement signals a DeFi op:
          LP fee claim, Meteora deposit/withdrawal, swap, etc.)
        """
        if tx_data.get("source") != "SYSTEM_PROGRAM":
            return None
        if tx_data.get("tokenTransfers"):
            return None

        sig = tx_data.get("signature", "")
        ts = tx_data.get("timestamp")
        if not ts:
            return None

        # Use nativeTransfers to find explicit SOL movements for our wallet.
        # This excludes transactions where we are only a fee payer.
        native_transfers = tx_data.get("nativeTransfers") or []
        net_lamports = 0
        for transfer in native_transfers:
            amount = transfer.get("amount", 0)
            if transfer.get("toUserAccount") == self.our_wallet:
                net_lamports += amount
            elif transfer.get("fromUserAccount") == self.our_wallet:
                net_lamports -= amount

        if net_lamports == 0:
            return None

        amount = (Decimal(abs(net_lamports)) / LAMPORTS_PER_SOL).quantize(SOL_QUANT)
        return FlowRow(
            timestamp_utc=datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            wallet="portfolio",
            type="deposit" if net_lamports > 0 else "withdrawal",
            sol_amount=amount,
            tx_signature=sig,
            notes="autoscan",
        )

    # ── Standard RPC fallback path ────────────────────────────────────────────

    def _rpc_fetch_transfers(
        self,
        last_sig: str | None,
        cutoff_ts: int | None,
    ) -> list[dict[str, Any]]:
        """Fetch all signatures then retrieve each transaction. Very slow."""
        sigs = self._collect_signatures(until=last_sig, cutoff_ts=cutoff_ts)
        txs = []
        for info in sigs:
            sig = info.get("signature")
            if not sig:
                continue
            tx = self._get_transaction_json(sig)
            if tx:
                # embed signature so _classify_transaction can retrieve it
                tx = dict(tx, signature=sig)
                txs.append(tx)
        return txs

    def _collect_signatures(
        self,
        until: str | None,
        cutoff_ts: int | None,
    ) -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []
        before: str | None = None

        while True:
            page = self.rpc_client.get_signatures_for_address(
                self.our_wallet, limit=1000, before=before, until=until
            )
            if not page:
                break

            stop_after = False
            for info in page:
                block_time = info.get("blockTime")
                if cutoff_ts is not None and block_time is not None and block_time < cutoff_ts:
                    stop_after = True
                    continue
                collected.append(info)

            last = page[-1].get("signature")
            if stop_after or len(page) < 1000 or not last:
                break
            before = last

        return collected

    def _get_transaction_json(self, signature: str) -> Optional[dict[str, Any]]:
        if hasattr(self.rpc_client, "get_transaction_json"):
            return self.rpc_client.get_transaction_json(signature)
        return None

    def _classify_transaction(
        self,
        signature: str,
        tx_data: dict[str, Any],
        fallback_block_time: int | None = None,
    ) -> FlowRow | None:
        """Classify a standard getTransaction(jsonParsed) response."""
        meta = tx_data.get("meta") or {}
        if meta.get("err") is not None:
            return None

        message = (tx_data.get("transaction") or {}).get("message") or {}
        instructions = message.get("instructions") or []
        program_ids: set[str] = set()
        for instr in instructions:
            if not isinstance(instr, dict):
                return None
            pid = instr.get("programId")
            if not pid:
                return None
            program_ids.add(pid)

        if not program_ids or not program_ids.issubset(PURE_SOL_ALLOW):
            return None

        account_keys = [_account_key(k) for k in message.get("accountKeys") or []]
        try:
            idx = account_keys.index(self.our_wallet)
        except ValueError:
            return None

        pre = meta.get("preBalances") or []
        post = meta.get("postBalances") or []
        if idx >= len(pre) or idx >= len(post):
            return None

        delta = int(post[idx]) - int(pre[idx])
        if delta == 0:
            return None

        block_time = tx_data.get("blockTime") or fallback_block_time
        if block_time is None:
            return None

        amount = (Decimal(abs(delta)) / LAMPORTS_PER_SOL).quantize(SOL_QUANT)
        return FlowRow(
            timestamp_utc=datetime.fromtimestamp(int(block_time), tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
            wallet="portfolio",
            type="deposit" if delta > 0 else "withdrawal",
            sol_amount=amount,
            tx_signature=signature,
            notes="autoscan",
        )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _extract_helius_key(rpc_url: str) -> str | None:
    """Extract api-key from a Helius RPC URL, or return None if not Helius."""
    if "helius" not in rpc_url.lower():
        return None
    parsed = urllib.parse.urlparse(rpc_url)
    params = urllib.parse.parse_qs(parsed.query)
    keys = params.get("api-key") or params.get("apiKey") or []
    return keys[0] if keys else None


def _http_get_json(url: str, retries: int = 3) -> list[Any]:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "Accept": "application/json",
                    "User-Agent": "valhalla-sol-flow-scanner/1.0",
                },
            )
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            if exc.code == 429 and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except Exception:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                raise
    return []


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
