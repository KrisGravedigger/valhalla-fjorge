#!/usr/bin/env python3
"""Auto-scan the main wallet for pure SOL capital-flow rows."""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from valhalla.sol_flow_scanner import FlowRow, SolFlowScanner  # noqa: E402
from valhalla.solana_rpc import SolanaRpcClient  # noqa: E402


FIELDS = [
    "timestamp_utc",
    "wallet",
    "type",
    "sol_amount",
    "tx_signature",
    "notes",
]
DEFAULT_FLOWS_PATH = PROJECT_ROOT / "output" / "capital_flows.csv"
DEFAULT_WATERMARK_PATH = PROJECT_ROOT / "output" / "sol_flow_scan.json"


def _load_env_file() -> None:
    try:
        import dotenv
    except ImportError:  # pragma: no cover - production fallback
        return
    dotenv.load_dotenv(PROJECT_ROOT / ".env")


def _resolve_rpc_url() -> str | None:
    if os.getenv("RPC_URL"):
        return os.getenv("RPC_URL")
    if os.getenv("HELIUS_RPC_URL"):
        return os.getenv("HELIUS_RPC_URL")
    api_key = os.getenv("HELIUS_API_KEY")
    if api_key:
        return f"https://mainnet.helius-rpc.com/?api-key={api_key}"
    return None


def _resolve_wallet() -> str | None:
    return os.getenv("WALLET_ADDRESS") or os.getenv("LPAGENT_WALLET")


def _default_start_date(flows_path: Path) -> str | None:
    if not flows_path.exists():
        return None
    latest = ""
    with flows_path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            timestamp = row.get("timestamp_utc", "")
            if timestamp > latest:
                latest = timestamp
    return latest[:10] if latest else None


def _append_rows(path: Path, rows: list[FlowRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row.as_csv_row())


def _write_watermark(path: Path, watermark: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(watermark, indent=2) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Scan pure SOL portfolio capital flows.")
    parser.add_argument("--dry-run", action="store_true", help="Print rows without writing.")
    parser.add_argument("--start-date", help="YYYY-MM-DD; used only before first watermark.")
    parser.add_argument("--flows-path", default=str(DEFAULT_FLOWS_PATH), help=argparse.SUPPRESS)
    parser.add_argument(
        "--watermark-path",
        default=str(DEFAULT_WATERMARK_PATH),
        help=argparse.SUPPRESS,
    )
    args = parser.parse_args(argv)

    _load_env_file()
    wallet = _resolve_wallet()
    rpc_url = _resolve_rpc_url()
    if not wallet:
        print("ERROR: set WALLET_ADDRESS in .env")
        return 1
    if not rpc_url:
        print("ERROR: set RPC_URL in .env")
        return 1

    flows_path = Path(args.flows_path)
    watermark_path = Path(args.watermark_path)
    start_date = args.start_date or _default_start_date(flows_path)

    scanner = SolFlowScanner(SolanaRpcClient(rpc_url), wallet)
    rows = scanner.scan_new(flows_path, watermark_path, start_date=start_date)

    print(f"{len(rows)} new flows found" + (", dry-run" if args.dry_run else ", appended"))
    for row in rows:
        print(",".join(row.as_csv_row()[field] for field in FIELDS))

    if args.dry_run:
        return 0

    if rows:
        _append_rows(flows_path, rows)
    if scanner.watermark_to_write:
        _write_watermark(watermark_path, scanner.watermark_to_write)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
