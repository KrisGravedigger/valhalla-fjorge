"""
Cross-checker that diffs lpagent positions against local positions.csv.

Identifies positions present in lpagent but missing from CSV,
converts them to MatchedPosition backfill rows, and prints a gap report.
"""

import csv
import logging
from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from .models import MatchedPosition

logger = logging.getLogger(__name__)


def _parse_lpagent_datetime(s: str) -> str:
    """
    Parse an ISO 8601 datetime string from lpagent and format it as YYYY-MM-DDTHH:MM.

    Input examples:
        "2026-03-31T14:23:45.000Z"
        "2026-03-31T14:23:45Z"
        "2026-03-31T14:23:45"

    Returns empty string if input is None or unparseable.
    """
    if not s:
        return ""
    # Strip trailing Z so fromisoformat can handle it
    cleaned = s.rstrip("Z")
    # Strip fractional seconds if present (fromisoformat on Python 3.6 doesn't handle them)
    if "." in cleaned:
        cleaned = cleaned.split(".")[0]
    try:
        dt = datetime.fromisoformat(cleaned)
        return dt.strftime("%Y-%m-%dT%H:%M")
    except (ValueError, TypeError) as e:
        logger.warning("Failed to parse lpagent datetime %r: %s", s, e)
        return ""


def _to_decimal(value, default: Optional[Decimal] = None) -> Optional[Decimal]:
    """Convert a value to Decimal, returning default if None/empty."""
    if value is None or value == "":
        return default
    try:
        return Decimal(str(value))
    except Exception as e:
        logger.warning("Failed to convert %r to Decimal: %s", value, e)
        return default


def _lpagent_to_position(raw: dict) -> MatchedPosition:
    """
    Convert a single lpagent API position dict to a MatchedPosition backfill row.

    All financial values use Decimal. Fields not available from lpagent are
    left as empty string / None / 0 per the MatchedPosition defaults.
    """
    token_id = raw.get("tokenId", "")
    pnl_block = raw.get("pnl") or {}
    token0 = raw.get("token0Info") or {}

    token_symbol = token0.get("token_symbol") or ""
    datetime_open = _parse_lpagent_datetime(raw.get("createdAt"))
    datetime_close = _parse_lpagent_datetime(raw.get("updatedAt"))

    sol_deployed = _to_decimal(raw.get("inputNative"))
    sol_received = _to_decimal(raw.get("outputNative"))
    pnl_sol = _to_decimal(raw.get("pnlNative"))
    pnl_pct = _to_decimal(pnl_block.get("percentNative"))
    meteora_fees = _to_decimal(raw.get("collectedFeeNative"))

    return MatchedPosition(
        target_wallet="lpagent_backfill",
        token=token_symbol,
        position_type="Spot",
        sol_deployed=sol_deployed,
        sol_received=sol_received,
        pnl_sol=pnl_sol,
        pnl_pct=pnl_pct,
        close_reason="normal",
        mc_at_open=0,
        jup_score=0,
        token_age="",
        token_age_days=None,
        token_age_hours=None,
        price_drop_pct=None,
        position_id=token_id[:8] if token_id else "",
        full_address=token_id,
        pnl_source="lpagent",
        meteora_deposited=sol_deployed,
        meteora_withdrawn=sol_received,
        meteora_fees=meteora_fees,
        meteora_pnl=pnl_sol,
        datetime_open=datetime_open,
        datetime_close=datetime_close,
        target_wallet_address=None,
        target_tx_signature=None,
        source_wallet_hold_min=None,
        source_wallet_pnl_pct=None,
        source_wallet_scenario=None,
        original_wallet="",
    )


class CrossChecker:
    """
    Compares lpagent positions against the local positions.csv.

    Usage:
        checker = CrossChecker("output/positions.csv")
        missing = checker.find_missing(lpagent_positions)
        checker.report(missing)
    """

    def __init__(self, positions_csv_path: str) -> None:
        self._csv_path = positions_csv_path

    def _load_existing_addresses(self) -> set:
        """
        Read positions.csv and return a set of non-empty full_address values.
        """
        addresses: set = set()
        try:
            with open(self._csv_path, "r", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    addr = row.get("full_address", "").strip()
                    if addr:
                        addresses.add(addr)
        except FileNotFoundError:
            logger.warning("positions.csv not found at %s — treating as empty", self._csv_path)
        return addresses

    def find_missing(self, lpagent_positions: List[dict]) -> List[MatchedPosition]:
        """
        Compare lpagent_positions against the local CSV.

        Returns a list of MatchedPosition objects for positions that are
        present in lpagent but absent from positions.csv (keyed on full_address = tokenId).

        Deduplicates lpagent response by tokenId (last occurrence wins)
        to handle positions that span date boundaries.
        """
        # Dedup by tokenId (last occurrence wins)
        deduped: dict = {pos["tokenId"]: pos for pos in lpagent_positions if pos.get("tokenId")}

        existing_addresses = self._load_existing_addresses()
        logger.info(
            "Cross-check: %d unique lpagent positions, %d existing CSV addresses",
            len(deduped),
            len(existing_addresses),
        )

        missing: List[MatchedPosition] = []
        for token_id, raw in deduped.items():
            if token_id not in existing_addresses:
                position = _lpagent_to_position(raw)
                missing.append(position)
                logger.debug("Missing position: %s", token_id[:8])

        return missing

    def report(self, missing: List[MatchedPosition]) -> None:
        """
        Print a human-readable gap report to stdout.

        Shows count of missing positions, total estimated PnL, and a
        per-position breakdown.
        """
        if not missing:
            print("  Cross-check: 0 missing positions — all clear.")
            return

        total_pnl = sum(
            (p.pnl_sol for p in missing if p.pnl_sol is not None),
            Decimal("0"),
        )
        sign = "+" if total_pnl >= 0 else ""

        print("  Cross-check results:")
        print(f"    - Missing from positions.csv: {len(missing)}")
        print(f"    - Estimated missing PnL: {sign}{total_pnl:.4f} SOL")
        print("    - Missing positions:")
        for pos in missing:
            pnl_val = pos.pnl_sol if pos.pnl_sol is not None else Decimal("0")
            pnl_sign = "+" if pnl_val >= 0 else ""
            print(
                f"        {pos.position_id} | {pos.token or '?'} | "
                f"{pos.datetime_open or '?'} | "
                f"{pnl_sign}{pnl_val:.4f} SOL"
            )
