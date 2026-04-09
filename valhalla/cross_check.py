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
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
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
        position_id=(token_id[:4] + token_id[-4:]) if len(token_id) >= 8 else token_id,
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
            print("  Cross-check: 0 missing positions - all clear.")
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
            token_display = (pos.token or "?").encode("ascii", errors="replace").decode("ascii")
            print(
                f"        {pos.position_id} | {token_display} | "
                f"{pos.datetime_open or '?'} | "
                f"{pnl_sign}{pnl_val:.4f} SOL"
            )

    def backfill(self, missing: List[MatchedPosition]) -> None:
        """
        Append missing positions to positions.csv as new rows.

        Reads the existing CSV header to determine the exact column order,
        then appends each MatchedPosition serialized with the same field
        formatting used by CsvWriter.generate_positions_csv().

        Prints the count of rows added.
        """
        if not missing:
            return

        # Read existing header to get exact column order
        fieldnames: List[str] = []
        existing_rows: List[dict] = []
        try:
            with open(self._csv_path, "r", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                fieldnames = reader.fieldnames or []
                existing_rows = list(reader)
        except FileNotFoundError:
            logger.warning(
                "positions.csv not found at %s — cannot backfill", self._csv_path
            )
            return

        if not fieldnames:
            logger.warning("positions.csv has no header — cannot backfill")
            return

        def _fmt_decimal(val: Optional[Decimal], places: int = 4) -> str:
            if val is None:
                return ""
            fmt = f"{{:.{places}f}}"
            return fmt.format(val)

        new_rows = []
        for pos in missing:
            row = {
                "datetime_open": pos.datetime_open or "",
                "datetime_close": pos.datetime_close or "",
                "target_wallet": pos.target_wallet or "",
                "token": pos.token or "",
                "position_type": pos.position_type or "",
                "sol_deployed": _fmt_decimal(pos.sol_deployed),
                "sol_received": _fmt_decimal(pos.sol_received),
                "pnl_sol": _fmt_decimal(pos.pnl_sol),
                "pnl_pct": _fmt_decimal(pos.pnl_pct, 2),
                "close_reason": pos.close_reason or "",
                "mc_at_open": f"{pos.mc_at_open:.2f}" if pos.mc_at_open is not None else "0.00",
                "jup_score": str(pos.jup_score) if pos.jup_score is not None else "0",
                "token_age": pos.token_age or "",
                "token_age_days": str(pos.token_age_days) if pos.token_age_days is not None else "",
                "token_age_hours": str(pos.token_age_hours) if pos.token_age_hours is not None else "",
                "price_drop_pct": f"{pos.price_drop_pct:.2f}" if pos.price_drop_pct is not None else "",
                "position_id": pos.position_id or "",
                "full_address": pos.full_address or "",
                "pnl_source": pos.pnl_source or "",
                "meteora_deposited": _fmt_decimal(pos.meteora_deposited),
                "meteora_withdrawn": _fmt_decimal(pos.meteora_withdrawn),
                "meteora_fees": _fmt_decimal(pos.meteora_fees),
                "meteora_pnl": _fmt_decimal(pos.meteora_pnl),
                "target_wallet_address": pos.target_wallet_address or "",
                "target_tx_signature": pos.target_tx_signature or "",
                "source_wallet_hold_min": str(pos.source_wallet_hold_min) if pos.source_wallet_hold_min is not None else "",
                "source_wallet_pnl_pct": f"{pos.source_wallet_pnl_pct:.2f}" if pos.source_wallet_pnl_pct is not None else "",
                "source_wallet_scenario": pos.source_wallet_scenario or "",
                "original_wallet": pos.original_wallet or "",
            }
            # Only keep keys present in the actual CSV header (handles schema evolution)
            new_rows.append({k: row.get(k, "") for k in fieldnames})

        # Append to the CSV (preserve existing rows, add new ones at end)
        with open(self._csv_path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(existing_rows)
            writer.writerows(new_rows)

        logger.info("Backfilled %d positions to %s", len(new_rows), self._csv_path)
        print(f"  Backfilled {len(new_rows)} positions to {self._csv_path}")
