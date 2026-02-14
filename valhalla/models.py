"""
Data models and utility functions for Valhalla parser.
"""

import re
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Optional, Tuple
from datetime import datetime


# Known Solana system programs to filter out
KNOWN_PROGRAMS = {
    "11111111111111111111111111111111",  # System Program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Token Program
    "ComputeBudget111111111111111111111111111111",  # Compute Budget
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",  # Associated Token
    "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",  # Metaplex
    "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",  # Meteora DLMM Program
    "SysvarRent111111111111111111111111111111111",  # Sysvar Rent
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",  # Token-2022
}

SOL_MINT = "So11111111111111111111111111111111111111112"


def short_id(addr: str) -> str:
    """Generate short ID from full address (first 4 + last 4 chars)"""
    return addr[:4] + addr[-4:]


def extract_date_from_filename(filename: str) -> Optional[str]:
    """
    Extract date from filename patterns like:
    - logs_20260212.txt -> 2026-02-12
    - logs_2026-02-12.txt -> 2026-02-12
    - sample_logs_3.txt -> None

    Returns YYYY-MM-DD string or None if not found.
    """
    # Pattern 1: YYYYMMDD
    match = re.search(r'(\d{4})(\d{2})(\d{2})', filename)
    if match:
        year, month, day = match.groups()
        try:
            # Validate date
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        except ValueError:
            pass

    # Pattern 2: YYYY-MM-DD
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', filename)
    if match:
        year, month, day = match.groups()
        try:
            # Validate date
            datetime(int(year), int(month), int(day))
            return f"{year}-{month}-{day}"
        except ValueError:
            pass

    return None


def make_iso_datetime(date_str: str, time_str: str) -> str:
    """
    Combine date and time into ISO 8601 format.

    Args:
        date_str: "YYYY-MM-DD" or empty string
        time_str: "[HH:MM]" format

    Returns:
        ISO datetime string: "2026-02-12T15:08:00" or "T15:08:00" if no date
    """
    # Extract HH:MM from [HH:MM]
    time_match = re.search(r'\[(\d{2}):(\d{2})\]', time_str)
    if not time_match:
        return ""

    hour, minute = time_match.groups()
    time_part = f"{hour}:{minute}:00"

    if date_str:
        return f"{date_str}T{time_part}"
    else:
        return f"T{time_part}"


def normalize_token_age(age_str: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Convert token age string to normalized values.

    Args:
        age_str: e.g. "5h ago", "3d ago", "2w ago", "3mo ago", "1yr ago"

    Returns:
        (age_days, age_hours) tuple. age_days is total days, age_hours is total hours.
        Returns (None, None) if unparseable.
    """
    if not age_str:
        return None, None

    match = re.match(r'(\d+)(h|d|w|mo|yr)\s*ago', age_str.strip())
    if not match:
        return None, None

    value = int(match.group(1))
    unit = match.group(2)

    if unit == 'h':
        return 0 if value < 24 else value // 24, value
    elif unit == 'd':
        return value, value * 24
    elif unit == 'w':
        return value * 7, value * 7 * 24
    elif unit == 'mo':
        return value * 30, value * 30 * 24
    elif unit == 'yr':
        return value * 365, value * 365 * 24

    return None, None


def parse_iso_datetime(dt_str: str) -> Optional[datetime]:
    """Parse ISO datetime string to datetime object. Returns None if invalid."""
    if not dt_str or 'T' not in dt_str:
        return None
    try:
        return datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
    except ValueError:
        return None


# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class OpenEvent:
    timestamp: str          # "[HH:MM]" from logs
    position_type: str      # "Spot" / "BidAsk"
    token_name: str         # "BadBunny"
    token_pair: str         # "BadBunny-SOL"
    target: str             # "20260125_C5JXfmK"
    market_cap: float       # 1047047.227
    token_age: str          # "5h ago"
    jup_score: int          # 81
    target_sol: float       # 2.0000
    your_sol: float         # 4.0000
    position_id: str        # "BUeWH73d"
    tx_signatures: List[str] = field(default_factory=list)
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class CloseEvent:
    timestamp: str
    target: str
    starting_sol: float
    starting_usd: float
    ending_sol: float
    ending_usd: float
    position_id: str        # "BUeWH73d"
    tx_signatures: List[str] = field(default_factory=list)
    total_sol: float = 0.0
    active_positions: int = 0
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class RugEvent:
    timestamp: str
    target: str
    token_pair: str
    position_address: str
    price_drop: float
    threshold: float
    position_id: Optional[str] = None
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class SkipEvent:
    timestamp: str
    target: str
    reason: str
    token_name: str
    token_address: str
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class FailsafeEvent:
    timestamp: str
    position_id: str
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class AddLiquidityEvent:
    timestamp: str
    position_id: str
    target: str
    amount_sol: float
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class SwapEvent:
    timestamp: str
    amount: str
    token_name: str
    token_address: str
    date: str = ""          # "YYYY-MM-DD" format


@dataclass
class MatchedPosition:
    """A matched open/close position with PnL calculation"""
    target_wallet: str
    token: str
    position_type: str
    sol_deployed: Optional[Decimal]
    sol_received: Optional[Decimal]
    pnl_sol: Optional[Decimal]
    pnl_pct: Optional[Decimal]
    close_reason: str
    mc_at_open: float
    jup_score: int
    token_age: str
    token_age_days: Optional[int] = None   # Normalized age in days
    token_age_hours: Optional[int] = None  # Normalized age in hours
    price_drop_pct: Optional[float] = None
    position_id: str = ""
    full_address: str = ""
    pnl_source: str = "pending"
    meteora_deposited: Optional[Decimal] = None
    meteora_withdrawn: Optional[Decimal] = None
    meteora_fees: Optional[Decimal] = None
    meteora_pnl: Optional[Decimal] = None
    datetime_open: str = ""     # ISO 8601: "2026-02-12T15:08:00" or "T15:08:00"
    datetime_close: str = ""    # ISO 8601: "2026-02-12T15:08:00" or "T15:08:00"


@dataclass
class MeteoraPnlResult:
    deposited_sol: Decimal      # SOL deposited (lamports -> SOL)
    withdrawn_sol: Decimal      # SOL withdrawn (lamports -> SOL)
    fees_sol: Decimal           # SOL fees claimed (lamports -> SOL)
    deposited_usd: Decimal      # Total USD deposited (both tokens)
    withdrawn_usd: Decimal      # Total USD withdrawn (both tokens)
    fees_usd: Decimal           # Total USD fees (both tokens)
    pnl_usd: Decimal            # PnL in USD (withdrawn + fees - deposited)
    pnl_sol: Decimal            # PnL converted to SOL via deposit-time SOL price
