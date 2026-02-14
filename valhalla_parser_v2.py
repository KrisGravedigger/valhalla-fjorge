#!/usr/bin/env python3
"""
Valhalla Bot Discord DM Log Parser v2
Parses Discord DM plain text logs and calculates per-position PnL using Meteora DLMM API.
"""

import re
import csv
import json
import time
import argparse
import urllib.request
import html
import shutil
from dataclasses import dataclass, field
from decimal import Decimal
from typing import List, Dict, Optional, Tuple
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timedelta

# Optional matplotlib for chart generation
try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


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
# Chart Generation
# ============================================================================

def _chart_cumulative_pnl(dated_positions: List[Tuple['MatchedPosition', datetime]], output_dir: str) -> None:
    """Generate cumulative PnL chart over time."""
    times = [dt for _, dt in dated_positions]
    cumulative = []
    running = Decimal('0')
    for pos, _ in dated_positions:
        running += pos.pnl_sol
        cumulative.append(float(running))

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(times, cumulative, linewidth=1.5, color='#2196F3')
    ax.axhline(y=0, color='gray', linewidth=0.5, linestyle='--')
    ax.fill_between(times, cumulative, 0,
                    where=[c >= 0 for c in cumulative], alpha=0.15, color='green')
    ax.fill_between(times, cumulative, 0,
                    where=[c < 0 for c in cumulative], alpha=0.15, color='red')
    ax.set_title('Cumulative PnL (SOL)')
    ax.set_ylabel('SOL')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(Path(output_dir) / 'pnl_cumulative.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: pnl_cumulative.png")


def _chart_win_rate_trend(dated_positions: List[Tuple['MatchedPosition', datetime]], output_dir: str) -> None:
    """Generate rolling win rate trend chart."""
    if len(dated_positions) < 2:
        return

    window = min(10, len(dated_positions))

    # Calculate rolling win rate
    position_numbers = []
    win_rates = []

    for i in range(len(dated_positions)):
        start_idx = max(0, i - window + 1)
        window_positions = dated_positions[start_idx:i + 1]

        wins = sum(1 for pos, _ in window_positions if pos.pnl_sol > 0)
        win_rate = (wins / len(window_positions)) * 100

        position_numbers.append(i + 1)
        win_rates.append(win_rate)

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(position_numbers, win_rates, linewidth=1.5, color='#FF9800', marker='o', markersize=3)
    ax.axhline(y=50, color='gray', linewidth=0.5, linestyle='--', label='50% threshold')
    ax.set_title(f'Win Rate Trend (Rolling {window} positions)')
    ax.set_xlabel('Position Number')
    ax.set_ylabel('Win Rate (%)')
    ax.set_ylim(0, 100)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(Path(output_dir) / 'win_rate_trend.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: win_rate_trend.png")


def _chart_pnl_by_strategy(dated_positions: List[Tuple['MatchedPosition', datetime]], output_dir: str) -> None:
    """Generate PnL by strategy chart (Spot vs BidAsk)."""
    # Separate by strategy
    spot_positions = [(p, dt) for p, dt in dated_positions if p.position_type == 'Spot']
    bidask_positions = [(p, dt) for p, dt in dated_positions if p.position_type == 'BidAsk']

    if not spot_positions and not bidask_positions:
        print("  No strategy data for PnL chart")
        return

    fig, ax = plt.subplots(figsize=(10, 5))

    # Plot Spot cumulative
    if spot_positions:
        spot_positions.sort(key=lambda x: x[1])
        times = [dt for _, dt in spot_positions]
        cumulative = []
        running = Decimal('0')
        for pos, _ in spot_positions:
            running += pos.pnl_sol
            cumulative.append(float(running))
        ax.plot(times, cumulative, linewidth=1.5, color='#4CAF50', label='Spot', marker='o', markersize=3)

    # Plot BidAsk cumulative
    if bidask_positions:
        bidask_positions.sort(key=lambda x: x[1])
        times = [dt for _, dt in bidask_positions]
        cumulative = []
        running = Decimal('0')
        for pos, _ in bidask_positions:
            running += pos.pnl_sol
            cumulative.append(float(running))
        ax.plot(times, cumulative, linewidth=1.5, color='#9C27B0', label='BidAsk', marker='s', markersize=3)

    ax.axhline(y=0, color='gray', linewidth=0.5, linestyle='--')
    ax.set_title('Cumulative PnL by Strategy')
    ax.set_ylabel('SOL')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(Path(output_dir) / 'pnl_by_strategy.png', dpi=120)
    plt.close(fig)
    print(f"  Generated: pnl_by_strategy.png")


def generate_charts(positions: List['MatchedPosition'], output_dir: str) -> None:
    """Generate PNG chart files from position data."""
    if not HAS_MATPLOTLIB:
        print("  matplotlib not installed, skipping charts")
        return

    # Filter positions with valid datetime_close
    dated = [(p, parse_iso_datetime(p.datetime_close)) for p in positions]
    dated = [(p, dt) for p, dt in dated if dt is not None]

    if len(dated) < 2:
        print("  Not enough dated positions for charts (need 2+)")
        return

    # Sort by close time
    dated.sort(key=lambda x: x[1])

    _chart_cumulative_pnl(dated, output_dir)
    _chart_win_rate_trend(dated, output_dir)
    _chart_pnl_by_strategy(dated, output_dir)


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
    timestamp_open: str
    timestamp_close: str
    target_wallet: str
    token: str
    position_type: str
    sol_deployed: Decimal
    sol_received: Decimal
    pnl_sol: Decimal
    pnl_pct: Decimal
    close_reason: str
    mc_at_open: float
    jup_score: int
    token_age: str
    token_age_days: Optional[int] = None   # Normalized age in days
    token_age_hours: Optional[int] = None  # Normalized age in hours
    price_drop_pct: Optional[float] = None
    position_id: str = ""
    full_address: str = ""
    pnl_source: str = "discord"
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


# ============================================================================
# PlainTextReader
# ============================================================================

class PlainTextReader:
    """Parse plain text Discord DM logs with [HH:MM] Author: format"""

    # Pattern to split messages: [HH:MM] at start of line
    MESSAGE_SPLIT = re.compile(r'^(?=\[\d{2}:\d{2}\])', flags=re.MULTILINE)
    # Pattern to extract author from first line
    AUTHOR_PATTERN = re.compile(r'^\[(\d{2}:\d{2})\]\s*(.+?):\s*\n', flags=re.MULTILINE)
    # Solscan TX signature from [https://solscan.io/tx/SIG]
    SOLSCAN_TX_PATTERN = re.compile(r'\[https://solscan\.io/tx/([A-Za-z0-9]+)\]')
    # Any URL in square brackets (for stripping)
    URL_BRACKET_PATTERN = re.compile(r'\[https?://[^\]]+\]')

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.header_date: Optional[str] = None

    def read(self) -> List[Tuple[str, str, List[str]]]:
        """Returns list of (timestamp, clean_text, tx_signatures) tuples"""
        with open(self.file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Check for date header (YYYYMMDD) at the top of the file
        lines = content.split('\n')
        if lines and lines[0].strip():
            first_line = lines[0].strip()
            # Match 8 digits (YYYYMMDD)
            if re.match(r'^\d{8}$', first_line):
                # Validate it's a real date
                try:
                    year = int(first_line[0:4])
                    month = int(first_line[4:6])
                    day = int(first_line[6:8])
                    datetime(year, month, day)
                    # Valid date found
                    self.header_date = f"{year:04d}-{month:02d}-{day:02d}"
                    # Strip the date line from content
                    content = '\n'.join(lines[1:])
                except ValueError:
                    # Not a valid date, continue with full content
                    pass

        raw_messages = self.MESSAGE_SPLIT.split(content)
        results = []

        for raw_msg in raw_messages:
            if not raw_msg.strip():
                continue

            # Extract author from first line
            author_match = self.AUTHOR_PATTERN.match(raw_msg)
            if not author_match:
                continue

            timestamp_str = author_match.group(1)  # "15:08"
            author = author_match.group(2)  # "APL. Valhalla Bot"

            # Filter: only Valhalla messages
            if 'valhalla' not in author.lower():
                continue

            timestamp = f"[{timestamp_str}]"

            # Extract Solscan signatures before stripping URLs
            tx_signatures = self.SOLSCAN_TX_PATTERN.findall(raw_msg)

            # Strip URLs in brackets and the author line prefix
            text = raw_msg[author_match.end():]  # Remove the [HH:MM] Author: line
            clean_text = self.URL_BRACKET_PATTERN.sub('', text)

            results.append((timestamp, clean_text, tx_signatures))

        return results


# ============================================================================
# HtmlReader
# ============================================================================

class HtmlReader(PlainTextReader):
    """Parse HTML Discord DM logs (from browser clipboard) with [HH:MM] Author: format"""

    def html_to_text(self, raw_html: str) -> str:
        """Convert HTML to clean text, extracting links as TEXT [URL]"""
        content = raw_html

        # Extract CF_HTML fragment if present
        start = content.find('<!--StartFragment-->')
        end = content.find('<!--EndFragment-->')
        if start >= 0 and end > start:
            content = content[start + len('<!--StartFragment-->'):end]

        # Replace links: <a href="URL">TEXT</a> → TEXT [URL]
        def replace_link(m):
            url = m.group(1)
            text = re.sub(r'<[^>]+>', '', m.group(2))  # strip tags inside anchor
            text = re.sub(r'\s+', ' ', text).strip()
            if text:
                return f'{text} [{url}]'
            else:
                return f'[{url}]'

        content = re.sub(
            r'<a\b[^>]*href\s*=\s*["\']([^"\']+)["\'][^>]*>(.*?)</a>',
            replace_link, content, flags=re.IGNORECASE | re.DOTALL
        )

        # HTML decode
        content = html.unescape(content)

        # Block elements → newlines
        content = re.sub(r'(?i)<\s*br\s*/?\s*>', '\n', content)
        content = re.sub(r'(?i)</\s*(div|p|li|tr|h[1-6])\s*>', '\n', content)

        # Strip remaining tags
        content = re.sub(r'<[^>]+>', '', content)

        # Clean whitespace
        content = re.sub(r'\n[ \t]+', '\n', content)
        content = re.sub(r'[ \t]{2,}', ' ', content)
        content = re.sub(r'\n{3,}', '\n\n', content)

        return content.strip()

    def read(self) -> List[Tuple[str, str, List[str]]]:
        """Convert HTML to text, then use PlainTextReader logic"""
        with open(self.file_path, 'r', encoding='utf-8', errors='ignore') as f:
            raw_html = f.read()

        # Check for date header before HTML content
        lines = raw_html.split('\n')
        if lines and lines[0].strip():
            first_line = lines[0].strip()
            # Match 8 digits (YYYYMMDD) before any HTML tags
            if re.match(r'^\d{8}$', first_line):
                # Validate it's a real date
                try:
                    year = int(first_line[0:4])
                    month = int(first_line[4:6])
                    day = int(first_line[6:8])
                    datetime(year, month, day)
                    # Valid date found
                    self.header_date = f"{year:04d}-{month:02d}-{day:02d}"
                    # Strip the date line from content
                    raw_html = '\n'.join(lines[1:])
                except ValueError:
                    # Not a valid date, continue with full content
                    pass

        # Convert HTML to plain text
        plain_text = self.html_to_text(raw_html)

        # Use PlainTextReader's message splitting logic
        raw_messages = self.MESSAGE_SPLIT.split(plain_text)
        results = []

        for raw_msg in raw_messages:
            if not raw_msg.strip():
                continue

            # Extract author from first line
            author_match = self.AUTHOR_PATTERN.match(raw_msg)
            if not author_match:
                continue

            timestamp_str = author_match.group(1)  # "15:08"
            author = author_match.group(2)  # "APL. Valhalla Bot"

            # Filter: only Valhalla messages
            if 'valhalla' not in author.lower():
                continue

            timestamp = f"[{timestamp_str}]"

            # Extract Solscan signatures before stripping URLs
            tx_signatures = self.SOLSCAN_TX_PATTERN.findall(raw_msg)

            # Strip URLs in brackets and the author line prefix
            text = raw_msg[author_match.end():]  # Remove the [HH:MM] Author: line
            clean_text = self.URL_BRACKET_PATTERN.sub('', text)

            results.append((timestamp, clean_text, tx_signatures))

        return results


def detect_input_format(file_path: str) -> str:
    """Auto-detect if file is HTML or plain text"""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_4k = f.read(4096)

    # Check for HTML markers
    if re.search(r'<html|<!DOCTYPE|<!--StartFragment|<div|<span|<a\s+href', first_4k, re.IGNORECASE):
        return 'html'
    return 'text'


# ============================================================================
# EventParser
# ============================================================================

class EventParser:
    """Parse events from Discord message text"""

    # Regex patterns (from v1)
    TIMESTAMP_PATTERN = r'\[(\d{2}:\d{2})\]'
    TARGET_PATTERN = r'Target:\s*(\S+)'
    POSITION_TYPE_PATTERN = r'(Spot|BidAsk)\s+1-Sided Position\s*\|\s*(\S+)-SOL'
    MARKET_CAP_PATTERN = r'MC:\s*\$([\d,]+\.?\d*)'
    TOKEN_AGE_PATTERN = r'Age:\s*(.+?)(?:\n|$)'
    JUP_SCORE_PATTERN = r'Jup Score:\s*(\d+)'
    YOUR_POS_PATTERN = r'Your Pos:.*?SOL:\s*([\d.]+)'
    TARGET_POS_PATTERN = r'Target Pos:.*?SOL:\s*([\d.]+)'

    # Position ID patterns
    OPEN_POSITION_ID_PATTERN = r'Opened New DLMM Position!\s*\((\w+)\)'
    CLOSE_POSITION_ID_PATTERN = r'Closed DLMM Position!\s*\((\w+)\)'
    FAILSAFE_POSITION_ID_PATTERN = r'Failsafe Activated \(DLMM\)\s*\((\w+)\)'
    ADD_LIQUIDITY_POSITION_ID_PATTERN = r'Added DLMM Liquidity\s*\((\w+)\)'
    LIQUIDITY_AMOUNT_PATTERN = r'Amount:\s*([\d.]+)\s*SOL'

    # Close event patterns
    STARTING_SOL_PATTERN = r'Starting SOL balance:\s*([\d.]+)\s*SOL\s*\(\$([\d,.]+)\s*USD\)'
    ENDING_SOL_PATTERN = r'Ending SOL balance:\s*([\d.]+)\s*SOL\s*\(\$([\d,.]+)\s*USD\)'
    TOTAL_SOL_PATTERN = r'Total SOL balance:\s*([\d.]+)\s*SOL.*?\((\d+)\s*Active'

    # Rug event patterns
    RUG_TARGET_PATTERN = r'Copied From:\s*(\S+)\)'
    RUG_POSITION_ID_PATTERN = r'Rug Check Stop Loss Executed\s*\(DLMM\)\s*\((\w+)\)'
    PRICE_DROP_PATTERN = r'Price Drop:\s*([\d.]+)%'
    RUG_THRESHOLD_PATTERN = r'Rug Check Threshold:\s*([\d.]+)%'
    POSITION_ADDRESS_PATTERN = r'Position:\s*(\S+)'
    PAIR_PATTERN = r'Pair:\s*(\S+)'

    # Skip event markers
    SKIP_REASON_AGE_MARKER = 'Skipping position due to token age restriction'
    SKIP_REASON_JUP_MARKER = 'Skipping position due to low Jupiter organic score restriction'
    SKIP_REASON_SOL_ONLY_MARKER = 'Skipping position due to SOL-only deposit restriction'
    SKIP_TOKEN_PATTERN = r'Token\s+([^:]+?):\s*(\S+)'

    # Swap pattern
    SWAP_PATTERN = r'Swapped\s+([\d,]+|all)\s+(.+?)\s+\((\S+)\)'

    def __init__(self, base_date: Optional[str] = None):
        """
        Initialize EventParser.

        Args:
            base_date: Starting date in YYYY-MM-DD format, or None
        """
        self.open_events: List[OpenEvent] = []
        self.close_events: List[CloseEvent] = []
        self.rug_events: List[RugEvent] = []
        self.skip_events: List[SkipEvent] = []
        self.swap_events: List[SwapEvent] = []
        self.failsafe_events: List[FailsafeEvent] = []
        self.add_liquidity_events: List[AddLiquidityEvent] = []
        self.base_date = base_date
        self.current_date = base_date

    def parse_messages(self, messages: List[Tuple[str, str, List[str]]]) -> None:
        """Parse all messages from PlainTextReader with midnight rollover detection"""
        prev_hour = None

        for timestamp, text, tx_signatures in messages:
            # Strip non-ASCII characters (emoji) before regex matching
            clean_text = re.sub(r'[^\x00-\x7F]+', '', text)

            # Detect midnight rollover if we have a base date
            if self.base_date:
                time_match = re.search(r'\[(\d{2}):(\d{2})\]', timestamp)
                if time_match:
                    hour = int(time_match.group(1))

                    # If time decreases (e.g., 23:50 -> 00:10), we crossed midnight
                    if prev_hour is not None and hour < prev_hour:
                        # Increment the current date by 1 day
                        current_dt = datetime.strptime(self.current_date, "%Y-%m-%d")
                        next_dt = current_dt + timedelta(days=1)
                        self.current_date = next_dt.strftime("%Y-%m-%d")
                        print(f"  Midnight rollover detected: now using {self.current_date}")

                    prev_hour = hour

            self._classify_and_parse_message(timestamp, clean_text, tx_signatures)

    def _classify_and_parse_message(self, timestamp: str, message: str, tx_signatures: List[str]) -> None:
        """Classify message type and parse accordingly"""
        # Skip "already closed" messages
        if "was already closed" in message:
            return

        # Check for each event type
        if "Opened New DLMM Position!" in message:
            event = self._parse_open_event(timestamp, message, tx_signatures)
            if event:
                event.date = self.current_date or ""
                self.open_events.append(event)

        elif "Closed DLMM Position!" in message:
            event = self._parse_close_event(timestamp, message, tx_signatures)
            if event:
                event.date = self.current_date or ""
                self.close_events.append(event)

        elif "Failsafe Activated (DLMM)" in message:
            event = self._parse_failsafe_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.failsafe_events.append(event)

        elif "Added DLMM Liquidity" in message:
            event = self._parse_add_liquidity_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.add_liquidity_events.append(event)

        elif "Rug Check Stop Loss Executed" in message:
            event = self._parse_rug_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.rug_events.append(event)

        elif "Skipping position due to" in message:
            event = self._parse_skip_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.skip_events.append(event)

        elif "Swapped" in message:
            event = self._parse_swap_event(timestamp, message)
            if event:
                event.date = self.current_date or ""
                self.swap_events.append(event)

    def _parse_open_event(self, timestamp: str, message: str, tx_signatures: List[str]) -> Optional[OpenEvent]:
        """Parse an open position event"""
        try:
            target_match = re.search(self.TARGET_PATTERN, message)
            position_type_match = re.search(self.POSITION_TYPE_PATTERN, message)
            mc_match = re.search(self.MARKET_CAP_PATTERN, message)
            age_match = re.search(self.TOKEN_AGE_PATTERN, message)
            jup_match = re.search(self.JUP_SCORE_PATTERN, message)
            your_sol_match = re.search(self.YOUR_POS_PATTERN, message)
            target_sol_match = re.search(self.TARGET_POS_PATTERN, message)
            position_id_match = re.search(self.OPEN_POSITION_ID_PATTERN, message)

            if not all([target_match, position_type_match, mc_match, age_match,
                       jup_match, your_sol_match, target_sol_match, position_id_match]):
                return None

            position_type = position_type_match.group(1)
            token_name = position_type_match.group(2)
            token_pair = f"{token_name}-SOL"
            target = target_match.group(1)
            market_cap = float(mc_match.group(1).replace(',', ''))
            token_age = age_match.group(1).strip()
            jup_score = int(jup_match.group(1))
            target_sol = float(target_sol_match.group(1))
            your_sol = float(your_sol_match.group(1))
            position_id = position_id_match.group(1)

            return OpenEvent(
                timestamp=timestamp,
                position_type=position_type,
                token_name=token_name,
                token_pair=token_pair,
                target=target,
                market_cap=market_cap,
                token_age=token_age,
                jup_score=jup_score,
                target_sol=target_sol,
                your_sol=your_sol,
                position_id=position_id,
                tx_signatures=tx_signatures
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse open event: {e}")
            return None

    def _parse_close_event(self, timestamp: str, message: str, tx_signatures: List[str]) -> Optional[CloseEvent]:
        """Parse a close position event"""
        try:
            target_match = re.search(self.TARGET_PATTERN, message)
            starting_match = re.search(self.STARTING_SOL_PATTERN, message)
            ending_match = re.search(self.ENDING_SOL_PATTERN, message)
            total_match = re.search(self.TOTAL_SOL_PATTERN, message)
            position_id_match = re.search(self.CLOSE_POSITION_ID_PATTERN, message)

            # total_match is optional (some close events don't have it)
            if not all([target_match, starting_match, ending_match, position_id_match]):
                return None

            target = target_match.group(1)
            starting_sol = float(starting_match.group(1))
            starting_usd = float(starting_match.group(2).replace(',', ''))
            ending_sol = float(ending_match.group(1))
            ending_usd = float(ending_match.group(2).replace(',', ''))
            total_sol = float(total_match.group(1)) if total_match else 0.0
            active_positions = int(total_match.group(2)) if total_match else 0
            position_id = position_id_match.group(1)

            return CloseEvent(
                timestamp=timestamp,
                target=target,
                starting_sol=starting_sol,
                starting_usd=starting_usd,
                ending_sol=ending_sol,
                ending_usd=ending_usd,
                position_id=position_id,
                tx_signatures=tx_signatures,
                total_sol=total_sol,
                active_positions=active_positions
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse close event: {e}")
            return None

    def _parse_failsafe_event(self, timestamp: str, message: str) -> Optional[FailsafeEvent]:
        """Parse a failsafe activation event"""
        try:
            position_id_match = re.search(self.FAILSAFE_POSITION_ID_PATTERN, message)

            if not position_id_match:
                return None

            position_id = position_id_match.group(1)

            return FailsafeEvent(
                timestamp=timestamp,
                position_id=position_id
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse failsafe event: {e}")
            return None

    def _parse_add_liquidity_event(self, timestamp: str, message: str) -> Optional[AddLiquidityEvent]:
        """Parse an add liquidity event"""
        try:
            position_id_match = re.search(self.ADD_LIQUIDITY_POSITION_ID_PATTERN, message)
            target_match = re.search(self.TARGET_PATTERN, message)
            amount_match = re.search(self.LIQUIDITY_AMOUNT_PATTERN, message)

            if not all([position_id_match, target_match, amount_match]):
                return None

            position_id = position_id_match.group(1)
            target = target_match.group(1)
            amount_sol = float(amount_match.group(1))

            return AddLiquidityEvent(
                timestamp=timestamp,
                position_id=position_id,
                target=target,
                amount_sol=amount_sol
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse add liquidity event: {e}")
            return None

    def _parse_rug_event(self, timestamp: str, message: str) -> Optional[RugEvent]:
        """Parse a rug pull event"""
        try:
            target_match = re.search(self.RUG_TARGET_PATTERN, message)
            pair_match = re.search(self.PAIR_PATTERN, message)
            position_match = re.search(self.POSITION_ADDRESS_PATTERN, message)
            drop_match = re.search(self.PRICE_DROP_PATTERN, message)
            threshold_match = re.search(self.RUG_THRESHOLD_PATTERN, message)
            position_id_match = re.search(self.RUG_POSITION_ID_PATTERN, message)

            if not all([target_match, pair_match, position_match, drop_match, threshold_match]):
                return None

            target = target_match.group(1)
            token_pair = pair_match.group(1)
            position_address = position_match.group(1)
            price_drop = float(drop_match.group(1))
            threshold = float(threshold_match.group(1))
            position_id = position_id_match.group(1) if position_id_match else None

            # Sanity check: verify position_id matches short_id of position_address
            if position_id and position_address:
                expected_short_id = short_id(position_address)
                if position_id != expected_short_id:
                    print(f"Warning: Rug event position_id mismatch: header={position_id}, address short_id={expected_short_id}")

            return RugEvent(
                timestamp=timestamp,
                target=target,
                token_pair=token_pair,
                position_address=position_address,
                price_drop=price_drop,
                threshold=threshold,
                position_id=position_id
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse rug event: {e}")
            return None

    def _parse_skip_event(self, timestamp: str, message: str) -> Optional[SkipEvent]:
        """Parse a skip event"""
        try:
            target_match = re.search(self.TARGET_PATTERN, message)

            if not target_match:
                return None

            target = target_match.group(1)

            # Determine reason
            if self.SKIP_REASON_AGE_MARKER in message:
                reason = "token age restriction"
            elif self.SKIP_REASON_JUP_MARKER in message:
                reason = "low Jupiter organic score"
            elif self.SKIP_REASON_SOL_ONLY_MARKER in message:
                reason = "SOL-only deposit restriction"
            else:
                reason = "unknown"

            # Extract token info - first token line is the actual token
            token_matches = re.findall(self.SKIP_TOKEN_PATTERN, message)
            if token_matches:
                token_name = token_matches[0][0].strip()
                token_address = token_matches[0][1].strip()
            else:
                token_name = "unknown"
                token_address = "unknown"

            return SkipEvent(
                timestamp=timestamp,
                target=target,
                reason=reason,
                token_name=token_name,
                token_address=token_address
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse skip event: {e}")
            return None

    def _parse_swap_event(self, timestamp: str, message: str) -> Optional[SwapEvent]:
        """Parse a swap event"""
        try:
            swap_match = re.search(self.SWAP_PATTERN, message)

            if not swap_match:
                return None

            amount = swap_match.group(1)
            token_name = swap_match.group(2)
            token_address = swap_match.group(3)

            return SwapEvent(
                timestamp=timestamp,
                amount=amount,
                token_name=token_name,
                token_address=token_address
            )
        except (ValueError, AttributeError) as e:
            print(f"Warning: Failed to parse swap event: {e}")
            return None


# ============================================================================
# AddressCache
# ============================================================================

class AddressCache:
    """JSON file persistence for short_id -> full_address mapping"""

    def __init__(self, cache_file: str):
        self.cache_file = cache_file
        self.cache: Dict[str, str] = {}
        self.load()

    def load(self) -> None:
        """Load cache from JSON file"""
        if Path(self.cache_file).exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load cache: {e}")
                self.cache = {}

    def save(self) -> None:
        """Save cache to JSON file"""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f, indent=2)
        except Exception as e:
            print(f"Warning: Failed to save cache: {e}")

    def get(self, short_id_val: str) -> Optional[str]:
        """Get full address from short ID"""
        return self.cache.get(short_id_val)

    def set(self, short_id_val: str, full_address: str) -> None:
        """Set short ID -> full address mapping"""
        self.cache[short_id_val] = full_address


# ============================================================================
# SolanaRpcClient
# ============================================================================

class SolanaRpcClient:
    """Solana JSON-RPC client using urllib"""

    def __init__(self, rpc_url: str):
        self.rpc_url = rpc_url
        self._delay = 0.7  # seconds between requests, increases on rate limit

    def get_transaction(self, signature: str) -> Optional[List[str]]:
        """
        Get transaction and extract account keys.
        Returns list of account key strings.
        """
        for attempt in range(5):
            try:
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        signature,
                        {
                            "encoding": "jsonParsed",
                            "maxSupportedTransactionVersion": 0
                        }
                    ]
                }

                req = urllib.request.Request(
                    self.rpc_url,
                    data=json.dumps(payload).encode('utf-8'),
                    headers={
                        "Content-Type": "application/json",
                        "User-Agent": "Mozilla/5.0",
                    }
                )

                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read())

                result = data.get('result')
                if not result:
                    return None

                # Extract account keys
                tx = result.get('transaction', {})
                message = tx.get('message', {})
                account_keys = message.get('accountKeys', [])

                # Account keys can be strings or objects with "pubkey" field
                keys = []
                for key in account_keys:
                    if isinstance(key, str):
                        keys.append(key)
                    elif isinstance(key, dict) and 'pubkey' in key:
                        keys.append(key['pubkey'])

                # Rate limiting - adaptive delay
                time.sleep(self._delay)
                return keys

            except urllib.error.HTTPError as e:
                if e.code == 429:
                    sleep_time = 5 * (2 ** attempt)  # 5, 10, 20, 40, 80s
                    print(f"  Rate limited, waiting {sleep_time}s...", end='', flush=True)
                    time.sleep(sleep_time)
                    # Increase base delay after rate limit hit
                    self._delay = min(self._delay * 2, 5.0)
                else:
                    print(f"  HTTP error {e.code}: {e.reason}")
                    return None

            except Exception as e:
                print(f"  RPC error: {e}")
                if attempt < 4:
                    time.sleep(5 * (2 ** attempt))
                else:
                    return None

        return None


# ============================================================================
# PositionResolver
# ============================================================================

class PositionResolver:
    """Resolve position short IDs to full addresses using RPC"""

    def __init__(self, cache: AddressCache, rpc_client: SolanaRpcClient):
        self.cache = cache
        self.rpc_client = rpc_client

    def resolve(self, position_id: str, tx_signatures: List[str]) -> Optional[str]:
        """
        Resolve position_id to full address.
        Returns full address or None if not found.
        """
        # Check cache first
        cached = self.cache.get(position_id)
        if cached:
            return cached

        # Try each transaction signature
        for sig in tx_signatures:
            account_keys = self.rpc_client.get_transaction(sig)
            if not account_keys:
                continue

            # Filter out known programs and short addresses
            candidates = [
                key for key in account_keys
                if key not in KNOWN_PROGRAMS and len(key) >= 32
            ]

            # Check each candidate
            for candidate in candidates:
                if short_id(candidate) == position_id:
                    # Found match
                    self.cache.set(position_id, candidate)
                    return candidate

        return None


# ============================================================================
# MeteoraPnlCalculator
# ============================================================================

class MeteoraPnlCalculator:
    """Calculate PnL using Meteora DLMM API"""

    def __init__(self):
        self.base_url = "https://dlmm-api.meteora.ag"
        self._pair_cache: Dict[str, Tuple[bool, bool]] = {}  # pair_addr -> (sol_is_x, sol_is_y)

    def _get_sol_side(self, pair_address: str) -> Optional[Tuple[bool, bool]]:
        """Determine which token (x or y) is SOL for a pair. Returns (sol_is_x, sol_is_y)."""
        if pair_address in self._pair_cache:
            return self._pair_cache[pair_address]

        pair_info = self._meteora_get(f"/pair/{pair_address}")
        if not pair_info:
            return None

        mint_x = pair_info.get('mint_x', '')
        mint_y = pair_info.get('mint_y', '')
        sol_is_x = (mint_x == SOL_MINT)
        sol_is_y = (mint_y == SOL_MINT)

        if not (sol_is_x or sol_is_y):
            return None

        self._pair_cache[pair_address] = (sol_is_x, sol_is_y)
        return (sol_is_x, sol_is_y)

    def calculate_pnl(self, address: str) -> Optional[MeteoraPnlResult]:
        """
        Calculate PnL for a position address.
        Returns MeteoraPnlResult or None if failed.
        """
        try:
            # Get position info to find pair_address
            pos_info = self._meteora_get(f"/position/{address}")
            if not pos_info:
                return None

            pair_address = pos_info.get('pair_address', '')
            if not pair_address:
                print(f"  Warning: No pair_address for {short_id(address)}")
                return None

            time.sleep(0.3)

            # Get mint info from pair to determine which token is SOL
            sol_side = self._get_sol_side(pair_address)
            if not sol_side:
                print(f"  Warning: No SOL token found in pair for {short_id(address)}")
                return None

            sol_is_x, sol_is_y = sol_side

            time.sleep(0.3)

            LAMPORTS = Decimal('1000000000')

            # Helper: compute SOL equivalent for a transaction entry.
            # Converts the non-SOL token to SOL using the per-transaction SOL price
            # derived from the SOL-side USD amount.
            def _tx_sol_equiv(entry, fallback_sol_price: Decimal) -> tuple[Decimal, Decimal, Decimal]:
                """Returns (sol_amount, sol_equiv_total, sol_price_used)"""
                sol_key = 'token_x_amount' if sol_is_x else 'token_y_amount'
                tok_key = 'token_y_amount' if sol_is_x else 'token_x_amount'
                sol_usd_key = 'token_x_usd_amount' if sol_is_x else 'token_y_usd_amount'
                tok_usd_key = 'token_y_usd_amount' if sol_is_x else 'token_x_usd_amount'

                sol_raw = Decimal(str(entry.get(sol_key, entry.get(sol_key.replace('_', 'X_' if sol_is_x else 'Y_'), 0))))
                sol_amt = sol_raw / LAMPORTS
                sol_usd = Decimal(str(entry.get(sol_usd_key, 0)))
                tok_usd = Decimal(str(entry.get(tok_usd_key, 0)))

                # Derive per-tx SOL price from SOL portion
                if sol_amt > 0 and sol_usd > 0:
                    sol_price = sol_usd / sol_amt
                else:
                    sol_price = fallback_sol_price

                # Convert token side to SOL at this tx's SOL price
                if tok_usd > 0 and sol_price > 0:
                    token_sol_equiv = tok_usd / sol_price
                else:
                    token_sol_equiv = Decimal('0')

                total_sol_equiv = sol_amt + token_sol_equiv
                total_usd = sol_usd + tok_usd
                return sol_amt, total_sol_equiv, total_usd, sol_price

            # Track a running SOL price for fallback (for txs with no SOL portion)
            running_sol_price = Decimal('0')

            # Get deposits - None means API error, [] means no deposits
            deposits = self._meteora_get(f"/position/{address}/deposits")
            if deposits is None:
                print(f"  API error on deposits for {short_id(address)}")
                return None
            dep_sol_total = Decimal('0')
            dep_sol_equiv = Decimal('0')
            dep_usd = Decimal('0')
            for dep in deposits:
                sol_amt, equiv, usd, price = _tx_sol_equiv(dep, running_sol_price)
                dep_sol_total += sol_amt
                dep_sol_equiv += equiv
                dep_usd += usd
                if price > 0:
                    running_sol_price = price

            time.sleep(0.3)

            # Get withdrawals - None means API error, [] means no withdrawals
            withdraws = self._meteora_get(f"/position/{address}/withdraws")
            if withdraws is None:
                print(f"  API error on withdraws for {short_id(address)}")
                return None
            wdr_sol_total = Decimal('0')
            wdr_sol_equiv = Decimal('0')
            wdr_usd = Decimal('0')
            for w in withdraws:
                sol_amt, equiv, usd, price = _tx_sol_equiv(w, running_sol_price)
                wdr_sol_total += sol_amt
                wdr_sol_equiv += equiv
                wdr_usd += usd
                if price > 0:
                    running_sol_price = price

            time.sleep(0.3)

            # Get claimed fees - None means API error, [] means no fees
            fees_list = self._meteora_get(f"/position/{address}/claim_fees")
            if fees_list is None:
                print(f"  API error on claim_fees for {short_id(address)}")
                return None
            fee_sol_total = Decimal('0')
            fee_sol_equiv = Decimal('0')
            fee_usd = Decimal('0')
            for f_entry in fees_list:
                sol_amt, equiv, usd, price = _tx_sol_equiv(f_entry, running_sol_price)
                fee_sol_total += sol_amt
                fee_sol_equiv += equiv
                fee_usd += usd
                if price > 0:
                    running_sol_price = price

            # PnL via per-transaction SOL equivalents
            pnl_sol = wdr_sol_equiv + fee_sol_equiv - dep_sol_equiv
            pnl_usd = wdr_usd + fee_usd - dep_usd

            return MeteoraPnlResult(
                deposited_sol=dep_sol_equiv,
                withdrawn_sol=wdr_sol_equiv,
                fees_sol=fee_sol_equiv,
                deposited_usd=dep_usd,
                withdrawn_usd=wdr_usd,
                fees_usd=fee_usd,
                pnl_usd=pnl_usd,
                pnl_sol=pnl_sol
            )

        except Exception as e:
            print(f"  Meteora API error for {short_id(address)}: {e}")
            return None

    def _meteora_get(self, path: str):
        """Make GET request to Meteora API"""
        try:
            url = f"{self.base_url}{path}"
            req = urllib.request.Request(url, headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            })

            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())

        except Exception as e:
            print(f"  Meteora GET error ({path}): {e}")
            return None


# ============================================================================
# PositionMatcher
# ============================================================================

class PositionMatcher:
    """Match open/close events and enrich with Meteora PnL"""

    def __init__(self, parser: EventParser):
        self.parser = parser

    def match_positions(self, meteora_results: Dict[str, MeteoraPnlResult],
                       resolved_addresses: Dict[str, str]) -> Tuple[List[MatchedPosition], List[OpenEvent]]:
        """
        Match open events with close/rug events using position_id lookup.
        Returns (matched_positions, unmatched_opens)
        """
        matched_positions: List[MatchedPosition] = []

        # Index opens by position_id
        open_by_id: Dict[str, OpenEvent] = {}
        for event in self.parser.open_events:
            open_by_id[event.position_id] = event

        # Index failsafe events by position_id
        failsafe_ids = {e.position_id for e in self.parser.failsafe_events}

        # Index add_liquidity events by position_id
        liquidity_by_id: Dict[str, List[AddLiquidityEvent]] = defaultdict(list)
        for event in self.parser.add_liquidity_events:
            liquidity_by_id[event.position_id].append(event)

        # Match closes to opens by position_id
        matched_ids = set()

        for close_event in self.parser.close_events:
            pid = close_event.position_id
            if pid in matched_ids:
                continue  # Skip duplicate close events (e.g., from overlapping log files)
            matched_ids.add(pid)

            if pid in open_by_id:
                open_event = open_by_id[pid]
                sol_deployed = Decimal(str(open_event.your_sol))

                # Add any extra liquidity
                for liq in liquidity_by_id.get(pid, []):
                    sol_deployed += Decimal(str(liq.amount_sol))

                # Discord PnL (fallback)
                sol_received = Decimal(str(close_event.ending_sol)) - Decimal(str(close_event.starting_sol))
                pnl_sol = sol_received - sol_deployed
                pnl_pct = (pnl_sol / sol_deployed * Decimal('100')) if sol_deployed > 0 else Decimal('0')

                close_reason = "failsafe" if pid in failsafe_ids else "normal"

                # Check if we have Meteora data
                full_addr = resolved_addresses.get(pid, "")
                meteora_result = meteora_results.get(pid)

                if meteora_result:
                    # Use Meteora PnL (USD-based, accounts for both token sides)
                    pnl_source = "meteora"
                    meteora_pnl = meteora_result.pnl_sol
                    meteora_pnl_pct = (meteora_pnl / meteora_result.deposited_sol * Decimal('100')) if meteora_result.deposited_sol > 0 else Decimal('0')

                    matched_positions.append(MatchedPosition(
                        timestamp_open=open_event.timestamp,
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token=open_event.token_name,
                        position_type=open_event.position_type,
                        sol_deployed=meteora_result.deposited_sol,
                        sol_received=meteora_result.withdrawn_sol,
                        pnl_sol=meteora_pnl,
                        pnl_pct=meteora_pnl_pct,
                        close_reason=close_reason,
                        mc_at_open=open_event.market_cap,
                        jup_score=open_event.jup_score,
                        token_age=open_event.token_age,
                        token_age_days=normalize_token_age(open_event.token_age)[0],
                        token_age_hours=normalize_token_age(open_event.token_age)[1],
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source=pnl_source,
                        meteora_deposited=meteora_result.deposited_sol,
                        meteora_withdrawn=meteora_result.withdrawn_sol,
                        meteora_fees=meteora_result.fees_sol,
                        meteora_pnl=meteora_pnl,
                        datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
                else:
                    # Use Discord PnL
                    matched_positions.append(MatchedPosition(
                        timestamp_open=open_event.timestamp,
                        timestamp_close=close_event.timestamp,
                        target_wallet=close_event.target,
                        token=open_event.token_name,
                        position_type=open_event.position_type,
                        sol_deployed=sol_deployed,
                        sol_received=sol_received,
                        pnl_sol=pnl_sol,
                        pnl_pct=pnl_pct,
                        close_reason=close_reason,
                        mc_at_open=open_event.market_cap,
                        jup_score=open_event.jup_score,
                        token_age=open_event.token_age,
                        token_age_days=normalize_token_age(open_event.token_age)[0],
                        token_age_hours=normalize_token_age(open_event.token_age)[1],
                        price_drop_pct=None,
                        position_id=pid,
                        full_address=full_addr,
                        pnl_source="discord",
                        datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                        datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                    ))
            else:
                # Close without matching open (pre-existing position)
                sol_received = Decimal(str(close_event.ending_sol)) - Decimal(str(close_event.starting_sol))
                full_addr = resolved_addresses.get(pid, "")

                matched_positions.append(MatchedPosition(
                    timestamp_open="",
                    timestamp_close=close_event.timestamp,
                    target_wallet=close_event.target,
                    token="unknown",
                    position_type="unknown",
                    sol_deployed=Decimal('0'),
                    sol_received=sol_received,
                    pnl_sol=Decimal('0'),
                    pnl_pct=Decimal('0'),
                    close_reason="unknown_open",
                    mc_at_open=0.0,
                    jup_score=0,
                    token_age="",
                    token_age_days=None,
                    token_age_hours=None,
                    price_drop_pct=None,
                    position_id=pid,
                    full_address=full_addr,
                    datetime_open="",
                    datetime_close=make_iso_datetime(close_event.date, close_event.timestamp)
                ))

        # Handle rug events (match by position_id if available)
        for rug_event in self.parser.rug_events:
            if rug_event.position_id:
                pid = rug_event.position_id
                matched_ids.add(pid)

                # If rug event has full address, add to resolved_addresses
                if rug_event.position_address and pid not in resolved_addresses:
                    resolved_addresses[pid] = rug_event.position_address

                if pid in open_by_id:
                    open_event = open_by_id[pid]
                    sol_deployed = Decimal(str(open_event.your_sol))

                    # Add any extra liquidity
                    for liq in liquidity_by_id.get(pid, []):
                        sol_deployed += Decimal(str(liq.amount_sol))

                    estimated_loss = sol_deployed * Decimal(str(rug_event.price_drop)) / Decimal('100')
                    pnl_sol = -estimated_loss
                    pnl_pct = -Decimal(str(rug_event.price_drop))

                    full_addr = resolved_addresses.get(pid, "")

                    matched_positions.append(MatchedPosition(
                        timestamp_open=open_event.timestamp,
                        timestamp_close=rug_event.timestamp,
                        target_wallet=rug_event.target,
                        token=open_event.token_name,
                        position_type=open_event.position_type,
                        sol_deployed=sol_deployed,
                        sol_received=Decimal('0'),
                        pnl_sol=pnl_sol,
                        pnl_pct=pnl_pct,
                        close_reason="rug",
                        mc_at_open=open_event.market_cap,
                        jup_score=open_event.jup_score,
                        token_age=open_event.token_age,
                        token_age_days=normalize_token_age(open_event.token_age)[0],
                        token_age_hours=normalize_token_age(open_event.token_age)[1],
                        price_drop_pct=rug_event.price_drop,
                        position_id=pid,
                        full_address=full_addr,
                        datetime_open=make_iso_datetime(open_event.date, open_event.timestamp),
                        datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                    ))
                else:
                    full_addr = resolved_addresses.get(pid, "")
                    matched_positions.append(MatchedPosition(
                        timestamp_open="",
                        timestamp_close=rug_event.timestamp,
                        target_wallet=rug_event.target,
                        token="unknown",
                        position_type="unknown",
                        sol_deployed=Decimal('0'),
                        sol_received=Decimal('0'),
                        pnl_sol=Decimal('0'),
                        pnl_pct=Decimal('0'),
                        close_reason="rug_unknown_open",
                        mc_at_open=0.0,
                        jup_score=0,
                        token_age="",
                        token_age_days=None,
                        token_age_hours=None,
                        price_drop_pct=rug_event.price_drop,
                        position_id=pid,
                        full_address=full_addr,
                        datetime_open="",
                        datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                    ))
            else:
                # Rug event without position_id - can't match properly
                matched_positions.append(MatchedPosition(
                    timestamp_open="",
                    timestamp_close=rug_event.timestamp,
                    target_wallet=rug_event.target,
                    token="unknown",
                    position_type="unknown",
                    sol_deployed=Decimal('0'),
                    sol_received=Decimal('0'),
                    pnl_sol=Decimal('0'),
                    pnl_pct=Decimal('0'),
                    close_reason="rug_unknown_open",
                    mc_at_open=0.0,
                    jup_score=0,
                    token_age="",
                    token_age_days=None,
                    token_age_hours=None,
                    price_drop_pct=rug_event.price_drop,
                    position_id="",
                    datetime_open="",
                    datetime_close=make_iso_datetime(rug_event.date, rug_event.timestamp)
                ))

        # Unmatched opens = opens whose position_id was never closed
        unmatched_opens = [o for o in self.parser.open_events if o.position_id not in matched_ids]

        return matched_positions, unmatched_opens


# ============================================================================
# CsvWriter
# ============================================================================

class CsvWriter:
    """Generate CSV files"""

    def generate_positions_csv(self, matched_positions: List[MatchedPosition],
                               unmatched_opens: List[OpenEvent],
                               output_path: str) -> None:
        """Generate positions.csv with all matched positions"""
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'timestamp_open', 'date_open', 'timestamp_close', 'date_close',
                'datetime_open', 'datetime_close',
                'target_wallet', 'token', 'position_type',
                'sol_deployed', 'sol_received', 'pnl_sol', 'pnl_pct', 'close_reason',
                'mc_at_open', 'jup_score', 'token_age', 'token_age_days', 'token_age_hours',
                'price_drop_pct', 'position_id',
                'full_address', 'pnl_source', 'meteora_deposited', 'meteora_withdrawn',
                'meteora_fees', 'meteora_pnl'
            ])

            for pos in matched_positions:
                # Extract dates from datetime fields
                date_open = pos.datetime_open.split('T')[0] if pos.datetime_open and 'T' in pos.datetime_open else ""
                date_close = pos.datetime_close.split('T')[0] if pos.datetime_close and 'T' in pos.datetime_close else ""

                writer.writerow([
                    pos.timestamp_open,
                    date_open,
                    pos.timestamp_close,
                    date_close,
                    pos.datetime_open,
                    pos.datetime_close,
                    pos.target_wallet,
                    pos.token,
                    pos.position_type,
                    f"{pos.sol_deployed:.4f}",
                    f"{pos.sol_received:.4f}",
                    f"{pos.pnl_sol:.4f}",
                    f"{pos.pnl_pct:.2f}",
                    pos.close_reason,
                    f"{pos.mc_at_open:.2f}",
                    pos.jup_score,
                    pos.token_age,
                    pos.token_age_days if pos.token_age_days is not None else "",
                    pos.token_age_hours if pos.token_age_hours is not None else "",
                    f"{pos.price_drop_pct:.2f}" if pos.price_drop_pct else "",
                    pos.position_id,
                    pos.full_address,
                    pos.pnl_source,
                    f"{pos.meteora_deposited:.4f}" if pos.meteora_deposited is not None else "",
                    f"{pos.meteora_withdrawn:.4f}" if pos.meteora_withdrawn is not None else "",
                    f"{pos.meteora_fees:.4f}" if pos.meteora_fees is not None else "",
                    f"{pos.meteora_pnl:.4f}" if pos.meteora_pnl is not None else ""
                ])

            # Add still-open positions
            for open_event in unmatched_opens:
                datetime_open = make_iso_datetime(open_event.date, open_event.timestamp)
                date_open = datetime_open.split('T')[0] if datetime_open and 'T' in datetime_open else ""
                age_days, age_hours = normalize_token_age(open_event.token_age)

                writer.writerow([
                    open_event.timestamp,
                    date_open,
                    "",  # No close timestamp
                    "",  # No date_close
                    datetime_open,
                    "",  # No datetime_close
                    open_event.target,
                    open_event.token_name,
                    open_event.position_type,
                    f"{open_event.your_sol:.4f}",
                    "",  # No received amount
                    "",  # No PnL
                    "",  # No PnL %
                    "still_open",
                    f"{open_event.market_cap:.2f}",
                    open_event.jup_score,
                    open_event.token_age,
                    age_days if age_days is not None else "",
                    age_hours if age_hours is not None else "",
                    "",  # No price_drop_pct
                    open_event.position_id,
                    "",  # No full_address
                    "",  # No pnl_source
                    "",  # No meteora_deposited
                    "",  # No meteora_withdrawn
                    "",  # No meteora_fees
                    ""   # No meteora_pnl
                ])

    def generate_summary_csv(self, matched_positions: List[MatchedPosition],
                            skip_events: List[SkipEvent],
                            output_path: str) -> None:
        """Generate summary.csv with per-target statistics"""
        # Find reference time (latest datetime_close across all positions)
        ref_time = None
        for pos in matched_positions:
            if pos.datetime_close:
                dt = parse_iso_datetime(pos.datetime_close)
                if dt:
                    if ref_time is None or dt > ref_time:
                        ref_time = dt

        # Aggregate by target wallet
        target_stats: Dict[str, Dict] = defaultdict(lambda: {
            'total_positions': 0,
            'wins': 0,
            'losses': 0,
            'rugs': 0,
            'total_pnl_sol': Decimal('0'),
            'total_sol_deployed': Decimal('0'),
            'min_date': None,
            'max_date': None,
            'mc_values': [],
            'jup_scores': [],
            'age_days_values': [],
            'positions_24h': [],
            'positions_72h': [],
            'positions_7d': []
        })

        for pos in matched_positions:
            stats = target_stats[pos.target_wallet]
            stats['total_positions'] += 1

            if pos.close_reason in ("rug", "rug_unknown_open"):
                stats['rugs'] += 1
                stats['losses'] += 1
            elif pos.close_reason == "unknown_open":
                stats['losses'] += 1  # Unknown close, count as loss
            elif pos.pnl_sol > 0:
                stats['wins'] += 1
            else:
                stats['losses'] += 1

            stats['total_pnl_sol'] += pos.pnl_sol
            stats['total_sol_deployed'] += pos.sol_deployed

            # Collect token metrics (skip 0/None values)
            if pos.mc_at_open and pos.mc_at_open > 0:
                stats['mc_values'].append(pos.mc_at_open)
            if pos.jup_score and pos.jup_score > 0:
                stats['jup_scores'].append(pos.jup_score)
            if pos.token_age_days is not None and pos.token_age_days >= 0:
                stats['age_days_values'].append(pos.token_age_days)

            # Track date range
            for dt_str in [pos.datetime_open, pos.datetime_close]:
                if dt_str and 'T' in dt_str:
                    date_part = dt_str.split('T')[0]
                    if date_part:  # Not empty (e.g., "2026-02-12")
                        if stats['min_date'] is None or date_part < stats['min_date']:
                            stats['min_date'] = date_part
                        if stats['max_date'] is None or date_part > stats['max_date']:
                            stats['max_date'] = date_part

            # Collect positions for time windows (if ref_time is available)
            if ref_time and pos.datetime_close:
                close_dt = parse_iso_datetime(pos.datetime_close)
                if close_dt:
                    # Check if position falls within each time window
                    hours_diff = (ref_time - close_dt).total_seconds() / 3600

                    if hours_diff <= 24:
                        stats['positions_24h'].append(pos)
                    if hours_diff <= 72:
                        stats['positions_72h'].append(pos)
                    if hours_diff <= 168:  # 7 days = 168 hours
                        stats['positions_7d'].append(pos)

        # Add skip counts
        skip_counts = defaultdict(int)
        for skip_event in skip_events:
            skip_counts[skip_event.target] += 1

        # Write summary
        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'target_wallet', 'total_positions', 'wins', 'losses', 'rugs', 'skips',
                'total_pnl_sol', 'avg_pnl_sol', 'win_rate_pct', 'avg_sol_deployed',
                'avg_mc', 'avg_jup_score', 'avg_token_age_days',
                'positions_24h', 'pnl_24h', 'win_rate_24h', 'rugs_24h',
                'positions_72h', 'pnl_72h', 'win_rate_72h', 'rugs_72h',
                'positions_7d', 'pnl_7d', 'win_rate_7d', 'rugs_7d',
                'avg_positions_per_day', 'date_range'
            ])

            for target, stats in target_stats.items():
                total_pos = stats['total_positions']
                wins = stats['wins']
                total_pnl = stats['total_pnl_sol']
                total_deployed = stats['total_sol_deployed']

                avg_pnl = total_pnl / total_pos if total_pos > 0 else Decimal('0')
                win_rate = Decimal(wins) / Decimal(total_pos) * Decimal('100') if total_pos > 0 else Decimal('0')
                avg_deployed = total_deployed / total_pos if total_pos > 0 else Decimal('0')

                # Calculate aggregate token metrics
                avg_mc = sum(stats['mc_values']) / len(stats['mc_values']) if stats['mc_values'] else 0
                avg_jup_score = sum(stats['jup_scores']) / len(stats['jup_scores']) if stats['jup_scores'] else 0
                avg_age_days = sum(stats['age_days_values']) / len(stats['age_days_values']) if stats['age_days_values'] else 0

                # Format date range
                if stats['min_date'] and stats['max_date']:
                    if stats['min_date'] == stats['max_date']:
                        date_range = stats['min_date']
                    else:
                        date_range = f"{stats['min_date']} to {stats['max_date']}"
                else:
                    date_range = ""

                # Calculate time-windowed stats
                def calc_window_stats(positions):
                    """Calculate stats for a time window of positions"""
                    if not positions:
                        return 0, Decimal('0'), Decimal('0'), 0

                    count = len(positions)
                    pnl = sum(p.pnl_sol for p in positions)
                    wins_in_window = sum(1 for p in positions if p.pnl_sol > 0 and p.close_reason not in ("rug", "rug_unknown_open", "unknown_open"))
                    rugs = sum(1 for p in positions if p.close_reason in ("rug", "rug_unknown_open"))
                    win_rate = Decimal(wins_in_window) / Decimal(count) * Decimal('100') if count > 0 else Decimal('0')

                    return count, pnl, win_rate, rugs

                count_24h, pnl_24h, wr_24h, rugs_24h = calc_window_stats(stats['positions_24h'])
                count_72h, pnl_72h, wr_72h, rugs_72h = calc_window_stats(stats['positions_72h'])
                count_7d, pnl_7d, wr_7d, rugs_7d = calc_window_stats(stats['positions_7d'])

                # Calculate avg positions per day
                avg_pos_per_day = ""
                if stats['min_date'] and stats['max_date']:
                    try:
                        min_dt = datetime.strptime(stats['min_date'], "%Y-%m-%d")
                        max_dt = datetime.strptime(stats['max_date'], "%Y-%m-%d")
                        days_diff = (max_dt - min_dt).days + 1  # +1 to include both start and end
                        if days_diff >= 1:
                            avg_pos_per_day = f"{total_pos / days_diff:.2f}"
                    except ValueError:
                        pass

                writer.writerow([
                    target,
                    total_pos,
                    wins,
                    stats['losses'],
                    stats['rugs'],
                    skip_counts.get(target, 0),
                    f"{total_pnl:.4f}",
                    f"{avg_pnl:.4f}",
                    f"{win_rate:.2f}",
                    f"{avg_deployed:.4f}",
                    f"{avg_mc:.2f}",
                    f"{avg_jup_score:.2f}",
                    f"{avg_age_days:.2f}",
                    count_24h if ref_time else "",
                    f"{pnl_24h:.4f}" if ref_time and count_24h > 0 else "",
                    f"{wr_24h:.2f}" if ref_time and count_24h > 0 else "",
                    rugs_24h if ref_time and count_24h > 0 else "",
                    count_72h if ref_time else "",
                    f"{pnl_72h:.4f}" if ref_time and count_72h > 0 else "",
                    f"{wr_72h:.2f}" if ref_time and count_72h > 0 else "",
                    rugs_72h if ref_time and count_72h > 0 else "",
                    count_7d if ref_time else "",
                    f"{pnl_7d:.4f}" if ref_time and count_7d > 0 else "",
                    f"{wr_7d:.2f}" if ref_time and count_7d > 0 else "",
                    rugs_7d if ref_time and count_7d > 0 else "",
                    avg_pos_per_day,
                    date_range
                ])


# ============================================================================
# JSON Export/Import Functions
# ============================================================================

def export_to_json(positions: List[MatchedPosition], unmatched_opens: List[OpenEvent],
                   skip_events: List[SkipEvent], output_path: str) -> None:
    """
    Export positions to JSON file for portable data persistence.

    Args:
        positions: List of matched positions
        unmatched_opens: List of still-open positions
        skip_events: List of skip events
        output_path: Path to output .valhalla.json file
    """
    # Convert positions to dicts
    positions_data = []
    for pos in positions:
        pos_dict = {
            "position_id": pos.position_id,
            "token": pos.token,
            "target_wallet": pos.target_wallet,
            "position_type": pos.position_type,
            "timestamp_open": pos.timestamp_open,
            "timestamp_close": pos.timestamp_close,
            "datetime_open": pos.datetime_open,
            "datetime_close": pos.datetime_close,
            "sol_deployed": str(pos.sol_deployed),
            "sol_received": str(pos.sol_received),
            "pnl_sol": str(pos.pnl_sol),
            "pnl_pct": str(pos.pnl_pct),
            "close_reason": pos.close_reason,
            "mc_at_open": pos.mc_at_open,
            "jup_score": pos.jup_score,
            "token_age": pos.token_age,
            "token_age_days": pos.token_age_days,
            "token_age_hours": pos.token_age_hours,
            "price_drop_pct": pos.price_drop_pct,
            "full_address": pos.full_address,
            "pnl_source": pos.pnl_source,
            "meteora_deposited": str(pos.meteora_deposited) if pos.meteora_deposited is not None else None,
            "meteora_withdrawn": str(pos.meteora_withdrawn) if pos.meteora_withdrawn is not None else None,
            "meteora_fees": str(pos.meteora_fees) if pos.meteora_fees is not None else None,
            "meteora_pnl": str(pos.meteora_pnl) if pos.meteora_pnl is not None else None
        }
        positions_data.append(pos_dict)

    # Convert unmatched opens to dicts
    still_open_data = []
    for open_event in unmatched_opens:
        age_days, age_hours = normalize_token_age(open_event.token_age)
        datetime_open = make_iso_datetime(open_event.date, open_event.timestamp)

        open_dict = {
            "position_id": open_event.position_id,
            "token": open_event.token_name,
            "target_wallet": open_event.target,
            "position_type": open_event.position_type,
            "timestamp_open": open_event.timestamp,
            "datetime_open": datetime_open,
            "sol_deployed": str(Decimal(str(open_event.your_sol))),
            "mc_at_open": open_event.market_cap,
            "jup_score": open_event.jup_score,
            "token_age": open_event.token_age,
            "token_age_days": age_days,
            "token_age_hours": age_hours
        }
        still_open_data.append(open_dict)

    # Gather metadata
    target_wallets = set()
    dates = set()
    for pos in positions:
        target_wallets.add(pos.target_wallet)
        # Extract dates from datetime fields
        if pos.datetime_open and 'T' in pos.datetime_open:
            dates.add(pos.datetime_open.split('T')[0])
        if pos.datetime_close and 'T' in pos.datetime_close:
            dates.add(pos.datetime_close.split('T')[0])

    for open_event in unmatched_opens:
        target_wallets.add(open_event.target)
        datetime_open = make_iso_datetime(open_event.date, open_event.timestamp)
        if datetime_open and 'T' in datetime_open:
            dates.add(datetime_open.split('T')[0])

    # Create JSON structure
    export_data = {
        "version": "1.0",
        "export_timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "positions": positions_data,
        "still_open": still_open_data,
        "metadata": {
            "total_positions": len(positions),
            "total_still_open": len(unmatched_opens),
            "target_wallets": sorted(target_wallets),
            "date_range": sorted([d for d in dates if d])  # Filter out empty dates
        }
    }

    # Write to file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, ensure_ascii=False)

    print(f"  Exported {len(positions)} positions and {len(unmatched_opens)} still-open to {output_path}")


def import_from_json(json_path: str) -> Tuple[List[MatchedPosition], List[dict]]:
    """
    Import positions from .valhalla.json file.

    Args:
        json_path: Path to .valhalla.json file

    Returns:
        Tuple of (positions, still_open_dicts)
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Validate version
    version = data.get('version', 'unknown')
    major_version = version.split('.')[0]
    if major_version != '1':
        print(f"  Warning: JSON file version {version} may not be compatible (expected 1.x)")

    # Convert position dicts to MatchedPosition objects
    positions = []
    for pos_dict in data.get('positions', []):
        # Helper to parse optional Decimal
        def parse_optional_decimal(val):
            if val is None or val == '':
                return None
            return Decimal(str(val))

        positions.append(MatchedPosition(
            timestamp_open=pos_dict.get('timestamp_open', ''),
            timestamp_close=pos_dict.get('timestamp_close', ''),
            target_wallet=pos_dict.get('target_wallet', ''),
            token=pos_dict.get('token', ''),
            position_type=pos_dict.get('position_type', ''),
            sol_deployed=Decimal(str(pos_dict.get('sol_deployed', '0'))),
            sol_received=Decimal(str(pos_dict.get('sol_received', '0'))),
            pnl_sol=Decimal(str(pos_dict.get('pnl_sol', '0'))),
            pnl_pct=Decimal(str(pos_dict.get('pnl_pct', '0'))),
            close_reason=pos_dict.get('close_reason', ''),
            mc_at_open=float(pos_dict.get('mc_at_open', 0.0)),
            jup_score=int(pos_dict.get('jup_score', 0)),
            token_age=pos_dict.get('token_age', ''),
            token_age_days=pos_dict.get('token_age_days'),
            token_age_hours=pos_dict.get('token_age_hours'),
            price_drop_pct=pos_dict.get('price_drop_pct'),
            position_id=pos_dict.get('position_id', ''),
            full_address=pos_dict.get('full_address', ''),
            pnl_source=pos_dict.get('pnl_source', 'discord'),
            meteora_deposited=parse_optional_decimal(pos_dict.get('meteora_deposited')),
            meteora_withdrawn=parse_optional_decimal(pos_dict.get('meteora_withdrawn')),
            meteora_fees=parse_optional_decimal(pos_dict.get('meteora_fees')),
            meteora_pnl=parse_optional_decimal(pos_dict.get('meteora_pnl')),
            datetime_open=pos_dict.get('datetime_open', ''),
            datetime_close=pos_dict.get('datetime_close', '')
        ))

    still_open_dicts = data.get('still_open', [])

    print(f"  Imported {len(positions)} positions and {len(still_open_dicts)} still-open from {json_path}")

    return positions, still_open_dicts


def merge_with_imported(new_positions: List[MatchedPosition],
                        imported_positions: List[MatchedPosition],
                        new_opens: List[OpenEvent],
                        imported_still_open: List[dict]) -> Tuple[List[MatchedPosition], List[OpenEvent]]:
    """
    Merge new data with imported data.

    Args:
        new_positions: Newly parsed matched positions
        imported_positions: Positions from imported JSON
        new_opens: Newly parsed open events (still open)
        imported_still_open: Still-open dicts from imported JSON

    Returns:
        Tuple of (merged_positions, merged_unmatched_opens)
    """
    # Index positions by position_id (new data wins on conflicts)
    positions_by_id = {}

    # Add imported positions first
    for pos in imported_positions:
        if pos.position_id:
            positions_by_id[pos.position_id] = pos

    # Add new positions (overwriting if same position_id)
    for pos in new_positions:
        if pos.position_id:
            positions_by_id[pos.position_id] = pos

    # Check if any imported still_open are now closed in new data
    new_position_ids = {p.position_id for p in new_positions if p.position_id}

    # Convert imported still_open to OpenEvents, excluding those now closed
    still_open_events = []
    for open_dict in imported_still_open:
        position_id = open_dict.get('position_id', '')

        # Skip if this position is now closed
        if position_id and position_id in new_position_ids:
            print(f"    Position {position_id} was still-open in import, now closed")
            continue

        # Convert to OpenEvent
        still_open_events.append(OpenEvent(
            timestamp=open_dict.get('timestamp_open', ''),
            position_type=open_dict.get('position_type', ''),
            token_name=open_dict.get('token', ''),
            token_pair=f"{open_dict.get('token', '')}-SOL",
            target=open_dict.get('target_wallet', ''),
            market_cap=float(open_dict.get('mc_at_open', 0.0)),
            token_age=open_dict.get('token_age', ''),
            jup_score=int(open_dict.get('jup_score', 0)),
            target_sol=float(open_dict.get('sol_deployed', '0')),
            your_sol=float(open_dict.get('sol_deployed', '0')),
            position_id=position_id,
            tx_signatures=[],
            date=open_dict.get('datetime_open', '').split('T')[0] if open_dict.get('datetime_open') else ''
        ))

    # Merge new_opens with still_open_events (dedup by position_id)
    opens_by_id = {}
    for event in still_open_events:
        if event.position_id:
            opens_by_id[event.position_id] = event

    for event in new_opens:
        if event.position_id:
            opens_by_id[event.position_id] = event  # New data wins

    merged_positions = list(positions_by_id.values())
    merged_opens = list(opens_by_id.values())

    print(f"  Merged: {len(merged_positions)} total positions, {len(merged_opens)} still open")

    return merged_positions, merged_opens


# ============================================================================
# Merge Functions
# ============================================================================

def merge_positions_csvs(csv_paths: List[str], output_dir: str) -> None:
    """
    Merge multiple positions.csv files, deduplicating by position_id.

    Args:
        csv_paths: List of paths to positions.csv files
        output_dir: Output directory for merged positions.csv and summary.csv
    """
    print(f"\nMerging {len(csv_paths)} positions.csv file(s)...")

    # Read all CSV files
    all_rows = []
    for csv_path in csv_paths:
        print(f"  Reading {csv_path}...")
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            print(f"    {len(rows)} positions")
            all_rows.extend(rows)

    print(f"  Total positions before deduplication: {len(all_rows)}")

    # Deduplicate by position_id
    # - Empty position_id: keep all (they're unique)
    # - Same position_id: keep the one from the LAST file (later file = newer data)
    seen_ids = {}
    deduplicated_rows = []

    for row in all_rows:
        position_id = row.get('position_id', '').strip()

        if not position_id:
            # Empty position_id - keep all of them
            deduplicated_rows.append(row)
        else:
            # Non-empty position_id - track and potentially replace
            seen_ids[position_id] = row

    # Add all tracked position_ids (latest version of each)
    deduplicated_rows.extend(seen_ids.values())

    print(f"  Positions after deduplication: {len(deduplicated_rows)}")

    # Convert rows back to MatchedPosition objects for summary calculation
    matched_positions = []

    for row in deduplicated_rows:
        # Skip still_open positions for summary (they have no PnL yet)
        if row.get('close_reason') == 'still_open':
            continue

        # Parse Decimal fields safely
        def parse_decimal(val: str) -> Decimal:
            if not val or val.strip() == '':
                return Decimal('0')
            return Decimal(val)

        def parse_optional_decimal(val: str) -> Optional[Decimal]:
            if not val or val.strip() == '':
                return None
            return Decimal(val)

        def parse_int(val: str) -> int:
            if not val or val.strip() == '':
                return 0
            return int(val)

        def parse_optional_int(val: str) -> Optional[int]:
            if not val or val.strip() == '':
                return None
            return int(val)

        def parse_float(val: str) -> float:
            if not val or val.strip() == '':
                return 0.0
            return float(val)

        def parse_optional_float(val: str) -> Optional[float]:
            if not val or val.strip() == '':
                return None
            return float(val)

        matched_positions.append(MatchedPosition(
            timestamp_open=row.get('timestamp_open', ''),
            timestamp_close=row.get('timestamp_close', ''),
            target_wallet=row.get('target_wallet', ''),
            token=row.get('token', ''),
            position_type=row.get('position_type', ''),
            sol_deployed=parse_decimal(row.get('sol_deployed', '0')),
            sol_received=parse_decimal(row.get('sol_received', '0')),
            pnl_sol=parse_decimal(row.get('pnl_sol', '0')),
            pnl_pct=parse_decimal(row.get('pnl_pct', '0')),
            close_reason=row.get('close_reason', ''),
            mc_at_open=parse_float(row.get('mc_at_open', '0')),
            jup_score=parse_int(row.get('jup_score', '0')),
            token_age=row.get('token_age', ''),
            token_age_days=parse_optional_int(row.get('token_age_days', '')),
            token_age_hours=parse_optional_int(row.get('token_age_hours', '')),
            price_drop_pct=parse_optional_float(row.get('price_drop_pct', '')),
            position_id=row.get('position_id', ''),
            full_address=row.get('full_address', ''),
            pnl_source=row.get('pnl_source', 'discord'),
            meteora_deposited=parse_optional_decimal(row.get('meteora_deposited', '')),
            meteora_withdrawn=parse_optional_decimal(row.get('meteora_withdrawn', '')),
            meteora_fees=parse_optional_decimal(row.get('meteora_fees', '')),
            meteora_pnl=parse_optional_decimal(row.get('meteora_pnl', '')),
            datetime_open=row.get('datetime_open', ''),
            datetime_close=row.get('datetime_close', '')
        ))

    # Write merged positions.csv
    output_path = Path(output_dir)
    positions_csv = output_path / 'positions.csv'
    summary_csv = output_path / 'summary.csv'

    print(f"\nWriting merged files...")

    # Write positions CSV (write all deduplicated rows, including still_open)
    with open(positions_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'timestamp_open', 'date_open', 'timestamp_close', 'date_close',
            'datetime_open', 'datetime_close',
            'target_wallet', 'token', 'position_type',
            'sol_deployed', 'sol_received', 'pnl_sol', 'pnl_pct', 'close_reason',
            'mc_at_open', 'jup_score', 'token_age', 'token_age_days', 'token_age_hours',
            'price_drop_pct', 'position_id',
            'full_address', 'pnl_source', 'meteora_deposited', 'meteora_withdrawn',
            'meteora_fees', 'meteora_pnl'
        ])
        writer.writeheader()
        writer.writerows(deduplicated_rows)

    print(f"  {positions_csv}")

    # Generate summary CSV (using only matched_positions, not still_open)
    csv_writer = CsvWriter()
    csv_writer.generate_summary_csv(matched_positions, [], str(summary_csv))  # Empty skip_events list

    print(f"  {summary_csv}")

    # Print summary
    print(f"\n{'='*60}")
    print(f"Merge Summary")
    print(f"{'='*60}")
    print(f"Total positions in merged file: {len(deduplicated_rows)}")
    print(f"Closed positions: {len(matched_positions)}")
    print(f"Still open positions: {len(deduplicated_rows) - len(matched_positions)}")

    if matched_positions:
        total_pnl = sum(p.pnl_sol for p in matched_positions)
        print(f"Total PnL: {total_pnl:.4f} SOL")

    print(f"\nDone!")


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Parse Valhalla Bot Discord DM logs and generate PnL analysis with Meteora API.'
    )
    parser.add_argument('input_files', nargs='*', help='Path(s) to Discord DM log file(s) (default: all files in input/ folder)')
    parser.add_argument('--output-dir', default='output', help='Output directory for CSV files (default: output/)')
    parser.add_argument('--rpc-url', default='https://api.mainnet-beta.solana.com',
                       help='Solana RPC URL (default: public mainnet)')
    parser.add_argument('--skip-rpc', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--skip-meteora', action='store_true', help=argparse.SUPPRESS)  # Hidden dev flag
    parser.add_argument('--no-archive', action='store_true', help='Skip moving processed files to archive/')
    parser.add_argument('--cache-file', help='Address cache JSON file (default: address_cache.json in output-dir)')
    parser.add_argument('--date', help='Date for logs in YYYY-MM-DD format (optional, will try to detect from filename)')
    parser.add_argument('--input-format', choices=['auto', 'text', 'html'], default='auto',
                       help='Input format: auto (detect), text (plain text), html (HTML from browser)')
    parser.add_argument('--merge', nargs='+', metavar='CSV_FILE',
                       help='Merge multiple positions.csv files (use instead of input_files)')
    parser.add_argument('--export-json', metavar='FILE',
                       help='Export results as .valhalla.json for incremental workflows')
    parser.add_argument('--import-json', metavar='FILE',
                       help='Import previous .valhalla.json to merge with new data')
    parser.add_argument('--skip-charts', action='store_true', help='Skip chart generation')

    args = parser.parse_args()

    # Create output directory if needed
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Handle merge mode
    if args.merge:
        merge_positions_csvs(args.merge, str(output_dir))
        return

    # Get input files - either from args or all files in input/ folder
    if not args.input_files:
        input_dir = Path('input')
        if input_dir.exists() and input_dir.is_dir():
            # Get all .txt and .html files in input/
            input_files = [str(f) for f in input_dir.iterdir() if f.is_file() and f.suffix in ['.txt', '.html']]
            if not input_files:
                parser.error("No .txt or .html files found in input/ folder")
            print(f"Processing all files in input/ folder: {len(input_files)} file(s)")
        else:
            parser.error("No input files specified and input/ folder not found")
    else:
        input_files = args.input_files

    # Determine cache file path
    cache_file = args.cache_file if args.cache_file else str(output_dir / 'address_cache.json')

    # Step 1: Read and parse all input files
    all_messages = []
    event_parser = EventParser()  # Will initialize per-file
    processed_files = []  # Track successfully processed files for archiving

    for input_file in input_files:
        # Detect format and create appropriate reader
        print(f"\nReading Discord logs: {input_file}")

        fmt = args.input_format
        if fmt == 'auto':
            fmt = detect_input_format(input_file)
            print(f"  Auto-detected format: {fmt}")

        if fmt == 'html':
            reader = HtmlReader(input_file)
        else:
            reader = PlainTextReader(input_file)

        messages = reader.read()
        print(f"  Found {len(messages)} Valhalla messages")

        # Determine date for this file - priority order:
        # 1. Filename prefix (YYYYMMDD_*.txt)
        # 2. In-file date header (first line)
        # 3. User prompt if neither found
        file_date = None
        date_source = None

        # Try filename first
        file_date = extract_date_from_filename(input_file)
        if file_date:
            date_source = "filename"
        # Then try in-file header
        elif reader.header_date:
            file_date = reader.header_date
            date_source = "in-file header"
        # Finally, prompt user
        else:
            print(f"  No date found in filename or file header")
            user_input = input(f"  Enter date for {Path(input_file).name} (YYYYMMDD): ").strip()
            if user_input and len(user_input) == 8 and user_input.isdigit():
                try:
                    year = int(user_input[0:4])
                    month = int(user_input[4:6])
                    day = int(user_input[6:8])
                    datetime(year, month, day)
                    file_date = f"{year:04d}-{month:02d}-{day:02d}"
                    date_source = "user input"
                except ValueError:
                    print(f"  Invalid date format, continuing without date")

        if file_date:
            print(f"  Date detected from {date_source}: {file_date}")
        else:
            print(f"  No date available")

        # Parse events with date context
        print(f"Parsing events (date: {file_date or 'none'})...")
        file_parser = EventParser(base_date=file_date)
        file_parser.parse_messages(messages)

        # Merge events into main parser
        event_parser.open_events.extend(file_parser.open_events)
        event_parser.close_events.extend(file_parser.close_events)
        event_parser.rug_events.extend(file_parser.rug_events)
        event_parser.skip_events.extend(file_parser.skip_events)
        event_parser.swap_events.extend(file_parser.swap_events)
        event_parser.failsafe_events.extend(file_parser.failsafe_events)
        event_parser.add_liquidity_events.extend(file_parser.add_liquidity_events)

        # Track for archiving
        processed_files.append((input_file, file_date))

    # Step 2: Print aggregated event counts
    print(f"\nTotal parsed events across {len(input_files)} file(s):")

    print(f"  Open positions: {len(event_parser.open_events)}")
    print(f"  Close events: {len(event_parser.close_events)}")
    print(f"  Failsafe events: {len(event_parser.failsafe_events)}")
    print(f"  Add liquidity events: {len(event_parser.add_liquidity_events)}")
    print(f"  Rug events: {len(event_parser.rug_events)}")
    print(f"  Skip events: {len(event_parser.skip_events)}")
    print(f"  Swap events: {len(event_parser.swap_events)}")

    # Step 3: Resolve addresses
    resolved_addresses: Dict[str, str] = {}
    cache = AddressCache(cache_file)

    if not args.skip_rpc:
        print(f"\nResolving position addresses via Solana RPC...")
        rpc_client = SolanaRpcClient(args.rpc_url)
        resolver = PositionResolver(cache, rpc_client)

        # Collect all events with position IDs and tx signatures
        seen_pids = set()
        events_to_resolve = []
        for event in event_parser.open_events + event_parser.close_events:
            if event.tx_signatures and event.position_id not in seen_pids:
                seen_pids.add(event.position_id)
                events_to_resolve.append((event.position_id, event.tx_signatures))

        total = len(events_to_resolve)
        for i, (pid, sigs) in enumerate(events_to_resolve, 1):
            print(f"  Resolving {i}/{total}: {pid}...", end='', flush=True)
            full_addr = resolver.resolve(pid, sigs)
            if full_addr:
                resolved_addresses[pid] = full_addr
                print(f" OK ({full_addr[:8]}...)")
            else:
                print(f" NOT FOUND")

        print(f"  Resolved {len(resolved_addresses)} addresses")
        cache.save()
    else:
        print(f"\nSkipping RPC resolution (--skip-rpc)")
        # Load from cache only
        for event in event_parser.open_events + event_parser.close_events:
            cached = cache.get(event.position_id)
            if cached:
                resolved_addresses[event.position_id] = cached
        print(f"  Loaded {len(resolved_addresses)} addresses from cache")

    # Step 4: Calculate Meteora PnL
    meteora_results: Dict[str, MeteoraPnlResult] = {}

    if not args.skip_meteora and resolved_addresses:
        print(f"\nFetching Meteora PnL data...")
        meteora_calc = MeteoraPnlCalculator()

        total = len(resolved_addresses)
        for i, (pid, full_addr) in enumerate(resolved_addresses.items(), 1):
            print(f"  Fetching {i}/{total}: {pid}...", end='', flush=True)
            result = meteora_calc.calculate_pnl(full_addr)
            if result:
                meteora_results[pid] = result
                print(f" PnL: {result.pnl_sol:.4f} SOL (${result.pnl_usd:.2f})")
            else:
                print(f" FAILED")

        print(f"  Retrieved PnL for {len(meteora_results)} positions")
    elif args.skip_meteora:
        print(f"\nSkipping Meteora API (--skip-meteora)")
    else:
        print(f"\nSkipping Meteora API (no resolved addresses)")

    # Step 5: Match positions
    print(f"\nMatching positions...")
    matcher = PositionMatcher(event_parser)
    matched_positions, unmatched_opens = matcher.match_positions(meteora_results, resolved_addresses)
    print(f"  Matched positions: {len(matched_positions)}")
    print(f"  Still open: {len(unmatched_opens)}")

    # Step 5.5: Import and merge with previous data if requested
    if args.import_json:
        print(f"\nImporting previous data from {args.import_json}...")
        imported_positions, imported_still_open = import_from_json(args.import_json)
        print(f"  Merging with new data...")
        matched_positions, unmatched_opens = merge_with_imported(
            matched_positions, imported_positions,
            unmatched_opens, imported_still_open
        )

    # Step 6: Generate CSVs
    positions_csv = output_dir / 'positions.csv'
    summary_csv = output_dir / 'summary.csv'

    print(f"\nGenerating CSV files...")
    csv_writer = CsvWriter()
    csv_writer.generate_positions_csv(matched_positions, unmatched_opens, str(positions_csv))
    csv_writer.generate_summary_csv(matched_positions, event_parser.skip_events, str(summary_csv))

    print(f"  {positions_csv}")
    print(f"  {summary_csv}")

    # Step 6.5: Generate charts
    if not args.skip_charts:
        print(f"\nGenerating charts...")
        generate_charts(matched_positions, str(output_dir))

    # Step 6.6: Export to JSON if requested
    if args.export_json:
        print(f"\nExporting to JSON...")
        export_to_json(matched_positions, unmatched_opens, event_parser.skip_events, args.export_json)

    # Step 6.7: Archive processed files
    if not args.no_archive and processed_files:
        print(f"\nArchiving processed files...")
        archive_dir = Path('archive')
        archive_dir.mkdir(parents=True, exist_ok=True)

        for input_file, file_date in processed_files:
            input_path = Path(input_file)
            base_name = input_path.name

            # Determine archived filename - prepend date if not already present
            if file_date and not re.match(r'^\d{8}', base_name):
                # Prepend date to filename
                date_compact = file_date.replace('-', '')
                archive_name = f"{date_compact}_{base_name}"
            else:
                archive_name = base_name

            archive_path = archive_dir / archive_name

            try:
                shutil.move(str(input_path), str(archive_path))
                print(f"  Archived: {archive_path}")
            except Exception as e:
                print(f"  Failed to archive {input_path}: {e}")

    # Step 7: Print summary stats
    print(f"\n{'='*60}")
    print(f"Summary Statistics")
    print(f"{'='*60}")

    total_pnl = sum(p.pnl_sol for p in matched_positions)
    meteora_count = sum(1 for p in matched_positions if p.pnl_source == 'meteora')
    discord_count = len(matched_positions) - meteora_count

    print(f"Total matched positions: {len(matched_positions)}")
    print(f"  - Using Meteora PnL: {meteora_count}")
    print(f"  - Using Discord PnL: {discord_count}")
    print(f"Still open positions: {len(unmatched_opens)}")
    print(f"Total PnL: {total_pnl:.4f} SOL")
    print(f"\nDone!")


if __name__ == '__main__':
    main()
