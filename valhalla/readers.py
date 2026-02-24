"""
Log file readers for plain text and HTML Discord DM exports.
"""

import re
import html
from dataclasses import dataclass, field
from typing import List, Tuple, Optional
from datetime import datetime


@dataclass
class ParsedMessage:
    """Structured result from reader.read(), replacing bare 3-tuple."""
    timestamp: str
    clean_text: str
    bot_tx_signatures: List[str]
    target_tx_signatures: List[str] = field(default_factory=list)
    target_wallet_address: Optional[str] = None


class PlainTextReader:
    """Parse plain text Discord DM logs with [HH:MM] Author: format"""

    # Pattern to split messages: [HH:MM] or [YYYY-MM-DDTHH:MM] at start of line
    MESSAGE_SPLIT = re.compile(r'^(?=\[(?:\d{4}-\d{2}-\d{2}T)?\d{2}:\d{2}\])', flags=re.MULTILINE)
    # Pattern to extract author from first line (supports both timestamp formats)
    AUTHOR_PATTERN = re.compile(r'^\[((?:\d{4}-\d{2}-\d{2}T)?\d{2}:\d{2})\]\s*(.+?):\s*\n', flags=re.MULTILINE)
    # Solscan TX signature from [https://solscan.io/tx/SIG] (fallback, captures all)
    SOLSCAN_TX_PATTERN = re.compile(r'\[https://solscan\.io/tx/([A-Za-z0-9]+)\]')
    # Labeled Solscan URLs: captures label and signature
    # Matches "Target Tx 1 [https://solscan.io/tx/SIG]" and "Your Solscan 1 [...]"
    # Uses [\w \t]+? to avoid matching across newlines
    LABELED_SOLSCAN_PATTERN = re.compile(
        r'([\w \t]+?)\s*\[https://solscan\.io/tx/([A-Za-z0-9]+)\]'
    )
    # lpagent.io portfolio URL: captures wallet address
    LPAGENT_PATTERN = re.compile(
        r'\[https://app\.lpagent\.io/portfolio\?address=([A-Za-z0-9]+)\]'
    )
    # Any URL in square brackets (for stripping)
    URL_BRACKET_PATTERN = re.compile(r'\[https?://[^\]]+\]')

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.header_date: Optional[str] = None

    def _parse_messages_from_text(self, text: str, date_prefix: str) -> List[ParsedMessage]:
        """
        Parse a plain-text Discord log into ParsedMessage objects.

        Args:
            text: Plain text content (date header already stripped).
            date_prefix: ISO date string from the file header (e.g. "2024-01-15"),
                         used to prefix bare HH:MM timestamps.  Pass '' if none.

        Returns:
            List of ParsedMessage for every Valhalla-authored message found.
        """
        raw_messages = self.MESSAGE_SPLIT.split(text)
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

            # Extract target wallet address before stripping URLs
            lpagent_match = self.LPAGENT_PATTERN.search(raw_msg)
            target_wallet_address = lpagent_match.group(1) if lpagent_match else None

            # Extract labeled tx signatures before stripping URLs
            target_tx_signatures = []
            bot_tx_signatures = []
            for label, sig in self.LABELED_SOLSCAN_PATTERN.findall(raw_msg):
                label_lower = label.lower().strip()
                if 'target' in label_lower:
                    target_tx_signatures.append(sig)
                else:
                    bot_tx_signatures.append(sig)

            # Fallback: only when labeled extraction found neither bot nor target
            # signatures (prevents double-counting URLs that were already captured)
            if not bot_tx_signatures and not target_tx_signatures:
                bot_tx_signatures = self.SOLSCAN_TX_PATTERN.findall(raw_msg)

            # Strip URLs in brackets and the author line prefix
            text_body = raw_msg[author_match.end():]  # Remove the [HH:MM] Author: line
            clean_text = self.URL_BRACKET_PATTERN.sub('', text_body)

            results.append(ParsedMessage(
                timestamp=timestamp,
                clean_text=clean_text,
                bot_tx_signatures=bot_tx_signatures,
                target_tx_signatures=target_tx_signatures,
                target_wallet_address=target_wallet_address
            ))

        return results

    def read(self) -> List[ParsedMessage]:
        """Returns list of ParsedMessage objects (timestamp, clean_text, signatures, etc.)"""
        with open(self.file_path, 'r', encoding='utf-8-sig') as f:
            content = f.read()

        # Check for date header (YYYYMMDD) at the top of the file
        lines = content.split('\n')
        date_prefix = ''
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
                    date_prefix = self.header_date
                    # Strip the date line from content
                    content = '\n'.join(lines[1:])
                except ValueError:
                    # Not a valid date, continue with full content
                    pass

        return self._parse_messages_from_text(content, date_prefix)


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

    def read(self) -> List[ParsedMessage]:
        """Convert HTML to text, then use PlainTextReader logic"""
        with open(self.file_path, 'r', encoding='utf-8-sig', errors='ignore') as f:
            raw_html = f.read()

        # Check for date header before HTML content
        lines = raw_html.split('\n')
        date_prefix = ''
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
                    date_prefix = self.header_date
                    # Strip the date line from content
                    raw_html = '\n'.join(lines[1:])
                except ValueError:
                    # Not a valid date, continue with full content
                    pass

        # Convert HTML to plain text, then delegate to shared parsing logic
        plain_text = self.html_to_text(raw_html)
        return self._parse_messages_from_text(plain_text, date_prefix)


def detect_input_format(file_path: str) -> str:
    """Auto-detect if file is HTML or plain text"""
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        first_4k = f.read(4096)

    # Check for HTML markers
    if re.search(r'<html|<!DOCTYPE|<!--StartFragment|<div|<span|<a\s+href', first_4k, re.IGNORECASE):
        return 'html'
    return 'text'
