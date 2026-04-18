#!/usr/bin/env python3
"""
DiscordChatExporter JSON → valhalla-fjorge input converter.

Converts a DCE JSON export (DiscordChatExporter -f Json) into the plain-text
format expected by valhalla_parser_v2.py / PlainTextReader.

Expected output format per message:
    [YYYY-MM-DDTHH:MM] Author name:\n
    message body with markdown links expanded to: text [url]

Usage:
    python dce_to_input.py <path_to_dce_export.json>
    python dce_to_input.py <path_to_dce_export.json> --out input/custom_name.txt

Output file is written to input/dce_YYYYMMDD_HHMMSS_discord.txt by default.
  YYYYMMDD = date of the first message in the export
  HHMMSS   = current local time of script execution (for uniqueness, mirrors
              the naming convention of save_clipboard.ps1)
"""

import argparse
import json
import re
import sys
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Discord markdown masked links: [text](url) → text [url]
_MD_LINK_RE = re.compile(r'\[([^\]]+)\]\((https?://[^)]+)\)')

# Discord custom emoji: <:name:id> or <a:name:id> (animated)
# These appear as raw syntax in DCE JSON content and should be removed.
_CUSTOM_EMOJI_RE = re.compile(r'<a?:[A-Za-z0-9_]+:\d+>')

# Collapse multiple spaces (but not newlines)
_MULTI_SPACE_RE = re.compile(r'[ \t]{2,}')

# Max one blank line between messages
_MULTI_BLANK_RE = re.compile(r'\n{3,}')


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_timestamp(ts_str: str) -> datetime:
    """Parse ISO 8601 timestamp with offset (DCE format) to local datetime."""
    # Python 3.7+ fromisoformat doesn't handle trailing 'Z' or all offset forms
    # well before 3.11, so normalise manually.
    ts_str = ts_str.strip()
    # Replace trailing Z with +00:00
    if ts_str.endswith('Z'):
        ts_str = ts_str[:-1] + '+00:00'
    dt = datetime.fromisoformat(ts_str)
    # Convert to local time
    return dt.astimezone(tz=None)


def _format_timestamp(dt: datetime) -> str:
    """Format local datetime as [YYYY-MM-DDTHH:MM] (matching PS1 output)."""
    return dt.strftime('[%Y-%m-%dT%H:%M]')


def _expand_md_links(text: str) -> str:
    """Replace markdown [text](url) with text [url]."""
    return _MD_LINK_RE.sub(lambda m: f'{m.group(1)} [{m.group(2)}]', text)


def _strip_custom_emoji(text: str) -> str:
    """Remove Discord custom emoji tokens like <:pepe:123456789>."""
    return _CUSTOM_EMOJI_RE.sub('', text)


def _clean_whitespace(text: str) -> str:
    """Collapse runs of spaces; allow at most one blank line; strip ends."""
    text = _MULTI_SPACE_RE.sub(' ', text)
    text = _MULTI_BLANK_RE.sub('\n\n', text)
    # Trim trailing whitespace from each line
    lines = [line.rstrip() for line in text.split('\n')]
    return '\n'.join(lines).strip()


def _author_name(author: dict) -> str:
    """Extract display name from DCE author object.

    DCE author fields: id, name, discriminator, nickname, isBot, ...
    'nickname' is the server/DM nickname when set, otherwise falls back to
    'name' (the global Discord username).
    """
    nickname = (author.get('nickname') or '').strip()
    name = (author.get('name') or '').strip()
    return nickname if nickname else name


# ---------------------------------------------------------------------------
# Core conversion
# ---------------------------------------------------------------------------

def convert_dce_json(json_path: Path) -> tuple[str, str]:
    """Convert DCE JSON export to plain-text log string.

    Returns:
        (output_text, first_message_date_str)
        first_message_date_str is 'YYYYMMDD' of the first message (local time),
        or today's date if no messages found.
    """
    with json_path.open(encoding='utf-8') as f:
        data = json.load(f)

    messages = data.get('messages', [])

    # Falls back to today if no message has a parseable timestamp
    first_date_str = datetime.now().strftime('%Y%m%d')
    first_date_captured = False
    output_lines: list[str] = []

    for msg in messages:
        # --- Timestamp ---
        ts_raw = msg.get('timestamp', '')
        if not ts_raw:
            continue
        try:
            local_dt = _parse_timestamp(ts_raw)
        except ValueError:
            # Skip messages with unparseable timestamps
            continue

        if not first_date_captured:
            first_date_str = local_dt.strftime('%Y%m%d')
            first_date_captured = True

        ts_formatted = _format_timestamp(local_dt)

        # --- Content ---
        content = msg.get('content', '') or ''

        # Expand markdown links before any other processing
        content = _expand_md_links(content)

        # Remove custom emoji tokens
        content = _strip_custom_emoji(content)

        # --- Embeds: skip entirely per spec ---
        # Embeds contain images (metlex performance charts etc.) that are
        # irrelevant to the text parser. Omitted intentionally.

        # --- Attachments: skip entirely per spec ---
        # Attachments from the Valhalla signal bot are typically images.
        # TODO: if you ever see non-image attachments (e.g. .txt files) in
        # DCE exports from this channel, consider extracting their content here.

        # --- Skip empty messages ---
        if not content.strip():
            continue

        # --- Author ---
        # PlainTextReader.AUTHOR_PATTERN expects "[timestamp] Author:\n" as the
        # first line of each message block, and filters on 'valhalla' in author.
        # Include author so the existing reader can apply its valhalla filter.
        author = _author_name(msg.get('author', {}))

        # Build message block: header line + body
        message_block = f'{ts_formatted} {author}:\n{content.strip()}'
        output_lines.append(message_block)

    output_text = '\n\n'.join(output_lines)
    output_text = _clean_whitespace(output_text)

    return output_text, first_date_str


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _build_output_path(first_date_str: str, input_dir: Path) -> Path:
    """Build default output path: input/dce_YYYYMMDD_HHMMSS_discord.txt"""
    time_str = datetime.now().strftime('%H%M%S')
    filename = f'dce_{first_date_str}_{time_str}_discord.txt'
    return input_dir / filename


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Convert DiscordChatExporter JSON export to valhalla-fjorge input format.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dce_to_input.py export.json
  python dce_to_input.py export.json --out input/custom.txt

Output goes to input/dce_YYYYMMDD_HHMMSS_discord.txt by default.
YYYYMMDD = date of first message; HHMMSS = current time (for uniqueness).
        """,
    )
    parser.add_argument('json_path', type=Path, help='Path to DCE JSON export file')
    parser.add_argument(
        '--out', '-o', type=Path, default=None,
        help='Output file path (default: input/dce_YYYYMMDD_HHMMSS_discord.txt)'
    )
    args = parser.parse_args()

    if not args.json_path.exists():
        print(f'Error: file not found: {args.json_path}', file=sys.stderr)
        sys.exit(1)

    if not args.json_path.suffix.lower() == '.json':
        print(f'Warning: expected a .json file, got: {args.json_path.suffix}')

    try:
        output_text, first_date_str = convert_dce_json(args.json_path)
    except (json.JSONDecodeError, KeyError) as e:
        print(f'Error parsing DCE JSON: {e}', file=sys.stderr)
        sys.exit(1)

    if not output_text:
        print('Warning: no messages with content found in the export. Output not written.')
        sys.exit(0)

    # Determine output path
    script_dir = Path(__file__).parent
    input_dir = script_dir / 'input'

    if args.out:
        out_path = args.out
        out_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        input_dir.mkdir(parents=True, exist_ok=True)
        out_path = _build_output_path(first_date_str, input_dir)

    out_path.write_text(output_text, encoding='utf-8')
    print(f'Saved to: {out_path}')
    print(f'Messages written: {output_text.count(chr(10) + chr(10)) + 1 if output_text else 0}')


if __name__ == '__main__':
    main()
