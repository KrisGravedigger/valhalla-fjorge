#!/usr/bin/env python3
"""
DCE orchestrator — pull Discord DM export and convert to valhalla-fjorge input.

Reads Discord token and channel ID from .env, calls DiscordChatExporter CLI,
then converts the resulting JSON through dce_to_input.py.

Usage:
    python dce_pull.py --after 2026-04-18
    python dce_pull.py --after 2026-04-18T12:00 --before 2026-04-19
    python dce_pull.py --after 2026-04-01 --keep-json
    python dce_pull.py --after 2026-04-01 --author-prefix ""

.env must contain:
    DCE_TOKEN=your_discord_user_token
    DISCORD_CHANNEL_ID=channel_id_digits
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# .env parser (stdlib only — no python-dotenv dependency)
# ---------------------------------------------------------------------------

def _load_dotenv(env_path: Path) -> dict[str, str]:
    """Parse a .env file into a dict. Supports KEY=VALUE and KEY="VALUE"."""
    result: dict[str, str] = {}
    if not env_path.exists():
        return result
    for line in env_path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        # Skip comments and blank lines
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            continue
        key, _, raw_value = line.partition('=')
        key = key.strip()
        value = raw_value.strip()
        # Strip surrounding quotes (single or double)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        result[key] = value
    return result


# ---------------------------------------------------------------------------
# Timestamp validation
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    '%Y-%m-%dT%H:%M',
    '%Y-%m-%d',
]


def _parse_date_arg(value: str) -> str:
    """Validate date/datetime string and return it as-is for DCE CLI."""
    for fmt in _DATE_FORMATS:
        try:
            datetime.strptime(value, fmt)
            return value
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f'Invalid date format: {value!r}. Expected YYYY-MM-DD or YYYY-MM-DDTHH:MM'
    )


# ---------------------------------------------------------------------------
# .gitignore safety guard
# ---------------------------------------------------------------------------

_GITIGNORE_REQUIRED = [
    '_temp/',
    'DiscordChatExporter.Cli.win-x64/*.json',
    '.env',
]


def _ensure_gitignore(project_root: Path) -> None:
    """Add missing entries to .gitignore if it exists."""
    gitignore_path = project_root / '.gitignore'
    if not gitignore_path.exists():
        return

    existing = gitignore_path.read_text(encoding='utf-8')
    existing_lines = set(existing.splitlines())

    missing = [entry for entry in _GITIGNORE_REQUIRED if entry not in existing_lines]
    if missing:
        additions = '\n'.join(missing)
        separator = '\n' if existing.endswith('\n') else '\n\n'
        gitignore_path.write_text(
            existing + separator + additions + '\n',
            encoding='utf-8',
        )
        print(f'[dce_pull] Added to .gitignore: {missing}')


# ---------------------------------------------------------------------------
# Token masking
# ---------------------------------------------------------------------------

def _mask_token(cmd: list[str], token: str) -> list[str]:
    """Return a copy of cmd with the token replaced by ***."""
    return ['***' if part == token else part for part in cmd]


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def main() -> None:
    project_root = Path(__file__).parent

    # --- Parse CLI args ---
    parser = argparse.ArgumentParser(
        description='Pull Discord DM export via DCE and convert to valhalla-fjorge input.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python dce_pull.py --after 2026-04-18
  python dce_pull.py --after 2026-04-18T06:00 --before 2026-04-19
  python dce_pull.py --after 2026-04-01 --keep-json --author-prefix ""
        """,
    )
    parser.add_argument(
        '--after', required=True, type=_parse_date_arg,
        help='Export messages after this date/datetime (YYYY-MM-DD or YYYY-MM-DDTHH:MM). Required.',
    )
    parser.add_argument(
        '--before', default=None, type=_parse_date_arg,
        help='Export messages before this date/datetime (optional).',
    )
    parser.add_argument(
        '--keep-json', action='store_true',
        help='Keep intermediate DCE JSON file after conversion (default: delete it).',
    )
    parser.add_argument(
        '--author-prefix', default='APL. ',
        help='String prepended to author name in output (default: "APL. "). '
             'Pass empty string to omit.',
    )
    args = parser.parse_args()

    # --- Load .env ---
    env_vars = _load_dotenv(project_root / '.env')
    # Also honour actual environment variables (env overrides .env)
    token = os.environ.get('DCE_TOKEN') or env_vars.get('DCE_TOKEN', '')
    channel_id = os.environ.get('DISCORD_CHANNEL_ID') or env_vars.get('DISCORD_CHANNEL_ID', '')

    if not token:
        print('Error: DCE_TOKEN is not set in .env or environment.', file=sys.stderr)
        sys.exit(1)
    if not channel_id:
        print('Error: DISCORD_CHANNEL_ID is not set in .env or environment.', file=sys.stderr)
        sys.exit(1)

    # --- Safety: update .gitignore ---
    _ensure_gitignore(project_root)

    # --- Prepare temp directory and output path ---
    temp_dir = project_root / '_temp'
    temp_dir.mkdir(parents=True, exist_ok=True)

    timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
    tmp_json = temp_dir / f'dce_{timestamp_str}.json'

    # --- Build DCE command ---
    dce_exe = project_root / 'DiscordChatExporter.Cli.win-x64' / 'DiscordChatExporter.Cli.exe'
    if not dce_exe.exists():
        print(f'Error: DCE executable not found at {dce_exe}', file=sys.stderr)
        sys.exit(1)

    dce_cmd = [
        str(dce_exe),
        'export',
        '-c', channel_id,
        '-t', token,         # token is masked in error output, never logged here
        '--after', args.after,
        '--format', 'Json',
        '-o', str(tmp_json),
    ]
    if args.before:
        dce_cmd.extend(['--before', args.before])

    # Log command with masked token
    safe_cmd = ' '.join(_mask_token(dce_cmd, token))
    print(f'[dce_pull] Running: {safe_cmd}')

    # --- Run DCE ---
    try:
        result = subprocess.run(
            dce_cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
        )
    except FileNotFoundError:
        print(
            f'Error: could not launch DCE. Is the executable present at {dce_exe}?',
            file=sys.stderr,
        )
        sys.exit(1)

    if result.stdout:
        # Print DCE stdout (progress info) — strip any accidental token leak
        safe_stdout = result.stdout.replace(token, '***')
        print(safe_stdout, end='')

    if result.returncode != 0:
        safe_stderr = result.stderr.replace(token, '***') if result.stderr else ''
        print(f'Error: DCE exited with code {result.returncode}.', file=sys.stderr)
        if safe_stderr:
            print(f'DCE stderr: {safe_stderr}', file=sys.stderr)
        sys.exit(result.returncode)

    if not tmp_json.exists():
        print(
            f'Error: DCE reported success but output file not found: {tmp_json}',
            file=sys.stderr,
        )
        sys.exit(1)

    print(f'[dce_pull] DCE export complete: {tmp_json}')

    # --- Convert JSON → txt ---
    converter = project_root / 'dce_to_input.py'
    convert_cmd = [
        sys.executable,
        str(converter),
        str(tmp_json),
        '--author-prefix', args.author_prefix,
    ]
    print(f'[dce_pull] Converting: {" ".join(convert_cmd)}')

    convert_result = subprocess.run(
        convert_cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    if convert_result.stdout:
        print(convert_result.stdout, end='')
    if convert_result.returncode != 0:
        print(f'Error: converter exited with code {convert_result.returncode}.', file=sys.stderr)
        if convert_result.stderr:
            print(convert_result.stderr, file=sys.stderr)
        sys.exit(convert_result.returncode)

    # --- Cleanup JSON ---
    if not args.keep_json:
        tmp_json.unlink(missing_ok=True)
        print(f'[dce_pull] Removed temporary JSON: {tmp_json.name}')
    else:
        print(f'[dce_pull] JSON retained at: {tmp_json}')

    # --- Summary ---
    date_range = args.after
    if args.before:
        date_range += f'..{args.before}'
    else:
        date_range += '..now'
    print(f'\n[dce_pull] Done. Exported messages from channel {channel_id} for range {date_range}.')
    print('[dce_pull] Converted file saved to input/dce_*.txt — ready for valhalla_parser_v2.py.')


if __name__ == '__main__':
    main()
