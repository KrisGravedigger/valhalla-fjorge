"""JSON state management and project-root paths for the pipeline."""

import json
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
STATE_FILE = PROJECT_ROOT / '.dce_state.json'
DCE_PULL = PROJECT_ROOT / 'dce_pull.py'
PARSER = PROJECT_ROOT / 'valhalla_parser_v2.py'
RECALC_PENDING = PROJECT_ROOT / 'tools' / 'recalc_pending.py'


def load_state() -> dict:
    """Wczytaj .dce_state.json. Zwraca pusty dict jesli plik nie istnieje."""
    if not STATE_FILE.exists():
        return {}
    try:
        data = json.loads(STATE_FILE.read_text(encoding='utf-8'))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(last_pull_until: datetime) -> None:
    """Zapisz last_pull_until do .dce_state.json (ISO 8601, timezone-aware)."""
    state = {'last_pull_until': last_pull_until.isoformat()}
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding='utf-8')


def parse_state_dt(state: dict) -> 'datetime | None':
    """Parsuj last_pull_until ze stanu. Zwraca None jesli brak lub blad."""
    raw = state.get('last_pull_until')
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None
