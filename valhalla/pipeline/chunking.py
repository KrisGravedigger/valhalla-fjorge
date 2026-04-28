"""Datetime formatting, parsing, and day-by-day chunk building."""

from datetime import datetime, timedelta, timezone

CHUNK_THRESHOLD_DAYS = 3
CHUNK_DELAY_MIN_SEC = 32
CHUNK_DELAY_MAX_SEC = 64

_DATE_FORMATS = [
    '%Y-%m-%dT%H:%M',
    '%Y-%m-%d',
]


def fmt_dt(dt: datetime) -> str:
    """Formatuj datetime do czytelnego stringa: YYYY-MM-DD HH:MM."""
    return dt.strftime('%Y-%m-%d %H:%M')


def fmt_ago(dt: datetime, now: datetime) -> str:
    """Czytelna informacja 'X dni temu' / 'X godz. Y min temu'."""
    delta = now - dt
    total_seconds = int(delta.total_seconds())
    if total_seconds < 0:
        return 'w przyszlosci?'
    days = delta.days
    hours, remainder = divmod(total_seconds % 86400, 3600)
    minutes = remainder // 60
    if days > 0:
        return f'{days} dni temu' if days != 1 else '1 dzien temu'
    if hours > 0:
        return f'{hours} godz. {minutes} min temu'
    return f'{minutes} min temu'


def dt_to_dce_str(dt: datetime) -> str:
    """Konwertuj datetime do formatu YYYY-MM-DDTHH:MM wymaganego przez dce_pull.py."""
    return dt.strftime('%Y-%m-%dT%H:%M')


def parse_user_date(raw: str) -> 'datetime | None':
    """Parsuj YYYY-MM-DD lub YYYY-MM-DDTHH:MM -> timezone-aware datetime (local)."""
    raw = raw.strip()
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    for fmt in _DATE_FORMATS:
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=local_tz)
        except ValueError:
            continue
    return None


def yesterday_midnight() -> datetime:
    """Wczoraj o 00:00 w czasie lokalnym (timezone-aware)."""
    local_tz = datetime.now(timezone.utc).astimezone().tzinfo
    today = datetime.now(local_tz).replace(hour=0, minute=0, second=0, microsecond=0)
    return today - timedelta(days=1)


def now_local() -> datetime:
    """Teraz w czasie lokalnym (timezone-aware)."""
    return datetime.now(timezone.utc).astimezone()


def build_chunks(start: datetime, end: datetime) -> list:
    """
    Podziel zakres [start, end] na dobowe chunki w czasie lokalnym.

    Jesli start i end sa w tej samej dobie -> jeden chunk [start, end].
    """
    local_tz = now_local().tzinfo
    start = start.astimezone(local_tz)
    end = end.astimezone(local_tz)

    chunks = []
    cursor = start

    while cursor < end:
        next_midnight = (cursor + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        chunk_end = min(next_midnight, end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end

    return chunks
