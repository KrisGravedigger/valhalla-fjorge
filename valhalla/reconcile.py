"""Read-only reconciliation against the known-broken legacy lpagent cache.

This module never writes to positions.csv or lpagent cache state. Runtime output is
limited to reconciliation reports under the requested output directory. Legacy
cache files are processed sequentially by filename date; if the same tokenId is
seen more than once, the later file and later row within that file wins.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Sequence

logger = logging.getLogger(__name__)

_LEGACY_WARNING = (
    "WARNING: lpagent data sourced from legacy daily cache files (known stale/overlapping).\n"
    "Results are approximate. Re-run after sub-project C (client redesign) for authoritative numbers."
)
_CSV_WARNING = "# WARNING: legacy cache, results approximate"


@dataclass(frozen=True)
class _MatchedRow:
    full_address: str
    token: str
    pnl_ours: Decimal | None
    pnl_lpagent: Decimal | None
    diff_sol: Decimal | str
    diff_pct: str


@dataclass(frozen=True)
class _ParsedPnl:
    value: Decimal | None
    issue: str | None = None
    is_error: bool = False


@dataclass(frozen=True)
class _LpAgentOnlyRow:
    token_id: str
    token: str
    opened: str
    pnl_native: str
    hint: str


@dataclass(frozen=True)
class _OursOnlyRow:
    full_address: str
    token: str
    datetime_close: str
    pnl_sol: str
    reason: str


@dataclass(frozen=True)
class _ReconcileResult:
    matched: list[_MatchedRow]
    lpagent_only: list[_LpAgentOnlyRow]
    ours_only: list[_OursOnlyRow]


def _parse_date(value: str, flag_name: str) -> date:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"Invalid {flag_name}: must be YYYY-MM-DD.") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"Invalid {flag_name}: must be YYYY-MM-DD.")
    return parsed


def _validate_date_range(from_date: str, to_date: str) -> tuple[date, date]:
    start = _parse_date(from_date, "--from")
    end = _parse_date(to_date, "--to")
    if start > end:
        raise ValueError("Invalid date range: --from must be on or before --to.")
    return start, end


def _iter_dates(from_date: str, to_date: str) -> list[date]:
    start, end = _validate_date_range(from_date, to_date)
    days: list[date] = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def _date_ranges(dates: list[date]) -> list[str]:
    if not dates:
        return []
    ranges: list[str] = []
    range_start = dates[0]
    previous = dates[0]
    for current in dates[1:]:
        if current == previous + timedelta(days=1):
            previous = current
            continue
        ranges.append(_format_date_range(range_start, previous))
        range_start = current
        previous = current
    ranges.append(_format_date_range(range_start, previous))
    return ranges


def _format_date_range(start: date, end: date) -> str:
    if start == end:
        return start.isoformat()
    return f"{start.isoformat()} through {end.isoformat()}"


def _missing_cache_dates(cache_dir: Path, from_date: str, to_date: str) -> list[date]:
    return [
        day
        for day in _iter_dates(from_date, to_date)
        if not (cache_dir / f"{day.isoformat()}.json").exists()
    ]


def _load_legacy_cache(cache_dir: Path, from_date: str, to_date: str) -> dict[str, dict[str, Any]]:
    """Load daily legacy cache files in [from_date, to_date], deduped by tokenId."""
    if not cache_dir.exists() or not cache_dir.is_dir():
        raise FileNotFoundError(f"Legacy cache directory not found: {cache_dir}")

    positions: dict[str, dict[str, Any]] = {}
    for day in _iter_dates(from_date, to_date):
        path = cache_dir / f"{day.isoformat()}.json"
        if not path.exists():
            continue

        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            logger.warning("Skipping malformed legacy cache file %s: %s", path, exc)
            continue

        if not isinstance(raw, list):
            logger.warning("Skipping legacy cache file %s: expected JSON array", path)
            continue

        for row_index, row in enumerate(raw):
            if not isinstance(row, dict):
                logger.warning("Skipping non-object lpagent row in %s at index %d", path, row_index)
                continue
            token_id = str(row.get("tokenId", "") or "").strip()
            if not token_id:
                logger.warning("Skipping lpagent row with empty tokenId in %s at index %d", path, row_index)
                continue
            positions[token_id] = row

    missing = _missing_cache_dates(cache_dir, from_date, to_date)
    for missing_range in _date_ranges(missing):
        logger.warning(
            "Cache files for %s not found - those days may have incomplete coverage.",
            missing_range,
        )
    return positions


def _load_positions_csv(path: Path) -> dict[str, dict[str, str]]:
    """Load positions.csv keyed by full_address, preferring non-lpagent duplicates."""
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"positions.csv not found: {path}")

    positions: dict[str, dict[str, str]] = {}
    with path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        if not reader.fieldnames or "full_address" not in reader.fieldnames:
            raise ValueError(f"positions.csv missing required full_address column: {path}")
        for row_index, row in enumerate(reader, start=2):
            full_address = (row.get("full_address") or "").strip()
            if not full_address:
                continue
            existing = positions.get(full_address)
            if existing is None:
                positions[full_address] = row
                continue

            logger.warning("Duplicate full_address %s in positions.csv at row %d", full_address, row_index)
            existing_source = (existing.get("pnl_source") or "").strip()
            new_source = (row.get("pnl_source") or "").strip()
            if existing_source == "lpagent" and new_source != "lpagent":
                positions[full_address] = row

    return positions


def _parse_decimal(value: object, field_name: str, row_id: str) -> _ParsedPnl:
    if value is None:
        issue = f"missing {field_name}"
        logging.warning("%s for matched record %s", issue, row_id)
        return _ParsedPnl(value=None, issue=issue)
    if isinstance(value, str) and value.strip() == "":
        issue = f"{field_name} field empty"
        logging.warning("%s for matched record %s", issue, row_id)
        return _ParsedPnl(value=None, issue=issue)

    try:
        return _ParsedPnl(value=Decimal(str(value).strip()))
    except (InvalidOperation, ValueError) as exc:
        issue = f"malformed {field_name}: {value}"
        logging.warning("%s for matched record %s (%s)", issue, row_id, exc)
        return _ParsedPnl(value=None, issue=issue, is_error=True)


def _lpagent_token(row: dict[str, Any]) -> str:
    token0 = row.get("token0Info")
    if isinstance(token0, dict):
        token = token0.get("token_symbol") or token0.get("symbol")
        if token:
            return _clean_text(str(token))
    return _clean_text(str(row.get("token") or row.get("tokenSymbol") or ""))


def _clean_text(value: str) -> str:
    return value.encode("ascii", errors="replace").decode("ascii")


def _lpagent_opened(row: dict[str, Any]) -> str:
    return str(row.get("createdAt") or row.get("datetime_open") or row.get("openedAt") or "")


def _lpagent_pnl_native(row: dict[str, Any]) -> str:
    return str(row.get("pnlNative") or "0")


def _compute_pnl_diff(our_row: dict[str, str], lpagent_row: dict[str, Any]) -> _MatchedRow:
    token_id = str(lpagent_row.get("tokenId", "") or our_row.get("full_address", "")).strip()
    pnl_ours = _parse_decimal(our_row.get("pnl_sol"), "local pnl", token_id)
    pnl_lpagent = _parse_decimal(lpagent_row.get("pnlNative"), "lpagent pnl", token_id)

    if pnl_ours.value is None or pnl_lpagent.value is None:
        error_issue = next(
            (pnl.issue for pnl in (pnl_ours, pnl_lpagent) if pnl.is_error and pnl.issue is not None),
            None,
        )
        error_diff_sol = f"ERROR ({error_issue})" if error_issue else "N/A"
        return _MatchedRow(
            full_address=our_row.get("full_address", "").strip(),
            token=_clean_text(our_row.get("token", "") or _lpagent_token(lpagent_row)),
            pnl_ours=pnl_ours.value,
            pnl_lpagent=pnl_lpagent.value,
            diff_sol=error_diff_sol,
            diff_pct="N/A",
        )

    numeric_diff_sol = pnl_ours.value - pnl_lpagent.value
    if pnl_lpagent.value == 0:
        diff_pct = "N/A"
    else:
        diff_pct_value = (numeric_diff_sol / abs(pnl_lpagent.value)) * Decimal("100")
        diff_pct = f"{diff_pct_value:+.2f}%"

    return _MatchedRow(
        full_address=our_row.get("full_address", "").strip(),
        token=_clean_text(our_row.get("token", "") or _lpagent_token(lpagent_row)),
        pnl_ours=pnl_ours.value,
        pnl_lpagent=pnl_lpagent.value,
        diff_sol=numeric_diff_sol,
        diff_pct=diff_pct,
    )


def _ours_only_reason(row: dict[str, str], from_date: str | None) -> str:
    if not from_date:
        return "not in lpagent cache for this range"
    closed = (row.get("datetime_close") or "").strip()
    if not closed:
        return "not in lpagent cache for this range"
    close_date_text = closed[:10]
    try:
        close_date = _parse_date(close_date_text, "datetime_close")
        start_date = _parse_date(from_date, "--from")
    except ValueError:
        logger.warning("Could not parse datetime_close %r for %s", closed, row.get("full_address", ""))
        return "not in lpagent cache for this range"
    if close_date < start_date:
        return "older than query window"
    return "not in lpagent cache for this range"


def _reconcile(
    lpagent_positions: dict[str, dict[str, Any]],
    our_positions: dict[str, dict[str, str]],
    from_date: str | None = None,
) -> _ReconcileResult:
    """Return matched, lpagent-only, and ours-only categories."""
    matched: list[_MatchedRow] = []
    for token_id in sorted(lpagent_positions.keys() & our_positions.keys()):
        matched.append(_compute_pnl_diff(our_positions[token_id], lpagent_positions[token_id]))

    lpagent_only = [
        _LpAgentOnlyRow(
            token_id=token_id,
            token=_lpagent_token(lpagent_positions[token_id]),
            opened=_lpagent_opened(lpagent_positions[token_id]),
            pnl_native=_lpagent_pnl_native(lpagent_positions[token_id]),
            hint="possible duplicate (cache overlap)",
        )
        for token_id in sorted(lpagent_positions.keys() - our_positions.keys())
    ]

    ours_only = [
        _OursOnlyRow(
            full_address=full_address,
            token=_clean_text(our_positions[full_address].get("token", "")),
            datetime_close=our_positions[full_address].get("datetime_close", ""),
            pnl_sol=our_positions[full_address].get("pnl_sol", ""),
            reason=_ours_only_reason(our_positions[full_address], from_date),
        )
        for full_address in sorted(our_positions.keys() - lpagent_positions.keys())
    ]

    return _ReconcileResult(matched=matched, lpagent_only=lpagent_only, ours_only=ours_only)


def _fmt_decimal(value: Decimal) -> str:
    return f"{value:+.6f}"


def _fmt_optional_decimal(value: Decimal | None) -> str:
    if value is None:
        return "N/A"
    return _fmt_decimal(value)


def _fmt_diff(value: Decimal | str) -> str:
    if isinstance(value, Decimal):
        return _fmt_decimal(value)
    return value


def _matched_numeric_rows(result: _ReconcileResult) -> list[_MatchedRow]:
    return [
        row
        for row in result.matched
        if row.pnl_ours is not None and row.pnl_lpagent is not None and isinstance(row.diff_sol, Decimal)
    ]


def _short(value: str, limit: int = 12) -> str:
    return value[:limit]


def _missing_notice(missing_dates: list[date]) -> str | None:
    ranges = _date_ranges(missing_dates)
    if not ranges:
        return None
    return (
        f"Note: cache files for {', '.join(ranges)} not found - "
        "those days may have incomplete coverage."
    )


def _render_console(
    result: _ReconcileResult,
    from_date: str,
    to_date: str,
    missing_dates: list[date] | None = None,
    include_warning: bool = True,
) -> None:
    if include_warning:
        print(_LEGACY_WARNING)
        print()

    print(f"=== Reconciliation Report: {from_date} -> {to_date} (legacy cache) ===")
    notice = _missing_notice(missing_dates or [])
    if notice:
        print(notice)
        print()
    print(f"Matched:       {len(result.matched)} positions")
    print(f"lpagent-only:  {len(result.lpagent_only)}  (may include duplicates due to cache overlap)")
    print(f"ours-only:     {len(result.ours_only)}")
    print()

    print("--- Matched positions (PnL diff) ---")
    print("full_address | token | pnl_ours | pnl_lpagent | diff_sol | diff_pct")
    for matched_row in result.matched:
        print(
            f"{_short(matched_row.full_address)} | {matched_row.token} | "
            f"{_fmt_optional_decimal(matched_row.pnl_ours)} | {_fmt_optional_decimal(matched_row.pnl_lpagent)} | "
            f"{_fmt_diff(matched_row.diff_sol)} | {matched_row.diff_pct}"
        )
    print()

    print("--- lpagent-only ---")
    print("tokenId | token | opened | pnlNative | hint")
    for lpagent_row in result.lpagent_only:
        print(
            f"{_short(lpagent_row.token_id)} | {lpagent_row.token} | "
            f"{lpagent_row.opened} | {lpagent_row.pnl_native} | {lpagent_row.hint}"
        )
    print()

    print("--- Ours-only ---")
    print("full_address | token | datetime_close | pnl_sol | reason")
    for ours_row in result.ours_only:
        print(
            f"{_short(ours_row.full_address)} | {ours_row.token} | {ours_row.datetime_close} | "
            f"{ours_row.pnl_sol} | {ours_row.reason}"
        )
    print()

    numeric_rows = _matched_numeric_rows(result)
    ours_total = sum((row.pnl_ours for row in numeric_rows if row.pnl_ours is not None), Decimal("0"))
    lpagent_total = sum((row.pnl_lpagent for row in numeric_rows if row.pnl_lpagent is not None), Decimal("0"))
    drift = ours_total - lpagent_total
    print("--- Aggregates ---")
    print(f"Total matched PnL (ours):    {_fmt_decimal(ours_total)} SOL")
    print(f"Total matched PnL (lpagent): {_fmt_decimal(lpagent_total)} SOL")
    print(f"Total drift:                 {_fmt_decimal(drift)} SOL")


def _markdown_table(headers: list[str], rows: list[list[str]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return "\n".join(lines)


def _report_stem(from_date: str, to_date: str) -> str:
    return f"reconciliation_{from_date}_{to_date}"


def _render_markdown(
    result: _ReconcileResult,
    from_date: str,
    to_date: str,
    output_dir: Path,
    missing_dates: list[date] | None = None,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{_report_stem(from_date, to_date)}.md"
    notice = _missing_notice(missing_dates or [])
    lines = [
        "> " + _LEGACY_WARNING.replace("\n", "\n> "),
        "",
        f"# Reconciliation Report: {from_date} -> {to_date} (legacy cache)",
        "",
    ]
    if notice:
        lines.extend([notice, ""])
    lines.extend(
        [
            f"- Matched: {len(result.matched)} positions",
            f"- lpagent-only: {len(result.lpagent_only)} (may include duplicates due to cache overlap)",
            f"- ours-only: {len(result.ours_only)}",
            "",
            "## Matched positions (PnL diff)",
            _markdown_table(
                ["full_address", "token", "pnl_ours", "pnl_lpagent", "diff_sol", "diff_pct"],
                [
                    [
                        row.full_address,
                        row.token,
                        _fmt_optional_decimal(row.pnl_ours),
                        _fmt_optional_decimal(row.pnl_lpagent),
                        _fmt_diff(row.diff_sol),
                        row.diff_pct,
                    ]
                    for row in result.matched
                ],
            ),
            "",
            "## lpagent-only",
            _markdown_table(
                ["tokenId", "token", "opened", "pnlNative", "hint"],
                [[row.token_id, row.token, row.opened, row.pnl_native, row.hint] for row in result.lpagent_only],
            ),
            "",
            "## Ours-only",
            _markdown_table(
                ["full_address", "token", "datetime_close", "pnl_sol", "reason"],
                [
                    [row.full_address, row.token, row.datetime_close, row.pnl_sol, row.reason]
                    for row in result.ours_only
                ],
            ),
            "",
            "## Aggregates",
        ]
    )
    numeric_rows = _matched_numeric_rows(result)
    ours_total = sum((row.pnl_ours for row in numeric_rows if row.pnl_ours is not None), Decimal("0"))
    lpagent_total = sum((row.pnl_lpagent for row in numeric_rows if row.pnl_lpagent is not None), Decimal("0"))
    drift = ours_total - lpagent_total
    lines.extend(
        [
            f"- Total matched PnL (ours): {_fmt_decimal(ours_total)} SOL",
            f"- Total matched PnL (lpagent): {_fmt_decimal(lpagent_total)} SOL",
            f"- Total drift: {_fmt_decimal(drift)} SOL",
            "",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def _write_csv(path: Path, headers: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        fh.write(_CSV_WARNING + "\n")
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _render_csvs(result: _ReconcileResult, from_date: str, to_date: str, output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = _report_stem(from_date, to_date)
    matched_path = output_dir / f"{stem}_matched.csv"
    lpagent_path = output_dir / f"{stem}_lpagent_only.csv"
    ours_path = output_dir / f"{stem}_ours_only.csv"

    _write_csv(
        matched_path,
        ["full_address", "token", "pnl_ours", "pnl_lpagent", "diff_sol", "diff_pct"],
        [
            {
                "full_address": row.full_address,
                "token": row.token,
                "pnl_ours": _fmt_optional_decimal(row.pnl_ours),
                "pnl_lpagent": _fmt_optional_decimal(row.pnl_lpagent),
                "diff_sol": _fmt_diff(row.diff_sol),
                "diff_pct": row.diff_pct,
            }
            for row in result.matched
        ],
    )
    _write_csv(
        lpagent_path,
        ["tokenId", "token", "opened", "pnlNative", "hint"],
        [
            {
                "tokenId": row.token_id,
                "token": row.token,
                "opened": row.opened,
                "pnlNative": row.pnl_native,
                "hint": row.hint,
            }
            for row in result.lpagent_only
        ],
    )
    _write_csv(
        ours_path,
        ["full_address", "token", "datetime_close", "pnl_sol", "reason"],
        [
            {
                "full_address": row.full_address,
                "token": row.token,
                "datetime_close": row.datetime_close,
                "pnl_sol": row.pnl_sol,
                "reason": row.reason,
            }
            for row in result.ours_only
        ],
    )
    return [matched_path, lpagent_path, ours_path]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Reconcile positions.csv against lpagent cache.")
    parser.add_argument("--from", dest="from_date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, metavar="YYYY-MM-DD")
    parser.add_argument("--legacy-cache", action="store_true")
    parser.add_argument("--output-dir", default="output")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")
    parser = _build_parser()
    args = parser.parse_args(argv)

    if not args.legacy_cache:
        print(
            "Error: --legacy-cache is required. Non-legacy mode (sub-project D) is not yet implemented.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        _validate_date_range(args.from_date, args.to_date)
    except ValueError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    print(_LEGACY_WARNING)
    print()

    output_dir = Path(args.output_dir)
    cache_dir = output_dir / "lpagent_cache"
    positions_path = output_dir / "positions.csv"
    try:
        missing_dates = _missing_cache_dates(cache_dir, args.from_date, args.to_date)
        lpagent_positions = _load_legacy_cache(cache_dir, args.from_date, args.to_date)
        our_positions = _load_positions_csv(positions_path)
    except (FileNotFoundError, ValueError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)

    result = _reconcile(lpagent_positions, our_positions, args.from_date)
    _render_console(result, args.from_date, args.to_date, missing_dates, include_warning=False)
    try:
        markdown_path = _render_markdown(result, args.from_date, args.to_date, output_dir, missing_dates)
        csv_paths = _render_csvs(result, args.from_date, args.to_date, output_dir)
    except OSError as error:
        print(f"Error: failed to write report ({type(error).__name__}: {error})", file=sys.stderr)
        sys.exit(1)

    print()
    print(f"Markdown report: {markdown_path}")
    for path in csv_paths:
        print(f"CSV report:      {path}")


if __name__ == "__main__":
    main()
