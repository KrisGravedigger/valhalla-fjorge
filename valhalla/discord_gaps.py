"""
Discord gap reporter — identifies date/time ranges where Discord logs are missing.

Based on lpagent_backfill rows in positions.csv: these are positions that lpagent
discovered but that were never parsed from Discord. Reports which dates/windows
need to be fetched from Discord to fill in the missing metadata.
"""
import csv
from datetime import datetime, date
from typing import Optional

DISCORD_SOURCES = {"meteora", "discord"}


def _parse_dt(s: str) -> Optional[datetime]:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def report_discord_gaps(csv_path: str, silent_if_none: bool = True) -> None:
    """
    Print a summary of date ranges where Discord data is missing,
    grouped by calendar date.

    Args:
        csv_path: Path to positions.csv
        silent_if_none: If True, print nothing when there are no lpagent rows.
    """
    records = []
    try:
        with open(csv_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                dt_open = _parse_dt(row["datetime_open"].strip())
                dt_close = _parse_dt(row["datetime_close"].strip())
                if dt_open is None:
                    continue
                records.append({
                    "dt_open": dt_open,
                    "dt_close": dt_close,
                    "pnl_source": row["pnl_source"].strip(),
                    "token": row["token"].strip(),
                })
    except FileNotFoundError:
        return

    lpagent_recs = [r for r in records if r["pnl_source"] == "lpagent"]
    if not lpagent_recs:
        if not silent_if_none:
            print("No lpagent_backfill rows — no Discord gaps to fill.")
        return

    discord_recs = sorted(
        [r for r in records if r["pnl_source"] in DISCORD_SOURCES],
        key=lambda r: r["dt_open"],
    )

    # Group lpagent rows by calendar date of dt_open
    by_date: dict[date, list] = {}
    for r in lpagent_recs:
        d = r["dt_open"].date()
        by_date.setdefault(d, []).append(r)

    print("\nDiscord gaps (lpagent_backfill rows with missing Discord metadata):")
    print("-" * 65)

    for day in sorted(by_date):
        cluster = by_date[day]
        day_start = min(r["dt_open"] for r in cluster)
        day_end = max(
            r["dt_close"] if r["dt_close"] else r["dt_open"]
            for r in cluster
        )
        tokens = list(dict.fromkeys(
            r["token"].encode("ascii", errors="replace").decode("ascii")
            for r in cluster
        ))
        token_str = ", ".join(tokens[:6]) + ("..." if len(tokens) > 6 else "")

        # Nearest Discord row before and after this day's cluster
        before = [r for r in discord_recs if r["dt_open"] < day_start]
        after = [r for r in discord_recs if r["dt_open"] > day_end]
        nb = before[-1] if before else None
        na = after[0] if after else None

        nb_str = nb["dt_open"].strftime("%H:%M") if nb else "none"
        na_str = na["dt_open"].strftime("%H:%M") if na else "none"
        nb_tok = nb["token"].encode("ascii", errors="replace").decode("ascii") if nb else ""
        na_tok = na["token"].encode("ascii", errors="replace").decode("ascii") if na else ""

        print(f"  {day}  {len(cluster):2d} pos  |  fetch: {day_start.strftime('%H:%M')} - {day_end.strftime('%H:%M')}  |  tokens: {token_str}")
        if nb:
            print(f"    context: last Discord before = {nb_str} ({nb_tok}), next after = {na_str} ({na_tok})")
        print()

    print(f"  Total: {len(lpagent_recs)} missing positions across {len(by_date)} day(s)")
    print("-" * 65)
