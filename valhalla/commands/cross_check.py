import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

from valhalla.lpagent_pipeline import (
    read_watermark as _read_watermark,
    write_watermark as _write_watermark,
    run_cross_check as _run_cross_check,
)
from valhalla.lpagent_client import DEFAULT_WALLET as _LPAGENT_DEFAULT_WALLET, REFRESH_WINDOW_HOURS as _REFRESH_WINDOW_HOURS


def run(args):
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    positions_csv = str(output_dir / "positions.csv")

    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    dates = args.cross_check
    if len(dates) == 0:
        # No dates given: use watermark to yesterday
        watermark = _read_watermark(str(output_dir))
        from_date = (
            datetime.strptime(watermark["min_safe_open_date"], "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
        to_date = yesterday
    elif len(dates) == 1:
        from_date = to_date = dates[0]
    else:
        from_date, to_date = dates[0], dates[1]

    if from_date > to_date:
        print(f"[Cross-check] Nothing to sync: from_date {from_date} > to_date {to_date}")
        return

    print(f"[Cross-check] {from_date} -> {to_date}")
    _cc_wallet = os.environ.get("LPAGENT_WALLET", _LPAGENT_DEFAULT_WALLET)
    try:
        count = _run_cross_check(
            from_date, to_date, positions_csv, str(output_dir), silent_if_empty=False
        )
        _cc_now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        if count > 0:
            _write_watermark(str(output_dir), {
                "wallet": _cc_wallet,
                "min_safe_open_date": to_date,
                "last_full_refresh_at": _cc_now_utc,
                "refresh_window_hours": _REFRESH_WINDOW_HOURS,
            })
            print(f"  Watermark updated to {to_date}")
        else:
            # Also update watermark when sync is clean (avoid re-querying)
            _write_watermark(str(output_dir), {
                "wallet": _cc_wallet,
                "min_safe_open_date": to_date,
                "last_full_refresh_at": _cc_now_utc,
                "refresh_window_hours": _REFRESH_WINDOW_HOURS,
            })
            print(f"  Watermark updated to {to_date}")
    except ValueError as e:
        print(f"[Cross-check] Error: {e}")
