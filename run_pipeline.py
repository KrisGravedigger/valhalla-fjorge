#!/usr/bin/env python3
"""
Valhalla full pipeline — non-interactive equivalent of:
  main.py [1] → cli.py [2] → recalc_pending → record_internal_nav

Usage:
  python run_pipeline.py               # normal run
  python run_pipeline.py --skip-pull   # skip Discord pull, run parser + NAV only
  python run_pipeline.py --nav-dry-run # skip snapshot write (prints NAV, no CSV)
"""
import io
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# Force UTF-8 on Windows (same fix as main.py)
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ("utf-8", "utf8"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent
STATE_FILE = ROOT / ".dce_state.json"

_ENV = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}


def _load_last_pull() -> "datetime | None":
    if not STATE_FILE.exists():
        return None
    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        raw = data.get("last_pull_until")
        if not raw:
            return None
        dt = datetime.fromisoformat(raw)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _save_last_pull(dt: datetime) -> None:
    state = {"last_pull_until": dt.isoformat()}
    STATE_FILE.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _run(label: str, cmd: list, **kwargs) -> int:
    print(f"\n{'='*60}")
    print(f"[pipeline] {label}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, env=_ENV, **kwargs)
    rc = result.returncode
    if rc != 0:
        print(f"\n[pipeline] {label} — exit {rc}", file=sys.stderr)
    return rc


def main() -> None:
    args = sys.argv[1:]
    skip_pull = "--skip-pull" in args
    nav_dry_run = "--nav-dry-run" in args

    now = datetime.now(timezone.utc)

    # ── Step 1: Discord pull ──────────────────────────────────────────────────
    if skip_pull:
        print("\n[pipeline] --skip-pull: skipping Discord pull.")
    else:
        last_pull = _load_last_pull()
        if last_pull is None:
            print(
                "[pipeline] ERROR: no pull state found in .dce_state.json.\n"
                "Run main.py manually once to establish the baseline, then use this script.",
                file=sys.stderr,
            )
            sys.exit(1)

        after = last_pull.strftime("%Y-%m-%dT%H:%M")
        before = now.strftime("%Y-%m-%dT%H:%M")
        print(f"\n[pipeline] Pull range: {after} → {before}")

        rc = _run(
            f"Discord pull ({after} → {before})",
            [sys.executable, str(ROOT / "dce_pull.py"), "--after", after, "--before", before],
        )
        if rc != 0:
            print("[pipeline] Pull failed — aborting.", file=sys.stderr)
            sys.exit(rc)

        _save_last_pull(now)

    # ── Step 2: Parse + lpagent cross-check ──────────────────────────────────
    # stdin=DEVNULL → "Retry failed positions? [Y/n]" gets EOFError → auto-N
    rc = _run(
        "Parser + lpagent cross-check",
        [sys.executable, str(ROOT / "valhalla_parser_v2.py"), "--lpagent"],
        stdin=subprocess.DEVNULL,
    )
    if rc != 0:
        print("[pipeline] Parser returned non-zero — continuing with recalc.", file=sys.stderr)

    # ── Step 3: Recalc pending ────────────────────────────────────────────────
    _run(
        "Recalc pending",
        [sys.executable, str(ROOT / "tools" / "recalc_pending.py")],
    )

    # ── Step 4: Internal NAV snapshot ────────────────────────────────────────
    nav_cmd = [sys.executable, str(ROOT / "tools" / "record_internal_nav.py")]
    if nav_dry_run:
        nav_cmd.append("--dry-run")

    rc = _run("Internal NAV snapshot", nav_cmd)
    if rc != 0:
        print("[pipeline] record_internal_nav failed — no snapshot written.", file=sys.stderr)
        sys.exit(rc)

    print("\n[pipeline] All steps complete.")


if __name__ == "__main__":
    main()
