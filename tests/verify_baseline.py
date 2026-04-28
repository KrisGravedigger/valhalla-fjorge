"""
Verification harness for Valhalla Fjorge parser pipeline.

Runs the parser on input/ files (--parse) and/or regenerates reports from
existing positions.csv (--report), then diffs every output file against the
golden snapshot in _baseline_pre_refactor/.

Exit codes:
  0 - all files match
  1 - one or more files differ (regression detected)
  2 - baseline directory missing
  3 - subprocess (parser) returned non-zero
  4 - prerequisite file/dir missing (output/positions.csv, input/ files)
"""

import argparse
import difflib
import io
import shutil
import subprocess
import sys
from pathlib import Path

# Force UTF-8 stdout/stderr on Windows so diff lines with Polish chars print
# without UnicodeEncodeError under cp1250 default codec.
if sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
if sys.stderr.encoding and sys.stderr.encoding.lower() not in ("utf-8", "utf8"):
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Paths — always resolved relative to project root, not cwd
# ---------------------------------------------------------------------------

# Project root is two levels up from this file: tests/verify_baseline.py
PROJECT_ROOT = Path(__file__).resolve().parent.parent

BASELINE_DIR = PROJECT_ROOT / "_baseline_pre_refactor"
TEST_OUTPUT_DIR = PROJECT_ROOT / "_test_output"
OUTPUT_DIR = PROJECT_ROOT / "output"
INPUT_DIR = PROJECT_ROOT / "input"
PARSER_SCRIPT = PROJECT_ROOT / "valhalla_parser_v2.py"

# ---------------------------------------------------------------------------
# Files compared in each mode
# ---------------------------------------------------------------------------

# Files verified after --parse mode
PARSE_FILES = [
    "positions.csv",
    "summary.csv",
    "skip_events.csv",
    "insufficient_balance.csv",
    # address_cache.json excluded: contains RPC-resolved addresses,
    # which are skipped with --skip-rpc so the cache is empty/different
]

# Files verified after --report mode
REPORT_FILES = [
    "loss_analysis.md",
    # wallet_trend.md excluded: contains reference_date derived from most
    # recent position timestamp — shifts as positions.csv accumulates,
    # producing false diffs between baseline capture time and now.
]

# PNG chart files (opt-in via --include-charts)
CHART_FILES = [
    "daily_entries.png",
    "daily_insufficient_balance.png",
    "daily_pnl.png",
    "daily_pnl_breakdown.png",
    "daily_pnl_pct.png",
    "daily_pnl_rolling_3d.png",
    "daily_pnl_rolling_7d.png",
    "daily_rugs.png",
    "daily_winrate.png",
    "filter_impact_p1.png",
    "filter_impact_p2.png",
    "filter_impact_p3.png",
    "filter_impact_p4.png",
    "hourly_utilization.png",
    "portfolio_cumulative.png",
]

MAX_DIFF_LINES = 5  # Max differing lines to show per file

# Per-file regex patterns for lines to strip before diffing.
# These are non-deterministic content (timestamps generated at runtime) that
# would always cause false-positive diffs across days.
import re
VOLATILE_LINE_PATTERNS = {
    "loss_analysis.md": [
        re.compile(r"^# Loss Analysis Report"),
        re.compile(r"^Generated: "),
    ],
}


def _strip_volatile(filename: str, text: str) -> str:
    """Remove non-deterministic lines (e.g., date headers) before diffing."""
    patterns = VOLATILE_LINE_PATTERNS.get(filename)
    if not patterns:
        return text
    kept = []
    for line in text.splitlines(keepends=True):
        if any(p.match(line) for p in patterns):
            continue
        kept.append(line)
    return "".join(kept)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def check_baseline(baseline_dir: Path) -> None:
    """Exit 2 if baseline directory is missing."""
    if not baseline_dir.exists():
        print(
            f"ERROR: baseline snapshot not found at `{baseline_dir}`\n"
            "Create it before running the harness.",
            file=sys.stderr,
        )
        sys.exit(2)


def clear_dir(directory: Path) -> None:
    """Remove and recreate a directory, ensuring no leftover files."""
    if directory.exists():
        shutil.rmtree(directory)
    directory.mkdir(parents=True, exist_ok=True)


def run_parser_subprocess(extra_args: list) -> None:
    """
    Run valhalla_parser_v2.py with given args via the same Python interpreter.
    Print stderr and exit 3 if the parser returns non-zero.
    Always runs with cwd=PROJECT_ROOT so relative paths inside parser work.
    """
    cmd = [sys.executable, str(PARSER_SCRIPT)] + extra_args
    print(f"\nRunning: {' '.join(str(a) for a in cmd)}\n")
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    # Stream stdout so progress is visible
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        print(f"ERROR: parser exited with code {result.returncode}", file=sys.stderr)
        if result.stderr:
            print(result.stderr, file=sys.stderr)
        sys.exit(3)
    # Show stderr warnings even on success (non-fatal)
    if result.stderr:
        print(result.stderr)


def compare_file(
    baseline_dir: Path,
    test_output_dir: Path,
    filename: str,
    binary: bool = False,
) -> list:
    """
    Compare one file between baseline and test_output.

    Returns a list of problem lines (empty list = files match).
    For text files, returns unified diff excerpt.
    For binary files, returns a one-line mismatch notice.
    Reports MISSING if file is absent from test_output but present in baseline.
    """
    baseline_path = baseline_dir / filename
    test_path = test_output_dir / filename

    if not baseline_path.exists():
        # Baseline doesn't have this file — skip silently
        return []

    if not test_path.exists():
        return [f"{filename}: MISSING from test output"]

    if binary:
        baseline_bytes = baseline_path.read_bytes()
        test_bytes = test_path.read_bytes()
        if baseline_bytes == test_bytes:
            return []
        return [f"{filename}: binary files differ ({len(baseline_bytes)} vs {len(test_bytes)} bytes)"]

    # Text comparison — strip volatile lines (timestamps) before diff
    baseline_text = _strip_volatile(filename, baseline_path.read_text(encoding="utf-8", errors="replace"))
    test_text = _strip_volatile(filename, test_path.read_text(encoding="utf-8", errors="replace"))
    baseline_lines = baseline_text.splitlines(keepends=True)
    test_lines = test_text.splitlines(keepends=True)

    if baseline_lines == test_lines:
        return []

    # Produce human-readable diff excerpt
    diff = list(
        difflib.unified_diff(
            baseline_lines,
            test_lines,
            fromfile=f"baseline/{filename}",
            tofile=f"test/{filename}",
        )
    )

    problems = [f"FAIL: {filename}"]
    shown = 0
    line_num = 0
    for diff_line in diff:
        if diff_line.startswith("@@"):
            # Extract line number hint from @@ -L,N +L,N @@ header
            parts = diff_line.split()
            if len(parts) >= 2:
                try:
                    line_num = abs(int(parts[1].split(",")[0]))
                except (ValueError, IndexError):
                    pass
        elif diff_line.startswith("-") and not diff_line.startswith("---"):
            if shown < MAX_DIFF_LINES:
                problems.append(f"  Line ~{line_num} differs:")
                problems.append(f"    baseline: {diff_line[1:].rstrip()}")
                line_num += 1
        elif diff_line.startswith("+") and not diff_line.startswith("+++"):
            if shown < MAX_DIFF_LINES:
                problems.append(f"    test:     {diff_line[1:].rstrip()}")
                shown += 1
            else:
                problems.append(f"  ... (showing first {MAX_DIFF_LINES} differences)")
                break
        else:
            if not diff_line.startswith("---") and not diff_line.startswith("+++"):
                line_num += 1

    return problems


def compare_outputs(
    baseline_dir: Path,
    test_output_dir: Path,
    file_list: list,
    binary: bool = False,
) -> list:
    """
    Compare all files in file_list.
    Returns a flat list of problem strings (one entry per failing file).
    """
    all_problems = []
    for filename in file_list:
        problems = compare_file(baseline_dir, test_output_dir, filename, binary=binary)
        if problems:
            all_problems.extend(problems)
    return all_problems


def print_results(all_problems: list, mode_name: str) -> bool:
    """
    Print comparison results. Returns True if all files matched.
    """
    if not all_problems:
        print(f"[{mode_name}] All files match baseline. OK")
        return True
    for line in all_problems:
        print(line)
    return False


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------

def run_parse_mode(
    baseline_dir: Path,
    test_output_dir: Path,
    include_charts: bool = False,
) -> bool:
    """
    Run parser on input/ files -> _test_output/, then diff against baseline.
    Returns True if all comparisons pass.
    """
    print("=" * 60)
    print("MODE: --parse")
    print("=" * 60)

    clear_dir(test_output_dir)

    # Collect input files explicitly so the parser doesn't depend on cwd
    input_files = sorted(INPUT_DIR.glob("*.txt")) + sorted(INPUT_DIR.glob("*.html"))
    if not input_files:
        print(f"ERROR: no input files found in {INPUT_DIR}", file=sys.stderr)
        sys.exit(4)

    # Seed _test_output/ with baseline state so the parser's merge path runs
    # against the canonical history. The input/ files are a slice already
    # represented in baseline; merge should be a no-op on duplicate events,
    # producing positions.csv identical to baseline. This tests the parse +
    # merge path for determinism without requiring full historical reparse.
    for fname in PARSE_FILES:
        src = baseline_dir / fname
        if src.exists():
            shutil.copy2(src, test_output_dir / fname)
    src_cache = baseline_dir / "address_cache.json"
    if src_cache.exists():
        shutil.copy2(src_cache, test_output_dir / "address_cache.json")

    parser_args = [
        "--skip-rpc",
        "--skip-meteora",
        "--no-archive",
        "--skip-charts",
        "--no-loss-analysis",
        "--no-wallet-trend",
        "--no-clipboard",
        "--output-dir", str(test_output_dir),
    ] + [str(f) for f in input_files]

    run_parser_subprocess(parser_args)

    all_problems = compare_outputs(baseline_dir, test_output_dir, list(PARSE_FILES))

    if include_charts:
        chart_problems = compare_outputs(
            baseline_dir, test_output_dir, CHART_FILES, binary=True
        )
        all_problems.extend(chart_problems)

    return print_results(all_problems, "parse")


def run_report_mode(
    baseline_dir: Path,
    test_output_dir: Path,
    include_charts: bool = False,
) -> bool:
    """
    Copy output/positions.csv -> _test_output/, then regenerate reports,
    then diff reports against baseline. Returns True if all comparisons pass.
    """
    print("=" * 60)
    print("MODE: --report")
    print("=" * 60)

    clear_dir(test_output_dir)

    # Seed _test_output/ with BASELINE files (not output/) so --report mode
    # is deterministic. output/ mutates daily as the user runs main.py;
    # using it would cause spurious diffs whenever positions.csv grows.
    # The baseline snapshot is the canonical fixed input for verification.
    source_csv = baseline_dir / "positions.csv"
    if not source_csv.exists():
        print(
            f"ERROR: {source_csv} not found. Cannot run --report mode.",
            file=sys.stderr,
        )
        sys.exit(4)

    dest_csv = test_output_dir / "positions.csv"
    shutil.copy2(source_csv, dest_csv)
    print(f"Copied {source_csv} -> {dest_csv}")

    # Copy address_cache.json from baseline so source wallet analysis is
    # deterministic (no live RPC calls, no cache drift).
    src_cache = baseline_dir / "address_cache.json"
    if src_cache.exists():
        shutil.copy2(src_cache, test_output_dir / "address_cache.json")
        print(f"Copied {src_cache} -> {test_output_dir / 'address_cache.json'}")

    # Copy .recommendations_state.json from output/ (it lives there, not in
    # baseline) so [new]/[ignored]/[done] tags match baseline-time state.
    # If user has manually marked recommendations since baseline, this can
    # still drift — but it's the best we can do without snapshotting state.
    src_rec_state = OUTPUT_DIR / ".recommendations_state.json"
    if src_rec_state.exists():
        shutil.copy2(src_rec_state, test_output_dir / ".recommendations_state.json")
        print(f"Copied {src_rec_state} -> {test_output_dir / '.recommendations_state.json'}")

    # Copy insufficient_balance.csv from baseline — loss_analysis depends on it.
    src_insuf = baseline_dir / "insufficient_balance.csv"
    if src_insuf.exists():
        shutil.copy2(src_insuf, test_output_dir / "insufficient_balance.csv")
        print(f"Copied {src_insuf} -> {test_output_dir / 'insufficient_balance.csv'}")

    parser_args = [
        "--no-input",
        "--skip-rpc",
        "--skip-meteora",
        "--skip-charts",
        "--no-wallet-trend",
        "--no-clipboard",
        "--output-dir", str(test_output_dir),
    ]

    run_parser_subprocess(parser_args)

    all_problems = compare_outputs(baseline_dir, test_output_dir, list(REPORT_FILES))

    if include_charts:
        chart_problems = compare_outputs(
            baseline_dir, test_output_dir, CHART_FILES, binary=True
        )
        all_problems.extend(chart_problems)

    return print_results(all_problems, "report")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Verification harness: runs parser pipeline and diffs output "
            "against golden baseline in _baseline_pre_refactor/."
        )
    )
    parser.add_argument(
        "--parse",
        action="store_true",
        help="Run parser on input/ files and diff CSVs against baseline.",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Regenerate reports from output/positions.csv and diff against baseline.",
    )
    parser.add_argument(
        "--include-charts",
        action="store_true",
        help=(
            "Include PNG chart binary comparison. Off by default "
            "because matplotlib rendering may not be fully deterministic."
        ),
    )
    args = parser.parse_args()

    # Default: run both modes if neither is specified
    run_parse = args.parse or (not args.parse and not args.report)
    run_report = args.report or (not args.parse and not args.report)

    check_baseline(BASELINE_DIR)

    all_passed = True

    if run_parse:
        passed = run_parse_mode(BASELINE_DIR, TEST_OUTPUT_DIR, args.include_charts)
        all_passed = all_passed and passed

    if run_report:
        passed = run_report_mode(BASELINE_DIR, TEST_OUTPUT_DIR, args.include_charts)
        all_passed = all_passed and passed

    if all_passed:
        print("\nResult: PASS -- all baseline checks green.")
        sys.exit(0)
    else:
        print("\nResult: FAIL -- one or more files differ from baseline.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
