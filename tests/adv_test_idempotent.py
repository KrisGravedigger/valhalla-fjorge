"""
Adversarial test: AC-4 happy path — deterministic across repeated runs.
Runs --parse twice, captures stdout of both runs (normalized for timing),
and asserts exit codes and file content are identical.
"""
import subprocess
import sys
import re
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TEST_OUTPUT_DIR = PROJECT_ROOT / "_test_output"
BASELINE_DIR = PROJECT_ROOT / "_baseline_pre_refactor"
HARNESS = PROJECT_ROOT / "tests" / "verify_baseline.py"
PARSE_FILES = ["positions.csv", "summary.csv", "skip_events.csv", "insufficient_balance.csv"]


def run_parse():
    return subprocess.run(
        [sys.executable, str(HARNESS), "--parse"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )


def normalize_output(text):
    """Strip timing/duration lines that legitimately differ between runs."""
    # Remove lines with elapsed time patterns like "0.12s", "elapsed: 1.234"
    lines = text.splitlines()
    normalized = []
    for line in lines:
        # Strip lines that are purely timing-related
        if re.search(r"elapsed|duration|\d+\.\d+s\b|took \d", line, re.IGNORECASE):
            continue
        normalized.append(line)
    return "\n".join(normalized)


def snapshot_outputs():
    """Read all PARSE_FILES from _test_output/ and return dict of filename->content."""
    result = {}
    for fname in PARSE_FILES:
        p = TEST_OUTPUT_DIR / fname
        if p.exists():
            result[fname] = p.read_text(encoding="utf-8")
        else:
            result[fname] = None
    return result


def main():
    if not BASELINE_DIR.exists():
        print("SKIP: baseline not present")
        sys.exit(0)

    # First run
    r1 = run_parse()
    snap1 = snapshot_outputs()
    stdout1 = normalize_output(r1.stdout)
    exit1 = r1.returncode

    # Second run
    r2 = run_parse()
    snap2 = snapshot_outputs()
    stdout2 = normalize_output(r2.stdout)
    exit2 = r2.returncode

    failures = []

    # Check exit codes match
    if exit1 != exit2:
        failures.append(f"Exit codes differ: run1={exit1}, run2={exit2}")

    # Check each file is identical between runs
    for fname in PARSE_FILES:
        c1, c2 = snap1.get(fname), snap2.get(fname)
        if c1 is None and c2 is None:
            continue  # both absent — consistent
        if c1 != c2:
            # Find first differing line
            lines1 = (c1 or "").splitlines()
            lines2 = (c2 or "").splitlines()
            for i, (l1, l2) in enumerate(zip(lines1, lines2), start=1):
                if l1 != l2:
                    failures.append(
                        f"{fname}: line {i} differs between run1 and run2\n"
                        f"  run1: {l1[:120]}\n"
                        f"  run2: {l2[:120]}"
                    )
                    break
            else:
                failures.append(f"{fname}: file lengths differ ({len(lines1)} vs {len(lines2)} lines)")

    if failures:
        print("FAIL: harness is NOT idempotent across two sequential --parse runs")
        for f in failures:
            print(f"  {f}")
        sys.exit(1)
    else:
        print(f"PASS: two sequential --parse runs produced identical outputs (exit code: {exit1})")
        sys.exit(0)


if __name__ == "__main__":
    main()
