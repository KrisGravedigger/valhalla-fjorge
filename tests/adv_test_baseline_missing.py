"""
Adversarial test: AC-2 adversarial case.
Temporarily rename _baseline_pre_refactor/ to simulate it missing.
Expects harness to exit with code 2 (not 1, not 3) and print a clear error.
"""
import subprocess
import sys
import os
import shutil
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
BASELINE_DIR = PROJECT_ROOT / "_baseline_pre_refactor"
BASELINE_BACKUP = PROJECT_ROOT / "_baseline_pre_refactor_adv_backup"
HARNESS = PROJECT_ROOT / "tests" / "verify_baseline.py"

def main():
    if BASELINE_BACKUP.exists():
        print("ERROR: backup dir already exists — previous test run left dirty state")
        sys.exit(1)

    # Rename baseline dir to simulate it missing
    BASELINE_DIR.rename(BASELINE_BACKUP)
    try:
        result = subprocess.run(
            [sys.executable, str(HARNESS), "--parse"],
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
        )
        output = result.stdout + result.stderr

        # Check exit code == 2
        if result.returncode != 2:
            print(f"FAIL: expected exit code 2 (missing baseline), got {result.returncode}")
            print(f"stdout: {result.stdout[:500]}")
            print(f"stderr: {result.stderr[:500]}")
            sys.exit(1)

        # Check that the error message is clear and mentions the baseline dir
        msg_ok = (
            "_baseline_pre_refactor" in output.lower()
            or "baseline" in output.lower()
        )
        if not msg_ok:
            print(f"FAIL: exit code was 2 but message doesn't mention baseline. output: {output[:300]}")
            sys.exit(1)

        print(f"PASS: exit code 2 and message mentions baseline. stderr: {result.stderr.strip()[:200]}")
        sys.exit(0)
    finally:
        # Always restore
        BASELINE_BACKUP.rename(BASELINE_DIR)


if __name__ == "__main__":
    main()
