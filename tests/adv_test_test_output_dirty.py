"""
Adversarial test: AC-4 adversarial case.
Plants a garbage file in _test_output/ BEFORE running --parse.
Expects: harness clears _test_output/ before running, so garbage.txt is absent after.
"""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TEST_OUTPUT_DIR = PROJECT_ROOT / "_test_output"
GARBAGE_FILE = TEST_OUTPUT_DIR / "adv_garbage_sentinel.txt"
HARNESS = PROJECT_ROOT / "tests" / "verify_baseline.py"


def main():
    # Ensure test_output dir exists and plant sentinel file
    TEST_OUTPUT_DIR.mkdir(exist_ok=True)
    GARBAGE_FILE.write_text("adversarial garbage — should be removed by harness")

    result = subprocess.run(
        [sys.executable, str(HARNESS), "--parse"],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )

    # Whether parser succeeds or fails is secondary; what matters is garbage is gone
    if GARBAGE_FILE.exists():
        print(f"FAIL: harness did NOT clean _test_output/ before run — adv_garbage_sentinel.txt still present")
        print(f"harness exit code: {result.returncode}")
        print(f"stdout: {result.stdout[:300]}")
        sys.exit(1)

    print(f"PASS: harness cleaned _test_output/ before run (garbage file absent). exit code: {result.returncode}")
    sys.exit(0)


if __name__ == "__main__":
    main()
