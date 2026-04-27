"""
Adversarial smoke test for doc 018 AC-4.

Runs the existing full baseline harness as a subprocess and requires exit 0.
"""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
HARNESS = PROJECT_ROOT / "tests" / "verify_baseline.py"


def main():
    result = subprocess.run(
        [sys.executable, str(HARNESS)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        print(f"FAIL: verify_baseline.py exited {result.returncode}")
        if result.stdout:
            print("stdout:")
            print(result.stdout[-4000:])
        if result.stderr:
            print("stderr:")
            print(result.stderr[-4000:])
        sys.exit(1)

    print("PASS: verify_baseline.py exited 0")
    sys.exit(0)


if __name__ == "__main__":
    main()
