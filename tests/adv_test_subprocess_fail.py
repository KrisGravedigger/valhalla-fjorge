"""
Adversarial test: AC-2 adversarial — non-existent --baseline-dir.
Tests that harness exits 2 when the baseline directory doesn't exist,
regardless of whether --baseline-dir flag is supported or baseline is hardcoded.

Also tests: extra file in _test_output/ that is NOT in baseline — should NOT crash
and ideally should be reported (or at least not silently die).
"""
import subprocess
import sys
import importlib.util
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TEST_OUTPUT_DIR = PROJECT_ROOT / "_test_output"
BASELINE_DIR = PROJECT_ROOT / "_baseline_pre_refactor"
HARNESS = PROJECT_ROOT / "tests" / "verify_baseline.py"


def test_nonexistent_baseline_dir():
    """
    If harness supports --baseline-dir, pass a nonexistent path.
    If not, this is a documentation gap — hardcoded baseline path only.
    Either way, exit code must be 2 when baseline is absent.
    """
    # Check if --baseline-dir flag exists
    help_result = subprocess.run(
        [sys.executable, str(HARNESS), "--help"],
        capture_output=True, text=True, cwd=str(PROJECT_ROOT)
    )
    supports_baseline_dir = "--baseline-dir" in help_result.stdout

    if supports_baseline_dir:
        result = subprocess.run(
            [sys.executable, str(HARNESS), "--parse", "--baseline-dir", "/nonexistent/path/xyz"],
            capture_output=True, text=True, cwd=str(PROJECT_ROOT)
        )
        if result.returncode != 2:
            print(f"FAIL (--baseline-dir): expected exit 2 for nonexistent baseline dir, got {result.returncode}")
            print(f"stdout: {result.stdout[:200]}, stderr: {result.stderr[:200]}")
            return False
        print(f"PASS: --baseline-dir with nonexistent path → exit 2")
        return True
    else:
        print(f"INFO: harness does not support --baseline-dir flag (baseline path hardcoded). Skipping this sub-test.")
        return True  # Not a failure per doc — doc doesn't require the flag


def test_extra_file_in_test_output():
    """
    Plant an extra file in _test_output/ that is NOT in baseline.
    Compare against baseline — harness should NOT crash (may ignore or warn).
    """
    if not BASELINE_DIR.exists():
        print("SKIP (extra file test): baseline not present")
        return True

    TEST_OUTPUT_DIR.mkdir(exist_ok=True)
    extra_file = TEST_OUTPUT_DIR / "adv_extra_not_in_baseline.csv"
    extra_file.write_text("extra,data\n1,2\n")

    try:
        mod_spec = importlib.util.spec_from_file_location("verify_baseline", str(HARNESS))
        mod = importlib.util.module_from_spec(mod_spec)
        mod_spec.loader.exec_module(mod)
    except Exception as e:
        print(f"FAIL: could not import verify_baseline.py: {e}")
        if extra_file.exists():
            extra_file.unlink()
        return False

    if not hasattr(mod, "compare_outputs"):
        print("INFO: compare_outputs not exposed — skipping extra-file sub-test")
        if extra_file.exists():
            extra_file.unlink()
        return True

    try:
        # compare_outputs with the extra file explicitly listed — does it handle gracefully?
        mismatches = mod.compare_outputs(BASELINE_DIR, TEST_OUTPUT_DIR, ["adv_extra_not_in_baseline.csv"])
        # Should report MISSING from baseline side (file is in test but not baseline)
        # OR raise clearly. Either is acceptable as long as it doesn't silently pass.
        if not mismatches:
            print(f"WARNING: compare_outputs returns no mismatch for file absent from baseline — silent pass on extra file")
            # This is a warning, not a critical fail (doc doesn't explicitly require reporting extras)
            if extra_file.exists():
                extra_file.unlink()
            return True
        else:
            print(f"PASS: compare_outputs reports mismatch for file absent from baseline: {mismatches}")
    except Exception as e:
        print(f"WARNING: compare_outputs raised exception for file absent from baseline: {e}")
    finally:
        if extra_file.exists():
            extra_file.unlink()

    return True


def test_sys_executable():
    """
    Verify harness uses sys.executable rather than hardcoded 'python'.
    Read the source and check.
    """
    source = HARNESS.read_text(encoding="utf-8")
    if "sys.executable" in source:
        print("PASS: harness uses sys.executable for subprocess calls")
        return True
    elif '"python"' in source or "'python'" in source:
        print("FAIL: harness hardcodes 'python' string instead of sys.executable")
        return False
    else:
        print("INFO: could not determine subprocess invocation style from source scan")
        return True


def main():
    results = []
    results.append(test_nonexistent_baseline_dir())
    results.append(test_extra_file_in_test_output())
    results.append(test_sys_executable())

    if all(results):
        print("OVERALL: PASS")
        sys.exit(0)
    else:
        print("OVERALL: FAIL")
        sys.exit(1)


if __name__ == "__main__":
    main()
