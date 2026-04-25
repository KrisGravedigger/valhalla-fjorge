"""
Adversarial test: AC-3 adversarial case — file in baseline but absent from _test_output/.
Runs --parse, then deletes _test_output/summary.csv, then calls compare_outputs directly.
Expected: compare_outputs reports the file as MISSING (not silently skips it).
"""
import sys
import importlib.util
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
TEST_OUTPUT_DIR = PROJECT_ROOT / "_test_output"
BASELINE_DIR = PROJECT_ROOT / "_baseline_pre_refactor"
HARNESS = PROJECT_ROOT / "tests" / "verify_baseline.py"
SUMMARY_OUT = TEST_OUTPUT_DIR / "summary.csv"


def load_harness():
    spec = importlib.util.spec_from_file_location("verify_baseline", str(HARNESS))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def main():
    # Ensure _test_output exists with a fresh state from a prior --parse run
    # (We rely on adv_test_regression_caught or similar having run first, OR
    # we create a minimal stub for summary.csv — but we need baseline to be present)
    if not BASELINE_DIR.exists():
        print("SKIP: baseline not present")
        sys.exit(0)

    # We test compare_outputs directly, giving it a _test_output/ that lacks summary.csv
    TEST_OUTPUT_DIR.mkdir(exist_ok=True)

    # Remove summary.csv from test output if it exists, to simulate missing file
    if SUMMARY_OUT.exists():
        SUMMARY_OUT.unlink()

    # Load harness
    try:
        mod = load_harness()
    except Exception as e:
        print(f"FAIL: could not import verify_baseline.py: {e}")
        sys.exit(1)

    if not hasattr(mod, "compare_outputs"):
        print("FAIL: verify_baseline.py does not expose compare_outputs() — cannot test")
        sys.exit(1)

    # Ask harness to compare just summary.csv (which is missing from test output)
    try:
        mismatches = mod.compare_outputs(BASELINE_DIR, TEST_OUTPUT_DIR, ["summary.csv"])
    except Exception as e:
        print(f"FAIL: compare_outputs() raised exception on missing file: {e}")
        sys.exit(1)

    if not mismatches:
        print("FAIL: compare_outputs silently skipped summary.csv even though it's missing from _test_output/")
        sys.exit(1)

    # Check the mismatch report says MISSING (per AC-3 language)
    output_text = str(mismatches).upper()
    if "MISSING" in output_text or "NOT FOUND" in output_text or "DOES NOT EXIST" in output_text:
        print(f"PASS: compare_outputs correctly reports MISSING for absent summary.csv. result: {mismatches}")
        sys.exit(0)
    else:
        print(f"FAIL: compare_outputs reported a mismatch but did not use MISSING language. Got: {mismatches}")
        sys.exit(1)


if __name__ == "__main__":
    main()
