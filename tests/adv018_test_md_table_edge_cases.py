"""
Adversarial test for doc 018 table extraction.

Exercises md_table with empty inputs, mixed row types, and pipe characters in
cells. Crashes are failures. Unescaped cell pipes are treated as malformed
markdown because they change the column count.
"""
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def pipe_count(line):
    return line.count("|")


def main():
    from valhalla.loss_report.tables import md_table

    failures = []

    try:
        empty_headers = md_table([], [])
        empty_rows = md_table(["A", "B"], [])
        mixed = md_table(["A", "B", "C"], [["x", Decimal("1.25"), None]])
        with_pipes = md_table(["A|bad", "B"], [["x|y", "z"]])
    except Exception as exc:
        print(f"FAIL: md_table crashed on edge-case input: {type(exc).__name__}: {exc}")
        sys.exit(1)

    for label, output in {
        "empty_headers": empty_headers,
        "empty_rows": empty_rows,
        "mixed": mixed,
        "with_pipes": with_pipes,
    }.items():
        if not isinstance(output, str):
            failures.append(f"{label}: returned non-string {type(output).__name__}")

    lines = [line for line in with_pipes.splitlines() if line.strip()]
    if len(lines) >= 3:
        expected = pipe_count(lines[1])
        bad_lines = [(idx + 1, line, pipe_count(line)) for idx, line in enumerate(lines) if pipe_count(line) != expected]
        if bad_lines:
            failures.append(
                "with_pipes: inconsistent markdown column separators: "
                + "; ".join(f"line {idx} has {count} pipes: {line!r}" for idx, line, count in bad_lines)
            )

    if failures:
        print("FAIL: md_table produced malformed edge-case output")
        for failure in failures:
            print(f"  {failure}")
        sys.exit(1)

    print("PASS: md_table handled edge cases without malformed pipe counts")
    sys.exit(0)


if __name__ == "__main__":
    main()
