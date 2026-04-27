"""
Adversarial test for doc 018 parser bridge imports.

_run_track_mode still lives in valhalla_parser_v2.py after doc 018, so the
parser must bridge the extracted helper names back into its private namespace.
"""
import ast
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PARSER = PROJECT_ROOT / "valhalla_parser_v2.py"

EXPECTED = {
    "_md_table": ("valhalla.loss_report.tables", "md_table"),
    "_scenario_label": ("valhalla.loss_report.formatters", "scenario_label"),
    "_fmt_sol": ("valhalla.loss_report.formatters", "fmt_sol"),
    "_fmt_pct": ("valhalla.loss_report.formatters", "fmt_pct"),
    "_fmt_mc": ("valhalla.loss_report.formatters", "fmt_mc"),
}


def main():
    tree = ast.parse(PARSER.read_text(encoding="utf-8"))
    found = {}

    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        module = node.module
        for alias in node.names:
            public_name = alias.name
            private_name = alias.asname or alias.name
            if private_name in EXPECTED:
                found[private_name] = (module, public_name)

    failures = []
    for private_name, expected in EXPECTED.items():
        actual = found.get(private_name)
        if actual != expected:
            failures.append(f"{private_name}: expected import {expected[1]} from {expected[0]}, got {actual}")

    if failures:
        print("FAIL: parser bridge imports are missing or wrong")
        for failure in failures:
            print(f"  {failure}")
        sys.exit(1)

    print("PASS: parser bridge imports for track-mode helpers are present")
    sys.exit(0)


if __name__ == "__main__":
    main()
