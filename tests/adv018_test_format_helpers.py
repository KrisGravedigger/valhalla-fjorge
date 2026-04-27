"""
Adversarial test for doc 018 AC-3 format helpers.

Boundary inputs should not crash fmt_mc, fmt_sol, or fmt_pct. This test
prints suspicious outputs as warnings but fails only on exceptions, because
the design doc promises consistency and Decimal support rather than exact
behavior for every malformed value.
"""
import sys
from decimal import Decimal
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def main():
    from valhalla.loss_report.formatters import fmt_mc, fmt_pct, fmt_sol

    cases = [
        0,
        None,
        Decimal("0"),
        Decimal("0.00001"),
        10**12,
        -1,
        Decimal("-0.00001"),
    ]
    funcs = [fmt_mc, fmt_sol, fmt_pct]
    failures = []
    warnings = []

    for func in funcs:
        for value in cases:
            try:
                rendered = func(value)
            except Exception as exc:
                failures.append(f"{func.__name__}({value!r}) raised {type(exc).__name__}: {exc}")
                continue

            if not isinstance(rendered, str) or rendered == "":
                warnings.append(f"{func.__name__}({value!r}) returned suspicious value {rendered!r}")
            elif value is None and rendered.lower() not in {"n/a", "none", "-"}:
                warnings.append(f"{func.__name__}(None) returned {rendered!r}")

    if warnings:
        print("WARN: silent edge-case outputs:")
        for warning in warnings:
            print(f"  {warning}")

    if failures:
        print("FAIL: format helpers crashed on boundary inputs")
        for failure in failures:
            print(f"  {failure}")
        sys.exit(1)

    print("PASS: format helpers handled boundary inputs without crashing")
    sys.exit(0)


if __name__ == "__main__":
    main()
