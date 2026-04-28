"""
Adversarial test for doc 018 open questions.

_fmt_age_hours, _fmt_age_threshold, and PARAM_LABELS must remain local to
generate_loss_report and must not be hoisted to report_builder module scope.
"""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


def main():
    import valhalla.loss_report.report_builder as report_builder

    forbidden = {"_fmt_age_hours", "_fmt_age_threshold", "PARAM_LABELS"}
    leaked = sorted(name for name in forbidden if name in dir(report_builder))

    if leaked:
        print(f"FAIL: local generate_loss_report helpers leaked to module scope: {leaked}")
        sys.exit(1)

    if not hasattr(report_builder, "generate_loss_report"):
        print("FAIL: report_builder does not expose generate_loss_report")
        sys.exit(1)

    print("PASS: age helpers and PARAM_LABELS are not module-level in report_builder")
    sys.exit(0)


if __name__ == "__main__":
    main()
