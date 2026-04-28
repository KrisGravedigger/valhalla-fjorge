"""
Adversarial test for doc 018 AC-2 lazy imports.

Blocks valhalla.loss_analyzer and valhalla.utilization before importing
build_action_items from the public loss_report package. If the package eagerly
imports either dependency, import fails here.
"""
import importlib.abc
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

BLOCKED = {"valhalla.loss_analyzer", "valhalla.utilization"}


class Blocker(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname in BLOCKED or any(fullname.startswith(name + ".") for name in BLOCKED):
            raise ImportError(f"blocked eager import of {fullname}")
        return None


def main():
    for name in list(sys.modules):
        if name == "valhalla.loss_report" or name.startswith("valhalla.loss_report."):
            del sys.modules[name]
    for name in BLOCKED:
        sys.modules.pop(name, None)

    blocker = Blocker()
    sys.meta_path.insert(0, blocker)
    try:
        from valhalla.loss_report import build_action_items
    except ImportError as exc:
        print(f"FAIL: public build_action_items import triggered eager dependency import: {exc}")
        sys.exit(1)
    finally:
        sys.meta_path.remove(blocker)

    if not callable(build_action_items):
        print("FAIL: build_action_items imported but is not callable")
        sys.exit(1)

    imported = sorted(name for name in BLOCKED if name in sys.modules)
    if imported:
        print(f"FAIL: blocked modules appeared in sys.modules after import: {imported}")
        sys.exit(1)

    print("PASS: build_action_items public import did not eagerly import blocked dependencies")
    sys.exit(0)


if __name__ == "__main__":
    main()
