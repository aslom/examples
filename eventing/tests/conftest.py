"""Make `scripts/` importable so the Phase 1 tooling modules can be unit-tested.

The scripts are deliberately *not* a package — they are run as
`python3 scripts/k8s_deploy.py`, so they import each other by bare module name
(`from proclib import Checks`). Putting the directory on sys.path here gives the
tests the same import surface the scripts see at runtime.
"""
import pathlib
import sys

_SCRIPTS = pathlib.Path(__file__).resolve().parents[1] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
