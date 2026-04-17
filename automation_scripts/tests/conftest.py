"""Pytest/unittest bootstrap so tests can import agents/* and utils/* modules."""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS_DIR = Path(__file__).resolve().parent
_AUTOMATION_ROOT = _TESTS_DIR.parent

for _sub in ("agents", "utils"):
    _path = _AUTOMATION_ROOT / _sub
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
