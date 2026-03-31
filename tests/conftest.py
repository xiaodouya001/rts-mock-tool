"""Local test bootstrap for mock-client tests."""

from pathlib import Path
import sys


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _REPO_ROOT / "src"
_TS_SRC_ROOT = _REPO_ROOT.parent / "transcribe_service" / "src"

for _path in (_SRC_ROOT, _TS_SRC_ROOT):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))
