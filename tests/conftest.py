"""Local test bootstrap for mock-tool tests."""

from pathlib import Path
import sys

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[1]
_SRC_ROOT = _REPO_ROOT / "src"

if _SRC_ROOT.exists() and str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from mock_tool import settings as mock_settings


# Tests must not inherit repository-local .env values such as real wss/kafka endpoints.
mock_settings.get_settings.cache_clear()
mock_settings._ENV_FILE = _REPO_ROOT / ".env.pytest-does-not-exist"


@pytest.fixture(autouse=True)
def isolate_mock_tool_env_file():
    mock_settings.get_settings.cache_clear()
    yield
    mock_settings.get_settings.cache_clear()
