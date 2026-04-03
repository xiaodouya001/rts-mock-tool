"""API tests for Mock Live-Chat endpoints."""

from __future__ import annotations

from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from mock_tool.live_chat import LiveChatConflictError, LiveChatValidationError
from mock_tool import settings as mock_settings
from mock_tool.server import app
import mock_tool.server as server_mod

UI_PREFIX = server_mod.SETTINGS.url_path_prefix


@pytest.fixture(autouse=True)
def isolate_server_settings(monkeypatch):
    monkeypatch.setattr(server_mod, "SETTINGS", mock_settings.get_settings())


def test_live_preview_endpoint_returns_preview(monkeypatch):
    preview = {
        "csv_filename": "chat.csv",
        "row_count": 1,
        "preview_row_count": 1,
        "preview_is_full": True,
        "recognized_columns": {"speaker": "speaker", "transcript": "transcript", "delay_ms": None},
        "sample_rows": [{"line_number": 2, "speaker": "Agent", "transcript": "hello", "delay_ms": None}],
    }
    manager = MagicMock()
    manager.preview_csv.return_value = preview
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with TestClient(app) as client:
        resp = client.post(
            f"{UI_PREFIX}/api/live/preview",
            json={"csv_text": "speaker,transcript\nAgent,hello\n", "csv_filename": "chat.csv"},
        )

    assert resp.status_code == 200
    assert resp.json() == preview


def test_live_preview_endpoint_maps_validation_error(monkeypatch):
    manager = MagicMock()
    manager.preview_csv.side_effect = LiveChatValidationError("bad csv")
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with TestClient(app) as client:
        resp = client.post(
            f"{UI_PREFIX}/api/live/preview",
            json={"csv_text": "speaker,transcript\n", "csv_filename": "chat.csv"},
        )

    assert resp.status_code == 400
    assert resp.json()["detail"] == "bad csv"


def test_live_start_endpoint_maps_conflict(monkeypatch):
    manager = MagicMock()
    manager.start = AsyncMock(side_effect=LiveChatConflictError("already running"))
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with TestClient(app) as client:
        resp = client.post(
            f"{UI_PREFIX}/api/live/start",
            json={
                "csv_text": "speaker,transcript\nAgent,hello\n",
                "csv_filename": "chat.csv",
                "conversation_id": "cid-1",
                "chars_per_second": 18,
                "pace_jitter_pct": 0.15,
            },
        )

    assert resp.status_code == 409
    assert resp.json()["detail"] == "already running"


def test_ui_config_endpoint_returns_kafka_fields(monkeypatch):
    monkeypatch.setattr(
        server_mod,
        "SETTINGS",
        replace(
            server_mod.SETTINGS,
            show_mock_live_chat=True,
            show_scenario_tests=True,
            show_concurrent_load_test=True,
        ),
    )
    with TestClient(app) as client:
        resp = client.get(f"{UI_PREFIX}/api/ui-config")
    assert resp.status_code == 200
    data = resp.json()
    assert "kafka_bootstrap" in data
    assert "kafka_topic" in data
    assert "kafka_mode" in data
    assert "ws_url" in data
    assert data["url_path_prefix"] == UI_PREFIX
    assert data["show_mock_live_chat"] is True
    assert data["show_scenario_tests"] is True
    assert data["show_concurrent_load_test"] is True
    assert isinstance(data["ws_url"], str) and data["ws_url"].startswith("ws://")


def test_ui_config_endpoint_returns_ui_visibility_flags(monkeypatch):
    monkeypatch.setattr(
        server_mod,
        "SETTINGS",
        replace(
            server_mod.SETTINGS,
            show_mock_live_chat=False,
            show_scenario_tests=True,
            show_concurrent_load_test=False,
        ),
    )

    with TestClient(app) as client:
        resp = client.get(f"{UI_PREFIX}/api/ui-config")

    assert resp.status_code == 200
    assert resp.json()["show_mock_live_chat"] is False
    assert resp.json()["show_scenario_tests"] is True
    assert resp.json()["show_concurrent_load_test"] is False


def test_live_status_and_stop_endpoints_delegate_to_manager(monkeypatch):
    snapshot = {"state": "completed", "history": [], "status_notes": []}
    manager = MagicMock()
    manager.snapshot.return_value = snapshot
    manager.stop = AsyncMock(return_value=snapshot)
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with TestClient(app) as client:
        status_resp = client.get(f"{UI_PREFIX}/api/live/status")
        stop_resp = client.post(f"{UI_PREFIX}/api/live/stop")

    assert status_resp.status_code == 200
    assert status_resp.json() == snapshot
    assert stop_resp.status_code == 200
    assert stop_resp.json() == snapshot
