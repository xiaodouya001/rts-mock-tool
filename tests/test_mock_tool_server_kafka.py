"""API tests for mock-client Kafka helper endpoints."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

from mock_tool.server import app
import mock_tool.server as server_mod

UI_PREFIX = server_mod.SETTINGS.url_path_prefix


def test_kafka_conversations_endpoint_returns_inventory(monkeypatch):
    manager = MagicMock()
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)
    monkeypatch.setattr(
        server_mod,
        "scan_topic_conversations",
        AsyncMock(
            return_value={
                "status": "ok",
                "topic": "AI_STAGING_TRANSCRIPTION",
                "conversation_count": 2,
                "conversations": [
                    {"conversation_id": "cid-1", "message_count": 4},
                    {"conversation_id": "cid-2", "message_count": 7},
                ],
            }
        ),
    )

    with TestClient(app) as client:
        resp = client.get(f"{UI_PREFIX}/api/kafka/conversations")

    assert resp.status_code == 200
    assert resp.json()["conversation_count"] == 2
    assert [item["conversation_id"] for item in resp.json()["conversations"]] == ["cid-1", "cid-2"]


def test_kafka_start_endpoint_requires_selected_conversation_id(monkeypatch):
    manager = MagicMock()
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with TestClient(app) as client:
        resp = client.post(f"{UI_PREFIX}/api/kafka/start")

    assert resp.status_code == 422
