"""coverage: mock_tool.kafka_viewer"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from mock_tool import kafka_viewer as kv
from mock_tool.kafka_viewer import KafkaViewer


class _BoomConsumer:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise RuntimeError("boom")


class _ListConsumer:
    def __init__(self, messages):
        self._messages = iter(messages)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._messages)
        except StopIteration as exc:  # pragma: no cover - protocol adapter
            raise StopAsyncIteration from exc


@pytest.mark.asyncio
async def test_consume_loop_logs_context_on_error(monkeypatch):
    viewer = KafkaViewer(
        bootstrap_servers="127.0.0.1:9092",
        topic="topic-a",
        on_error=MagicMock(),
    )
    viewer._consumer = _BoomConsumer()
    viewer._subscribers = {"sid1": AsyncMock()}

    log_exc = MagicMock()
    monkeypatch.setattr(kv.log, "exception", log_exc)

    await viewer._consume_loop()

    log_exc.assert_called_once()
    args, kwargs = log_exc.call_args
    assert args[0] == "Kafka consume loop error"
    assert kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert kwargs["topic"] == "topic-a"
    assert kwargs["subscriber_count"] == 1
    assert kwargs["exc_type"] == "RuntimeError"
    assert kwargs["error"] == "boom"
    viewer._on_error.assert_called_once_with("boom")


@pytest.mark.asyncio
async def test_consume_loop_filters_by_selected_conversation_id():
    viewer = KafkaViewer(
        bootstrap_servers="127.0.0.1:9092",
        topic="topic-a",
        conversation_id="cid-1",
    )
    viewer._consumer = _ListConsumer(
        [
            SimpleNamespace(
                topic="topic-a",
                partition=0,
                offset=10,
                key="cid-1",
                value={"metaData": {"conversationId": "cid-1"}, "payload": {"text": "a"}},
                timestamp=1,
            ),
            SimpleNamespace(
                topic="topic-a",
                partition=0,
                offset=11,
                key="cid-2",
                value={"metaData": {"conversationId": "cid-2"}, "payload": {"text": "b"}},
                timestamp=2,
            ),
        ]
    )
    _, queue = viewer.subscribe()

    await viewer._consume_loop()

    event = queue.get_nowait()
    assert event["key"] == "cid-1"
    with pytest.raises(asyncio.QueueEmpty):
        queue.get_nowait()
