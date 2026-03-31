"""Additional coverage tests for mock_tool.kafka_viewer."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from mock_tool import kafka_viewer as kv


class _CancelledConsumer:
    def __aiter__(self):
        return self

    async def __anext__(self):
        raise asyncio.CancelledError


class _SingleMessageConsumer:
    def __init__(self, message):
        self._message = message
        self._seen = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._seen:
            raise StopAsyncIteration
        self._seen = True
        return self._message


class _RetryQueue:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self._put_calls = 0

    def put_nowait(self, event):
        self._put_calls += 1
        if self._put_calls == 1:
            raise asyncio.QueueFull
        self.events.append(event)

    def get_nowait(self):
        return {"old": True}


class _AlwaysFullQueue:
    def __init__(self) -> None:
        self.put_calls = 0

    def put_nowait(self, _event):
        self.put_calls += 1
        raise asyncio.QueueFull

    def get_nowait(self):
        return {"old": True}


class _EmptyThenRetryQueue:
    def __init__(self) -> None:
        self.events: list[dict] = []
        self._put_calls = 0

    def put_nowait(self, event):
        self._put_calls += 1
        if self._put_calls == 1:
            raise asyncio.QueueFull
        self.events.append(event)

    def get_nowait(self):
        raise asyncio.QueueEmpty


class _FakeKafkaConsumer:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.stopped = False
        self.assigned = None
        self.end_map = {}
        self.batch = {}
        self.positions = {}

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def assign(self, tps) -> None:
        self.assigned = list(tps)

    async def end_offsets(self, tps):
        return {tp: self.end_map[tp] for tp in tps}

    async def getmany(self, *tps, timeout_ms: int, max_records: int):
        if isinstance(self.batch, list):
            return self.batch.pop(0)
        return self.batch

    async def position(self, tp):
        return self.positions[tp]


class _FakeAdmin:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.started = False
        self.closed = False
        self.infos = []
        self.low_after = {}

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def describe_topics(self, _topics):
        return self.infos

    async def delete_records(self, _to_delete, timeout_ms: int):
        return self.low_after


@pytest.mark.asyncio
async def test_kafka_viewer_start_and_stop_manage_consumer(monkeypatch):
    created: list[_FakeKafkaConsumer] = []

    def fake_consumer(*args, **kwargs):
        consumer = _FakeKafkaConsumer(*args, **kwargs)
        created.append(consumer)
        return consumer

    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a", "cid-1")
    monkeypatch.setattr(kv, "AIOKafkaConsumer", fake_consumer)
    monkeypatch.setattr(viewer, "_consume_loop", AsyncMock(return_value=None))

    await viewer.start()
    await asyncio.sleep(0)
    await viewer.stop()

    consumer = created[0]
    assert viewer.bootstrap_servers == "127.0.0.1:9092"
    assert viewer.topic == "topic-a"
    assert viewer.conversation_id == "cid-1"
    assert consumer.started is True
    assert consumer.stopped is True
    assert consumer.args[0] == "topic-a"
    assert consumer.kwargs["group_id"] is None


def test_kafka_viewer_subscribe_and_unsubscribe():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")

    sid, queue = viewer.subscribe()
    assert sid in viewer._subscribers
    assert queue is viewer._subscribers[sid]

    viewer.unsubscribe(sid)
    assert sid not in viewer._subscribers


@pytest.mark.asyncio
async def test_consume_loop_ignores_cancelled_error_without_callback():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")
    viewer._consumer = _CancelledConsumer()

    await viewer._consume_loop()


@pytest.mark.asyncio
async def test_consume_loop_retries_after_queue_full():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")
    viewer._consumer = _SingleMessageConsumer(
        SimpleNamespace(
            topic="topic-a",
            partition=0,
            offset=10,
            key="cid-1",
            value={"metaData": {"conversationId": "cid-1"}},
            timestamp=123,
        )
    )
    queue = _RetryQueue()
    viewer._subscribers = {"sid": queue}

    await viewer._consume_loop()

    assert len(queue.events) == 1
    assert queue.events[0]["offset"] == 10


@pytest.mark.asyncio
async def test_consume_loop_drops_message_when_queue_stays_full():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")
    viewer._consumer = _SingleMessageConsumer(
        SimpleNamespace(
            topic="topic-a",
            partition=0,
            offset=10,
            key="cid-1",
            value={"metaData": {"conversationId": "cid-1"}},
            timestamp=123,
        )
    )
    queue = _AlwaysFullQueue()
    viewer._subscribers = {"sid": queue}

    await viewer._consume_loop()

    assert queue.put_calls == 2


@pytest.mark.asyncio
async def test_consume_loop_falls_back_to_key_and_handles_queue_empty():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a", conversation_id="cid-1")
    viewer._consumer = _SingleMessageConsumer(
        SimpleNamespace(
            topic="topic-a",
            partition=0,
            offset=10,
            key="cid-1",
            value={"metaData": {}},
            timestamp=123,
        )
    )
    queue = _EmptyThenRetryQueue()
    viewer._subscribers = {"sid": queue}

    await viewer._consume_loop()

    assert queue.events[0]["key"] == "cid-1"


@pytest.mark.asyncio
async def test_consume_loop_uses_key_when_value_is_not_a_dict():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a", conversation_id="cid-1")
    viewer._consumer = _SingleMessageConsumer(
        SimpleNamespace(
            topic="topic-a",
            partition=0,
            offset=10,
            key="cid-1",
            value="raw-text",
            timestamp=123,
        )
    )
    _, queue = viewer.subscribe()

    await viewer._consume_loop()

    assert queue.get_nowait()["key"] == "cid-1"


@pytest.mark.asyncio
async def test_consume_loop_logs_without_error_callback(monkeypatch):
    class _LocalBoomConsumer:
        def __aiter__(self):
            return self

        async def __anext__(self):
            raise RuntimeError("boom")

    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")
    viewer._consumer = _LocalBoomConsumer()
    monkeypatch.setattr(kv.log, "exception", MagicMock())

    await viewer._consume_loop()


@pytest.mark.asyncio
async def test_stop_handles_cancelled_task_and_missing_consumer():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")
    blocker = asyncio.create_task(asyncio.Event().wait())
    viewer._task = blocker

    await viewer.stop()

    with pytest.raises(asyncio.CancelledError):
        await blocker
    await viewer.stop()


@pytest.mark.asyncio
async def test_stop_handles_consumer_without_task():
    viewer = kv.KafkaViewer("127.0.0.1:9092", "topic-a")
    viewer._consumer = _FakeKafkaConsumer()

    await viewer.stop()

    assert viewer._consumer is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("infos", "error_text"),
    [
        ([], "Unable to fetch topic metadata"),
        ([{"error_code": 42, "partitions": []}], "Topic unavailable"),
    ],
)
async def test_scan_topic_conversations_returns_metadata_errors(monkeypatch, infos, error_text):
    admin = _FakeAdmin()
    admin.infos = infos
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "for_code", lambda _code: RuntimeError())

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a")

    assert result["status"] == "error"
    assert error_text in result["error"]
    assert admin.started is True
    assert admin.closed is True


@pytest.mark.asyncio
async def test_scan_topic_conversations_returns_empty_when_no_partitions(monkeypatch):
    admin = _FakeAdmin()
    admin.infos = [{"error_code": 0, "partitions": []}]
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a")

    assert result == {
        "status": "ok",
        "topic": "topic-a",
        "conversation_count": 0,
        "conversations": [],
    }


@pytest.mark.asyncio
async def test_scan_topic_conversations_returns_empty_when_offsets_are_zero(monkeypatch):
    admin = _FakeAdmin()
    admin.infos = [{"error_code": 0, "partitions": [{"partition": 0}]}]
    consumer = _FakeKafkaConsumer()
    tp = kv.TopicPartition("topic-a", 0)
    consumer.end_map = {tp: 0}
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "AIOKafkaConsumer", lambda **_kwargs: consumer)

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a")

    assert result["conversation_count"] == 0
    assert consumer.started is True
    assert consumer.stopped is True


@pytest.mark.asyncio
async def test_scan_topic_conversations_collects_counts_from_metadata_and_key(monkeypatch):
    admin = _FakeAdmin()
    admin.infos = [{"error_code": 0, "partitions": [{"partition": 0}]}]
    consumer = _FakeKafkaConsumer()
    tp = kv.TopicPartition("topic-a", 0)
    consumer.end_map = {tp: 3}
    consumer.batch = {
        tp: [
            SimpleNamespace(key=None, value={"metaData": {"conversationId": "cid-2"}}),
            SimpleNamespace(key="cid-1", value=None),
            SimpleNamespace(key="-", value={"metaData": {"conversationId": "-"}}),
        ]
    }
    consumer.positions = {tp: 3}
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "AIOKafkaConsumer", lambda **_kwargs: consumer)

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a")

    assert result == {
        "status": "ok",
        "topic": "topic-a",
        "conversation_count": 2,
        "conversations": [
            {"conversation_id": "cid-1", "message_count": 1},
            {"conversation_id": "cid-2", "message_count": 1},
        ],
    }


@pytest.mark.asyncio
async def test_scan_topic_conversations_handles_for_code_failure_and_multiple_batches(monkeypatch):
    admin = _FakeAdmin()
    admin.infos = [{"error_code": 42, "partitions": [{"partition": 0}]}]
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "for_code", MagicMock(side_effect=RuntimeError("boom")))

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a")

    assert "error_code=42" in result["error"]

    admin = _FakeAdmin()
    admin.infos = [{"error_code": 0, "partitions": [{"partition": 0}]}]
    consumer = _FakeKafkaConsumer()
    tp = kv.TopicPartition("topic-a", 0)
    consumer.end_map = {tp: 2}
    consumer.batch = [
        {tp: [SimpleNamespace(key="cid-1", value=None)]},
        {tp: [SimpleNamespace(key="cid-1", value=None)]},
    ]
    consumer.positions = {tp: 1}
    calls = {"count": 0}

    async def position(_tp):
        calls["count"] += 1
        return 1 if calls["count"] == 1 else 2

    consumer.position = position
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "AIOKafkaConsumer", lambda **_kwargs: consumer)

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a")

    assert result["conversation_count"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("infos", "error_text"),
    [
        ([], "Unable to fetch topic metadata"),
        ([{"error_code": 42, "partitions": []}], "Topic unavailable"),
        ([{"error_code": 0, "partitions": []}], "Topic has no partitions"),
    ],
)
async def test_purge_topic_messages_returns_metadata_errors(monkeypatch, infos, error_text):
    admin = _FakeAdmin()
    admin.infos = infos
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "for_code", lambda _code: RuntimeError())

    result = await kv.purge_topic_messages("127.0.0.1:9092", "topic-a")

    assert result["status"] == "error"
    assert error_text in result["error"]


@pytest.mark.asyncio
async def test_purge_topic_messages_reports_already_empty_topic(monkeypatch):
    admin_meta = _FakeAdmin()
    admin_meta.infos = [{"error_code": 0, "partitions": [{"partition": 0}]}]
    consumer = _FakeKafkaConsumer()
    tp = kv.TopicPartition("topic-a", 0)
    consumer.end_map = {tp: 0}

    monkeypatch.setattr(
        kv,
        "AIOKafkaAdminClient",
        lambda **_kwargs: admin_meta,
    )
    monkeypatch.setattr(kv, "AIOKafkaConsumer", lambda **_kwargs: consumer)

    result = await kv.purge_topic_messages("127.0.0.1:9092", "topic-a")

    assert result == {
        "status": "ok",
        "topic": "topic-a",
        "message": "The topic already has no messages to delete",
        "partitions_truncated": 0,
    }


@pytest.mark.asyncio
async def test_purge_topic_messages_deletes_records(monkeypatch):
    admin_meta = _FakeAdmin()
    admin_meta.infos = [{"error_code": 0, "partitions": [{"partition": 0}, {"partition": 1}]}]
    admin_delete = _FakeAdmin()
    consumer = _FakeKafkaConsumer()
    tp0 = kv.TopicPartition("topic-a", 0)
    tp1 = kv.TopicPartition("topic-a", 1)
    consumer.end_map = {tp0: 5, tp1: 0}
    admin_delete.low_after = {tp0: 5}
    admins = [admin_meta, admin_delete]

    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admins.pop(0))
    monkeypatch.setattr(kv, "AIOKafkaConsumer", lambda **_kwargs: consumer)

    result = await kv.purge_topic_messages("127.0.0.1:9092", "topic-a")

    assert result == {
        "status": "ok",
        "topic": "topic-a",
        "message": "Committed messages were deleted with DeleteRecords",
        "partitions_truncated": 1,
        "low_watermark_after": {"0": 5},
    }
    assert consumer.stopped is True


@pytest.mark.asyncio
async def test_purge_topic_messages_handles_for_code_failure(monkeypatch):
    admin = _FakeAdmin()
    admin.infos = [{"error_code": 42, "partitions": [{"partition": 0}]}]
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)
    monkeypatch.setattr(kv, "for_code", MagicMock(side_effect=RuntimeError("boom")))

    result = await kv.purge_topic_messages("127.0.0.1:9092", "topic-a")

    assert "error_code=42" in result["error"]
