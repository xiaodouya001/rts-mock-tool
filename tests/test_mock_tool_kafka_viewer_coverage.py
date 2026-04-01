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
        self.subscribed_topics: list[str] | None = None
        # Partition ids returned after subscribe()+getmany() (local metadata discovery).
        self.metadata_partition_nums: list[int] | None = None
        self.end_map = {}
        self.batch = {}
        self.positions = {}

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def subscribe(self, topics) -> None:
        self.subscribed_topics = list(topics)

    def assignment(self):
        return set(self.assigned) if self.assigned else set()

    def partitions_for_topic(self, _topic):
        return None

    def assign(self, tps) -> None:
        self.assigned = list(tps)

    async def end_offsets(self, tps):
        return {tp: self.end_map[tp] for tp in tps}

    async def getmany(self, *tps, timeout_ms: int = 0, max_records: int | None = None):
        if not tps and self.subscribed_topics is not None:
            topic = self.subscribed_topics[0]
            nums = self.metadata_partition_nums if self.metadata_partition_nums is not None else [0]
            self.assigned = [kv.TopicPartition(topic, p) for p in nums]
            return {}
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

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def describe_topics(self, _topics):
        return self.infos


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

    result = await kv.scan_topic_conversations(
        "127.0.0.1:9092",
        "topic-a",
        kafka_mode="aws_msk",
    )

    assert result["status"] == "error"
    assert error_text in result["error"]
    assert admin.started is True
    assert admin.closed is True


@pytest.mark.asyncio
async def test_scan_topic_conversations_returns_empty_when_no_partitions(monkeypatch):
    admin = _FakeAdmin()
    admin.infos = [{"error_code": 0, "partitions": []}]
    monkeypatch.setattr(kv, "AIOKafkaAdminClient", lambda **_kwargs: admin)

    result = await kv.scan_topic_conversations(
        "127.0.0.1:9092",
        "topic-a",
        kafka_mode="aws_msk",
    )

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

    result = await kv.scan_topic_conversations(
        "127.0.0.1:9092",
        "topic-a",
        kafka_mode="aws_msk",
    )

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

    result = await kv.scan_topic_conversations(
        "127.0.0.1:9092",
        "topic-a",
        kafka_mode="aws_msk",
    )

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

    result = await kv.scan_topic_conversations(
        "127.0.0.1:9092",
        "topic-a",
        kafka_mode="aws_msk",
    )

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

    result = await kv.scan_topic_conversations(
        "127.0.0.1:9092",
        "topic-a",
        kafka_mode="aws_msk",
    )

    assert result["conversation_count"] == 1


@pytest.mark.asyncio
async def test_scan_topic_conversations_local_uses_consumer_not_admin(monkeypatch):
    """local mode: partition list from subscribe/poll; scanning uses a second consumer."""
    created: list[_FakeKafkaConsumer] = []
    tp = kv.TopicPartition("topic-a", 0)

    def factory(**kwargs):
        c = _FakeKafkaConsumer(**kwargs)
        if len(created) == 1:
            c.end_map = {tp: 0}
        created.append(c)
        return c

    def _reject_admin(**_kwargs):
        raise AssertionError("no admin in local scan")

    monkeypatch.setattr(kv, "AIOKafkaAdminClient", _reject_admin)
    monkeypatch.setattr(kv, "AIOKafkaConsumer", factory)

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a", kafka_mode="local")

    assert result["conversation_count"] == 0
    assert len(created) == 2
    meta, scan_c = created[0], created[1]
    assert meta.subscribed_topics == ["topic-a"]
    assert scan_c.assigned == [tp]


@pytest.mark.asyncio
async def test_list_topic_partitions_local_uses_partitions_for_topic_fallback(monkeypatch):
    """Cover consumer path when assignment stays empty until metadata lists partitions."""

    class _LazyMetaConsumer(_FakeKafkaConsumer):
        def partitions_for_topic(self, topic: str):
            return {0} if topic == "topic-a" else None

        async def getmany(self, *tps, timeout_ms: int = 0, max_records: int | None = None):
            if not tps and self.subscribed_topics is not None:
                return {}
            return await super().getmany(*tps, timeout_ms=timeout_ms, max_records=max_records)

    created: list[_FakeKafkaConsumer] = []
    tp = kv.TopicPartition("topic-a", 0)

    def factory(**kwargs):
        c = _LazyMetaConsumer(**kwargs)
        if len(created) == 1:
            c.end_map = {tp: 0}
        created.append(c)
        return c

    def _no_admin(**_k):
        raise AssertionError("admin")

    monkeypatch.setattr(kv, "AIOKafkaAdminClient", _no_admin)
    monkeypatch.setattr(kv, "AIOKafkaConsumer", factory)

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "topic-a", kafka_mode="local")

    assert result["conversation_count"] == 0
    assert len(created) == 2


@pytest.mark.asyncio
async def test_list_topic_partitions_local_errors_when_no_partitions_found(monkeypatch):
    class _NoPartitionsConsumer(_FakeKafkaConsumer):
        async def getmany(self, *tps, timeout_ms: int = 0, max_records: int | None = None):
            if not tps and self.subscribed_topics is not None:
                return {}
            return await super().getmany(*tps, timeout_ms=timeout_ms, max_records=max_records)

        def partitions_for_topic(self, _topic):
            return None

    monkeypatch.setattr(kv, "AIOKafkaConsumer", lambda **kw: _NoPartitionsConsumer(**kw))

    result = await kv.scan_topic_conversations("127.0.0.1:9092", "missing-topic", kafka_mode="local")

    assert result["status"] == "error"
    assert "Unable to discover partitions" in result["error"]
