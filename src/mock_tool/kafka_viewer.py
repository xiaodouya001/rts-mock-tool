"""Kafka consumer helper — subscribe to a topic and broadcast messages to SSE subscribers."""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any, Literal

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import for_code
from aiokafka.structs import TopicPartition

log = logging.getLogger(__name__)


async def _list_topic_partitions_via_admin(
    bootstrap_servers: str,
    topic: str,
    extra: dict[str, Any],
) -> tuple[list[TopicPartition] | None, dict[str, Any] | None]:
    """Return ``(topic_partitions, error_response)`` using Admin ``describe_topics``."""
    admin_meta = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers, **extra)
    await admin_meta.start()
    try:
        infos = await admin_meta.describe_topics([topic])
    finally:
        await admin_meta.close()

    if not infos:
        return None, {
            "status": "error",
            "error": f"Unable to fetch topic metadata (cluster unreachable or request rejected): {topic}",
        }

    meta = infos[0]
    err_code = meta.get("error_code", 0)
    if err_code:
        try:
            err_name = type(for_code(err_code)).__name__
        except Exception:
            err_name = f"error_code={err_code}"
        return None, {
            "status": "error",
            "error": f"Topic unavailable ({err_name}): {topic}",
        }

    partitions = meta.get("partitions") or []
    if not partitions:
        return [], None

    tps = [TopicPartition(topic, p["partition"]) for p in partitions]
    return tps, None


async def _list_topic_partitions_via_consumer(
    bootstrap_servers: str,
    topic: str,
    extra: dict[str, Any],
    *,
    poll_timeout_ms: int = 500,
    max_wait_rounds: int = 200,
) -> tuple[list[TopicPartition] | None, dict[str, Any] | None]:
    """Discover partitions with a plain consumer (subscribe + poll); no Admin metadata."""
    consumer_kw: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "request_timeout_ms": 30_000,
    }
    consumer_kw.update(extra)
    consumer = AIOKafkaConsumer(**consumer_kw)
    await consumer.start()
    try:
        consumer.subscribe([topic])
        assigned: set[TopicPartition] = set()
        for _ in range(max_wait_rounds):
            await consumer.getmany(timeout_ms=poll_timeout_ms)
            assigned = consumer.assignment()
            if assigned:
                break
        if not assigned:
            raw = consumer.partitions_for_topic(topic)
            if raw:
                assigned = {TopicPartition(topic, p) for p in raw}
        if not assigned:
            return None, {
                "status": "error",
                "error": (
                    f"Unable to discover partitions for topic (ensure it exists and is reachable): {topic}"
                ),
            }
        return sorted(assigned, key=lambda tp: tp.partition), None
    finally:
        await consumer.stop()


async def _resolve_topic_partitions(
    bootstrap_servers: str,
    topic: str,
    *,
    connection_extra_kwargs: dict[str, Any] | None,
    kafka_mode: Literal["local", "aws_msk"],
) -> tuple[list[TopicPartition] | None, dict[str, Any] | None]:
    extra = connection_extra_kwargs or {}
    if kafka_mode == "local":
        return await _list_topic_partitions_via_consumer(bootstrap_servers, topic, extra)
    return await _list_topic_partitions_via_admin(bootstrap_servers, topic, extra)


class KafkaViewer:
    """Consume Kafka continuously and fan out messages to registered asyncio queues."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        conversation_id: str | None = None,
        on_error: Any = None,
        *,
        connection_extra_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._conversation_id = conversation_id.strip() if conversation_id else None
        self._connection_extra_kwargs = connection_extra_kwargs or {}
        self._consumer: AIOKafkaConsumer | None = None
        self._subscribers: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._task: asyncio.Task | None = None
        self._on_error = on_error

    @property
    def bootstrap_servers(self) -> str:
        return self._bootstrap

    @property
    def topic(self) -> str:
        return self._topic

    @property
    def conversation_id(self) -> str | None:
        return self._conversation_id

    def subscribe(self) -> tuple[str, asyncio.Queue[dict[str, Any]]]:
        """Register one subscriber and return ``(subscriber_id, queue)``."""
        sid = uuid.uuid4().hex[:8]
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
        self._subscribers[sid] = q
        return sid, q

    def unsubscribe(self, sid: str) -> None:
        self._subscribers.pop(sid, None)

    async def start(self) -> None:
        """Start the Kafka consume loop."""
        consumer_kwargs: dict[str, Any] = {
            "bootstrap_servers": self._bootstrap,
            "group_id": None,
            "auto_offset_reset": "earliest",
            "enable_auto_commit": False,
            "value_deserializer": lambda v: json.loads(v) if v else None,
            "key_deserializer": lambda k: k.decode("utf-8") if k else None,
        }
        consumer_kwargs.update(self._connection_extra_kwargs)
        self._consumer = AIOKafkaConsumer(self._topic, **consumer_kwargs)
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                cid = None
                if isinstance(msg.value, dict):
                    cid = (msg.value.get("metaData") or {}).get("conversationId")
                cid = cid or msg.key
                if self._conversation_id and cid != self._conversation_id:
                    continue
                event = {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key,
                    "value": msg.value,
                    "timestamp": msg.timestamp,
                }
                for sid, q in list(self._subscribers.items()):
                    try:
                        q.put_nowait(event)
                    except asyncio.QueueFull:
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            q.put_nowait(event)
                        except asyncio.QueueFull:
                            pass
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.exception(
                "Kafka consume loop error",
                bootstrap_servers=self._bootstrap,
                topic=self._topic,
                subscriber_count=len(self._subscribers),
                exc_type=type(exc).__name__,
                error=str(exc),
            )
            if self._on_error:
                self._on_error(str(exc))

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None


async def scan_topic_conversations(
    bootstrap_servers: str,
    topic: str,
    *,
    poll_timeout_ms: int = 250,
    max_records: int = 1000,
    connection_extra_kwargs: dict[str, Any] | None = None,
    kafka_mode: Literal["local", "aws_msk"] = "local",
) -> dict[str, Any]:
    """Scan one topic from earliest to end and summarize unique conversationId values."""
    extra = connection_extra_kwargs or {}
    tps, meta_err = await _resolve_topic_partitions(
        bootstrap_servers,
        topic,
        connection_extra_kwargs=extra,
        kafka_mode=kafka_mode,
    )
    if meta_err is not None:
        return meta_err
    assert tps is not None
    if not tps:
        return {
            "status": "ok",
            "topic": topic,
            "conversation_count": 0,
            "conversations": [],
        }
    scan_consumer_kw: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "enable_auto_commit": False,
        "auto_offset_reset": "earliest",
        "value_deserializer": lambda v: json.loads(v) if v else None,
        "key_deserializer": lambda k: k.decode("utf-8") if k else None,
        "request_timeout_ms": 30_000,
    }
    scan_consumer_kw.update(extra)
    consumer = AIOKafkaConsumer(**scan_consumer_kw)
    await consumer.start()
    try:
        consumer.assign(tps)
        end_map = await consumer.end_offsets(tps)
        if all(end_map[tp] <= 0 for tp in tps):
            return {
                "status": "ok",
                "topic": topic,
                "conversation_count": 0,
                "conversations": [],
            }

        counts: dict[str, int] = {}
        while True:
            batch = await consumer.getmany(*tps, timeout_ms=poll_timeout_ms, max_records=max_records)
            for records in batch.values():
                for msg in records:
                    cid = None
                    if isinstance(msg.value, dict):
                        cid = (msg.value.get("metaData") or {}).get("conversationId")
                    cid = cid or msg.key
                    if cid and cid != "-":
                        key = str(cid)
                        counts[key] = counts.get(key, 0) + 1

            positions = await asyncio.gather(*(consumer.position(tp) for tp in tps))
            if all(pos >= end_map[tp] for tp, pos in zip(tps, positions)):
                break
    finally:
        await consumer.stop()

    conversations = [
        {"conversation_id": cid, "message_count": counts[cid]}
        for cid in sorted(counts)
    ]
    return {
        "status": "ok",
        "topic": topic,
        "conversation_count": len(conversations),
        "conversations": conversations,
    }
