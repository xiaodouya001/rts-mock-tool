"""Additional coverage tests for mock_tool.server."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import runpy
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

import mock_tool.server as server_mod
from mock_tool.ws_driver import ScenarioResult


@pytest.fixture(autouse=True)
def isolate_server_globals(monkeypatch):
    monkeypatch.setattr(server_mod, "_sse_queues", [])
    monkeypatch.setattr(server_mod, "_load_stop_event", None)
    monkeypatch.setattr(server_mod, "_load_task", None)
    monkeypatch.setattr(server_mod, "_kafka_viewer", None)
    monkeypatch.setattr(server_mod, "_kafka_forward_task", None)
    server_mod.stats.reset()
    server_mod.stats.load_running = False


def test_sse_put_drop_oldest_replaces_existing_message():
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
    queue.put_nowait("old")

    server_mod._sse_put_drop_oldest(queue, "new")

    assert queue.get_nowait() == "new"


def test_sse_put_drop_oldest_handles_queue_empty_and_loop_exhaustion():
    class _EmptyQueue:
        def put_nowait(self, _payload):
            raise asyncio.QueueFull

        def get_nowait(self):
            raise asyncio.QueueEmpty

    server_mod._sse_put_drop_oldest(_EmptyQueue(), "payload")


def test_broadcast_sse_and_emit_enqueue_messages():
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=2)
    server_mod._sse_queues.append(queue)

    server_mod._broadcast_sse("demo", {"ok": True})
    asyncio.run(server_mod._emit("demo2", {"ok": False}))

    payloads = [queue.get_nowait(), queue.get_nowait()]
    assert "event: demo" in payloads[0]
    assert '"ok": true' in payloads[0]
    assert "event: demo2" in payloads[1]


@pytest.mark.asyncio
async def test_lifespan_cleans_up_running_resources(monkeypatch):
    pusher_started = asyncio.Event()

    async def fake_stats_pusher():
        pusher_started.set()
        await asyncio.Event().wait()

    load_task = asyncio.create_task(asyncio.Event().wait())
    forward_task = asyncio.create_task(asyncio.Event().wait())
    stop_event = asyncio.Event()
    viewer = SimpleNamespace(stop=AsyncMock(return_value=None))
    manager = SimpleNamespace(shutdown=AsyncMock(return_value=None))
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=1)
    server_mod._sse_queues.append(queue)

    monkeypatch.setattr(server_mod, "_stats_pusher", fake_stats_pusher)
    monkeypatch.setattr(server_mod, "_load_stop_event", stop_event)
    monkeypatch.setattr(server_mod, "_load_task", load_task)
    monkeypatch.setattr(server_mod, "_kafka_forward_task", forward_task)
    monkeypatch.setattr(server_mod, "_kafka_viewer", viewer)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    async with server_mod.lifespan(server_mod.app):
        await pusher_started.wait()

    assert stop_event.is_set() is True
    viewer.stop.assert_awaited_once()
    manager.shutdown.assert_awaited_once()
    assert server_mod._sse_queues == []
    with suppress(asyncio.CancelledError):
        await load_task
    with suppress(asyncio.CancelledError):
        await forward_task


def test_index_endpoint_returns_static_html(monkeypatch):
    manager = MagicMock()
    manager.shutdown = AsyncMock(return_value=None)
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with TestClient(server_mod.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert "Mock" in response.text


@pytest.mark.asyncio
async def test_sse_endpoint_yields_stats_payload_when_idle(monkeypatch):
    class _Request:
        def __init__(self):
            self.calls = 0

        async def is_disconnected(self):
            self.calls += 1
            return self.calls > 1

    reset = MagicMock()
    snapshot = MagicMock(return_value={"sent": 0})
    monkeypatch.setattr(server_mod.stats, "reset", reset)
    monkeypatch.setattr(server_mod.stats, "snapshot", snapshot)
    server_mod.stats.load_running = False

    response = await server_mod.sse(_Request())
    payload = await anext(response.body_iterator)

    assert "event: stats" in payload
    reset.assert_called_once()
    snapshot.assert_called()


@pytest.mark.asyncio
async def test_sse_endpoint_yields_keepalive_on_timeout(monkeypatch):
    class _Request:
        def __init__(self):
            self.calls = 0

        async def is_disconnected(self):
            self.calls += 1
            return False if self.calls == 1 else True

    original_wait_for = server_mod.asyncio.wait_for

    async def fake_wait_for(awaitable, timeout):
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(server_mod.asyncio, "wait_for", fake_wait_for)
    server_mod.stats.load_running = True

    response = await server_mod.sse(_Request())
    payload = await anext(response.body_iterator)

    assert payload == ": keepalive\n\n"
    monkeypatch.setattr(server_mod.asyncio, "wait_for", original_wait_for)


@pytest.mark.asyncio
async def test_sse_endpoint_breaks_immediately_when_disconnected():
    class _Request:
        async def is_disconnected(self):
            return True

    response = await server_mod.sse(_Request())
    with pytest.raises(StopAsyncIteration):
        await anext(response.body_iterator)


@pytest.mark.asyncio
async def test_sse_endpoint_finalizer_tolerates_missing_queue_membership(monkeypatch):
    class _Request:
        def __init__(self):
            self.calls = 0

        async def is_disconnected(self):
            self.calls += 1
            return False if self.calls == 1 else True

    async def fake_wait_for(awaitable, timeout):
        if hasattr(awaitable, "close"):
            awaitable.close()
        raise asyncio.TimeoutError

    monkeypatch.setattr(server_mod.asyncio, "wait_for", fake_wait_for)
    server_mod.stats.load_running = True
    response = await server_mod.sse(_Request())
    server_mod._sse_queues.clear()

    assert await anext(response.body_iterator) == ": keepalive\n\n"
    with pytest.raises(StopAsyncIteration):
        await anext(response.body_iterator)


@pytest.mark.asyncio
async def test_run_scenario_handles_unknown_name():
    result = await server_mod.run_scenario(name="NOPE", ws_url="ws://unit-test", n_messages=3)

    assert "Unknown scenario" in result["error"]


@pytest.mark.asyncio
async def test_run_scenario_passes_n_messages_when_supported(monkeypatch):
    broadcasts: list[tuple[str, dict]] = []

    async def scenario(*, ws_url: str, emit, n_messages: int):
        assert ws_url == "ws://unit-test"
        assert n_messages == 7
        await emit("scenario_step", {"from": "scenario"})
        return ScenarioResult(name="N-03", passed=True, steps=[{"ok": True}])

    monkeypatch.setattr(server_mod, "SCENARIOS", {"N-03": scenario})
    monkeypatch.setattr(server_mod, "_broadcast_sse", lambda event_type, data: broadcasts.append((event_type, data)))

    result = await server_mod.run_scenario(name="N-03", ws_url="ws://unit-test", n_messages=7)

    assert result["passed"] is True
    assert [item[0] for item in broadcasts] == ["scenario_start", "scenario_step", "scenario_done"]


@pytest.mark.asyncio
async def test_run_scenario_without_n_messages_uses_default_signature(monkeypatch):
    async def scenario(*, ws_url: str, emit):
        assert ws_url == "ws://unit-test"
        await emit("scenario_step", {"from": "scenario"})
        return ScenarioResult(name="E-04", passed=True, steps=[])

    monkeypatch.setattr(server_mod, "SCENARIOS", {"E-04": scenario})

    result = await server_mod.run_scenario(name="E-04", ws_url="ws://unit-test", n_messages=9)

    assert result["passed"] is True


@pytest.mark.asyncio
async def test_run_all_scenarios_aggregates_results(monkeypatch):
    monkeypatch.setattr(server_mod, "SCENARIOS", {"A": object(), "B": object()})
    monkeypatch.setattr(
        server_mod,
        "run_scenario",
        AsyncMock(side_effect=[{"scenario": "A"}, {"scenario": "B"}]),
    )

    result = await server_mod.run_all_scenarios(ws_url="ws://unit-test", n_messages=2)

    assert result == [{"scenario": "A"}, {"scenario": "B"}]


@pytest.mark.asyncio
async def test_load_start_rejects_when_already_running():
    class _BusyTask:
        def done(self):
            return False

    server_mod._load_task = _BusyTask()

    result = await server_mod.load_start()

    assert result == {"error": "A load test is already running"}


@pytest.mark.asyncio
async def test_load_start_and_stop_manage_task(monkeypatch):
    broadcasts: list[tuple[str, dict]] = []
    started = asyncio.Event()

    async def fake_run_load_test(**kwargs):
        started.set()

    monkeypatch.setattr(server_mod, "_broadcast_sse", lambda event_type, data: broadcasts.append((event_type, data)))
    monkeypatch.setattr(server_mod, "run_load_test", fake_run_load_test)

    result = await server_mod.load_start(
        ws_url="ws://unit-test",
        concurrency=2,
        messages_per_conv=3,
        interval_ms=0,
        ramp_up_ms=10,
    )
    await started.wait()
    await server_mod._load_task

    assert result["status"] == "started"
    assert result["total_sessions"] == 2
    stop_result = await server_mod.load_stop()
    assert stop_result == {"status": "stopping"}
    assert [event for event, _ in broadcasts[:2]] == ["stats", "load_start"]


@pytest.mark.asyncio
async def test_load_stop_sets_existing_stop_event():
    stop_event = asyncio.Event()
    server_mod._load_stop_event = stop_event

    result = await server_mod.load_stop()

    assert result == {"status": "stopping"}
    assert stop_event.is_set() is True


@pytest.mark.asyncio
async def test_load_stop_without_stop_event_returns_stopping():
    server_mod._load_stop_event = None

    result = await server_mod.load_stop()

    assert result == {"status": "stopping"}


@pytest.mark.asyncio
async def test_get_status_returns_snapshot(monkeypatch):
    monkeypatch.setattr(server_mod.stats, "snapshot", MagicMock(return_value={"ack": 1}))

    assert await server_mod.get_status() == {"ack": 1}


@pytest.mark.asyncio
async def test_live_start_and_clear_map_validation_and_conflict(monkeypatch):
    manager = MagicMock()
    manager.start = AsyncMock(side_effect=server_mod.LiveChatValidationError("bad live request"))
    manager.clear = AsyncMock(side_effect=server_mod.LiveChatConflictError("still running"))
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    with pytest.raises(HTTPException) as start_exc:
        await server_mod.live_start(MagicMock())
    with pytest.raises(HTTPException) as clear_exc:
        await server_mod.live_clear()

    assert start_exc.value.status_code == 400
    assert clear_exc.value.status_code == 409


@pytest.mark.asyncio
async def test_live_clear_success_returns_manager_snapshot(monkeypatch):
    manager = MagicMock()
    manager.clear = AsyncMock(return_value={"state": "idle"})
    monkeypatch.setattr(server_mod, "_live_chat_manager", manager)

    assert await server_mod.live_clear() == {"state": "idle"}


@pytest.mark.asyncio
async def test_kafka_start_success_failure_and_stop(monkeypatch):
    broadcasts: list[tuple[str, dict]] = []
    old_forward = asyncio.create_task(asyncio.Event().wait())
    old_viewer = SimpleNamespace(stop=AsyncMock(return_value=None))
    queue: asyncio.Queue[dict] = asyncio.Queue()
    callbacks: list = []

    class _Viewer:
        bootstrap_servers = "127.0.0.1:9092"
        topic = "topic-a"
        conversation_id = "cid-1"

        def __init__(self, *args, **kwargs):
            self.stop = AsyncMock(return_value=None)
            self.start = AsyncMock(return_value=None)
            callbacks.append(kwargs["on_error"])

        def subscribe(self):
            return "sid-1", queue

    monkeypatch.setattr(server_mod, "_broadcast_sse", lambda event_type, data: broadcasts.append((event_type, data)))
    monkeypatch.setattr(server_mod, "_kafka_forward_task", old_forward)
    monkeypatch.setattr(server_mod, "_kafka_viewer", old_viewer)
    monkeypatch.setattr(server_mod, "KafkaViewer", _Viewer)

    result = await server_mod.kafka_start(
        bootstrap="127.0.0.1:9092",
        topic="topic-a",
        conversation_id="cid-1",
    )
    callbacks[0]("broker down")
    queue.put_nowait({"message": "hello"})
    await asyncio.sleep(0)

    assert result["status"] == "kafka_consumer_started"
    assert any(event == "kafka_message" for event, _ in broadcasts)
    assert any(event == "kafka_error" for event, _ in broadcasts)
    stop_result = await server_mod.kafka_stop()
    assert stop_result == {"status": "kafka_consumer_stopped"}
    with suppress(asyncio.CancelledError):
        await old_forward

    class _FailViewer(_Viewer):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.start = AsyncMock(side_effect=RuntimeError("kafka down"))

    monkeypatch.setattr(server_mod, "KafkaViewer", _FailViewer)
    result = await server_mod.kafka_start(
        bootstrap="127.0.0.1:9092",
        topic="topic-a",
        conversation_id="cid-1",
    )

    assert result == {"status": "kafka_consumer_failed", "error": "kafka down"}


@pytest.mark.asyncio
async def test_kafka_conversations_maps_exceptions(monkeypatch):
    monkeypatch.setattr(
        server_mod,
        "scan_topic_conversations",
        AsyncMock(side_effect=RuntimeError("scan failed")),
    )

    result = await server_mod.kafka_conversations(bootstrap="127.0.0.1:9092", topic="topic-a")

    assert result == {"status": "error", "error": "scan failed"}


@pytest.mark.asyncio
async def test_kafka_stop_handles_done_task_and_viewer(monkeypatch):
    done_task = asyncio.create_task(asyncio.sleep(0))
    await done_task
    viewer = SimpleNamespace(stop=AsyncMock(return_value=None))
    monkeypatch.setattr(server_mod, "_kafka_forward_task", done_task)
    monkeypatch.setattr(server_mod, "_kafka_viewer", viewer)

    result = await server_mod.kafka_stop()

    assert result == {"status": "kafka_consumer_stopped"}
    viewer.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_stop_handles_missing_viewer(monkeypatch):
    monkeypatch.setattr(server_mod, "_kafka_forward_task", None)
    monkeypatch.setattr(server_mod, "_kafka_viewer", None)

    result = await server_mod.kafka_stop()

    assert result == {"status": "kafka_consumer_stopped"}


@pytest.mark.asyncio
async def test_kafka_purge_handles_error_non_ok_resume_and_stopped(monkeypatch):
    broadcasts: list[tuple[str, dict]] = []
    queue_waiter = asyncio.create_task(asyncio.Event().wait())
    viewer = SimpleNamespace(
        bootstrap_servers="127.0.0.1:9092",
        topic="topic-a",
        conversation_id="cid-1",
        stop=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(server_mod, "_broadcast_sse", lambda event_type, data: broadcasts.append((event_type, data)))
    monkeypatch.setattr(server_mod, "_kafka_viewer", viewer)
    monkeypatch.setattr(server_mod, "_kafka_forward_task", queue_waiter)

    monkeypatch.setattr(
        server_mod,
        "purge_topic_messages",
        AsyncMock(side_effect=RuntimeError("purge failed")),
    )
    result = await server_mod.kafka_purge("127.0.0.1:9092", "topic-a")
    assert result == {"status": "error", "error": "purge failed"}
    with suppress(asyncio.CancelledError):
        await queue_waiter

    monkeypatch.setattr(server_mod, "_kafka_viewer", None)
    monkeypatch.setattr(server_mod, "_kafka_forward_task", None)
    monkeypatch.setattr(
        server_mod,
        "purge_topic_messages",
        AsyncMock(return_value={"status": "error", "error": "topic missing"}),
    )
    result = await server_mod.kafka_purge("127.0.0.1:9092", "topic-a")
    assert result == {"status": "error", "error": "topic missing"}

    monkeypatch.setattr(
        server_mod,
        "purge_topic_messages",
        AsyncMock(return_value={"status": "ok", "topic": "topic-a"}),
    )
    result = await server_mod.kafka_purge("127.0.0.1:9092", "topic-a", restart_consumer=False)
    assert result["consumer"] == "stopped"

    viewer = SimpleNamespace(
        bootstrap_servers="127.0.0.1:9092",
        topic="topic-a",
        conversation_id="cid-1",
        stop=AsyncMock(return_value=None),
    )
    monkeypatch.setattr(server_mod, "_kafka_viewer", viewer)
    monkeypatch.setattr(
        server_mod,
        "kafka_start",
        AsyncMock(return_value={"status": "kafka_consumer_started", "topic": "topic-a", "conversation_id": "cid-1"}),
    )
    result = await server_mod.kafka_purge("127.0.0.1:9092", "topic-a")

    assert result["purge"]["status"] == "ok"
    assert result["status"] == "kafka_consumer_started"
    assert any(event == "kafka_purged" for event, _ in broadcasts)


@pytest.mark.asyncio
async def test_stats_pusher_emits_stats_then_propagates_cancellation(monkeypatch):
    broadcasts: list[tuple[str, dict]] = []
    original_sleep = server_mod.asyncio.sleep
    counter = {"calls": 0}

    async def fake_sleep(_seconds: float):
        counter["calls"] += 1
        if counter["calls"] > 1:
            raise asyncio.CancelledError

    monkeypatch.setattr(server_mod, "_broadcast_sse", lambda event_type, data: broadcasts.append((event_type, data)))
    monkeypatch.setattr(server_mod.asyncio, "sleep", fake_sleep)

    with pytest.raises(asyncio.CancelledError):
        await server_mod._stats_pusher()

    assert broadcasts[0][0] == "stats"
    monkeypatch.setattr(server_mod.asyncio, "sleep", original_sleep)


def test_main_configures_logging_and_runs_uvicorn(monkeypatch):
    configure_logging = MagicMock()
    uvicorn_run = MagicMock()
    monkeypatch.setattr(server_mod, "configure_logging", configure_logging)
    monkeypatch.setattr(server_mod.uvicorn, "run", uvicorn_run)

    server_mod.main()

    configure_logging.assert_called_once()
    uvicorn_run.assert_called_once()


def test_module_entrypoint_invokes_main(monkeypatch):
    configure_logging = MagicMock()
    uvicorn_run = MagicMock(side_effect=SystemExit(0))
    monkeypatch.setattr(server_mod.uvicorn, "run", uvicorn_run)
    monkeypatch.setattr(server_mod, "configure_logging", configure_logging)
    saved_module = sys.modules.pop("mock_tool.server", None)

    try:
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("mock_tool.server", run_name="__main__")
    finally:
        if saved_module is not None:
            sys.modules["mock_tool.server"] = saved_module

    assert exc_info.value.code == 0
