"""Additional coverage tests for mock_tool.live_chat."""

from __future__ import annotations

import asyncio
import runpy
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import mock_tool.live_chat as live_chat


class _IdleKafkaConsumer:
    instances: list["_IdleKafkaConsumer"] = []

    def __init__(self, *_args, **_kwargs) -> None:
        self.queue: asyncio.Queue[object] = asyncio.Queue()
        self.started = False
        self.stopped = False
        type(self).instances.append(self)

    async def start(self) -> None:
        self.started = True

    async def stop(self) -> None:
        self.stopped = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self.queue.get()
        if item is StopAsyncIteration:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item


def test_preview_payload_without_limit_and_state_without_history():
    row = live_chat.LiveChatRow(line_number=2, speaker="Agent", transcript="hello", delay_ms=1.5)
    parsed = live_chat.ParsedLiveChatCsv(
        filename="chat.csv",
        rows=[row],
        recognized_columns={"speaker": "speaker", "transcript": "transcript", "delay_ms": None},
    )
    note = live_chat.LiveChatStatusNote(
        note_id="note-1",
        at="2026-03-31T00:00:00.000Z",
        message="ok",
        level="info",
    )
    state = live_chat.LiveChatSessionState(history=[row.as_dict()], status_notes=[note])

    assert parsed.preview_payload(sample_limit=None)["preview_is_full"] is True
    assert state.as_dict(include_history=False)["status_notes"] == [note.as_dict()]
    assert "history" not in state.as_dict(include_history=False)


def test_parse_live_chat_csv_validation_branches():
    with pytest.raises(live_chat.LiveChatValidationError, match="header row"):
        live_chat.parse_live_chat_csv("", "empty.csv")

    with pytest.raises(live_chat.LiveChatValidationError, match="speaker column"):
        live_chat.parse_live_chat_csv("transcript\nhello\n", "no-speaker.csv")

    with pytest.raises(live_chat.LiveChatValidationError, match="transcript column"):
        live_chat.parse_live_chat_csv("speaker\nAgent\n", "no-transcript.csv")

    with pytest.raises(live_chat.LiveChatValidationError, match="transcript must not be empty"):
        live_chat.parse_live_chat_csv("speaker,transcript\nAgent,   \n", "blank-text.csv")

    with pytest.raises(live_chat.LiveChatValidationError, match="at least one non-empty data row"):
        live_chat.parse_live_chat_csv("speaker,transcript\n,\n", "blank-row.csv")


def test_parse_live_chat_csv_skips_none_rows_and_blank_rows(monkeypatch):
    class _Reader:
        fieldnames = ["speaker", "transcript"]

        def __init__(self):
            self.line_num = 1
            self._rows = iter(
                [
                    None,
                    {"speaker": " ", "transcript": " "},
                    {"speaker": "Agent", "transcript": "hello"},
                ]
            )

        def __iter__(self):
            return self

        def __next__(self):
            row = next(self._rows)
            self.line_num += 1
            return row

    monkeypatch.setattr(live_chat.csv, "DictReader", lambda *_args, **_kwargs: _Reader())

    parsed = live_chat.parse_live_chat_csv("ignored", "custom.csv")

    assert [row.transcript for row in parsed.rows] == ["hello"]


def test_parse_live_chat_csv_ignores_duplicate_normalized_headers():
    parsed = live_chat.parse_live_chat_csv(
        "speaker,Speaker,transcript\nAgent,Ignored,hello\n",
        "dup.csv",
    )

    assert parsed.recognized_columns["speaker"] == "speaker"


def test_delay_runtime_and_response_helpers():
    assert live_chat._normalize_header(" Speaker Roles ") == "speakerroles"
    assert live_chat._canonical_speaker("speaker_1;agent", line_number=2) == "Agent"
    assert live_chat._canonical_speaker("prefix|customer", line_number=2) == "Customer"
    assert live_chat._parse_delay("", line_number=2) is None
    with pytest.raises(live_chat.LiveChatValidationError, match="must be a number"):
        live_chat._parse_delay("abc", line_number=2)
    with pytest.raises(live_chat.LiveChatValidationError, match="greater than or equal to 0"):
        live_chat._parse_delay("-1", line_number=2)
    with pytest.raises(live_chat.LiveChatValidationError, match="must not be empty"):
        live_chat._require_runtime_value("ws_url", "   ")
    assert live_chat._response_error_detail(None) == "No server response received"
    assert "ERROR [E1001]" in live_chat._response_error_detail(
        {"metaData": {"eventType": "ERROR"}, "error": {"code": "E1001", "message": "bad", "details": "detail"}}
    )
    assert live_chat._response_error_detail({"metaData": {"eventType": "ACK"}}) == (
        "Expected ACK but received eventType='ACK'"
    )


def test_message_builder_and_pace_delay(monkeypatch):
    ongoing = live_chat._build_live_chat_message(
        conversation_id="cid-1",
        sequence_number=1,
        event_type="SESSION_ONGOING",
        start_ts="2026-03-31T00:00:00.000Z",
        speaker="Agent",
        transcript="hello",
    )
    complete = live_chat._build_live_chat_message(
        conversation_id="cid-1",
        sequence_number=2,
        event_type="SESSION_COMPLETE",
        start_ts="2026-03-31T00:00:00.000Z",
        speaker="System",
        transcript="EOL",
    )
    assert "speakTimeStamp" in ongoing["payload"]
    assert complete["metaData"]["callEndTimeStamp"] is not None

    row = live_chat.LiveChatRow(line_number=2, speaker="Agent", transcript="hello", delay_ms=250)
    assert live_chat._pace_delay_seconds(
        row,
        chars_per_second=10,
        jitter_pct=0.5,
        rng=SimpleNamespace(uniform=lambda _a, _b: 0.2),
    ) == 0.25

    row = live_chat.LiveChatRow(line_number=2, speaker="Agent", transcript="hello", delay_ms=None)
    assert live_chat._pace_delay_seconds(
        row,
        chars_per_second=10,
        jitter_pct=0.5,
        rng=SimpleNamespace(uniform=lambda _a, _b: 0.2),
    ) == 0.6


@pytest.mark.asyncio
async def test_sleep_or_stop_returns_immediately_for_zero_and_set_event():
    stop_event = asyncio.Event()
    await live_chat._sleep_or_stop(stop_event, 0)
    stop_event.set()
    await live_chat._sleep_or_stop(stop_event, 1)


@pytest.mark.asyncio
async def test_manager_stop_clear_shutdown_and_emit_helpers(monkeypatch):
    emitted: list[tuple[str, dict]] = []

    async def _emit(event_type: str, data: dict) -> None:
        emitted.append((event_type, data))

    manager = live_chat.LiveChatManager(emit=_emit)

    task = asyncio.create_task(asyncio.Event().wait())
    manager._task = task
    manager._stop_event = None
    manager._state = live_chat.LiveChatSessionState(state="running")
    snapshot = await manager.stop()
    assert snapshot["stop_requested"] is True
    with pytest.raises(asyncio.CancelledError):
        await task

    conflict_task = asyncio.create_task(asyncio.Event().wait())
    manager._task = conflict_task
    with pytest.raises(live_chat.LiveChatConflictError):
        await manager.clear()
    conflict_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await conflict_task

    manager._task = None
    cleared = await manager.clear()
    assert cleared["state"] == "idle"

    assert await manager.stop() == manager.snapshot()

    task = asyncio.create_task(asyncio.Event().wait())
    manager._task = task
    manager._stop_event = asyncio.Event()
    await manager.shutdown()
    assert manager._task is None
    assert manager._stop_event is None
    with pytest.raises(asyncio.CancelledError):
        await task

    manager._append_note("one", level="info")
    manager._upsert_note("fixed-id", "first", level="info")
    manager._upsert_note("fixed-id", "second", level="warning")
    for index in range(live_chat._MAX_HISTORY + 2):
        manager._append_history({"idx": index})
    for index in range(live_chat._MAX_HISTORY + 2):
        manager._append_note(f"note-{index}", level="info")
    await manager._emit_error("boom")

    assert len(manager._state.history) == live_chat._MAX_HISTORY
    assert len(manager._state.status_notes) == live_chat._MAX_HISTORY
    assert any(event_type == "live_error" for event_type, _data in emitted)

    done_task = asyncio.create_task(asyncio.sleep(0))
    await done_task
    manager._task = done_task
    manager._stop_event = None
    await manager.shutdown()

    manager._state.status_notes.clear()
    for index in range(live_chat._MAX_HISTORY + 2):
        manager._upsert_note(f"note-{index}", f"note-{index}", level="info")
    assert len(manager._state.status_notes) == live_chat._MAX_HISTORY


@pytest.mark.asyncio
async def test_consume_kafka_filters_other_conversations_and_sets_complete():
    emitted: list[tuple[str, dict]] = []

    async def _emit(event_type: str, data: dict) -> None:
        emitted.append((event_type, data))

    manager = live_chat.LiveChatManager(emit=_emit)
    manager._state = live_chat.LiveChatSessionState(conversation_id="cid-1")
    complete_seen = asyncio.Event()
    pending_sent_at = {1: time.monotonic()}
    consumer = _IdleKafkaConsumer()
    consumer.queue.put_nowait(
        SimpleNamespace(
            value={"metaData": {"conversationId": "cid-2"}, "payload": {"sequenceNumber": 0}},
            partition=0,
            offset=1,
            timestamp=1,
        )
    )
    consumer.queue.put_nowait(
        SimpleNamespace(
            value={
                "metaData": {"conversationId": "cid-1", "eventType": "SESSION_ONGOING"},
                "payload": {
                    "sequenceNumber": 1,
                    "speaker": "Agent",
                    "transcript": "hello",
                    "speakTimeStamp": "2026-03-31T00:00:00.000Z",
                },
            },
            partition=0,
            offset=2,
            timestamp=2,
        )
    )
    consumer.queue.put_nowait(
        SimpleNamespace(
            value={
                "metaData": {
                    "conversationId": "cid-1",
                    "eventType": "SESSION_COMPLETE",
                    "callEndTimeStamp": "2026-03-31T00:00:01.000Z",
                },
                "payload": {
                    "sequenceNumber": 2,
                    "speaker": "System",
                    "transcript": "EOL",
                },
            },
            partition=0,
            offset=3,
            timestamp=3,
        )
    )
    consumer.queue.put_nowait(StopAsyncIteration)

    await manager._consume_kafka(
        consumer=consumer,
        pending_sent_at=pending_sent_at,
        complete_seen=complete_seen,
    )

    assert manager._state.kafka_messages == 2
    assert complete_seen.is_set() is True
    assert emitted[0][0] == "live_message"
    assert manager._state.history[1]["kafka_lag_ms"] is None


@pytest.mark.asyncio
async def test_run_session_cancelled_before_first_row(monkeypatch):
    _IdleKafkaConsumer.instances.clear()
    manager = live_chat.LiveChatManager(emit=AsyncMock())
    manager._stop_event = asyncio.Event()
    manager._stop_event.set()
    manager._state = live_chat.LiveChatSessionState(
        state="running",
        conversation_id="cid-1",
        csv_filename="chat.csv",
        ws_url="ws://unit-test",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="topic-a",
        chars_per_second=18,
        pace_jitter_pct=0,
        started_at="2026-03-31T00:00:00.000Z",
    )
    ws = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(live_chat, "AIOKafkaConsumer", _IdleKafkaConsumer)
    monkeypatch.setattr(live_chat, "_open_ws", AsyncMock(return_value=ws))

    parsed = live_chat.parse_live_chat_csv("speaker,transcript\nAgent,Hello\n", "chat.csv")
    await manager._run_session(parsed)

    assert manager.snapshot()["state"] == "completed"
    ws.close.assert_awaited_once()
    assert _IdleKafkaConsumer.instances[-1].stopped is True


@pytest.mark.asyncio
async def test_run_session_cancelled_after_rows(monkeypatch):
    _IdleKafkaConsumer.instances.clear()
    manager = live_chat.LiveChatManager(emit=AsyncMock())
    manager._stop_event = asyncio.Event()
    manager._state = live_chat.LiveChatSessionState(
        state="running",
        conversation_id="cid-1",
        csv_filename="chat.csv",
        ws_url="ws://unit-test",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="topic-a",
        chars_per_second=18,
        pace_jitter_pct=0,
        started_at="2026-03-31T00:00:00.000Z",
    )
    ws = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(live_chat, "AIOKafkaConsumer", _IdleKafkaConsumer)
    monkeypatch.setattr(live_chat, "_open_ws", AsyncMock(return_value=ws))

    async def fake_send_and_recv(_ws, _message, *, on_sent=None):
        if on_sent is not None:
            on_sent()
        manager._stop_event.set()
        return {"metaData": {"eventType": "TRANSCRIPT_ACK"}}

    monkeypatch.setattr(live_chat, "_send_and_recv", fake_send_and_recv)

    parsed = live_chat.parse_live_chat_csv("speaker,transcript\nAgent,Hello\n", "chat.csv")
    await manager._run_session(parsed)

    assert manager.snapshot()["state"] == "completed"


@pytest.mark.asyncio
async def test_run_session_fails_on_bad_ack_and_ignores_close_error(monkeypatch):
    _IdleKafkaConsumer.instances.clear()
    emitted: list[tuple[str, dict]] = []

    async def _emit(event_type: str, data: dict) -> None:
        emitted.append((event_type, data))

    manager = live_chat.LiveChatManager(emit=_emit)
    manager._stop_event = asyncio.Event()
    manager._state = live_chat.LiveChatSessionState(
        state="running",
        conversation_id="cid-1",
        csv_filename="chat.csv",
        ws_url="ws://unit-test",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="topic-a",
        chars_per_second=18,
        pace_jitter_pct=0,
        started_at="2026-03-31T00:00:00.000Z",
    )
    ws = SimpleNamespace(close=AsyncMock(side_effect=RuntimeError("close failed")))
    monkeypatch.setattr(live_chat, "AIOKafkaConsumer", _IdleKafkaConsumer)
    monkeypatch.setattr(live_chat, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        live_chat,
        "_send_and_recv",
        AsyncMock(return_value={"metaData": {"eventType": "ERROR"}, "error": {"code": "E1001", "message": "bad"}}),
    )

    parsed = live_chat.parse_live_chat_csv("speaker,transcript\nAgent,Hello\n", "chat.csv")
    await manager._run_session(parsed)

    snapshot = manager.snapshot()
    assert snapshot["state"] == "failed"
    assert "ERROR [E1001]" in snapshot["error"]
    assert any(event_type == "live_error" for event_type, _ in emitted)


@pytest.mark.asyncio
async def test_run_session_fails_on_bad_complete_ack(monkeypatch):
    _IdleKafkaConsumer.instances.clear()
    manager = live_chat.LiveChatManager(emit=AsyncMock())
    manager._stop_event = asyncio.Event()
    manager._state = live_chat.LiveChatSessionState(
        state="running",
        conversation_id="cid-1",
        csv_filename="chat.csv",
        ws_url="ws://unit-test",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="topic-a",
        chars_per_second=18,
        pace_jitter_pct=0,
        started_at="2026-03-31T00:00:00.000Z",
    )
    ws = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr(live_chat, "AIOKafkaConsumer", _IdleKafkaConsumer)
    monkeypatch.setattr(live_chat, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        live_chat,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "TRANSCRIPT_ACK"}},
                {"metaData": {"eventType": "ERROR"}, "error": {"code": "E1009", "message": "bad complete"}},
            ]
        ),
    )

    parsed = live_chat.parse_live_chat_csv("speaker,transcript\nAgent,Hello\n", "chat.csv")
    await manager._run_session(parsed)

    snapshot = manager.snapshot()
    assert snapshot["state"] == "failed"
    assert "bad complete" in snapshot["error"]


@pytest.mark.asyncio
async def test_run_session_handles_consumer_start_failure(monkeypatch):
    class _FailingConsumer:
        async def start(self) -> None:
            raise RuntimeError("consumer down")

        async def stop(self) -> None:
            return None

    manager = live_chat.LiveChatManager(emit=AsyncMock())
    manager._stop_event = asyncio.Event()
    manager._state = live_chat.LiveChatSessionState(
        state="running",
        conversation_id="cid-1",
        csv_filename="chat.csv",
        ws_url="ws://unit-test",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="topic-a",
        chars_per_second=18,
        pace_jitter_pct=0,
        started_at="2026-03-31T00:00:00.000Z",
    )
    monkeypatch.setattr(live_chat, "AIOKafkaConsumer", lambda *_args, **_kwargs: _FailingConsumer())

    parsed = live_chat.parse_live_chat_csv("speaker,transcript\nAgent,Hello\n", "chat.csv")
    await manager._run_session(parsed)

    assert manager.snapshot()["state"] == "failed"
