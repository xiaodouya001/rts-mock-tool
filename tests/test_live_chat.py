"""Tests for mock_tool.live_chat."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from mock_tool.live_chat import (
    LiveChatConflictError,
    LiveChatManager,
    LiveChatPreviewRequest,
    LiveChatStartRequest,
    LiveChatValidationError,
    parse_live_chat_csv,
)


def test_parse_live_chat_csv_recognizes_aliases_and_delay():
    parsed = parse_live_chat_csv(
        "role,content,typing_delay_ms\n"
        "Agent,Hello there,250\n"
        "customer,Need help,\n",
        "sample.csv",
    )

    assert parsed.filename == "sample.csv"
    assert parsed.recognized_columns == {
        "speaker": "role",
        "transcript": "content",
        "delay_ms": "typing_delay_ms",
    }
    assert [row.speaker for row in parsed.rows] == ["Agent", "Customer"]
    assert parsed.rows[0].delay_ms == 250.0
    preview = parsed.preview_payload()
    assert preview["row_count"] == 2
    assert preview["preview_row_count"] == 2
    assert preview["preview_is_full"] is True


def test_parse_live_chat_csv_accepts_spreadsheet_style_role_values():
    parsed = parse_live_chat_csv(
        "Speaker Roles,Transcription\n"
        "SPEAKER_1;AGENT,你好\n"
        "SPEAKER_2;CUSTOMER,喂你好\n",
        "sheet.csv",
    )

    assert parsed.recognized_columns["speaker"] == "Speaker Roles"
    assert parsed.recognized_columns["transcript"] == "Transcription"
    assert [row.speaker for row in parsed.rows] == ["Agent", "Customer"]
    assert [row.transcript for row in parsed.rows] == ["你好", "喂你好"]


def test_parse_live_chat_csv_rejects_invalid_speaker_with_line_number():
    with pytest.raises(LiveChatValidationError, match=r"CSV line 2: speaker must be Agent or Customer"):
        parse_live_chat_csv("speaker,text\nSystem,Done\n", "bad.csv")


class _FakeKafkaConsumer:
    instances: list["_FakeKafkaConsumer"] = []

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


async def _collect_events(sink: list[tuple[str, dict]]) -> callable:
    async def _emit(event_type: str, data: dict) -> None:
        sink.append((event_type, data))

    return _emit


@pytest.mark.asyncio
async def test_live_chat_manager_runs_rows_and_auto_completes(monkeypatch):
    _FakeKafkaConsumer.instances.clear()
    emitted: list[tuple[str, dict]] = []
    manager = LiveChatManager(emit=await _collect_events(emitted))

    ws = SimpleNamespace(close=AsyncMock())
    monkeypatch.setattr("mock_tool.live_chat.AIOKafkaConsumer", _FakeKafkaConsumer)
    monkeypatch.setattr("mock_tool.live_chat._open_ws", AsyncMock(return_value=ws))

    async def fake_send_and_recv(_ws, message, *, on_sent=None):
        if on_sent is not None:
            on_sent()
        consumer = _FakeKafkaConsumer.instances[-1]
        sequence_number = message["payload"]["sequenceNumber"]
        consumer.queue.put_nowait(
            SimpleNamespace(
                partition=0,
                offset=sequence_number,
                value=message,
                timestamp=1_710_000_000_000,
            )
        )
        ack_type = "EOL_ACK" if message["metaData"]["eventType"] == "SESSION_COMPLETE" else "TRANSCRIPT_ACK"
        return {"metaData": {"eventType": ack_type}}

    monkeypatch.setattr("mock_tool.live_chat._send_and_recv", fake_send_and_recv)

    request = LiveChatStartRequest(
        csv_text="speaker,transcript\nAgent,Hello\nCustomer,Hi there\n",
        csv_filename="chat.csv",
        ws_url="ws://127.0.0.1:8080/ws/v1/realtime-transcriptions",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        conversation_id="live-chat-1",
        chars_per_second=20,
        pace_jitter_pct=0,
    )

    snapshot = await manager.start(request)
    assert snapshot["state"] == "running"

    assert manager._task is not None
    await manager._task
    final = manager.snapshot()

    assert final["state"] == "completed"
    assert final["ack_count"] == 3
    assert final["sent_rows"] == 2
    assert final["kafka_messages"] == 3
    assert [item["speaker"] for item in final["history"]] == ["Agent", "Customer", "System"]
    assert final["history"][-1]["event_type"] == "SESSION_COMPLETE"
    assert [note["message"] for note in final["status_notes"]] == ["Connected to WebSocket and Kafka."]
    assert not any(note["message"] == "Session completed." for note in final["status_notes"])
    assert any(event_type == "live_message" for event_type, _ in emitted)
    assert any(
        event_type == "live_status" and data["state"] == "completed"
        for event_type, data in emitted
    )


@pytest.mark.asyncio
async def test_live_chat_manager_rejects_second_start_while_running(monkeypatch):
    _FakeKafkaConsumer.instances.clear()
    blocker = asyncio.Event()
    manager = LiveChatManager(emit=AsyncMock())

    monkeypatch.setattr("mock_tool.live_chat.AIOKafkaConsumer", _FakeKafkaConsumer)
    monkeypatch.setattr(
        "mock_tool.live_chat._open_ws",
        AsyncMock(return_value=SimpleNamespace(close=AsyncMock())),
    )

    async def blocked_send_and_recv(_ws, _message, *, on_sent=None):
        if on_sent is not None:
            on_sent()
        await blocker.wait()
        return {"metaData": {"eventType": "TRANSCRIPT_ACK"}}

    monkeypatch.setattr("mock_tool.live_chat._send_and_recv", blocked_send_and_recv)

    request = LiveChatStartRequest(
        csv_text="speaker,transcript\nAgent,Hello\n",
        csv_filename="chat.csv",
        ws_url="ws://127.0.0.1:8080/ws/v1/realtime-transcriptions",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        conversation_id="live-chat-2",
        chars_per_second=20,
        pace_jitter_pct=0,
    )

    await manager.start(request)
    await asyncio.sleep(0)

    with pytest.raises(LiveChatConflictError):
        await manager.start(request.model_copy(update={"conversation_id": "live-chat-3"}))

    await manager.stop()
    blocker.set()


@pytest.mark.asyncio
async def test_live_chat_manager_stop_cancels_running_session(monkeypatch):
    _FakeKafkaConsumer.instances.clear()
    blocker = asyncio.Event()
    manager = LiveChatManager(emit=AsyncMock())

    monkeypatch.setattr("mock_tool.live_chat.AIOKafkaConsumer", _FakeKafkaConsumer)
    monkeypatch.setattr(
        "mock_tool.live_chat._open_ws",
        AsyncMock(return_value=SimpleNamespace(close=AsyncMock())),
    )

    async def blocked_send_and_recv(_ws, _message, *, on_sent=None):
        if on_sent is not None:
            on_sent()
        await blocker.wait()
        return {"metaData": {"eventType": "TRANSCRIPT_ACK"}}

    monkeypatch.setattr("mock_tool.live_chat._send_and_recv", blocked_send_and_recv)

    request = LiveChatStartRequest(
        csv_text="speaker,transcript\nAgent,Hello\nCustomer,Still here\n",
        csv_filename="chat.csv",
        ws_url="ws://127.0.0.1:8080/ws/v1/realtime-transcriptions",
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        conversation_id="live-chat-4",
        chars_per_second=20,
        pace_jitter_pct=0,
    )

    await manager.start(request)
    await asyncio.sleep(0)
    blocker.set()
    snapshot = await manager.stop()

    assert snapshot["state"] == "completed"
    assert snapshot["stop_requested"] is True
    stop_notes = [note for note in snapshot["status_notes"] if "stop" in note["message"].lower()]
    assert len(stop_notes) == 1
    assert stop_notes[0]["message"] == "Live-Chat stopped before session completion."


def test_live_chat_manager_preview_uses_same_csv_parser():
    manager = LiveChatManager(emit=AsyncMock())

    preview = manager.preview_csv(
        LiveChatPreviewRequest(
            csv_text="sender,message\nCustomer,Testing\n",
            csv_filename="preview.csv",
        )
    )

    assert preview["csv_filename"] == "preview.csv"
    assert preview["recognized_columns"]["speaker"] == "sender"
    assert preview["recognized_columns"]["transcript"] == "message"


def test_live_chat_manager_preview_show_all_returns_full_csv():
    manager = LiveChatManager(emit=AsyncMock())

    preview = manager.preview_csv(
        LiveChatPreviewRequest(
            csv_text="speaker,transcript\n" + "\n".join(
                f"Agent,row-{index}" for index in range(1, 16)
            ),
            csv_filename="full.csv",
            show_all=True,
        )
    )

    assert preview["row_count"] == 15
    assert preview["preview_row_count"] == 15
    assert preview["preview_is_full"] is True
    assert len(preview["sample_rows"]) == 15
