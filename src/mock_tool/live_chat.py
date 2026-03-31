"""Mock Live-Chat runner for CSV-driven websocket playback and Kafka rendering."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Literal

from aiokafka import AIOKafkaConsumer
from pydantic import BaseModel, Field

try:
    from .ws_driver import _open_ws, _random_hex, _send_and_recv, _utc_now_iso
except ImportError:  # pragma: no cover - direct script execution fallback
    from ws_driver import _open_ws, _random_hex, _send_and_recv, _utc_now_iso

Speaker = Literal["Agent", "Customer"]
StateName = Literal["idle", "running", "stopping", "completed", "failed"]
StatusLevel = Literal["info", "success", "warning", "error"]
EmitFn = Callable[[str, dict[str, Any]], asyncio.Future | asyncio.Task | Any]

_SPEAKER_ALIASES = {"speaker", "role", "sender", "speakerroles"}
_TRANSCRIPT_ALIASES = {"transcript", "transcription", "text", "message", "content"}
_DELAY_ALIASES = {"delayms", "pausems", "waitms", "typingdelayms"}
_MAX_HISTORY = 500


class LiveChatValidationError(ValueError):
    """Raised when the uploaded CSV or runtime inputs are invalid."""


class LiveChatConflictError(RuntimeError):
    """Raised when a second live-chat run is requested while one is already active."""


class LiveChatPreviewRequest(BaseModel):
    csv_text: str = Field(..., min_length=1)
    csv_filename: str = Field(default="conversation.csv", min_length=1, max_length=256)
    show_all: bool = False


class LiveChatStartRequest(LiveChatPreviewRequest):
    ws_url: str = Field(..., min_length=1, max_length=2048)
    kafka_bootstrap: str = Field(..., min_length=1, max_length=512)
    kafka_topic: str = Field(..., min_length=1, max_length=512)
    conversation_id: str = Field(..., min_length=1, max_length=64)
    chars_per_second: float = Field(..., gt=0, le=200)
    pace_jitter_pct: float = Field(..., ge=0, le=1)


@dataclass(frozen=True)
class LiveChatRow:
    line_number: int
    speaker: Speaker
    transcript: str
    delay_ms: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "line_number": self.line_number,
            "speaker": self.speaker,
            "transcript": self.transcript,
            "delay_ms": self.delay_ms,
        }


@dataclass(frozen=True)
class ParsedLiveChatCsv:
    filename: str
    rows: list[LiveChatRow]
    recognized_columns: dict[str, str | None]

    def preview_payload(self, *, sample_limit: int | None = 12) -> dict[str, Any]:
        if sample_limit is None:
            shown_rows = self.rows
        else:
            shown_rows = self.rows[:sample_limit]
        return {
            "csv_filename": self.filename,
            "row_count": len(self.rows),
            "preview_row_count": len(shown_rows),
            "preview_is_full": len(shown_rows) == len(self.rows),
            "recognized_columns": self.recognized_columns,
            "sample_rows": [row.as_dict() for row in shown_rows],
        }


@dataclass
class LiveChatStatusNote:
    note_id: str
    at: str
    message: str
    level: StatusLevel

    def as_dict(self) -> dict[str, Any]:
        return {
            "note_id": self.note_id,
            "at": self.at,
            "message": self.message,
            "level": self.level,
        }


@dataclass
class LiveChatSessionState:
    state: StateName = "idle"
    conversation_id: str | None = None
    csv_filename: str | None = None
    preview: dict[str, Any] | None = None
    history: list[dict[str, Any]] = field(default_factory=list)
    status_notes: list[LiveChatStatusNote] = field(default_factory=list)
    error: str | None = None
    ws_url: str | None = None
    kafka_bootstrap: str | None = None
    kafka_topic: str | None = None
    chars_per_second: float | None = None
    pace_jitter_pct: float | None = None
    started_at: str | None = None
    finished_at: str | None = None
    sent_rows: int = 0
    ack_count: int = 0
    kafka_messages: int = 0
    stop_requested: bool = False

    def as_dict(self, *, include_history: bool = True) -> dict[str, Any]:
        payload = {
            "state": self.state,
            "conversation_id": self.conversation_id,
            "csv_filename": self.csv_filename,
            "preview": self.preview,
            "error": self.error,
            "ws_url": self.ws_url,
            "kafka_bootstrap": self.kafka_bootstrap,
            "kafka_topic": self.kafka_topic,
            "chars_per_second": self.chars_per_second,
            "pace_jitter_pct": self.pace_jitter_pct,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "sent_rows": self.sent_rows,
            "ack_count": self.ack_count,
            "kafka_messages": self.kafka_messages,
            "stop_requested": self.stop_requested,
            "status_notes": [note.as_dict() for note in self.status_notes],
        }
        if include_history:
            payload["history"] = list(self.history)
        return payload


def _normalize_header(name: str) -> str:
    return "".join(ch for ch in name.strip().lower() if ch.isalnum())


def _canonical_speaker(raw: str, *, line_number: int) -> Speaker:
    normalized = raw.strip().lower()
    for separator in (";", ":", "|"):
        if separator in normalized:
            normalized = normalized.split(separator)[-1].strip()
            break
    if normalized == "agent":
        return "Agent"
    if normalized == "customer":
        return "Customer"
    raise LiveChatValidationError(
        f"CSV line {line_number}: speaker must be Agent or Customer; got {raw!r}"
    )


def _parse_delay(raw: str, *, line_number: int) -> float | None:
    if raw.strip() == "":
        return None
    try:
        value = float(raw)
    except ValueError as exc:
        raise LiveChatValidationError(
            f"CSV line {line_number}: delay_ms must be a number"
        ) from exc
    if value < 0:
        raise LiveChatValidationError(
            f"CSV line {line_number}: delay_ms must be greater than or equal to 0"
        )
    return value


def parse_live_chat_csv(csv_text: str, csv_filename: str = "conversation.csv") -> ParsedLiveChatCsv:
    text = csv_text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(text))
    fieldnames = reader.fieldnames or []
    if not fieldnames:
        raise LiveChatValidationError("CSV must include a header row")

    normalized_to_original: dict[str, str] = {}
    for header in fieldnames:
        normalized = _normalize_header(header)
        if normalized and normalized not in normalized_to_original:
            normalized_to_original[normalized] = header

    speaker_header = next(
        (normalized_to_original.get(alias) for alias in _SPEAKER_ALIASES if alias in normalized_to_original),
        None,
    )
    transcript_header = next(
        (normalized_to_original.get(alias) for alias in _TRANSCRIPT_ALIASES if alias in normalized_to_original),
        None,
    )
    delay_header = next(
        (normalized_to_original.get(alias) for alias in _DELAY_ALIASES if alias in normalized_to_original),
        None,
    )

    if speaker_header is None:
        raise LiveChatValidationError(
            "CSV must include a speaker column (aliases: speaker, role, sender)"
        )
    if transcript_header is None:
        raise LiveChatValidationError(
            "CSV must include a transcript column (aliases: transcript, text, message, content)"
        )

    rows: list[LiveChatRow] = []
    for raw_row in reader:
        if raw_row is None:
            continue
        line_number = reader.line_num
        if all((value or "").strip() == "" for value in raw_row.values()):
            continue

        transcript = (raw_row.get(transcript_header) or "").strip()
        if not transcript:
            raise LiveChatValidationError(
                f"CSV line {line_number}: transcript must not be empty"
            )
        speaker = _canonical_speaker(raw_row.get(speaker_header) or "", line_number=line_number)
        delay_ms = _parse_delay(raw_row.get(delay_header) or "", line_number=line_number) if delay_header else None
        rows.append(
            LiveChatRow(
                line_number=line_number,
                speaker=speaker,
                transcript=transcript,
                delay_ms=delay_ms,
            )
        )

    if not rows:
        raise LiveChatValidationError("CSV must contain at least one non-empty data row")

    return ParsedLiveChatCsv(
        filename=csv_filename,
        rows=rows,
        recognized_columns={
            "speaker": speaker_header,
            "transcript": transcript_header,
            "delay_ms": delay_header,
        },
    )


def _build_live_chat_message(
    *,
    conversation_id: str,
    sequence_number: int,
    event_type: str,
    start_ts: str,
    speaker: Speaker | Literal["System"],
    transcript: str,
) -> dict[str, Any]:
    now = _utc_now_iso()
    payload: dict[str, Any] = {
        "sequenceNumber": sequence_number,
        "speaker": speaker,
        "transcript": transcript,
        "engineProvider": "FanoLabs",
        "dialect": "yue-x-auto",
        "isFinal": True,
    }
    if event_type == "SESSION_ONGOING":
        payload["speakTimeStamp"] = now
        payload["transcriptGenerateTimeStamp"] = now

    return {
        "metaData": {
            "conversationId": conversation_id,
            "callStartTimeStamp": start_ts,
            "callEndTimeStamp": now if event_type == "SESSION_COMPLETE" else None,
            "eventType": event_type,
        },
        "payload": payload,
    }


def _response_error_detail(response: dict[str, Any] | None) -> str:
    if not response:
        return "No server response received"
    event_type = response.get("metaData", {}).get("eventType")
    if event_type == "ERROR":
        error = response.get("error") or {}
        code = error.get("code", "?")
        message = error.get("message") or ""
        details = error.get("details") or ""
        suffix = f": {details}" if details else ""
        return f"Server returned ERROR [{code}] {message}{suffix}"
    return f"Expected ACK but received eventType={event_type!r}"


def _pace_delay_seconds(
    row: LiveChatRow,
    *,
    chars_per_second: float,
    jitter_pct: float,
    rng: random.Random,
) -> float:
    if row.delay_ms is not None:
        return row.delay_ms / 1000.0
    base = max(len(row.transcript), 1) / chars_per_second
    if jitter_pct > 0:
        base *= max(0.1, 1 + rng.uniform(-jitter_pct, jitter_pct))
    return max(base, 0.0)


def _require_runtime_value(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise LiveChatValidationError(f"{name} must not be empty")
    return normalized


async def _sleep_or_stop(stop_event: asyncio.Event, delay_seconds: float) -> None:
    if delay_seconds <= 0:
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=delay_seconds)
    except asyncio.TimeoutError:
        return


class LiveChatManager:
    """Manage a single active CSV-driven mock live-chat session."""

    def __init__(self, *, emit: EmitFn):
        self._emit = emit
        self._task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._state = LiveChatSessionState()
        self._lock = asyncio.Lock()

    def snapshot(self, *, include_history: bool = True) -> dict[str, Any]:
        return self._state.as_dict(include_history=include_history)

    def preview_csv(self, request: LiveChatPreviewRequest) -> dict[str, Any]:
        parsed = parse_live_chat_csv(request.csv_text, request.csv_filename)
        return parsed.preview_payload(sample_limit=None if request.show_all else 12)

    async def start(self, request: LiveChatStartRequest) -> dict[str, Any]:
        parsed = parse_live_chat_csv(request.csv_text, request.csv_filename)
        ws_url = _require_runtime_value("ws_url", request.ws_url)
        kafka_bootstrap = _require_runtime_value("kafka_bootstrap", request.kafka_bootstrap)
        kafka_topic = _require_runtime_value("kafka_topic", request.kafka_topic)
        conversation_id = _require_runtime_value("conversation_id", request.conversation_id)
        async with self._lock:
            if self._task and not self._task.done():
                raise LiveChatConflictError("A Mock Live-Chat session is already running")

            now = _utc_now_iso()
            self._stop_event = asyncio.Event()
            self._state = LiveChatSessionState(
                state="running",
                conversation_id=conversation_id,
                csv_filename=parsed.filename,
                preview=parsed.preview_payload(),
                ws_url=ws_url,
                kafka_bootstrap=kafka_bootstrap,
                kafka_topic=kafka_topic,
                chars_per_second=float(request.chars_per_second),
                pace_jitter_pct=float(request.pace_jitter_pct),
                started_at=now,
            )
            self._upsert_note(
                "connection-state",
                "Preparing Mock Live-Chat session...",
                level="info",
            )
            self._task = asyncio.create_task(self._run_session(parsed))

        await self._emit_status()
        return self.snapshot()

    async def stop(self) -> dict[str, Any]:
        async with self._lock:
            task = self._task
            stop_event = self._stop_event
            if not task or task.done():
                return self.snapshot()
            if stop_event is not None:
                stop_event.set()
            self._state.stop_requested = True
            self._state.state = "stopping"
            self._upsert_note(
                "stop-state",
                "Stopping Mock Live-Chat session...",
                level="warning",
            )

        await self._emit_status()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        return self.snapshot()

    async def clear(self) -> dict[str, Any]:
        async with self._lock:
            if self._task and not self._task.done():
                raise LiveChatConflictError("Stop the running Mock Live-Chat session before clearing it")
            self._task = None
            self._stop_event = None
            self._state = LiveChatSessionState()

        await self._emit_status()
        return self.snapshot()

    async def shutdown(self) -> None:
        task = self._task
        stop_event = self._stop_event
        if stop_event is not None:
            stop_event.set()
        if task and not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._stop_event = None

    def _append_note(self, message: str, *, level: StatusLevel) -> None:
        note = LiveChatStatusNote(
            note_id=f"note-{_random_hex(10)}",
            at=_utc_now_iso(),
            message=message,
            level=level,
        )
        self._state.status_notes.append(note)
        if len(self._state.status_notes) > _MAX_HISTORY:
            self._state.status_notes = self._state.status_notes[-_MAX_HISTORY:]

    def _upsert_note(self, note_id: str, message: str, *, level: StatusLevel) -> None:
        for note in self._state.status_notes:
            if note.note_id == note_id:
                note.at = _utc_now_iso()
                note.message = message
                note.level = level
                return

        note = LiveChatStatusNote(
            note_id=note_id,
            at=_utc_now_iso(),
            message=message,
            level=level,
        )
        self._state.status_notes.append(note)
        if len(self._state.status_notes) > _MAX_HISTORY:
            self._state.status_notes = self._state.status_notes[-_MAX_HISTORY:]

    def _append_history(self, item: dict[str, Any]) -> None:
        self._state.history.append(item)
        if len(self._state.history) > _MAX_HISTORY:
            self._state.history = self._state.history[-_MAX_HISTORY:]

    async def _emit_status(self) -> None:
        await self._emit("live_status", self.snapshot(include_history=False))

    async def _emit_error(self, message: str) -> None:
        await self._emit(
            "live_error",
            {
                "conversation_id": self._state.conversation_id,
                "message": message,
                "state": self._state.state,
            },
        )

    async def _run_session(self, parsed: ParsedLiveChatCsv) -> None:
        assert self._stop_event is not None

        stop_event = self._stop_event
        rng = random.Random()
        complete_seen = asyncio.Event()
        pending_sent_at: dict[int, float] = {}
        consumer = AIOKafkaConsumer(
            self._state.kafka_topic,
            bootstrap_servers=self._state.kafka_bootstrap,
            group_id=None,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(value) if value else None,
            key_deserializer=lambda key: key.decode("utf-8") if key else None,
        )
        ws = None
        consumer_task: asyncio.Task[None] | None = None

        try:
            await consumer.start()
            consumer_task = asyncio.create_task(
                self._consume_kafka(
                    consumer=consumer,
                    pending_sent_at=pending_sent_at,
                    complete_seen=complete_seen,
                )
            )
            ws = await _open_ws(self._state.ws_url or "", self._state.conversation_id)
            self._upsert_note(
                "connection-state",
                "Connected to WebSocket and Kafka.",
                level="success",
            )
            await self._emit_status()

            start_ts = self._state.started_at or _utc_now_iso()
            for sequence_number, row in enumerate(parsed.rows):
                await _sleep_or_stop(
                    stop_event,
                    _pace_delay_seconds(
                        row,
                        chars_per_second=self._state.chars_per_second or 18.0,
                        jitter_pct=self._state.pace_jitter_pct or 0.0,
                        rng=rng,
                    ),
                )
                if stop_event.is_set():
                    raise asyncio.CancelledError

                message = _build_live_chat_message(
                    conversation_id=self._state.conversation_id or "",
                    sequence_number=sequence_number,
                    event_type="SESSION_ONGOING",
                    start_ts=start_ts,
                    speaker=row.speaker,
                    transcript=row.transcript,
                )
                response = await _send_and_recv(
                    ws,
                    message,
                    on_sent=lambda seq=sequence_number: pending_sent_at.__setitem__(seq, time.monotonic()),
                )
                if response is None or response.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
                    raise RuntimeError(
                        f"CSV line {row.line_number}: {_response_error_detail(response)}"
                    )
                self._state.sent_rows = sequence_number + 1
                self._state.ack_count += 1

            if stop_event.is_set():
                raise asyncio.CancelledError

            complete_sequence = len(parsed.rows)
            complete_message = _build_live_chat_message(
                conversation_id=self._state.conversation_id or "",
                sequence_number=complete_sequence,
                event_type="SESSION_COMPLETE",
                start_ts=start_ts,
                speaker="System",
                transcript="EOL",
            )
            response = await _send_and_recv(
                ws,
                complete_message,
                on_sent=lambda seq=complete_sequence: pending_sent_at.__setitem__(seq, time.monotonic()),
            )
            if response is None or response.get("metaData", {}).get("eventType") != "EOL_ACK":
                raise RuntimeError(_response_error_detail(response))
            self._state.ack_count += 1
            await asyncio.wait_for(complete_seen.wait(), timeout=10)

            self._state.state = "completed"
            self._state.finished_at = _utc_now_iso()
            await self._emit_status()
        except asyncio.CancelledError:
            self._state.state = "completed"
            self._state.finished_at = _utc_now_iso()
            self._upsert_note(
                "stop-state",
                "Live-Chat stopped before session completion.",
                level="warning",
            )
            await self._emit_status()
        except Exception as exc:
            self._state.state = "failed"
            self._state.error = str(exc)
            self._state.finished_at = _utc_now_iso()
            self._append_note(str(exc), level="error")
            await self._emit_error(str(exc))
            await self._emit_status()
        finally:
            if consumer_task and not consumer_task.done():
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
            if ws is not None:
                try:
                    await ws.close()
                except Exception:
                    pass
            await consumer.stop()
            self._task = None
            self._stop_event = None

    async def _consume_kafka(
        self,
        *,
        consumer: AIOKafkaConsumer,
        pending_sent_at: dict[int, float],
        complete_seen: asyncio.Event,
    ) -> None:
        async for message in consumer:
            value = message.value or {}
            meta = value.get("metaData") or {}
            payload = value.get("payload") or {}
            if meta.get("conversationId") != self._state.conversation_id:
                continue

            sequence_number = payload.get("sequenceNumber")
            sent_at = pending_sent_at.pop(sequence_number, None)
            lag_ms = None
            if sent_at is not None:
                lag_ms = round((time.monotonic() - sent_at) * 1000, 1)

            item = {
                "message_id": f"{message.partition}:{message.offset}",
                "conversation_id": meta.get("conversationId"),
                "event_type": meta.get("eventType"),
                "speaker": payload.get("speaker"),
                "transcript": payload.get("transcript"),
                "sequence_number": sequence_number,
                "created_at": payload.get("speakTimeStamp") or meta.get("callEndTimeStamp"),
                "kafka_partition": message.partition,
                "kafka_offset": message.offset,
                "kafka_timestamp": message.timestamp,
                "kafka_lag_ms": lag_ms,
            }
            self._append_history(item)
            self._state.kafka_messages += 1
            await self._emit("live_message", item)

            if meta.get("eventType") == "SESSION_COMPLETE":
                complete_seen.set()
