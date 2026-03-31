"""Mock Client backend — FastAPI app for scenarios, load tests, and Kafka replay.

Startup:
    python -m mock_tool.server
    # Open http://127.0.0.1:8088 in a browser.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

try:
    from .kafka_viewer import KafkaViewer, purge_topic_messages, scan_topic_conversations
    from .live_chat import (
        LiveChatConflictError,
        LiveChatManager,
        LiveChatPreviewRequest,
        LiveChatStartRequest,
        LiveChatValidationError,
    )
    from .logging_config import configure_logging
    from .settings import get_settings
    from .ws_driver import SCENARIOS, Stats, run_load_test
except ImportError:  # pragma: no cover - direct script execution fallback
    from kafka_viewer import KafkaViewer, purge_topic_messages, scan_topic_conversations
    from live_chat import (
        LiveChatConflictError,
        LiveChatManager,
        LiveChatPreviewRequest,
        LiveChatStartRequest,
        LiveChatValidationError,
    )
    from logging_config import configure_logging
    from settings import get_settings
    from ws_driver import SCENARIOS, Stats, run_load_test

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------

stats = Stats()
_sse_queues: list[asyncio.Queue[str]] = []
_load_stop_event: asyncio.Event | None = None
_load_task: asyncio.Task | None = None
_kafka_viewer: KafkaViewer | None = None
_kafka_forward_task: asyncio.Task | None = None
SETTINGS = get_settings()

DEFAULT_WS_URL = SETTINGS.default_ws_url
DEFAULT_KAFKA_BOOTSTRAP = SETTINGS.default_kafka_bootstrap
DEFAULT_KAFKA_TOPIC = SETTINGS.default_kafka_topic
_live_chat_manager = LiveChatManager(emit=lambda event_type, data: _emit(event_type, data))


# ---------------------------------------------------------------------------
# SSE broadcasting
# ---------------------------------------------------------------------------

def _sse_put_drop_oldest(q: asyncio.Queue[str], payload: str) -> None:
    """Enqueue one SSE payload; if the queue is full, drop the oldest event and retry so load/Kafka bursts do not disconnect the UI."""
    for _ in range(10_000):
        try:
            q.put_nowait(payload)
            return
        except asyncio.QueueFull:
            try:
                q.get_nowait()
            except asyncio.QueueEmpty:
                continue


def _broadcast_sse(event_type: str, data: dict[str, Any]) -> None:
    payload = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"
    for q in _sse_queues:
        _sse_put_drop_oldest(q, payload)


async def _emit(event_type: str, data: dict[str, Any]) -> None:
    _broadcast_sse(event_type, data)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    _pusher = asyncio.create_task(_stats_pusher())
    yield
    _pusher.cancel()

    global _load_stop_event, _load_task, _kafka_forward_task, _kafka_viewer

    if _load_stop_event:
        _load_stop_event.set()
    if _load_task and not _load_task.done():
        _load_task.cancel()
        _load_task = None
    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
        _kafka_forward_task = None
    if _kafka_viewer:
        await _kafka_viewer.stop()
        _kafka_viewer = None
    await _live_chat_manager.shutdown()

    for q in _sse_queues:
        _sse_put_drop_oldest(q, "")
    _sse_queues.clear()


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="Mock Client", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", response_class=HTMLResponse)
async def index():
    html_path = STATIC_DIR / "index.html"
    return HTMLResponse(content=html_path.read_text(encoding="utf-8"))


@app.get("/api/events")
async def sse(request: Request):
    """SSE stream that pushes scenario progress, Kafka messages, and metrics."""
    q: asyncio.Queue[str] = asyncio.Queue(maxsize=50_000)
    _sse_queues.append(q)

    # Refreshing the browser creates a new SSE stream. The server still retains the
    # last load test's in-memory metrics, so clear the UI only when no load test is
    # currently running. That avoids interrupting an active run and still lets multiple
    # tabs observe live data.
    if not stats.load_running:
        stats.reset()
        _broadcast_sse("stats", stats.snapshot())

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15)
                    yield msg
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            if q in _sse_queues:
                _sse_queues.remove(q)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/scenario/run")
async def run_scenario(
    name: str = Query(...),
    ws_url: str = Query(DEFAULT_WS_URL),
    n_messages: int = Query(
        5,
        ge=1,
        le=100,
        description=(
            "N-01: number of SESSION_ONGOING messages to send. "
            "N-02: number of idempotent seq values in [0..N). "
            "N-03: total business messages in the session, including SESSION_COMPLETE. "
            "E-09: the second out-of-order frame uses seq=max(2,N). "
            "All other fixed error scenarios ignore this parameter."
        ),
    ),
):
    """Run one predefined scenario."""
    if name not in SCENARIOS:
        return {"error": f"Unknown scenario: {name}. Available values: {list(SCENARIOS.keys())}"}

    fn = SCENARIOS[name]
    _broadcast_sse("scenario_start", {"scenario": name})

    import inspect
    sig = inspect.signature(fn)
    kwargs: dict[str, Any] = {"ws_url": ws_url, "emit": _emit}
    if "n_messages" in sig.parameters:
        kwargs["n_messages"] = n_messages

    result = await fn(**kwargs)
    summary = {"scenario": result.name, "passed": result.passed, "steps": result.steps}
    _broadcast_sse("scenario_done", summary)
    return summary


@app.post("/api/scenario/run-all")
async def run_all_scenarios(
    ws_url: str = Query(DEFAULT_WS_URL),
    n_messages: int = Query(5, ge=1, le=100),
):
    """Run all scenarios sequentially."""
    results = []
    for name in SCENARIOS:
        r = await run_scenario(name=name, ws_url=ws_url, n_messages=n_messages)
        results.append(r)
    return results


@app.post("/api/load/start")
async def load_start(
    ws_url: str = Query(DEFAULT_WS_URL),
    concurrency: int = Query(
        10,
        ge=1,
        le=10_000,
        description="Number of sessions running concurrently in this round, roughly matching the peak concurrent WebSocket count.",
    ),
    messages_per_conv: int = Query(
        10,
        ge=1,
        le=1000,
        description="Total business messages sent by one connection, including one SESSION_COMPLETE.",
    ),
    interval_ms: float = Query(20, ge=0),
    ramp_up_ms: float = Query(
        0,
        ge=0,
        description="Connection ramp-up time in milliseconds. Values above 0 spread connection starts evenly across this interval to avoid saturating the TCP backlog. 0 starts all connections immediately.",
    ),
):
    """Start a concurrent load test on the normal happy path only: multiple `SESSION_ONGOING` frames plus a final `SESSION_COMPLETE`, with no error or edge-case scenarios."""
    global _load_stop_event, _load_task
    if _load_task and not _load_task.done():
        return {"error": "A load test is already running"}

    _load_stop_event = asyncio.Event()

    stats.reset()
    stats.load_running = True
    _broadcast_sse("stats", stats.snapshot())

    total_sessions = concurrency
    _broadcast_sse(
        "load_start",
        {
            "concurrency": concurrency,
            "messages_per_conv": messages_per_conv,
            "interval_ms": interval_ms,
            "ramp_up_ms": ramp_up_ms,
            "total_sessions": total_sessions,
            "note": (
                f"This round runs {total_sessions} sessions with about {concurrency} concurrent connections"
                f"{' (ramp-up %.0fms)' % ramp_up_ms if ramp_up_ms > 0 else ''}. "
                "The run finishes after all messages are sent. You can click Stop first to prevent not-yet-started sessions from entering the queue."
            ),
        },
    )

    async def _run():
        try:
            await run_load_test(
                ws_url=ws_url,
                stats=stats,
                emit=_emit,
                concurrency=concurrency,
                messages_per_conv=messages_per_conv,
                interval_ms=interval_ms,
                ramp_up_ms=ramp_up_ms,
                stop_event=_load_stop_event,
            )
        finally:
            stats.finish()

    _load_task = asyncio.create_task(_run())
    return {
        "status": "started",
        "concurrency": concurrency,
        "ramp_up_ms": ramp_up_ms,
        "total_sessions": total_sessions,
    }


@app.post("/api/load/stop")
async def load_stop():
    """Stop the load test immediately and let cleanup continue in the background."""
    global _load_stop_event
    if _load_stop_event:
        _load_stop_event.set()
    return {"status": "stopping"}


@app.get("/api/status")
async def get_status():
    return stats.snapshot()


@app.post("/api/live/preview")
async def live_preview(request: LiveChatPreviewRequest):
    try:
        return _live_chat_manager.preview_csv(request)
    except LiveChatValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/live/start")
async def live_start(request: LiveChatStartRequest):
    try:
        return await _live_chat_manager.start(request)
    except LiveChatValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except LiveChatConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/live/stop")
async def live_stop():
    return await _live_chat_manager.stop()


@app.post("/api/live/clear")
async def live_clear():
    try:
        return await _live_chat_manager.clear()
    except LiveChatConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.get("/api/live/status")
async def live_status():
    return _live_chat_manager.snapshot()


@app.post("/api/kafka/start")
async def kafka_start(
    bootstrap: str = Query(DEFAULT_KAFKA_BOOTSTRAP),
    topic: str = Query(DEFAULT_KAFKA_TOPIC),
    conversation_id: str = Query(..., min_length=1),
):
    """Start the Kafka consumer and forward messages through SSE."""
    global _kafka_viewer, _kafka_forward_task

    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
    if _kafka_viewer:
        await _kafka_viewer.stop()

    def _on_kafka_error(err: str) -> None:
        _broadcast_sse("kafka_error", {"error": err})

    _kafka_viewer = KafkaViewer(
        bootstrap_servers=bootstrap,
        topic=topic,
        conversation_id=conversation_id,
        on_error=_on_kafka_error,
    )
    sid, q = _kafka_viewer.subscribe()

    async def _forward():
        try:
            while True:
                msg = await q.get()
                _broadcast_sse("kafka_message", msg)
        except asyncio.CancelledError:
            pass

    try:
        await _kafka_viewer.start()
    except Exception as e:
        _broadcast_sse("kafka_error", {"error": str(e)})
        return {"status": "kafka_consumer_failed", "error": str(e)}

    _kafka_forward_task = asyncio.create_task(_forward())
    _broadcast_sse("kafka_status", {"connected": True, "topic": topic, "conversation_id": conversation_id})
    return {"status": "kafka_consumer_started", "topic": topic, "conversation_id": conversation_id}


@app.get("/api/kafka/conversations")
async def kafka_conversations(
    bootstrap: str = Query(DEFAULT_KAFKA_BOOTSTRAP),
    topic: str = Query(DEFAULT_KAFKA_TOPIC),
):
    """Scan the topic once and return distinct conversationId values for selection."""
    try:
        return await scan_topic_conversations(bootstrap_servers=bootstrap, topic=topic)
    except Exception as exc:
        return {"status": "error", "error": str(exc)}


@app.post("/api/kafka/stop")
async def kafka_stop():
    global _kafka_viewer, _kafka_forward_task
    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
        _kafka_forward_task = None
    if _kafka_viewer:
        await _kafka_viewer.stop()
        _kafka_viewer = None
    _broadcast_sse("kafka_status", {"connected": False})
    return {"status": "kafka_consumer_stopped"}


@app.post("/api/kafka/purge")
async def kafka_purge(
    bootstrap: str = Query(DEFAULT_KAFKA_BOOTSTRAP),
    topic: str = Query(DEFAULT_KAFKA_TOPIC),
    restart_consumer: bool = Query(
        True,
        description="Whether to automatically re-subscribe after purge if the same bootstrap+topic was already being consumed",
    ),
):
    """Delete all committed messages in the topic with DeleteRecords. The local Kafka consumer is stopped first to avoid offset confusion."""
    global _kafka_viewer, _kafka_forward_task

    resume_bs: str | None = None
    resume_topic: str | None = None
    resume_conversation_id: str | None = None
    if (
        _kafka_viewer
        and restart_consumer
        and _kafka_viewer.bootstrap_servers == bootstrap
        and _kafka_viewer.topic == topic
    ):
        resume_bs, resume_topic = bootstrap, topic
        resume_conversation_id = _kafka_viewer.conversation_id

    if _kafka_forward_task and not _kafka_forward_task.done():
        _kafka_forward_task.cancel()
        _kafka_forward_task = None
    if _kafka_viewer:
        await _kafka_viewer.stop()
        _kafka_viewer = None
    _broadcast_sse("kafka_status", {"connected": False})

    try:
        result = await purge_topic_messages(bootstrap, topic)
    except Exception as e:
        return {"status": "error", "error": str(e)}

    if result.get("status") != "ok":
        return result

    _broadcast_sse("kafka_purged", {"topic": topic, "detail": result})

    if resume_bs and resume_topic and resume_conversation_id:
        started = await kafka_start(
            bootstrap=resume_bs,
            topic=resume_topic,
            conversation_id=resume_conversation_id,
        )
        return {"purge": result, **started}

    return {**result, "consumer": "stopped", "hint": "Click Start Consumer again to inspect the latest state"}


# ---------------------------------------------------------------------------
# Periodic stats push
# ---------------------------------------------------------------------------

async def _stats_pusher():
    while True:
        await asyncio.sleep(1)
        _broadcast_sse("stats", stats.snapshot())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    configure_logging(level=SETTINGS.log_level, format=SETTINGS.log_format)
    uvicorn.run(
        app,
        host=SETTINGS.host,
        port=SETTINGS.port,
        reload=False,
        log_level=SETTINGS.log_level.lower(),
        timeout_graceful_shutdown=3,
        log_config=None,
    )


if __name__ == "__main__":
    main()
