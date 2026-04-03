"""WebSocket client driver - message generation, scenario engine, and concurrent load testing."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

import websockets
import websockets.exceptions

try:
    from .settings import build_auth_token, get_settings
except ImportError:  # pragma: no cover - direct script execution fallback
    from settings import build_auth_token, get_settings

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Message generation
# ---------------------------------------------------------------------------

_TRANSCRIPTS_ZH = [
    "Hello, how can I assist you today?",
    "I'd like to review the balance on my checking account.",
    "No problem, let me pull that up for you.",
    "Your available balance is one thousand two hundred thirty-four dollars.",
    "Do you need help with anything else today?",
    "I also want to understand the current credit-card promotions.",
    "Right now we are offering twenty dollars back on one hundred dollars of spend.",
    "That promotion runs through the end of the month.",
    "Great, thanks for clarifying that.",
    "You're welcome, and have a pleasant day.",
    "I need to file a complaint about my last support experience.",
    "I'm sorry about that. Could you walk me through what happened?",
    "I waited on the phone for half an hour before someone answered.",
    "Thank you for the feedback. We'll review the queueing flow.",
    "What should I do if I forgot my password?",
    "You can reset it with a verification code sent to your phone.",
]

_TRANSCRIPTS_EN = [
    "Hello, how can I help you today?",
    "I'd like to check my account balance please.",
    "Sure, let me look that up for you.",
    "Your current balance is twelve hundred dollars.",
    "Is there anything else I can help with?",
    "Can you tell me about the rewards program?",
    "We currently have a cashback promotion running.",
    "The promotion is valid until the end of this month.",
    "Great, thank you for the information.",
    "You're welcome. Have a nice day!",
    "I need to report an unauthorized transaction.",
    "I'm sorry to hear that. Let me escalate this for you.",
    "How long will the investigation take?",
    "Typically it takes three to five business days.",
    "Can I update my mailing address?",
    "Of course, what is your new address?",
]


def _random_hex(n: int = 4) -> str:
    return uuid.uuid4().hex[:n]


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def random_transcript() -> str:
    pool = _TRANSCRIPTS_ZH + _TRANSCRIPTS_EN
    return random.choice(pool)


def generate_message(
    conversation_id: str,
    seq: int,
    *,
    event_type: str = "SESSION_ONGOING",
    start_ts: str | None = None,
) -> dict[str, Any]:
    """Generate one message that matches the InboundMessage schema."""
    now = _utc_now_iso()
    if event_type == "SESSION_COMPLETE":
        payload = {
            "sequenceNumber": seq,
            "speaker": "System",
            "transcript": "EOL",
            "engineProvider": "FanoLabs",
            "dialect": "yue-x-auto",
            "isFinal": True,
        }
    else:
        speaker = random.choice(["Agent", "Customer"])
        payload = {
            "sequenceNumber": seq,
            "speaker": speaker,
            "transcript": random_transcript(),
            "engineProvider": "FanoLabs",
            "dialect": "yue-x-auto",
            "isFinal": True,
            "speakTimeStamp": now,
            "transcriptGenerateTimeStamp": now,
        }
    return {
        "metaData": {
            "conversationId": conversation_id,
            "callStartTimeStamp": start_ts or now,
            "callEndTimeStamp": now if event_type == "SESSION_COMPLETE" else None,
            "eventType": event_type,
        },
        "payload": payload,
    }


def _session_message_split(total_messages: int) -> tuple[int, int]:
    """Split total session business messages into ONGOING count and COMPLETE seq.

    ``total_messages`` includes the final ``SESSION_COMPLETE`` frame (minimum 1; if
    only the end frame is sent, seq=0).

    Returns ``(ongoing_count, complete_seq)``.
    """
    total = max(1, total_messages)
    ongoing_count = max(0, total - 1)
    complete_seq = total - 1
    return ongoing_count, complete_seq


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

@dataclass
class Stats:
    load_running: bool = False
    sent: int = 0
    ack: int = 0
    error: int = 0
    active_connections: int = 0
    latencies: list[float] = field(default_factory=list)
    server_latencies: list[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)
    end_time: float | None = None
    recent_errors: deque = field(default_factory=lambda: deque(maxlen=100))

    def record_load_error(
        self,
        *,
        stage: str,
        cid: str,
        detail: str,
        seq: int | None = None,
        event_type: str | None = None,
        server_resp: dict | None = None,
    ) -> None:
        """Load-test path only: increment the error count, append to the ring buffer, and log it."""
        self.error += 1
        entry: dict[str, Any] = {
            "stage": stage,
            "cid": cid,
            "detail": detail[:800],
        }
        if seq is not None:
            entry["seq"] = seq
        if event_type is not None:
            entry["eventType"] = event_type
        if server_resp is not None:
            entry["server_resp"] = server_resp
        self.recent_errors.append(entry)
        log.warning("Load test error %s", entry)

    def snapshot(self) -> dict[str, Any]:
        finished_at = self.end_time if self.end_time is not None else time.monotonic()
        elapsed = max(finished_at - self.start_time, 0.001)
        send_tps = round(self.sent / elapsed, 1) if self.sent > 0 else 0.0
        ack_tps = round(self.ack / elapsed, 1) if self.ack > 0 else 0.0
        return {
            "load_running": self.load_running,
            "sent": self.sent,
            "ack": self.ack,
            "error": self.error,
            "active_connections": self.active_connections,
            "tps": ack_tps,
            "send_tps": send_tps,
            "ack_tps": ack_tps,
            "p50_ms": _percentile_ms(self.latencies, 0.50),
            "p95_ms": _percentile_ms(self.latencies, 0.95),
            "p99_ms": _percentile_ms(self.latencies, 0.99),
            "server_p50_ms": _percentile_ms(self.server_latencies, 0.50),
            "server_p95_ms": _percentile_ms(self.server_latencies, 0.95),
            "server_p99_ms": _percentile_ms(self.server_latencies, 0.99),
            "elapsed_sec": round(elapsed, 1),
            "recent_errors": list(self.recent_errors),
        }

    def finish(self) -> None:
        self.load_running = False
        if self.end_time is None:
            self.end_time = time.monotonic()

    def reset(self) -> None:
        self.load_running = False
        self.sent = 0
        self.ack = 0
        self.error = 0
        self.active_connections = 0
        self.latencies.clear()
        self.server_latencies.clear()
        self.recent_errors.clear()
        self.start_time = time.monotonic()
        self.end_time = None


# ---------------------------------------------------------------------------
# SSE event broadcasting
# ---------------------------------------------------------------------------

EventCallback = Callable[[str, dict[str, Any]], Coroutine[Any, Any, None]]
LoadStatsEmitCallback = Callable[[bool], Coroutine[Any, Any, None]]


# ---------------------------------------------------------------------------
# Scenario engine
# ---------------------------------------------------------------------------

@dataclass
class ScenarioResult:
    name: str
    passed: bool
    steps: list[dict[str, Any]] = field(default_factory=list)


def _format_server_error(resp: dict | None) -> str:
    """Extract a readable error message from a server ERROR frame."""
    if not resp:
        return ""
    err = resp.get("error") or {}
    code = err.get("code", "?")
    msg = err.get("message", "")
    details = err.get("details", "")
    parts = [f"[{code}] {msg}"]
    if details:
        parts.append(details[:300])
    return " — ".join(parts)


def _format_ws_connect_error(exc: BaseException) -> tuple[str, dict | None]:
    """Format handshake-failure details and return ``(summary, server_json_or_none)``."""
    resp = getattr(exc, "response", None)
    if resp is not None:
        sc = getattr(resp, "status_code", None) or getattr(resp, "status", None)
        body = getattr(resp, "body", None)
        server_resp: dict | None = None
        body_text = ""
        if body:
            try:
                import json
                server_resp = json.loads(body)
                err = server_resp.get("error", {})
                body_text = f"[{err.get('code', '?')}] {err.get('message', '')}"
                if err.get("details"):
                    body_text += f" — {err['details']}"
            except (json.JSONDecodeError, ValueError, AttributeError):
                try:
                    body_text = body.decode("utf-8", errors="replace").strip()[:200]
                except Exception:
                    body_text = repr(body)[:120]
        status_map = {400: "Bad request", 429: "Connection limit exceeded", 503: "Service unavailable"}
        reason = status_map.get(sc, str(exc))
        detail = f"HTTP {sc} — {reason}" + (f": {body_text}" if body_text else "")
        return detail, server_resp

    cause = exc.__cause__
    if cause:
        return f"{type(exc).__name__}: {exc} (cause: {type(cause).__name__}: {cause})", None
    return f"{type(exc).__name__}: {exc}", None


def _resolve_auth_token(auth_token: str | None) -> str | None:
    settings = get_settings()
    if not settings.auth_enabled:
        return None
    if auth_token is not None:
        normalized = auth_token.strip()
        return normalized or None
    return build_auth_token(settings)


def _build_ws_headers(auth_token: str | None) -> dict[str, str] | None:
    resolved = _resolve_auth_token(auth_token)
    if resolved is None:
        return None
    return {"Authorization": f"Bearer {resolved}"}


async def _open_ws(
    ws_url: str,
    conversation_id: str | None,
    auth_token: str | None = None,
    override_headers: dict[str, str] | None = None,
    retries: int = 3,
    retry_delay: float = 0.5,
) -> websockets.WebSocketClientProtocol:
    uri = ws_url if conversation_id is None else f"{ws_url}?conversationId={conversation_id}"
    headers = override_headers if override_headers is not None else _build_ws_headers(auth_token)
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            connect_kwargs: dict[str, Any] = {"open_timeout": 30}
            if headers is not None:
                connect_kwargs["additional_headers"] = headers
            return await websockets.connect(uri, **connect_kwargs)
        except Exception as e:
            last_exc = e
            if attempt < retries:
                await asyncio.sleep(retry_delay * attempt)
    raise last_exc  # type: ignore[misc]


async def _send_and_recv(
    ws: websockets.WebSocketClientProtocol,
    msg: dict | str,
    *,
    on_sent: Callable[[], None] | None = None,
) -> dict | None:
    """Send one message and receive one response. Return ``None`` if the connection is already closed."""
    text = msg if isinstance(msg, str) else json.dumps(msg, ensure_ascii=False)
    await ws.send(text)
    if on_sent is not None:
        on_sent()
    try:
        resp = await asyncio.wait_for(ws.recv(), timeout=10)
        return json.loads(resp)
    except (websockets.exceptions.ConnectionClosed, asyncio.TimeoutError):
        return None


def _percentile_ms(samples_sec: list[float], percentile: float) -> float:
    """Linear-interpolated percentile in milliseconds for end-to-end / server timings."""
    if not samples_sec:
        return 0.0

    ordered = sorted(samples_sec)
    if len(ordered) == 1:
        return round(ordered[0] * 1000, 2)

    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower] * 1000, 2)

    weight = position - lower
    value = ordered[lower] + (ordered[upper] - ordered[lower]) * weight
    return round(value * 1000, 2)


async def _expect_handshake_error(
    *,
    result: ScenarioResult,
    emit: EventCallback,
    action: str,
    expected_status: int,
    expected_code: str,
    unexpected_success_hint: str | None = None,
    connect: Callable[[], Coroutine[Any, Any, websockets.WebSocketClientProtocol]],
) -> ScenarioResult:
    """Run one handshake expected to fail with HTTP + JSON and record the result."""
    try:
        ws = await connect()
    except Exception as exc:
        detail, srv_resp = _format_ws_connect_error(exc)
        status_code = getattr(getattr(exc, "response", None), "status_code", None) or getattr(
            getattr(exc, "response", None), "status", None
        )
        step = {
            "action": action,
            "resp_type": f"HTTP {status_code}" if status_code is not None else "HTTP ?",
        }
        if srv_resp:
            step["error_code"] = (srv_resp.get("error") or {}).get("code")
        if status_code != expected_status or step.get("error_code") != expected_code:
            result.passed = False
            step["error"] = (
                f"Expected HTTP {expected_status} / {expected_code}, got: {detail}"
            )
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})
        return result

    result.passed = False
    error = (
        f"Handshake unexpectedly succeeded; expected HTTP {expected_status} / {expected_code}"
    )
    if unexpected_success_hint:
        error = f"{error}. {unexpected_success_hint}"
    step = {
        "action": action,
        "error": error,
    }
    result.steps.append(step)
    await emit("scenario_step", {"scenario": result.name, "step": step})
    try:
        await ws.close()
    except Exception:
        pass
    return result


async def _send_expect_error_and_close(
    ws: websockets.WebSocketClientProtocol,
    msg: dict | str,
    *,
    action: str,
    expected_code: str,
    expected_close: int,
    expected_conversation_id: str | None = None,
    result: ScenarioResult,
    emit: EventCallback,
) -> None:
    """Send a message expected to trigger ERROR + Close, then verify the error and close codes."""
    resp = await _send_and_recv(ws, msg)
    step = {"action": action, "resp_type": resp.get("metaData", {}).get("eventType") if resp else None}
    if resp and resp.get("metaData", {}).get("eventType") == "ERROR":
        err_code = resp.get("error", {}).get("code")
        step["error_code"] = err_code
        step["conversation_id"] = resp.get("metaData", {}).get("conversationId")
        if err_code != expected_code:
            result.passed = False
            step["error"] = f"Expected {expected_code}, got {err_code}"
        elif (
            expected_conversation_id is not None
            and step.get("conversation_id") != expected_conversation_id
        ):
            result.passed = False
            step["error"] = (
                f"Expected conversationId={expected_conversation_id}, "
                f"got {step.get('conversation_id')}"
            )
    else:
        result.passed = False
        step["error"] = "Expected an ERROR frame"
    result.steps.append(step)
    await emit("scenario_step", {"scenario": result.name, "step": step})

    try:
        await asyncio.wait_for(ws.wait_closed(), timeout=5)
        close_code = ws.close_code
        step_c = {"action": "verify_close", "close_code": close_code}
        if close_code != expected_close:
            result.passed = False
            step_c["error"] = f"Expected close_code={expected_close}, got {close_code}"
        result.steps.append(step_c)
        await emit("scenario_step", {"scenario": result.name, "step": step_c})
    except asyncio.TimeoutError:
        result.passed = False
        result.steps.append({"action": "verify_close", "error": "Timed out waiting for close"})


async def _session_ongoing_plus_complete_and_close(
    ws: Any,
    cid: str,
    meta_base: dict[str, Any],
    n_messages: int,
    result: ScenarioResult,
    emit: EventCallback,
) -> None:
    """Send ONGOING messages plus SESSION_COMPLETE based on the total session message count, then verify close 1000."""
    ongoing_count, complete_seq = _session_message_split(n_messages)
    for seq in range(ongoing_count):
        msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
        resp = await _send_and_recv(ws, msg)
        step = {
            "action": "send_ongoing",
            "seq": seq,
            "resp_type": resp.get("metaData", {}).get("eventType") if resp else None,
        }
        if not resp or resp.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
            result.passed = False
            step["error"] = "Expected TRANSCRIPT_ACK"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})

    msg = generate_message(cid, complete_seq, event_type="SESSION_COMPLETE", **meta_base)
    resp = await _send_and_recv(ws, msg)
    step = {
        "action": "send_complete",
        "seq": complete_seq,
        "resp_type": resp.get("metaData", {}).get("eventType") if resp else None,
    }
    if not resp or resp.get("metaData", {}).get("eventType") != "EOL_ACK":
        result.passed = False
        step["error"] = "Expected EOL_ACK"
    result.steps.append(step)
    await emit("scenario_step", {"scenario": result.name, "step": step})

    try:
        await asyncio.wait_for(ws.wait_closed(), timeout=5)
        close_code = ws.close_code
        step = {"action": "verify_close", "close_code": close_code}
        if close_code != 1000:
            result.passed = False
            step["error"] = f"Expected close_code=1000, got {close_code}"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})
    except asyncio.TimeoutError:
        result.passed = False
        result.steps.append({"action": "verify_close", "error": "Timed out waiting for close"})


async def _session_ongoing_only(
    ws: Any,
    cid: str,
    meta_base: dict[str, Any],
    n_messages: int,
    result: ScenarioResult,
    emit: EventCallback,
) -> None:
    """For N-01, send only SESSION_ONGOING and verify every message returns TRANSCRIPT_ACK."""
    total = max(1, n_messages)
    for seq in range(total):
        msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
        resp = await _send_and_recv(ws, msg)
        step = {
            "action": "send_ongoing",
            "seq": seq,
            "resp_type": resp.get("metaData", {}).get("eventType") if resp else None,
        }
        if not resp or resp.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
            result.passed = False
            step["error"] = "Expected TRANSCRIPT_ACK"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})


async def scenario_a_normal_flow(
    ws_url: str, emit: EventCallback, n_messages: int = 5,
) -> ScenarioResult:
    """N-01: normal in-session processing with ``SESSION_ONGOING`` only."""
    result = ScenarioResult(name="N-01", passed=True)
    cid = f"mock-N01-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        await _session_ongoing_only(
            ws, cid, meta_base, n_messages, result, emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_b_idempotent(
    ws_url: str,
    emit: EventCallback,
    n_messages: int = 5,
) -> ScenarioResult:
    """N-02: idempotent replay. For each ``seq`` in ``[0, n_messages)``, send ``SESSION_ONGOING`` once and then replay the same frame; both sends should ACK."""
    result = ScenarioResult(name="N-02", passed=True)
    cid = f"mock-N02-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        for seq in range(max(1, n_messages)):
            msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
            resp1 = await _send_and_recv(ws, msg)
            step1 = {
                "action": "send_first",
                "seq": seq,
                "resp_type": resp1.get("metaData", {}).get("eventType") if resp1 else None,
            }
            if not resp1 or resp1.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
                result.passed = False
                step1["error"] = "Expected ACK on first send"
            result.steps.append(step1)
            await emit("scenario_step", {"scenario": result.name, "step": step1})

            resp2 = await _send_and_recv(ws, msg)
            step2 = {
                "action": "send_duplicate",
                "seq": seq,
                "resp_type": resp2.get("metaData", {}).get("eventType") if resp2 else None,
            }
            if not resp2 or resp2.get("metaData", {}).get("eventType") != "TRANSCRIPT_ACK":
                result.passed = False
                step2["error"] = "Expected ACK on replay (idempotent)"
            result.steps.append(step2)
            await emit("scenario_step", {"scenario": result.name, "step": step2})
    finally:
        await ws.close()

    return result


async def scenario_c_out_of_order(
    ws_url: str,
    emit: EventCallback,
    n_messages: int = 5,
) -> ScenarioResult:
    """E-09: out-of-order sequence. Send seq 0, then jump to seq ``jump`` (``jump=max(2, n_messages)``), expecting E1006 + close 1008."""
    result = ScenarioResult(name="E-09", passed=True)
    jump_seq = max(2, n_messages)
    cid = f"mock-E09-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        # seq 0 is valid.
        msg0 = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        resp0 = await _send_and_recv(ws, msg0)
        step0 = {"action": "send_seq0", "resp_type": resp0.get("metaData", {}).get("eventType") if resp0 else None}
        result.steps.append(step0)
        await emit("scenario_step", {"scenario": result.name, "step": step0})

        # Out of order: skip the intermediate seq values.
        msg5 = generate_message(cid, jump_seq, event_type="SESSION_ONGOING", **meta_base)
        resp5 = await _send_and_recv(ws, msg5)
        step5 = {
            "action": f"send_seq{jump_seq}_ooo",
            "resp_type": resp5.get("metaData", {}).get("eventType") if resp5 else None,
        }
        if resp5 and resp5.get("metaData", {}).get("eventType") == "ERROR":
            err_code = resp5.get("error", {}).get("code")
            step5["error_code"] = err_code
            if err_code != "E1006":
                result.passed = False
                step5["error"] = f"Expected E1006, got {err_code}"
        else:
            result.passed = False
            step5["error"] = "Expected an ERROR frame"
        result.steps.append(step5)
        await emit("scenario_step", {"scenario": result.name, "step": step5})

        # Verify the close code.
        try:
            await asyncio.wait_for(ws.wait_closed(), timeout=5)
            close_code = ws.close_code
            step_c = {"action": "verify_close", "close_code": close_code}
            if close_code != 1008:
                result.passed = False
                step_c["error"] = f"Expected close_code=1008, got {close_code}"
            result.steps.append(step_c)
            await emit("scenario_step", {"scenario": result.name, "step": step_c})
        except asyncio.TimeoutError:
            result.passed = False
            result.steps.append({"action": "verify_close", "error": "Timed out waiting for close"})
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_d1_invalid_json(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-04: invalid JSON -> E1001 + close 1007."""
    result = ScenarioResult(name="E-04", passed=True)
    cid = f"mock-E04-{_random_hex(6)}"
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        resp = await _send_and_recv(ws, "this is not valid json!!!")
        step = {"action": "send_bad_json", "resp_type": resp.get("metaData", {}).get("eventType") if resp else None}
        if resp and resp.get("error", {}).get("code") == "E1001":
            step["error_code"] = "E1001"
        else:
            result.passed = False
            step["error"] = "Expected E1001"
        result.steps.append(step)
        await emit("scenario_step", {"scenario": result.name, "step": step})

        try:
            await asyncio.wait_for(ws.wait_closed(), timeout=5)
            close_code = ws.close_code
            step_c = {"action": "verify_close", "close_code": close_code}
            if close_code != 1007:
                result.passed = False
                step_c["error"] = f"Expected close_code=1007, got {close_code}"
            result.steps.append(step_c)
            await emit("scenario_step", {"scenario": result.name, "step": step_c})
        except asyncio.TimeoutError:
            result.passed = False
            result.steps.append({"action": "verify_close", "error": "Timed out waiting for close"})
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e01_missing_query_conversation_id(
    ws_url: str,
    emit: EventCallback,
) -> ScenarioResult:
    """E-01: missing query ``conversationId`` during the handshake should return HTTP 400 / E1003."""
    result = ScenarioResult(name="E-01", passed=True)
    return await _expect_handshake_error(
        result=result,
        emit=emit,
        action="connect_without_query",
        expected_status=400,
        expected_code="E1003",
        connect=lambda: _open_ws(ws_url, None, retries=1),
    )


async def scenario_d2_schema_error(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-06: missing required fields -> ERROR + close 1008."""
    result = ScenarioResult(name="E-06", passed=True)
    cid = f"mock-E06-{_random_hex(6)}"
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = {
            "metaData": {"eventType": "SESSION_ONGOING"},
            "payload": {"dialect": "yue-x-auto"},
        }
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_bad_schema",
            expected_code="E1003",
            expected_close=1008,
            expected_conversation_id=cid,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e05_invalid_enum(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-05: invalid enum value, expecting E1002 + close 1008."""
    result = ScenarioResult(name="E-05", passed=True)
    cid = f"mock-E05-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["metaData"]["eventType"] = "INVALID"
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_bad_enum",
            expected_code="E1002",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e07_wrong_type(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-07: wrong field type, covering both non-object top-level JSON and field type mismatches."""
    result = ScenarioResult(name="E-07", passed=True)
    cid = f"mock-E07-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    async def _run_subcase(msg: dict | str, *, action: str) -> None:
        try:
            ws = await _open_ws(ws_url, cid)
        except Exception as e:
            result.passed = False
            result.steps.append({"action": "connect", "error": str(e)})
            await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
            return

        try:
            await _send_expect_error_and_close(
                ws,
                msg,
                action=action,
                expected_code="E1004",
                expected_close=1008,
                expected_conversation_id=cid,
                result=result,
                emit=emit,
            )
        finally:
            try:
                await ws.close()
            except Exception:
                pass

    await _run_subcase("[]", action="send_non_object_json")

    bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
    bad_msg["metaData"]["conversationId"] = 123
    await _run_subcase(bad_msg, action="send_wrong_type_field")

    return result


async def scenario_e08_invalid_timestamp(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-08: invalid timestamp format, expecting E1005 + close 1008."""
    result = ScenarioResult(name="E-08", passed=True)
    cid = f"mock-E08-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["payload"]["speakTimeStamp"] = "2025-03-21T18:32:20.000+08:00"
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_bad_timestamp",
            expected_code="E1005",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result




async def scenario_e14_conversation_id_mismatch(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-14: query/body conversationId mismatch, expecting E1009 + close 1008."""
    result = ScenarioResult(name="E-14", passed=True)
    cid = f"mock-E14-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["metaData"]["conversationId"] = f"{cid}-other"
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_conversation_id_mismatch",
            expected_code="E1009",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e15_business_rule_violation(ws_url: str, emit: EventCallback) -> ScenarioResult:
    """E-15: business-rule validation failure, expecting E1009 + close 1008."""
    result = ScenarioResult(name="E-15", passed=True)
    cid = f"mock-E15-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        bad_msg = generate_message(cid, 0, event_type="SESSION_ONGOING", **meta_base)
        bad_msg["payload"]["isFinal"] = False
        await _send_expect_error_and_close(
            ws,
            bad_msg,
            action="send_business_rule_violation",
            expected_code="E1009",
            expected_close=1008,
            result=result,
            emit=emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


async def scenario_e16_second_concurrent_sender(
    ws_url: str,
    emit: EventCallback,
) -> ScenarioResult:
    """E-16: second concurrent sender should be rejected during handshake with HTTP 403 / E1009."""
    result = ScenarioResult(name="E-16", passed=True)
    cid = f"mock-E16-{_random_hex(6)}"
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})

    try:
        primary_ws = await _open_ws(ws_url, cid, retries=1)
    except Exception as exc:
        result.passed = False
        result.steps.append({"action": "connect_primary_sender", "error": str(exc)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        await _expect_handshake_error(
            result=result,
            emit=emit,
            action="connect_second_sender",
            expected_status=403,
            expected_code="E1009",
            connect=lambda: _open_ws(ws_url, cid, retries=1),
        )
    finally:
        try:
            await primary_ws.close()
        except Exception:
            pass

    return result


async def scenario_e17_invalid_bearer_jwt(
    ws_url: str,
    emit: EventCallback,
) -> ScenarioResult:
    """E-17: invalid Bearer JWT should be rejected during handshake with HTTP 401 / E1010."""
    result = ScenarioResult(name="E-17", passed=True)
    cid = f"mock-E17-{_random_hex(6)}"
    await emit("conversation_registered", {"conversation_id": cid, "scenario": result.name})
    return await _expect_handshake_error(
        result=result,
        emit=emit,
        action="connect_with_invalid_bearer",
        expected_status=401,
        expected_code="E1010",
        unexpected_success_hint=(
            "The target service likely has AUTH_ENABLED=false or is not loading the expected JWT auth config"
        ),
        connect=lambda: _open_ws(
            ws_url,
            cid,
            retries=1,
            override_headers={"Authorization": "Bearer invalid.jwt.token"},
        ),
    )


async def scenario_g_session_complete(
    ws_url: str,
    emit: EventCallback,
    n_messages: int = 5,
) -> ScenarioResult:
    """N-03: session-complete flow. ``n_messages`` is the total number of session messages including COMPLETE; the final outcome is ACK + close 1000."""
    result = ScenarioResult(name="N-03", passed=True)
    cid = f"mock-N03-{_random_hex(6)}"
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    await emit(
                "conversation_registered",
                {"conversation_id": cid, "scenario": result.name},
            )

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        result.passed = False
        result.steps.append({"action": "connect", "error": str(e)})
        await emit("scenario_step", {"scenario": result.name, "step": result.steps[-1]})
        return result

    try:
        await _session_ongoing_plus_complete_and_close(
            ws, cid, meta_base, n_messages, result, emit,
        )
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return result


SCENARIOS: dict[str, Any] = {
    "N-01": scenario_a_normal_flow,
    "N-02": scenario_b_idempotent,
    "N-03": scenario_g_session_complete,
    "E-01": scenario_e01_missing_query_conversation_id,
    "E-04": scenario_d1_invalid_json,
    "E-05": scenario_e05_invalid_enum,
    "E-06": scenario_d2_schema_error,
    "E-07": scenario_e07_wrong_type,
    "E-08": scenario_e08_invalid_timestamp,
    "E-09": scenario_c_out_of_order,
    "E-14": scenario_e14_conversation_id_mismatch,
    "E-15": scenario_e15_business_rule_violation,
    "E-16": scenario_e16_second_concurrent_sender,
    "E-17": scenario_e17_invalid_bearer_jwt,
}


# ---------------------------------------------------------------------------
# Concurrent load-test driver
# ---------------------------------------------------------------------------

async def _load_single_conversation(
    ws_url: str,
    stats: Stats,
    emit: EventCallback,
    n_messages: int,
    interval_ms: float,
    *,
    emit_stats: LoadStatsEmitCallback | None = None,
    sse_register_cid: bool = False,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Single-conversation load test with ``n_messages`` business messages, including one ``SESSION_COMPLETE``.

    ``sse_register_cid`` controls whether to emit ``conversation_registered`` to the
    UI. It defaults to ``False`` under high load to avoid flooding SSE.
    """
    ongoing_count, complete_seq = _session_message_split(n_messages)
    cid = f"load-{_random_hex(8)}"
    if sse_register_cid:
        await emit("conversation_registered", {"conversation_id": cid})
    start_ts = _utc_now_iso()
    meta_base = {"start_ts": start_ts}
    interval = interval_ms / 1000.0

    def mark_sent() -> None:
        stats.sent += 1

    try:
        ws = await _open_ws(ws_url, cid)
    except Exception as e:
        detail, srv_resp = _format_ws_connect_error(e)
        stats.record_load_error(stage="connect", cid=cid, detail=detail, server_resp=srv_resp)
        await emit("load_error", {"cid": cid, "stage": "connect", "error": detail, "server_resp": srv_resp})
        if emit_stats is not None:
            await emit_stats(False)
        return

    stats.active_connections += 1
    if emit_stats is not None:
        await emit_stats(False)
    try:
        for seq in range(ongoing_count):
            if stop_event and stop_event.is_set():
                break
            msg = generate_message(cid, seq, event_type="SESSION_ONGOING", **meta_base)
            t0 = time.monotonic()
            resp = await _send_and_recv(ws, msg, on_sent=mark_sent)
            latency = time.monotonic() - t0
            stats.latencies.append(latency)
            if resp and resp.get("metaData", {}).get("eventType") == "TRANSCRIPT_ACK":
                stats.ack += 1
                srv_ms = (resp.get("payload") or {}).get("serverProcessingMs")
                if isinstance(srv_ms, (int, float)):
                    stats.server_latencies.append(float(srv_ms) / 1000.0)
            else:
                et = resp.get("metaData", {}).get("eventType") if resp else None
                if resp is None:
                    detail = "No response (10s timeout or connection already closed)"
                elif et == "ERROR":
                    detail = f"Server error: {_format_server_error(resp)}"
                else:
                    detail = f"Expected TRANSCRIPT_ACK, got eventType={et!r}"
                stats.record_load_error(
                    stage="ongoing",
                    cid=cid,
                    seq=seq,
                    detail=detail,
                    event_type=et,
                    server_resp=resp,
                )
                await emit(
                    "load_error",
                    {"cid": cid, "stage": "ongoing", "seq": seq, "error": detail, "server_resp": resp},
                )
            if emit_stats is not None:
                await emit_stats(False)
            if interval > 0:
                await asyncio.sleep(interval)

        if stop_event and stop_event.is_set():
            return

        # SESSION_COMPLETE
        msg = generate_message(cid, complete_seq, event_type="SESSION_COMPLETE", **meta_base)
        t0 = time.monotonic()
        resp = await _send_and_recv(ws, msg, on_sent=mark_sent)
        latency = time.monotonic() - t0
        stats.latencies.append(latency)
        if resp and resp.get("metaData", {}).get("eventType") == "EOL_ACK":
            stats.ack += 1
            srv_ms = (resp.get("payload") or {}).get("serverProcessingMs")
            if isinstance(srv_ms, (int, float)):
                stats.server_latencies.append(float(srv_ms) / 1000.0)
        else:
            et = resp.get("metaData", {}).get("eventType") if resp else None
            if resp is None:
                detail = "No response (10s timeout or connection already closed)"
            elif et == "ERROR":
                detail = f"Server error: {_format_server_error(resp)}"
            else:
                detail = f"Expected EOL_ACK, got eventType={et!r}"
            stats.record_load_error(
                stage="complete",
                cid=cid,
                seq=complete_seq,
                detail=detail,
                event_type=et,
                server_resp=resp,
            )
            await emit(
                "load_error",
                {
                    "cid": cid,
                    "stage": "complete",
                    "seq": complete_seq,
                    "error": detail,
                    "server_resp": resp,
                },
            )
        if emit_stats is not None:
            await emit_stats(False)
    except Exception as e:
        stats.record_load_error(stage="exception", cid=cid, detail=repr(e))
        await emit("load_error", {"cid": cid, "stage": "exception", "error": str(e)})
        if emit_stats is not None:
            await emit_stats(False)
    finally:
        stats.active_connections -= 1
        if emit_stats is not None:
            await emit_stats(False)
        try:
            await ws.close()
        except Exception:
            pass


async def run_load_test(
    ws_url: str,
    stats: Stats,
    emit: EventCallback,
    concurrency: int = 10,
    messages_per_conv: int = 10,
    interval_ms: float = 20,
    ramp_up_ms: float = 0,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Concurrent load test: one round equals ``concurrency`` independent sessions, each on its own WebSocket.

    Create ``concurrency`` asyncio tasks with a semaphore of the same size, so the
    whole batch connects and stays online at the same time until each session finishes.

    If ``ramp_up_ms`` is greater than 0, start all connections evenly across that
    interval (linear ramp-up) to avoid an instantaneous surge that saturates the
    server TCP backlog. ``0`` means unlimited rate and all connections start immediately.
    """
    err_sse_cap = min(2000, max(100, concurrency + 200))
    err_sse_left = err_sse_cap
    last_stats_emit_at = 0.0
    stats_emit_interval_sec = 0.15

    async def emit_throttled(event_type: str, data: dict[str, Any]) -> None:
        nonlocal err_sse_left
        if event_type == "load_error":
            if err_sse_left <= 0:
                return
            err_sse_left -= 1
        await emit(event_type, data)

    async def emit_progress_stats(force: bool = False) -> None:
        nonlocal last_stats_emit_at
        now = time.monotonic()
        if not force and now - last_stats_emit_at < stats_emit_interval_sec:
            return
        last_stats_emit_at = now
        await emit_throttled("stats", stats.snapshot())

    await emit_throttled("stats", stats.snapshot())
    sem = asyncio.Semaphore(concurrency)
    tasks: list[asyncio.Task] = []

    ramp_interval = (ramp_up_ms / 1000.0 / concurrency) if ramp_up_ms > 0 and concurrency > 1 else 0

    async def _guarded(idx: int) -> None:
        if ramp_interval > 0:
            await asyncio.sleep(ramp_interval * idx)
        async with sem:
            if stop_event and stop_event.is_set():
                return
            await _load_single_conversation(
                ws_url,
                stats,
                emit_throttled,
                messages_per_conv,
                interval_ms,
                emit_stats=emit_progress_stats,
                sse_register_cid=False,
                stop_event=stop_event,
            )

    batch_size = concurrency
    for i in range(batch_size):
        if stop_event and stop_event.is_set():
            break
        tasks.append(asyncio.create_task(_guarded(i)))

    await asyncio.gather(*tasks, return_exceptions=True)
    load_cancelled = bool(stop_event and stop_event.is_set())
    stats.finish()
    await emit_throttled("load_done", {**stats.snapshot(), "load_cancelled": load_cancelled})
