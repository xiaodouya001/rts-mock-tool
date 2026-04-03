"""Additional coverage tests for mock_tool.ws_driver."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

import mock_tool.ws_driver as ws_driver


class _FakeWs:
    def __init__(self, *, close_code: int = 1000, wait_closed_side_effect=None, close_side_effect=None):
        self.close_code = close_code
        self.send = AsyncMock()
        self.recv = AsyncMock()
        self.close = AsyncMock(side_effect=close_side_effect)
        self.wait_closed = AsyncMock(side_effect=wait_closed_side_effect, return_value=None)


async def _collect(events: list[tuple[str, dict]], event_type: str, data: dict) -> None:
    events.append((event_type, data))


def test_stats_helpers_and_percentiles(monkeypatch):
    stats = ws_driver.Stats()
    log_warning = MagicMock()
    monkeypatch.setattr(ws_driver.log, "warning", log_warning)

    stats.record_load_error(
        stage="ongoing",
        cid="cid-1",
        detail="boom",
        seq=3,
        event_type="SESSION_ONGOING",
        server_resp={"status": "bad"},
    )
    snapshot = stats.snapshot()
    old_end = stats.end_time
    stats.finish()
    stats.finish()
    stats.reset()

    assert snapshot["recent_errors"][0]["seq"] == 3
    assert snapshot["recent_errors"][0]["eventType"] == "SESSION_ONGOING"
    assert snapshot["recent_errors"][0]["server_resp"] == {"status": "bad"}
    assert old_end is None
    assert stats.end_time is None
    assert ws_driver._format_server_error(None) == ""
    assert ws_driver._format_server_error(
        {"error": {"code": "E1001", "message": "bad", "details": "detail"}}
    ) == "[E1001] bad — detail"
    assert ws_driver._percentile_ms([], 0.5) == 0.0
    assert ws_driver._percentile_ms([0.2], 0.5) == 200.0
    assert ws_driver._percentile_ms([0.1, 0.2, 0.3], 0.0) == 100.0


def test_format_ws_connect_error_variants():
    response_exc = RuntimeError("boom")
    response_exc.response = SimpleNamespace(
        status_code=400,
        body=b'{"error":{"code":"E1003","message":"bad","details":"detail"}}',
    )
    detail, server_resp = ws_driver._format_ws_connect_error(response_exc)
    assert "HTTP 400" in detail
    assert server_resp["error"]["code"] == "E1003"

    raw_exc = RuntimeError("boom")
    raw_exc.response = SimpleNamespace(status=503, body=b"\xff")
    detail, server_resp = ws_driver._format_ws_connect_error(raw_exc)
    assert "HTTP 503" in detail
    assert server_resp is None

    cause_exc = RuntimeError("outer")
    cause_exc.__cause__ = ValueError("inner")
    detail, server_resp = ws_driver._format_ws_connect_error(cause_exc)
    assert "cause: ValueError: inner" in detail
    assert server_resp is None

    plain_detail, plain_resp = ws_driver._format_ws_connect_error(RuntimeError("plain"))
    assert plain_detail == "RuntimeError: plain"
    assert plain_resp is None

    bodyless_exc = RuntimeError("bodyless")
    bodyless_exc.response = SimpleNamespace(status_code=400, body=None)
    detail, _ = ws_driver._format_ws_connect_error(bodyless_exc)
    assert "HTTP 400" in detail

    repr_exc = RuntimeError("repr-body")
    repr_exc.response = SimpleNamespace(status=400, body="{")
    detail, _ = ws_driver._format_ws_connect_error(repr_exc)
    assert "'{'" in detail


def test_auth_resolution_and_header_helpers(monkeypatch):
    monkeypatch.setattr(
        ws_driver,
        "get_settings",
        lambda: SimpleNamespace(auth_enabled=False),
    )
    assert ws_driver._resolve_auth_token("explicit") is None

    monkeypatch.setattr(
        ws_driver,
        "get_settings",
        lambda: SimpleNamespace(auth_enabled=True),
    )
    monkeypatch.setattr(ws_driver, "build_auth_token", lambda _settings: "generated")
    assert ws_driver._resolve_auth_token("  ") is None
    assert ws_driver._resolve_auth_token(None) == "generated"
    assert ws_driver._build_ws_headers(None) == {"Authorization": "Bearer generated"}


@pytest.mark.asyncio
async def test_open_ws_retries_then_succeeds_and_then_raises(monkeypatch):
    sleep = AsyncMock()
    connect = AsyncMock(side_effect=[RuntimeError("boom"), AsyncMock()])
    monkeypatch.setattr(ws_driver.asyncio, "sleep", sleep)
    monkeypatch.setattr(ws_driver.websockets, "connect", connect)
    monkeypatch.setattr(ws_driver, "_build_ws_headers", lambda _token: None)

    ws = await ws_driver._open_ws("ws://unit-test", "cid-1", retries=2)

    assert ws is not None
    sleep.assert_awaited_once_with(0.5)

    connect = AsyncMock(side_effect=[RuntimeError("boom1"), RuntimeError("boom2")])
    monkeypatch.setattr(ws_driver.websockets, "connect", connect)
    with pytest.raises(RuntimeError, match="boom2"):
        await ws_driver._open_ws("ws://unit-test", "cid-1", retries=2)


@pytest.mark.asyncio
async def test_send_expect_error_and_close_branches():
    events: list[tuple[str, dict]] = []
    result = ws_driver.ScenarioResult(name="X", passed=True)
    ws = _FakeWs(close_code=999)

    send_and_recv = AsyncMock(
        side_effect=[
            {"metaData": {"eventType": "ERROR", "conversationId": "cid-1"}, "error": {"code": "E9999"}},
            {"metaData": {"eventType": "ERROR", "conversationId": "wrong"}, "error": {"code": "E1001"}},
            {"metaData": {"eventType": "ACK"}},
        ]
    )
    original = ws_driver._send_and_recv
    ws_driver._send_and_recv = send_and_recv
    try:
        await ws_driver._send_expect_error_and_close(
            ws,
            {"bad": True},
            action="bad",
            expected_code="E1001",
            expected_close=1008,
            result=result,
            emit=lambda event_type, data: _collect(events, event_type, data),
        )
        await ws_driver._send_expect_error_and_close(
            ws,
            {"bad": True},
            action="bad",
            expected_code="E1001",
            expected_close=1008,
            expected_conversation_id="cid-1",
            result=result,
            emit=lambda event_type, data: _collect(events, event_type, data),
        )
        ws.wait_closed = AsyncMock(side_effect=asyncio.TimeoutError)
        await ws_driver._send_expect_error_and_close(
            ws,
            {"bad": True},
            action="bad",
            expected_code="E1001",
            expected_close=1008,
            result=result,
            emit=lambda event_type, data: _collect(events, event_type, data),
        )
    finally:
        ws_driver._send_and_recv = original

    assert result.passed is False
    assert any(data["step"].get("error") == "Expected an ERROR frame" for event_type, data in events if event_type == "scenario_step")


@pytest.mark.asyncio
async def test_session_helpers_cover_error_paths(monkeypatch):
    events: list[tuple[str, dict]] = []
    result = ws_driver.ScenarioResult(name="X", passed=True)
    ws = _FakeWs(close_code=999)

    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[None, None]),
    )
    await ws_driver._session_ongoing_only(
        ws,
        "cid-1",
        {"start_ts": "2026-03-31T00:00:00.000Z"},
        1,
        result,
        lambda event_type, data: _collect(events, event_type, data),
    )

    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[None, None]),
    )
    await ws_driver._session_ongoing_plus_complete_and_close(
        ws,
        "cid-1",
        {"start_ts": "2026-03-31T00:00:00.000Z"},
        1,
        result,
        lambda event_type, data: _collect(events, event_type, data),
    )

    ws.wait_closed = AsyncMock(side_effect=asyncio.TimeoutError)
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "TRANSCRIPT_ACK"}},
                {"metaData": {"eventType": "EOL_ACK"}},
            ]
        ),
    )
    await ws_driver._session_ongoing_plus_complete_and_close(
        ws,
        "cid-1",
        {"start_ts": "2026-03-31T00:00:00.000Z"},
        2,
        result,
        lambda event_type, data: _collect(events, event_type, data),
    )

    assert result.passed is False
    assert any("Timed out waiting for close" in step.get("error", "") for step in result.steps)


@pytest.mark.asyncio
async def test_session_helpers_cover_remaining_ack_branches(monkeypatch):
    events: list[tuple[str, dict]] = []
    result = ws_driver.ScenarioResult(name="Y", passed=True)
    ws = _FakeWs(close_code=1000)

    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                None,
                {"metaData": {"eventType": "EOL_ACK"}},
            ]
        ),
    )
    await ws_driver._session_ongoing_plus_complete_and_close(
        ws,
        "cid-1",
        {"start_ts": "2026-03-31T00:00:00.000Z"},
        2,
        result,
        lambda event_type, data: _collect(events, event_type, data),
    )

    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(return_value={"metaData": {"eventType": "TRANSCRIPT_ACK"}}),
    )
    await ws_driver._session_ongoing_only(
        ws,
        "cid-1",
        {"start_ts": "2026-03-31T00:00:00.000Z"},
        1,
        result,
        lambda event_type, data: _collect(events, event_type, data),
    )

    assert any(step["action"] == "send_ongoing" and step.get("error") == "Expected TRANSCRIPT_ACK" for step in result.steps)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario_fn",
    [
        ws_driver.scenario_a_normal_flow,
        ws_driver.scenario_b_idempotent,
        ws_driver.scenario_c_out_of_order,
        ws_driver.scenario_d1_invalid_json,
        ws_driver.scenario_d2_schema_error,
        ws_driver.scenario_e05_invalid_enum,
        ws_driver.scenario_e07_wrong_type,
        ws_driver.scenario_e08_invalid_timestamp,
        ws_driver.scenario_e14_conversation_id_mismatch,
        ws_driver.scenario_e15_business_rule_violation,
        ws_driver.scenario_e16_second_concurrent_sender,
        ws_driver.scenario_e17_invalid_bearer_jwt,
        ws_driver.scenario_g_session_complete,
    ],
)
async def test_scenarios_return_connect_error_when_open_ws_fails(monkeypatch, scenario_fn):
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=RuntimeError("connect failed")))

    result = await scenario_fn("ws://unit-test", lambda _event_type, _data: asyncio.sleep(0))

    assert result.passed is False
    assert result.steps[0]["action"].startswith("connect")


@pytest.mark.asyncio
async def test_scenario_a_b_c_and_d1_cover_success_and_error_paths(monkeypatch):
    events: list[tuple[str, dict]] = []
    ws = _FakeWs(close_side_effect=RuntimeError("close failed"), close_code=1008)
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))

    monkeypatch.setattr(ws_driver, "_session_ongoing_only", AsyncMock(return_value=None))
    result = await ws_driver.scenario_a_normal_flow(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
    )
    assert result.passed is True

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[None, {"metaData": {"eventType": "TRANSCRIPT_ACK"}}]),
    )
    result = await ws_driver.scenario_b_idempotent(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
        n_messages=1,
    )
    assert result.passed is False

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[{"metaData": {"eventType": "TRANSCRIPT_ACK"}}, None]),
    )
    result = await ws_driver.scenario_b_idempotent(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
        n_messages=1,
    )
    assert result.passed is False

    ws = _FakeWs(close_code=999)
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "TRANSCRIPT_ACK"}},
                {"metaData": {"eventType": "ERROR"}, "error": {"code": "WRONG"}},
            ]
        ),
    )
    result = await ws_driver.scenario_c_out_of_order(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
        n_messages=1,
    )
    assert result.passed is False

    ws = _FakeWs(close_code=1008, wait_closed_side_effect=asyncio.TimeoutError, close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "TRANSCRIPT_ACK"}},
                {"metaData": {"eventType": "ACK"}},
            ]
        ),
    )
    result = await ws_driver.scenario_c_out_of_order(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
        n_messages=1,
    )
    assert result.passed is False

    ws = _FakeWs(close_code=999, close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(return_value={"metaData": {"eventType": "ERROR"}, "error": {"code": "E1001"}}),
    )
    result = await ws_driver.scenario_d1_invalid_json(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
    )
    assert result.passed is False


@pytest.mark.asyncio
async def test_scenario_c_success_path_and_d1_remaining_branches(monkeypatch):
    events: list[tuple[str, dict]] = []

    ws = _FakeWs(close_code=1008)
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "TRANSCRIPT_ACK"}},
                {"metaData": {"eventType": "ERROR"}, "error": {"code": "E1006"}},
            ]
        ),
    )
    result = await ws_driver.scenario_c_out_of_order(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
        n_messages=1,
    )
    assert result.passed is True

    ws = _FakeWs(wait_closed_side_effect=asyncio.TimeoutError, close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(return_value={"metaData": {"eventType": "ACK"}}),
    )
    result = await ws_driver.scenario_d1_invalid_json(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
    )
    assert result.passed is False

    ws = _FakeWs(close_code=1007)
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(return_value={"metaData": {"eventType": "ERROR"}, "error": {"code": "E1001"}}),
    )
    result = await ws_driver.scenario_d1_invalid_json(
        "ws://unit-test",
        lambda event_type, data: _collect(events, event_type, data),
    )
    assert result.passed is True


@pytest.mark.asyncio
async def test_scenario_e01_covers_expected_and_unexpected_handshake_outcomes(monkeypatch):
    class _HandshakeError(RuntimeError):
        pass

    err = _HandshakeError("bad handshake")
    err.response = SimpleNamespace(
        status_code=400,
        body=b'{"error":{"code":"E1003","message":"bad"}}',
    )
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=err))

    result = await ws_driver.scenario_e01_missing_query_conversation_id(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is True

    err = _HandshakeError("bad handshake")
    err.response = SimpleNamespace(
        status_code=429,
        body=b'{"error":{"code":"OTHER","message":"bad"}}',
    )
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=err))
    result = await ws_driver.scenario_e01_missing_query_conversation_id(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is False

    err = _HandshakeError("bad handshake")
    err.response = SimpleNamespace(status_code=429, body=None)
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=err))
    result = await ws_driver.scenario_e01_missing_query_conversation_id(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is False

    ws = _FakeWs(close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    result = await ws_driver.scenario_e01_missing_query_conversation_id(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is False
    ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_scenario_e16_and_e17_cover_handshake_guardrails(monkeypatch):
    class _HandshakeError(RuntimeError):
        pass

    err = _HandshakeError("second sender rejected")
    err.response = SimpleNamespace(
        status_code=403,
        body=b'{"error":{"code":"E1009","message":"conflict"}}',
    )
    primary_ws = _FakeWs(close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=[primary_ws, err]))
    result = await ws_driver.scenario_e16_second_concurrent_sender(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is True
    primary_ws.close.assert_awaited_once()

    err = _HandshakeError("auth rejected")
    err.response = SimpleNamespace(
        status_code=401,
        body=b'{"error":{"code":"E1010","message":"auth failed"}}',
    )
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=err))
    result = await ws_driver.scenario_e17_invalid_bearer_jwt(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is True

    primary_ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=[primary_ws, _FakeWs()]))
    result = await ws_driver.scenario_e16_second_concurrent_sender(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is False
    primary_ws.close.assert_awaited_once()

    success_ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=success_ws))
    result = await ws_driver.scenario_e17_invalid_bearer_jwt(
        "ws://unit-test",
        lambda _event_type, _data: asyncio.sleep(0),
    )
    assert result.passed is False
    assert "AUTH_ENABLED=false" in result.steps[0]["error"]
    success_ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_send_expect_wrapper_scenarios_cover_finally_branches(monkeypatch):
    async def _emit(_event_type: str, _data: dict) -> None:
        return None

    for fn in (
        ws_driver.scenario_d2_schema_error,
        ws_driver.scenario_e05_invalid_enum,
        ws_driver.scenario_e08_invalid_timestamp,
        ws_driver.scenario_e14_conversation_id_mismatch,
        ws_driver.scenario_e15_business_rule_violation,
    ):
        ws = _FakeWs(close_side_effect=RuntimeError("close failed"))
        monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
        monkeypatch.setattr(ws_driver, "_send_expect_error_and_close", AsyncMock(return_value=None))
        result = await fn("ws://unit-test", _emit)
        assert result.passed is True

    ws = _FakeWs(close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(ws_driver, "_send_expect_error_and_close", AsyncMock(return_value=None))
    result = await ws_driver.scenario_e07_wrong_type("ws://unit-test", _emit)
    assert result.passed is True
    assert ws_driver._send_expect_error_and_close.await_count >= 2

    ws = _FakeWs(close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(ws_driver, "_session_ongoing_plus_complete_and_close", AsyncMock(return_value=None))
    result = await ws_driver.scenario_g_session_complete("ws://unit-test", _emit)
    assert result.passed is True


@pytest.mark.asyncio
async def test_load_single_conversation_connect_stop_and_error_paths(monkeypatch):
    emitted: list[tuple[str, dict]] = []
    stats = ws_driver.Stats()

    class _HandshakeError(RuntimeError):
        pass

    err = _HandshakeError("bad handshake")
    err.response = SimpleNamespace(status_code=400, body=b'{"error":{"code":"E1003","message":"bad"}}')
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(side_effect=err))
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=lambda event_type, data: _collect(emitted, event_type, data),
        n_messages=2,
        interval_ms=0,
        sse_register_cid=True,
    )
    assert stats.error == 1

    stop_event = asyncio.Event()
    stop_event.set()
    ws = _FakeWs(close_side_effect=RuntimeError("close failed"))
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=ws_driver.Stats(),
        emit=lambda event_type, data: _collect(emitted, event_type, data),
        n_messages=2,
        interval_ms=0,
        stop_event=stop_event,
    )

    ws = _FakeWs()
    sleep = AsyncMock()
    monkeypatch.setattr(ws_driver.asyncio, "sleep", sleep)
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "ERROR"}, "error": {"code": "E1001", "message": "bad"}},
                {"metaData": {"eventType": "ERROR"}, "error": {"code": "E1002", "message": "bad complete"}},
            ]
        ),
    )
    stats = ws_driver.Stats()
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=lambda event_type, data: _collect(emitted, event_type, data),
        n_messages=2,
        interval_ms=1,
    )

    assert stats.error >= 2
    sleep.assert_awaited()

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(ws_driver, "_send_and_recv", AsyncMock(return_value=None))
    stats = ws_driver.Stats()
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=lambda event_type, data: _collect(emitted, event_type, data),
        n_messages=2,
        interval_ms=0,
    )

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[{"metaData": {"eventType": "ACK"}}, {"metaData": {"eventType": "ACK"}}]),
    )
    stats = ws_driver.Stats()
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=lambda event_type, data: _collect(emitted, event_type, data),
        n_messages=2,
        interval_ms=0,
    )


@pytest.mark.asyncio
async def test_load_single_conversation_records_server_processing_ms(monkeypatch):
    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(
            side_effect=[
                {"metaData": {"eventType": "TRANSCRIPT_ACK"}, "payload": {"serverProcessingMs": 50}},
                {"metaData": {"eventType": "EOL_ACK"}, "payload": {"serverProcessingMs": 75}},
            ]
        ),
    )
    stats = ws_driver.Stats()

    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=lambda _event_type, _data: asyncio.sleep(0),
        n_messages=2,
        interval_ms=0,
    )

    assert stats.server_latencies == [0.05, 0.075]


@pytest.mark.asyncio
async def test_load_single_conversation_remaining_detail_and_exception_paths(monkeypatch):
    emitted: list[tuple[str, dict]] = []

    async def emit(event_type: str, data: dict) -> None:
        emitted.append((event_type, data))

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))

    async def ack_without_server_ms(_ws, _msg, *, on_sent=None):
        if on_sent is not None:
            on_sent()
        return {"metaData": {"eventType": "TRANSCRIPT_ACK"}, "payload": {"serverProcessingMs": "bad"}}

    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[None, {"metaData": {"eventType": "EOL_ACK"}, "payload": {"serverProcessingMs": "bad"}}]),
    )
    stats = ws_driver.Stats()
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=emit,
        n_messages=2,
        interval_ms=0,
    )

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=[{"metaData": {"eventType": "ACK"}}, {"metaData": {"eventType": "ACK"}}]),
    )
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=ws_driver.Stats(),
        emit=emit,
        n_messages=2,
        interval_ms=0,
    )

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(
        ws_driver,
        "_send_and_recv",
        AsyncMock(side_effect=RuntimeError("send failed")),
    )
    stats = ws_driver.Stats()
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=emit,
        n_messages=1,
        interval_ms=0,
    )

    ws = _FakeWs()
    monkeypatch.setattr(ws_driver, "_open_ws", AsyncMock(return_value=ws))
    monkeypatch.setattr(ws_driver, "_send_and_recv", ack_without_server_ms)
    stats = ws_driver.Stats()
    await ws_driver._load_single_conversation(
        ws_url="ws://unit-test",
        stats=stats,
        emit=emit,
        n_messages=1,
        interval_ms=0,
    )
    assert stats.sent == 1


@pytest.mark.asyncio
async def test_run_load_test_throttles_errors_and_honors_ramp_and_stop(monkeypatch):
    emitted: list[tuple[str, dict]] = []

    async def emit(event_type: str, data: dict) -> None:
        emitted.append((event_type, data))

    async def noisy_load(_ws_url, _stats, emit_fn, *_args, **_kwargs):
        for index in range(300):
            await emit_fn("load_error", {"index": index})

    monkeypatch.setattr(ws_driver, "_load_single_conversation", noisy_load)
    monkeypatch.setattr(ws_driver.asyncio, "sleep", AsyncMock(return_value=None))
    stats = ws_driver.Stats()

    await ws_driver.run_load_test(
        ws_url="ws://unit-test",
        stats=stats,
        emit=emit,
        concurrency=1,
        messages_per_conv=1,
        interval_ms=0,
        ramp_up_ms=100,
    )

    load_errors = [data for event_type, data in emitted if event_type == "load_error"]
    assert len(load_errors) == 201
    assert emitted[0][0] == "stats"
    assert emitted[-1][0] == "load_done"

    stop_event = asyncio.Event()
    emitted.clear()
    load_single = AsyncMock(return_value=None)
    monkeypatch.setattr(ws_driver, "_load_single_conversation", load_single)

    async def ramp_sleep(_seconds: float):
        stop_event.set()

    monkeypatch.setattr(ws_driver.asyncio, "sleep", ramp_sleep)
    await ws_driver.run_load_test(
        ws_url="ws://unit-test",
        stats=ws_driver.Stats(),
        emit=emit,
        concurrency=2,
        messages_per_conv=1,
        interval_ms=0,
        ramp_up_ms=100,
        stop_event=stop_event,
    )
    assert emitted[-1][1]["load_cancelled"] is True
    assert load_single.await_count in {0, 1}

    stop_event = asyncio.Event()
    stop_event.set()
    emitted.clear()
    await ws_driver.run_load_test(
        ws_url="ws://unit-test",
        stats=ws_driver.Stats(),
        emit=emit,
        concurrency=2,
        messages_per_conv=1,
        interval_ms=0,
        ramp_up_ms=0,
        stop_event=stop_event,
    )
    assert emitted[-1][1]["load_cancelled"] is True
