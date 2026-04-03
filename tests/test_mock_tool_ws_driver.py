"""Scenario tests for mock_tool.ws_driver."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import uvicorn
from fastapi import FastAPI, WebSocket

from mock_tool import ws_driver

pytestmark = [
    pytest.mark.filterwarnings(
        "ignore:websockets\\.legacy is deprecated; see .* upgrade instructions:DeprecationWarning"
    ),
    pytest.mark.filterwarnings(
        "ignore:websockets\\.server\\.WebSocketServerProtocol is deprecated:DeprecationWarning"
    ),
    pytest.mark.filterwarnings(
        "ignore:remove second argument of ws_handler:DeprecationWarning"
    ),
]


@pytest.fixture(autouse=True)
def disable_ambient_auth_token(monkeypatch):
    monkeypatch.setattr(ws_driver, "build_auth_token", lambda: None)
    monkeypatch.setattr(
        ws_driver,
        "get_settings",
        lambda: SimpleNamespace(auth_enabled=False),
    )


@pytest.fixture
async def live_ws_url(unused_tcp_port: int) -> str:
    app = FastAPI()

    @app.websocket("/transcribe-svc/ws/v1/realtime-transcriptions")
    async def realtime_transcriptions(websocket: WebSocket) -> None:
        cid = websocket.query_params.get("conversationId")
        await websocket.accept()

        raw = await websocket.receive_text()
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            await websocket.send_json(_error_frame(cid, "E1001"))
            await websocket.close(code=1007)
            return

        if not isinstance(msg, dict):
            await websocket.send_json(_error_frame(cid, "E1004"))
            await websocket.close(code=1008)
            return

        meta = msg.get("metaData")
        payload = msg.get("payload")
        if not isinstance(meta, dict) or not isinstance(payload, dict):
            await websocket.send_json(_error_frame(cid, "E1003"))
            await websocket.close(code=1008)
            return

        conversation_id = meta.get("conversationId")
        if conversation_id is None:
            await websocket.send_json(_error_frame(cid, "E1003"))
            await websocket.close(code=1008)
            return
        if not isinstance(conversation_id, str):
            await websocket.send_json(_error_frame(cid, "E1004"))
            await websocket.close(code=1008)
            return

        await websocket.send_json(
            {
                "metaData": {
                    "eventType": "TRANSCRIPT_ACK",
                    "conversationId": cid,
                }
            }
        )
        await websocket.close(code=1000)

    config = uvicorn.Config(
        app,
        host="127.0.0.1",
        port=unused_tcp_port,
        ws="websockets",
        access_log=False,
        log_level="warning",
        lifespan="off",
    )
    server = uvicorn.Server(config)
    task = asyncio.create_task(server.serve())

    for _ in range(200):
        if getattr(server, "started", False):
            break
        if task.done():
            task.result()
        await asyncio.sleep(0.01)
    else:
        server.should_exit = True
        await task
        pytest.fail("mock scenario test server did not start")

    try:
        yield f"ws://127.0.0.1:{unused_tcp_port}/transcribe-svc/ws/v1/realtime-transcriptions"
    finally:
        server.should_exit = True
        await task


async def _collect_events(_event_type: str, _data: dict) -> None:
    return None


def _error_frame(conversation_id: str | None, code: str) -> dict[str, dict[str, str | None]]:
    return {
        "metaData": {
            "eventType": "ERROR",
            "conversationId": conversation_id,
        },
        "error": {
            "code": code,
            "message": "test stub error",
        },
    }


async def test_mock_tool_e06_scenario_preserves_handshake_conversation_id(
    live_ws_url: str,
):
    result = await ws_driver.scenario_d2_schema_error(live_ws_url, _collect_events)

    assert result.passed is True
    error_step = next(step for step in result.steps if step["action"] == "send_bad_schema")
    assert error_step["error_code"] == "E1003"
    assert error_step["conversation_id"].startswith("mock-E06-")
    close_step = next(step for step in result.steps if step["action"] == "verify_close")
    assert close_step["close_code"] == 1008


async def test_mock_tool_e07_scenario_covers_non_object_json_and_wrong_type(
    live_ws_url: str,
):
    result = await ws_driver.scenario_e07_wrong_type(live_ws_url, _collect_events)

    assert result.passed is True
    non_object_step = next(step for step in result.steps if step["action"] == "send_non_object_json")
    wrong_type_step = next(step for step in result.steps if step["action"] == "send_wrong_type_field")
    assert non_object_step["error_code"] == "E1004"
    assert wrong_type_step["error_code"] == "E1004"
    assert non_object_step["conversation_id"].startswith("mock-E07-")
    assert wrong_type_step["conversation_id"].startswith("mock-E07-")
    close_codes = [step["close_code"] for step in result.steps if step["action"] == "verify_close"]
    assert close_codes == [1008, 1008]


@pytest.mark.asyncio
async def test_mock_tool_n03_scenario_sends_default_dialect_for_ongoing_and_complete():
    ws = AsyncMock()
    ws.close = AsyncMock()
    ws.wait_closed = AsyncMock(return_value=None)
    ws.close_code = 1000
    sent_messages: list[dict] = []

    async def fake_send_and_recv(_ws, msg, *, on_sent=None):
        sent_messages.append(msg)
        if on_sent is not None:
            on_sent()
        event_type = msg["metaData"]["eventType"]
        return {
            "metaData": {
                "eventType": "TRANSCRIPT_ACK"
                if event_type == "SESSION_ONGOING"
                else "EOL_ACK"
            }
        }

    with (
        patch.object(ws_driver, "_open_ws", new=AsyncMock(return_value=ws)),
        patch.object(ws_driver, "_send_and_recv", new=AsyncMock(side_effect=fake_send_and_recv)),
    ):
        result = await ws_driver.scenario_g_session_complete(
            "ws://unit-test", _collect_events, n_messages=2
        )

    assert result.passed is True
    assert [msg["metaData"]["eventType"] for msg in sent_messages] == [
        "SESSION_ONGOING",
        "SESSION_COMPLETE",
    ]
    assert [msg["payload"]["dialect"] for msg in sent_messages] == [
        "yue-x-auto",
        "yue-x-auto",
    ]


@pytest.mark.asyncio
async def test_mock_tool_e06_scenario_bad_schema_still_includes_default_dialect():
    ws = AsyncMock()
    ws.close = AsyncMock()
    captured: dict[str, dict] = {}

    async def fake_send_expect_error_and_close(
        _ws,
        msg,
        *,
        action,
        expected_code,
        expected_close,
        expected_conversation_id=None,
        result,
        emit,
    ):
        captured["msg"] = msg

    with (
        patch.object(ws_driver, "_open_ws", new=AsyncMock(return_value=ws)),
        patch.object(
            ws_driver,
            "_send_expect_error_and_close",
            new=AsyncMock(side_effect=fake_send_expect_error_and_close),
        ),
    ):
        result = await ws_driver.scenario_d2_schema_error("ws://unit-test", _collect_events)

    assert result.passed is True
    assert captured["msg"]["payload"]["dialect"] == "yue-x-auto"


@pytest.mark.asyncio
async def test_open_ws_includes_authorization_header_from_settings():
    fake_ws = AsyncMock()

    with (
        patch.object(
            ws_driver,
            "build_auth_token",
            return_value="jwt-token",
        ),
        patch.object(
            ws_driver,
            "get_settings",
            return_value=SimpleNamespace(auth_enabled=True),
        ),
        patch.object(
            ws_driver.websockets,
            "connect",
            new=AsyncMock(return_value=fake_ws),
        ) as connect_mock,
    ):
        ws = await ws_driver._open_ws("ws://unit-test", "conv-1", retries=1)

    assert ws is fake_ws
    connect_mock.assert_awaited_once_with(
        "ws://unit-test?conversationId=conv-1",
        open_timeout=30,
        additional_headers={"Authorization": "Bearer jwt-token"},
    )


@pytest.mark.asyncio
async def test_open_ws_omits_authorization_header_when_no_token_is_configured():
    fake_ws = AsyncMock()

    with (
        patch.object(
            ws_driver,
            "build_auth_token",
            return_value=None,
        ),
        patch.object(
            ws_driver,
            "get_settings",
            return_value=SimpleNamespace(auth_enabled=True),
        ),
        patch.object(
            ws_driver.websockets,
            "connect",
            new=AsyncMock(return_value=fake_ws),
        ) as connect_mock,
    ):
        ws = await ws_driver._open_ws("ws://unit-test", "conv-1", retries=1)

    assert ws is fake_ws
    connect_mock.assert_awaited_once_with(
        "ws://unit-test?conversationId=conv-1",
        open_timeout=30,
    )


@pytest.mark.asyncio
async def test_open_ws_omits_authorization_header_when_auth_is_disabled():
    fake_ws = AsyncMock()

    with patch.object(
        ws_driver.websockets,
        "connect",
        new=AsyncMock(return_value=fake_ws),
    ) as connect_mock:
        ws = await ws_driver._open_ws(
            "ws://unit-test",
            "conv-1",
            auth_token="explicit-token",
            retries=1,
        )

    assert ws is fake_ws
    connect_mock.assert_awaited_once_with(
        "ws://unit-test?conversationId=conv-1",
        open_timeout=30,
    )


@pytest.mark.asyncio
async def test_open_ws_uses_explicit_auth_token_over_generated_token():
    fake_ws = AsyncMock()

    with (
        patch.object(
            ws_driver,
            "build_auth_token",
            return_value="generated-token",
        ),
        patch.object(
            ws_driver,
            "get_settings",
            return_value=SimpleNamespace(auth_enabled=True),
        ),
        patch.object(
            ws_driver.websockets,
            "connect",
            new=AsyncMock(return_value=fake_ws),
        ) as connect_mock,
    ):
        ws = await ws_driver._open_ws(
            "ws://unit-test",
            "conv-1",
            auth_token="explicit-token",
            retries=1,
        )

    assert ws is fake_ws
    connect_mock.assert_awaited_once_with(
        "ws://unit-test?conversationId=conv-1",
        open_timeout=30,
        additional_headers={"Authorization": "Bearer explicit-token"},
    )


@pytest.mark.asyncio
async def test_open_ws_uses_override_headers_even_when_auth_is_disabled():
    fake_ws = AsyncMock()

    with patch.object(
        ws_driver.websockets,
        "connect",
        new=AsyncMock(return_value=fake_ws),
    ) as connect_mock:
        ws = await ws_driver._open_ws(
            "ws://unit-test",
            "conv-1",
            retries=1,
            override_headers={"Authorization": "Bearer invalid.jwt.token"},
        )

    assert ws is fake_ws
    connect_mock.assert_awaited_once_with(
        "ws://unit-test?conversationId=conv-1",
        open_timeout=30,
        additional_headers={"Authorization": "Bearer invalid.jwt.token"},
    )

