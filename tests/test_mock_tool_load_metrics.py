"""Tests for mock-client load metrics semantics."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest

from mock_tool import ws_driver


async def _emit(_event_type: str, _data: dict) -> None:
    return None


def test_stats_snapshot_reports_ack_tps_and_interpolated_percentiles():
    stats = ws_driver.Stats(
        sent=6,
        ack=4,
        error=1,
        active_connections=2,
        latencies=[0.10, 0.20],
        server_latencies=[0.01, 0.03],
        start_time=10.0,
        end_time=12.0,
    )

    snapshot = stats.snapshot()

    assert snapshot["send_tps"] == 3.0
    assert snapshot["ack_tps"] == 2.0
    assert snapshot["tps"] == 2.0
    assert snapshot["p50_ms"] == 150.0
    assert snapshot["p95_ms"] == 195.0
    assert snapshot["server_p50_ms"] == 20.0
    assert snapshot["server_p99_ms"] == 29.8


@pytest.mark.asyncio
async def test_load_single_conversation_counts_sent_after_socket_write_even_without_response():
    stats = ws_driver.Stats()
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock(side_effect=asyncio.TimeoutError())
    ws.close = AsyncMock()

    with patch.object(ws_driver, "_open_ws", new=AsyncMock(return_value=ws)):
        await ws_driver._load_single_conversation(
            ws_url="ws://unit-test",
            stats=stats,
            emit=_emit,
            n_messages=1,
            interval_ms=0,
        )

    assert stats.sent == 1
    assert stats.ack == 0
    assert stats.error == 1
    assert stats.active_connections == 0


@pytest.mark.asyncio
async def test_load_single_conversation_does_not_count_send_when_socket_write_fails():
    stats = ws_driver.Stats()
    ws = AsyncMock()
    ws.send = AsyncMock(side_effect=RuntimeError("send failed"))
    ws.close = AsyncMock()

    with patch.object(ws_driver, "_open_ws", new=AsyncMock(return_value=ws)):
        await ws_driver._load_single_conversation(
            ws_url="ws://unit-test",
            stats=stats,
            emit=_emit,
            n_messages=1,
            interval_ms=0,
        )

    assert stats.sent == 0
    assert stats.ack == 0
    assert stats.error == 1
    assert stats.active_connections == 0


@pytest.mark.asyncio
async def test_load_single_conversation_sends_default_dialect_for_ongoing_and_complete():
    stats = ws_driver.Stats()
    ws = AsyncMock()
    ws.send = AsyncMock()
    ws.recv = AsyncMock(
        side_effect=[
            json.dumps({"metaData": {"eventType": "TRANSCRIPT_ACK"}, "payload": {}}),
            json.dumps({"metaData": {"eventType": "EOL_ACK"}, "payload": {}}),
        ]
    )
    ws.close = AsyncMock()

    with patch.object(ws_driver, "_open_ws", new=AsyncMock(return_value=ws)):
        await ws_driver._load_single_conversation(
            ws_url="ws://unit-test",
            stats=stats,
            emit=_emit,
            n_messages=2,
            interval_ms=0,
        )

    assert ws.send.await_count == 2
    sent_payloads = [json.loads(call.args[0]) for call in ws.send.await_args_list]
    assert sent_payloads[0]["payload"]["dialect"] == "yue-x-auto"
    assert sent_payloads[1]["payload"]["dialect"] == "yue-x-auto"
