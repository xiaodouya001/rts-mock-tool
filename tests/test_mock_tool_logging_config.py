"""Tests for mock_tool.logging_config."""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

from mock_tool import logging_config as lc


def test_json_serializer_keeps_unicode():
    payload = lc._json_serializer({"message": "咖啡"})
    assert "咖啡" in payload
    assert json.loads(payload)["message"] == "咖啡"


def test_configure_logging_uses_env_level_and_json(monkeypatch, capsys):
    monkeypatch.setenv("MOCK_CLIENT_LOG_LEVEL", "warning")
    monkeypatch.setenv("MOCK_CLIENT_LOG_FORMAT", "json")

    lc.configure_logging()
    logging.getLogger("uvicorn").warning("boot")

    root = logging.getLogger()
    assert root.level == logging.WARNING
    assert logging.getLogger("uvicorn").level == logging.WARNING
    assert logging.getLogger("uvicorn").propagate is True
    assert logging.getLogger("aiokafka").level == logging.CRITICAL

    payload = json.loads(capsys.readouterr().err.strip())
    assert payload["event"] == "boot"
    assert payload["logger"] == "uvicorn"
    assert payload["service"] == lc.SERVICE_NAME
    assert payload["level"] == "warning"


def test_configure_logging_auto_uses_json_when_not_tty(monkeypatch, capsys):
    monkeypatch.delenv("MOCK_CLIENT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_LOG_FORMAT", raising=False)

    with patch.object(lc.sys.stderr, "isatty", return_value=False):
        lc.configure_logging(format="auto")
    logging.getLogger("mock-client").info("ready")

    payload = json.loads(capsys.readouterr().err.strip())
    assert payload["event"] == "ready"
    assert payload["logger"] == "mock-client"
    assert payload["service"] == lc.SERVICE_NAME


def test_configure_logging_can_suppress_health_access_logs(monkeypatch, capsys):
    monkeypatch.delenv("MOCK_CLIENT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_LOG_FORMAT", raising=False)

    lc.configure_logging(
        level="INFO",
        format="json",
        suppress_health_access_logs=True,
    )
    logger = logging.getLogger("uvicorn.access")
    logger.info(
        '%s - "%s %s HTTP/%s" %d',
        "127.0.0.1:1234",
        "GET",
        "/transcribe-svc-mock-tool/health",
        "1.1",
        200,
    )
    logger.info(
        '%s - "%s %s HTTP/%s" %d',
        "127.0.0.1:1234",
        "GET",
        "/api/status",
        "1.1",
        200,
    )

    output = capsys.readouterr().err
    assert "/transcribe-svc-mock-tool/health" not in output
    assert "/api/status" in output


def test_configure_logging_console_mode(monkeypatch, capsys):
    monkeypatch.delenv("MOCK_CLIENT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_LOG_FORMAT", raising=False)

    with patch.object(lc.sys.stderr, "isatty", return_value=True):
        lc.configure_logging(level="INFO", format="console")
    logging.getLogger("fastapi").info("hello")

    output = capsys.readouterr().err
    assert "hello" in output
    assert "service" in output
    assert lc.SERVICE_NAME in output
    assert "[info" in output.lower() or "info" in output.lower()


def test_configure_logging_invalid_level_falls_back_to_info():
    lc.configure_logging(level="not-a-level", format="json")

    root = logging.getLogger()
    assert root.level == logging.INFO
    assert logging.getLogger("fastapi").level == logging.INFO
