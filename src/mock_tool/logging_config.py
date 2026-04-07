"""Structured logging configuration for the mock-client tools."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from typing import Any, Literal

import structlog

SERVICE_NAME = "realtime-transcribe-service-mock-client"
_FRAMEWORK_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi", "starlette")
_UVICORN_ACCESS_LINE_RE = re.compile(
    r'"[A-Z]+\s+(?P<path>\S+)\s+HTTP/[0-9.]+"',
    re.IGNORECASE,
)
_SUPPRESSED_ACCESS_PATH_SUFFIXES = ("/health",)
_TRUE_ENV_VALUES = frozenset({"1", "true", "yes", "on"})


def _json_serializer(obj: Any, **kwargs: Any) -> str:
    """Serialize JSON with ``ensure_ascii=False`` so Unicode stays readable."""
    kwargs.setdefault("ensure_ascii", False)
    return json.dumps(obj, **kwargs)


def _add_service_name(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Tag every mock-client log event with its service name."""
    event_dict["service"] = SERVICE_NAME
    return event_dict


def _env_flag_enabled(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE_ENV_VALUES


def _normalize_access_path(path: object) -> str | None:
    if not isinstance(path, str):
        return None
    normalized = path.split("?", 1)[0].rstrip("/")
    return normalized or "/"


def _extract_uvicorn_access_path(record: logging.LogRecord) -> str | None:
    if record.name != "uvicorn.access":
        return None

    args = record.args
    if isinstance(args, tuple) and len(args) >= 3:
        normalized = _normalize_access_path(args[2])
        if normalized is not None:
            return normalized

    match = _UVICORN_ACCESS_LINE_RE.search(record.getMessage())
    if match is None:
        return None
    return _normalize_access_path(match.group("path"))


class _SuppressHealthAccessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        path = _extract_uvicorn_access_path(record)
        if path is None:
            return True
        return not any(
            path == suffix or path.endswith(suffix)
            for suffix in _SUPPRESSED_ACCESS_PATH_SUFFIXES
        )


_SHARED_PROCESSORS: list[structlog.typing.Processor] = [
    _add_service_name,
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_logger_name,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
]


def _configure_stdlib_logging(
    log_level: int,
    renderer: structlog.processors.JSONRenderer | structlog.dev.ConsoleRenderer,
    foreign_pre_chain: list[structlog.typing.Processor],
    *,
    suppress_health_access_logs: bool = False,
) -> None:
    """Route stdlib logging through the same renderer as structlog."""
    processor_formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=foreign_pre_chain,
    )

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(processor_formatter)
    handler.setLevel(log_level)
    if suppress_health_access_logs:
        handler.addFilter(_SuppressHealthAccessFilter())

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    for name in _FRAMEWORK_LOGGERS:
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(log_level)


def configure_logging(
    *,
    level: str | None = None,
    format: Literal["json", "console", "auto"] | None = None,
    suppress_health_access_logs: bool | None = None,
) -> None:
    """Configure structlog for the mock-client server and helpers."""
    level_name = (level or os.environ.get("MOCK_CLIENT_LOG_LEVEL", "INFO")).upper()
    fmt = (format or os.environ.get("MOCK_CLIENT_LOG_FORMAT", "auto")).lower()
    suppress_health_access_logs = (
        _env_flag_enabled("MOCK_CLIENT_SUPPRESS_HEALTH_ACCESS_LOGS")
        if suppress_health_access_logs is None
        else suppress_health_access_logs
    )
    resolved_level = getattr(logging, level_name, logging.INFO)

    if fmt == "auto":
        use_json = not sys.stderr.isatty()
    else:
        use_json = fmt == "json"

    if use_json:
        renderer = structlog.processors.JSONRenderer(serializer=_json_serializer)
        processors = [
            structlog.stdlib.filter_by_level,
            *_SHARED_PROCESSORS,
            structlog.processors.dict_tracebacks,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]
    else:
        renderer = structlog.dev.ConsoleRenderer(
            colors=sys.stderr.isatty(),
            pad_event_to=25,
            sort_keys=False,
        )
        processors = [
            structlog.stdlib.filter_by_level,
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]

    _configure_stdlib_logging(
        resolved_level,
        renderer,
        _SHARED_PROCESSORS,
        suppress_health_access_logs=suppress_health_access_logs,
    )

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    logging.getLogger("aiokafka").setLevel(logging.CRITICAL)
