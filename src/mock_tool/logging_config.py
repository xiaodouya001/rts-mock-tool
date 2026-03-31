"""Structured logging configuration for the mock-client tools."""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Literal

import structlog

SERVICE_NAME = "realtime-transcribe-service-mock-client"
_FRAMEWORK_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi", "starlette")


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
) -> None:
    """Route stdlib logging through the same renderer as structlog."""
    processor_formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=foreign_pre_chain,
    )

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(processor_formatter)
    handler.setLevel(log_level)

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
) -> None:
    """Configure structlog for the mock-client server and helpers."""
    level_name = (level or os.environ.get("MOCK_CLIENT_LOG_LEVEL", "INFO")).upper()
    fmt = (format or os.environ.get("MOCK_CLIENT_LOG_FORMAT", "auto")).lower()
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

    _configure_stdlib_logging(resolved_level, renderer, _SHARED_PROCESSORS)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    logging.getLogger("aiokafka").setLevel(logging.CRITICAL)
