"""Runtime settings for the mock-tool package."""

from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import jwt

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_FILE = _PROJECT_ROOT / ".env"
_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
_LOG_FORMATS = {"json", "console", "auto"}
_TRUE_VALUES = {"1", "true", "yes", "on"}
_FALSE_VALUES = {"0", "false", "no", "off"}


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized = value.strip()
        if (
            len(normalized) >= 2
            and normalized[0] == normalized[-1]
            and normalized[0] in {"'", '"'}
        ):
            normalized = normalized[1:-1]
        values[key.strip()] = normalized
    return values


@lru_cache(maxsize=1)
def _env_file_values() -> dict[str, str]:
    return _load_env_file(_ENV_FILE)


def _get_setting(name: str, default: str) -> str:
    return os.environ.get(name, _env_file_values().get(name, default))


def _get_optional_setting(name: str) -> str | None:
    value = os.environ.get(name, _env_file_values().get(name))
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _require_non_empty(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _parse_port(name: str, value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not (1 <= port <= 65535):
        raise ValueError(f"{name} must be in the range 1..65535")
    return port


def _parse_log_level(name: str, value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in _LOG_LEVELS:
        raise ValueError(f"{name} must be one of: {', '.join(sorted(_LOG_LEVELS))}")
    return normalized


def _parse_log_format(name: str, value: str) -> Literal["json", "console", "auto"]:
    normalized = value.strip().lower()
    if normalized not in _LOG_FORMATS:
        raise ValueError(f"{name} must be one of: json, console, auto")
    return normalized  # type: ignore[return-value]


def _parse_bool(name: str, value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    raise ValueError(f"{name} must be one of: true, false, 1, 0, yes, no, on, off")


def _parse_positive_int(name: str, value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be greater than 0")
    return parsed


def _parse_kafka_mode(name: str, value: str) -> Literal["local", "aws_msk"]:
    normalized = value.strip().lower()
    if normalized in ("local", "aws_msk"):
        return normalized  # type: ignore[return-value]
    raise ValueError(f"{name} must be one of: local, aws_msk")


@dataclass(frozen=True)
class MockClientSettings:
    host: str
    port: int
    log_level: str
    log_format: Literal["json", "console", "auto"]
    default_ws_url: str
    auth_enabled: bool
    auth_token: str | None
    auth_signing_material: str | None
    auth_subject: str
    auth_ttl_days: int
    kafka_bootstrap: str
    kafka_topic: str
    kafka_mode: Literal["local", "aws_msk"]
    kafka_aws_region: str | None
    kafka_ssl_ca_file: str | None
    kafka_aws_debug_creds: bool


def build_auth_claims(
    subject: str,
    ttl_days: int,
    *,
    now: datetime | None = None,
    jti: str | None = None,
) -> dict[str, Any]:
    issued_at = now or datetime.now(timezone.utc)
    return {
        "sub": subject,
        "iat": int(issued_at.timestamp()),
        "exp": int((issued_at + timedelta(days=ttl_days)).timestamp()),
        "jti": jti or uuid.uuid4().hex,
    }


def generate_hs256_token(
    signing_material: str,
    subject: str,
    ttl_days: int,
    *,
    now: datetime | None = None,
    jti: str | None = None,
) -> tuple[str, dict[str, Any]]:
    claims = build_auth_claims(subject, ttl_days, now=now, jti=jti)
    token = jwt.encode(claims, signing_material, algorithm="HS256")
    return token, claims


@lru_cache(maxsize=1)
def get_settings() -> MockClientSettings:
    kafka_mode = _parse_kafka_mode(
        "MOCK_CLIENT_KAFKA_MODE",
        _get_setting("MOCK_CLIENT_KAFKA_MODE", "local"),
    )
    kafka_aws_region = _get_optional_setting("MOCK_CLIENT_KAFKA_AWS_REGION")
    kafka_ssl_ca_file = _get_optional_setting("MOCK_CLIENT_KAFKA_SSL_CA_FILE")
    kafka_aws_debug_creds = _parse_bool(
        "MOCK_CLIENT_KAFKA_AWS_DEBUG_CREDS",
        _get_setting("MOCK_CLIENT_KAFKA_AWS_DEBUG_CREDS", "false"),
    )
    kafka_bootstrap = _require_non_empty(
        "MOCK_CLIENT_KAFKA_BOOTSTRAP",
        _get_setting("MOCK_CLIENT_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
    )
    kafka_topic = _require_non_empty(
        "MOCK_CLIENT_KAFKA_TOPIC",
        _get_setting("MOCK_CLIENT_KAFKA_TOPIC", "AI_STAGING_TRANSCRIPTION"),
    )

    if kafka_mode == "aws_msk" and not kafka_aws_region:
        raise ValueError(
            "MOCK_CLIENT_KAFKA_AWS_REGION must be set when MOCK_CLIENT_KAFKA_MODE=aws_msk"
        )

    return MockClientSettings(
        host=_require_non_empty(
            "MOCK_CLIENT_HOST",
            _get_setting("MOCK_CLIENT_HOST", "0.0.0.0"),
        ),
        port=_parse_port(
            "MOCK_CLIENT_PORT",
            _get_setting("MOCK_CLIENT_PORT", "8088"),
        ),
        log_level=_parse_log_level(
            "MOCK_CLIENT_LOG_LEVEL",
            _get_setting("MOCK_CLIENT_LOG_LEVEL", "INFO"),
        ),
        log_format=_parse_log_format(
            "MOCK_CLIENT_LOG_FORMAT",
            _get_setting("MOCK_CLIENT_LOG_FORMAT", "auto"),
        ),
        default_ws_url=_require_non_empty(
            "MOCK_CLIENT_DEFAULT_WS_URL",
            _get_setting(
                "MOCK_CLIENT_DEFAULT_WS_URL",
                "ws://127.0.0.1:8080/ws/v1/realtime-transcriptions",
            ),
        ),
        auth_enabled=_parse_bool(
            "AUTH_ENABLED",
            _get_setting("AUTH_ENABLED", "false"),
        ),
        auth_token=_get_optional_setting("MOCK_CLIENT_AUTH_TOKEN"),
        auth_signing_material=_get_optional_setting("MOCK_CLIENT_AUTH_SIGNING_MATERIAL"),
        auth_subject=_require_non_empty(
            "MOCK_CLIENT_AUTH_SUBJECT",
            _get_setting("MOCK_CLIENT_AUTH_SUBJECT", "mock-client"),
        ),
        auth_ttl_days=_parse_positive_int(
            "MOCK_CLIENT_AUTH_TTL_DAYS",
            _get_setting("MOCK_CLIENT_AUTH_TTL_DAYS", "30"),
        ),
        kafka_bootstrap=kafka_bootstrap,
        kafka_topic=kafka_topic,
        kafka_mode=kafka_mode,
        kafka_aws_region=kafka_aws_region,
        kafka_ssl_ca_file=kafka_ssl_ca_file,
        kafka_aws_debug_creds=kafka_aws_debug_creds,
    )


def build_auth_token(
    settings: MockClientSettings | None = None,
    *,
    now: datetime | None = None,
) -> str | None:
    current = settings or get_settings()
    if not current.auth_enabled:
        return None
    if current.auth_token is not None:
        return current.auth_token
    if current.auth_signing_material is None:
        return None

    token, _claims = generate_hs256_token(
        current.auth_signing_material,
        current.auth_subject,
        current.auth_ttl_days,
        now=now,
    )
    return token
