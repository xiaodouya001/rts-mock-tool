"""Tests for mock_tool.settings."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
import jwt

from mock_tool import settings as mock_settings


@pytest.fixture(autouse=True)
def isolate_mock_tool_dotenv(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setattr(mock_settings, "_env_file_values", lambda: {})
    yield
    mock_settings.get_settings.cache_clear()


def test_get_settings_reads_prefixed_environment(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_HOST", "127.0.0.1")
    monkeypatch.setenv("MOCK_CLIENT_PORT", "9099")
    monkeypatch.setenv("MOCK_CLIENT_LOG_LEVEL", "debug")
    monkeypatch.setenv("MOCK_CLIENT_LOG_FORMAT", "json")
    monkeypatch.setenv("MOCK_CLIENT_URL_PATH_PREFIX", "mock-ui")
    monkeypatch.setenv("MOCK_CLIENT_SHOW_MOCK_LIVE_CHAT", "false")
    monkeypatch.setenv("MOCK_CLIENT_SHOW_SCENARIO_TESTS", "true")
    monkeypatch.setenv("MOCK_CLIENT_SHOW_CONCURRENT_LOAD_TEST", "false")
    monkeypatch.setenv("MOCK_CLIENT_DEFAULT_WS_URL", "ws://service.example/ws")
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("MOCK_CLIENT_AUTH_TOKEN", "token-123")
    monkeypatch.setenv("MOCK_CLIENT_AUTH_SIGNING_MATERIAL", "signing-material-123")
    monkeypatch.setenv("MOCK_CLIENT_AUTH_SUBJECT", "fano-mock")
    monkeypatch.setenv("MOCK_CLIENT_AUTH_TTL_DAYS", "45")
    monkeypatch.setenv("MOCK_CLIENT_KAFKA_BOOTSTRAP", "kafka.example:9092")
    monkeypatch.setenv("MOCK_CLIENT_KAFKA_TOPIC", "TOPIC_A")

    settings = mock_settings.get_settings()

    assert settings.host == "127.0.0.1"
    assert settings.port == 9099
    assert settings.log_level == "DEBUG"
    assert settings.log_format == "json"
    assert settings.url_path_prefix == "/mock-ui"
    assert settings.show_mock_live_chat is False
    assert settings.show_scenario_tests is True
    assert settings.show_concurrent_load_test is False
    assert settings.default_ws_url == "ws://service.example/ws"
    assert settings.auth_enabled is True
    assert settings.auth_token == "token-123"
    assert settings.auth_signing_material == "signing-material-123"
    assert settings.auth_subject == "fano-mock"
    assert settings.auth_ttl_days == 45
    assert settings.kafka_bootstrap == "kafka.example:9092"
    assert settings.kafka_topic == "TOPIC_A"
    assert settings.kafka_mode == "local"
    assert settings.kafka_aws_region is None
    assert settings.kafka_ssl_ca_file is None
    assert settings.kafka_aws_debug_creds is False
    assert settings.suppress_health_access_logs is False


def test_get_settings_reads_prefixed_health_access_log_toggle(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_SUPPRESS_HEALTH_ACCESS_LOGS", "true")

    settings = mock_settings.get_settings()

    assert settings.suppress_health_access_logs is True


def test_get_settings_ignores_service_env_names(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.delenv("MOCK_CLIENT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_LOG_FORMAT", raising=False)
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "json")

    settings = mock_settings.get_settings()

    assert settings.log_level == "INFO"
    assert settings.log_format == "auto"


def test_get_settings_uses_prefixed_default_ws_url_when_unset(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.delenv("MOCK_CLIENT_DEFAULT_WS_URL", raising=False)

    settings = mock_settings.get_settings()

    assert (
        settings.default_ws_url
        == "ws://127.0.0.1:8080/transcribe-svc/ws/v1/realtime-transcriptions"
    )


def test_get_settings_uses_default_url_path_prefix_when_unset(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.delenv("MOCK_CLIENT_URL_PATH_PREFIX", raising=False)

    settings = mock_settings.get_settings()

    assert settings.url_path_prefix == "/transcribe-svc-mock-tool"


def test_get_settings_shows_all_ui_sections_when_unset(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.delenv("MOCK_CLIENT_SHOW_MOCK_LIVE_CHAT", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_SHOW_SCENARIO_TESTS", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_SHOW_CONCURRENT_LOAD_TEST", raising=False)

    settings = mock_settings.get_settings()

    assert settings.show_mock_live_chat is True
    assert settings.show_scenario_tests is True
    assert settings.show_concurrent_load_test is True


def test_normalize_url_path_prefix_str_normalizes_and_rejects_invalid_values():
    assert mock_settings.normalize_url_path_prefix_str("abc") == "/abc"
    assert mock_settings.normalize_url_path_prefix_str("/api/v1/") == "/api/v1"
    assert mock_settings.normalize_url_path_prefix_str("////a////b//") == "/a/b"
    assert mock_settings.normalize_url_path_prefix_str("   ") == ""
    assert mock_settings.normalize_url_path_prefix_str("///") == ""
    assert mock_settings.normalize_url_path_prefix_str(None) == ""  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="MOCK_CLIENT_URL_PATH_PREFIX"):
        mock_settings.normalize_url_path_prefix_str("/../bad")


def test_get_settings_rejects_invalid_port(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_PORT", "70000")

    with pytest.raises(ValueError, match="MOCK_CLIENT_PORT"):
        mock_settings.get_settings()


def test_get_settings_treats_blank_auth_token_as_disabled(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_AUTH_TOKEN", "   ")

    settings = mock_settings.get_settings()

    assert settings.auth_token is None


def test_get_settings_ignores_service_auth_env_names(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.delenv("MOCK_CLIENT_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_AUTH_SIGNING_MATERIAL", raising=False)
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv("AUTH_JWT_SIGNING_MATERIAL", "signing-material-from-service")

    settings = mock_settings.get_settings()

    assert settings.auth_token is None
    assert settings.auth_signing_material is None


def test_build_auth_token_returns_none_when_auth_is_disabled():
    settings = mock_settings.MockClientSettings(
        host="0.0.0.0",
        port=8088,
        log_level="INFO",
        log_format="auto",
        default_ws_url="ws://unit-test",
        auth_enabled=False,
        auth_token="prebuilt-token",
        auth_signing_material="signing-material",
        auth_subject="mock-client",
        auth_ttl_days=30,
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        kafka_mode="local",
        kafka_aws_region=None,
        kafka_ssl_ca_file=None,
        kafka_aws_debug_creds=False,
    )

    assert mock_settings.build_auth_token(settings) is None


def test_build_auth_token_prefers_explicit_token():
    settings = mock_settings.MockClientSettings(
        host="0.0.0.0",
        port=8088,
        log_level="INFO",
        log_format="auto",
        default_ws_url="ws://unit-test",
        auth_enabled=True,
        auth_token="prebuilt-token",
        auth_signing_material="signing-material",
        auth_subject="mock-client",
        auth_ttl_days=30,
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        kafka_mode="local",
        kafka_aws_region=None,
        kafka_ssl_ca_file=None,
        kafka_aws_debug_creds=False,
    )

    assert mock_settings.build_auth_token(settings) == "prebuilt-token"


def test_build_auth_claims_uses_expected_shape():
    now = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)

    claims = mock_settings.build_auth_claims(
        "mock-client",
        30,
        now=now,
        jti="fixed-jti",
    )

    assert claims == {
        "sub": "mock-client",
        "iat": int(now.timestamp()),
        "exp": int(now.timestamp()) + 30 * 24 * 60 * 60,
        "jti": "fixed-jti",
    }


def test_build_auth_token_generates_hs256_jwt_from_signing_material():
    now = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)
    signing_material = "signing-material-0123456789-material-012345"
    settings = mock_settings.MockClientSettings(
        host="0.0.0.0",
        port=8088,
        log_level="INFO",
        log_format="auto",
        default_ws_url="ws://unit-test",
        auth_enabled=True,
        auth_token=None,
        auth_signing_material=signing_material,
        auth_subject="mock-client",
        auth_ttl_days=30,
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        kafka_mode="local",
        kafka_aws_region=None,
        kafka_ssl_ca_file=None,
        kafka_aws_debug_creds=False,
    )

    token = mock_settings.build_auth_token(settings, now=now)
    claims = jwt.decode(
        token,
        signing_material,
        algorithms=["HS256"],
        options={"verify_iat": False},
    )

    assert claims["sub"] == "mock-client"
    assert claims["iat"] == int(now.timestamp())
    assert claims["exp"] == int(now.timestamp()) + 30 * 24 * 60 * 60
    assert isinstance(claims["jti"], str)
    assert claims["jti"]


def test_generate_hs256_token_returns_token_and_claims():
    now = datetime(2026, 3, 30, 12, 0, 0, tzinfo=timezone.utc)
    signing_material = "signing-material-0123456789-material-012345"

    token, claims = mock_settings.generate_hs256_token(
        signing_material,
        "mock-client",
        30,
        now=now,
        jti="fixed-jti",
    )

    decoded = jwt.decode(
        token,
        signing_material,
        algorithms=["HS256"],
        options={"verify_iat": False},
    )

    assert claims == decoded


def test_load_env_file_supports_local_dotenv():
    env_path = Path(__file__).with_name("_test_mock_tool.env")
    env_path.write_text(
        "MOCK_CLIENT_LOG_LEVEL=WARNING\n"
        "MOCK_CLIENT_KAFKA_TOPIC='topic-b'\n",
        encoding="utf-8",
    )

    try:
        values = mock_settings._load_env_file(env_path)
    finally:
        env_path.unlink(missing_ok=True)

    assert values["MOCK_CLIENT_LOG_LEVEL"] == "WARNING"
    assert values["MOCK_CLIENT_KAFKA_TOPIC"] == "topic-b"


def test_load_env_file_ignores_blank_comment_and_invalid_lines(tmp_path):
    env_path = tmp_path / "mixed.env"
    env_path.write_text(
        "\n"
        "   \n"
        "# comment line\n"
        "NOT_A_KV_LINE\n"
        "MOCK_CLIENT_LOG_LEVEL=INFO\n",
        encoding="utf-8",
    )

    values = mock_settings._load_env_file(env_path)

    assert values == {"MOCK_CLIENT_LOG_LEVEL": "INFO"}


def test_load_env_file_missing_returns_empty_dict(tmp_path):
    assert mock_settings._load_env_file(tmp_path / "missing.env") == {}


def test_require_non_empty_rejects_blank_value():
    with pytest.raises(ValueError, match="TEST_NAME must not be empty"):
        mock_settings._require_non_empty("TEST_NAME", "   ")


def test_parse_port_rejects_non_integer():
    with pytest.raises(ValueError, match="MOCK_CLIENT_PORT must be an integer"):
        mock_settings._parse_port("MOCK_CLIENT_PORT", "abc")


def test_parse_log_level_rejects_unknown_value():
    with pytest.raises(ValueError, match="MOCK_CLIENT_LOG_LEVEL must be one of"):
        mock_settings._parse_log_level("MOCK_CLIENT_LOG_LEVEL", "verbose")


def test_parse_log_format_rejects_unknown_value():
    with pytest.raises(ValueError, match="MOCK_CLIENT_LOG_FORMAT must be one of"):
        mock_settings._parse_log_format("MOCK_CLIENT_LOG_FORMAT", "pretty")


def test_parse_bool_rejects_unknown_value():
    with pytest.raises(ValueError, match="AUTH_ENABLED must be one of"):
        mock_settings._parse_bool("AUTH_ENABLED", "maybe")


def test_parse_positive_int_rejects_non_integer():
    with pytest.raises(ValueError, match="MOCK_CLIENT_AUTH_TTL_DAYS must be an integer"):
        mock_settings._parse_positive_int("MOCK_CLIENT_AUTH_TTL_DAYS", "abc")


def test_parse_positive_int_rejects_zero_and_negative():
    with pytest.raises(ValueError, match="MOCK_CLIENT_AUTH_TTL_DAYS must be greater than 0"):
        mock_settings._parse_positive_int("MOCK_CLIENT_AUTH_TTL_DAYS", "0")


def test_get_settings_aws_msk_requires_region(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_KAFKA_MODE", "aws_msk")
    monkeypatch.delenv("MOCK_CLIENT_KAFKA_AWS_REGION", raising=False)

    with pytest.raises(ValueError, match="MOCK_CLIENT_KAFKA_AWS_REGION"):
        mock_settings.get_settings()


def test_get_settings_rejects_invalid_kafka_mode(monkeypatch):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_KAFKA_MODE", "plaintext")

    with pytest.raises(ValueError, match="MOCK_CLIENT_KAFKA_MODE"):
        mock_settings.get_settings()


def test_parse_kafka_mode_rejects_unknown():
    with pytest.raises(ValueError, match="X must be one of"):
        mock_settings._parse_kafka_mode("X", "invalid")


def test_parse_kafka_mode_normalizes_case():
    assert mock_settings._parse_kafka_mode("MOCK_CLIENT_KAFKA_MODE", "LOCAL") == "local"
    assert mock_settings._parse_kafka_mode("MOCK_CLIENT_KAFKA_MODE", "AWS_MSK") == "aws_msk"


def test_parse_kafka_mode_rejects_admin():
    with pytest.raises(ValueError, match="MOCK_CLIENT_KAFKA_MODE"):
        mock_settings._parse_kafka_mode("MOCK_CLIENT_KAFKA_MODE", "admin")


def test_get_settings_kafka_bootstrap_topic_from_dotenv_mock_keys_only(monkeypatch, tmp_path):
    mock_settings.get_settings.cache_clear()
    env_path = tmp_path / ".env"
    env_path.write_text(
        "MOCK_CLIENT_KAFKA_BOOTSTRAP=file-only:9092\nMOCK_CLIENT_KAFKA_TOPIC=file-topic\n",
        encoding="utf-8",
    )
    file_vals = mock_settings._load_env_file(env_path)
    monkeypatch.setattr(mock_settings, "_env_file_values", lambda: file_vals)
    for key in ("MOCK_CLIENT_KAFKA_BOOTSTRAP", "MOCK_CLIENT_KAFKA_TOPIC"):
        monkeypatch.delenv(key, raising=False)

    settings = mock_settings.get_settings()

    assert settings.kafka_bootstrap == "file-only:9092"
    assert settings.kafka_topic == "file-topic"


def test_build_auth_token_returns_none_when_signing_material_missing():
    settings = mock_settings.MockClientSettings(
        host="0.0.0.0",
        port=8088,
        log_level="INFO",
        log_format="auto",
        default_ws_url="ws://unit-test",
        auth_enabled=True,
        auth_token=None,
        auth_signing_material=None,
        auth_subject="mock-client",
        auth_ttl_days=30,
        kafka_bootstrap="127.0.0.1:9092",
        kafka_topic="AI_STAGING_TRANSCRIPTION",
        kafka_mode="local",
        kafka_aws_region=None,
        kafka_ssl_ca_file=None,
        kafka_aws_debug_creds=False,
    )

    assert mock_settings.build_auth_token(settings) is None
