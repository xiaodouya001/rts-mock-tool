"""coverage: mock_tool.kafka_connection"""

from __future__ import annotations

import builtins
import importlib
import ssl
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from mock_tool import kafka_connection as kc
from mock_tool.settings import MockClientSettings


def _settings(**overrides: object) -> MockClientSettings:
    base: dict[str, object] = {
        "host": "0.0.0.0",
        "port": 8088,
        "log_level": "INFO",
        "log_format": "auto",
        "default_ws_url": "ws://x",
        "auth_enabled": False,
        "auth_token": None,
        "auth_signing_material": None,
        "auth_subject": "mock-client",
        "auth_ttl_days": 30,
        "kafka_bootstrap": "127.0.0.1:9092",
        "kafka_topic": "t",
        "kafka_mode": "local",
        "kafka_aws_region": None,
        "kafka_ssl_ca_file": None,
        "kafka_aws_debug_creds": False,
    }
    base.update(overrides)
    return MockClientSettings(**base)  # type: ignore[arg-type]


def test_kafka_connection_extra_local_plaintext():
    out = kc.kafka_connection_extra_kwargs(_settings())
    assert out == {"security_protocol": "PLAINTEXT"}


def test_kafka_connection_extra_aws_msk_iam():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    token_provider = MagicMock()
    with patch.object(kc.ssl, "create_default_context", return_value=ssl_ctx) as create_ctx, patch.object(
        kc, "MSKTokenProvider", return_value=token_provider
    ) as provider_ctor:
        out = kc.kafka_connection_extra_kwargs(
            _settings(
                kafka_mode="aws_msk",
                kafka_aws_region="ap-east-1",
                kafka_ssl_ca_file="/tmp/ca.pem",
                kafka_aws_debug_creds=True,
            ),
        )

    create_ctx.assert_called_once_with(cafile="/tmp/ca.pem")
    provider_ctor.assert_called_once_with("ap-east-1", aws_debug_creds=True)
    assert out["security_protocol"] == "SASL_SSL"
    assert out["sasl_mechanism"] == "OAUTHBEARER"
    assert out["ssl_context"] is ssl_ctx
    assert out["sasl_oauth_token_provider"] is token_provider


def test_kafka_connection_extra_aws_msk_requires_region():
    with pytest.raises(ValueError, match="kafka_aws_region"):
        kc.kafka_connection_extra_kwargs(
            _settings(kafka_mode="aws_msk", kafka_aws_region=None),
        )


def test_generate_msk_auth_token_success(monkeypatch):
    fake = types.SimpleNamespace()
    fake.MSKAuthTokenProvider = MagicMock(
        generate_auth_token=MagicMock(return_value=("signed-token", 9_999_999)),
    )
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", fake)
    token, expiry = kc._generate_msk_auth_token("ap-east-1", aws_debug_creds=True)
    assert token == "signed-token"
    assert expiry == 9_999_999
    fake.MSKAuthTokenProvider.generate_auth_token.assert_called_once_with(
        "ap-east-1",
        aws_debug_creds=True,
    )


def test_generate_msk_auth_token_requires_signer_package():
    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "aws_msk_iam_sasl_signer":
            raise ImportError("missing signer")
        return real_import(name, globals, locals, fromlist, level)

    try:
        with patch.object(builtins, "__import__", side_effect=fake_import):
            importlib.reload(kc)
            with pytest.raises(RuntimeError, match="aws-msk-iam-sasl-signer-python"):
                kc._generate_msk_auth_token("ap-east-1")
    finally:
        importlib.reload(kc)


@pytest.mark.asyncio
async def test_msk_token_provider_caches_until_near_expiry():
    with patch.object(
        kc,
        "_generate_msk_auth_token",
        side_effect=[("token-1", 9_999_999_999_999), ("token-2", 1)],
    ) as gen:
        provider = kc.MSKTokenProvider("ap-east-1", aws_debug_creds=True)
        first = await provider.token()
        second = await provider.token()

    assert first == "token-1"
    assert second == "token-1"
    assert gen.call_count == 1


@pytest.mark.asyncio
async def test_msk_token_provider_refreshes_after_expiry_window():
    with patch.object(
        kc,
        "_generate_msk_auth_token",
        side_effect=[("token-1", 100_000), ("token-2", 9_999_999_999_999_999)],
    ) as gen, patch(
        "mock_tool.kafka_connection.time.time",
        side_effect=[0.05, 45.0],
    ):
        provider = kc.MSKTokenProvider("ap-east-1", aws_debug_creds=False)
        assert await provider.token() == "token-1"
        assert await provider.token() == "token-2"

    assert gen.call_count == 2
