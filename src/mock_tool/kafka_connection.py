"""Kafka client wiring aligned with Realtime Transcribe Service (local vs AWS MSK IAM).

- ``local``: PLAINTEXT (local docker-compose style). Use normal Kafka clients; the mock tool
  does not create topics.
- ``aws_msk``: SASL_SSL + OAUTHBEARER via ``aws-msk-iam-sasl-signer-python``.
"""

from __future__ import annotations

import asyncio
import ssl
import threading
import time
from typing import Any

from aiokafka.abc import AbstractTokenProvider

from mock_tool.settings import MockClientSettings


def _generate_msk_auth_token(region: str, *, aws_debug_creds: bool = False) -> tuple[str, int]:
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise RuntimeError(
            "aws-msk-iam-sasl-signer-python is required when kafka_mode=aws_msk"
        ) from exc
    return MSKAuthTokenProvider.generate_auth_token(
        region,
        aws_debug_creds=aws_debug_creds,
    )


class MSKTokenProvider(AbstractTokenProvider):
    """Async aiokafka token provider backed by the AWS MSK IAM signer."""

    def __init__(self, region: str, *, aws_debug_creds: bool = False) -> None:
        self._region = region
        self._aws_debug_creds = aws_debug_creds
        self._token: str | None = None
        self._expiry_ms = 0
        self._lock = threading.Lock()

    def _refresh_token(self) -> str:
        now_ms = int(time.time() * 1000)
        with self._lock:
            if self._token is not None and now_ms < self._expiry_ms - 60_000:
                return self._token

            token, expiry_ms = _generate_msk_auth_token(
                self._region,
                aws_debug_creds=self._aws_debug_creds,
            )
            self._token = token
            self._expiry_ms = expiry_ms
            return token

    async def token(self) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]
        return await asyncio.get_running_loop().run_in_executor(None, self._refresh_token)


def kafka_connection_extra_kwargs(settings: MockClientSettings) -> dict[str, Any]:
    """Return kwargs to merge into aiokafka clients (no ``bootstrap_servers``)."""
    if settings.kafka_mode == "aws_msk":
        region = settings.kafka_aws_region
        if not region:
            raise ValueError("kafka_aws_region is required when kafka_mode=aws_msk")
        return {
            "security_protocol": "SASL_SSL",
            "ssl_context": ssl.create_default_context(cafile=settings.kafka_ssl_ca_file),
            "sasl_mechanism": "OAUTHBEARER",
            "sasl_oauth_token_provider": MSKTokenProvider(
                region,
                aws_debug_creds=settings.kafka_aws_debug_creds,
            ),
        }
    return {"security_protocol": "PLAINTEXT"}
