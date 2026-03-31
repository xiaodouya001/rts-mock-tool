"""Tests for mock_tool.generate_jwt."""

from __future__ import annotations

import json
import runpy
import sys
from types import SimpleNamespace

import jwt
import pytest

from mock_tool import generate_jwt
from mock_tool import settings as mock_settings


def test_resolve_inputs_prefers_explicit_signing_material_and_subject():
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=True,
                auth_signing_material="settings-material",
                auth_subject="settings-subject",
                auth_ttl_days=30,
            ),
        )

        signing_material, subject, ttl_days = generate_jwt._resolve_inputs(
            signing_material="explicit-material",
            subject="explicit-subject",
            days=45,
        )

    assert signing_material == "explicit-material"
    assert subject == "explicit-subject"
    assert ttl_days == 45


def test_main_prints_raw_token_by_default(capsys):
    signing_material = "signing-material-0123456789-material-012345"

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=True,
                auth_signing_material=signing_material,
                auth_subject="settings-subject",
                auth_ttl_days=30,
            ),
        )
        exit_code = generate_jwt.main(["--sub", "fano-backend", "--days", "7"])

    token = capsys.readouterr().out.strip()
    claims = jwt.decode(
        token,
        signing_material,
        algorithms=["HS256"],
        options={"verify_iat": False},
    )

    assert exit_code == 0
    assert claims["sub"] == "fano-backend"
    assert claims["exp"] > claims["iat"]


def test_main_supports_json_output(capsys):
    signing_material = "signing-material-0123456789-material-012345"

    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=True,
                auth_signing_material=signing_material,
                auth_subject="mock-client",
                auth_ttl_days=30,
            ),
        )
        exit_code = generate_jwt.main(["--json"])

    output = json.loads(capsys.readouterr().out)
    claims = jwt.decode(
        output["token"],
        signing_material,
        algorithms=["HS256"],
        options={"verify_iat": False},
    )

    assert exit_code == 0
    assert output["claims"] == claims
    assert output["authorization_header"] == f"Bearer {output['token']}"


def test_main_fails_when_no_signing_material_is_available(capsys):
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=True,
                auth_signing_material=None,
                auth_subject="mock-client",
                auth_ttl_days=30,
            ),
        )
        with pytest.raises(SystemExit) as exc_info:
            generate_jwt.main([])

    assert exc_info.value.code == 2
    assert "No HS256 signing material available" in capsys.readouterr().err


def test_main_fails_when_auth_is_disabled(capsys):
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=False,
                auth_signing_material="signing-material-0123456789-material-012345",
                auth_subject="mock-client",
                auth_ttl_days=30,
            ),
        )
        with pytest.raises(SystemExit) as exc_info:
            generate_jwt.main([])

    assert exc_info.value.code == 2
    assert "AUTH_ENABLED=false" in capsys.readouterr().err


def test_resolve_inputs_rejects_blank_subject():
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=True,
                auth_signing_material="signing-material-0123456789-material-012345",
                auth_subject="settings-subject",
                auth_ttl_days=30,
            ),
        )
        with pytest.raises(ValueError, match="JWT subject must not be empty"):
            generate_jwt._resolve_inputs(
                signing_material=None,
                subject="   ",
                days=7,
            )


def test_resolve_inputs_rejects_non_positive_ttl():
    with pytest.MonkeyPatch.context() as monkeypatch:
        monkeypatch.setattr(
            generate_jwt,
            "get_settings",
            lambda: SimpleNamespace(
                auth_enabled=True,
                auth_signing_material="signing-material-0123456789-material-012345",
                auth_subject="settings-subject",
                auth_ttl_days=30,
            ),
        )
        with pytest.raises(ValueError, match="Token TTL days must be greater than 0"):
            generate_jwt._resolve_inputs(
                signing_material=None,
                subject=None,
                days=0,
            )


def test_module_entrypoint_raises_system_exit_zero(monkeypatch, capsys):
    mock_settings.get_settings.cache_clear()
    monkeypatch.setattr(mock_settings, "_env_file_values", lambda: {})
    monkeypatch.setenv("AUTH_ENABLED", "true")
    monkeypatch.setenv(
        "MOCK_CLIENT_AUTH_SIGNING_MATERIAL",
        "signing-material-0123456789-material-012345",
    )
    monkeypatch.setenv("MOCK_CLIENT_AUTH_SUBJECT", "mock-client")
    monkeypatch.setenv("MOCK_CLIENT_AUTH_TTL_DAYS", "30")
    monkeypatch.setattr(sys, "argv", ["generate_jwt.py"])
    saved_module = sys.modules.pop("mock_tool.generate_jwt", None)

    try:
        with pytest.raises(SystemExit) as exc_info:
            runpy.run_module("mock_tool.generate_jwt", run_name="__main__")
    finally:
        if saved_module is not None:
            sys.modules["mock_tool.generate_jwt"] = saved_module

    assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip()
    mock_settings.get_settings.cache_clear()
