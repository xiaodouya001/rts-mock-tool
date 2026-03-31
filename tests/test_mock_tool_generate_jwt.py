"""Tests for mock_tool.generate_jwt."""

from __future__ import annotations

import json
from types import SimpleNamespace

import jwt
import pytest

from mock_tool import generate_jwt


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
