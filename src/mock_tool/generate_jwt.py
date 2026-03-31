"""Generate a local HS256 JWT for the mock client."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

try:
    from .settings import generate_hs256_token, get_settings
except ImportError:  # pragma: no cover - direct script execution fallback
    from settings import generate_hs256_token, get_settings


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate a HS256 Bearer JWT for mock-client local testing.",
    )
    parser.add_argument(
        "--signing-material",
        dest="signing_material",
        help=(
            "HS256 signing material. Defaults to MOCK_CLIENT_AUTH_SIGNING_MATERIAL."
        ),
    )
    parser.add_argument(
        "--sub",
        dest="subject",
        help="JWT sub claim. Defaults to MOCK_CLIENT_AUTH_SUBJECT.",
    )
    parser.add_argument(
        "--days",
        type=int,
        help="Token TTL in days. Defaults to MOCK_CLIENT_AUTH_TTL_DAYS.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a JSON object containing the token and claims instead of the raw token.",
    )
    return parser


def _resolve_inputs(
    *,
    signing_material: str | None,
    subject: str | None,
    days: int | None,
) -> tuple[str, str, int]:
    settings = get_settings()
    if not settings.auth_enabled:
        raise ValueError(
            "AUTH_ENABLED=false in mock-client config. Enable it before generating or reading a token."
        )
    resolved_signing_material = (signing_material or settings.auth_signing_material or "").strip()
    if not resolved_signing_material:
        raise ValueError(
            "No HS256 signing material available. Set --signing-material or MOCK_CLIENT_AUTH_SIGNING_MATERIAL."
        )

    resolved_subject = (subject or settings.auth_subject).strip()
    if not resolved_subject:
        raise ValueError("JWT subject must not be empty.")

    resolved_days = days if days is not None else settings.auth_ttl_days
    if resolved_days <= 0:
        raise ValueError("Token TTL days must be greater than 0.")

    return resolved_signing_material, resolved_subject, resolved_days


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        signing_material, subject, ttl_days = _resolve_inputs(
            signing_material=args.signing_material,
            subject=args.subject,
            days=args.days,
        )
    except ValueError as exc:
        parser.error(str(exc))

    token, claims = generate_hs256_token(signing_material, subject, ttl_days)
    if args.json:
        print(
            json.dumps(
                {
                    "token": token,
                    "claims": claims,
                    "authorization_header": f"Bearer {token}",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    else:
        print(token)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
