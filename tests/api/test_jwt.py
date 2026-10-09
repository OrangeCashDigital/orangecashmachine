# -*- coding: utf-8 -*-
"""Round-trip de apps/api/auth/jwt.py (HS256) tras la sustitución python-jose -> PyJWT (ADR-0051).

Hermético: sin servidor, sin Redis ni base de datos. Solo sign/verify con un
secreto local; la API del módulo no toca infraestructura.
"""

from __future__ import annotations

import jwt as pyjwt
from api.auth.jwt import create_token, verify_token

_SECRET = "test-secret"
_ALGORITHM = "HS256"


def test_round_trip_sign_then_verify() -> None:
    """create_token -> verify_token recupera el payload completo."""
    payload = {"sub": "operator", "roles": ["admin"]}

    token = create_token(payload, _SECRET, algorithm=_ALGORITHM)
    decoded = verify_token(token, _SECRET, algorithm=_ALGORITHM)

    assert decoded is not None
    assert decoded["sub"] == "operator"
    assert decoded["roles"] == ["admin"]


def test_token_is_standard_compact_jwt_hs256() -> None:
    """El token emitido es un JWT compacto estándar firmado con HS256."""
    token = create_token({"sub": "operator"}, _SECRET, algorithm=_ALGORITHM)

    header = pyjwt.get_unverified_header(token)

    assert header["alg"] == "HS256"
    assert header["typ"] == "JWT"


def test_iat_and_exp_claims_are_set() -> None:
    """create_token inyecta iat (inicio) y exp (expiración), exp > iat."""
    token = create_token({"sub": "operator"}, _SECRET, algorithm=_ALGORITHM)
    decoded = verify_token(token, _SECRET, algorithm=_ALGORITHM)

    assert decoded is not None
    assert "iat" in decoded
    assert "exp" in decoded
    assert decoded["exp"] > decoded["iat"]


def test_invalid_token_returns_none() -> None:
    """Fail-soft: texto que no es JWT -> None, sin excepción."""
    assert verify_token("not-a-jwt", _SECRET, algorithm=_ALGORITHM) is None


def test_tampered_signature_returns_none() -> None:
    """Firma no válida -> None (DecodeError capturada)."""
    token = create_token({"sub": "operator"}, _SECRET, algorithm=_ALGORITHM)
    header, payload, _ = token.split(".")

    assert verify_token(f"{header}.{payload}.AAAA", _SECRET, algorithm=_ALGORITHM) is None


def test_wrong_secret_returns_none() -> None:
    """Secreto distinto -> InvalidSignatureError -> None."""
    token = create_token({"sub": "operator"}, "other-secret", algorithm=_ALGORITHM)

    assert verify_token(token, _SECRET, algorithm=_ALGORITHM) is None


def test_wrong_algorithm_returns_none() -> None:
    """Algoritmo esperado distinto del del token -> None."""
    token = create_token({"sub": "operator"}, _SECRET, algorithm=_ALGORITHM)

    assert verify_token(token, _SECRET, algorithm="HS512") is None


def test_expired_token_returns_none() -> None:
    """exp en el pasado -> ExpiredSignatureError -> None."""
    token = create_token(
        {"sub": "operator"},
        _SECRET,
        algorithm=_ALGORITHM,
        expire_minutes=-5,
    )

    assert verify_token(token, _SECRET, algorithm=_ALGORITHM) is None
