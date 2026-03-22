from __future__ import annotations

"""core/config/loader/secret_masker.py — Ofuscación de credenciales sensibles."""

from typing import Any

_SENSITIVE_KEYS = frozenset({
    "password", "passwd", "secret", "token", "api_key", "apikey",
    "private_key", "auth", "credential", "credentials", "access_key",
    "secret_key", "jwt", "bearer", "passphrase", "encryption_key",
})


class SecretMasker:
    MASK = "***REDACTED***"

    @classmethod
    def mask(cls, data: Any, _depth: int = 0) -> Any:
        if _depth > 10:
            return data
        if isinstance(data, dict):
            return {
                k: cls.MASK if cls._is_sensitive(k) else cls.mask(v, _depth + 1)
                for k, v in data.items()
            }
        if isinstance(data, list):
            return [cls.mask(v, _depth + 1) for v in data]
        return data

    @classmethod
    def _is_sensitive(cls, key: str) -> bool:
        return any(s in key.lower() for s in _SENSITIVE_KEYS)
