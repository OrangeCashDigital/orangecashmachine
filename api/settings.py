# -*- coding: utf-8 -*-
"""
api/settings.py
===============

Configuración de la API Gateway via pydantic-settings.

SSOT de todas las variables de entorno que consume la API.

Estrategia de env vars
----------------------
- Vars propias de la API:  prefijo OCM_API_  (jwt_secret → OCM_API_JWT_SECRET)
- Vars compartidas de OCM: validation_alias explícito sin prefijo
                           (redis_host → REDIS_HOST, ya definida en .env)

Esto garantiza SSOT: REDIS_PASSWORD vive una sola vez en .env,
compartida por todos los subsistemas de OCM sin duplicación.

Principios: SOLID · SSOT · Fail-Fast · SafeOps · KISS · DRY
"""
from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    """
    Configuración de la API Gateway.

    env_prefix="OCM_API_" aplica a todos los campos EXCEPTO
    los que declaran validation_alias explícito (REDIS_*).

    Fail-Fast: campos requeridos sin default lanzan ValidationError
    antes de que el servidor escuche conexiones.
    """

    model_config = SettingsConfigDict(
        env_prefix="OCM_API_",        # OCM_API_JWT_SECRET, OCM_API_ENV, etc.
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",               # ignora vars desconocidas — OCP
    )

    # ── Auth ──────────────────────────────────────────────────────────
    # Lee: OCM_API_JWT_SECRET
    jwt_secret: str = Field(
        ...,
        description="Secreto para firmar/verificar JWT (>=32 chars)",
    )
    # Lee: OCM_API_JWT_ALGORITHM
    jwt_algorithm: str = Field(default="HS256")
    # Lee: OCM_API_JWT_EXPIRE_MINUTES
    jwt_expire_minutes: int = Field(default=60, ge=1, le=1440)

    # ── Redis — SSOT: validation_alias omite el prefijo OCM_API_ ─────
    # Lee: REDIS_HOST (variable compartida con el resto de OCM)
    redis_host: str = Field(
        default="localhost",
        validation_alias="REDIS_HOST",
    )
    # Lee: REDIS_PORT
    redis_port: int = Field(
        default=6379,
        ge=1, le=65535,
        validation_alias="REDIS_PORT",
    )
    # Lee: REDIS_DB
    redis_db: int = Field(
        default=0,
        ge=0, le=15,
        validation_alias="REDIS_DB",
    )
    # Lee: REDIS_PASSWORD
    redis_password: Optional[str] = Field(
        default=None,
        validation_alias="REDIS_PASSWORD",
    )

    # Compuesto en model_validator — nunca leer directamente
    redis_url: str = Field(
        default="",
        description="Compuesto automáticamente desde REDIS_* — no setear manualmente",
    )

    # ── Servidor ──────────────────────────────────────────────────────
    # Lee: OCM_API_HOST / OCM_API_PORT / OCM_API_ENV
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000, ge=1, le=65535)
    env:  str = Field(default="development")

    # ── Rate limiting ─────────────────────────────────────────────────
    # Lee: OCM_API_RATE_LIMIT_RPM
    rate_limit_rpm: int = Field(
        default=60,
        ge=1,
        le=10_000,
        description="Requests por minuto por IP",
    )

    # ── Validators ────────────────────────────────────────────────────

    @field_validator("jwt_secret")
    @classmethod
    def secret_not_empty(cls, v: str) -> str:
        """Fail-Fast: secreto corto es error de seguridad, no warning."""
        if not v or len(v) < 32:
            raise ValueError(
                "OCM_API_JWT_SECRET debe tener al menos 32 caracteres. "
                "Generar con: python3 -c \"import secrets; print(secrets.token_hex(32))\""
            )
        return v

    @model_validator(mode="after")
    def compose_redis_url(self) -> "ApiSettings":
        """
        SSOT: compone redis_url desde REDIS_HOST/PORT/DB/PASSWORD.
        El password no se duplica — se lee directamente de REDIS_PASSWORD.
        SafeOps: redis_url nunca se loguea; usar redis_url_sanitized.
        """
        if self.redis_password:
            # Convención Redis: redis://:password@host:port/db (usuario vacío)
            self.redis_url = (
                f"redis://:{self.redis_password}"
                f"@{self.redis_host}:{self.redis_port}/{self.redis_db}"
            )
        else:
            self.redis_url = (
                f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"
            )
        return self

    @property
    def redis_url_sanitized(self) -> str:
        """URL de Redis sin password — segura para logs. SafeOps."""
        if self.redis_password:
            return f"redis://:***@{self.redis_host}:{self.redis_port}/{self.redis_db}"
        return self.redis_url

    @property
    def is_production(self) -> bool:
        return self.env.lower() == "production"


@lru_cache(maxsize=1)
def get_settings() -> ApiSettings:
    """
    Singleton de configuración — SSOT para toda la API.
    lru_cache garantiza una sola instancia por proceso.
    No re-lee .env en cada request.
    """
    return ApiSettings()
