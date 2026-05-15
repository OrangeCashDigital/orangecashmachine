# -*- coding: utf-8 -*-
"""
infrastructure/timeouts.py
===========================

SSOT de todos los timeouts del sistema.

Regla de uso
------------
NUNCA escribir un número mágico de timeout en el código.
Importar siempre desde aquí:

    from infrastructure.timeouts import Timeouts
    redis_client = Redis(socket_timeout=Timeouts.REDIS_OPERATION_S)

Justificación de valores
-------------------------
Los valores reflejan SLAs observados en producción con exchanges
cripto (alta latencia p99) y Iceberg sobre object storage (S3/GCS).
Ajustar mediante variables de entorno en entornos de CI/staging.

Principios: SSOT, KISS, Fail-Fast ante latencia anómala.
"""
from __future__ import annotations

import os


class Timeouts:
    """
    Timeouts del sistema en segundos.

    Todos los valores son floats para compatibilidad con asyncio,
    aiohttp, httpx, redis-py, y el resto de clientes.

    Override via env vars en staging/CI:
        TIMEOUT_REDIS_CONNECT_S=0.5 python -m pytest ...
    """

    # ── Redis ─────────────────────────────────────────────────────────────
    REDIS_CONNECT_S: float = float(
        os.getenv("TIMEOUT_REDIS_CONNECT_S", "2.0")
    )
    REDIS_OPERATION_S: float = float(
        os.getenv("TIMEOUT_REDIS_OPERATION_S", "1.0")
    )

    # ── CCXT / Exchange HTTP ───────────────────────────────────────────────
    # p99 en Bybit/KuCoin spot: ~800ms. 10s = margen conservador.
    CCXT_REQUEST_S: float = float(
        os.getenv("TIMEOUT_CCXT_REQUEST_S", "10.0")
    )
    # Conexión TCP inicial al exchange
    CCXT_CONNECT_S: float = float(
        os.getenv("TIMEOUT_CCXT_CONNECT_S", "5.0")
    )

    # ── Iceberg / PyIceberg ────────────────────────────────────────────────
    # scan() sobre S3 con partition pruning: p99 ~3s para tablas <100GB.
    # Sin timeout → deadlock silencioso en producción.
    ICEBERG_SCAN_S: float = float(
        os.getenv("TIMEOUT_ICEBERG_SCAN_S", "30.0")
    )
    ICEBERG_WRITE_S: float = float(
        os.getenv("TIMEOUT_ICEBERG_WRITE_S", "60.0")
    )

    # ── Base de datos (SQLite catalog, PostgreSQL) ─────────────────────────
    DB_CONNECT_S: float = float(
        os.getenv("TIMEOUT_DB_CONNECT_S", "3.0")
    )
    DB_QUERY_S: float = float(
        os.getenv("TIMEOUT_DB_QUERY_S", "10.0")
    )

    # ── Pipeline end-to-end ────────────────────────────────────────────────
    # Limit superior de seguridad para asyncio.wait_for() en tests y CI.
    PIPELINE_RUN_S: float = float(
        os.getenv("TIMEOUT_PIPELINE_RUN_S", "300.0")
    )
    RESAMPLE_RUN_S: float = float(
        os.getenv("TIMEOUT_RESAMPLE_RUN_S", "120.0")
    )
