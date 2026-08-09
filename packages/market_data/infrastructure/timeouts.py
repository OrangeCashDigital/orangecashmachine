# -*- coding: utf-8 -*-
"""
market_data/infrastructure/timeouts.py
=======================================

SSOT de todos los timeouts del sistema.

Regla de uso
------------
NUNCA escribir un número mágico de timeout en el código.
Importar siempre desde aquí — ruta canónica market_data.*:

    from market_data.infrastructure.timeouts import Timeouts
    redis_client = Redis(socket_timeout=Timeouts.REDIS_OPERATION_S)

(Nota F-011: el antiguo ejemplo "from infrastructure.timeouts import
Timeouts" apuntaba a un módulo que NO existe — ruta corregida.)

Naturaleza de los valores
-------------------------
Estimaciones conservadoras de operador, ajustables por env vars.
Son COTA OPERATIVA inicial, NO medición garantizada: recalibrar con
telemetría real antes de confiar en cada valor como referencia p99.

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
    REDIS_CONNECT_S: float = float(os.getenv("TIMEOUT_REDIS_CONNECT_S", "2.0"))
    REDIS_OPERATION_S: float = float(os.getenv("TIMEOUT_REDIS_OPERATION_S", "1.0"))

    # ── CCXT / Exchange HTTP ───────────────────────────────────────────────
    # Cota operativa inicial (~0.8s latencia típica en spot Bybit/KuCoin);
    # 10s = margen conservador. Recalibrar con telemetría real.
    CCXT_REQUEST_S: float = float(os.getenv("TIMEOUT_CCXT_REQUEST_S", "10.0"))
    # Conexión TCP inicial al exchange
    CCXT_CONNECT_S: float = float(os.getenv("TIMEOUT_CCXT_CONNECT_S", "5.0"))

    # ── Iceberg / PyIceberg ────────────────────────────────────────────────
    # Cota operativa: scan() sobre S3 con partition pruning tarda típicamente
    # ~3s en tablas <100GB (no medido en prod aun).
    # Sin timeout → deadlock silencioso en producción.
    ICEBERG_SCAN_S: float = float(os.getenv("TIMEOUT_ICEBERG_SCAN_S", "30.0"))
    ICEBERG_WRITE_S: float = float(os.getenv("TIMEOUT_ICEBERG_WRITE_S", "60.0"))

    # ── Base de datos (SQLite catalog, PostgreSQL) ─────────────────────────
    DB_CONNECT_S: float = float(os.getenv("TIMEOUT_DB_CONNECT_S", "3.0"))
    DB_QUERY_S: float = float(os.getenv("TIMEOUT_DB_QUERY_S", "10.0"))

    # ── Pipeline end-to-end ────────────────────────────────────────────────
    # Limit superior de seguridad para asyncio.wait_for() en tests y CI.
    PIPELINE_RUN_S: float = float(os.getenv("TIMEOUT_PIPELINE_RUN_S", "300.0"))
    RESAMPLE_RUN_S: float = float(os.getenv("TIMEOUT_RESAMPLE_RUN_S", "120.0"))
