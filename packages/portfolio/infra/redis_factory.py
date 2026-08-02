# -*- coding: utf-8 -*-
"""
portfolio/infra/redis_factory.py
==================================

Factory SSOT para conexiones Redis consumidas por PortfolioService.

Antes de este modulo, la construccion de redis.Redis(...) estaba
duplicada caracter por caracter entre execute_live.py y rebalance.py.
Ver docs/audits/2026-08-composition-root-audit.md, hallazgo 3.3.

Principios: SRP . DRY . SSOT
"""

from __future__ import annotations

import redis as redis_lib


def build_redis_client(
    *,
    host: str = "localhost",
    port: int = 6379,
    db: int = 1,
    socket_timeout: float = 3.0,
) -> redis_lib.Redis:
    """
    Construye el cliente Redis usado por RedisPositionStore.

    SSOT: unica funcion que instancia redis.Redis para el bounded
    context de portfolio. decode_responses=False es fijo --
    RedisPositionStore serializa/deserializa binario.
    """
    return redis_lib.Redis(
        host=host,
        port=port,
        db=db,
        socket_timeout=socket_timeout,
        decode_responses=False,
    )
