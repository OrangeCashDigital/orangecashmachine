# -*- coding: utf-8 -*-
"""
ocm_platform/infra/state/encoding.py
======================================

Codificación de segmentos de claves Redis.

Responsabilidad
---------------
Proveer una función pura y sin estado para codificar segmentos arbitrarios
de texto en un formato seguro para usarse como componente de clave Redis.

Por qué existe este módulo
--------------------------
Las claves Redis en OCM siguen una estructura jerárquica separada por ':'.
Valores como "BTC/USDT" o "bybit_swap" contienen caracteres que colisionan
con ese separador o con otros caracteres especiales del protocolo.

encode_redis_key resuelve esto con base64 urlsafe (sin padding), garantizando
que cualquier string arbitrario sea un segmento de clave seguro y reversible.

Consumidores
------------
  cursor_store.py        — encode_cursor_key delega aquí (SSOT)
  gap_registry.py        — _gap_key / _gap_prefix
  lateness_calibration.py — _lateness_key
  backfill.py            — claves de origen y backfill

Principios
----------
SRP  — única responsabilidad: codificación de segmentos de clave
SSOT — una implementación, cero duplicaciones
KISS — función pura, sin estado, sin efectos secundarios
"""
from __future__ import annotations

import base64


def encode_redis_key(value: str) -> str:
    """
    Codifica un segmento de clave Redis en base64 urlsafe sin padding.

    Garantiza que caracteres especiales (/, :, espacios) no corrompan
    la estructura jerárquica de las claves Redis (separador ':').

    Parameters
    ----------
    value : str
        Segmento a codificar (exchange, symbol, timeframe, env…).

    Returns
    -------
    str
        Cadena base64 urlsafe sin '=' de relleno.

    Examples
    --------
    >>> encode_redis_key("BTC/USDT")
    'QlRDL1VTRFQ'
    >>> encode_redis_key("bybit")
    'Ynliafl'
    """
    return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")
