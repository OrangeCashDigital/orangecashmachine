# -*- coding: utf-8 -*-
"""
market_data/ports/
==================

Contratos (puertos) del bounded context market_data.

Exports públicos
----------------
AnomalyRegistryPort — contrato de registro de anomalías de calidad
CursorStorePort     — contrato de persistencia de cursores de ingesta
encode_cursor_key   — función pura de codificación de claves de cursor
ExchangeAdapter     — contrato de adapters de exchange
MetricsPusherPort   — contrato de empuje de métricas al backend
OHLCVStorage        — contrato de storage OHLCV (Silver layer)

Arquitectura
------------
Los consumidores (domain, application) importan SIEMPRE desde ports/.
Los adapters concretos (outbound/) implementan estos contratos.
Esta separación garantiza DIP y permite inyección de mocks en tests.

Regla de dependencias
---------------------
ports/     → NO importa desde adapters/, application/ ni domain/
application/ → importa desde ports/ y domain/
adapters/  → importa desde ports/ para declarar qué implementa
"""

from market_data.ports.exchange import ExchangeAdapter
from market_data.ports.observability import MetricsPusherPort
from market_data.ports.quality import AnomalyRegistryPort
from market_data.ports.state import CursorStorePort, encode_cursor_key
from market_data.ports.storage import OHLCVStorage

__all__ = [
    "AnomalyRegistryPort",
    "CursorStorePort",
    "encode_cursor_key",
    "ExchangeAdapter",
    "MetricsPusherPort",
    "OHLCVStorage",
]
