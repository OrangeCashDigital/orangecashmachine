# -*- coding: utf-8 -*-
"""
shared/exceptions/
==================

Excepciones base compartidas entre bounded contexts.

Jerarquía
---------
OCMError                   — raíz de todas las excepciones de OCM
  ├── OCMConfigError       — errores de configuración (ocm.config)
  ├── OCMRuntimeError      — errores de ejecución de pipeline
  └── OCMInfrastructureError — errores de conectividad (exchange, redis, kafka)

Uso
---
Cada bounded context subclasifica desde aquí para su dominio:

    # en market_data/exceptions.py
    from shared.exceptions import OCMError

    class MarketDataError(OCMError): ...
    class FetchError(MarketDataError): ...

Regla: ZERO imports de módulos internos del proyecto (BC-01).
"""


class OCMError(Exception):
    """Raíz de la jerarquía de excepciones de OrangeCashMachine."""


class OCMConfigError(OCMError):
    """Error en el sistema de configuración (Hydra/OmegaConf/Pydantic)."""


class OCMRuntimeError(OCMError):
    """Error durante la ejecución de un pipeline o run."""


class OCMInfrastructureError(OCMError):
    """Error de conectividad con infraestructura externa (exchange, Redis, Kafka)."""


__all__ = [
    "OCMError",
    "OCMConfigError",
    "OCMRuntimeError",
    "OCMInfrastructureError",
]
