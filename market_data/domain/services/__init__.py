# -*- coding: utf-8 -*-
"""
market_data/domain/services/__init__.py
=========================================

Servicios de dominio del bounded context market_data.

Estado actual
-------------
Este submódulo está reservado para Domain Services puros (DDD):
lógica de negocio que coordina múltiples VOs/entidades sin I/O.

Los servicios actualmente identificados como candidatos
(DataQualityPolicy, QualityPipeline) viven en market_data.quality
porque orquestan I/O, registros SQLite y lineage — no son
servicios de dominio puros sino application services.

Importar directamente desde sus ubicaciones canónicas:
  from market_data.quality.pipeline import QualityPipeline
  from market_data.quality.policies.data_quality_policy import DataQualityPolicy

Principios
----------
DIP    — el dominio no importa desde quality ni processing
SSOT   — cada tipo vive en un único lugar
KISS   — no crear indirecciones que no añaden valor
"""
from __future__ import annotations

__all__: list = []
