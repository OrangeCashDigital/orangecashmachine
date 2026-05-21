# -*- coding: utf-8 -*-
"""
market_data/domain/policies/
==============================
Policies de dominio — reglas de negocio stateless.

Importar directamente desde los módulos SSOT:
  from market_data.domain.policies.data_quality_policy import DataQualityPolicy
  from market_data.domain.policies.repair import RepairPolicy

No re-exportar desde aquí: data_quality_policy importa DataQualityReport
de application/, lo que crea un ciclo si este __init__ hace eager imports.
"""

# Namespace puro — sin imports eager para evitar ciclos de inicialización.
