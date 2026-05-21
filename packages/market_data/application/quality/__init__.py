# -*- coding: utf-8 -*-
"""
market_data/application/quality/
==================================
Lógica de calidad en capa de aplicación.

Importar directamente desde los módulos SSOT:
  from market_data.application.quality.data_quality import DataQualityChecker, DataQualityReport
  from market_data.application.quality.pipeline import QualityPipeline
  from market_data.application.quality.report import DataQualityReport  # alias ge_checker

No re-exportar desde aquí: pipeline importa de domain.policies e infrastructure,
lo que puede crear ciclos si este __init__ hace eager imports.
"""

# Namespace puro — sin imports eager para evitar ciclos de inicialización.
