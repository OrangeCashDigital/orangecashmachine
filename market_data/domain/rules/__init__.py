# -*- coding: utf-8 -*-
"""
market_data/domain/rules/__init__.py
======================================

Reglas de negocio del bounded context market_data.

Estado actual
-------------
Este submódulo está reservado para reglas de dominio puras (sin I/O,
sin pandas, sin dependencias de infraestructura).

Las reglas de validación de velas (CandleValidator, CandleNormalizer)
viven en market_data.processing.validation — tienen dependencias de
processing layer (pandas, Silver schema) y no pertenecen al dominio puro.

Las invariantes de dataset (check_dataset_invariants) viven en
market_data.quality.invariants — operan sobre DataFrames y pertenecen
a la capa de quality/application.

Importar directamente desde sus ubicaciones canónicas:
  from market_data.processing.validation import CandleValidator
  from market_data.quality.invariants.invariants import check_dataset_invariants

Principios
----------
DIP    — el dominio no importa desde processing ni quality
SSOT   — cada tipo vive en un único lugar
KISS   — no crear indirecciones que no añaden valor
"""
from __future__ import annotations

__all__: list = []
