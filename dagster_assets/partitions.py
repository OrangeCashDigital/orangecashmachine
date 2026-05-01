# -*- coding: utf-8 -*-
"""
dagster_assets/partitions.py
============================

SSOT de definiciones de partición para OrangeCashMachine.

Problema sin particiones
------------------------
Sin particiones, re-materializar un asset OHLCV reprocesa todo el
histórico desde config_start_date. Para BTC/USDT 1m desde 2024-01-01
eso son ~500k filas por symbol — horas de bloqueo en un backfill.

Con particiones diarias
-----------------------
- Re-materialización = solo el día afectado (fail aislado)
- Backfill = Dagster paraleliza días automáticamente
- Linaje explícito en la UI: qué día está completo, cuál falló
- Compatible con pyiceberg partitionSpec (partition by day)

Configuración
-------------
OCM_OHLCV_START_DATE: debe coincidir con config_start_date del YAML
de configuración de OCM para garantizar alineación SSOT.

Fail-Fast: formato inválido explota en import time, no mid-run.

Principios: SSOT · DRY · Fail-Fast · KISS
"""
from __future__ import annotations

import os
from datetime import date

from dagster import DailyPartitionsDefinition, WeeklyPartitionsDefinition

# ---------------------------------------------------------------------------
# Resolución SSOT de fecha de inicio
# ---------------------------------------------------------------------------
# OCM_OHLCV_START_DATE debe coincidir con el config_start_date de OCM.
# Default conservador: 2024-01-01 (inicio del histórico reciente).

_START_DATE_RAW: str = os.environ.get("OCM_OHLCV_START_DATE", "2024-01-01")

# Fail-Fast: validar en import time — mejor que un KeyError críptico
# en el primer tick del schedule de producción.
try:
    date.fromisoformat(_START_DATE_RAW)
except ValueError as exc:
    raise EnvironmentError(
        f"OCM_OHLCV_START_DATE={_START_DATE_RAW!r} no es fecha ISO válida. "
        "Formato requerido: YYYY-MM-DD. "
        "Ejemplo: OCM_OHLCV_START_DATE=2024-01-01"
    ) from exc

# ---------------------------------------------------------------------------
# Partición diaria — granularidad operacional para trading data
# ---------------------------------------------------------------------------
# end_offset=1: incluye el día actual (incompleto durante la sesión).
# Útil para ingesta incremental que corre varias veces al día.
DAILY_PARTITIONS = DailyPartitionsDefinition(
    start_date = _START_DATE_RAW,
    end_offset = 1,
)

# ---------------------------------------------------------------------------
# Partición semanal — para gold layer / reporting agregado
# ---------------------------------------------------------------------------
# No usada en bronze/silver. Disponible para assets de calidad y resúmenes.
WEEKLY_PARTITIONS = WeeklyPartitionsDefinition(
    start_date = _START_DATE_RAW,
)

__all__ = ["DAILY_PARTITIONS", "WEEKLY_PARTITIONS"]
