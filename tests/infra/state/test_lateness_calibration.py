# -*- coding: utf-8 -*-
"""
tests/infra/state/test_lateness_calibration.py
===============================================

Suite de LatenessCalibrationStore con InMemoryGapStore — sin Redis.

Principios de test
------------------
Aislamiento total : InMemoryGapStore, sin Redis, sin Prometheus.
Fail-Fast         : un assert por responsabilidad.
DRY               : fixture ``cal_store`` centraliza construcción.
Nomenclatura      : test_<método>_<condición>_<resultado_esperado>

Cobertura
---------
set:
  - persiste payload con campos obligatorios
  - guardrail cap: p99 > 60min → clampado a 60min
  - guardrail floor: p99 < 1min → clampado a 1min
  - p99 dentro de rango → persiste sin modificar
  - returns True en éxito

get_lateness_ms:
  - retorna int si existe calibración
  - retorna None si no existe (SafeOps)
  - retorna valor clampeado si guardrail actuó

get:
  - retorna dict completo con todos los campos
  - retorna None si no existe

delete:
  - elimina calibración existente → returns True
  - elimina inexistente → returns False (SafeOps)
"""
from __future__ import annotations

import pytest

from ocm_platform.infra.state.lateness_calibration import LatenessCalibrationStore
from ocm_platform.infra.state.gap_store             import InMemoryGapStore

# ── Constantes de test ───────────────────────────────────────────────────────
_ENV       = "test"
_EXCHANGE  = "bybit"
_TIMEFRAME = "1h"
_P99_MS    = 5 * 60_000   # 5 min — dentro de guardrails
_P95_MS    = 3 * 60_000
_SAMPLES   = 100

_CAP_MS   = 60 * 60_000   # 60 min — límite superior
_FLOOR_MS =  1 * 60_000   #  1 min — límite inferior


# ── Fixture ──────────────────────────────────────────────────────────────────

@pytest.fixture
def cal_store() -> LatenessCalibrationStore:
    """LatenessCalibrationStore respaldado por InMemoryGapStore — sin Redis."""
    return LatenessCalibrationStore(store=InMemoryGapStore(), env=_ENV)


@pytest.fixture
def cal_store_with_entry(cal_store: LatenessCalibrationStore) -> LatenessCalibrationStore:
    """Store con una calibración ya persistida."""
    cal_store.set(
        exchange     = _EXCHANGE,
        timeframe    = _TIMEFRAME,
        lateness_ms  = _P99_MS,
        p95_ms       = _P95_MS,
        sample_count = _SAMPLES,
    )
    return cal_store


# ══════════════════════════════════════════════════════════════════════════════
# set
# ══════════════════════════════════════════════════════════════════════════════

class TestSet:

    def test_set_retorna_true_en_exito(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        result = cal_store.set(
            exchange    = _EXCHANGE,
            timeframe   = _TIMEFRAME,
            lateness_ms = _P99_MS,
        )
        assert result is True

    def test_set_persiste_campos_obligatorios(
        self, cal_store_with_entry: LatenessCalibrationStore
    ) -> None:
        data = cal_store_with_entry.get(_EXCHANGE, _TIMEFRAME)
        assert data is not None
        assert data["exchange"]    == _EXCHANGE
        assert data["timeframe"]   == _TIMEFRAME
        assert data["lateness_ms"] == _P99_MS
        assert data["p95_ms"]      == _P95_MS
        assert data["sample_count"] == _SAMPLES
        assert "calibrated_at" in data
        assert "source"        in data

    def test_set_guardrail_cap_clampea_a_60min(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        enorme = _CAP_MS + 999_000  # muy por encima del cap
        cal_store.set(exchange=_EXCHANGE, timeframe=_TIMEFRAME, lateness_ms=enorme)
        ms = cal_store.get_lateness_ms(_EXCHANGE, _TIMEFRAME)
        assert ms == _CAP_MS

    def test_set_guardrail_floor_clampea_a_1min(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        minimo = 1_000  # 1 segundo — por debajo del floor
        cal_store.set(exchange=_EXCHANGE, timeframe=_TIMEFRAME, lateness_ms=minimo)
        ms = cal_store.get_lateness_ms(_EXCHANGE, _TIMEFRAME)
        assert ms == _FLOOR_MS

    def test_set_valor_dentro_de_rango_no_se_modifica(
        self, cal_store_with_entry: LatenessCalibrationStore
    ) -> None:
        ms = cal_store_with_entry.get_lateness_ms(_EXCHANGE, _TIMEFRAME)
        assert ms == _P99_MS


# ══════════════════════════════════════════════════════════════════════════════
# get_lateness_ms
# ══════════════════════════════════════════════════════════════════════════════

class TestGetLatenessMs:

    def test_retorna_int_si_existe(
        self, cal_store_with_entry: LatenessCalibrationStore
    ) -> None:
        ms = cal_store_with_entry.get_lateness_ms(_EXCHANGE, _TIMEFRAME)
        assert isinstance(ms, int)
        assert ms == _P99_MS

    def test_retorna_none_si_no_existe(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        result = cal_store.get_lateness_ms(_EXCHANGE, _TIMEFRAME)
        assert result is None

    def test_diferentes_timeframes_son_independientes(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        cal_store.set(exchange=_EXCHANGE, timeframe="1h",  lateness_ms=5 * 60_000)
        cal_store.set(exchange=_EXCHANGE, timeframe="15m", lateness_ms=2 * 60_000)
        assert cal_store.get_lateness_ms(_EXCHANGE, "1h")  == 5 * 60_000
        assert cal_store.get_lateness_ms(_EXCHANGE, "15m") == 2 * 60_000

    def test_diferentes_exchanges_son_independientes(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        cal_store.set(exchange="bybit",  timeframe=_TIMEFRAME, lateness_ms=5 * 60_000)
        cal_store.set(exchange="kucoin", timeframe=_TIMEFRAME, lateness_ms=3 * 60_000)
        assert cal_store.get_lateness_ms("bybit",  _TIMEFRAME) == 5 * 60_000
        assert cal_store.get_lateness_ms("kucoin", _TIMEFRAME) == 3 * 60_000


# ══════════════════════════════════════════════════════════════════════════════
# get
# ══════════════════════════════════════════════════════════════════════════════

class TestGet:

    def test_get_retorna_dict_completo(
        self, cal_store_with_entry: LatenessCalibrationStore
    ) -> None:
        data = cal_store_with_entry.get(_EXCHANGE, _TIMEFRAME)
        assert isinstance(data, dict)
        campos = {"exchange", "timeframe", "lateness_ms", "p95_ms",
                  "sample_count", "calibrated_at", "window_hours", "source"}
        assert campos.issubset(data.keys())

    def test_get_retorna_none_si_no_existe(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        assert cal_store.get(_EXCHANGE, _TIMEFRAME) is None


# ══════════════════════════════════════════════════════════════════════════════
# delete
# ══════════════════════════════════════════════════════════════════════════════

class TestDelete:

    def test_delete_existente_retorna_true(
        self, cal_store_with_entry: LatenessCalibrationStore
    ) -> None:
        result = cal_store_with_entry.delete(_EXCHANGE, _TIMEFRAME)
        assert result is True

    def test_delete_elimina_entrada(
        self, cal_store_with_entry: LatenessCalibrationStore
    ) -> None:
        cal_store_with_entry.delete(_EXCHANGE, _TIMEFRAME)
        assert cal_store_with_entry.get(_EXCHANGE, _TIMEFRAME) is None

    def test_delete_inexistente_retorna_false(
        self, cal_store: LatenessCalibrationStore
    ) -> None:
        result = cal_store.delete(_EXCHANGE, _TIMEFRAME)
        assert result is False


# ══════════════════════════════════════════════════════════════════════════════
# Invariante DIP
# ══════════════════════════════════════════════════════════════════════════════

class TestDIPInvariant:

    def test_store_acepta_inmemory_sin_redis(self) -> None:
        """LatenessCalibrationStore construible con InMemoryGapStore — DIP verificado."""
        store = LatenessCalibrationStore(store=InMemoryGapStore(), env="test")
        assert store.get_lateness_ms("bybit", "1h") is None
