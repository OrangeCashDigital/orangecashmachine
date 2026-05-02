# -*- coding: utf-8 -*-
"""
tests/infra/state/test_gap_registry.py
========================================

Suite de GapRegistry con InMemoryGapStore — cero dependencias externas.

Principios de test
------------------
Aislamiento total : InMemoryGapStore, sin Redis, sin red.
Fail-Fast         : cada test verifica una sola responsabilidad.
DRY               : fixture ``registry`` centraliza construcción.
Nomenclatura      : test_<método>_<condición>_<resultado_esperado>

Cobertura
---------
register:
  - primer registro persiste con campos correctos
  - registro duplicado es ignorado (idempotente)
  - repair_attempts arranca en 0

mark_healed:
  - gap existente es eliminado → returns True
  - gap inexistente → returns False (SafeOps, no lanza)

is_irrecoverable:
  - gap irrecuperable persiste → returns True
  - gap normal no es irrecuperable → returns False

increment_attempts:
  - incrementa repair_attempts en 1 por llamada
  - múltiples llamadas acumulan correctamente

list_pending:
  - retorna lista vacía si no hay gaps
  - retorna todos los gaps registrados
  - filtra por exchange si se pasa argumento

count_pending:
  - retorna 0 si no hay gaps
  - retorna conteo correcto con múltiples gaps

get:
  - retorna dict con todos los campos si existe
  - retorna None si no existe (SafeOps)
"""
from __future__ import annotations

import pytest

from ocm_platform.infra.state.gap_registry import GapRegistry
from ocm_platform.infra.state.gap_store    import InMemoryGapStore

# ── Constantes de test ───────────────────────────────────────────────────────
_ENV       = "test"
_EXCHANGE  = "bybit"
_SYMBOL    = "BTC/USDT"
_TIMEFRAME = "1h"
_START_MS  = 1_700_000_000_000
_END_MS    = 1_700_003_600_000
_EXPECTED  = 1
_GAP_S     = 3600.0


# ── Fixture ──────────────────────────────────────────────────────────────────

@pytest.fixture
def registry() -> GapRegistry:
    """GapRegistry respaldado por InMemoryGapStore — sin Redis."""
    return GapRegistry(store=InMemoryGapStore(), env=_ENV)


@pytest.fixture
def registry_with_gap(registry: GapRegistry) -> GapRegistry:
    """Registry con un gap ya registrado."""
    registry.register(
        exchange    = _EXCHANGE,
        symbol      = _SYMBOL,
        timeframe   = _TIMEFRAME,
        start_ms    = _START_MS,
        end_ms      = _END_MS,
        expected    = _EXPECTED,
        gap_seconds = _GAP_S,
    )
    return registry


# ══════════════════════════════════════════════════════════════════════════════
# register
# ══════════════════════════════════════════════════════════════════════════════

class TestRegister:

    def test_register_nuevo_gap_persiste(self, registry: GapRegistry) -> None:
        result = registry.register(
            exchange    = _EXCHANGE,
            symbol      = _SYMBOL,
            timeframe   = _TIMEFRAME,
            start_ms    = _START_MS,
            end_ms      = _END_MS,
            expected    = _EXPECTED,
            gap_seconds = _GAP_S,
        )
        assert result is True
        data = registry.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert data is not None
        assert data["exchange"]   == _EXCHANGE
        assert data["symbol"]     == _SYMBOL
        assert data["timeframe"]  == _TIMEFRAME
        assert data["start_ms"]   == _START_MS
        assert data["end_ms"]     == _END_MS
        assert data["expected"]   == _EXPECTED

    def test_register_repair_attempts_arranca_en_cero(
        self, registry_with_gap: GapRegistry
    ) -> None:
        data = registry_with_gap.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert data is not None
        assert data["repair_attempts"] == 0

    def test_register_duplicado_es_ignorado(
        self, registry_with_gap: GapRegistry
    ) -> None:
        # Segundo register sobre la misma clave — no debe sobrescribir
        result = registry_with_gap.register(
            exchange    = _EXCHANGE,
            symbol      = _SYMBOL,
            timeframe   = _TIMEFRAME,
            start_ms    = _START_MS,
            end_ms      = _END_MS + 999,   # valor diferente — no debe persistir
            expected    = 99,
            gap_seconds = _GAP_S,
        )
        assert result is False
        data = registry_with_gap.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert data is not None
        assert data["expected"] == _EXPECTED  # valor original intacto


# ══════════════════════════════════════════════════════════════════════════════
# mark_healed
# ══════════════════════════════════════════════════════════════════════════════

class TestMarkHealed:

    def test_mark_healed_gap_existente_retorna_true(
        self, registry_with_gap: GapRegistry
    ) -> None:
        result = registry_with_gap.mark_healed(
            _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS
        )
        assert result is True

    def test_mark_healed_elimina_gap(
        self, registry_with_gap: GapRegistry
    ) -> None:
        registry_with_gap.mark_healed(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        data = registry_with_gap.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert data is None

    def test_mark_healed_gap_inexistente_retorna_false(
        self, registry: GapRegistry
    ) -> None:
        result = registry.mark_healed(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert result is False

    def test_mark_healed_irrecoverable_persiste_centinela(
        self, registry_with_gap: GapRegistry
    ) -> None:
        registry_with_gap.mark_healed(
            _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS, irreversible=True
        )
        assert registry_with_gap.is_irrecoverable(
            _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS
        ) is True


# ══════════════════════════════════════════════════════════════════════════════
# is_irrecoverable
# ══════════════════════════════════════════════════════════════════════════════

class TestIsIrrecoverable:

    def test_gap_normal_no_es_irrecuperable(
        self, registry_with_gap: GapRegistry
    ) -> None:
        assert registry_with_gap.is_irrecoverable(
            _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS
        ) is False

    def test_gap_inexistente_no_es_irrecuperable(
        self, registry: GapRegistry
    ) -> None:
        assert registry.is_irrecoverable(
            _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS
        ) is False


# ══════════════════════════════════════════════════════════════════════════════
# increment_attempts
# ══════════════════════════════════════════════════════════════════════════════

class TestIncrementAttempts:

    def test_increment_una_vez(self, registry_with_gap: GapRegistry) -> None:
        registry_with_gap.increment_attempts(
            _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS
        )
        data = registry_with_gap.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert data is not None
        assert data["repair_attempts"] == 1

    def test_increment_acumula(self, registry_with_gap: GapRegistry) -> None:
        for _ in range(3):
            registry_with_gap.increment_attempts(
                _EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS
            )
        data = registry_with_gap.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert data is not None
        assert data["repair_attempts"] == 3

    def test_increment_gap_inexistente_no_lanza(
        self, registry: GapRegistry
    ) -> None:
        # SafeOps: no debe propagar excepción
        registry.increment_attempts(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)


# ══════════════════════════════════════════════════════════════════════════════
# list_pending / count_pending
# ══════════════════════════════════════════════════════════════════════════════

class TestListAndCount:

    def test_list_pending_vacio(self, registry: GapRegistry) -> None:
        assert registry.list_pending() == []

    def test_count_pending_vacio(self, registry: GapRegistry) -> None:
        assert registry.count_pending() == 0

    def test_list_pending_retorna_gap_registrado(
        self, registry_with_gap: GapRegistry
    ) -> None:
        gaps = registry_with_gap.list_pending()
        assert len(gaps) == 1
        assert gaps[0]["symbol"] == _SYMBOL

    def test_count_pending_un_gap(
        self, registry_with_gap: GapRegistry
    ) -> None:
        assert registry_with_gap.count_pending() == 1

    def test_list_pending_multiples_gaps(self, registry: GapRegistry) -> None:
        for i in range(3):
            registry.register(
                exchange    = _EXCHANGE,
                symbol      = f"SYM{i}/USDT",
                timeframe   = _TIMEFRAME,
                start_ms    = _START_MS + i * 3_600_000,
                end_ms      = _END_MS   + i * 3_600_000,
                expected    = 1,
                gap_seconds = _GAP_S,
            )
        assert registry.count_pending() == 3
        assert len(registry.list_pending()) == 3

    def test_list_pending_filtra_por_exchange(
        self, registry: GapRegistry
    ) -> None:
        registry.register(
            exchange    = "bybit",
            symbol      = _SYMBOL,
            timeframe   = _TIMEFRAME,
            start_ms    = _START_MS,
            end_ms      = _END_MS,
            expected    = 1,
            gap_seconds = _GAP_S,
        )
        registry.register(
            exchange    = "kucoin",
            symbol      = _SYMBOL,
            timeframe   = _TIMEFRAME,
            start_ms    = _START_MS,
            end_ms      = _END_MS,
            expected    = 1,
            gap_seconds = _GAP_S,
        )
        bybit  = registry.list_pending(exchange="bybit")
        kucoin = registry.list_pending(exchange="kucoin")
        assert len(bybit)  == 1
        assert len(kucoin) == 1
        assert bybit[0]["exchange"]  == "bybit"
        assert kucoin[0]["exchange"] == "kucoin"


# ══════════════════════════════════════════════════════════════════════════════
# get
# ══════════════════════════════════════════════════════════════════════════════

class TestGet:

    def test_get_gap_existente_retorna_dict(
        self, registry_with_gap: GapRegistry
    ) -> None:
        data = registry_with_gap.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert isinstance(data, dict)
        assert "gap_seconds" in data
        assert "detected_at" in data
        assert "source"      in data

    def test_get_gap_inexistente_retorna_none(
        self, registry: GapRegistry
    ) -> None:
        result = registry.get(_EXCHANGE, _SYMBOL, _TIMEFRAME, _START_MS)
        assert result is None


# ══════════════════════════════════════════════════════════════════════════════
# Invariante DIP: InMemoryGapStore satisface GapStorePort
# ══════════════════════════════════════════════════════════════════════════════

class TestDIPInvariant:

    def test_inmemory_satisface_gapstoreport(self) -> None:
        from ocm_platform.infra.state.gap_store import GapStorePort
        store = InMemoryGapStore()
        assert isinstance(store, GapStorePort)

    def test_registry_acepta_inmemory_sin_redis(self) -> None:
        """GapRegistry construible con InMemoryGapStore — DIP verificado."""
        reg = GapRegistry(store=InMemoryGapStore(), env="test")
        assert reg.count_pending() == 0
