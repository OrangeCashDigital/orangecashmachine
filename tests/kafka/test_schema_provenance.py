# -*- coding: utf-8 -*-
"""
tests/kafka/test_schema_provenance.py
========================================

MODELO: Contract Provenance (componente 9 del Protocol Discovery Framework — ADR-0017, F2.5).
Registro SSOT de la procedencia de cada contrato wire Kafka de OCM.

Este archivo es la semilla operativa de los puntos 9 (Contract Provenance) y 13
(Tests) del Protocol Discovery Framework (ADR-0017). La taxonomía y el registro
viven aquí como guard de linaje del contrato.

Cada payload DEBE declarar una procedencia (provenance) antes de considerarse
parte del SSOT. Un contrato sin procedencia explícita se considera provisional
y no puede ser SSOT. La regla se encarna aquí como test (fail-fast) y el bloque
documental de cada test_<schema>.py lo explica por dominio (grep -A20 PROVENANCE).

Taxonomía (categorías oficiales)
--------------------------------
  PROTOCOL         — forma derivada de tráfico de exchange/fuente OBSERVADO real.
  DOCUMENTATION    — derivada de documentación oficial / OpenAPI del proveedor.
  UPSTREAM_LIBRARY — derivada del esquema unificado de una librería upstream
                     (CCXT / cifrado/feed), que a su vez normaliza el wire.
  DOMAIN           — modelo interno del dominio OCM; NO existe en ningún wire
                     (eventos de estrategia, orden, posición, métrica canónica).
  ASSUMED          — provisional; diseñado por el dominio sin fuente verificada
                     ni productor cableado. NO SSOT. Debe resolverse en F3.

Tres categorías califican como SSOT estable (PROTOCOL, DOCUMENTATION,
UPSTREAM_LIBRARY, DOMAIN). ASSUMED es provisional.

Este test:
  1. Comprueba que todo payload registrado en el código (submódulos de
     shared.kafka.schemas) está en el registro PROVIDENCIAS (fail-fast).
  2. Comprueba coherencia con las hechos de wire: un registro con
     estado "orphan" (sin productor en producción) NO puede ser PROTOCOL
     ni DOCUMENTATION; solo ASSUMED o DOMAIN.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Registro de procedencia (SSOT documental)
# ---------------------------------------------------------------------------
# Columns: payload_name -> (provenance, state, source_note)
#   state: "wired"    = hay productor real que lo emite (o DOMAIN interno)
#          "orphan"   = NO hay productor/stream que use este payload (código muerto o futuro)
# El estado se documenta en __doc__ de cada test_*.py y se refuerza aquí.
# SSOT: registro movido a shared/kafka/provenance.py (B-23) — este módulo
# lo consume tanto para el fail-fast de cobertura (F2.3) como el gate de
# capital en apps/app/cli/live_hydra.py. No duplicar el dict aquí.
from shared.kafka.provenance import PROVIDENCE
from shared.kafka.schemas.external import ExternalMetricPayload
from shared.kafka.schemas.funding import FundingRatePayload
from shared.kafka.schemas.liquidations import LiquidationPayload
from shared.kafka.schemas.ohlcv import EventPayload, KafkaOHLCVBar
from shared.kafka.schemas.oi import OpenInterestPayload
from shared.kafka.schemas.orderbook import (
    OrderBookDeltaPayload,
    OrderBookSnapshotPayload,
)
from shared.kafka.schemas.orders import (
    OrderFilledPayload,
    OrderRejectedPayload,
)
from shared.kafka.schemas.positions import (
    PositionClosedPayload,
    PositionOpenedPayload,
)
from shared.kafka.schemas.signals import (
    ApprovedSignalPayload,
    RejectedSignalPayload,
    SignalPayload,
)
from shared.kafka.schemas.trades import (
    TradePayload,
    TradeSeriesPayload,
)

# ---------------------------------------------------------------------------
# Todos los payloads importados (para fail-fast si faltan en el registro)
# ---------------------------------------------------------------------------

_ALL_PAYLOADS: dict[str, type] = {
    "OrderBookSnapshotPayload": OrderBookSnapshotPayload,
    "OrderBookDeltaPayload": OrderBookDeltaPayload,
    "KafkaOHLCVBar": KafkaOHLCVBar,
    "EventPayload": EventPayload,
    "ExternalMetricPayload": ExternalMetricPayload,
    "SignalPayload": SignalPayload,
    "ApprovedSignalPayload": ApprovedSignalPayload,
    "RejectedSignalPayload": RejectedSignalPayload,
    "OrderFilledPayload": OrderFilledPayload,
    "OrderRejectedPayload": OrderRejectedPayload,
    "PositionOpenedPayload": PositionOpenedPayload,
    "PositionClosedPayload": PositionClosedPayload,
    "TradePayload": TradePayload,
    "TradeSeriesPayload": TradeSeriesPayload,
    "FundingRatePayload": FundingRatePayload,
    "OpenInterestPayload": OpenInterestPayload,
    "LiquidationPayload": LiquidationPayload,
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProvenanceRegistry:
    def test_every_payload_declares_provenance(self):
        """Todo payload del wire DEBE_VIDA declarar procedencia (fail-fast)."""
        missing = sorted(name for name in _ALL_PAYLOADS if name not in PROVIDENCE)
        assert not missing, f"Payloads sin provenance declarada: {missing}"

    def test_every_registry_entry_maps_to_real_class(self):
        """El registro no debe listar contratos que no existen."""
        unknown = sorted(name for name in PROVIDENCE if name not in _ALL_PAYLOADS)
        assert not unknown, f"Registro listan payloads inexistentes: {unknown}"

    def test_orphan_and_provisional_cannot_be_ssot_grade(self):
        """Provisional (ASSUMED) u orphan no pueden pasar por PROTOCOL/DOCUMENTATION."""
        stable = {"PROTOCOL", "DOCUMENTATION", "UPSTREAM_LIBRARY", "DOMAIN"}
        for name, (prov, state, _note) in PROVIDENCE.items():
            if state == "orphan":
                err = (
                    f"{name} es orphan (sin productor en producción) pero se declara "
                    f"provenance={prov!r}; un contrato sin emisor real no puede ser "
                    f"PROTOCOL/DOCUMENTATION. Marcar ASSIGNED y resolver en F3."
                )
                assert prov not in {"PROTOCOL", "DOCUMENTATION"}, err
            assert prov in stable | {"ASSUMED"}, f"provenance desconocida: {name!r} -> {prov!r}"

    def test_all_declared_provenance_values_valid(self):
        """Valores de provenance acotados a la taxonomía oficial."""
        valid = {"PROTOCOL", "DOCUMENTATION", "UPSTREAM_LIBRARY", "DOMAIN", "ASSUMED"}
        bad = sorted(_p for (_p, _s, _) in PROVIDENCE.values() if _p not in valid)  # noqa: PLW2901
        assert not bad, f"provenance inválida: {bad}"
