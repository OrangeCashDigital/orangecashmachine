"""shared/kafka/provenance.py

SSOT de procedencia (provenance) de los payloads Kafka del sistema, según
el Protocol Discovery Framework (ADR-0017).

Cada payload registrado en `shared.kafka.schemas` DEBE declarar su
procedencia antes de considerarse parte del SSOT operativo. Un contrato
sin procedencia explícita se considera provisional (ASSUMED) y no puede
ser SSOT ni habilitar capital real (Promotion Rule, ADR-0017 punto 14).

Este módulo es la ÚNICA fuente de verdad para el registro `PROVIDENCE`.
`tests/kafka/test_schema_provenance.py` lo consume para el fail-fast de
cobertura (F2.3); código de producción (p. ej. `apps/app/cli/live_hydra.py`,
B-23) lo consume para el gate de capital. Mantener el registro en un único
lugar evita que ambos consumidores diverjan (SSOT, DIP: el CLI depende de
esta abstracción de producción, no de un módulo de tests).

Taxonomía (categorías oficiales, ver ADR-0017 §Contract Provenance):
  PROTOCOL         — forma derivada de tráfico de exchange/fuente OBSERVADO real.
  DOCUMENTATION    — derivada de documentación oficial / OpenAPI del proveedor.
  UPSTREAM_LIBRARY — derivada del esquema unificado de una librería upstream
                     (CCXT / cryptofeed), que a su vez normaliza el wire.
  DOMAIN           — modelo interno del dominio OCM; NO existe en ningún wire
                     (eventos de estrategia, orden, posición, métrica canónica).
  ASSUMED          — provisional; diseñado por el dominio sin fuente verificada
                     ni productor cableado. NO SSOT. Debe resolverse en F3.

Solo PROTOCOL, DOCUMENTATION, UPSTREAM_LIBRARY y DOMAIN califican como SSOT
estable (Promotion Rule satisfecha). ASSUMED es provisional y bloquea capital
real para cualquier flujo que dependa de ese payload (B-23).

wire_status:
  "wired"  = hay productor/consumidor real que usa este payload en producción.
  "orphan" = NO hay productor/stream que use este payload (código muerto o futuro).
"""

from __future__ import annotations

_PROMOTED_STATES = frozenset({"PROTOCOL", "DOCUMENTATION", "UPSTREAM_LIBRARY", "DOMAIN"})

# ---------------------------------------------------------------------------
# Registro SSOT — (categoria, wire_status, justificación)
# ---------------------------------------------------------------------------
PROVIDENCE: dict[str, tuple[str, str, str]] = {
    # --- PROTOCOL (tráfico observado) ---
    "OrderBookSnapshotPayload": (
        "PROTOCOL",
        "wired",
        "WS Bybit v2 (cryptofeed); u/seq/cts del raw; P0: docs/audits/p0_bybit/evidence/",
    ),
    "OrderBookDeltaPayload": (
        "PROTOCOL",
        "wired",
        "WS Bybit v2 atómico multinivel (cryptofeed); u/seq/cts del raw; P0: docs/audits/p0_bybit/evidence/",
    ),
    # --- UPSTREAM_LIBRARY (esquema unificado CCXT) ---
    "KafkaOHLCVBar": ("UPSTREAM_LIBRARY", "wired", "CCXT fetch_ohlcv tuple (timestamp,o,h,l,c,v)"),
    "EventPayload": ("UPSTREAM_LIBRARY", "wired", "CCXT OHLCV cuesco; envoltura interna"),
    # --- DOCUMENTATION (OpenAPI / docs oficial) ---
    "ExternalMetricPayload": ("DOCUMENTATION", "wired", "CoinGlass/CMC OpenAPI; ver test_external_wire.py"),
    # --- DOMAIN (dominio interno, no existe en wire) ---
    "SignalPayload": ("DOMAIN", "wired", "estrategia → RiskGate (evento propio)"),
    "ApprovedSignalPayload": ("DOMAIN", "wired", "RiskGate → ejecución (evento propio)"),
    "RejectedSignalPayload": ("DOMAIN", "wired", "RiskGate rechazo (evento propio)"),
    "OrderFilledPayload": ("DOMAIN", "wired", "OMSS→portfolio (evento propio)"),
    "OrderRejectedPayload": ("DOMAIN", "wired", "OMSS→ops (evento propio)"),
    "PositionOpenedPayload": ("DOMAIN", "wired", "portfolio (evento propio)"),
    "PositionClosedPayload": ("DOMAIN", "wired", "portfolio (evento propio)"),
    # --- ASSUMED (provisional — sin productor real que lo use) ---
    "TradePayload": (
        "ASSUMED",
        "orphan",
        "no productor usa el schema (kafka_trade_publisher serializa raw NormalizedTrade JSON)",
    ),
    "TradeSeriesPayload": ("ASSUMED", "orphan", "no productor emitente (TradesAggregator pendiente)"),
    "FundingRatePayload": (
        "ASSUMED",
        "orphan",
        "campos extra (interval_h, predicted_rate, next_funding_ms) sin fuente; CCXT solo da ts+rate",
    ),
    "OpenInterestPayload": (
        "ASSUMED",
        "orphan",
        "open_interest_value/mark_price derivados sin fuente; CCXT solo da ts+oi",
    ),
    "LiquidationPayload": ("ASSUMED", "orphan", "sin fuente ni productor (on_liquidation es código muerto)"),
}


def is_promoted(schema_name: str) -> bool:
    """True si `schema_name` tiene provenance SSOT estable (Promotion Rule, ADR-0017 §14).

    Fail-closed por diseño: un schema ausente del registro NO se considera
    promovido — debe registrarse explícitamente en PROVIDENCE primero
    (mismo principio fail-fast que aplica el test de cobertura F2.3).

    Parameters
    ----------
    schema_name : nombre de la clase del payload (p. ej. "OrderFilledPayload").

    Returns
    -------
    bool : True solo si la categoría registrada está en
        {PROTOCOL, DOCUMENTATION, UPSTREAM_LIBRARY, DOMAIN}. False si el
        schema no está registrado o su categoría es ASSUMED.
    """
    entry = PROVIDENCE.get(schema_name)
    if entry is None:
        return False
    category, _wire_status, _justification = entry
    return category in _PROMOTED_STATES


def require_promoted(*schema_names: str) -> None:
    """Fail-closed: lanza ValueError si algún schema en `schema_names` no está promovido.

    Uso previsto: guards de arranque que condicionan capital real a que la
    Promotion Rule (ADR-0017 §14) esté satisfecha para los payloads críticos
    del flujo de ejecución (p. ej. OrderFilledPayload, OrderRejectedPayload).

    Raises
    ------
    ValueError : con el detalle de cada schema no promovido (nombre,
        categoría actual si está registrado, o "no registrado").
    """
    failures: list[str] = []
    for name in schema_names:
        entry = PROVIDENCE.get(name)
        if entry is None:
            failures.append(f"{name}: no registrado en PROVIDENCE")
        elif entry[0] not in _PROMOTED_STATES:
            failures.append(f"{name}: categoría '{entry[0]}' (ASSUMED, no promovido)")
    if failures:
        raise ValueError("Promotion Rule (ADR-0017 §14) no satisfecha para: " + "; ".join(failures))
