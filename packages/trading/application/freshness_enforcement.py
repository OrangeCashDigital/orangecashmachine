# -*- coding: utf-8 -*-
"""
trading/application/freshness_enforcement.py
============================================

Enforcement de freshness pre-orden (execution gate del OMS).

Responsabilidad
---------------
Resolver la identidad OPERACIONAL del stream a partir de la señal, consultar
su freshness y BLOQUEAR (fail-closed) si no puede garantizarse que los datos
están frescos. Es el único punto donde trading traduce
``symbol → stream_id``: la identidad la decide el catálogo
(``shared/contracts/stream_identity.py``), nunca este módulo ni el OMS.

Flujo (fail-closed en cada paso)
--------------------------------
    symbol
      → catálogo.resolve(exchange, symbol)      # 0/ambiguo ⇒ bloqueo
      → consumer.get_freshness(stream_id)       # error ⇒ bloqueo
      → snapshot.stream_id == stream_id         # mismatch ⇒ bloqueo
      → snapshot.is_stale                       # stale ⇒ bloqueo
      → OK

Contrato con el resto de la cadena
----------------------------------
· Startup gate (``TradingCompositionRoot.assemble_live``) y execution gate
  (este módulo, invocado por ``OMS.submit``) son controles INDEPENDIENTES:
  el segundo no confía en que el primero se haya ejecutado.
· No hay fallback a ``signal.symbol`` como ``stream_id`` (F-02).
· Una identidad desconocida NUNCA se degrada a "fresh".

Principios: SRP · DIP · FAIL-CLOSED · KISS · BC-59 (application > ports > domain)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from shared.contracts.boundaries import FreshnessSnapshot
    from shared.contracts.stream_identity import StreamIdentityCatalog
    from trading.ports.inbound.freshness_consumer import FreshnessConsumerPort


# ---------------------------------------------------------------------------
# Errores (bloqueo explícito — el OMS los convierte en rechazo trazable)
# ---------------------------------------------------------------------------


class FreshnessBlockedError(Exception):
    """
    La orden NO puede enviarse porque no se pudo garantizar freshness.

    ``reason`` es un código estable para logs/alertas:
        unknown_stream_identity | ambiguous_stream_identity | provider_unavailable
        identity_mismatch | stale
    """

    def __init__(self, reason: str, *, stream_id: Optional[str] = None, detail: str = "") -> None:
        self.reason = reason
        self.stream_id = stream_id
        self.detail = detail
        message = f"freshness blocked: {reason}"
        if stream_id:
            message = f"{message} | stream_id={stream_id}"
        if detail:
            message = f"{message} | {detail}"
        super().__init__(message)


class FreshnessUnknownError(FreshnessBlockedError):
    """No existe identidad declarada para el símbolo (0 o >1 bindings)."""


class FreshnessUnavailableError(FreshnessBlockedError):
    """El proveedor de freshness no pudo responder (error de proveedor)."""


class FreshnessIdentityMismatchError(FreshnessBlockedError):
    """El snapshot devuelto pertenece a otro stream — no certifica este."""


class FreshnessStaleError(FreshnessBlockedError):
    """Los datos del stream están obsoletos respecto del umbral."""


# ---------------------------------------------------------------------------
# FreshnessEnforcement
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FreshnessEnforcement:
    """
    Enforcement pre-orden: identidad (catálogo) + estado (consumer).

    Se construye en el Composition Root y se inyecta en el OMS. Es inmutable:
    no hay setters ni flags que permitan desactivar el enforcement en runtime
    (un cambio de política exige reensamblar el runtime).

    Invariantes (fail-fast)
    -----------------------
    · ``consumer`` y ``catalog`` no nulos; ``exchange`` no vacío.
    """

    consumer: "FreshnessConsumerPort"
    catalog: "StreamIdentityCatalog"
    exchange: str

    def __post_init__(self) -> None:
        if self.consumer is None:
            raise ValueError("FreshnessEnforcement: consumer es obligatorio")
        if self.catalog is None:
            raise ValueError("FreshnessEnforcement: catalog es obligatorio (autoridad del mapping)")
        if not self.exchange or not self.exchange.strip():
            raise ValueError("FreshnessEnforcement: exchange no puede ser vacío")

    def stream_id_for(self, symbol: str) -> str:
        """
        ``stream_id`` canónico declarado para ``symbol`` en el venue configurado.

        Raises
        ------
        FreshnessUnknownError
            0 bindings (``unknown_stream_identity``) o >1 (``ambiguous_stream_identity``).
        """
        from shared.contracts.stream_identity import (
            AmbiguousStreamBindingError,
            StreamIdentityError,
        )

        try:
            identity = self.catalog.resolve(self.exchange, symbol)
        except AmbiguousStreamBindingError as exc:
            raise FreshnessUnknownError("ambiguous_stream_identity", detail=str(exc)) from exc
        except StreamIdentityError as exc:
            raise FreshnessUnknownError("unknown_stream_identity", detail=str(exc)) from exc
        return identity.stream_id

    def check(self, symbol: str) -> "FreshnessSnapshot":
        """
        Verifica freshness para ``symbol`` o eleva ``FreshnessBlockedError``.

        Returns
        -------
        FreshnessSnapshot del stream declarado, solo si está fresco y su
        identidad coincide con la solicitada.
        """
        stream_id = self.stream_id_for(symbol)
        try:
            snapshot = self.consumer.get_freshness(stream_id)
        except Exception as exc:  # noqa: BLE001 — fail-closed: cualquier fallo del proveedor bloquea
            raise FreshnessUnavailableError(
                "provider_unavailable",
                stream_id=stream_id,
                detail=f"{type(exc).__name__}: {exc}",
            ) from exc
        if snapshot.stream_id != stream_id:
            raise FreshnessIdentityMismatchError(
                "identity_mismatch",
                stream_id=stream_id,
                detail=f"snapshot.stream_id={snapshot.stream_id!r}",
            )
        if snapshot.is_stale:
            raise FreshnessStaleError(
                "stale",
                stream_id=stream_id,
                detail=(
                    f"lag_ms={snapshot.lag_ms} threshold_ms={snapshot.threshold_ms} "
                    f"reason={snapshot.staleness_reason!r}"
                ),
            )
        return snapshot


__all__ = [
    "FreshnessBlockedError",
    "FreshnessEnforcement",
    "FreshnessIdentityMismatchError",
    "FreshnessStaleError",
    "FreshnessUnavailableError",
    "FreshnessUnknownError",
]
