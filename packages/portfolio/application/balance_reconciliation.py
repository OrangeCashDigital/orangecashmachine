# -*- coding: utf-8 -*-
"""portfolio/application/balance_reconciliation.py

Reconciliación de balance gateada por freshness (B-MD-009 / issue #223).

Respeta BC-13: portfolio NO importa trading. El gate se hace directamente
sobre FreshnessPort (shared/contracts/boundaries.py), no sobre
FreshnessEnforcement de trading.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from shared.contracts.boundaries import BalancePort, FreshnessPort


@dataclass(frozen=True, slots=True)
class ReconciliationAlert:
    reason: str
    symbol: str
    detail: str
    detected_at: datetime


@dataclass(frozen=True, slots=True)
class BalanceReconciliation:
    """Reconciliación periódica on-chain + exchange gateada por freshness."""

    freshness_port: "FreshnessPort"
    balance_port: "BalancePort"
    stream_id_for_symbol: dict[str, str]
    divergence_threshold_usd: float = 1.0

    def __post_init__(self) -> None:
        if self.freshness_port is None:
            raise ValueError("BalanceReconciliation: freshness_port es obligatorio")
        if self.balance_port is None:
            raise ValueError("BalanceReconciliation: balance_port es obligatorio")
        if self.divergence_threshold_usd < 0:
            raise ValueError("BalanceReconciliation: divergence_threshold_usd >= 0")

    def reconcile(self, symbol: str, internal_balance_usd: float) -> Optional[ReconciliationAlert]:
        stream_id = self.stream_id_for_symbol.get(symbol)
        if stream_id is None:
            return ReconciliationAlert(
                reason="unknown_stream_identity",
                symbol=symbol,
                detail=f"symbol={symbol!r} no tiene stream_id declarado",
                detected_at=datetime.now(timezone.utc),
            )

        # Gate 1: freshness (fail-closed)
        try:
            snapshot = self.freshness_port.get_freshness(stream_id)
        except Exception as exc:  # noqa: BLE001
            return ReconciliationAlert(
                reason="freshness_unavailable",
                symbol=symbol,
                detail=f"{type(exc).__name__}: {exc}",
                detected_at=datetime.now(timezone.utc),
            )

        if snapshot.stream_id != stream_id:
            return ReconciliationAlert(
                reason="identity_mismatch",
                symbol=symbol,
                detail=f"snapshot.stream_id={snapshot.stream_id!r}",
                detected_at=datetime.now(timezone.utc),
            )
        if snapshot.is_stale:
            return ReconciliationAlert(
                reason="freshness_stale",
                symbol=symbol,
                detail=f"lag_ms={snapshot.lag_ms} threshold_ms={snapshot.threshold_ms}",
                detected_at=datetime.now(timezone.utc),
            )

        # Gate 2: proveedor de balance
        try:
            balance_snapshot = self.balance_port.get_balance("USDT")
        except Exception as exc:  # noqa: BLE001
            return ReconciliationAlert(
                reason="balance_provider_error",
                symbol=symbol,
                detail=f"{type(exc).__name__}: {exc}",
                detected_at=datetime.now(timezone.utc),
            )

        external = float(getattr(balance_snapshot, "total", getattr(balance_snapshot, "amount", 0.0)))
        divergence = abs(external - internal_balance_usd)
        if divergence > self.divergence_threshold_usd:
            return ReconciliationAlert(
                reason="balance_divergence",
                symbol=symbol,
                detail=(
                    f"external={external} internal={internal_balance_usd} "
                    f"divergence={divergence} threshold={self.divergence_threshold_usd}"
                ),
                detected_at=datetime.now(timezone.utc),
            )
        return None


__all__ = ["BalanceReconciliation", "ReconciliationAlert"]
