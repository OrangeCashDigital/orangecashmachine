# -*- coding: utf-8 -*-
"""
tests/market_data/domain/_identity_fixtures.py
================================================

Fixtures compartidos de identidad canónica (G1/ADR-0049) para los tests
de dominio. Reutilizar en todos los casos — nunca duplicar construcciones.

Nota: `_` de prefijo → NO se importa como suite de tests.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from market_data.domain.entities.listing_identity import ListingIdentity
from market_data.domain.value_objects.contract_terms import ContractTerms
from market_data.domain.value_objects.instrument_type import InstrumentType
from market_data.domain.value_objects.lifecycle import LifecycleState
from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource


def utc(dt: datetime) -> datetime:
    """Normaliza un datetime a UTC (los tests usan timezone-aware)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def t0() -> datetime:
    return utc(datetime(2026, 1, 1, 12, 0, 0))


@dataclass(frozen=True)
class IdentityFixture:
    """Fixture canónico: BTC spot en bybit con su binding inicial."""

    identity: ListingIdentity
    pid: ListingId
    p0: datetime
    p1: datetime
    venue_provenance: Provenance

    @property
    def bybit_btc_spot(self) -> ContractTerms:
        return ContractTerms(instrument_type=InstrumentType.SPOT, underlying="BTC", quote="USDT")


def make_btc_spot_identity(provenance: Provenance | None = None, venue: str = "bybit") -> IdentityFixture:
    """ListingIdentity SPOT BTC/USDT en un venue + bindings temporales."""
    pid = ListingId(f"opaque-btc-spot-{venue}")
    p0 = t0()
    p1 = p0 + timedelta(days=365)
    prov = provenance or Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE, observed="fixture")
    identity = ListingIdentity(
        listing_id=pid,
        venue=venue,
        instrument_type=InstrumentType.SPOT,
        terms=ContractTerms(instrument_type=InstrumentType.SPOT, underlying="BTC", quote="USDT"),
        lifecycle=LifecycleState.ACTIVE,
    )
    return IdentityFixture(identity=identity, pid=pid, p0=p0, p1=p1, venue_provenance=prov)


def make_listing_id(key: str) -> ListingId:
    return ListingId(key)
