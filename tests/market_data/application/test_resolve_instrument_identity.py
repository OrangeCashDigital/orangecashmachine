# -*- coding: utf-8 -*-
"""
tests/market_data/application/test_resolve_instrument_identity.py
==================================================================

Tests del use case ResolveInstrumentIdentityUseCase (GATE B).

El use case une el port InstrumentIdentityRegistryPort con la política de
resolución FAIL-CLOSED. Usa un registry in-memory (doble de test) para no
depender de infraestructura (DIP) — la implementación real del port es un
gate posterior.

Invariantes de contrato:
- 0 candidatos activos   → UnknownIdentityError (FAIL-CLOSED, D10)
- 1 candidato activo     → devuelve la Listing Identity (RESOLVE)
- >1 candidatos activos  → AmbiguousResolutionError (FAIL-CLOSED, D10)
- determinista y venue-scoped (D03): sin equivalencia cross-venue
- D15 boundary: el use case NO introduce relación económica.
"""

from __future__ import annotations

from datetime import timedelta
from typing import List

import pytest
from market_data.application.use_cases.resolve_instrument_identity import (
    ResolveIdentityRequest,
    ResolveInstrumentIdentityUseCase,
)
from market_data.domain.exceptions import AmbiguousResolutionError, UnknownIdentityError
from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.symbol_binding import SymbolBinding
from market_data.ports.outbound.instrument_identity_registry import (
    InstrumentIdentityRegistryPort,
)

from tests.market_data.domain._identity_fixtures import make_btc_spot_identity
from tests.market_data.domain.test_symbol_binding import _binding


class TestPortConformance:
    def test_in_memory_registry_conforms_to_port(self):
        """El doble de test implementa el Protocol (BC-04/DIP)."""
        fx = make_btc_spot_identity()
        reg = _registry([], {fx.pid: fx.identity})
        assert isinstance(reg, InstrumentIdentityRegistryPort)


class _InMemoryRegistry:
    """Doble de test del port — solo lectura, sin implementación de infra.

    Recibe bindings y un mapa listing_id → identity. Reproduce los
    contracts del Protocol: devuelve bindings sin filtrar por tiempo;
    la decisión la toma la política (no el registry).
    """

    def __init__(self, bindings: List[SymbolBinding], identities) -> None:
        self._bindings = bindings
        self._identities = identities

    def find_bindings(self, native_symbol: str, context: str) -> List[SymbolBinding]:
        return [b for b in self._bindings if b.native_symbol == native_symbol and b.context == context]

    def get_identity(self, listing_id: ListingId):
        try:
            return self._identities[listing_id]
        except KeyError as exc:  # pragma: no cover - contrato de test
            raise UnknownIdentityError(f"identity {listing_id} not present in registry") from exc


def _registry(bindings, identities):
    return _InMemoryRegistry(bindings, identities)


class TestResolveUseCase:
    def test_zero_candidates_fail_closed(self):
        fx = make_btc_spot_identity()
        uc = ResolveInstrumentIdentityUseCase(_registry([], {fx.pid: fx.identity}))
        with pytest.raises(UnknownIdentityError):
            uc.execute(ResolveIdentityRequest(native_symbol="XXXUSD", context="bybit", at=fx.p0))

    def test_one_candidate_resolves(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        uc = ResolveInstrumentIdentityUseCase(_registry([b], {fx.pid: fx.identity}))
        resolved = uc.execute(
            ResolveIdentityRequest(native_symbol="BTCUSDT", context="bybit", at=fx.p0 + timedelta(minutes=1))
        )
        assert resolved.listing_id == fx.pid
        assert resolved.venue == "bybit"

    def test_multiple_active_candidates_ambiguity(self):
        fx = make_btc_spot_identity()
        other = make_btc_spot_identity(venue="kucoin")
        a = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        b = _binding("BTCUSDT", other.pid, fx.p0, fx.p1)
        uc = ResolveInstrumentIdentityUseCase(_registry([a, b], {fx.pid: fx.identity, other.pid: other.identity}))
        with pytest.raises(AmbiguousResolutionError):
            uc.execute(ResolveIdentityRequest(native_symbol="BTCUSDT", context="bybit", at=fx.p0))


class TestResolveVenueScoped:
    def test_same_symbol_diff_context_is_venue_scoped(self):
        # D03: la resolución depende del contexto (venue).
        bybit = make_btc_spot_identity(venue="bybit")
        kucoin = make_btc_spot_identity(venue="kucoin")
        a = _binding("BTCUSDT", bybit.pid, bybit.p0, bybit.p1, context="bybit")
        b = _binding("BTCUSDT", kucoin.pid, kucoin.p0, kucoin.p1, context="kucoin")

        reg = _registry([a, b], {bybit.pid: bybit.identity, kucoin.pid: kucoin.identity})
        uc = ResolveInstrumentIdentityUseCase(reg)

        resolved_bybit = uc.execute(ResolveIdentityRequest(native_symbol="BTCUSDT", context="bybit", at=bybit.p0))
        resolved_kucoin = uc.execute(ResolveIdentityRequest(native_symbol="BTCUSDT", context="kucoin", at=kucoin.p0))
        assert resolved_bybit.listing_id != resolved_kucoin.listing_id
