# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_symbol_binding.py
=================================================

Tests de SymbolBinding — G1-D10 (ADR-0049).

Invariantes:
- binding asociación contextual-temporal native_symbol ↔ ListingId;
- periodo semi-abierto (valid_from ≤ t < valid_to);
- un cambio de símbolo NO crea una identidad nueva (misma ListingId);
- bindings coexistiendo por contexto/tiempo son legítimos;
- venue-scoped (D03): sin equivalencia cross-venue.
"""

from __future__ import annotations

from datetime import timedelta

from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource
from market_data.domain.value_objects.symbol_binding import SymbolBinding

from tests.market_data.domain._identity_fixtures import make_btc_spot_identity


def _prov() -> Provenance:
    return Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE, observed="test")


def _binding(native_symbol: str, pid, p_start, p_end, context: str = "bybit") -> SymbolBinding:
    return SymbolBinding(
        native_symbol=native_symbol,
        context=context,
        listing_id=pid,
        valid_from=p_start,
        valid_to=p_end,
        provenance=_prov(),
    )


class TestBindingTemporalRangeD10:
    def test_valid_from_inclusive_valid_to_exclusive(self):
        fx = make_btc_spot_identity()
        p0 = fx.p0
        p1 = fx.p1
        b = _binding("BTCUSDT", fx.pid, p0, p1)

        assert b.covers(p0) is True  # inclusive en valid_from
        assert b.covers(p0 + timedelta(minutes=1)) is True
        assert b.covers(p1 - timedelta(minutes=1)) is True
        assert b.covers(p1) is False  # exclusivo en valid_to

    def test_open_ended_binding_covers_infinity(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, None)
        assert b.covers(fx.p0 + timedelta(days=10**6)) is True

    def test_before_valid_from_not_covered(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        assert b.covers(fx.p0 - timedelta(seconds=1)) is False


class TestSymbolChangePreservesIdentityD10:
    def test_symbol_change_new_binding_same_identity(self):
        # Un cambio de símbolo NO modifica la identidad: nuevo binding,
        # mismo listing_id (G1-D10/D01).
        fx = make_btc_spot_identity()
        old_binding = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        new_binding = _binding("BTCUSDT2", fx.pid, fx.p1, None)  # símbolo renombrado

        assert old_binding.listing_id == fx.pid
        assert new_binding.listing_id == fx.pid
        assert old_binding.native_symbol != new_binding.native_symbol
        assert old_binding.context == new_binding.context


class TestBindingContextCoexistenceD10:
    def test_bindings_coexist_across_contexts(self):
        # One-to-many entre contextos es legítimo (payload/producto del
        # mismo listing en un contexto de reporte distinto).
        fx = make_btc_spot_identity()
        primary = _binding("BTCUSDT", fx.pid, fx.p0, None)
        report = SymbolBinding(
            native_symbol="bybit:BTCUSDT",
            context="reporter",
            listing_id=fx.pid,
            valid_from=fx.p0,
            valid_to=None,
            provenance=_prov(),
        )
        assert primary.listing_id == report.listing_id
        assert primary.context != report.context

    def test_binding_is_venue_scoped(self):
        # D03: el binding referencia la identidad venue-scoped; no crea
        # equivalencia cross-venue.
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, None)
        assert b.context == "bybit"
