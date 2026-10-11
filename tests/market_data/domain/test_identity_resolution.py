# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_identity_resolution.py
======================================================

Tests de la política de resolución FAIL-CLOSED — G1-D10 (ADR-0049).

Invariantes:
- 0 candidates activos → UnknownIdentityError (FAIL-CLOSED);
- 1 candidato activo  → RESOLVE;
- >1 candidatos activos → AmbiguousResolutionError (FAIL-CLOSED);
- periodo semi-abierto (valid_from inclusive, valid_to exclusive);
- nunca se selecciona un candidato silenciosamente.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from market_data.domain.exceptions import AmbiguousResolutionError, UnknownIdentityError
from market_data.domain.policies.identity_resolution import resolve_unique, select_active_bindings

from tests.market_data.domain._identity_fixtures import make_btc_spot_identity
from tests.market_data.domain.test_symbol_binding import _binding


class TestResolutionFailClosedD10:
    def test_zero_candidates_fail_closed(self):
        fx = make_btc_spot_identity()
        # Binding cuyo periodo NO cubre fx.p0 (empieza después) ⇒ 0 activos.
        b = _binding("BTCUSDT", fx.pid, fx.p1 + timedelta(days=1), fx.p1 + timedelta(days=2))
        with pytest.raises(UnknownIdentityError):
            resolve_unique([b], fx.p0)

    def test_one_active_candidate_resolves(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        assert resolve_unique([b], fx.p0 + timedelta(minutes=1)).listing_id == fx.pid

    def test_multiple_active_candidates_ambiguity(self):
        fx = make_btc_spot_identity()
        a = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        b = _binding("BTCUSDT", make_btc_spot_identity(venue="kucoin").pid, fx.p0, fx.p1)
        with pytest.raises(AmbiguousResolutionError):
            resolve_unique([a, b], fx.p0)


class TestTemporalBoundaryPixel:
    def test_valid_from_inclusive(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        active = select_active_bindings([b], fx.p0)
        assert len(active) == 1

    def test_valid_to_exclusive(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        active_at_end = select_active_bindings([b], fx.p1)
        assert active_at_end == []

    def test_deterministic_for_same_input(self):
        fx = make_btc_spot_identity()
        b = _binding("BTCUSDT", fx.pid, fx.p0, fx.p1)
        t = fx.p0 + timedelta(days=30)
        assert resolve_unique([b], t).listing_id == resolve_unique([b], t).listing_id
