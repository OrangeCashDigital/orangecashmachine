# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_listing_identity.py
===================================================

Tests de la entidad ListingIdentity — G1/ADR-0049.

Invariantes de contrato cubiertos:
- D01 : Symbol ≠ Identity; la identidad es la entidad negociable.
- D03 : same_product / different venue ⇒ identidades DISTINTAS.
- D09 : lifecycle de la misma identidad NO crea identidades nuevas.
- D12 : DefinitionVersion (misma entidad) ≠ nueva identidad.
- D19 : ListingId es opaco; no expone formato físico canónico.
"""

from __future__ import annotations

import pytest
from market_data.domain.exceptions import InvalidLifecycleTransitionError
from market_data.domain.value_objects.lifecycle import LifecycleState
from market_data.domain.value_objects.listing_identity import ListingId
from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource

from tests.market_data.domain._identity_fixtures import make_btc_spot_identity


def _listing_id(key: str) -> ListingId:
    return ListingId(f"opaque-{key}")


class TestListingIdentityD03:
    def test_same_product_different_venue_distinct_identities(self):
        # D03: los dos listings del mismo producto en venues distintos son
        # identidades distintas — nunca se comparte identidad.
        bybit = make_btc_spot_identity(venue="bybit").identity
        kucoin = make_btc_spot_identity(venue="kucoin").identity

        assert bybit.listing_id != kucoin.listing_id
        assert bybit.venue == "bybit"
        assert kucoin.venue == "kucoin"
        assert bybit.terms == kucoin.terms  # mismo producto económico
        assert bybit is not kucoin

    def test_same_venue_same_terms_same_identity(self):
        # Dos refs a igual ListingId representan la misma identidad.
        a = make_btc_spot_identity(venue="bybit").identity
        b = make_btc_spot_identity(venue="bybit").identity
        assert a.listing_id == b.listing_id
        assert a.listing_id == a.listing_id


class TestListingIdentityD01:
    def test_symbol_is_not_identity(self):
        # D01: la identidad es la entidad; el símbolo es representación.
        # El modelo no tiene atributo "symbol" de identidad en la identidad.
        fx = make_btc_spot_identity(venue="bybit").identity
        assert "symbol" not in vars(fx)


class TestListingIdentityD09:
    def test_lifecycle_transition_preserves_identity(self):
        # D09: ACTIVE → EXPIRED NO crea identidad nueva.
        fx = make_btc_spot_identity(venue="bybit")
        started = fx.identity
        expired = started.transition(LifecycleState.EXPIRED)

        assert expired.listing_id == started.listing_id
        assert expired.venue == started.venue
        assert expired.instrument_type == started.instrument_type
        assert expired.terms == started.terms
        assert expired.lifecycle is LifecycleState.EXPIRED
        assert started.lifecycle is LifecycleState.ACTIVE

    def test_delisted_after_expired(self):
        fx = make_btc_spot_identity(venue="bybit")
        expired = fx.identity.transition(LifecycleState.EXPIRED)
        delisted = expired.transition(LifecycleState.DELISTED)
        assert delisted.listing_id == fx.identity.listing_id
        assert delisted.lifecycle is LifecycleState.DELISTED

    def test_invalid_transition_fails_closed(self):
        fx = make_btc_spot_identity(venue="bybit")
        with pytest.raises(InvalidLifecycleTransitionError):
            fx.identity.transition(LifecycleState.DELISTED)  # ACTIVE→DELISTED no es declarado

    def test_state_change_not_identity_change(self):
        # Un cambio de lifecycle no altera la identidad opaca.
        fx = make_btc_spot_identity(venue="bybit")
        assert fx.identity.transition(LifecycleState.SUSPENDED).listing_id == fx.identity.listing_id


class TestListingIdentityD12:
    def test_definition_version_preserves_identity(self):
        # D12: nueva DefinitionVersion sobre la MISMA identidad.
        fx = make_btc_spot_identity(venue="bybit")
        prov = Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE, observed="expiry-stability")
        v2 = fx.identity.new_definition_version(fx.identity.terms, prov)

        assert v2.listing_id == fx.identity.listing_id
        assert v2.definition_version == fx.identity.definition_version + 1
        assert v2.venue == fx.identity.venue

    def test_definition_version_conflict_provenance_rejected(self):
        # UNKNOWN_CONFLICT no puede generar una versión admisible (D11).
        fx = make_btc_spot_identity(venue="bybit")
        conflicted = Provenance(source=ProvenanceSource.UNKNOWN_CONFLICT, observed="split-brain")
        with pytest.raises(ValueError):
            fx.identity.new_definition_version(fx.identity.terms, conflicted)


class TestListingIdD19:
    def test_opaque_and_equal_by_value(self):
        # D19 (BLOCKED): el id es opaco en el dominio; igualdad por valor
        # en memoria, sin formato físico público declarado.
        a = _listing_id("bybit-BTCUSDT")
        b = _listing_id("bybit-BTCUSDT")
        assert a == b
        assert hash(a) == hash(b)


class TestListingIdentityOpenClosed:
    def test_identity_is_explicitly_venue_scoped(self):
        fx = make_btc_spot_identity(venue="bybit")
        assert fx.identity.venue == "bybit"
        fx2 = make_btc_spot_identity(venue="kucoin")
        assert fx2.identity.venue == "kucoin"
