# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_provenance.py
=============================================

Tests de Provenance / ProvenanceSource — G1-D11 (ADR-0049).

Invariantes:
- taxonomía de orígenes cerrada y canonizable;
- todo hecho/transición/relación relevante conserva provenance (D11);
- UNKNOWN_CONFLICT existe y fuerza FAIL-CLOSED por diseño;
- provenance es inmutable y siempre con timestamp UTC.
"""

from __future__ import annotations

from datetime import timezone

import pytest
from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource


class TestProvenanceSourceTaxonomyD11:
    def test_closed_taxonomy(self):
        assert {s.value for s in ProvenanceSource} == {
            "observed_from_venue",
            "administrative_ocm",
            "derived",
            "unknown_conflict",
        }

    def test_str_compatibility(self):
        assert ProvenanceSource.ADMINISTRATIVE_OCM == "administrative_ocm"

    def test_unknown_conflict_is_explicit_source(self):
        # D11: el conflicto no resoluble es una categoría explícita.
        assert ProvenanceSource.UNKNOWN_CONFLICT is ProvenanceSource.UNKNOWN_CONFLICT


class TestProvenanceRecord:
    def test_provenance_requires_source(self):
        p = Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE, observed="snapshot")
        assert p.source is ProvenanceSource.OBSERVED_FROM_VENUE
        assert p.observed == "snapshot"

    def test_immutable_frozen(self):
        p = Provenance(source=ProvenanceSource.DERIVED, observed="computed")
        with pytest.raises((AttributeError, Exception)):
            p.source = ProvenanceSource.ADMINISTRATIVE_OCM  # frozen

    def test_recorded_at_utc(self):
        p = Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE)
        assert p.recorded_at.tzinfo is timezone.utc

    def test_conflict_provenance_can_be_carried_for_evidence(self):
        # El conflicto se conserva SIN resolverlo (evidencia, no resolución).
        p = Provenance(source=ProvenanceSource.UNKNOWN_CONFLICT, observed="bybit vs exchange-meta")
        assert p.source is ProvenanceSource.UNKNOWN_CONFLICT
