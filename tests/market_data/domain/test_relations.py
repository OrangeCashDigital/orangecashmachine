# -*- coding: utf-8 -*-
"""
tests/market_data/domain/test_relations.py
============================================

Tests de relaciones identity-to-identity — G1-D13/D14 (ADR-0049).

Invariantes:
- SUCCESSOR_OF y REPLACED_BY son las ÚNICAS relaciones (D14);
- tipadas + direccionales (D13 regla 4);
- NO se infieren por símbolo/fecha/venue/lifecycle (D14 R2);
- SYMBOL_CHANGED_TO NO es identity-to-identity (vive en SymbolBinding);
- una relación siempre lleva provenance (D13 regla 5; D11);
- UNKNOWN_CONFLICT no puede generar una relación (D11).
"""

from __future__ import annotations

import pytest
from market_data.domain.value_objects.provenance import Provenance, ProvenanceSource
from market_data.domain.value_objects.relations import IdentityRelation, RelationType

from tests.market_data.domain._identity_fixtures import make_listing_id


class TestRelationCatalogD14:
    def test_only_successor_and_replaced_exist(self):
        # D14 R5: catálogo cerrado — solo estas dos relaciones.
        assert {r.value for r in RelationType} == {"successor_of", "replaced_by"}
        assert "SYMBOL_CHANGED_TO" not in [r.name for r in RelationType]

    def test_successor_of_and_replaced_by_are_distinct(self):
        # No son etiquetas equivalentes (D14).
        assert RelationType.SUCCESSOR_OF != RelationType.REPLACED_BY
        assert RelationType.SUCCESSOR_OF.value != RelationType.REPLACED_BY.value


class TestRelationDirectionalityD13:
    def test_relations_are_directional(self):
        # source → target; la relación es asimétrica.
        a = make_listing_id("succ-A")
        b = make_listing_id("succ-B")
        prov = Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE, observed="announcement")
        forward = IdentityRelation(source=a, target=b, relation_type=RelationType.SUCCESSOR_OF, provenance=prov)

        assert forward.source == a
        assert forward.target == b
        assert forward.relation_type is RelationType.SUCCESSOR_OF

    def test_self_relation_rejected(self):
        a = make_listing_id("self")
        prov = Provenance(source=ProvenanceSource.OBSERVED_FROM_VENUE, observed="x")
        with pytest.raises(ValueError):
            IdentityRelation(source=a, target=a, relation_type=RelationType.SUCCESSOR_OF, provenance=prov)


class TestNoHeuristicInferenceD14:
    def test_relation_classification_not_inferred_by_symbol_or_date(self):
        # D14 R2: la clasificación no puede inferirse silenciosamente.
        # El modelo exige que una relación sea declarada con su tipo
        # explícito — aquí no existe una función que "decida" por
        # símbolo/fecha/venue/lifecycle.
        module_has_no_inference_function = not hasattr(
            __import__("market_data.domain.value_objects.relations", fromlist=["infer"]), "infer"
        )
        assert module_has_no_inference_function


class TestProvenanceRequiredD11:
    def test_relation_requires_provenance(self):
        a = make_listing_id("p-A")
        b = make_listing_id("p-B")
        with pytest.raises(TypeError):
            IdentityRelation(a, b, RelationType.REPLACED_BY)  # falten provenance (kwarg obligatorio)

    def test_conflict_provenance_rejected(self):
        # UNKNOWN_CONFLICT no puede generar una relación (D11).
        a = make_listing_id("c-A")
        b = make_listing_id("c-B")
        conflicted = Provenance(source=ProvenanceSource.UNKNOWN_CONFLICT, observed="split")
        with pytest.raises(ValueError):
            IdentityRelation(
                source=a,
                target=b,
                relation_type=RelationType.REPLACED_BY,
                provenance=conflicted,
            )
