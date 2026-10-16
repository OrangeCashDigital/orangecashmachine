# -*- coding: utf-8 -*-
"""
market_data/domain/value_objects/listing_identity.py
======================================================

ListingId — identificador OPACO de una Listing Identity (G1/ADR-0049).

El VO ListingId es la unidad de referencia de la identidad; ListingIdentity
(la entidad negociable) vive en domain/entities/listing_identity.py para
respetar la dirección DDD (entities dependen de value_objects).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ListingId:
    """
    Identificador opaco de una Listing Identity.

    Abstracción opaca dentro del dominio (D19 BLOCKED): no se declara
    UUID, encoding, formato físico ni wire. Almacena una clave simbólica
    NO canónica (solo para igualdad en memoria); el esquema físico real
    lo decidirá D19 en un gate posterior.
    """

    _opaque_key: str

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ListingId):
            return NotImplemented
        return self._opaque_key == other._opaque_key

    def __hash__(self) -> int:
        return hash(self._opaque_key)

    def __str__(self) -> str:
        return f"ListingId({self._opaque_key!r})"

    def __repr__(self) -> str:
        return f"ListingId({self._opaque_key!r})"


__all__ = ["ListingId"]
