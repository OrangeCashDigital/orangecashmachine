# -*- coding: utf-8 -*-
"""
shared/contracts/stream_identity.py
===================================

Identidad OPERACIONAL de stream de datos de mercado + catálogo de binding.

Alcance (D-1, opción (a) aprobada por el owner)
----------------------------------------------
· Binding mínimo y único: ``(exchange, symbol) → stream_id``.
· Formato canónico: ``ws:{exchange}:{symbol}`` (ej. ``ws:bybit:BTC/USDT``).
· El catálogo es la AUTORIDAD del mapping: ningún consumidor construye
  ``stream_id`` por su cuenta (prohibido duplicar la identidad en OMS,
  Composition Root o adaptadores).

Límites explícitos
------------------
· NO modela identidad de instrumento (G1/ADR-0049). ``ListingId`` es opaco
  (D19 BLOCKED) y aquí no se serializa ni se interpreta: la identidad del
  instrumento queda desacoplada de la identidad operacional del stream.
· NO modela equivalencia cross-venue (D03): el binding es venue-scoped.
· El catálogo G1 completo queda fuera de alcance (decisión posterior).

Fail-closed (regla del contrato)
--------------------------------
· Binding desconocido      → ``UnknownStreamBindingError``
· Binding duplicado/ambiguo → ``AmbiguousStreamBindingError``
· ``stream_id`` mal formado → ``InvalidStreamIdError``
NUNCA se resuelve por heurística ni se degrada a un valor por defecto.

Por qué vive en ``shared``
--------------------------
``shared`` es el único punto del que pueden depender market_data (productor
de freshness) y trading (consumidor) sin violar BC-50 (trading no importa
market_data) ni duplicar la gramática en dos BCs. La forma wire del id ya es
un contrato compartido: ``FreshnessSnapshot.stream_id``
(``shared/contracts/boundaries.py``).

Principios: SSOT · DIP · FAIL-CLOSED · KISS · BC-01 (stdlib only) · D19 · D03
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

#: Prefijo canónico del transporte para los streams operacionales (D-1).
STREAM_ID_PREFIX: str = "ws"

#: Separador de la gramática ``{prefijo}:{exchange}:{symbol}``.
STREAM_ID_SEPARATOR: str = ":"


# ---------------------------------------------------------------------------
# Errores
# ---------------------------------------------------------------------------


class StreamIdentityError(Exception):
    """Base de los fallos de identidad operacional de stream (FAIL-CLOSED)."""


class InvalidStreamIdError(StreamIdentityError):
    """El ``stream_id`` no cumple la gramática canónica o no hace round-trip."""


class UnknownStreamBindingError(StreamIdentityError):
    """No existe binding declarado para ``(exchange, symbol)`` (FAIL-CLOSED)."""


class AmbiguousStreamBindingError(StreamIdentityError):
    """Hay más de un binding para la misma clave — la ambigüedad es un fallo."""


# ---------------------------------------------------------------------------
# StreamProvider — vista estructural del productor de un stream
# ---------------------------------------------------------------------------


@runtime_checkable
class StreamProvider(Protocol):
    """
    Vista estructural mínima de un productor de stream de mercado.

    La satisface cualquier implementación de ``TradesSourceProtocol``
    (``source_id`` + ``is_running``) sin que trading importe market_data.

    Uso: el startup gate de LIVE exige un provider declarado y comprueba que
    su ``source_id`` pertenece al catálogo (identidad válida ⇒ si no, bloqueo).
    """

    @property
    def source_id(self) -> str:
        """Identificador operacional del stream (gramática ``ws:{exchange}:{symbol}``)."""
        ...

    @property
    def is_running(self) -> bool:
        """True si el provider está produciendo datos."""
        ...


# ---------------------------------------------------------------------------
# StreamIdentity
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StreamIdentity:
    """
    Identidad operacional de un stream (venue-scoped, sin identidad de instrumento).

    Atributos
    ---------
    exchange : venue del stream (se normaliza a minúsculas; sin separador ``:``).
    symbol   : símbolo canónico del adapter (p. ej. ``"BTC/USDT"``). Puede
               contener ``:`` (p. ej. ``"ETH/USDT:USDT"``) — la gramática usa
               ``split`` con ``maxsplit=2`` para preservar el round-trip.

    Invariantes (fail-fast en construcción)
    ---------------------------------------
    · ``exchange`` y ``symbol`` no vacíos tras ``strip()``.
    · ``exchange`` no contiene ``:`` (rompería la gramática y el round-trip).
    · ``parse(render(x)) == x`` para toda instancia válida.
    """

    exchange: str
    symbol: str

    def __post_init__(self) -> None:
        exchange = self.exchange.strip().lower()
        symbol = self.symbol.strip()
        if not exchange:
            raise ValueError("StreamIdentity.exchange must be non-empty")
        if not symbol:
            raise ValueError("StreamIdentity.symbol must be non-empty")
        if STREAM_ID_SEPARATOR in exchange:
            raise ValueError(f"StreamIdentity.exchange must not contain {STREAM_ID_SEPARATOR!r}, got {exchange!r}")
        object.__setattr__(self, "exchange", exchange)
        object.__setattr__(self, "symbol", symbol)

    @property
    def stream_id(self) -> str:
        """Render canónico: ``ws:{exchange}:{symbol}`` — ÚNICA vía de construcción."""
        return f"{STREAM_ID_PREFIX}{STREAM_ID_SEPARATOR}{self.exchange}{STREAM_ID_SEPARATOR}{self.symbol}"

    @classmethod
    def parse(cls, stream_id: str) -> "StreamIdentity":
        """
        Inverso exacto de ``stream_id`` (FAIL-CLOSED).

        Raises
        ------
        InvalidStreamIdError
            Prefijo distinto de ``ws``, número de segmentos incorrecto,
            componentes vacíos o round-trip no idéntico.
        """
        if not isinstance(stream_id, str):
            raise InvalidStreamIdError(f"stream_id must be str, got {type(stream_id).__name__}")
        parts = stream_id.strip().split(STREAM_ID_SEPARATOR, 2)
        if len(parts) != 3:
            raise InvalidStreamIdError(
                f"stream_id {stream_id!r} does not match '{STREAM_ID_PREFIX}:{{exchange}}:{{symbol}}'"
            )
        prefix, exchange, symbol = parts
        if prefix != STREAM_ID_PREFIX:
            raise InvalidStreamIdError(f"stream_id {stream_id!r} has unsupported prefix {prefix!r}")
        # Un symbol sólo admite 1 ':' como separador base:settle (ej. ETH/USDT:USDT).
        # Cualquier ':' extra indica gramática inválida — fail-closed.
        if symbol.count(STREAM_ID_SEPARATOR) > 1:
            raise InvalidStreamIdError(
                f"stream_id {stream_id!r} has extra {STREAM_ID_SEPARATOR!r} "
                f"in symbol segment {symbol!r}"
            )
        identity = cls(exchange=exchange, symbol=symbol)
        if identity.stream_id != stream_id.strip():
            raise InvalidStreamIdError(f"stream_id {stream_id!r} does not round-trip (got {identity.stream_id!r})")
        return identity


# ---------------------------------------------------------------------------
# StreamIdentityCatalog
# ---------------------------------------------------------------------------


class StreamIdentityCatalog:
    """
    Catálogo inmutable de bindings ``(exchange, symbol) → StreamIdentity``.

    Es la AUTORIDAD del mapping (D-1). Se construye una vez, en composición,
    y no admite mutación posterior: añadir un binding es recompilar el
    catálogo, no mutar estado en runtime (KISS + fail-closed).

    Invariantes
    -----------
    · Sin duplicados: dos entradas con la misma clave ⇒ ``AmbiguousStreamBindingError``.
    · ``resolve`` de una clave ausente ⇒ ``UnknownStreamBindingError`` (nunca None).
    · ``len(catalog) == 0`` es un catálogo válido pero vacío: el startup gate
      de LIVE lo rechaza (no hay stream que vigilar).
    """

    def __init__(self, bindings: Iterable[tuple[str, str]]) -> None:
        identities: list[StreamIdentity] = []
        by_key: dict[tuple[str, str], StreamIdentity] = {}
        by_stream_id: dict[str, StreamIdentity] = {}
        for exchange, symbol in bindings:
            identity = StreamIdentity(exchange=exchange, symbol=symbol)
            key = (identity.exchange, identity.symbol)
            if key in by_key:
                raise AmbiguousStreamBindingError(
                    f"duplicate stream binding for exchange={identity.exchange!r} symbol={identity.symbol!r}"
                )
            if identity.stream_id in by_stream_id:
                raise AmbiguousStreamBindingError(f"duplicate stream_id {identity.stream_id!r}")
            by_key[key] = identity
            by_stream_id[identity.stream_id] = identity
            identities.append(identity)
        self._identities: tuple[StreamIdentity, ...] = tuple(identities)
        self._by_key = by_key
        self._by_stream_id = by_stream_id

    def __len__(self) -> int:
        return len(self._identities)

    def __contains__(self, key: object) -> bool:
        if isinstance(key, StreamIdentity):
            return (key.exchange, key.symbol) in self._by_key
        if isinstance(key, str):
            return key in self._by_stream_id
        return False

    def resolve(self, exchange: str, symbol: str) -> StreamIdentity:
        """
        Resuelve el binding declarado para ``(exchange, symbol)``.

        Raises
        ------
        UnknownStreamBindingError
            Si la clave no está declarada (FAIL-CLOSED — nunca se inventa).
        """
        identity = StreamIdentity(exchange=exchange, symbol=symbol)
        resolved = self._by_key.get((identity.exchange, identity.symbol))
        if resolved is None:
            raise UnknownStreamBindingError(
                f"no stream binding for exchange={identity.exchange!r} symbol={identity.symbol!r}"
            )
        return resolved

    def contains_stream_id(self, stream_id: str) -> bool:
        """True si el ``stream_id`` está declarado; False si no lo está o es inválido."""
        try:
            identity = StreamIdentity.parse(stream_id)
        except StreamIdentityError:
            return False
        return identity.stream_id in self._by_stream_id

    def stream_ids(self) -> tuple[str, ...]:
        """Todos los ``stream_id`` declarados, en el orden de construcción."""
        return tuple(identity.stream_id for identity in self._identities)


__all__ = [
    "STREAM_ID_PREFIX",
    "STREAM_ID_SEPARATOR",
    "StreamIdentity",
    "StreamIdentityCatalog",
    "StreamIdentityError",
    "StreamProvider",
    "InvalidStreamIdError",
    "UnknownStreamBindingError",
    "AmbiguousStreamBindingError",
]
