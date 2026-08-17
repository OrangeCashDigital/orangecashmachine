"""architecture_linter.models — tipos del linter de gobernanza arquitectónica OCM.

Modelos de datos compartidos por engine, reglas y reporters.
Sin imports internos de OCM (herramienta independiente, stdlib-only).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class Severity(str, Enum):
    """Severidad de un hallazgo (configurable por regla)."""

    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class Status(str, Enum):
    """Estado del resultado de una regla.

    PASS   — el invariante se cumple (evidencia positiva presente).
    FAIL   — el invariante se viola (evidencia primaria).
    UNKNOWN— no se puede determinar estáticamente; razón concreta.
    PARTIAL— semántica formal: algunos niveles pasan y otros fallan (documentado por regla).
    """

    PASS = "PASS"  # nosec B105 — literal de estado del enum, no una contraseña
    FAIL = "FAIL"
    UNKNOWN = "UNKNOWN"
    PARTIAL = "PARTIAL"


@dataclass(frozen=True)
class Evidence:
    """Una pieza de evidencia primaria: ruta real + línea + símbolo + texto."""

    file: str
    line: Optional[int]
    symbol: Optional[str]
    text: str

    def __str__(self) -> str:
        loc = f"{self.file}:{self.line}" if self.line is not None else self.file
        return f"{loc} [{self.symbol or '-'}] {self.text}"


@dataclass
class Finding:
    """Hallazgo individual emitido por una regla."""

    rule_id: str
    severity: Severity
    status: Status
    message: str
    file: Optional[str] = None
    line: Optional[int] = None
    symbol: Optional[str] = None
    evidence: list[Evidence] = field(default_factory=list)
    related_files: list[str] = field(default_factory=list)
    related_symbols: list[str] = field(default_factory=list)
    confidence: float = 1.0
    concept: Optional[str] = None
    owner: Optional[str] = None
    consumer: Optional[str] = None
    producer: Optional[str] = None


@dataclass
class RuleResult:
    """Resultado agregado de una regla."""

    rule_id: str
    rule_name: str
    status: Status
    findings: list[Finding] = field(default_factory=list)
    summary: str = ""

    @property
    def failed(self) -> bool:
        return self.status == Status.FAIL or self.status == Status.PARTIAL


@dataclass
class PositionStore:
    """Almacén mutable de posición detectado (concepto 'position')."""

    owner_class: str
    attr: str
    file: str
    line: int
    value_type: str
    mutations: list[Evidence] = field(default_factory=list)
    reads: list[Evidence] = field(default_factory=list)
    is_ssot: bool = False
    semantics: list[str] = field(default_factory=list)


@dataclass
class OrderStore:
    """Almacén mutable de órdenes detectado (concepto 'order')."""

    owner_class: str
    attr: str
    file: str
    line: int
    mutations: list[Evidence] = field(default_factory=list)
