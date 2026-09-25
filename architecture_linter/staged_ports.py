"""architecture_linter.staged_ports — núcleo semántico de Staged Ports Gobernados.

Los registros declarativos de staged ports viven en `policies/registry.yaml`
(SSOT, ADR-0044 §3.6). Este módulo es el núcleo común — stdlib-only y fail-closed —
de gramática y semántica: ARCH-006 (B2) lo consultará para decidir si un port
huérfano está legítimamente staged, y M26 (B3, `scripts/audit_validator.py`)
reutilizará la misma semántica para validar el registro completo.

PRINCIPIO FAIL-CLOSED
    Cualquier condición que impida demostrar que un Staged Port es válido produce
    FAIL. Un error de parsing jamás devuelve "registry vacío", "PASS" ni "skip".

GRAMÁTICA YAML CONSTREÑIDA (subconjunto mínimo soportado)
    Solo se parsea la clave de nivel superior `staged_ports:` tal y como existe en
    el registry actual:

        staged_ports: []            # registro vacío (válido)
        staged_ports:
          - port: MiPort
            file: packages/market_data/ports/outbound/mi_port.py
            adr: ADR-0028
            adr_section: "§3.6"
            current_phase: F1
            target_phase: F2
            consumer: B-MD-004
            tracking_item: B-MD-004
            owner: market_data
            expires: "2026-12-31"
            justification: "texto en una sola línea"
            revocation: "texto en una sola línea"
        rules:                      # el resto del YAML se ignora

    Reglas de gramática:
    - Los valores son escalares de una sola línea (`clave: valor`), con comillas
      simples o dobles balanceadas opcionales. Colores internos al valor se toleran
      (se corta por el primer `:`).
    - El primer campo de cada entrada vive en la misma línea que el item `- `.
    - Los campos permitidos son EXACTAMENTE los de ADR-0044 §3.1 (12 campos); un
      campo desconocido se rechaza (FAIL) para impedir bypass por campos extra.
    - `staged_ports` debe aparecer una sola vez y en una línea de nivel superior;
      si falta o se repite -> FAIL.
    - NO se soportan: bloques multilínea, flujo `{...}` / `[...]`, anclas, aliases,
      comentarios dentro del valor ni la gramática YAML general.

RESPONSABILIDADES DELEGADAS AL ENFORCEMENT (fuera de B1)
    - Resolución de identidad (`port` -> clase `Protocol` real bajo un directorio de
      ports) exige el recorrido AST de ARCH-006; NO se duplica aquí. Queda en
      M26/B3.
    - Caso I (entrada cuyo port ya tiene consumidor real según el índice AST) se
      evalúa contra el repositorio, no contra la declaración; queda en M26/B3.
    - En B1 el único chequeo contra disco es la existencia del archivo cuando se
      provee `root`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

REQUIRED_FIELDS: tuple[str, ...] = (
    "port",
    "file",
    "adr",
    "adr_section",
    "current_phase",
    "target_phase",
    "consumer",
    "tracking_item",
    "owner",
    "expires",
    "justification",
    "revocation",
)
REQUIRED_FIELD_SET: frozenset[str] = frozenset(REQUIRED_FIELDS)

_STAGED_KEY = "staged_ports"


@dataclass(frozen=True)
class ParseResult:
    """Resultado de cargar/parsear la sección `staged_ports`."""

    ok: bool
    errors: tuple[str, ...] = field(default_factory=tuple)
    entries: tuple[dict[str, str], ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class RegistryReport:
    """Veredicto fail-closed de validación del registro (o de un error de parse)."""

    ok: bool
    errors: tuple[str, ...] = field(default_factory=tuple)


def load_staged_registry(path: Path | None) -> ParseResult:
    """Carga `policies/registry.yaml` y parsea su subconjunto staged.

    `path=None` o archivo inexistente/ilegible -> FAIL (fail-closed), jamás vacío.
    """
    if path is None:
        return ParseResult(ok=False, errors=("policies/registry.yaml no provisto",))
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return ParseResult(ok=False, errors=(f"registry inexistente o ilegible: {path}",))
    return parse_staged_registry(text)


def parse_staged_registry(text: str) -> ParseResult:
    """Parse del subconjunto YAML constreñido de `staged_ports`.

    Cualquier desviación de la gramática soportada -> FAIL (fail-closed).
    `staged_ports: []` -> registro vacío válido.
    """
    lines = text.splitlines()
    positions: list[int] = []
    for i, raw in enumerate(lines):
        if raw.startswith((" ", "\t")):
            continue
        if raw.startswith("#"):
            continue
        if raw.strip().startswith(_STAGED_KEY):
            positions.append(i)
    if not positions:
        return ParseResult(ok=False, errors=("registry sin clave 'staged_ports' (SSOT ADR-0044 §3.6)",))
    if len(positions) > 1:
        return ParseResult(ok=False, errors=("clave 'staged_ports' duplicada a nivel superior",))
    header = lines[positions[0]].strip()
    if header == f"{_STAGED_KEY}: []":
        return ParseResult(ok=True)
    if header != f"{_STAGED_KEY}:":
        return ParseResult(ok=False, errors=(f"'staged_ports': tipo no soportado ({header!r})",))
    entries, errors = _parse_items(lines, positions[0] + 1)
    if errors:
        return ParseResult(ok=False, errors=tuple(errors))
    return ParseResult(ok=True, entries=tuple(entries))


def validate_entry(entry: dict[str, str], *, today: date, root: Path | None = None) -> list[str]:
    """Valida la semántica de UNA entrada staged. Devuelve lista de errores (vacía = válida).

    `root` opcional: si se provee, verifica la existencia del archivo referenciado.
    No resuelve identidad AST (delegado a M26/B3), ni detecta consumidores (Caso I).
    """
    errors: list[str] = []
    for field_name in REQUIRED_FIELDS:
        if field_name not in entry:
            errors.append(f"campo obligatorio ausente: {field_name!r}")
        elif entry[field_name] == "":
            errors.append(f"campo obligatorio vacío: {field_name!r}")

    expires_raw = entry.get("expires")
    if expires_raw:
        errors.extend(_expiry_errors(expires_raw, today))

    if root is not None:
        file_path = entry.get("file")
        if file_path and not (root / file_path).exists():
            errors.append(f"archivo referenciado no existe: {file_path}")
    return errors


def validate_registry(parse: ParseResult, *, today: date, root: Path | None = None) -> RegistryReport:
    """Valida el registro staged completo (fail-closed).

    Si el parse falló, el veredicto es FAIL con los errores de parse. Si hay
    entradas, valida forma/fechas, detecta entradas duplicadas por `port` y, si se
    provee `root`, la existencia de cada archivo referenciado.
    """
    if not parse.ok:
        return RegistryReport(ok=False, errors=parse.errors)
    errors: list[str] = []
    seen: set[str] = set()
    for entry in parse.entries:
        port = entry.get("port")
        label = port or "<sin port>"
        if port:
            if port in seen:
                errors.append(f"Port duplicado: {port!r}")
            seen.add(port)
        for msg in validate_entry(entry, today=today, root=root):
            errors.append(f"{label}: {msg}")
    if errors:
        return RegistryReport(ok=False, errors=tuple(errors))
    return RegistryReport(ok=True)


def get_staged_entry(parse: ParseResult | None, *, port: str) -> dict[str, str] | None:
    """Busca la entrada staged por nombre de port (para ARCH-006/B2).

    Desacoplada del análisis de consumidores: devuelve la entrada declarada o None.
    La validez de la cobertura se decide con `validate_entry`.
    """
    if parse is None:
        return None
    for entry in parse.entries:
        if entry.get("port") == port:
            return entry
    return None


def _expiry_errors(expires_raw: str, today: date) -> list[str]:
    """Errores de expiración: ISO (extendida) inválida, pasada o = evaluación."""
    _ISO_EXTENDED = re.compile(r"^\d{4}-\d{2}-\d{2}$")
    if not _ISO_EXTENDED.fullmatch(expires_raw):
        return [f"expires {expires_raw!r} no es fecha ISO-8601 válida (formato YYYY-MM-DD)"]
    try:
        expires = date.fromisoformat(expires_raw)
    except ValueError:
        return [f"expires {expires_raw!r} no es fecha ISO-8601 válida (formato YYYY-MM-DD)"]
    if expires < today:
        return [f"expires {expires_raw} expirada (evaluación {today.isoformat()})"]
    if expires == today:
        return [
            f"expires {expires_raw} coincide con la evaluación {today.isoformat()}:"
            " un staged exige fecha futura (renovación requerida)"
        ]
    return []


def _parse_items(lines: list[str], start: int) -> tuple[list[dict[str, str]], list[str]]:
    """Parse de los items `- key: value` bajo `staged_ports:`.

    Termina al encontrar una línea de nivel superior con `:` (p. ej. `rules:`).
    Devuelve (entries, errores): si hay errores, la gramática falló (fail-closed).
    """
    entries: list[dict[str, str]] = []
    current: dict[str, str] | None = None
    for lineno, raw in enumerate(lines[start:], start=start + 1):
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not raw[0].isspace():
            if stripped.startswith(("-", _STAGED_KEY)):
                return entries, [f"línea {lineno}: estructura no soportada tras 'staged_ports:': {stripped!r}"]
            if ":" in stripped:
                return entries, []
            return entries, [f"línea {lineno}: línea de nivel superior inesperada: {stripped!r}"]
        if stripped.startswith("- "):
            current, err = _add_item(entries, stripped)
        else:
            err = _extend_entry(current, stripped)
        if err:
            return entries, [f"línea {lineno}: {err}"]
    return entries, []


def _add_item(entries: list[dict[str, str]], line: str) -> tuple[dict[str, str] | None, str | None]:
    """Añade un item `- clave: valor` (primer campo de una entrada)."""
    if line == "-":
        return None, "'-' sin 'clave: valor' no soportado"
    body = line[2:].strip()
    if not body:
        return None, "item vacío tras '-'"
    key, value, err = _split_field(body)
    if err:
        return None, err
    if key not in REQUIRED_FIELD_SET:
        return None, f"campo no permitido: {key!r}"
    entry: dict[str, str] = {key: value}
    entries.append(entry)
    return entry, None


def _extend_entry(current: dict[str, str] | None, line: str) -> str | None:
    """Añade un campo `clave: valor` a la entrada vigente."""
    if current is None:
        return f"campo sin entrada activa: {line!r}"
    key, value, err = _split_field(line)
    if err:
        return err
    if key not in REQUIRED_FIELD_SET:
        return f"campo no permitido: {key!r}"
    if key in current:
        return f"campo duplicado en entrada: {key!r}"
    current[key] = value
    return None


def _split_field(line: str) -> tuple[str, str, str | None]:
    """Corta `clave: valor` de una línea escalar (por el primer `:`)."""
    if ":" not in line:
        return "", "", f"campo sin 'clave: valor': {line!r}"
    key, _, raw_value = line.partition(":")
    key = key.strip()
    if not key:
        return "", "", f"clave vacía en: {line!r}"
    value = raw_value.strip()
    if not value:
        return key, "", None
    unquoted, err = _unquote(value)
    if err:
        return key, "", err
    return key, unquoted, None


def _unquote(value: str) -> tuple[str, str | None]:
    """Quita comillas simples/dobles balanceadas; comillas sin cerrar -> error."""
    if value[0] in ('"', "'"):
        if len(value) < 2 or value[-1] != value[0]:
            return "", f"comilla sin cerrar en valor: {value!r}"
        return value[1:-1], None
    if value[-1] in ('"', "'"):
        return "", f"comilla de cierre sin apertura en valor: {value!r}"
    return value, None
