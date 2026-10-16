"""ARCH-006 — Orphaned Contract / Port.

Detecta interfaces/ports sin consumidores, sin implementaciones, con
implementaciones inexistentes, documentación contradictoria o en capa
incorrecta. Distingue orphaned contract de reserved-but-unused y de
cobertura legítima por Staged Port (ADR-0044).

Semántica de cobertura (B2):
    consumer exists                      -> PASS
    RESERVED legacy (feature_reader)     -> PASS (por diseño, sin migración)
    staged registry válido (núcleo B1)   -> PASS (aportando evidencia)
    resto                                -> FAIL (huérfano)

Fail-closed sobre `policies/registry.yaml`: si el registry no existe, no
parsea o es inválido, ningún port puede reclamar cobertura staged.

El kernel de detección (protocol classes, RepoContext.references, consumidores,
traversal) no cambia; solo la decisión de cobertura consume el núcleo
`architecture_linter.staged_ports` (B1). La responsabilidad de validar el
registry completo (ADR/tracking/duplicados globales, Caso I consumidor+staged)
pertenece a M26 (B3), no a esta regla.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule
from architecture_linter.staged_ports import ParseResult, load_staged_registry, validate_entry

PORTS_DIRS = ("packages/market_data/ports", "packages/portfolio/ports")
# Contracts reservados por diseño (documentados como futuros) — legacy, intacto.
# feature_reader permanece exactamente como está: no se elimina, no se migra,
# no se amplía (MarketDataViewPort NO puede entrar aquí: usa staged registry).
RESERVED = {"feature_reader"}

REGISTRY_RELPATH = Path("policies") / "registry.yaml"


class Arch006Rule(Rule):
    rule_id = "ARCH-006"
    rule_name = "Orphaned Contract / Port"
    description = (
        "Detecta ports/contracts huérfanos (sin consumidores/implementaciones reales, "
        "o con docstring que referencia implementaciones inexistentes) salvo cobertura "
        "legítima por RESERVED legacy o Staged Port válido (ADR-0044)."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        findings: list[Finding] = []
        staged = load_staged_registry(ctx.root / REGISTRY_RELPATH)
        port_files = [p for p in ctx.files if any(p.is_relative_to(ctx.root / d) for d in PORTS_DIRS)]

        for p in port_files:
            text = ctx.text(p)
            if not text:
                continue
            for cls_name, cls_line in _protocol_classes(ctx, p):
                finding = self._class_finding(ctx, p, text, cls_line, cls_name, staged)
                if finding is not None:
                    findings.append(finding)
        return findings or [
            self.finding(
                Status.PASS,
                "Ningún port/contract huérfano detectado.",
                confidence=0.9,
                concept="port",
            )
        ]

    def _class_finding(
        self,
        ctx: RepoContext,
        p: Path,
        text: str,
        cls_line: int,
        cls_name: str,
        staged: ParseResult,
    ) -> Finding | None:
        # consumidores fuera del propio archivo
        consumers = [(path, line) for (path, line) in ctx.references(cls_name) if path != p]
        # implementaciones citadas en docstring
        doc_impls = _docstring_impl_paths(text, cls_name)
        missing_impls = [path_str for path_str in doc_impls if not _impl_exists(ctx, path_str)]
        # contradicción inbound/outbound
        contradictory = "INBOUND" in text[:2000] and "outbound" in str(p)

        is_reserved = any(r in str(p) for r in RESERVED)
        if consumers or is_reserved:
            return None

        registry_path = ctx.root / REGISTRY_RELPATH
        coverage_errors, coverage_desc = _staged_coverage(staged, cls_name, root=ctx.root, today=_evaluation_date())
        if coverage_errors is None:
            return self.finding(
                Status.FAIL,
                f"Port/contract {cls_name} huérfano en {p} — 0 consumidores reales"
                f"{' y docstring referencia implementaciones inexistentes' if missing_impls else ''}",
                file=str(p),
                line=cls_line,
                symbol=cls_name,
                evidence=_orphan_evidence(p, cls_line, cls_name, missing_impls, contradictory),
                related_files=sorted({str(x[0]) for x in consumers}),
                confidence=0.95,
                concept="port",
            )
        if coverage_errors:
            return self.finding(
                Status.FAIL,
                f"Port/contract {cls_name} huérfano en {p} — 0 consumidores reales y "
                f"{coverage_desc}: {'; '.join(coverage_errors)}",
                file=str(p),
                line=cls_line,
                symbol=cls_name,
                evidence=_orphan_evidence(
                    p,
                    cls_line,
                    cls_name,
                    missing_impls,
                    contradictory,
                    extra=Evidence(
                        str(registry_path),
                        None,
                        None,
                        f"staged coverage invalid: {'; '.join(coverage_errors)}",
                    ),
                ),
                related_files=sorted({str(x[0]) for x in consumers}),
                confidence=0.95,
                concept="port",
            )
        return self.finding(
            Status.PASS,
            f"Port/contract {cls_name} cubierto por staged válido (ADR-0044) en {p} — {coverage_desc}",
            file=str(p),
            line=cls_line,
            symbol=cls_name,
            evidence=_orphan_evidence(
                p,
                cls_line,
                cls_name,
                missing_impls,
                contradictory,
                extra=Evidence(str(registry_path), None, cls_name, f"staged coverage valid: {coverage_desc}"),
            ),
            related_files=sorted({str(x[0]) for x in consumers}),
            confidence=0.95,
            concept="port",
        )


def _evaluation_date() -> date:
    """Fecha de evaluación de expiración (monkeypatcheable en tests)."""
    return date.today()


def _staged_coverage(
    staged: ParseResult,
    port_name: str,
    *,
    root: Path,
    today: date,
) -> tuple[list[str] | None, str | None]:
    """Decisión fail-closed de cobertura staged para un port huérfano.

    Devuelve (errores, descripción):
      (None, None)            -> el port no está registrado (huérfano real).
      ([], desc)              -> cobertura staged válida (PASS).
      ([errores...], desc)    -> intento staged inválido / registry no disponible (FAIL).
    """
    if not staged.ok:
        return list(staged.errors), "registry staged no disponible (fail-closed)"
    matches = [entry for entry in staged.entries if entry.get("port") == port_name]
    if not matches:
        return None, None
    if len(matches) > 1:
        return [f"entrada staged duplicada para {port_name!r}"], "registry staged inválido (duplicado)"
    errors = validate_entry(matches[0], today=today, root=root)
    if errors:
        return errors, "cobertura staged inválida"
    return [], "cobertura staged válida"


def _orphan_evidence(
    path: Path,
    cls_line: int,
    cls_name: str,
    missing_impls: list[str],
    contradictory: bool,
    extra: Evidence | None = None,
) -> list[Evidence]:
    evidence = [
        Evidence(str(path), cls_line, cls_name, f"class {cls_name} (Protocol)"),
        Evidence(str(path), None, None, "0 consumidores fuera del archivo de definición"),
    ]
    if missing_impls:
        evidence.append(
            Evidence(
                str(path),
                None,
                None,
                f"implementaciones citadas inexistentes: {', '.join(missing_impls)}",
            )
        )
    if contradictory:
        evidence.append(Evidence(str(path), None, None, "docstring se autodeclara INBOUND en ports/outbound/"))
    if extra is not None:
        evidence.append(extra)
    return evidence


def _protocol_classes(ctx: RepoContext, path: Path) -> list[tuple[str, int]]:
    info = ctx.module(path)
    if not info:
        return []
    return [(cls.name, cls.line) for cls in info.classes if cls.is_protocol or "Protocol" in " ".join(cls.bases)]


def _docstring_impl_paths(text: str, cls_name: str) -> list[str]:
    """Extrae rutas de módulos citadas en el docstring (implementaciones de referencia)."""
    import re

    out: list[str] = []
    for m in re.finditer(r"([a-z_][a-z0-9_]*\.[a-z_][a-z0-9_.]*\.[A-Z][A-Za-z0-9_]*)", text):
        path_str = m.group(1)
        if path_str not in out:
            out.append(path_str)
    return out


def _impl_exists(ctx: RepoContext, dotted_path: str) -> bool:
    """Verifica si una ruta 'modulo.paquete.Clase' existe como clase en el repo."""
    cls_name = dotted_path.split(".")[-1]
    for path, name, _line in ctx.all_classes():
        if name == cls_name:
            return True
    return False
