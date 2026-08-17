"""ARCH-006 — Orphaned Contract / Port.

Detecta interfaces/ports sin consumidores, sin implementaciones, con
implementaciones inexistentes, documentación contradictoria o en capa
incorrecta. Distingue orphaned contract de reserved-but-unused.
"""

from __future__ import annotations

from pathlib import Path

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule

PORTS_DIRS = ("packages/market_data/ports", "packages/portfolio/ports")
# Contracts reservados por diseño (documentados como futuros) — configurable
RESERVED = {"feature_reader"}


class Arch006Rule(Rule):
    rule_id = "ARCH-006"
    rule_name = "Orphaned Contract / Port"
    description = (
        "Detecta ports/contracts huérfanos (sin consumidores/implementaciones reales, "
        "o con docstring que referencia implementaciones inexistentes)."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        findings: list[Finding] = []
        port_files = [p for p in ctx.files if any(p.is_relative_to(ctx.root / d) for d in PORTS_DIRS)]

        for p in port_files:
            text = ctx.text(p)
            if not text:
                continue
            for cls_name, cls_line in _protocol_classes(ctx, p):
                # consumidores fuera del propio archivo
                consumers = [(path, line) for (path, line) in ctx.references(cls_name) if path != p]
                # implementaciones citadas en docstring
                doc_impls = _docstring_impl_paths(text, cls_name)
                missing_impls = [path_str for path_str in doc_impls if not _impl_exists(ctx, path_str)]
                # contradicción inbound/outbound
                contradictory = "INBOUND" in text[:2000] and "outbound" in str(p)

                is_reserved = any(r in str(p) for r in RESERVED)
                if not consumers and not is_reserved:
                    evidence = [
                        Evidence(str(p), cls_line, cls_name, f"class {cls_name} (Protocol)"),
                        Evidence(str(p), None, None, "0 consumidores fuera del archivo de definición"),
                    ]
                    if missing_impls:
                        evidence.append(
                            Evidence(
                                str(p),
                                None,
                                None,
                                f"implementaciones citadas inexistentes: {', '.join(missing_impls)}",
                            )
                        )
                    if contradictory:
                        evidence.append(
                            Evidence(str(p), None, None, "docstring se autodeclara INBOUND en ports/outbound/")
                        )
                    findings.append(
                        self.finding(
                            Status.FAIL,
                            f"Port/contract {cls_name} huérfano en {p} — 0 consumidores reales"
                            f"{' y docstring referencia implementaciones inexistentes' if missing_impls else ''}",
                            file=str(p),
                            line=cls_line,
                            symbol=cls_name,
                            evidence=evidence,
                            related_files=sorted({str(x[0]) for x in consumers}),
                            confidence=0.95,
                            concept="port",
                        )
                    )
        return findings or [
            self.finding(
                Status.PASS,
                "Ningún port/contract huérfano detectado.",
                confidence=0.9,
                concept="port",
            )
        ]


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
