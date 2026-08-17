"""ARCH-008 — False Capability / Stub.

Detecta componentes de producción que aparentan proporcionar una capacidad
pero no la ejecutan. La detección es por comportamiento estructural (raise
NotImplementedError/StopAsyncIteration, estado que nunca alcanza operativo,
métodos públicos vacíos); el marcador documental NOT_IMPLEMENTED es solo
evidencia de refuerzo, nunca el trigger único — distingue documentación
legítima de comportamiento ejecutable.
"""

from __future__ import annotations

from architecture_linter.analyzers.behavior import analyze_stub_class
from architecture_linter.engine import RepoContext
from architecture_linter.models import Finding, Status
from architecture_linter.rules.base import Rule


class Arch008Rule(Rule):
    rule_id = "ARCH-008"
    rule_name = "False Capability / Stub"
    description = (
        "Detecta clases que exponen una interfaz funcional sin ejecutar la capacidad "
        "prometida (stub de producción) por comportamiento estructural, distinguiendo "
        "interfaces abstractas intencionales y documentación legítima."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        findings: list[Finding] = []
        seen: set[tuple[str, str]] = set()
        for path in ctx.files:
            info = ctx.module(path)
            if not info:
                continue
            for cls in info.classes:
                if cls.is_protocol or cls.is_abstract or cls.is_enum:
                    continue
                stub = analyze_stub_class(ctx, path, cls)
                if stub is None:
                    continue
                key = (str(path), cls.name)
                if key in seen:
                    continue
                seen.add(key)
                findings.append(
                    self.finding(
                        Status.FAIL,
                        f"Stub de producción: {cls.name} expone capacidad sin ejecutarla ({'; '.join(stub.triggers)}).",
                        file=str(path),
                        line=cls.line,
                        symbol=cls.name,
                        evidence=stub.evidence,
                        confidence=0.9,
                        concept="capability",
                        producer=cls.name,
                    )
                )
        return findings or [
            self.finding(
                Status.PASS,
                "Sin stubs de producción detectados (interfaces abstractas excluidas).",
                confidence=0.9,
                concept="capability",
            )
        ]
