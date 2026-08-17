"""architecture_linter.rules.base — clase base de reglas del linter."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, RuleResult, Severity, Status


class Rule(ABC):
    """Regla arquitectónica. No conoce el CLI; solo consume RepoContext."""

    rule_id: str = "ARCH-000"
    rule_name: str = "Base"
    description: str = ""
    default_severity: Severity = Severity.ERROR
    enabled: bool = True

    def __init__(
        self,
        severity: Optional[Severity] = None,
        enabled: Optional[bool] = None,
        allow: Optional[set[str]] = None,
    ) -> None:
        if severity is not None:
            self.default_severity = severity
        if enabled is not None:
            self.enabled = enabled
        self.allow: set[str] = allow or set()

    @abstractmethod
    def analyze(self, ctx: RepoContext) -> list[Finding]:
        """Analiza el contexto y devuelve los hallazgos (vacío si PASS)."""

    def run(self, ctx: RepoContext) -> RuleResult:
        findings = self.analyze(ctx)
        status = self._aggregate_status(findings)
        return RuleResult(
            rule_id=self.rule_id,
            rule_name=self.rule_name,
            status=status,
            findings=findings,
            summary=self._summarize(findings, status),
        )

    def _aggregate_status(self, findings: list[Finding]) -> Status:
        if not findings:
            return Status.PASS
        if any(f.status == Status.FAIL for f in findings):
            return Status.FAIL
        if any(f.status == Status.PARTIAL for f in findings):
            return Status.PARTIAL
        if all(f.status == Status.PASS for f in findings):
            return Status.PASS
        return Status.UNKNOWN

    def _summarize(self, findings: list[Finding], status: Status) -> str:
        if not findings:
            return f"{self.rule_id}: no se encontró evidencia de violación"
        return f"{self.rule_id}: {len(findings)} finding(s) — {status.value}"

    # ── Helpers de evidencia ──────────────────────────────────────────────── #

    def ev(self, file: str, line: Optional[int], symbol: Optional[str], text: str) -> Evidence:
        return Evidence(file=file, line=line, symbol=symbol, text=text)

    def finding(
        self,
        status: Status,
        message: str,
        *,
        file: Optional[str] = None,
        line: Optional[int] = None,
        symbol: Optional[str] = None,
        evidence: Optional[list[Evidence]] = None,
        related_files: Optional[list[str]] = None,
        related_symbols: Optional[list[str]] = None,
        confidence: float = 1.0,
        concept: Optional[str] = None,
        owner: Optional[str] = None,
        consumer: Optional[str] = None,
        producer: Optional[str] = None,
    ) -> Finding:
        return Finding(
            rule_id=self.rule_id,
            severity=self.default_severity,
            status=status,
            message=message,
            file=file,
            line=line,
            symbol=symbol,
            evidence=evidence or [],
            related_files=related_files or [],
            related_symbols=related_symbols or [],
            confidence=confidence,
            concept=concept,
            owner=owner,
            consumer=consumer,
            producer=producer,
        )
