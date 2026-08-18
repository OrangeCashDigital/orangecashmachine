"""Golden tests del Architecture Governance Linter contra el repo OCM real.

Reflejan el estado ACTUAL del repositorio (verificado manualmente en
docs/audits/2026-08-16-architecture-linter.md). Si una regla cambia su
veredicto, este test lo detecta: o el código cambió (revisar), o la regla
introdujo un falso positivo/negativo (corregir).

Solo se ejecutan si existe el repo OCM real (pytest corre desde la raíz).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from architecture_linter.config import load_config
from architecture_linter.engine import LinterEngine, RepoContext
from architecture_linter.models import Status
from architecture_linter.rules import build_rules

REPO_ROOT = Path(__file__).resolve().parents[2]

# Estado esperado (verificado) tras remediación arquitectónica — 2026-08-16.
GOLDEN_EXPECTED: dict[str, Status] = {
    "ARCH-001": Status.FAIL,  # multiple position owners (trading + portfolio)
    "ARCH-002": Status.FAIL,  # divergencia semántica WAC vs reemplazo/pop
    "ARCH-003": Status.PASS,  # gate de reconciliación submit-time e inicio de ciclo (ADR-0029, manage_open_orders)
    "ARCH-004": Status.FAIL,  # sin balance real; sizing contra capital_usd
    "ARCH-005": Status.FAIL,  # cadena de freshness rota en niveles 3–6
    "ARCH-006": Status.PASS,  # ports huérfanos eliminados o cableados (remediación)
    "ARCH-007": Status.FAIL,  # 8 contratos duplicados/homónimos (ExchangeCircuitOpenError consolidado)
    "ARCH-008": Status.FAIL,  # 1 stub de producción (WSTradesSource; InfraMetricsKafkaProducer eliminado)
    "ARCH-009": Status.PASS,  # capas BC-08 respetadas (ignore_imports documentados)
    "ARCH-010": Status.FAIL,  # estado mutable duplicado (position ×6, order ×4)
}


@pytest.mark.skipif(not (REPO_ROOT / "architecture_linter" / "importlinter.toml").is_file(), reason="requiere repo OCM")
def test_golden_statuses_repo_actual() -> None:
    cfg = load_config()
    ctx = RepoContext(root=REPO_ROOT, roots=cfg.roots, exclude_dirs=frozenset(cfg.exclude_dirs))
    rules = build_rules(
        enabled=None,
        severity_overrides=cfg.severity_overrides,
        allow=cfg.allow,
    )
    results = LinterEngine(ctx).run(rules)
    by_id = {r.rule_id: r.status for r in results}
    assert set(by_id) == set(GOLDEN_EXPECTED), f"Reglas divergentes: {set(by_id) ^ set(GOLDEN_EXPECTED)}"
    for rule_id, expected in GOLDEN_EXPECTED.items():
        assert by_id[rule_id] == expected, f"{rule_id}: esperado {expected.value}, obtenido {by_id[rule_id].value}"


@pytest.mark.skipif(not (REPO_ROOT / "architecture_linter" / "importlinter.toml").is_file(), reason="requiere repo OCM")
def test_golden_arch006_orphan_ports() -> None:
    """Tras la remediación no debe quedar ningún port huérfano (regresión guard)."""
    cfg = load_config()
    ctx = RepoContext(root=REPO_ROOT, roots=cfg.roots, exclude_dirs=frozenset(cfg.exclude_dirs))
    rules = build_rules(enabled={"ARCH-006"}, allow=cfg.allow)
    (res,) = LinterEngine(ctx).run(rules)
    assert res.status == Status.PASS
    symbols = {f.symbol for f in res.findings}
    # Puertos huérfanos eliminados/cableados en la remediación — no deben reaparecer.
    for legacy in (
        "MarketDataSourcePort",  # golden F3 — eliminado (docstring contradictorio)
        "EventPublisherPort",
        "EventConsumerPort",
        "BronzeStoragePort",
        "CircuitBreakerPort",
        "OrderBookSourceProtocol",
    ):
        assert legacy not in symbols, f"reapareció port huérfano: {legacy}"


@pytest.mark.skipif(not (REPO_ROOT / "architecture_linter" / "importlinter.toml").is_file(), reason="requiere repo OCM")
def test_golden_arch007_duplicates() -> None:
    """Los duplicados golden de ARCH-007 deben estar reportados y CompositionRoot excluido."""
    cfg = load_config()
    ctx = RepoContext(root=REPO_ROOT, roots=cfg.roots, exclude_dirs=frozenset(cfg.exclude_dirs))
    rules = build_rules(enabled={"ARCH-007"}, allow=cfg.allow)
    (res,) = LinterEngine(ctx).run(rules)
    symbols = {f.symbol for f in res.findings}
    for expected in (
        "OrderStatus",
        "StorageFactoryPort",
        "AnomalyRegistryPort",
        "QualityPipelineResult",
        "RetryExhaustedError",
        "SchemaVersionError",
        "_TransientProxy",
        "PipelineContext",
    ):
        assert expected in symbols, f"falta {expected}"
    # ExchangeCircuitOpenError consolidado en domain — no debe reportarse como duplicado.
    assert "ExchangeCircuitOpenError" not in symbols, "duplicado reintroducido"
    assert "CompositionRoot" not in symbols  # allowlist ADR-0003


@pytest.mark.skipif(not (REPO_ROOT / "architecture_linter" / "importlinter.toml").is_file(), reason="requiere repo OCM")
def test_golden_arch008_stubs() -> None:
    cfg = load_config()
    ctx = RepoContext(root=REPO_ROOT, roots=cfg.roots, exclude_dirs=frozenset(cfg.exclude_dirs))
    rules = build_rules(enabled={"ARCH-008"}, allow=cfg.allow)
    (res,) = LinterEngine(ctx).run(rules)
    symbols = {f.symbol for f in res.findings}
    assert "WSTradesSource" in symbols  # golden F1 — diseñado con fallback REST, honesto
    assert "InfraMetricsKafkaProducer" not in symbols  # eliminado en remediación
    # No FPs: streams legítimos (StopAsyncIteration = terminación normal) y null-objects
    for legit in ("GapAwareStream", "GapRecoveryFetcher", "TradesBackfillFetcher", "NullMetrics", "NoopGapPublisher"):
        assert legit not in symbols, f"FP ARCH-008: {legit}"
