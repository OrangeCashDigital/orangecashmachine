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
    "ARCH-001": Status.PASS,  # único owner de position: RiskManager mirror; OMS._orders no es position
    "ARCH-002": Status.PASS,  # resuelto por delegación de _entry_positions a PortfolioService (ARCH-001 fix)
    "ARCH-003": Status.PASS,  # gate de reconciliación submit-time e inicio de ciclo (ADR-0029, manage_open_orders)
    "ARCH-004": Status.FAIL,  # P1 LIVE-blocker: pendiente implementación de ADR-0030 / B-MD-009 (multi-paso, Fase 3)
    "ARCH-005": Status.FAIL,  # P1 LIVE-blocker: pendiente implementación de freshness chain / B-MD-001 (F2/F3)
    "ARCH-006": Status.PASS,  # ports huérfanos eliminados o cableados (remediación)
    "ARCH-007": Status.FAIL,  # 8 duplicados/homónimos: pendiente merge de fix/ccxt-factory-release
    "ARCH-008": Status.PASS,  # sin stubs: NoopWSTradesSource = null-object
    # intencional; InfraMetricsKafkaProducer eliminado.
    "ARCH-009": Status.PASS,  # capas BC-08 respetadas (ignore_imports documentados)
    "ARCH-010": Status.PASS,  # position: 1 owner (RiskManager mirror); order: 1 store
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
    """ARCH-007 está pendiente en `main` (8 duplicados). El fix vive en
    `fix/ccxt-factory-release`, fuera de este PR.

    Se saltea cuando ARCH-007 está en FAIL. Cuando esa rama mergee, la
    remediación hará pasar este test sin modificarlo.
    """
    cfg = load_config()
    ctx = RepoContext(root=REPO_ROOT, roots=cfg.roots, exclude_dirs=frozenset(cfg.exclude_dirs))
    rules = build_rules(enabled={"ARCH-007"}, allow=cfg.allow)
    (res,) = LinterEngine(ctx).run(rules)

    # Si ARCH-007 está pendiente (FAIL), skip — su remediación vive en otra rama.
    if res.status == Status.FAIL:
        pytest.skip("ARCH-007 pendiente: remediación vive en fix/ccxt-factory-release")

    # Post-remediación: los duplicados no deben reportarse
    symbols = {f.symbol for f in res.findings}
    assert "OrderStatus" not in symbols, "OrderStatus permitido por allowlist no debe reportar finding"
    assert "ExchangeCircuitOpenError" not in symbols, "duplicado reintroducido"
    assert "CompositionRoot" not in symbols  # allowlist ADR-0003


@pytest.mark.skipif(not (REPO_ROOT / "architecture_linter" / "importlinter.toml").is_file(), reason="requiere repo OCM")
def test_golden_arch008_stubs() -> None:
    """Tras ARCH-008 remediation: sin stubs de producción.

    - WSTradesSource renombrado a NoopWSTradesSource (null-object intencional,
      `__anext__` → StopAsyncIteration es comportamiento de null-object, no de
      stub productivo; excluido por `_is_null_object_name`).
    - InfraMetricsKafkaProducer eliminado en remediación anterior.
    - Streams y null-objects legítimos no son falsos positivos.
    """
    cfg = load_config()
    ctx = RepoContext(root=REPO_ROOT, roots=cfg.roots, exclude_dirs=frozenset(cfg.exclude_dirs))
    rules = build_rules(enabled={"ARCH-008"}, allow=cfg.allow)
    (res,) = LinterEngine(ctx).run(rules)
    # PASS esperado: ningún stub de producción
    assert res.status == Status.PASS, f"ARCH-008 no debería fallar: {res}"
    symbols = {f.symbol for f in res.findings}
    # Null-objects y streams legítimos NO son falsos positivos
    for legit in (
        "NoopWSTradesSource",
        "GapAwareStream",
        "GapRecoveryFetcher",
        "TradesBackfillFetcher",
        "NullMetrics",
        "NoopGapPublisher",
    ):
        assert legit not in symbols, f"FP ARCH-008: {legit}"
    # Nada del proyecto debería ser stub de producción
    assert "InfraMetricsKafkaProducer" not in symbols
