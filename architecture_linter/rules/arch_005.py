"""ARCH-005 — Market Data Freshness Boundary.

Analiza la cadena Market Data → Strategy → Risk → Execution distinguiendo:
detección de stale/silencio, recovery, timestamp, metadata de freshness,
estado consultable, propagación y enforcement pre-orden.
Reporta exactamente dónde se rompe la cadena (PARTIAL por niveles).
"""

from __future__ import annotations

from typing import Optional

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule

DETECTION_SYMBOLS = ("wait_for", "TimeoutError", "_handle_silence_gap", "gap_threshold_ms")
RECOVERY_SYMBOLS = ("_run_recovery", "_restart_source", "GapRecoveryFetcher", "recovery_factory")
FRESHNESS_SYMBOLS = ("freshness", "stale", "lag_ms", "last_trade_ms", "age_ms", "stale_ms", "sequence_gap")
ENFORCEMENT_FILES = ("packages/trading/execution", "packages/trading/risk")


class Arch005Rule(Rule):
    rule_id = "ARCH-005"
    rule_name = "Market Data Freshness Boundary"
    description = (
        "Verifica por niveles si la cadena Market Data → Strategy → Risk → Execution conoce "
        "y propaga el estado de freshness/stale data; FAIL donde la cadena se rompe."
    )

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        def refs_in(symbol: str, roots: tuple[str, ...]) -> list:
            out = []
            for path, line in ctx.references(symbol):
                if any(path.is_relative_to(ctx.root / r) for r in roots):
                    out.append((path, line))
            return out

        # Nivel 1 — detección interna de silencio/stale
        detection_refs: list = []
        for sym in DETECTION_SYMBOLS:
            detection_refs.extend(refs_in(sym, ("packages/market_data/adapters/inbound/websocket",)))

        # Nivel 2 — recovery
        recovery_refs: list = []
        for sym in RECOVERY_SYMBOLS:
            recovery_refs.extend(refs_in(sym, ("packages/market_data",)))

        # Nivel 3 — estado consultable en el port TradesSource
        port_freshness: list = []
        for sym in FRESHNESS_SYMBOLS:
            port_freshness.extend(refs_in(sym, ("packages/market_data/ports/inbound/trades_source.py",)))

        # Nivel 4 — contrato/parámetro en FeatureSource y TradesSourceProtocol
        contract_refs: list = []
        for sym in FRESHNESS_SYMBOLS:
            contract_refs.extend(refs_in(sym, ("shared/contracts/boundaries.py", "packages/market_data/ports")))

        # Nivel 5 — propagación a trading/portfolio
        propagation_refs: list = []
        for sym in FRESHNESS_SYMBOLS:
            propagation_refs.extend(refs_in(sym, ("packages/trading", "packages/portfolio")))

        # Nivel 6 — enforcement antes de ejecutar orden
        enforcement_refs: list = []
        for sym in FRESHNESS_SYMBOLS:
            enforcement_refs.extend(refs_in(sym, ENFORCEMENT_FILES))

        evidence = [
            Evidence(str(p), line, None, _line_text(ctx.text(p), line))
            for (p, line) in (detection_refs + recovery_refs)[:6]
        ]

        levels = {
            "1-detección interna": bool(detection_refs),
            "2-recovery": bool(recovery_refs),
            "3-estado consultable en port": bool(port_freshness),
            "4-contrato (boundaries/ports)": bool(contract_refs),
            "5-propagación a trading/portfolio": bool(propagation_refs),
            "6-enforcement pre-orden": bool(enforcement_refs),
        }

        missing = [name for name, ok in levels.items() if not ok]
        present = [name for name, ok in levels.items() if ok]

        # Golden: detección/recovery presentes; estado/contrato/propagación/enforcement ausentes
        if len(missing) > 0:
            has_detection_recovery = levels["1-detección interna"] or levels["2-recovery"]
            assertion = (
                "Existe detección/recovery de silencio pero NO existe freshness como estado "
                "consultable ni su propagación/enforcement (la frontera auditada es la cadena "
                "Market Data → Strategy → Risk → Execution, no la implementación del gap scan)."
                if has_detection_recovery
                else "No se detectó detección de silencio ni recovery en la cadena."
            )
            return [
                self.finding(
                    Status.FAIL,
                    f"{assertion} Presentes: {', '.join(present) or 'ninguno'}. Ausentes: {', '.join(missing)}.",
                    file=str(detection_refs[0][0]) if detection_refs else None,
                    line=detection_refs[0][1] if detection_refs else None,
                    symbol="GapAwareStream",
                    evidence=evidence,
                    related_files=sorted({str(p) for (p, _) in (detection_refs + recovery_refs)}),
                    related_symbols=[
                        "GapAwareStream._handle_silence_gap",
                        "TradesSourceProtocol",
                        "FeatureSource.load_features",
                        "OMS.submit",
                    ],
                    confidence=0.9,
                    concept="freshness",
                    producer="GapAwareStream",
                    consumer="OMS/RiskManager",
                )
            ]

        return [
            self.finding(
                Status.PASS,
                "Cadena de freshness completa en todos los niveles.",
                confidence=0.9,
                concept="freshness",
            )
        ]


def _line_text(text: Optional[str], lineno: int) -> str:
    if not text:
        return ""
    lines = text.splitlines()
    if 1 <= lineno <= len(lines):
        return lines[lineno - 1].strip()
    return ""
