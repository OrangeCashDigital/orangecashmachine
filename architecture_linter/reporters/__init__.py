"""architecture_linter.reporters — emisión de resultados.

- human: formato `ARCH-001 ERROR FAIL file:line Symbol/Evidence/Reason`.
- json: estructura completa para CI.
- sarif: opcional (SARIF 2.1.0) para integración en GH Code Scanning / VS Code.
"""

from __future__ import annotations

import json
from pathlib import Path

from architecture_linter.models import RuleResult, Status


def render_human(results: list[RuleResult], root: Path) -> str:
    lines: list[str] = []
    for res in results:
        lines.append(f"{res.rule_id} {res.summary}")
        for f in res.findings:
            loc = f"{f.file}:{f.line}" if f.line is not None else (f.file or "-")
            sym = f"Symbol={f.symbol}" if f.symbol else "-"
            reason = f.message
            ev = "; ".join(str(e) for e in f.evidence[:3])
            lines.append(f"{res.rule_id} {f.severity.value.upper()} {f.status.value} {loc} {sym} {reason}")
            if ev:
                lines.append(f"    Evidence: {ev}")
            if f.confidence < 1.0:
                lines.append(f"    Confidence: {f.confidence:.2f}")
    lines.append(_summary_line(results))
    return "\n".join(lines)


def render_json(results: list[RuleResult], root: Path, include_evidence: bool = True) -> str:
    payload = {
        "schema_version": "1.0",
        "linter": "architecture_linter",
        "root": str(root),
        "rules": [
            {
                "rule_id": r.rule_id,
                "rule_name": r.rule_name,
                "status": r.status.value,
                "summary": r.summary,
                "findings": [
                    {
                        "rule_id": f.rule_id,
                        "severity": f.severity.value,
                        "status": f.status.value,
                        "message": f.message,
                        "file": f.file,
                        "line": f.line,
                        "symbol": f.symbol,
                        "confidence": f.confidence,
                        "concept": f.concept,
                        "owner": f.owner,
                        "consumer": f.consumer,
                        "producer": f.producer,
                        "related_files": f.related_files,
                        "related_symbols": f.related_symbols,
                        **({"evidence": [e.__dict__ for e in f.evidence]} if include_evidence else {}),
                    }
                    for f in r.findings
                ],
            }
            for r in results
        ],
        "summary": {
            "total_rules": len(results),
            "passed": sum(1 for r in results if r.status == Status.PASS),
            "failed": sum(1 for r in results if r.status == Status.FAIL),
            "partial": sum(1 for r in results if r.status == Status.PARTIAL),
            "unknown": sum(1 for r in results if r.status == Status.UNKNOWN),
            "findings_total": sum(len(r.findings) for r in results),
            "failed_findings": sum(len(r.findings) for r in results if r.status == Status.FAIL),
        },
    }
    return json.dumps(payload, indent=2, ensure_ascii=False)


def render_sarif(results: list[RuleResult], root: Path) -> str:
    """SARIF 2.1.0 mínimo: resultados + reglas con ID y shortDescription."""
    sarif_rules: list[dict] = []
    sarif_results: list[dict] = []
    for res in results:
        rule_index = len(sarif_rules)
        sarif_rules.append(
            {
                "id": res.rule_id,
                "name": res.rule_name,
                "shortDescription": {"text": res.summary},
            }
        )
        for f in res.findings:
            region = {}
            if f.line is not None:
                region = {"startLine": f.line}
            sarif_results.append(
                {
                    "ruleId": res.rule_id,
                    "ruleIndex": rule_index,
                    "level": _sarif_level(f.status),
                    "message": {"text": f.message},
                    "locations": [
                        {
                            "physicalLocation": {
                                "artifactLocation": {"uri": f.file or ""},
                                **({"region": region} if region else {}),
                            }
                        }
                    ],
                }
            )
    payload = {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "architecture_linter",
                        "informationUri": "https://github.com/anomalyco/orangecashmachine",
                        "rules": sarif_rules,
                    }
                },
                "results": sarif_results,
            }
        ],
    }
    return json.dumps(payload, indent=2)


def _sarif_level(status: Status) -> str:
    if status == Status.FAIL:
        return "error"
    if status == Status.PARTIAL:
        return "warning"
    return "none"


def _summary_line(results: list[RuleResult]) -> str:
    n_pass = sum(1 for r in results if r.status == Status.PASS)
    n_fail = sum(1 for r in results if r.status == Status.FAIL)
    n_partial = sum(1 for r in results if r.status == Status.PARTIAL)
    n_unknown = sum(1 for r in results if r.status == Status.UNKNOWN)
    return f"== Resumen: {len(results)} reglas | PASS={n_pass} FAIL={n_fail} PARTIAL={n_partial} UNKNOWN={n_unknown} =="
