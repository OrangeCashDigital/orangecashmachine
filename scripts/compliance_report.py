#!/usr/bin/env python3
"""Nightly compliance report for maintainability metrics (B-54/B-55).

Runs ruff (C901/PLR/SIM) + vulture + audit_validator (M21-M25) and
generates a summary report. Intended for nightly CI or local execution.
Non-blocking by design.

Usage:
    python scripts/compliance_report.py              # stdout
    python scripts/compliance_report.py --json       # JSON output
    python scripts/compliance_report.py --save PATH  # save to file
"""

from __future__ import annotations

import contextlib
import json
import subprocess
import sys
from datetime import date
from pathlib import Path


def _run(cmd: list[str]) -> tuple[int, str]:
    """Run a command and return (returncode, combined output)."""
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parent.parent,
    )
    return result.returncode, result.stdout + result.stderr


def _parse_ruff_violations(output: str) -> dict[str, int]:
    """Parse ruff --statistics output into {rule: count}."""
    violations: dict[str, int] = {}
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("Found") or line.startswith("No fixes"):
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[0].isdigit():
            count = int(parts[0])
            rule = parts[1]
            violations[rule] = count
    return violations


def _parse_vulture_findings(output: str) -> list[dict[str, str | int]]:
    """Parse vulture output into list of findings."""
    findings: list[dict[str, str | int]] = []
    for line in output.splitlines():
        line = line.strip()
        if not line or line.startswith("POSITIVES") or line.startswith("----"):
            continue
        if ":" in line and "%" in line:
            # Format: path:line: description (XX% confidence)
            parts = line.rsplit("(", 1)
            if len(parts) == 2:
                loc = parts[0].strip().rstrip(":")
                conf_str = parts[1].strip().rstrip(")")
                try:
                    confidence = int(conf_str.replace("% confidence", ""))
                except ValueError:
                    confidence = 0
                file_path, _, rest = loc.partition(":")
                line_no, _, desc = rest.partition(":")
                findings.append(
                    {
                        "file": file_path,
                        "line": line_no.strip(),
                        "description": desc.strip(),
                        "confidence": confidence,
                    }
                )
    return findings


def _parse_pass_line(line: str) -> dict[str, int]:
    """Parse the PASS summary line from audit_validator."""
    result: dict[str, int] = {
        "findings": 0,
        "rules": 0,
        "warnings": 0,
        "skipped": 0,
    }
    if "findings" in line:
        parts = line.split("findings")
        if len(parts) >= 2:
            num_str = parts[0].split("—")[-1].strip()
            with contextlib.suppress(ValueError):
                result["findings"] = int(num_str)
    if "reglas" in line:
        parts = line.split("reglas")
        if len(parts) >= 2:
            num_str = parts[0].split(",")[-1].strip()
            with contextlib.suppress(ValueError):
                result["rules"] = int(num_str)
    if "warnings" in line:
        parts = line.split("warnings")
        if len(parts) >= 2:
            num_str = parts[0].split("(")[-1].strip()
            with contextlib.suppress(ValueError):
                result["warnings"] = int(num_str)
    if "skipped" in line:
        parts = line.split("skipped")
        if len(parts) >= 2:
            num_str = parts[0].split("(")[-1].strip()
            with contextlib.suppress(ValueError):
                result["skipped"] = int(num_str)
    return result


def _parse_audit_validator(output: str) -> dict[str, object]:
    """Parse audit_validator output into structured result."""
    details: list[str] = []
    result: dict[str, object] = {
        "status": "PASS",
        "findings": 0,
        "rules": 0,
        "warnings": 0,
        "skipped": 0,
        "details": details,
    }
    for line in output.splitlines():
        line = line.strip()
        if line.startswith("PASS"):
            result["status"] = "PASS"
            parsed = _parse_pass_line(line)
            result["findings"] = parsed["findings"]
            result["rules"] = parsed["rules"]
            result["warnings"] = parsed["warnings"]
            result["skipped"] = parsed["skipped"]
        elif line.startswith("FAIL"):
            result["status"] = "FAIL"
        elif line.startswith("ERROR"):
            result["status"] = "ERROR"
        elif line.startswith("[") and "]" in line:
            # Detail line like: [M21] ...
            details.append(line)
    return result


def generate_report() -> dict:
    """Generate the compliance report as a dict."""
    today = date.today().isoformat()

    # Run ruff
    ruff_code, ruff_out = _run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            ".",
            "--select",
            "C901,PLR,SIM",
            "--statistics",
        ]
    )
    ruff_violations = _parse_ruff_violations(ruff_out)
    ruff_total = sum(ruff_violations.values())

    # Run vulture
    vulture_code, vulture_out = _run(
        [
            sys.executable,
            "-m",
            "vulture",
            "packages",
            "ocm",
            "shared",
            "apps",
            "--min-confidence",
            "80",
        ]
    )
    vulture_findings = _parse_vulture_findings(vulture_out)
    vulture_total = len(vulture_findings)

    # Run ruff check (full, for error count)
    ruff_full_code, ruff_full_out = _run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            ".",
        ]
    )
    ruff_full_total = 0
    for line in ruff_full_out.splitlines():
        if line.startswith("Found"):
            parts = line.split()
            if len(parts) >= 2:
                with contextlib.suppress(ValueError):
                    ruff_full_total = int(parts[1])

    # Run audit_validator (M21-M25)
    audit_code, audit_out = _run(
        [
            sys.executable,
            "scripts/audit_validator.py",
        ]
    )
    audit_result = _parse_audit_validator(audit_out)

    # Determine overall status
    status = "PASS"
    if ruff_code != 0 or vulture_code != 0:
        status = "WARN"
    if audit_result["status"] == "FAIL":
        status = "FAIL"

    report = {
        "report_type": "nightly_compliance",
        "date": today,
        "timestamp": f"{today}T00:00:00Z",
        "ruff_complexity": {
            "rules": "C901,PLR,SIM",
            "violations": ruff_violations,
            "total": ruff_total,
            "exit_code": ruff_code,
        },
        "ruff_full": {
            "total": ruff_full_total,
            "exit_code": ruff_full_code,
        },
        "vulture": {
            "findings_count": vulture_total,
            "findings": vulture_findings[:20],  # top 20
            "exit_code": vulture_code,
        },
        "registry_validation": {
            "status": audit_result["status"],
            "findings": audit_result["findings"],
            "rules": audit_result["rules"],
            "warnings": audit_result["warnings"],
            "skipped": audit_result["skipped"],
            "details": audit_result["details"][:10],  # type: ignore[index]  # top 10
        },
        "summary": {
            "ruff_complexity_violations": ruff_total,
            "ruff_full_errors": ruff_full_total,
            "vulture_findings": vulture_total,
            "registry_status": audit_result["status"],
            "status": status,
        },
    }
    return report


def _print_text(report: dict) -> None:
    """Print human-readable report."""
    print(f"═══ Nightly Compliance Report — {report['date']} ═══")
    print()
    s = report["summary"]
    print(f"  Status: {s['status']}")
    print(f"  Ruff (C901/PLR/SIM) violations: {s['ruff_complexity_violations']}")
    print(f"  Ruff (full E/F/I/C901/PLR/SIM) errors: {s['ruff_full_errors']}")
    print(f"  Vulture findings: {s['vulture_findings']}")
    print(f"  Registry validation (M21-M25): {s['registry_status']}")
    print()

    rc = report["ruff_complexity"]
    if rc["violations"]:
        print("  Ruff C901/PLR/SIM violations by rule:")
        for rule, count in sorted(rc["violations"].items(), key=lambda x: -x[1]):
            print(f"    {rule}: {count}")
        print()

    v = report["vulture"]
    if v["findings"]:
        print(f"  Vulture findings (top {len(v['findings'])}):")
        for f in v["findings"][:10]:
            print(f"    {f['file']}:{f['line']} — {f['description']} ({f['confidence']}%)")
        if v["findings_count"] > 10:
            print(f"    ... and {v['findings_count'] - 10} more")
        print()

    rv = report["registry_validation"]
    if rv["details"]:
        print(f"  Registry validation details ({rv['status']}):")
        for d in rv["details"]:
            print(f"    {d}")
        print()


def main() -> None:
    report = generate_report()

    if "--json" in sys.argv:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    elif "--save" in sys.argv:
        idx = sys.argv.index("--save")
        if idx + 1 < len(sys.argv):
            path = Path(sys.argv[idx + 1])
            path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
            print(f"Report saved to {path}")
        else:
            print("Error: --save requires a path", file=sys.stderr)
            sys.exit(1)
    else:
        _print_text(report)


if __name__ == "__main__":
    main()
