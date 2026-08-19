"""tests/architecture/test_domain_subprocess_guard — R11: pureza de dominio.

R11 → H-11 (B-20): `subprocess` en domain está prohibido (pureza de dominio).
La resolución de git_hash se inyecta desde el composition root (ADR-0006) —
el dominio nunca ejecuta subprocess.

Metodología (requisito del plan maestro §6): cada regla lleva UNA prueba
POSITIVA (árbol real limpio → sin violaciones; cero falsos positivos) y UNA
prueba NEGATIVA (anti-patrón → violación; la regla detecta). Ninguna regla se
incorpora sin demostrar ambos casos.

Fallar aquí = violación de boundary arquitectónica, no bug de lógica.
"""

from __future__ import annotations

from pathlib import Path

from scripts.domain_subprocess_guard import guard_domain_subprocess

ROOT = Path(__file__).resolve().parent.parent.parent


def _write(tree_root: Path, relpath: str, content: str) -> None:
    p = tree_root / relpath
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def _domain(tree_root: Path, relpath: str, content: str) -> None:
    _write(tree_root, f"packages/example/domain/{relpath}", content)


class TestMasterPositive:
    """El árbol actual cumple R11 (cero falsos positivos)."""

    def test_domain_is_clean_on_real_tree(self):
        violations = guard_domain_subprocess(ROOT)
        assert not violations, "R11 disparó en el árbol sin defectos:\n" + "\n".join(violations)


class TestNegativeImportSubprocess:
    """Anti-patrón: `import subprocess` en domain → violación."""

    def test_plain_import_detected(self, tmp_path):
        _domain(
            tmp_path,
            "probe.py",
            "import subprocess\n\ndef f() -> None:\n    pass\n",
        )
        violations = guard_domain_subprocess(tmp_path)
        assert violations, "R11 no detectó import subprocess en domain"
        assert "R11" in violations[0]

    def test_aliased_import_detected(self, tmp_path):
        _domain(
            tmp_path,
            "probe.py",
            "import subprocess as sp\n\ndef f() -> None:\n    sp.run(['git', 'rev-parse'])\n",
        )
        violations = guard_domain_subprocess(tmp_path)
        assert violations

    def test_from_import_detected(self, tmp_path):
        _domain(
            tmp_path,
            "probe.py",
            "from subprocess import check_output\n\ndef f() -> None:\n    return check_output(['git'])\n",
        )
        violations = guard_domain_subprocess(tmp_path)
        assert violations

    def test_lazy_import_in_function_detected(self, tmp_path):
        _domain(
            tmp_path,
            "probe.py",
            "def f() -> str:\n    import subprocess\n    return subprocess.run(['git'])\n",
        )
        violations = guard_domain_subprocess(tmp_path)
        assert violations


class TestNegativeCleanDomain:
    """Anti-patrón no presente: domain limpio → sin violaciones."""

    def test_pure_domain_ok(self, tmp_path):
        _domain(
            tmp_path,
            "types.py",
            "from dataclasses import dataclass\n\n@dataclass\nclass Probe:\n    git_hash: str = 'unknown'\n",
        )
        violations = guard_domain_subprocess(tmp_path)
        assert not violations

    def test_subprocess_in_application_is_not_domain(self, tmp_path):
        """subprocess fuera de domain/ (infra/application) NO es violación R11."""
        _write(
            tmp_path,
            "packages/example/infrastructure/resolver.py",
            "import subprocess\n\ndef resolve() -> str:\n    return subprocess.check_output(['git']).decode()\n",
        )
        violations = guard_domain_subprocess(tmp_path)
        assert not violations


class TestDomainDetection:
    """El guard solo analiza paquetes con carpeta domain/."""

    def test_no_packages_dir_returns_empty(self, tmp_path):
        violations = guard_domain_subprocess(tmp_path)
        assert violations == []
