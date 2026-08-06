"""tests/architecture/test_app_layer_guard — Pruebas de política del guard de apps/.

Blindaje de la Application Layer (PLAN-Maestro-Ingenieria.md §6): cada hallazgo
ya corregido de AUDIT-apps-2026-08-03 (H1, H4, H6, H8, H12) se convierte en una
regla permanente. Específicamente:
    R12 → H1 · R13 → H4 · R14 → H8 · R15 → H12 · R16 → H6

Metodología: cada regla lleva UNA prueba POSITIVA (código limpio → sin
violaciones; asegura cero falsos positivos) y UNA prueba NEGATIVA (anti-patrón →
violación; asegura que la regla detecta). Ninguna regla se considera incorporada
sin demostrar ambos casos (requisito de AUDIT-* y del plan maestro §6).

Fallar aquí = violación de boundary arquitectónica, no bug de lógica.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.app_layer_guard import (
    check_cli_main_not_god,
    check_flow_helpers_single_source,
    check_middleware_excludes_probes,
    check_no_argparse_in_use_cases,
    check_no_config_ctor_in_use_cases,
    check_no_getattr_default_in_use_cases,
    check_no_logger_remove_outside_bootstrap,
    check_no_namespace_in_use_cases,
    check_no_sigterm_outside_bootstrap,
    check_no_vars_in_use_cases,
    check_run_result_single_source,
    check_silent_paths_single_source,
    guard_app,
)

ROOT = Path(__file__).resolve().parent.parent.parent


def _write(tree_root: Path, relpath: str, content: str) -> None:
    p = tree_root / relpath
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")


def _uc(tree_root: Path, relpath: str, content: str) -> None:
    """Escribe un archivo en apps/app/use_cases/ del árbol sintético."""
    _write(tree_root, f"apps/app/use_cases/{relpath}", content)


def _cli(tree_root: Path, relpath: str, content: str) -> None:
    _write(tree_root, f"apps/app/cli/{relpath}", content)


# ── Master: el guard debe estar silencioso sobre el árbol real ────────────────


class TestMasterPositive:
    """El árbol actual cumple todas las reglas (cero falsos positivos)."""

    def test_guard_app_is_clean_on_real_tree(self):
        violations = guard_app(ROOT)
        assert not violations, "Guard disparó en el árbol sin defectos:\n" + "\n".join(violations)


# ── R12 → H1: use_cases sin argparse / Namespace / constructores de config ────


class TestNoArgparseInUseCases:
    def test_positive_real_tree(self):
        assert check_no_argparse_in_use_cases(ROOT) == []

    def test_negative_import(self, tmp_path):
        _uc(tmp_path, "x.py", "import argparse\n\nvalue: int = 1\n")
        out = check_no_argparse_in_use_cases(tmp_path)
        assert any("import argparse" in v for v in out), out

    def test_negative_from_import(self, tmp_path):
        _uc(tmp_path, "x.py", "from argparse import Namespace\n")
        out = check_no_argparse_in_use_cases(tmp_path)
        assert out, out

    def test_negative_attribute_usage(self, tmp_path):
        _uc(tmp_path, "x.py", "def f(p):\n    parser = p.ArgumentParser()\n")
        # p.ArgumentParser no es argparse — no debe disparar
        assert check_no_argparse_in_use_cases(tmp_path) == []


class TestNoConfigCtorInUseCases:
    def test_positive_real_tree(self):
        assert check_no_config_ctor_in_use_cases(ROOT) == []

    @pytest.mark.parametrize("ctor", ["TradingConfig", "RiskConfig", "AppRiskConfig"])
    def test_negative_ctor(self, tmp_path, ctor):
        _uc(tmp_path, "x.py", f"cfg = {ctor}(a=1, b=2)\n")
        out = check_no_config_ctor_in_use_cases(tmp_path)
        assert any(ctor in m for m in out), out


class TestNoNamespaceInUseCases:
    def test_positive_real(self):
        assert check_no_namespace_in_use_cases(ROOT) == []

    def test_negative_annotation(self, tmp_path):
        _uc(tmp_path, "x.py", "def run(args: argparse.Namespace) -> None:\n    pass\n")
        out = check_no_namespace_in_use_cases(tmp_path)
        assert out, out

    def test_negative_name(self, tmp_path):
        _uc(tmp_path, "x.py", "ns = Namespace\n")
        out = check_no_namespace_in_use_cases(tmp_path)
        assert out, out


class TestNoVarsInUseCases:
    def test_positive_real(self):
        assert check_no_vars_in_use_cases(ROOT) == []

    def test_negative(self, tmp_path):
        _uc(tmp_path, "x.py", "data = vars(cli_args)\n")
        out = check_no_vars_in_use_cases(tmp_path)
        assert out, out


# ── R13 → H4: sin getattr con default en use_cases ────────────────────────────


class TestNoGetattrDefaultInUseCases:
    def test_positive_real(self):
        # execute_live.shutdown() usa getattr(resource, "close") — 2 args legítimo
        assert check_no_getattr_default_in_use_cases(ROOT) == []

    def test_negative_positional_default(self, tmp_path):
        _uc(tmp_path, "x.py", "v = getattr(cfg.risk.order, 'min_order_usd', 10.0)\n")
        out = check_no_getattr_default_in_use_cases(tmp_path)
        assert any("getattr" in m for m in out), out

    def test_negative_keyword_default(self, tmp_path):
        _uc(tmp_path, "x.py", "v = getattr(cfg, 'symbol', default='BTC/USDT')\n")
        out = check_no_getattr_default_in_use_cases(tmp_path)
        assert out, out

    def test_positive_two_arg_getattr(self, tmp_path):
        _uc(tmp_path, "x.py", "method = getattr(resource, 'close')\n")
        assert check_no_getattr_default_in_use_cases(tmp_path) == []


# ── R14 → H8: scaffolding de CLI en una sola fuente	───────────────────────────


class TestNoSigtermOutsideBootstrap:
    def test_positive_real(self):
        assert check_no_sigterm_outside_bootstrap(ROOT) == []

    def test_negative(self, tmp_path):
        _cli(tmp_path, "x.py", "def _handle_sigterm(signum, frame):\n    raise SystemExit(1)\n")
        out = check_no_sigterm_outside_bootstrap(tmp_path)
        assert any("sigterm" in m for m in out), out


class TestNoLoggerRemoveOutsideBootstrap:
    def test_positive_real(self):
        assert check_no_logger_remove_outside_bootstrap(ROOT) == []

    def test_negative(self, tmp_path):
        _cli(tmp_path, "x.py", "from loguru import logger\nlogger.remove()\nlogger.add(sys.stderr)\n")
        out = check_no_logger_remove_outside_bootstrap(tmp_path)
        assert any("logger.remove" in m for m in out), out

    def test_positive_docstring_mention(self, tmp_path):
        # Mencionar la regla en un comentario NO debe disparar (precisión, cero FP)
        _cli(
            tmp_path,
            "x.py",
            "def main():\n    # usar setup_logging de _bootstrap en vez de logger.remove()\n    return 0\n",
        )
        assert check_no_logger_remove_outside_bootstrap(tmp_path) == []


class TestFlowHelpersSingleSource:
    def test_positive_real(self):
        assert check_flow_helpers_single_source(ROOT) == []

    def test_negative_duplicated(self, tmp_path):
        _cli(tmp_path, "_bootstrap.py", "def setup_logging(**kw):\n    pass\n")
        _cli(tmp_path, "live_hydra.py", "def setup_logging(**kw):\n    pass\n")
        out = check_flow_helpers_single_source(tmp_path)
        assert any("duplicado" in m for m in out), out

    def test_negative_moved_out(self, tmp_path):
        _cli(tmp_path, "_bootstrap.py", "def setup_logging(**kw):\n    pass\n")
        _cli(tmp_path, "live_hydra.py", "def assemble_cli_config(**kw):\n    pass\n")
        out = check_flow_helpers_single_source(tmp_path)
        assert any("fuera de" in m for m in out), out


class TestCliMainNotGod:
    def test_positive_real(self):
        assert check_cli_main_not_god(ROOT) == []

    def test_negative_many_branches(self, tmp_path):
        body = "\n".join(f"    if v == {i}:\n        pass" for i in range(25))
        _cli(tmp_path, "live_hydra.py", f"def main(argv=None):\n{body}\n    return 0\n")
        out = check_cli_main_not_god(tmp_path)
        assert out, out


# ── R15 → H12: un único CycleRunResult ───────────────────────────────────────


class TestRunResultSingleSource:
    def test_positive_real(self):
        assert check_run_result_single_source(ROOT) == []

    def test_negative_redefined(self, tmp_path):
        _write(tmp_path, "apps/app/use_cases/run_result.py", "class CycleRunResult:\n    pass\n")
        _uc(tmp_path, "other.py", "class CycleRunResult:\n    pass\n")
        out = check_run_result_single_source(tmp_path)
        assert any("redefinido" in m for m in out), out

    def test_negative_legacy_result_classes(self, tmp_path):
        _write(tmp_path, "apps/app/use_cases/run_result.py", "class CycleRunResult:\n    pass\n")
        _uc(tmp_path, "x.py", "class PaperRunResult:\n    pass\nclass LiveRunResult:\n    pass\n")
        out = check_run_result_single_source(tmp_path)
        assert any("PaperRunResult" in m for m in out), out
        assert any("LiveRunResult" in m for m in out), out

    def test_negative_moved_out(self, tmp_path):
        _write(tmp_path, "apps/app/use_cases/ok.py", "class CycleRunResult:\n    pass\n")
        out = check_run_result_single_source(tmp_path)
        assert any("run_result.py" in m for m in out), out


# ── R16 → H6: SILENT_PATHS SSOT + exclusión de probes en middleware ───────────


class TestSilentPathsSingleSource:
    def test_positive_real(self):
        assert check_silent_paths_single_source(ROOT) == []

    def test_negative_redefined_outside(self, tmp_path):
        _write(tmp_path, "apps/api/middleware/__init__.py", "SILENT_PATHS = frozenset({'/health'})\n")
        _write(tmp_path, "apps/api/middleware/logging.py", "SILENT_PATHS = frozenset({'/health'})\n")
        out = check_silent_paths_single_source(tmp_path)
        assert out, out


class TestMiddlewareExcludesProbes:
    def test_positive_real(self):
        assert check_middleware_excludes_probes(ROOT) == []

    def test_negative_missing_import(self, tmp_path):
        _write(tmp_path, "apps/api/middleware/__init__.py", "SILENT_PATHS = frozenset({'/health'})\n")
        _write(
            tmp_path,
            "apps/api/middleware/rate_limit.py",
            "class RateLimitMiddleware:\n"
            "    async def dispatch(self, request, call_next):\n"
            "        return await call_next(request)\n",
        )
        out = check_middleware_excludes_probes(tmp_path)
        assert any("importar SILENT_PATHS" in m for m in out), out

    def test_negative_exclusion_after_await(self, tmp_path):
        _write(tmp_path, "apps/api/middleware/__init__.py", "SILENT_PATHS = frozenset({'/health'})\n")
        _write(
            tmp_path,
            "apps/api/middleware/rate_limit.py",
            "from api.middleware import SILENT_PATHS\n"
            "class RateLimitMiddleware:\n"
            "    async def dispatch(self, request, call_next):\n"
            "        resp = await call_next(request)\n"
            "        if request.url.path in SILENT_PATHS:\n"
            "            return resp\n"
            "        return resp\n",
        )
        out = check_middleware_excludes_probes(tmp_path)
        assert any("antes de procesar" in m for m in out), out
