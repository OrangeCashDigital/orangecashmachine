# -*- coding: utf-8 -*-
"""
market_data/infrastructure/quality/ge_checker.py
=================================================

Adaptador Great Expectations → DataQualityCheckerPort.

Responsabilidad
---------------
Ejecutar la suite OHLCV de GE contra un DataFrame Silver y traducir
los resultados a DataQualityReport (dominio).

Principios
----------
DIP    — satisface DataQualityCheckerPort sin herencia; duck typing / Protocol
SRP    — solo traducción GE → dominio; la lógica de expectativas está en ge_suite.py
SafeOps — catch-all interno: nunca propaga excepciones al QualityPipeline
OCP    — nueva suite = nueva instancia con suite distinta; sin modificar esta clase

Integración en arquitectura (BC-31 safe)
-----------------------------------------
infrastructure/ puede importar quality/ (ports y domain).
quality/pipeline.py importa DataQualityCheckerPort (port), nunca este módulo.
El cableado vive en el Composition Root (pipeline_factory.py).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pandas as pd

if TYPE_CHECKING:
    # DataQualityReport: tipo de dominio usado solo en anotaciones de retorno.
    # Import bajo TYPE_CHECKING evita dependencia circular infra → domain en runtime.
    # from __future__ import annotations garantiza evaluación lazy (PEP 563).
    from market_data.domain.quality.types import DataQualityReport

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

_GE_DATASOURCE_NAME = "market_data_pipeline"
_GE_ASSET_NAME = "ohlcv_frames"
_GE_BATCH_DEF_NAME = "full_batch"
_GE_VALIDATION_NAME = "ohlcv_validation"

# Expectativas cuyo fallo se clasifica como CRITICAL (no WARNING)
_CRITICAL_EXPECTATIONS = frozenset(
    {
        "expect_column_to_exist",
        "expect_column_values_to_not_be_null",
        "expect_column_pair_values_a_to_be_greater_than_b",
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_increasing",
    }
)


# ---------------------------------------------------------------------------
# Dataclass de issue GE (traducción intermedia antes de DataQualityReport)
# ---------------------------------------------------------------------------


@dataclass(slots=True, frozen=True)
class _GEIssue:
    expectation_type: str
    column: str | None
    element_count: int
    unexpected_count: int
    unexpected_rate: float
    severity: str  # "critical" | "warning"
    message: str


# ---------------------------------------------------------------------------
# Adaptador principal
# ---------------------------------------------------------------------------


class GEChecker:
    """
    Implementa DataQualityCheckerPort usando Great Expectations en modo efímero.

    Modo ephemeral: sin filesystem, sin Data Docs, sin config externa.
    El contexto GE se crea una vez en __init__ y se reutiliza.

    Uso típico (vía factory):
        checker = GEChecker(timeframe="1h", exchange="bybit", rows_removed=0)
        report  = checker.check(df, symbol="BTC/USDT")

    Thread-safety
    -------------
    No compartir instancias entre threads — el ValidationDefinition de GE
    usa estado mutable interno. Usar una instancia por invocación (factory).
    """

    def __init__(
        self,
        timeframe: str,
        exchange: str,
        rows_removed: int = 0,
    ) -> None:
        self._timeframe = timeframe
        self._exchange = exchange
        self._rows_removed = rows_removed
        self._context = self._build_context()

    # ── Port ────────────────────────────────────────────────────────────────

    def check(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
    ) -> "DataQualityReport":
        """
        Ejecuta la suite GE sobre df y retorna DataQualityReport.

        Fail-soft: ante cualquier error de GE, retorna reporte con
        un issue INTERNAL_ERROR de severidad warning para no bloquear
        el pipeline por fallo de observabilidad.
        """
        try:
            return self._run_ge(df, symbol=symbol)
        except Exception as exc:
            log.warning(
                "GEChecker: error inesperado ejecutando GE — retornando reporte degradado",
                exc_info=exc,
            )
            return self._degraded_report(symbol=symbol, df=df, error=exc)

    # ── Internos ─────────────────────────────────────────────────────────────

    def _build_context(self) -> Any:
        """Construye un DataContext efímero de GE (sin filesystem)."""
        import great_expectations as gx

        return gx.get_context(mode="ephemeral")

    def _run_ge(
        self,
        df: pd.DataFrame,
        *,
        symbol: str,
    ) -> "DataQualityReport":
        import great_expectations as gx

        from market_data.infrastructure.quality.ge_suite import build_ohlcv_suite

        ctx = self._context

        # ── 1. Preparar DataFrame (resetear índice temporal → columna) ──
        df_ge = self._prepare_dataframe(df)

        # ── 2. Datasource + Asset + BatchDefinition ──────────────────────
        if _GE_DATASOURCE_NAME not in [s.name for s in ctx.data_sources.all()]:
            source = ctx.data_sources.add_pandas(_GE_DATASOURCE_NAME)
            asset = source.add_dataframe_asset(_GE_ASSET_NAME)
            _batch_def = asset.add_batch_definition_whole_dataframe(_GE_BATCH_DEF_NAME)
        else:
            source = ctx.data_sources.get(_GE_DATASOURCE_NAME)
            asset = source.get_asset(_GE_ASSET_NAME)
            _batch_def = asset.get_batch_definition(_GE_BATCH_DEF_NAME)

        # ── 3. Suite ─────────────────────────────────────────────────────
        try:
            suite = ctx.suites.get("ohlcv_silver_suite")
        except Exception:
            suite = build_ohlcv_suite(ctx)

        # ── 4. ValidationDefinition + ejecución ──────────────────────────
        try:
            val_def = ctx.validation_definitions.get(_GE_VALIDATION_NAME)
        except Exception:
            val_def = ctx.validation_definitions.add(
                gx.ValidationDefinition(
                    name=_GE_VALIDATION_NAME,
                    data=_batch_def,
                    suite=suite,
                )
            )

        ge_result = val_def.run(batch_parameters={"dataframe": df_ge})

        # ── 5. Traducción GE → DataQualityReport ─────────────────────────
        issues = self._translate_results(ge_result)
        return self._build_report(df=df, symbol=symbol, issues=issues)

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Prepara el DataFrame para GE:
        - Resetea el índice DatetimeIndex → columna '_ts_index' (int64 epoch ms)
          para que GE pueda validar unicidad y monotonía.
        - Copia superficial para no mutar el original (inmutabilidad de dominio).
        """
        df_copy = df.copy(deep=False)
        if df_copy.index.name or isinstance(df_copy.index, pd.DatetimeIndex):
            df_copy = df_copy.reset_index()
            ts_col = df_copy.columns[0]  # primer col = índice original
            df_copy["_ts_index"] = pd.to_datetime(df_copy[ts_col]).astype("int64")
            df_copy = df_copy.drop(columns=[ts_col])
        # Forzar float64 en columnas OHLCV para expectativas de tipo
        for col in ("open", "high", "low", "close", "volume"):
            if col in df_copy.columns:
                df_copy[col] = df_copy[col].astype("float64", errors="ignore")
        return df_copy

    def _translate_results(self, ge_result: Any) -> list[_GEIssue]:
        """
        Traduce ExpectationValidationResult de GE a lista de _GEIssue.

        Solo procesa expectativas fallidas (success=False).
        """
        issues: list[_GEIssue] = []

        results = getattr(ge_result, "results", [])
        if not results:
            # GX 1.x puede retornar resultado con estructura diferente
            results = self._extract_results_gx1(ge_result)

        for res in results:
            if self._is_success(res):
                continue

            exp_type = self._get_expectation_type(res)
            col = self._get_column(res)
            counts = self._get_counts(res)
            severity = "critical" if exp_type in _CRITICAL_EXPECTATIONS else "warning"

            issues.append(
                _GEIssue(
                    expectation_type=exp_type,
                    column=col,
                    element_count=counts["element_count"],
                    unexpected_count=counts["unexpected_count"],
                    unexpected_rate=counts["unexpected_rate"],
                    severity=severity,
                    message=self._format_message(exp_type, col, counts),
                )
            )

        return issues

    def _build_report(
        self,
        df: pd.DataFrame,
        symbol: str,
        issues: list[_GEIssue],
    ) -> "DataQualityReport":
        """
        Construye DataQualityReport desde issues GE traducidos.

        Traducción de severidad GE → dominio:
          _GEIssue.severity == "critical" → QualityIssue de alta severidad
          _GEIssue.severity == "warning"  → QualityIssue de media severidad
        """
        from market_data.domain.quality.types import DataQualityReport, QualityIssue

        domain_issues = [
            QualityIssue(
                check=issue.expectation_type,
                severity=issue.severity,
                description=issue.message,
                affected_rows=issue.unexpected_count,
                details={
                    "column": issue.column,
                    "unexpected_rate": issue.unexpected_rate,
                    "source": "great_expectations",
                },
            )
            for issue in issues
        ]

        import datetime as _dt

        from market_data.domain.quality.types import _get_git_hash

        return DataQualityReport(
            symbol=symbol,
            timeframe=self._timeframe,
            exchange=self._exchange,
            rows=len(df),
            checked_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
            git_hash=_get_git_hash(),
            issues=domain_issues,
        )

    def _degraded_report(
        self,
        symbol: str,
        df: pd.DataFrame,
        error: Exception,
    ) -> "DataQualityReport":
        """Reporte mínimo de fallback cuando GE falla internamente."""
        import datetime as _dt

        from market_data.domain.quality.types import DataQualityReport, QualityIssue, _get_git_hash

        return DataQualityReport(
            symbol=symbol,
            timeframe=self._timeframe,
            exchange=self._exchange,
            rows=len(df),
            checked_at=_dt.datetime.now(_dt.timezone.utc).isoformat(),
            git_hash=_get_git_hash(),
            issues=[
                QualityIssue(
                    check="ge_internal_error",
                    severity="warning",
                    description=f"GE checker error: {type(error).__name__}: {error}",
                    affected_rows=0,
                    details={"source": "great_expectations"},
                )
            ],
        )

    # ── Helpers de parseo de resultados GE (multi-versión) ───────────────────

    def _extract_results_gx1(self, ge_result: Any) -> list[Any]:
        """Extrae results de GX 1.x si la estructura top-level difiere."""
        # GX 1.x: result.suite_results o result.run_results
        for attr in ("suite_results", "run_results", "expectation_results"):
            val = getattr(ge_result, attr, None)
            if val is not None:
                if isinstance(val, dict):
                    return list(val.values())
                if isinstance(val, list):
                    return val
        return []

    def _is_success(self, res: Any) -> bool:
        return bool(getattr(res, "success", True))

    def _get_expectation_type(self, res: Any) -> str:
        cfg = getattr(res, "expectation_config", None)
        if cfg:
            return getattr(cfg, "expectation_type", getattr(cfg, "type", "unknown"))
        return "unknown"

    def _get_column(self, res: Any) -> str | None:
        cfg = getattr(res, "expectation_config", None)
        if cfg:
            kwargs = getattr(cfg, "kwargs", getattr(cfg, "column", {}))
            if isinstance(kwargs, dict):
                return kwargs.get("column")
        return None

    def _get_counts(self, res: Any) -> dict:
        result_dict = getattr(res, "result", {}) or {}
        if not isinstance(result_dict, dict):
            result_dict = {}
        return {
            "element_count": result_dict.get("element_count", 0),
            "unexpected_count": result_dict.get("unexpected_count", 0),
            "unexpected_rate": result_dict.get("unexpected_percent", 0.0) / 100.0,
        }

    def _format_message(
        self,
        exp_type: str,
        column: str | None,
        counts: dict,
    ) -> str:
        col_str = f"[{column}]" if column else ""
        rate = counts["unexpected_rate"]
        n = counts["unexpected_count"]
        return f"{exp_type}{col_str}: {n} valores inesperados ({rate:.1%})"


# ---------------------------------------------------------------------------
# Factory — Composition Root la inyecta en QualityPipeline
# ---------------------------------------------------------------------------


def ge_checker_factory(
    timeframe: str,
    exchange: str,
    rows_removed: int,
) -> GEChecker:
    """
    Factory que satisface CheckerFactory type alias.

    Registrar en pipeline_factory.py:
        from market_data.infrastructure.quality.ge_checker import ge_checker_factory
        pipeline = QualityPipeline(checker_factory=ge_checker_factory)
    """
    return GEChecker(
        timeframe=timeframe,
        exchange=exchange,
        rows_removed=rows_removed,
    )


__all__ = ["GEChecker", "ge_checker_factory"]
