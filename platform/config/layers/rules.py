# ==============================================================================
# OrangeCashMachine — L5: Business Rules Layer
# ==============================================================================
#
# RESPONSABILIDAD ÚNICA:
#   Invariantes cross-field que Pydantic field validators no pueden expresar
#   porque involucran múltiples secciones de config.
#
# PRINCIPIOS:
#   - Fail-fast: cualquier violación lanza ConfigRuleViolation → startup abortado
#   - Cada regla tiene nombre, descripción y mensaje de error accionable
#   - Las reglas son funciones puras — sin efectos secundarios
#   - Agregar reglas aquí, NUNCA en validators de Pydantic models
#
# CÓMO AGREGAR UNA REGLA:
#   1. Definir función _rule_<nombre>(cfg: AppConfig) -> None
#   2. Agregarla a apply_business_rules() en orden lógico
#   3. Documentar qué viola y cómo corregirlo en el mensaje de error
# ==============================================================================

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from ocm_platform.config.schema import AppConfig  # SSOT — schema.py, no models


class ConfigRuleViolation(RuntimeError):
    """
    Violación de regla de negocio en config.
    Siempre fail-fast — el sistema NO debe arrancar con config inválida.
    """
    def __init__(self, rule: str, message: str, fix: str) -> None:
        self.rule = rule
        self.fix = fix
        super().__init__(
            f"\n[ConfigRule:{rule}] {message}\n"
            f"  → FIX: {fix}"
        )


def apply_business_rules(cfg: "AppConfig") -> None:
    """
    Aplica todas las reglas de negocio en orden.
    Fail-fast: la primera violación aborta — no acumula errores.

    Para acumulación de errores en el futuro:
        Cambiar a lista + raise al final (pero fail-fast es correcto aquí).
    """
    _rule_production_never_dry_run(cfg)
    _rule_resample_source_in_historical(cfg)
    _rule_order_range_coherent(cfg)
    _rule_max_backfill_production(cfg)
    _rule_require_confirmation_in_prod(cfg)

    logger.debug("config_pipeline_l5 | rules=ok rules_verified=5")


# ------------------------------------------------------------------------------
# Reglas individuales
# ------------------------------------------------------------------------------

def _rule_production_never_dry_run(cfg: "AppConfig") -> None:
    """
    REGLA: production + dry_run=True es una contradicción peligrosa.

    dry_run=True en producción significa que el sistema parece operar
    pero silenciosamente NO escribe datos reales. Esto puede pasar
    desapercibido durante horas.
    """
    if cfg.environment.name == "production" and cfg.safety.dry_run:
        raise ConfigRuleViolation(
            rule="PRODUCTION_DRY_RUN",
            message=(
                "environment.name=production con safety.dry_run=True. "
                "Producción operaría en modo simulado silenciosamente."
            ),
            fix="Setear safety.dry_run=false en env/production.yaml o via CLI.",
        )


def _rule_resample_source_in_historical(cfg: "AppConfig") -> None:
    """
    REGLA: resample.source_tf debe estar en historical.timeframes.

    Si ResamplePipeline intenta resamplear desde 1m pero historical
    no descarga 1m, el pipeline produce datos vacíos silenciosamente.
    """
    source_tf = cfg.pipeline.resample.source_tf
    historical_tfs = cfg.pipeline.historical.timeframes

    if source_tf not in historical_tfs:
        raise ConfigRuleViolation(
            rule="RESAMPLE_SOURCE_CONSISTENCY",
            message=(
                f"pipeline.resample.source_tf={source_tf!r} "
                f"no está en pipeline.historical.timeframes={historical_tfs}. "
                "ResamplePipeline no tendría datos fuente."
            ),
            fix=(
                f"Agregar {source_tf!r} a pipeline.historical.timeframes "
                f"o cambiar resample.source_tf a uno de {historical_tfs}."
            ),
        )


def _rule_order_range_coherent(cfg: "AppConfig") -> None:
    """
    REGLA: max_order_usd debe ser estrictamente mayor que min_order_usd.
    """
    risk_order = cfg.risk.order
    if risk_order.max_order_usd <= risk_order.min_order_usd:
        raise ConfigRuleViolation(
            rule="ORDER_RANGE",
            message=(
                f"risk.order.max_order_usd ({risk_order.max_order_usd}) "
                f"no es mayor que min_order_usd ({risk_order.min_order_usd})."
            ),
            fix="Setear max_order_usd > min_order_usd en config/risk/risk.yaml.",
        )


def _rule_max_backfill_production(cfg: "AppConfig") -> None:
    """
    REGLA: producción no puede tener max_backfill_days > 90.
    Previene reingestiones masivas accidentales en producción.
    """
    if cfg.environment.name == "production" and cfg.safety.max_backfill_days > 90:
        raise ConfigRuleViolation(
            rule="PRODUCTION_BACKFILL_LIMIT",
            message=(
                f"safety.max_backfill_days={cfg.safety.max_backfill_days} "
                "supera el límite de 90 días en producción."
            ),
            fix="Reducir safety.max_backfill_days ≤ 90 en env/production.yaml.",
        )


def _rule_require_confirmation_in_prod(cfg: "AppConfig") -> None:
    """
    REGLA: producción debe tener require_confirmation=True.
    Protección contra ejecución accidental de operaciones destructivas.
    """
    if cfg.environment.name == "production" and not cfg.safety.require_confirmation:
        raise ConfigRuleViolation(
            rule="PRODUCTION_REQUIRE_CONFIRMATION",
            message="safety.require_confirmation=False en environment=production.",
            fix="Setear safety.require_confirmation=true en env/production.yaml.",
        )
