# ==============================================================================
# OrangeCashMachine — Config Pipeline (State Machine)
# ==============================================================================
#
# PRECEDENCIA FORMAL (ley del sistema — no bypassear):
#
#   RAW YAML
#     │  [L1] Hydra compose  — merge de todos los YAMLs + resolve ${oc.env:*}
#     ↓
#   DictConfig
#     │  [L2] Env override   — OCM_* mutan claves específicas (post-Hydra)
#     ↓
#   DictConfig (mutated)
#     │  [L3] Coercion       — OmegaConf → plain Python dict
#     ↓
#   dict
#     │  [L4] Validation     — Pydantic valida tipos y estructura (SIN reglas)
#     ↓
#   AppConfig (typed)
#     │  [L5] Business rules — invariantes cross-field, fail-fast
#     ↓
#   AppConfig (frozen)   ← ÚNICO estado que usa la aplicación
#
# REGLAS INVARIANTES:
#   - Cada capa tiene UNA responsabilidad
#   - Pydantic NO inyecta defaults que sombreen valores de Hydra
#   - env_overrides corre DESPUÉS de Hydra, ANTES de Pydantic
#   - AppConfig es frozen — cero mutación post-L5
#   - Los errores de cada capa identifican LA capa que falló
# ==============================================================================

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from loguru import logger
from omegaconf import DictConfig, OmegaConf

# Helpers de coerción importados desde hydra_loader (DRY — SSOT allí).
# L3 los orquesta; la implementación vive junto a las constantes que usan
# (_HYDRA_INTERNAL, _NULLABLE_KEYS) para evitar dependencia circular.
from core.config.hydra_loader import _strip_hydra_internals, _normalize_empty_strings


class ConfigStage(Enum):
    """Etapas formales del pipeline. Cada una mapea a una responsabilidad."""
    RAW         = auto()   # Output de Hydra compose
    ENV_MUTATED = auto()   # Post OCM_* env overrides
    COERCED     = auto()   # Plain dict (post OmegaConf.to_container)
    VALIDATED   = auto()   # AppConfig Pydantic (tipos verificados)
    FROZEN      = auto()   # Inmutable — la app lee solo desde aquí


@dataclass(frozen=True)
class ConfigTransition:
    """Registro inmutable de una transición entre etapas."""
    from_stage: ConfigStage
    to_stage: ConfigStage
    mutations: int = 0   # cantidad de overrides/cambios aplicados en esta transición


class ConfigPipelineError(RuntimeError):
    """Error de pipeline — indica en qué capa ocurrió el fallo."""
    def __init__(self, stage: ConfigStage, message: str, cause: Exception | None = None) -> None:
        self.stage = stage
        super().__init__(f"[ConfigPipeline:{stage.name}] {message}")
        if cause:
            self.__cause__ = cause


class ConfigPipeline:
    """
    Pipeline formal de configuración.

    Uso:
        # Dentro de @hydra.main:
        pipeline = ConfigPipeline(cfg)
        app_config = pipeline.run()

    Diseño:
        - Fail-fast en cualquier capa que viole su contrato
        - Trazabilidad: cada transición queda logueada con su stage
        - Orden inmutable: el pipeline no puede reordenar capas
        - Sin efectos secundarios: no muta el DictConfig original de Hydra
    """

    def __init__(self, raw_cfg: DictConfig) -> None:
        self._raw = raw_cfg
        self._transitions: list[ConfigTransition] = []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> Any:  # → AppConfig
        """
        Ejecuta el pipeline completo.
        Retorna AppConfig frozen o lanza ConfigPipelineError con el stage que falló.
        """
        cfg = self._l1_hydra_compose()
        cfg, mutations = self._l2_env_override(cfg)
        raw_dict = self._l3_coerce(cfg)
        app_config = self._l4_validate(raw_dict)
        app_config = self._l5_business_rules(app_config)

        logger.info(
            "config_pipeline_complete | env={} dry_run={} transitions={}",
            getattr(getattr(app_config, "environment", None), "name", "?"),
            getattr(getattr(app_config, "safety", None), "dry_run", "?"),
            len(self._transitions),
        )
        return app_config

    @property
    def transitions(self) -> list[ConfigTransition]:
        """Historial de transiciones del pipeline (inmutable post-run)."""
        return list(self._transitions)

    # ------------------------------------------------------------------
    # L1: Hydra compose (ya ocurrió — solo trazamos)
    # ------------------------------------------------------------------

    def _l1_hydra_compose(self) -> DictConfig:
        """
        L1: Hydra compose.
        La composición ya ocurrió en @hydra.main.
        Este paso valida que el DictConfig sea usable y lo traza.
        """
        try:
            top_level_keys = list(self._raw.keys())
            logger.debug("config_pipeline_l1 | keys_count={} top_keys={}", len(top_level_keys), top_level_keys)
            self._record(ConfigStage.RAW, ConfigStage.ENV_MUTATED)
            return self._raw
        except Exception as exc:
            raise ConfigPipelineError(ConfigStage.RAW, "DictConfig inaccesible", exc) from exc

    # ------------------------------------------------------------------
    # L2: OCM_* env override (mutation layer)
    # ------------------------------------------------------------------

    def _l2_env_override(self, cfg: DictConfig) -> tuple[DictConfig, int]:
        """
        L2: Env override.
        OCM_* mutan el DictConfig DESPUÉS de Hydra, ANTES de Pydantic.
        Esta es la ÚNICA fuente autorizada de mutación runtime.
        """
        try:
            from core.config.layers.env_override import apply_env_overrides
            mutated, mutations = apply_env_overrides(cfg)
            logger.debug("config_pipeline_l2 | ocm_overrides={}", mutations)
            self._record(ConfigStage.ENV_MUTATED, ConfigStage.COERCED, mutations)
            return mutated, mutations
        except Exception as exc:
            raise ConfigPipelineError(ConfigStage.ENV_MUTATED, str(exc), exc) from exc

    # ------------------------------------------------------------------
    # L3: OmegaConf → dict (coercion layer)
    # ------------------------------------------------------------------

    def _l3_coerce(self, cfg: DictConfig) -> dict:
        """
        L3: Coerción de tipos.
        OmegaConf → plain Python dict + limpieza de artefactos Hydra.

        Responsabilidades (SSOT de coerción):
          - OmegaConf.to_container: resuelve interpolaciones, convierte a dict nativo
          - _strip_hydra_internals: elimina claves _hydra_* que Hydra inyecta
          - _normalize_empty_strings: convierte "" → None en claves nullable (password, user, db)
          - throw_on_missing=True: fail-fast si algún ${oc.env:VAR} no está seteado
        """
        try:
            raw_dict = OmegaConf.to_container(cfg, resolve=True, throw_on_missing=True)
            _strip_hydra_internals(raw_dict)   # muta in-place — no reasignar
            _normalize_empty_strings(raw_dict)  # muta in-place — no reasignar
            logger.debug("config_pipeline_l3 | coerce=ok keys={}", list(raw_dict.keys()))
            self._record(ConfigStage.COERCED, ConfigStage.VALIDATED)
            return raw_dict  # type: ignore[return-value]
        except Exception as exc:
            raise ConfigPipelineError(
                ConfigStage.COERCED,
                "OmegaConf.to_container falló — variable de entorno faltante o interpolación rota",
                exc,
            ) from exc

    # ------------------------------------------------------------------
    # L4: Pydantic validation (validation only — no business rules)
    # ------------------------------------------------------------------

    def _l4_validate(self, raw_dict: dict) -> Any:
        """
        L4: Validación Pydantic.
        Responsabilidad ÚNICA: tipos y estructura.
        SIN reglas de negocio (esas van en L5).
        """
        try:
            from core.config.layers.validation import validate_and_coerce
            app_config = validate_and_coerce(raw_dict)
            logger.debug("config_pipeline_l4 | pydantic=ok")
            self._record(ConfigStage.VALIDATED, ConfigStage.FROZEN)
            return app_config
        except Exception as exc:
            raise ConfigPipelineError(ConfigStage.VALIDATED, str(exc), exc) from exc

    # ------------------------------------------------------------------
    # L5: Business rules (cross-field invariants)
    # ------------------------------------------------------------------

    def _l5_business_rules(self, app_config: Any) -> Any:
        """
        L5: Reglas de negocio.
        Invariantes cross-field que Pydantic no puede expresar.
        Fail-fast: cualquier violación aborta el startup.
        """
        try:
            from core.config.layers.rules import apply_business_rules
            apply_business_rules(app_config)
            logger.debug("config_pipeline_l5 | rules=ok")
            return app_config
        except Exception as exc:
            raise ConfigPipelineError(ConfigStage.FROZEN, str(exc), exc) from exc

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _record(
        self,
        from_stage: ConfigStage,
        to_stage: ConfigStage,
        mutations: int = 0,
    ) -> None:
        self._transitions.append(ConfigTransition(from_stage, to_stage, mutations))
