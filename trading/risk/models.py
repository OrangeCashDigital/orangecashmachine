# -*- coding: utf-8 -*-
"""
trading/risk/models.py
=======================

Modelos Pydantic de configuración de riesgo.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

from pathlib import Path
from typing import Union

from pydantic import BaseModel, Field, model_validator


class PositionConfig(BaseModel):
    """Límites de posición."""
    max_position_pct:   float = Field(0.05, gt=0.0, le=1.0)
    max_open_positions: int   = Field(3,    ge=1)


class StopLossConfig(BaseModel):
    """Configuración de stop-loss."""
    enabled:     bool  = True
    default_pct: float = Field(0.02, gt=0.0, lt=1.0)


class DrawdownConfig(BaseModel):
    """Límites de drawdown."""
    max_daily_drawdown_pct: float = Field(0.05, gt=0.0, lt=1.0)
    max_total_drawdown_pct: float = Field(0.15, gt=0.0, lt=1.0)
    halt_on_breach:         bool  = True


class OrderLimits(BaseModel):
    """Límites de tamaño de orden en USD."""
    min_order_usd: float = Field(10.0,   ge=0.0)
    max_order_usd: float = Field(1000.0, gt=0.0)

    @model_validator(mode="after")
    def _min_lt_max(self) -> "OrderLimits":
        if self.min_order_usd >= self.max_order_usd:
            raise ValueError(
                f"min_order_usd ({self.min_order_usd}) "
                f"must be < max_order_usd ({self.max_order_usd})"
            )
        return self


class SignalFilterConfig(BaseModel):
    """
    Filtros aplicados a señales antes de llegar al RiskManager.

    Separado de RiskConfig (SRP) — la confianza mínima es una
    concern de la señal, no de la gestión de riesgo financiero.
    """
    min_confidence: float = Field(0.8, ge=0.0, le=1.0)


class RiskConfig(BaseModel):
    """
    Configuración completa de riesgo.

    Compatible con config/risk/risk.yaml:
        risk:
          position:  {...}
          stop_loss: {...}
          drawdown:  {...}
          order:     {...}
          signal_filter: {...}   # opcional — defaults si ausente

    Uso desde YAML:
        cfg = RiskConfig.from_yaml("config/risk/risk.yaml")

    Uso directo:
        cfg = RiskConfig()   # usa defaults
    """
    position:      PositionConfig    = Field(default_factory=PositionConfig)
    stop_loss:     StopLossConfig    = Field(default_factory=StopLossConfig)
    drawdown:      DrawdownConfig    = Field(default_factory=DrawdownConfig)
    order:         OrderLimits       = Field(default_factory=OrderLimits)
    signal_filter: SignalFilterConfig = Field(default_factory=SignalFilterConfig)

    # ------------------------------------------------------------------
    # Backwards compat — delegado a signal_filter
    # ------------------------------------------------------------------

    @property
    def min_confidence(self) -> float:
        """Alias legacy — usar signal_filter.min_confidence directamente."""
        return self.signal_filter.min_confidence

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "RiskConfig":
        """
        Carga desde YAML. Soporta estructura con clave raíz ``risk:``.

        SafeOps: retorna defaults con warning ante cualquier error de I/O
        o parsing. Nunca lanza.
        """
        import yaml
        from loguru import logger

        try:
            raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
            return cls.model_validate(cls._extract_root(raw))
        except FileNotFoundError:
            logger.warning("RiskConfig: {} no encontrado — usando defaults", path)
        except PermissionError:
            logger.error("RiskConfig: sin permisos para leer {} — usando defaults", path)
        except yaml.YAMLError as exc:
            logger.error("RiskConfig: YAML inválido en {} | {} — usando defaults", path, exc)
        except Exception as exc:
            logger.error("RiskConfig: error inesperado cargando {} | {} — usando defaults", path, exc)
        return cls()

    @classmethod
    def from_dict(cls, data: dict) -> "RiskConfig":
        """Carga desde dict (OmegaConf / Hydra compatible)."""
        return cls.model_validate(cls._extract_root(data))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_root(raw: object) -> dict:
        """
        Extrae el bloque interior si el YAML/dict tiene clave raíz ``risk:``.

        DRY: único punto de normalización para from_yaml y from_dict.
        """
        if isinstance(raw, dict):
            return raw.get("risk", raw)
        return {}
