# -*- coding: utf-8 -*-
"""
trading/risk/models.py
=======================

Modelos Pydantic de configuracion de riesgo.

Principios: SOLID · KISS · DRY · SafeOps
"""
from __future__ import annotations

from pathlib import Path
from typing import Union

from pydantic import BaseModel, Field, model_validator


class PositionConfig(BaseModel):
    """Limites de posicion."""
    max_position_pct:   float = Field(0.05, gt=0.0, le=1.0)
    max_open_positions: int   = Field(3,    ge=1)


class StopLossConfig(BaseModel):
    """Configuracion de stop-loss."""
    enabled:     bool  = True
    default_pct: float = Field(0.02, gt=0.0, lt=1.0)


class DrawdownConfig(BaseModel):
    """Limites de drawdown."""
    max_daily_drawdown_pct: float = Field(0.05, gt=0.0, lt=1.0)
    max_total_drawdown_pct: float = Field(0.15, gt=0.0, lt=1.0)
    halt_on_breach:         bool  = True


class OrderLimits(BaseModel):
    """Limites de tamano de orden en USD."""
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
    Filtros aplicados a senales antes de llegar al RiskManager.

    Separado de RiskConfig (SRP) -- la confianza minima es una
    concern de la senal, no de la gestion de riesgo financiero.
    """
    min_confidence: float = Field(0.8, ge=0.0, le=1.0)


class RiskConfig(BaseModel):
    """
    Configuracion completa de riesgo.

    Compatible con config/risk/risk.yaml:
        risk:
          position:  {...}
          stop_loss: {...}
          drawdown:  {...}
          order:     {...}
          signal_filter: {...}

    Shortcuts de construccion directa (tests y CLI):
        RiskConfig(min_confidence=0.7)
        RiskConfig(position=PositionConfig(max_open_positions=2))
    """
    position:      PositionConfig     = Field(default_factory=PositionConfig)
    stop_loss:     StopLossConfig     = Field(default_factory=StopLossConfig)
    drawdown:      DrawdownConfig     = Field(default_factory=DrawdownConfig)
    order:         OrderLimits        = Field(default_factory=OrderLimits)
    signal_filter: SignalFilterConfig = Field(default_factory=SignalFilterConfig)

    model_config = {"populate_by_name": True}

    def __init__(self, **data):
        min_confidence  = data.pop("min_confidence",  None)
        super().__init__(**data)
        if min_confidence is not None:
            self.signal_filter = SignalFilterConfig(min_confidence=min_confidence)

    @property
    def min_confidence(self) -> float:
        """SSOT: delegado a signal_filter.min_confidence."""
        return self.signal_filter.min_confidence


    @classmethod
    def from_yaml(cls, path: Union[str, Path]) -> "RiskConfig":
        """
        Carga desde YAML. Soporta estructura con clave raiz ``risk:``.

        SafeOps: retorna defaults con warning ante cualquier error de I/O
        o parsing. Nunca lanza.
        """
        import yaml
        from loguru import logger

        try:
            raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
            return cls.model_validate(cls._extract_root(raw))
        except FileNotFoundError:
            logger.warning("RiskConfig: {} no encontrado -- usando defaults", path)
        except PermissionError:
            logger.error("RiskConfig: sin permisos para leer {} -- usando defaults", path)
        except yaml.YAMLError as exc:
            logger.error("RiskConfig: YAML invalido en {} | {} -- usando defaults", path, exc)
        except Exception as exc:
            logger.error("RiskConfig: error inesperado cargando {} | {} -- usando defaults", path, exc)
        return cls()

    @classmethod
    def from_dict(cls, data: dict) -> "RiskConfig":
        """Carga desde dict (OmegaConf / Hydra compatible)."""
        return cls.model_validate(cls._extract_root(data))

    @staticmethod
    def _extract_root(raw: object) -> dict:
        """
        Extrae el bloque interior si el YAML/dict tiene clave raiz ``risk:``.

        DRY: unico punto de normalizacion para from_yaml y from_dict.
        """
        if isinstance(raw, dict):
            return raw.get("risk", raw)
        return {}
