# -*- coding: utf-8 -*-
"""
trading/strategies/registry.py
================================

Plugin system para estrategias de trading.

Diseño
------
Registro global de estrategias identificadas por nombre string.
Permite desacoplar completamente la configuración de la instanciación:
el YAML dice ``strategy: ema_crossover``, el registry devuelve la clase.

Dos formas de registro:
  1. Decorador   → @register_strategy("my_strategy")
  2. Función     → register_strategy("name", MyStrategyClass)

Uso
---
    # Registrar (en el módulo de la estrategia):
    @register_strategy("ema_crossover")
    class EMACrossoverStrategy(BaseStrategy): ...

    # Instanciar desde config:
    cls    = StrategyRegistry.get("ema_crossover")
    strat  = cls(symbol="BTC/USDT", timeframe="1h", fast_period=9)

    # Listar disponibles:
    names  = StrategyRegistry.list()

SafeOps
-------
- get() con nombre desconocido lanza StrategyNotFoundError explícito.
- El registro es thread-safe.
- Estrategias built-in se auto-registran al importar este módulo.

Principios: SOLID · KISS · DRY · SafeOps · OCP
"""
from __future__ import annotations

import threading
from typing import Callable, Type, Union, overload

from trading.strategies.base import BaseStrategy


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class StrategyNotFoundError(KeyError):
    """Estrategia no registrada."""
    def __init__(self, name: str, available: list[str]) -> None:
        self.name      = name
        self.available = available
        super().__init__(
            f"Estrategia {name!r} no registrada. "
            f"Disponibles: {sorted(available)}"
        )


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class _StrategyRegistry:
    """
    Registro global de estrategias. Singleton interno — acceder via
    el módulo-level ``StrategyRegistry``.
    """

    def __init__(self) -> None:
        self._registry: dict[str, Type[BaseStrategy]] = {}
        self._lock = threading.Lock()

    def register(
        self,
        name:  str,
        cls:   Type[BaseStrategy],
    ) -> None:
        """Registra una clase de estrategia con un nombre string."""
        if not issubclass(cls, BaseStrategy):
            raise TypeError(
                f"register: {cls.__name__} no es subclase de BaseStrategy"
            )
        with self._lock:
            if name in self._registry:
                existing = self._registry[name]
                if existing is not cls:
                    raise ValueError(
                        f"Estrategia {name!r} ya registrada como "
                        f"{existing.__name__}. No se puede sobrescribir."
                    )
            self._registry[name] = cls

    def get(self, name: str) -> Type[BaseStrategy]:
        """Retorna la clase registrada. Lanza StrategyNotFoundError si no existe."""
        with self._lock:
            cls = self._registry.get(name)
            if cls is None:
                raise StrategyNotFoundError(name, list(self._registry))
            return cls

    def list(self) -> list[str]:
        """Retorna los nombres de todas las estrategias registradas."""
        with self._lock:
            return sorted(self._registry.keys())

    def __contains__(self, name: str) -> bool:
        with self._lock:
            return name in self._registry

    def __repr__(self) -> str:
        return f"StrategyRegistry(registered={self.list()})"


# Instancia global — única fuente de verdad
StrategyRegistry = _StrategyRegistry()


# ---------------------------------------------------------------------------
# Decorador / función de registro
# ---------------------------------------------------------------------------

@overload
def register_strategy(name: str) -> Callable[[Type[BaseStrategy]], Type[BaseStrategy]]: ...
@overload
def register_strategy(name: str, cls: Type[BaseStrategy]) -> Type[BaseStrategy]: ...

def register_strategy(
    name: str,
    cls:  Union[Type[BaseStrategy], None] = None,
) -> Union[Type[BaseStrategy], Callable]:
    """
    Registra una estrategia en el StrategyRegistry global.

    Uso como decorador:
        @register_strategy("ema_crossover")
        class EMACrossoverStrategy(BaseStrategy): ...

    Uso como función:
        register_strategy("ema_crossover", EMACrossoverStrategy)
    """
    if cls is not None:
        StrategyRegistry.register(name, cls)
        return cls

    def decorator(klass: Type[BaseStrategy]) -> Type[BaseStrategy]:
        StrategyRegistry.register(name, klass)
        return klass

    return decorator


# ---------------------------------------------------------------------------
# Auto-registro de estrategias built-in
# ---------------------------------------------------------------------------
# Importar aquí registra automáticamente al importar este módulo.
# Nuevas estrategias se añaden en su propio archivo + aquí (OCP).
# ---------------------------------------------------------------------------

def _register_builtins() -> None:
    # Auto-registra built-ins al importar este módulo.
    # try/except por estrategia: fallo parcial, no total (fail partial).
    # El error queda logueado y auditable — el registry sigue operativo.
    import importlib
    import logging

    _builtins = [
        ("ema_crossover", "trading.strategies.ema_crossover", "EMACrossoverStrategy"),
    ]
    for name, module_path, class_name in _builtins:
        try:
            mod = importlib.import_module(module_path)
            cls = getattr(mod, class_name)
            StrategyRegistry.register(name, cls)
        except Exception as exc:  # noqa: BLE001
            logging.getLogger(__name__).warning(
                "registry: builtin %r failed to load (%s: %s) — skipped",
                name, type(exc).__name__, exc,
            )


_register_builtins()


def bootstrap_strategies() -> None:
    """Bootstrap explícito del registry de estrategias.

    Llamar desde el entrypoint de la aplicación antes de usar
    StrategyRegistry.get(). Idempotente — seguro llamar múltiples veces.

    El auto-registro en import-time (_register_builtins) ya cubre el
    caso normal. Esta función existe para bootstrap explícito en:
      - entrypoints que necesitan garantía de orden
      - tests que quieren control total sobre el registry
    """
    _register_builtins()


__all__ = [
    "StrategyRegistry",
    "StrategyNotFoundError",
    "register_strategy",
    "bootstrap_strategies",
]
