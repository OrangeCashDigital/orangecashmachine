"""
market_data/safety/environment_validator.py
=============================================

Validación de coherencia de entorno antes de ejecutar pipelines.

Diseño
------
Solo valida lo que es verificable localmente (sin red):
- Coherencia env string vs credenciales
- Exchanges habilitados sin credenciales en producción
- test_symbol explícito en producción (evita defaults genéricos)

Los checks que requieren red (¿es testnet real? ¿responde el exchange?)
son responsabilidad de validate_exchange_connection, que ya existe.

Principios
----------
KISS   — reglas mínimas y explícitas, sin heurísticas complejas
SOLID  — SRP: solo coherencia de entorno, no lógica de negocio
SafeOps — siempre loggea antes de lanzar

Uso
---
    validator = EnvironmentValidator()
    validator.check(config, run_cfg)   # lanza EnvironmentMismatchError si incoherente
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from core.config.schema import AppConfig
    from core.config.runtime import RunConfig

__all__ = ["EnvironmentValidator", "EnvironmentMismatchError"]


class EnvironmentMismatchError(RuntimeError):
    """
    Incoherencia crítica entre entorno y configuración.

    Se lanza antes de ejecutar cualquier pipeline para evitar
    operaciones peligrosas (ej: producción sin credenciales).
    """


class EnvironmentValidator:
    """
    Valida coherencia de entorno antes de ejecutar pipelines.

    Checks implementados (todos locales, sin red)
    ---------------------------------------------
    1. En producción: todos los exchanges habilitados deben tener credenciales.
    2. En producción: no debe haber exchanges con test_symbol genérico ("BTC/USDT")
       sin haberlo configurado explícitamente — esto indica config copiada de dev.
    3. En cualquier entorno: al menos un exchange debe estar habilitado.

    Extensión
    ---------
    Añadir checks aquí, no en run_application ni en adapters.
    Cada check es un método privado _check_* que lanza EnvironmentMismatchError.
    """

    _GENERIC_TEST_SYMBOLS = frozenset({"BTC/USDT", "ETH/USDT"})
    _PRODUCTION_ENVS      = frozenset({"production", "prod"})

    def check(self, config: "AppConfig", run_cfg: "RunConfig") -> None:
        """
        Ejecuta todos los checks de entorno.

        Falla rápido en el primer problema crítico encontrado.
        Loggea cada check para auditabilidad.

        Raises
        ------
        EnvironmentMismatchError — si algún check falla.
        """
        log = logger.bind(
            env=run_cfg.env,
            component="EnvironmentValidator",
        )

        log.debug("environment_validation_start", env=run_cfg.env)

        self._check_exchanges_enabled(config, run_cfg, log)

        if run_cfg.env in self._PRODUCTION_ENVS:
            self._check_production_credentials(config, log)
            self._check_production_test_symbols(config, log)
            self._check_storage_path(config, log)

        log.info("environment_validation_passed", env=run_cfg.env)

    # ------------------------------------------------------------------ #
    # Checks privados
    # ------------------------------------------------------------------ #

    @staticmethod
    def _check_exchanges_enabled(config: "AppConfig", run_cfg: "RunConfig", log) -> None:
        """Al menos un exchange debe estar configurado."""
        exchanges = getattr(config, "exchanges", [])
        if not exchanges:
            log.critical(
                "environment_check_failed",
                check="exchanges_enabled",
                reason="No exchanges configured",
            )
            raise EnvironmentMismatchError(
                f"No exchanges configured for env='{run_cfg.env}'. "
                "Check your config file."
            )

    @staticmethod
    def _check_production_credentials(config: "AppConfig", log) -> None:
        """En producción, todos los exchanges habilitados deben tener credenciales."""
        exchanges = getattr(config, "exchanges", [])
        missing = [
            exc.name.value
            for exc in exchanges
            if not getattr(exc, "has_credentials", False)
        ]
        if missing:
            log.critical(
                "environment_check_failed",
                check="production_credentials",
                exchanges_missing_credentials=missing,
            )
            raise EnvironmentMismatchError(
                f"Production environment requires credentials for all exchanges. "
                f"Missing: {missing}"
            )

    def _check_production_test_symbols(self, config: "AppConfig", log) -> None:
        """
        En producción, test_symbol genérico indica config copiada de dev.

        Un test_symbol genérico (BTC/USDT) no es peligroso per se,
        pero en futures puede causar el mismo bug P0: mismatch entre
        el símbolo configurado y el universo de mercados cargado.
        """
        exchanges = getattr(config, "exchanges", [])
        suspicious = [
            exc.name.value
            for exc in exchanges
            if getattr(exc, "test_symbol", None) in self._GENERIC_TEST_SYMBOLS
            and getattr(exc.markets, "futures", None) is not None
            and getattr(exc.markets.futures, "enabled", False)
        ]
        if suspicious:
            log.warning(
                "environment_check_warning",
                check="production_test_symbols",
                reason="Generic test_symbol on futures exchange — may cause market_type mismatch",
                exchanges=suspicious,
            )
            # Warning, no error — el fix real está en _select_test_symbol
            # pero queremos visibilidad en producción

    @staticmethod
    def _check_storage_path(config: "AppConfig", log) -> None:
        """
        En producción, el storage path NO debe apuntar a local_data_lake
        ni a data_platform/. Indica config de desarrollo sin sobrescribir.
        """
        storage = getattr(config, "storage", None)
        data_lake = getattr(storage, "data_lake", None) if storage else None
        path_str = str(getattr(data_lake, "path", ""))
        forbidden = ("local_data_lake", "data_platform/data_lake")
        if any(fragment in path_str for fragment in forbidden):
            log.critical(
                "environment_check_failed",
                check="storage_path",
                storage_path=path_str,
                reason="Production storage path looks like a development path",
            )
            raise EnvironmentMismatchError(
                f"Production storage path '{path_str}' contains a development fragment. "
                "Set DATA_LAKE_PATH to a production path (e.g. /var/lib/orangecashmachine/data_lake)."
            )
