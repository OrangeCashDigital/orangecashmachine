# -*- coding: utf-8 -*-
"""
portfolio.bootstrap.composition_root
=====================================
Composition Root único y formal para el bounded context portfolio.

Responsabilidad única
----------------------
Este módulo es el ÚNICO punto donde se decide qué implementación
concreta de PositionStore se inyecta en PortfolioService. Toda la
decisión de cableado vive aquí.

Ningún módulo fuera de bootstrap/ puede instanciar RedisPositionStore
o InMemoryPositionStore directamente — enforced por import-linter.

Principios aplicados
---------------------
DIP      — PortfolioService y RebalanceService reciben abstracciones
            (PositionStore Protocol); CompositionRoot las conecta con
            implementaciones concretas.
SRP      — Una sola razón para cambiar: qué implementación se usa.
KISS     — API pública: CompositionRoot.assemble(config).
Fail-Fast — Valida AppConfig antes de instanciar cualquier adaptador.
SafeOps  — Redis deshabilitado → InMemoryPositionStore, sin lanzar.
SSOT     — La decisión Redis/InMemory usa integrations.redis.enabled,
            la misma bandera que ya gobierna el cursor store —
            no se introduce una bandera paper/live redundante.

Referencia
----------
Seemann, Mark. «Dependency Injection in .NET», capítulo Composition Root.
Martin, Robert C. «Clean Architecture», capítulo 26.

Contratos enforced: BC-43 (PositionStore adapters solo instanciables
desde portfolio/bootstrap/composition_root) y BC-44 (portfolio layer
order) en architecture/importlinter.toml.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ocm.config.schema import AppConfig
    from portfolio.ports.position_store import PositionStore
    from portfolio.services.portfolio_service import PortfolioService
    from portfolio.services.rebalance_service import RebalanceService

__all__ = ["CompositionRoot", "assemble"]


@dataclass(frozen=True, slots=True)
class CompositionRoot:
    """
    Grafo de dependencias ensamblado para portfolio.

    Inmutable tras construcción (frozen=True) — garantiza que nadie
    puede inyectar dependencias distintas después del arranque.

    Uso canónico
    ------------
        config = load_config()
        root   = CompositionRoot.assemble(config)
        root.portfolio_service.open_position(...)
        signals = root.rebalance_service.rebalance(
            root.portfolio_service.snapshot(), targets
        )
    """

    portfolio_service: "PortfolioService"
    rebalance_service: "RebalanceService"
    redis_client: Any = None

    @classmethod
    def assemble(
        cls,
        config: "AppConfig",
        *,
        capital_usd_override: float | None = None,
    ) -> "CompositionRoot":
        """
        Ensambla el grafo completo de dependencias para portfolio.

        Fail-Fast: valida config antes de instanciar cualquier adaptador.
        Si AppConfig está incompleto, falla aquí — no en el primer request.

        Args:
            config: AppConfig validado por el pipeline L1-L5 de ocm.config.
            capital_usd_override: si se provee, gana sobre
                config.portfolio.capital_usd. Existe para callers como
                live trading donde el capital NO debe tomar el default
                de AppConfig por SafeOps (ver app/cli/live_hydra.py) —
                None preserva el comportamiento SSOT existente para
                todos los demás callers.

        Returns:
            CompositionRoot inmutable listo para producción.

        Raises:
            ValueError: si config es None.
        """
        if config is None:
            raise ValueError(
                "CompositionRoot.assemble() requiere AppConfig no-nula. "
                "El pipeline de config (L1-L5) debe completar antes de ensamblar."
            )

        store, redis_client = cls.build_position_store(config)

        from portfolio.services.portfolio_service import PortfolioService

        capital_usd = capital_usd_override if capital_usd_override is not None else config.portfolio.capital_usd
        portfolio_service = PortfolioService(
            capital_usd=capital_usd,
            store=store,
            exchange=config.portfolio.exchange,
        )

        from portfolio.services.rebalance_service import RebalanceService

        rebalance_service = RebalanceService(
            drift_threshold=config.portfolio.rebalance_drift_threshold,
            min_delta_pct=config.portfolio.rebalance_min_delta_pct,
        )

        return cls(
            portfolio_service=portfolio_service,
            rebalance_service=rebalance_service,
            redis_client=redis_client,
        )

    @classmethod
    def build_position_store(cls, config: "AppConfig") -> tuple["PositionStore", Any]:
        """
        Decide RedisPositionStore vs InMemoryPositionStore.

        SSOT de la decisión: config.integrations.redis.enabled — la misma
        bandera que ya gobierna el cursor store de market_data. No se
        introduce una bandera nueva (paper/live) para no duplicar SSOT.

        SafeOps: si Redis está deshabilitado, retorna InMemoryPositionStore
        sin intentar conectar. Nunca lanza por ausencia de Redis.

        Args:
            config: AppConfig con integrations.redis configurado.

        Returns:
            PositionStore listo para inyectar en PortfolioService.
        """
        from loguru import logger

        redis_cfg = config.integrations.redis

        if not redis_cfg.enabled:
            logger.info("[composition-root:portfolio] redis.enabled=false — usando InMemoryPositionStore")
            from portfolio.infra.memory_store import InMemoryPositionStore

            return InMemoryPositionStore(), None

        from portfolio.infra.redis_factory import build_redis_client
        from portfolio.infra.redis_store import RedisPositionStore

        redis_client = build_redis_client(
            host=redis_cfg.host,
            port=redis_cfg.port,
            db=redis_cfg.db,
            password=(redis_cfg.password.get_secret_value() if redis_cfg.password else None),
            socket_timeout=float(redis_cfg.socket_timeout),
        )

        logger.info("[composition-root:portfolio] redis.enabled=true — usando RedisPositionStore")
        return (
            RedisPositionStore(
                redis_client=redis_client,
                exchange=config.portfolio.exchange,
                ttl_seconds=config.portfolio.position_ttl_days * 24 * 3600,
            ),
            redis_client,
        )

    def close(self) -> None:
        """Cierra recursos externos abiertos por assemble() (Resiliencia/SafeOps).

        Fail-soft: nunca lanza -- un fallo cerrando no debe impedir que el
        caller complete su propio shutdown. No-op si no hubo Redis
        involucrado (InMemoryPositionStore).
        """
        if self.redis_client is None:
            return
        try:
            self.redis_client.close()
        except Exception as exc:
            from loguru import logger

            logger.warning("CompositionRoot.close: error cerrando redis_client | {}", exc)

    def __repr__(self) -> str:
        return (
            f"CompositionRoot(portfolio_service="
            f"{type(self.portfolio_service).__name__}, "
            f"rebalance_service={type(self.rebalance_service).__name__})"
        )


# ── Alias funcional ─────────────────────────────────────────────────────
def assemble(config: "AppConfig") -> CompositionRoot:
    """Shorthand de CompositionRoot.assemble(config)."""
    return CompositionRoot.assemble(config)
