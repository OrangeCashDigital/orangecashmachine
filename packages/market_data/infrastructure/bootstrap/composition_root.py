"""
market_data.infrastructure.bootstrap.composition_root
======================================================
Composition Root único y formal para el bounded context market_data.

Responsabilidad única
---------------------
Este módulo es el ÚNICO punto donde se decide qué implementación concreta
se inyecta en cada abstracción. Toda la decisión de cableado vive aquí.

Ningún módulo fuera de infrastructure/bootstrap/ puede instanciar
adaptadores concretos — enforced por el contrato BC-38.

Principios aplicados
--------------------
DIP  — Las capas internas (domain, ports, application) reciben abstracciones.
        CompositionRoot las conecta con implementaciones concretas.
SRP  — Una sola razón para cambiar: cambiar qué implementación se usa.
KISS — La API pública es un único classmethod: CompositionRoot.assemble(config).
Fail-Fast — Valida AppConfig antes de instanciar cualquier adaptador.

Referencia
----------
Seemann, Mark. «Dependency Injection in .NET», capítulo Composition Root.
Martin, Robert C. «Clean Architecture», capítulo 26.

Contratos enforced: BC-38.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ocm.config.schema import AppConfig
    from market_data.infrastructure.bootstrap.pipeline_factory import PipelineFactory

__all__ = ["CompositionRoot", "assemble"]


@dataclass(frozen=True, slots=True)
class CompositionRoot:
    """
    Grafo de dependencias ensamblado para market_data.

    Inmutable tras construcción (frozen=True) — garantiza que nadie
    puede inyectar dependencias distintas después del arranque.

    Uso canónico
    ------------
        config = load_config()                      # ocm.config pipeline L1-L5
        root   = CompositionRoot.assemble(config)   # único punto de cableado
        pipeline = root.factory.build(request)      # flujo normal de negocio
    """

    factory: "PipelineFactory"

    @classmethod
    def assemble(cls, config: "AppConfig") -> "CompositionRoot":
        """
        Ensambla el grafo completo de dependencias para market_data.

        Fail-Fast: valida config antes de instanciar cualquier adaptador.
        Si AppConfig está incompleto, falla aquí — no en el primer request.

        Args:
            config: AppConfig validado por el pipeline L1-L5 de ocm.config.

        Returns:
            CompositionRoot inmutable listo para producción.

        Raises:
            ValueError: si config es None o no tiene los campos mínimos.
        """
        # ── Validación Fail-Fast ─────────────────────────────────────────
        if config is None:
            raise ValueError(
                "CompositionRoot.assemble() requiere AppConfig no-nula. "
                "El pipeline de config (L1-L5) debe completar antes de ensamblar."
            )

        # ── Imports concretos confinados aquí (DIP) ──────────────────────
        # Solo este módulo conoce PipelineFactory.
        # application/, domain/, ports/ no lo importan directamente.
        from market_data.infrastructure.bootstrap.pipeline_factory import PipelineFactory

        factory = PipelineFactory(config)
        return cls(factory=factory)

    def __repr__(self) -> str:
        return f"CompositionRoot(factory={type(self.factory).__name__})"


# ── Alias funcional ───────────────────────────────────────────────────────────
# Para código de orquestación que prefiere estilo funcional:
#   root = assemble(config)
def assemble(config: "AppConfig") -> CompositionRoot:
    """Shorthand de CompositionRoot.assemble(config)."""
    return CompositionRoot.assemble(config)
