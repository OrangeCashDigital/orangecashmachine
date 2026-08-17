"""ARCH-009 — Layer / Dependency Governance.

Reproduce TODOS los contratos `layers` y `forbidden` del SSOT de import-linter
(architecture_linter/importlinter.toml) mediante AST, incluyendo sus listas
`ignore_imports` de deuda técnica documentada. No inventa capas ni aristas:
la evidencia proviene del grafo real de imports (incluyendo lazy imports, que
el grafo estático de import-linter no ve).

Excepción documentada: los módulos `*composition_root` son el punto único de
ensamblaje (ADR-0003/ADR-0004, patrones BC-38/42/43/50/55) — sus imports hacia
otros bounded contexts son wiring intencional y no se marcan como violación.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from architecture_linter.engine import RepoContext
from architecture_linter.models import Evidence, Finding, Status
from architecture_linter.rules.base import Rule

IMPORTLINTER_TOML = Path("architecture_linter/importlinter.toml")

COMPOSITION_ROOT_SUFFIX = "composition_root"


@dataclass
class LayerContract:
    container: str = "market_data"
    layers: list[str] = field(default_factory=list)
    ignore_imports: list[str] = field(default_factory=list)
    source: str = str(IMPORTLINTER_TOML)
    name: str = "BC-08"

    @property
    def containers(self) -> list[str]:
        return [self.container]


@dataclass
class ForbiddenContract:
    name: str = ""
    source_modules: list[str] = field(default_factory=list)
    forbidden_modules: list[str] = field(default_factory=list)
    ignore_imports: list[str] = field(default_factory=list)
    allow_indirect_imports: bool = False
    source: str = str(IMPORTLINTER_TOML)


def load_layer_contracts(path: Path | None = None) -> list[LayerContract]:
    """Extrae TODOS los contratos type=layers del config de import-linter (SSOT)."""
    cfg_path = path or IMPORTLINTER_TOML
    out: list[LayerContract] = []
    if not cfg_path.is_file():
        return out
    try:
        with cfg_path.open("rb") as fh:
            data = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError):
        return out

    for c in data.get("tool", {}).get("importlinter", {}).get("contracts", []):
        if c.get("type") != "layers":
            continue
        containers = [str(x) for x in c.get("containers", [])]
        if not containers:
            continue
        for cont in containers:
            out.append(
                LayerContract(
                    container=cont,
                    layers=[str(x) for x in c.get("layers", [])],
                    ignore_imports=[str(i) for i in c.get("ignore_imports", [])],
                    source=str(cfg_path),
                    name=str(c.get("name", "BC-NN")),
                )
            )
    return out


def load_forbidden_contracts(path: Path | None = None) -> list[ForbiddenContract]:
    """Extrae los contratos type=forbidden del config de import-linter (SSOT)."""
    cfg_path = path or IMPORTLINTER_TOML
    out: list[ForbiddenContract] = []
    if not cfg_path.is_file():
        return out
    try:
        with cfg_path.open("rb") as fh:
            data = tomllib.load(fh)
    except (OSError, tomllib.TOMLDecodeError):
        return out

    for c in data.get("tool", {}).get("importlinter", {}).get("contracts", []):
        if c.get("type") != "forbidden":
            continue
        sm = [str(x) for x in c.get("source_modules", [])]
        fm = [str(x) for x in c.get("forbidden_modules", [])]
        if not sm or not fm:
            continue
        out.append(
            ForbiddenContract(
                name=str(c.get("name", "BC-NN")),
                source_modules=sm,
                forbidden_modules=fm,
                ignore_imports=[str(i) for i in c.get("ignore_imports", [])],
                allow_indirect_imports=bool(c.get("allow_indirect_imports", False)),
                source=str(cfg_path),
            )
        )
    return out


class Arch009Rule(Rule):
    rule_id = "ARCH-009"
    rule_name = "Layer / Dependency Governance (import-linter contracts)"
    description = (
        "Reproduce los contratos layers y forbidden de import-linter vía AST, "
        "incluyendo ignore_imports y lazy imports, con la excepción de composition roots."
    )

    def __init__(
        self,
        severity=None,
        enabled=None,
        contract: LayerContract | list[LayerContract] | None = None,
        forbidden: list[ForbiddenContract] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(severity=severity, enabled=enabled, **kwargs)
        if contract is None:
            self.contracts = load_layer_contracts()
        elif isinstance(contract, LayerContract):
            self.contracts = [contract]
        else:
            self.contracts = contract
        self.forbidden = forbidden if forbidden is not None else load_forbidden_contracts()

    def analyze(self, ctx: RepoContext) -> list[Finding]:
        findings: list[Finding] = []
        layer_findings = 0

        for contract in self.contracts:
            layer_findings += self._analyze_layer_contract(ctx, contract, findings)
        self._analyze_forbidden_contracts(ctx, findings)

        if layer_findings == 0 and not self.contracts and not findings:
            return [
                self.finding(
                    Status.UNKNOWN,
                    f"No se pudo leer ningún contrato de capas desde {IMPORTLINTER_TOML}.",
                    confidence=0.9,
                    concept="layer",
                )
            ]
        return findings or [
            self.finding(
                Status.PASS,
                f"Sin violaciones de capa ni de dependencias prohibidas ({len(self.contracts)} layer contracts, "
                f"{len(self.forbidden)} forbidden contracts).",
                confidence=0.9,
                concept="layer",
            )
        ]

    # ── Capas ────────────────────────────────────────────────────────────── #

    def _analyze_layer_contract(self, ctx: RepoContext, contract: LayerContract, findings: list[Finding]) -> int:
        if not contract.layers:
            return 0
        index_by_layer = {name: i for i, name in enumerate(contract.layers)}
        layer_prefixes: list[tuple[str, str]] = [
            (layer, f"{cont}.{layer}") for cont in contract.containers for layer in contract.layers
        ]

        count = 0
        for path in ctx.files:
            info = ctx.module(path)
            if not info:
                continue
            src_layer = self._layer_of(ctx, path, contract)
            if src_layer is None:
                continue
            src_index = index_by_layer[src_layer]
            for imported_module, line in info.imports:
                tgt_layer = self._layer_of_module(imported_module, layer_prefixes)
                if tgt_layer is None:
                    continue
                tgt_index = index_by_layer[tgt_layer]
                if tgt_index >= src_index:
                    continue  # importar capa más externa/igual = permitido
                if self._is_ignored(ctx, path, imported_module, contract.ignore_imports):
                    continue
                count += 1
                findings.append(
                    self.finding(
                        Status.FAIL,
                        f"Violación de capa {contract.name}: {src_layer} importa {tgt_layer} ({imported_module}).",
                        file=str(path),
                        line=line,
                        symbol=imported_module,
                        evidence=[Evidence(str(path), line, imported_module, f"import {imported_module}")],
                        confidence=0.95,
                        concept="layer",
                        producer=src_layer,
                        consumer=tgt_layer,
                    )
                )
        return count

    def _layer_of(self, ctx: RepoContext, path: Path, contract: LayerContract) -> str | None:
        for container in contract.containers:
            for layer in contract.layers:
                container_dir = _container_dir(ctx.root, container)
                candidate = container_dir / layer
                if path.is_relative_to(candidate):
                    return layer
        return None

    def _layer_of_module(self, module: str, prefixes: list[tuple[str, str]]) -> str | None:
        for layer, prefix in prefixes:
            if module == prefix or module.startswith(prefix + "."):
                return layer
        return None

    # ── Prohibiciones ────────────────────────────────────────────────────── #

    def _analyze_forbidden_contracts(self, ctx: RepoContext, findings: list[Finding]) -> None:
        for contract in self.forbidden:
            self._analyze_forbidden_contract(ctx, contract, findings)

    def _analyze_forbidden_contract(
        self, ctx: RepoContext, contract: ForbiddenContract, findings: list[Finding]
    ) -> None:
        for path in ctx.files:
            mod = _relative_module(ctx, path)
            if not any(mod == s or mod.startswith(s + ".") for s in contract.source_modules):
                continue
            if mod.endswith(COMPOSITION_ROOT_SUFFIX):
                continue  # wiring intencional del composition root (ADR-0003/0004)
            info = ctx.module(path)
            if not info:
                continue
            for imported_module, line in info.imports:
                if not any(
                    imported_module == f or imported_module.startswith(f + ".") for f in contract.forbidden_modules
                ):
                    continue
                if self._is_ignored(ctx, path, imported_module, contract.ignore_imports):
                    continue
                findings.append(
                    self.finding(
                        Status.FAIL,
                        f"Dependencia prohibida {contract.name}: {mod} importa {imported_module}.",
                        file=str(path),
                        line=line,
                        symbol=imported_module,
                        evidence=[Evidence(str(path), line, imported_module, f"import {imported_module}")],
                        confidence=0.9,
                        concept="dependency",
                        producer=mod,
                        consumer=imported_module,
                    )
                )

    # ── Ignore imports ───────────────────────────────────────────────────── #

    def _is_ignored(self, ctx: RepoContext, path: Path, imported_module: str, ignore_imports: list[str]) -> bool:
        rel = _relative_module(ctx, path)
        for ignored in ignore_imports:
            if "->" not in ignored:
                continue
            src, tgt = (s.strip() for s in ignored.split("->", 1))
            if rel == src and imported_module == tgt:
                return True
        return False


def _container_dir(root: Path, container: str) -> Path:
    """Directorio real de un contenedor (bajo packages/ para los BCs, raíz para ocm/etc.)."""
    dotted = container.replace(".", "/")
    under_packages = root / "packages" / dotted
    if under_packages.exists():
        return under_packages
    return root / dotted


def _relative_module(ctx: RepoContext, path: Path) -> str:
    try:
        rel = path.relative_to(ctx.root)
    except ValueError:
        return str(path)
    parts = rel.parts
    # packages/<bc>/<layer>/<...>.py → market_data.<layer>.<...>
    if parts and parts[0] == "packages" and len(parts) >= 3:
        return ".".join(parts[1:-1]) + "." + parts[-1][:-3]
    return ".".join(parts)[:-3]
