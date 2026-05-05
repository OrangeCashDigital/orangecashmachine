"""
invariants.py
=============

Invariantes formales del sistema OrangeCashMachine.

Responsabilidad
---------------
Definir y verificar qué significa que un dataset Silver esté "consistente".
Estas son las reglas que el sistema debe cumplir en todo momento — no
son opcionales ni best-effort.

Por qué existe este módulo
--------------------------
Sin invariantes explícitas, "el sistema funciona" es una afirmación
subjetiva. Con ellas, es verificable: o las cumple o no.

Uso
---
    from market_data.quality.schemas.invariants import check_dataset_invariants

    result = check_dataset_invariants(manifest)
    if not result.ok:
        for violation in result.violations:
            logger.error(violation)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List

import pandas as pd

from market_data.processing.utils.timeframe import timeframe_to_ms


# ==========================================================
# Resultado de verificación
# ==========================================================

@dataclass
class InvariantResult:
    """Resultado de verificar las invariantes de un dataset."""
    ok:         bool
    violations: List[str] = field(default_factory=list)
    warnings:   List[str] = field(default_factory=list)

    def add_violation(self, msg: str) -> None:
        self.violations.append(msg)
        self.ok = False

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def summary(self) -> str:
        status = "OK" if self.ok else "VIOLATED"
        lines  = [f"InvariantResult [{status}] violations={len(self.violations)} warnings={len(self.warnings)}"]
        for v in self.violations:
            lines.append(f"  VIOLATION: {v}")
        for w in self.warnings:
            lines.append(f"  WARNING:   {w}")
        return "\n".join(lines)


# ==========================================================
# Invariantes del sistema
# ==========================================================
# Cada función verifica exactamente una invariante (SRP).
# Retorna (ok: bool, mensaje: str) para ser composables.
# ==========================================================

# Máximo gap permitido entre max_ts del dataset y ahora.
# Si el dataset tiene más de N velas de retraso, algo está mal.
_MAX_LAG_CANDLES = 48  # 48 velas = 2 días en 1h, 48 min en 1m


def _check_manifest_not_empty(manifest: Dict) -> tuple[bool, str]:
    """El manifest debe tener al menos una partición."""
    partitions = manifest.get("partitions", [])
    if not partitions:
        return False, "El manifest no tiene particiones"
    return True, ""


def _check_required_fields(manifest: Dict) -> tuple[bool, str]:
    """El manifest debe tener todos los campos requeridos."""
    required = {"symbol", "timeframe", "exchange", "version", "partitions"}
    missing  = required - set(manifest.keys())
    if missing:
        return False, f"Campos requeridos ausentes en manifest: {sorted(missing)}"
    return True, ""


def _check_partition_timestamps(partition: Dict, idx: int) -> tuple[bool, str]:
    """min_ts debe ser anterior a max_ts en cada partición."""
    try:
        min_ts = pd.Timestamp(partition["min_ts"])
        max_ts = pd.Timestamp(partition["max_ts"])
        if min_ts >= max_ts:
            return False, f"Partición {idx}: min_ts >= max_ts ({min_ts} >= {max_ts})"
        return True, ""
    except Exception as exc:
        return False, f"Partición {idx}: timestamps inválidos — {exc}"


def _check_partition_rows(partition: Dict, idx: int) -> tuple[bool, str]:
    """Cada partición debe tener al menos 1 fila."""
    rows = partition.get("rows", 0)
    if rows < 1:
        return False, f"Partición {idx}: rows={rows} (debe ser >= 1)"
    return True, ""


def _check_no_partition_overlap(partitions: List[Dict]) -> tuple[bool, str]:
    """Las particiones no deben solaparse temporalmente."""
    try:
        ranges = [
            (pd.Timestamp(p["min_ts"]), pd.Timestamp(p["max_ts"]), i)
            for i, p in enumerate(partitions)
        ]
        ranges.sort(key=lambda x: x[0])
        for i in range(len(ranges) - 1):
            _, end_i, idx_i = ranges[i]
            start_j, _, idx_j = ranges[i + 1]
            if end_i > start_j:
                return False, (
                    f"Particiones {idx_i} y {idx_j} se solapan: "
                    f"end={end_i} > start={start_j}"
                )
        return True, ""
    except Exception as exc:
        return False, f"Error verificando solapamiento: {exc}"


def _check_dataset_lag(manifest: Dict) -> tuple[bool, str]:
    """
    El dataset no debe estar demasiado desactualizado.

    Calcula cuántas velas han pasado desde max_ts hasta ahora.
    Si supera _MAX_LAG_CANDLES, el pipeline probablemente está caído.
    """
    try:
        timeframe  = manifest.get("timeframe", "1h")
        tf_ms      = timeframe_to_ms(timeframe)
        partitions = manifest.get("partitions", [])
        if not partitions:
            return True, ""  # ya validado por _check_manifest_not_empty

        max_ts  = max(pd.Timestamp(p["max_ts"]) for p in partitions)
        now     = pd.Timestamp.now(tz="UTC")
        lag_ms  = (now - max_ts).total_seconds() * 1000
        lag_candles = int(lag_ms / tf_ms)

        if lag_candles > _MAX_LAG_CANDLES:
            return False, (
                f"Dataset con {lag_candles} velas de retraso "
                f"(max={_MAX_LAG_CANDLES}, timeframe={timeframe}, "
                f"max_ts={max_ts.isoformat()})"
            )
        return True, ""
    except Exception as exc:
        return False, f"Error verificando lag del dataset: {exc}"


def _check_version_is_positive(manifest: Dict) -> tuple[bool, str]:
    """El número de versión debe ser positivo."""
    version = manifest.get("version", 0)
    if not isinstance(version, int) or version < 1:
        return False, f"version={version} inválida (debe ser int >= 1)"
    return True, ""


# ==========================================================
# API pública
# ==========================================================

def check_dataset_invariants(
    manifest: Dict,
    check_lag: bool = True,
) -> InvariantResult:
    """
    Verifica todas las invariantes de un dataset Silver.

    Parameters
    ----------
    manifest : dict
        Contenido del latest.json del dataset.
    check_lag : bool
        Si True, verifica que el dataset no esté demasiado desactualizado.
        Deshabilitar en tests o backfills históricos.

    Returns
    -------
    InvariantResult
        ok=True si todas las invariantes se cumplen.
        ok=False con lista de violaciones si alguna falla.
    """
    result = InvariantResult(ok=True)

    # Invariante 1: campos requeridos
    ok, msg = _check_required_fields(manifest)
    if not ok:
        result.add_violation(msg)
        return result  # sin campos requeridos no podemos continuar

    # Invariante 2: manifest no vacío
    ok, msg = _check_manifest_not_empty(manifest)
    if not ok:
        result.add_violation(msg)
        return result

    # Invariante 3: versión válida
    ok, msg = _check_version_is_positive(manifest)
    if not ok:
        result.add_violation(msg)

    # Invariante 4: timestamps válidos por partición
    partitions = manifest.get("partitions", [])
    for idx, partition in enumerate(partitions):
        ok, msg = _check_partition_timestamps(partition, idx)
        if not ok:
            result.add_violation(msg)

        ok, msg = _check_partition_rows(partition, idx)
        if not ok:
            result.add_violation(msg)

    # Invariante 5: sin solapamiento entre particiones
    ok, msg = _check_no_partition_overlap(partitions)
    if not ok:
        result.add_violation(msg)

    # Invariante 6: lag máximo (opcional — desactivar en backfills)
    if check_lag:
        ok, msg = _check_dataset_lag(manifest)
        if not ok:
            result.add_warning(msg)  # warning, no violación: el pipeline puede estar pausado

    return result
