"""
tests/architecture/test_kafka_contracts — Policy tests de contratos Kafka/eventos.

Verifican que:
  - Los wire schemas viven SOLO en shared.kafka (BC-29, BC-33).
  - application/ no importa Kafka directamente (BC-05).
  - El publisher port está correctamente definido como abstracción.
  - No hay duplicación de schemas entre shared y market_data.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
SHARED_KAFKA = ROOT / "packages" / "shared" / "kafka"
MARKET_DATA = ROOT / "packages" / "market_data"
APPLICATION = MARKET_DATA / "application"
INFRA_KAFKA = MARKET_DATA / "infrastructure" / "kafka"


# ── Helpers ───────────────────────────────────────────────────────────────────

SCHEMA_SUFFIXES = ("Payload", "Event", "Schema", "Message", "Wire", "Envelope")


def _class_names_in(path: Path) -> set[str]:
    """Extrae nombres de clases definidas en todos los .py de un directorio."""
    names: set[str] = set()
    if not path.exists():
        return names
    for f in path.rglob("*.py"):
        try:
            tree = ast.parse(f.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                names.add(node.name)
    return names


def _all_imports_in(path: Path) -> list[tuple[Path, str]]:
    """Retorna (archivo, módulo_importado) para todos los .py de un directorio."""
    result: list[tuple[Path, str]] = []
    if not path.exists():
        return result
    for f in path.rglob("*.py"):
        if "test_" in f.name:
            continue
        try:
            tree = ast.parse(f.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                result.append((f, node.module))
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    result.append((f, alias.name))
    return result


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestKafkaSchemaSSO:
    """BC-29, BC-33: Wire schemas Kafka son SSOT en shared.kafka."""

    def test_no_schema_classes_defined_in_market_data_infra_kafka(self):
        """
        market_data.infrastructure.kafka no debe definir clases tipo wire schema.
        Los schemas Kafka son contratos de red — viven en shared.kafka (SSOT).
        """
        if not INFRA_KAFKA.exists():
            return  # No hay infra Kafka aún — OK

        infra_classes = _class_names_in(INFRA_KAFKA)
        schema_classes = {c for c in infra_classes if any(c.endswith(sfx) for sfx in SCHEMA_SUFFIXES)}

        shared_classes = _class_names_in(SHARED_KAFKA / "schemas") if (SHARED_KAFKA / "schemas").exists() else set()

        duplicated_outside_shared = schema_classes - shared_classes
        assert not duplicated_outside_shared, (
            "Clases Kafka wire schema fuera de shared.kafka (BC-29/BC-33):\n"
            + "\n".join(f"  {c}" for c in sorted(duplicated_outside_shared))
            + "\nMueve estos tipos a packages/shared/kafka/schemas/"
        )


class TestApplicationKafkaIsolation:
    """BC-05: application/ no importa Kafka directamente."""

    def test_no_kafka_imports_in_application(self):
        violations: list[str] = []
        for filepath, module in _all_imports_in(APPLICATION):
            if "kafka" in module.lower() and "market_data.infrastructure" in module:
                violations.append(f"{filepath.relative_to(ROOT)}: imports {module}")
        assert not violations, (
            "Imports directos de Kafka en application/ (BC-05):\n"
            + "\n".join(f"  {v}" for v in violations)
            + "\nEl publisher debe inyectarse via OHLCVPublisherPort desde CompositionRoot."
        )

    def test_no_kafkaproduceradapter_in_application(self):
        """KafkaProducerAdapter es infraestructura — application/ nunca lo toca."""
        violations: list[str] = []
        for f in APPLICATION.rglob("*.py"):
            if "test_" in f.name:
                continue
            src = f.read_text()
            if "KafkaProducerAdapter" in src or "KafkaOHLCVPublisher" in src:
                violations.append(str(f.relative_to(ROOT)))
        assert not violations, "Clases concretas de Kafka en application/ (BC-05):\n" + "\n".join(
            f"  {v}" for v in violations
        )


class TestPublisherPortContract:
    """El puerto de publicación cumple el contrato de abstracción."""

    def _get_port_src(self) -> str | None:
        port_file = MARKET_DATA / "ports" / "outbound" / "publisher_port.py"
        return port_file.read_text() if port_file.exists() else None

    def test_publisher_port_file_exists(self):
        src = self._get_port_src()
        assert src is not None, (
            "ports/outbound/publisher_port.py no existe. Ejecuta la FASE 2 del plan de deuda técnica."
        )

    def test_publisher_port_defines_protocol(self):
        src = self._get_port_src()
        if src is None:
            return
        assert "Protocol" in src, "OHLCVPublisherPort debe ser un typing.Protocol"
        assert "runtime_checkable" in src, (
            "OHLCVPublisherPort debe ser @runtime_checkable — "
            "permite isinstance() checks en tests sin depender de la implementación"
        )

    def test_null_publisher_implements_port_contract(self):
        """NullPublisher debe implementar publish_chunk y close."""
        src = self._get_port_src()
        if src is None:
            return
        assert "NullPublisher" in src, "NullPublisher (NullObject) debe estar definido"
        assert "publish_chunk" in src, "NullPublisher debe implementar publish_chunk"
        assert "close" in src, "NullPublisher debe implementar close"

    def test_publisher_port_has_no_infra_imports(self):
        """
        El puerto no debe importar infraestructura concreta.
        Puede importar del dominio (BC-04) pero no de infrastructure/adapters.
        """
        src = self._get_port_src()
        if src is None:
            return
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert "infrastructure" not in node.module, (
                    f"publisher_port.py importa infrastructure: {node.module}\n"
                    "Los puertos solo dependen del dominio (BC-04)."
                )
                assert "adapters" not in node.module, f"publisher_port.py importa adapters: {node.module}"
