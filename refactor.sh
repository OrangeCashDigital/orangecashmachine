#!/bin/bash
# =============================================================================
# OrangeCashMachine – Refactor de estructura de carpetas
# =============================================================================
# Ejecutar desde la raíz del proyecto: bash refactor.sh
#
# Cambios:
#   services/exchange/        → market_data/adapters/exchange/
#   services/data_providers/  → market_data/adapters/data_providers/
#   services/observability/   → SPLIT:
#                               infra/observability/server.py   (start/push)
#                               market_data/observability/metrics.py (contadores)
#   services/state/           → infra/state/
#   services/                 → (eliminado)
#
# Transformers duplicados:
#   market_data/processing/transformers/ → eliminado (era re-export)
# =============================================================================
set -euo pipefail

echo ""
echo "════════════════════════════════════════════════"
echo "  OCM Refactor – inicio"
echo "════════════════════════════════════════════════"

echo ""
echo "── 1. Creando estructura nueva..."
mkdir -p market_data/adapters
mkdir -p market_data/observability
mkdir -p infra/observability
mkdir -p infra/state

echo "── 2. Moviendo módulos..."
cp -r services/exchange        market_data/adapters/exchange
cp -r services/data_providers  market_data/adapters/data_providers
cp -r services/state/.         infra/state/
touch market_data/adapters/__init__.py
touch market_data/observability/__init__.py
touch infra/observability/__init__.py

echo "── 3. Splitteando observability/metrics.py..."

cat > infra/observability/server.py << 'PYEOF'
"""
infra/observability/server.py
==============================
Infraestructura de observabilidad: servidor HTTP de métricas y push al Pushgateway.

Responsabilidad única: ciclo de vida del servidor de métricas.
No contiene contadores de dominio — esos viven en market_data/observability/metrics.py
"""
from __future__ import annotations

import time as _time
from loguru import logger as _log
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    push_to_gateway,
    start_http_server,
    REGISTRY,
)

PIPELINE_LAST_RUN = Gauge(
    "ocm_pipeline_last_run_timestamp",
    "Timestamp Unix del último run completado (exitoso o parcial)",
    ["exchange"],
)

PIPELINE_HEARTBEAT = Counter(
    "ocm_pipeline_heartbeat_total",
    "Incrementa en cada run — usado como deadman switch",
    ["exchange"],
)


def start_metrics_server(port: int = 8000) -> None:
    """Levanta el servidor HTTP de métricas en el puerto indicado."""
    start_http_server(port)


def push_metrics(
    exchange: str = "local",
    gateway: str = "localhost:9091",
    registry: CollectorRegistry = REGISTRY,
) -> None:
    """
    Empuja métricas al Pushgateway al finalizar el pipeline.

    Diseño
    ------
    • job=ocm_pipeline_{exchange} — un job por exchange evita
      last-write-wins cuando exchanges corren en paralelo.
    • NO se hace delete — los counters deben persistir entre runs.
    • Actualiza PIPELINE_LAST_RUN e incrementa PIPELINE_HEARTBEAT antes del push.

    SafeOps: nunca lanza excepción al caller.
    """
    job = f"ocm_pipeline_{exchange}"
    try:
        PIPELINE_LAST_RUN.labels(exchange=exchange).set(_time.time())
        PIPELINE_HEARTBEAT.labels(exchange=exchange).inc()
        push_to_gateway(gateway, job=job, registry=registry)
        _log.bind(job=job, gateway=gateway).debug("metrics_pushed")
    except Exception as exc:
        _log.bind(job=job, gateway=gateway).warning("metrics_push_failed | error={}", exc)
PYEOF

cp services/observability/metrics.py market_data/observability/metrics.py

python3 - << 'PYEOF'
import re

path = "market_data/observability/metrics.py"
with open(path) as f:
    content = f.read()

removals = [
    r'PIPELINE_LAST_RUN = Gauge\(.*?\)\n\n',
    r'PIPELINE_HEARTBEAT = Counter\(.*?\)\n\n',
    r'def start_metrics_server\(.*?\n\n',
    r'def push_metrics\(.*?)(?=\n# =|\ndef |\Z)',
    r'# =+\n# Push hacia Pushgateway\n# =+\n\n*',
    r'# =+\n# Servidor de métricas\n# =+\n\n*',
]
for pattern in removals:
    content = re.sub(pattern, '', content, flags=re.DOTALL)

note = '# Servidor de métricas y push → infra.observability.server\n\n'
content = content.replace(
    'from __future__ import annotations\n\n',
    f'from __future__ import annotations\n\n{note}'
)

with open(path, "w") as f:
    f.write(content)

print("  metrics.py limpiado correctamente")
PYEOF

echo "── 4. Actualizando imports..."
find . -name "*.py" \
  -not -path "./.venv/*" \
  -not -path "*/__pycache__/*" | xargs sed -i \
  -e 's|from services\.observability\.metrics import start_metrics_server|from infra.observability.server import start_metrics_server|g' \
  -e 's|from services\.observability\.metrics import push_metrics|from infra.observability.server import push_metrics|g' \
  -e 's|from services\.observability\.metrics|from market_data.observability.metrics|g' \
  -e 's|from services\.exchange|from market_data.adapters.exchange|g' \
  -e 's|from services\.data_providers|from market_data.adapters.data_providers|g' \
  -e 's|from services\.state|from infra.state|g'

echo "── 5. Eliminando processing/transformers (re-export vacío)..."
rm -rf market_data/processing/transformers

echo "── 6. Eliminando services/..."
rm -rf services/

echo ""
echo "── 7. Verificación de imports huérfanos..."
ORPHANS=$(grep -rn "from services" . --include="*.py" \
  --exclude-dir=__pycache__ --exclude-dir=.venv 2>/dev/null || true)

if [ -z "$ORPHANS" ]; then
    echo "   ✅ Sin referencias huérfanas a services/"
else
    echo "   ⚠️  Referencias pendientes:"
    echo "$ORPHANS"
fi

echo ""
echo "── 8. Estructura resultante..."
find market_data/adapters market_data/observability infra/observability infra/state \
  -name "*.py" | sort

echo ""
echo "════════════════════════════════════════════════"
echo "  OCM Refactor – completado"
echo "════════════════════════════════════════════════"
echo ""
echo "Próximos pasos:"
echo "  1. python main.py --show-config"
echo "  2. pytest tests/"
echo "  3. git add -A && git commit -m 'refactor: services/ → adapters/ + infra/'"
echo ""
