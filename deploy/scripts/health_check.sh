#!/usr/bin/env bash
# ==============================================================================
# OrangeCashMachine — Verificador Unificado de Salud (Health Contract L1-L5)
# ==============================================================================
# Evalúa el estado operacional real de la plataforma Market Data y sus dependencias.
# No asume que "streaming active" significa que todo el dominio está saludable.
#
# Uso:
#   ./deploy/scripts/health_check.sh [--json]
# ==============================================================================
set -uo pipefail

JSON_OUTPUT=0
for arg in "$@"; do
  case "$arg" in
    --json) JSON_OUTPUT=1 ;;
  esac
done

# ── Evaluaciones ────────────────────────────────────────────────────────────

# 1. Realtime Streaming (L1: systemd unit)
STREAMING_ACTIVE=0
if systemctl is-active --quiet ocm-streaming@bybit.service 2>/dev/null; then
  STREAMING_ACTIVE=1
elif pgrep -f "streaming_hydra.py" >/dev/null 2>&1; then
  STREAMING_ACTIVE=1 # Fallback si corre fuera de systemd en dev
fi

# 2. Market Data HTTP API (L1/L3: /health endpoint en :8001)
API_HEALTHY=0
API_STATUS_CODE="000"
if RESPONSE=$(curl -s -o /tmp/ocm_health.json -w "%{http_code}" http://localhost:8001/health 2>/dev/null); then
  API_STATUS_CODE="$RESPONSE"
  if [ "$API_STATUS_CODE" -eq 200 ]; then
    API_HEALTHY=1
  fi
fi

# 3. Infrastructure: Redis (L2)
REDIS_HEALTHY=0
if docker exec ocm_redis redis-cli -p 6379 ping >/dev/null 2>&1; then
  REDIS_HEALTHY=1
elif redis-cli -p 6379 ping >/dev/null 2>&1; then
  REDIS_HEALTHY=1
fi

# 4. Infrastructure: Kafka (L2)
KAFKA_HEALTHY=0
if docker exec ocm_kafka kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
  KAFKA_HEALTHY=1
elif nc -z localhost 9093 >/dev/null 2>&1; then
  KAFKA_HEALTHY=1
fi

# 5. Batch Pipeline Timer (L1: systemd timer)
BATCH_TIMER_ACTIVE=0
if systemctl is-active --quiet ocm-pipeline-batch.timer 2>/dev/null; then
  BATCH_TIMER_ACTIVE=1
fi

# ── Consolidación del Estado (MARKET_DATA_HEALTHY / DEGRADED / DOWN) ─────────
MARKET_DATA_OVERALL="DOWN"

if [ "$STREAMING_ACTIVE" -eq 1 ] && [ "$API_HEALTHY" -eq 1 ] && [ "$KAFKA_HEALTHY" -eq 1 ]; then
  MARKET_DATA_OVERALL="HEALTHY"
elif [ "$STREAMING_ACTIVE" -eq 1 ] || [ "$API_HEALTHY" -eq 1 ]; then
  MARKET_DATA_OVERALL="DEGRADED"
else
  MARKET_DATA_OVERALL="STOPPED"
fi

# ── Salida ──────────────────────────────────────────────────────────────────
if [ "$JSON_OUTPUT" -eq 1 ]; then
  cat <<EOF
{
  "market_data": {
    "overall": "$MARKET_DATA_OVERALL",
    "realtime_streaming_bybit": $([ "$STREAMING_ACTIVE" -eq 1 ] && echo 'true' || echo 'false'),
    "http_api_8001": $([ "$API_HEALTHY" -eq 1 ] && echo 'true' || echo 'false'),
    "batch_timer": $([ "$BATCH_TIMER_ACTIVE" -eq 1 ] && echo 'true' || echo 'false')
  },
  "infrastructure": {
    "kafka": $([ "$KAFKA_HEALTHY" -eq 1 ] && echo 'true' || echo 'false'),
    "redis": $([ "$REDIS_HEALTHY" -eq 1 ] && echo 'true' || echo 'false')
  }
}
EOF
else
  printf 'OrangeCashMachine Health Contract\n'
  printf '──────────────────────────────────────────────\n'
  printf 'MARKET DATA PLATFORM\n'
  printf '  Realtime Streaming (bybit)  [%s]\n' "$([ "$STREAMING_ACTIVE" -eq 1 ] && echo '● HEALTHY' || echo '○ STOPPED')"
  printf '  HTTP API & Engine (:8001)   [%s] (HTTP %s)\n' "$([ "$API_HEALTHY" -eq 1 ] && echo '● HEALTHY' || echo '○ STOPPED')" "$API_STATUS_CODE"
  printf '  Batch Pipeline (Timer)      [%s]\n' "$([ "$BATCH_TIMER_ACTIVE" -eq 1 ] && echo '● ACTIVE' || echo '○ INACTIVE')"
  printf '  Overall Market Data         [%s]\n' "$MARKET_DATA_OVERALL"
  printf 'INFRASTRUCTURE\n'
  printf '  Kafka Broker                [%s]\n' "$([ "$KAFKA_HEALTHY" -eq 1 ] && echo '● HEALTHY' || echo '○ UNHEALTHY')"
  printf '  Redis Store                 [%s]\n' "$([ "$REDIS_HEALTHY" -eq 1 ] && echo '● HEALTHY' || echo '○ UNHEALTHY')"
  printf '──────────────────────────────────────────────\n'
  printf 'FINAL VERDICT: MARKET_DATA_%s\n' "$MARKET_DATA_OVERALL"
fi

# Exit code: 0 si HEALTHY o DEGRADED (operativo parcial), 1 si DOWN
if [ "$MARKET_DATA_OVERALL" = "DOWN" ] || [ "$MARKET_DATA_OVERALL" = "STOPPED" ]; then
  exit 1
fi
exit 0
