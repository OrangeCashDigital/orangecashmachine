#!/usr/bin/env bash
# ==============================================================================
# health_check.sh — contrato de salud del dominio Market Data (L1..L4)
# ==============================================================================
# L1 Process    : systemd units (streaming system, market-data system|user)
# L2 Dependency : Kafka broker, Redis
# L3 Data Flow  : frescura de orderbook.raw y ohlcv.raw (mensajes en ventana)
# L4 Processing : frescura de Bronze Iceberg (parquet reciente)
#
# Salida semántica (una línea por dominio):
#   MARKET_DATA_HEALTHY=HEALTHY|DEGRADED|DOWN
#   INFRA_HEALTHY=HEALTHY|DEGRADED|DOWN
#   OBSERVABILITY_HEALTHY=HEALTHY|DEGRADED|DOWN
# Exit 0 si MARKET_DATA_HEALTHY=HEALTHY; 1 si DEGRADED; 2 si DOWN.
# ==============================================================================
set -u

WINDOW_MIN=15          # ventana de frescura para tópicos y Bronze
BRONZE_DIR="${OCM_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}/data_platform/iceberg_warehouse/bronze"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ocm_kafka}"
REDIS_CONTAINER="${REDIS_CONTAINER:-ocm_redis}"

l1_streaming() { systemctl is-active --quiet ocm-streaming.service && echo OK || echo FAIL; }

l1_marketdata() {
    # Acepta unidad de sistema o de usuario (deploy sin sudo)
    if systemctl is-active --quiet ocm-market-data.service 2>/dev/null; then echo OK; return; fi
    if systemctl --user is-active --quiet ocm-market-data.service 2>/dev/null; then
        curl -fsS -m 5 http://localhost:8001/health >/dev/null 2>&1 && echo OK || echo FAIL
        return
    fi
    echo FAIL
}

l2_kafka() {
    timeout 10 docker exec "${KAFKA_CONTAINER}" kafka-broker-api-versions \
        --bootstrap-server localhost:9092 >/dev/null 2>&1 && echo OK || echo FAIL
}

l2_redis() {
    local pass="${REDIS_PASSWORD:-ocm_local_dev}"
    timeout 5 docker exec "${REDIS_CONTAINER}" redis-cli -a "${pass}" ping 2>/dev/null \
        | grep -q PONG && echo OK || echo FAIL
}

# offsets de un tópico: "p0:p1:p2" sumados
_topic_offsets() {
    timeout 20 docker exec "${KAFKA_CONTAINER}" kafka-get-offsets \
        --bootstrap-server localhost:9092 --topic "$1" --time "$2" 2>/dev/null \
        | awk -F: '{s+=$3} END {print s+0}'
}

# OK si el tópico recibió mensajes en la última WINDOW_MIN minutos
l3_topic_fresh() {
    local topic="$1" latest earliest_ms now_ms
    latest=$(_topic_offsets "${topic}" latest)
    now_ms=$(date +%s%3N)
    earliest_ms=$((now_ms - WINDOW_MIN * 60 * 1000))
    local past
    past=$(_topic_offsets "${topic}" "${earliest_ms}")
    if [[ "${latest}" -eq 0 ]]; then echo EMPTY
    elif (( latest > past )); then echo OK
    else echo STALE; fi
}

l4_bronze_fresh() {
    find "${BRONZE_DIR}" -name "*.parquet" -mmin -"${WINDOW_MIN}" 2>/dev/null | head -1 \
        | grep -q . && echo OK || echo STALE
}

S=$(l1_streaming); MD=$(l1_marketdata); K=$(l2_kafka); R=$(l2_redis)
OB=$(l3_topic_fresh orderbook.raw); OH=$(l3_topic_fresh ohlcv.raw); BR=$(l4_bronze_fresh)

# ── Agregación ────────────────────────────────────────────────────────────────
INFRA=HEALTHY
[[ "${K}" == FAIL ]] && INFRA=DOWN
[[ "${R}" == FAIL && "${INFRA}" != DOWN ]] && INFRA=DEGRADED

MD_STATE=HEALTHY
fails=0
for c in "${S}" "${MD}" "${K}"; do [[ "${c}" == FAIL ]] && fails=$((fails+1)); done
[[ ${fails} -ge 2 ]] && MD_STATE=DOWN
for sig in "${OB}" "${OH}" "${BR}"; do
    if [[ "${sig}" == STALE && "${MD_STATE}" == HEALTHY ]]; then MD_STATE=DEGRADED; fi
done
[[ "${K}" == FAIL ]] && MD_STATE=DOWN

OBS=DOWN
curl -fsS -m 3 http://localhost:9091/-/healthy >/dev/null 2>&1 && OBS=HEALTHY

echo "MARKET_DATA_HEALTHY=${MD_STATE}"
echo "INFRA_HEALTHY=${INFRA}"
echo "OBSERVABILITY_HEALTHY=${OBS}"
echo "# detail: streaming=${S} market-data=${MD} kafka=${K} redis=${R} orderbook.raw=${OB} ohlcv.raw=${OH} bronze=${BR} pushgateway=${OBS}"

case "${MD_STATE}" in HEALTHY) exit 0 ;; DEGRADED) exit 1 ;; *) exit 2 ;; esac
