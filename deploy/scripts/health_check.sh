#!/usr/bin/env bash
# ==============================================================================
# health_check.sh — contrato de salud del dominio Market Data (L1..L4)
# ==============================================================================
# L1 Process    : systemd units (streaming, market-data)
# L2 Dependency : Kafka broker, Redis
# L3 Data Flow  : frescura de orderbook.raw y ohlcv.raw (offsets Kafka)
# L4 Processing : frescura de Bronze Iceberg (parquet < 15 min)
#
# Salida semántica (una línea por dominio):
#   MARKET_DATA_HEALTHY=HEALTHY|DEGRADED|DOWN
#   INFRA_HEALTHY=HEALTHY|DEGRADED|DOWN
#   OBSERVABILITY_HEALTHY=HEALTHY|DEGRADED|DOWN
# Exit 0 si MARKET_DATA_HEALTHY=HEALTHY; 1 si DEGRADED; 2 si DOWN.
# ==============================================================================
set -u

STALE_S_DATA=300        # 5 min sin mensajes = stale
STALE_S_BRONZE=900      # 15 min sin parquet nuevo = stale
BRONZE_DIR="${OCM_REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}/data_platform/iceberg_warehouse/bronze"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-ocm_kafka}"

l1_streaming() { systemctl is-active --quiet ocm-streaming.service && echo OK || echo FAIL; }
l1_marketdata() {
    systemctl is-active --quiet ocm-market-data.service 2>/dev/null \
      && curl -fsS -m 5 http://localhost:8001/health >/dev/null 2>&1 && echo OK || echo FAIL
}
l2_kafka() { timeout 10 docker exec "${KAFKA_CONTAINER}" kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1 && echo OK || echo FAIL; }
l2_redis() { timeout 5 docker exec ocm_redis redis-cli ping 2>/dev/null | grep -q PONG && echo OK || echo FAIL; }

# offset más reciente de un tópico (partición con mayor offset)
topic_latest() {
    timeout 20 docker exec "${KAFKA_CONTAINER}" kafka-get-offsets \
        --bootstrap-server localhost:9092 --topic "$1" --time latest 2>/dev/null \
        | awk -F: '{if ($3+0 > m) m=$3+0} END {print m+0}'
}

l3_topic_fresh() {
    local topic="$1"
    local latest before
    latest=$(topic_latest "${topic}")
    sleep 10
    before=$(topic_latest "${topic}")
    if [[ "${before}" -gt "${latest}" ]]; then echo OK
    elif [[ "${latest}" -eq 0 ]]; then echo EMPTY
    else echo STALE; fi
}

l4_bronze_fresh() {
    local newest
    newest=$(find "${BRONZE_DIR}" -name "*.parquet" -mmin -"$((STALE_S_BRONZE / 60))" 2>/dev/null | head -1)
    [[ -n "${newest}" ]] && echo OK || echo STALE
}

S=$(l1_streaming); MD=$(l1_marketdata); K=$(l2_kafka); R=$(l2_redis)
OB=$(l3_topic_fresh orderbook.raw); OH=$(l3_topic_fresh ohlcv.raw); BR=$(l4_bronze_fresh)

# ── Agregación ────────────────────────────────────────────────────────────────
INFRA=HEALTHY
[[ "${K}" == FAIL ]] && INFRA=DOWN
{ [[ "${R}" == FAIL ]] || [[ "${K}" == FAIL ]]; } && INFRA=DEGRADED

MD_STATE=HEALTHY
fails=0
for c in "${S}" "${MD}" "${K}"; do [[ "${c}" == FAIL ]] && fails=$((fails+1)); done
[[ ${fails} -ge 2 ]] && MD_STATE=DOWN
[[ "${OB}" == STALE ]] && [[ "${MD_STATE}" == HEALTHY ]] && MD_STATE=DEGRADED
[[ "${OH}" == STALE ]] && [[ "${MD_STATE}" == HEALTHY ]] && MD_STATE=DEGRADED
[[ "${BR}" == STALE ]] && [[ "${MD_STATE}" == HEALTHY ]] && MD_STATE=DEGRADED
[[ "${K}" == FAIL ]] && MD_STATE=DOWN

OBS=DOWN
curl -fsS -m 3 http://localhost:9091/-/healthy >/dev/null 2>&1 && OBS=DEGRADED

echo "MARKET_DATA_HEALTHY=${MD_STATE}"
echo "INFRA_HEALTHY=${INFRA}"
echo "OBSERVABILITY_HEALTHY=${OBS}"
echo "# detail: streaming=${S} market-data=${MD} kafka=${K} redis=${R} orderbook.raw=${OB} ohlcv.raw=${OH} bronze=${BR} pushgateway=${OBS}"

case "${MD_STATE}" in HEALTHY) exit 0 ;; DEGRADED) exit 1 ;; *) exit 2 ;; esac
