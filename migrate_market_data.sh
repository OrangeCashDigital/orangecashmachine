#!/usr/bin/env bash
set -euo pipefail

ROOT="market_data"

if [ ! -d "$ROOT" ]; then
    echo "ERROR: No se encuentra $ROOT/. Ejecuta desde la raíz del proyecto."
    exit 1
fi

if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "ERROR: Hay cambios sin commitear. Haz commit o stash antes de continuar."
    exit 1
fi

echo "✓ Preflight OK"

echo "── Creando estructura de carpetas..."
mkdir -p \
    "$ROOT/ingestion/websocket" \
    "$ROOT/ingestion/rest" \
    "$ROOT/ingestion/adapters" \
    "$ROOT/processing/fetchers" \
    "$ROOT/processing/pipelines" \
    "$ROOT/processing/strategies" \
    "$ROOT/processing/transformers" \
    "$ROOT/processing/utils" \
    "$ROOT/quality/validators" \
    "$ROOT/quality/policies" \
    "$ROOT/quality/invariants" \
    "$ROOT/quality/schemas" \
    "$ROOT/storage/bronze" \
    "$ROOT/storage/silver" \
    "$ROOT/storage/gold"
echo "✓ Carpetas creadas"

echo "── Creando __init__.py..."
PACKAGES=(
    "$ROOT/ingestion"
    "$ROOT/ingestion/websocket"
    "$ROOT/ingestion/rest"
    "$ROOT/ingestion/adapters"
    "$ROOT/processing"
    "$ROOT/processing/fetchers"
    "$ROOT/processing/pipelines"
    "$ROOT/processing/strategies"
    "$ROOT/processing/transformers"
    "$ROOT/processing/utils"
    "$ROOT/quality"
    "$ROOT/quality/validators"
    "$ROOT/quality/policies"
    "$ROOT/quality/invariants"
    "$ROOT/quality/schemas"
    "$ROOT/storage"
    "$ROOT/storage/bronze"
    "$ROOT/storage/silver"
    "$ROOT/storage/gold"
)
for pkg in "${PACKAGES[@]}"; do
    if [ ! -f "$pkg/__init__.py" ]; then
        touch "$pkg/__init__.py"
        git add "$pkg/__init__.py"
    fi
done
echo "✓ __init__.py creados"

move() {
    local src="$1"
    local dst="$2"
    if [ ! -f "$src" ]; then
        echo "  SKIP (no existe): $src"
        return
    fi
    if [ -f "$dst" ]; then
        echo "  SKIP (ya en destino): $dst"
        return
    fi
    git mv "$src" "$dst"
    echo "  mv $src → $dst"
}

echo "── Moviendo a processing/..."
move "$ROOT/batch/fetchers/fetcher.py"            "$ROOT/processing/fetchers/fetcher.py"
move "$ROOT/batch/fetchers/__init__.py"           "$ROOT/processing/fetchers/__init__.py"
move "$ROOT/batch/pipelines/unified_pipeline.py"  "$ROOT/processing/pipelines/unified_pipeline.py"
move "$ROOT/batch/pipelines/__init__.py"          "$ROOT/processing/pipelines/__init__.py"
move "$ROOT/batch/strategies/base.py"             "$ROOT/processing/strategies/base.py"
move "$ROOT/batch/strategies/backfill.py"         "$ROOT/processing/strategies/backfill.py"
move "$ROOT/batch/strategies/incremental.py"      "$ROOT/processing/strategies/incremental.py"
move "$ROOT/batch/strategies/repair.py"           "$ROOT/processing/strategies/repair.py"
move "$ROOT/batch/strategies/__init__.py"         "$ROOT/processing/strategies/__init__.py"
move "$ROOT/batch/transformers/transformer.py"    "$ROOT/processing/transformers/transformer.py"
move "$ROOT/batch/transformers/__init__.py"       "$ROOT/processing/transformers/__init__.py"
move "$ROOT/batch/schemas/timeframe.py"           "$ROOT/processing/utils/timeframe.py"
echo "✓ processing/ listo"

echo "── Moviendo a quality/..."
move "$ROOT/batch/pipelines/quality_pipeline.py"        "$ROOT/quality/pipeline.py"
move "$ROOT/batch/schemas/data_quality.py"              "$ROOT/quality/validators/data_quality.py"
move "$ROOT/batch/schemas/cross_exchange_validator.py"  "$ROOT/quality/validators/cross_exchange_validator.py"
move "$ROOT/batch/schemas/data_quality_policy.py"       "$ROOT/quality/policies/data_quality_policy.py"
move "$ROOT/batch/schemas/invariants.py"                "$ROOT/quality/invariants/invariants.py"
move "$ROOT/batch/schemas/ohlcv_schema.py"              "$ROOT/quality/schemas/ohlcv_schema.py"
move "$ROOT/batch/schemas/__init__.py"                  "$ROOT/quality/schemas/__init__.py"
echo "✓ quality/ listo"

echo "── Moviendo a storage/..."
move "$ROOT/batch/storage/bronze_storage.py"   "$ROOT/storage/bronze/bronze_storage.py"
move "$ROOT/batch/storage/bronze_retention.py" "$ROOT/storage/bronze/bronze_retention.py"
move "$ROOT/batch/storage/silver_storage.py"   "$ROOT/storage/silver/silver_storage.py"
move "$ROOT/batch/storage/gold_storage.py"     "$ROOT/storage/gold/gold_storage.py"
move "$ROOT/batch/storage/feature_engineer.py" "$ROOT/storage/gold/feature_engineer.py"
move "$ROOT/batch/storage/snapshot.py"         "$ROOT/storage/gold/snapshot.py"
move "$ROOT/batch/storage/__init__.py"         "$ROOT/storage/__init__.py"
echo "✓ storage/ listo"

echo "── Moviendo a ingestion/..."
move "$ROOT/streams/collectors/orderbook_stream.py" "$ROOT/ingestion/websocket/orderbook_stream.py"
move "$ROOT/streams/collectors/trades_stream.py"    "$ROOT/ingestion/websocket/trades_stream.py"
move "$ROOT/streams/collectors/__init__.py"         "$ROOT/ingestion/websocket/__init__.py"
move "$ROOT/streams/manager/stream_manager.py"      "$ROOT/ingestion/websocket/manager.py"
echo "✓ ingestion/ listo"

echo "── Limpiando carpetas origen..."
for dir in \
    "$ROOT/batch/fetchers" \
    "$ROOT/batch/pipelines" \
    "$ROOT/batch/schemas" \
    "$ROOT/batch/storage" \
    "$ROOT/batch/strategies" \
    "$ROOT/batch/transformers" \
    "$ROOT/batch" \
    "$ROOT/streams/collectors" \
    "$ROOT/streams/manager" \
    "$ROOT/streams"
do
    if [ -f "$dir/__init__.py" ]; then
        git rm -f "$dir/__init__.py" 2>/dev/null && echo "  rm $dir/__init__.py" || true
    fi
    rmdir "$dir" 2>/dev/null && echo "  rmdir $dir" || true
done
echo "✓ Carpetas origen limpiadas"

echo "── Actualizando imports..."
rewrite() {
    local old="$1"
    local new="$2"
    grep -rl "$old" . --include="*.py" | while read -r f; do
        sed -i "s|${old}|${new}|g" "$f"
    done
    echo "  import: $old → $new"
}

rewrite "market_data\.batch\.fetchers"                        "market_data.processing.fetchers"
rewrite "market_data\.batch\.pipelines\.unified_pipeline"     "market_data.processing.pipelines.unified_pipeline"
rewrite "market_data\.batch\.strategies"                      "market_data.processing.strategies"
rewrite "market_data\.batch\.transformers"                    "market_data.processing.transformers"
rewrite "market_data\.batch\.schemas\.timeframe"              "market_data.processing.utils.timeframe"
rewrite "market_data\.batch\.pipelines\.quality_pipeline"     "market_data.quality.pipeline"
rewrite "market_data\.batch\.schemas\.data_quality\b"         "market_data.quality.validators.data_quality"
rewrite "market_data\.batch\.schemas\.cross_exchange_validator" "market_data.quality.validators.cross_exchange_validator"
rewrite "market_data\.batch\.schemas\.data_quality_policy"    "market_data.quality.policies.data_quality_policy"
rewrite "market_data\.batch\.schemas\.invariants"             "market_data.quality.invariants.invariants"
rewrite "market_data\.batch\.schemas\.ohlcv_schema"           "market_data.quality.schemas.ohlcv_schema"
rewrite "market_data\.batch\.schemas"                         "market_data.quality.schemas"
rewrite "market_data\.batch\.storage\.bronze_storage"         "market_data.storage.bronze.bronze_storage"
rewrite "market_data\.batch\.storage\.bronze_retention"       "market_data.storage.bronze.bronze_retention"
rewrite "market_data\.batch\.storage\.silver_storage"         "market_data.storage.silver.silver_storage"
rewrite "market_data\.batch\.storage\.gold_storage"           "market_data.storage.gold.gold_storage"
rewrite "market_data\.batch\.storage\.feature_engineer"       "market_data.storage.gold.feature_engineer"
rewrite "market_data\.batch\.storage\.snapshot"               "market_data.storage.gold.snapshot"
rewrite "market_data\.batch\.storage"                         "market_data.storage"
rewrite "market_data\.streams\.collectors"                    "market_data.ingestion.websocket"
rewrite "market_data\.streams\.manager"                       "market_data.ingestion.websocket"
rewrite "market_data\.streams"                                "market_data.ingestion.websocket"
echo "✓ Imports actualizados"

echo "── Staging..."
git add -u
git add market_data/

echo ""
echo "============================================================"
echo "  Refactor completado. Pasos siguientes:"
echo "  1. Revisar:  git diff --staged"
echo "  2. Verificar: python -c 'import market_data'"
echo "  3. Tests:    pytest"
echo "  4. Commit:   git commit -m 'refactor(market-data): reorganizar en capas medallón'"
echo "============================================================"
