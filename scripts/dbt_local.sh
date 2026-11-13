#!/usr/bin/env bash
# Roda o dbt com os caminhos do projeto: scripts/dbt_local.sh build --full-refresh
set -euo pipefail
cd "$(dirname "$0")/.."
export WAREHOUSE_PATH="$PWD/dados/warehouse.duckdb" LAKE_PATH="$PWD/dados/lake" REFERENCIA_PATH="$PWD/dados/raw/referencia"
export DBT_SEND_ANONYMOUS_USAGE_STATS=false
exec "${DBT_BIN:-dbt}" "$@" --project-dir dbt --profiles-dir dbt --target-path "$PWD/dados/dbt/target" --log-path "$PWD/dados/dbt/logs"
