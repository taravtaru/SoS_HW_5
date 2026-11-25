#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")/.."
project="movie-cinema-test-$$"
export GENERATOR_INTERVAL_SECONDS=0 AGGREGATION_INTERVAL_SECONDS=0
export PRODUCER_PORT=0 AGGREGATION_PORT=0 REGISTRY_PORT=0 GRAFANA_PORT=0 S3_PORT=0

compose() {
    if [ -n "${COMPOSE_BIN:-}" ]; then
        "$COMPOSE_BIN" -p "$project" "$@"
    elif docker compose version >/dev/null 2>&1; then
        docker compose -p "$project" "$@"
    else
        docker-compose -p "$project" "$@"
    fi
}

cleanup() {
    status=$?
    trap - EXIT
    if [ "$status" -ne 0 ]; then
        compose logs --tail 35 || true
    fi
    compose down --volumes --remove-orphans || true
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

compose up --build --wait --wait-timeout 240
compose exec -T aggregation python -m unittest -v tests.test_pipeline.PipelineTests
compose stop kafka-1
compose exec -T aggregation python -m unittest -v tests.test_pipeline.FailoverTests
