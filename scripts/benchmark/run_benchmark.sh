#!/usr/bin/env bash
# Ejecuta la matriz experimental de ETICORE DB Benchmark.
# Uso:
#   scripts/benchmark/run_benchmark.sh --dry-run
#   scripts/benchmark/run_benchmark.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec python "${ROOT}/runner/python/src/orchestration/runner.py" "$@"

