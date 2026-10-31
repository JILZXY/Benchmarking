#!/usr/bin/env bash
# Ejecuta las validaciones iniciales y lanza la matriz completa (o filtrada) en primer plano.
# Uso:
#   scripts/benchmark/run_benchmarks.sh [--resume] [argumentos adicionales...]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ ! -f "${ROOT}/.env" && -f "${ROOT}/.env.example" ]]; then
  cp "${ROOT}/.env.example" "${ROOT}/.env"
fi

python "${ROOT}/scripts/setup/verify_docker_env.py"
python "${ROOT}/dataset/generator/generate.py" --verify-only

exec python "${ROOT}/runner/python/src/orchestration/runner.py" "$@"

