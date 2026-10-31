#!/usr/bin/env bash
# Captura y guarda los planes de ejecución (EXPLAIN) de Q1 y Q2 para el motor activo.
# Uso:
#   scripts/evidence/capture_explain.sh --engine mongodb --profile PROFILE_C_SEVERE
#   scripts/evidence/capture_explain.sh --engine postgresql --profile PROFILE_C_SEVERE
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec python "${ROOT}/runner/python/src/adapters/explain_runner.py" "$@"

