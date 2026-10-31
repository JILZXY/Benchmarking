#!/usr/bin/env bash
# Carga el dataset de intentos en el motor indicado y verifica su integridad.
# Uso:
#   scripts/data/load_data.sh <mongodb|postgresql|verify> [argumentos adicionales...]
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
MODE="${1:-}"
shift || true

case "${MODE}" in
  mongodb)
    exec python "${ROOT}/runner/python/src/load/mongodb_loader.py" "$@"
    ;;
  postgresql)
    exec python "${ROOT}/runner/python/src/load/postgresql_loader.py" "$@"
    ;;
  verify)
    exec python "${ROOT}/runner/python/src/load/verify_load.py" "$@"
    ;;
  *)
    echo "Uso: $0 <mongodb|postgresql|verify> [argumentos...]" >&2
    exit 2
    ;;
esac

