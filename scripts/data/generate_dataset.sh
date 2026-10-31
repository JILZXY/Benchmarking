#!/usr/bin/env bash
# Genera el dataset experimental de intentos y su manifiesto SHA-256.
# Uso:
#   scripts/data/generate_dataset.sh                 # usa DATASET_SIZE/DATASET_SEED o 500000/20261030
#   scripts/data/generate_dataset.sh --count 1000    # prueba pequeña
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec python "${ROOT}/dataset/generator/generate.py" "$@"

