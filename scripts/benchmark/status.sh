#!/usr/bin/env bash
# Muestra el estado general y progreso de la matriz experimental.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec python "${ROOT}/runner/python/src/orchestration/background_ctl.py" status

