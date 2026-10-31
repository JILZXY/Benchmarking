#!/usr/bin/env bash
# Solicita una detención controlada, guarda el estado en checkpoint.json y permite reanudar luego.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
exec python "${ROOT}/runner/python/src/orchestration/background_ctl.py" stop

