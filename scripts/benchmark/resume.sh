#!/usr/bin/env bash
# Recupera el estado anterior desde checkpoint.json y continúa las ejecuciones pendientes en segundo plano.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ ! -f "${ROOT}/.env" && -f "${ROOT}/.env.example" ]]; then
  cp "${ROOT}/.env.example" "${ROOT}/.env"
fi

python "${ROOT}/scripts/setup/verify_docker_env.py"
exec python "${ROOT}/runner/python/src/orchestration/background_ctl.py" resume "$@"

