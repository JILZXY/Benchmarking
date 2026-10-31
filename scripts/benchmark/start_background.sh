#!/usr/bin/env bash
# Ejecuta las validaciones iniciales e inicia el benchmark en segundo plano.
# Evita iniciar dos ejecuciones simultáneas y guarda los logs en results/logs/.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

if [[ ! -f "${ROOT}/.env" && -f "${ROOT}/.env.example" ]]; then
  cp "${ROOT}/.env.example" "${ROOT}/.env"
fi

python "${ROOT}/scripts/setup/verify_docker_env.py"
python "${ROOT}/dataset/generator/generate.py" --verify-only

exec python "${ROOT}/runner/python/src/orchestration/background_ctl.py" start "$@"

