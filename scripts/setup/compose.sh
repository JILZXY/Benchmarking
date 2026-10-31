#!/usr/bin/env bash
# Wrapper de docker compose que carga .env + el perfil experimental indicado.
#
# Uso:  scripts/setup/compose.sh <a|b|c> <argumentos de docker compose...>
# Ej.:  scripts/setup/compose.sh c --profile postgresql up -d
#       scripts/setup/compose.sh a --profile "*" down
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

case "${1:-}" in
  a|A) PROFILE_FILE="profile_a_baseline.env" ;;
  b|B) PROFILE_FILE="profile_b_medium.env" ;;
  c|C) PROFILE_FILE="profile_c_severe.env" ;;
  *) echo "Uso: $0 <a|b|c> <argumentos de docker compose...>" >&2; exit 2 ;;
esac
shift

if [[ ! -f "${ROOT}/.env" ]]; then
  echo "No existe ${ROOT}/.env. Copiar .env.example a .env primero." >&2
  exit 1
fi

exec docker compose \
  --project-directory "${ROOT}" \
  --env-file "${ROOT}/.env" \
  --env-file "${ROOT}/config/profiles/${PROFILE_FILE}" \
  "$@"

