#!/usr/bin/env bash
# Healthcheck de MongoDB para el benchmark.
#
# 1) Hasta que el motor responde por primera vez, ejecuta un ping real con
#    mongosh contra la IP del contenedor (no contra 127.0.0.1: durante la
#    inicialización el entrypoint levanta un mongod temporal solo en localhost,
#    lo que daría un falso positivo).
# 2) Una vez listo, deja un marcador en tmpfs y en adelante solo comprueba que
#    el puerto acepte conexiones TCP. mongosh es un proceso Node.js que consume
#    ~100 MB y CPU dentro del cgroup del motor; ejecutarlo periódicamente
#    contaminaría las mediciones, especialmente en PROFILE_C_SEVERE.
#
# Si mongod termina (p. ej. por OOM), el contenedor se detiene porque mongod es
# el proceso principal, por lo que el chequeo TCP es suficiente tras el arranque.
set -euo pipefail

MARKER=/run/bench/mongo-ready
PORT=27017
HOST="$(hostname -i | awk '{print $1}')"

if [[ -f "${MARKER}" ]]; then
  exec 3<>"/dev/tcp/${HOST}/${PORT}"
  exit 0
fi

mongosh --quiet --norc --host "${HOST}" --port "${PORT}" \
  --eval 'quit(db.adminCommand({ ping: 1 }).ok === 1 ? 0 : 1)'

touch "${MARKER}"

