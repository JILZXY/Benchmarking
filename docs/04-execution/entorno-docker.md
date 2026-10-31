# Entorno Docker del benchmark

## Servicios

| Servicio | Imagen | Perfil Compose | Límites | Volumen persistente |
| --- | --- | --- | --- | --- |
| `mongodb` | `mongo:8.0.12` | `mongodb` | perfil experimental | `mongodb_data`, `mongodb_configdb` |
| `postgresql` | `postgres:17.6-bookworm` | `postgresql` | perfil experimental | `postgresql_data` |
| `prometheus` | `prom/prometheus:v3.5.0` | siempre activo | 1 CPU / 1024 MB | `prometheus_data` |
| `cadvisor` | `gcr.io/cadvisor/cadvisor:v0.52.1` | siempre activo | 1 CPU / 512 MB | — |
| `node-exporter` | `prom/node-exporter:v1.9.1` | siempre activo | 0.5 CPU / 128 MB | — |
| `runner` | `eticore-bench-runner` (build, `python:3.12.11-slim-bookworm`) | `runner` | 4 CPU / 1536 MB | bind mounts de `dataset/`, `results/`, `evidence/` |

Ninguna imagen usa `latest`. Las versiones se pueden cambiar en `.env`.

## Archivos de configuración

| Archivo | Contenido |
| --- | --- |
| [`compose.yaml`](../../compose.yaml) | Servicios, límites, healthchecks, volúmenes |
| [`.env.example`](../../.env.example) | Imágenes, credenciales, dataset, puertos, límites de monitorización |
| [`config/profiles/*.env`](../../config/profiles) | CPU, RAM y caché del motor para cada perfil |
| [`config/mongodb/mongod.conf`](../../config/mongodb/mongod.conf) | WiredTiger, red, log de operaciones con `usedDisk` |
| [`config/postgresql/postgresql.conf`](../../config/postgresql/postgresql.conf) | Conexiones, memoria, `pg_stat_statements`, `log_temp_files = 0` |
| [`monitoring/prometheus/prometheus.yml`](../../monitoring/prometheus/prometheus.yml) | Captura cada 1 s de cAdvisor y Node Exporter |

## Uso

```powershell
Copy-Item .env.example .env          # ajustar credenciales

# Verificación del entorno (Docker, Compose, cgroups v2, capacidad)
python scripts/setup/verify_docker_env.py

# Levantar un motor con un perfil (la monitorización arranca sola)
.\scripts\setup\compose.ps1 c --profile postgresql up -d --wait

# Confirmar los límites efectivos dentro del contenedor
python scripts/setup/verify_docker_env.py --live

# Cambiar de motor: detener uno antes de levantar el otro
.\scripts\setup\compose.ps1 c stop postgresql
.\scripts\setup\compose.ps1 c --profile mongodb up -d --wait

# Ejecutar el runner (trabajo puntual)
.\scripts\setup\compose.ps1 c --profile runner run --rm runner

# Detener todo
.\scripts\setup\compose.ps1 a --profile "*" down
```

En Linux/macOS se usa `scripts/setup/compose.sh` con los mismos argumentos.

> Al pasar `--env-file`, Compose **deja de leer `.env` de forma automática**.
> Los wrappers siempre pasan `.env` y después el perfil, que tiene prioridad.

### Cambio de perfil

Un perfil se aplica recreando el contenedor del motor con las variables del
nuevo perfil (`up -d` detecta el cambio y lo recrea). Los datos se conservan en
el volumen. Recrear el contenedor también deja las cachés frías, igual para
ambos motores.

## Límites de recursos y cgroups v2

Verificado con Docker Engine 29.7.2, Compose v5.4.0, cgroups v2 (driver
`cgroupfs`) y kernel WSL2 6.18:

| Clave de Compose | Archivo cgroup v2 | Valor esperado (perfil C) |
| --- | --- | --- |
| `cpus: 1` | `cpu.max` | `100000 100000` |
| `mem_limit: 1024m` | `memory.max` | `1073741824` |
| `memswap_limit: 1024m` | `memory.swap.max` | `0` |

- `mem_swappiness` **no se usa**: cgroups v2 no lo soporta y Docker lo
  descarta. La ausencia de swap se garantiza con `memswap_limit == mem_limit`.
- Detalles y justificación de la memoria por perfil: [ADR-002](../decisions/ADR-002-limites-recursos-y-memoria.md).

## Aislamiento

- Los motores tienen perfiles de Compose distintos: levantar uno no inicia el
  otro, y el runner no tiene `depends_on` hacia ellos. Es el runner quien
  verifica que el motor esté listo.
- Monitorización y runner corren en contenedores propios, con cgroups y límites
  propios, fuera de la cuota del motor.
- El healthcheck de MongoDB solo usa `mongosh` hasta el primer éxito. Después
  hace una comprobación TCP, para no ejecutar periódicamente un proceso Node.js
  dentro del cgroup del motor. En PostgreSQL se usa `pg_isready`, que es ligero.
- `restart: "no"` en los motores, para que un OOM no quede oculto por un
  reinicio automático.

## Limitaciones conocidas (Docker Desktop / WSL2)

- Node Exporter mide la VM de WSL2, no Windows.
- La VM dispone de ~7.6 GiB. Con PROFILE_A (4 GiB para el motor) más la
  monitorización y el runner, la memoria reservable llega a ~7.1 GiB. Se
  recomienda subir `memory` en `%UserProfile%\.wslconfig` (p. ej. `12GB`) o
  reducir `RUNNER_MEM_LIMIT`.
- El runner monta `/var/run/docker.sock` para inspeccionar contenedores
  (OOMKilled, límites, logs). Esto equivale a dar acceso root al daemon; es
  aceptable solo en un laboratorio local.

