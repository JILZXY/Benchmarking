# ADR-002 — Límites de recursos y memoria de los motores por perfil

- **Estado:** aceptada
- **Fecha:** 2026-10-30

## Contexto

Los perfiles experimentales (README §7) definen CPU y RAM del contenedor del
motor evaluado, con swappiness 0. El entorno real es Docker Engine 29.7.2,
Docker Compose v5.4.0 y cgroups v2 (Docker Desktop sobre WSL2).

Los motores no dimensionan su caché principal del mismo modo:

| Motor | Valor por defecto |
| --- | --- |
| MongoDB (WiredTiger) | `max(0.5 × (RAM − 1 GB), 0.25 GB)` usando el límite del cgroup |
| PostgreSQL | `shared_buffers = 128MB` sin importar la RAM disponible |

Con los defaults, PostgreSQL usaría 128 MB en los tres perfiles, mientras que
MongoDB usaría 1.5 GB en A. Eso mezclaría el efecto del perfil con una
diferencia de configuración.

## Decisión

1. **Límites del contenedor** (`compose.yaml`, ancla `x-engine-limits`):
   - `cpus` → `cpu.max` (cuota CFS). Se elige cuota y no `cpuset` porque el KPI
     `container_cpu_cfs_throttled_seconds_total` mide justamente la cuota CFS.
   - `mem_limit` → `memory.max`.
   - `memswap_limit = mem_limit` → `memory.swap.max = 0` (sin swap).
   - **No se usa `mem_swappiness`**: no está soportado en cgroups v2 y Docker
     lo descarta. El requisito "swappiness 0" se cumple impidiendo el swap.
   - `restart: "no"` y `oom_kill_disable: false`: un OOM debe quedar visible.
2. **Caché principal = 25 % de la RAM del perfil en ambos motores**, y
   `effective_cache_size` = 50 %:

   | Perfil | CPU | RAM | `wiredTigerCacheSizeGB` | `shared_buffers` | `effective_cache_size` |
   | --- | --: | --: | --: | --: | --: |
   | PROFILE_A_BASELINE | 4 | 4096 MB | 1 | 1024MB | 2048MB |
   | PROFILE_B_MEDIUM | 2 | 2048 MB | 0.5 | 512MB | 1024MB |
   | PROFILE_C_SEVERE | 1 | 1024 MB | 0.25 | 256MB | 512MB |

   25 % coincide con el default de MongoDB en B y C y con la recomendación
   habitual de PostgreSQL. 0.25 GB es el mínimo de WiredTiger.
3. **Variables de carga, no de perfil:** `work_mem` (4 MB / 64 MB) y
   `allowDiskUse` (false / true) las fija el runner por sesión/consulta en Q2.
   El límite de memoria de `$sort` de MongoDB
   (`internalQueryMaxBlockingSortMemoryUsageBytes`, 100 MB) se deja en su valor
   por defecto.
4. **Monitorización y runner fuera del cgroup del motor:** cada servicio tiene
   su propio contenedor y sus propios límites. Opcionalmente se pueden fijar
   núcleos disjuntos con `ENGINE_CPUSET`, `MONITORING_CPUSET` y `RUNNER_CPUSET`.

## Consecuencias

- Las diferencias entre perfiles reflejan el recurso disponible y no un cambio
  de configuración desigual entre motores.
- La memoria del contenedor incluye, además de la caché, conexiones, buffers
  de ordenamiento, `/dev/shm` (PostgreSQL) y la caché de páginas del kernel
  atribuida al cgroup.
- Cambiar los porcentajes requiere actualizar solo `config/profiles/*.env`.

