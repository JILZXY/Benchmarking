# Ejecución en segundo plano, control de estado y requisitos de energía

## 1. Scripts de control

Todos los scripts están disponibles tanto para Linux/WSL2 (`*.sh`) como para Windows PowerShell (`*.ps1`) dentro de [`scripts/benchmark/`](../../scripts/benchmark):

| Script (Bash / PowerShell) | Responsabilidad |
| --- | --- |
| `start_background.sh` / `start_background.ps1` | Ejecuta las validaciones previas (`verify_docker_env.py` + verificación SHA-256 del dataset), comprueba que no haya otra instancia en curso (`results/raw/orchestrator.lock.json`) y lanza el orquestador desacoplado de la terminal actual, guardando logs en `results/logs/background_start_<UTC>.log`. |
| `status.sh` / `status.ps1` | Muestra en JSON el estado general (`IDLE`, `RUNNING`, `PAUSED`, `STOPPED_OR_INTERRUPTED`, `COMPLETED`), motor actual, perfil actual, consulta actual, concurrencia, repetición, ejecuciones completadas, pendientes, fallidas, interrumpidas y porcentaje de progreso (`0..100 %`). |
| `stop.sh` / `stop.ps1` | Solicita una detención controlada (`results/raw/stop.requested` + señal de interrupción), reconcilia atómicamente `checkpoint.json` y libera el lockfile para poder reanudar más tarde. |
| `resume.sh` / `resume.ps1` | Valida el entorno Docker, recupera el estado desde `results/raw/checkpoint.json` y continúa en segundo plano desde la primera ejecución pendiente sin repetir corridas en estado `ok`, `spill` u `oom`. Acepta `--retry-errors` si se desea reintentar fallos de infraestructura. |
| `run_benchmarks.sh` / `run_benchmarks.ps1` | Ejecuta las validaciones previas y corre el orquestador en primer plano. |

> [!NOTE]
> El servicio `runner` en `compose.yaml` tiene `restart: "no"` (nunca `restart: always`), evitando que Docker reinicie por su cuenta la matriz sin pasar por el gestor de checkpoints.

## 2. Flujo de uso recomendado (Windows PowerShell)

```powershell
# 1. Iniciar toda la matriz en segundo plano (puede cerrarse la terminal después)
.\scripts\benchmark\start_background.ps1

# 2. Consultar el progreso en cualquier momento
.\scripts\benchmark\status.ps1

# 3. Detener de forma controlada si se necesita pausar el experimento
.\scripts\benchmark\stop.ps1

# 4. Reanudar exactamente desde la primera ejecución pendiente
.\scripts\benchmark\resume.ps1
```

## 3. Requisitos para mantener activo el equipo y Docker Desktop durante ejecuciones largas

La matriz oficial de **108 ejecuciones** (con `60 s` warmup + `300 s` medición + `60 s` cooldown = `7 minutos` por corrida) requiere aproximadamente **12.6 horas** continuas.

> [!IMPORTANT]
> Ejecutar un proceso en segundo plano permite cerrar la ventana de terminal (`PowerShell` / `bash`), pero **el proceso se detendrá si el sistema operativo anfitrión entra en suspensión, hibernación, cierra la sesión del usuario o apaga el motor de Docker Desktop / WSL2**.

### Configuración requerida en el equipo anfitrión (Windows + Docker Desktop WSL2)

1. **Alimentación y suspensión de Windows**:
   - Mantener el equipo conectado a la corriente eléctrica (AC).
   - En *Configuración → Sistema → Inicio/apagado y batería → Pantalla y suspensión*:
     - *Con corriente alterna, poner el dispositivo en estado de suspensión después de*: **Nunca** (`powercfg /change standby-timeout-ac 0`).
     - *Hibernación con corriente alterna*: **Nunca** (`powercfg /change hibernate-timeout-ac 0`).
   - Si es una laptop, configurar el cierre de tapa con corriente alterna en **No hacer nada** (o mantener la tapa abierta).
2. **Docker Desktop y WSL2**:
   - En *Docker Desktop → Settings → General*: desactivar **Resource Saver mode** (para que Docker Desktop no pause la máquina virtual Linux cuando no detecta actividad interactiva).
   - No reiniciar ni actualizar Docker Desktop ni ejecutar `wsl --shutdown` mientras el estado sea `RUNNING`.
3. **Recuperación ante cortes fortuitos**:
   - Si el equipo se apaga o reinicia por un corte de energía o actualización forzada, todos los resultados previos (`ok`, `spill`, `oom`) están sincronizados en disco (`os.fsync`) tanto en `results/normalized/benchmark_results.csv` como en `results/raw/checkpoint.json`. Basta con volver a iniciar Docker Desktop y ejecutar `.\scripts\benchmark\resume.ps1`.

