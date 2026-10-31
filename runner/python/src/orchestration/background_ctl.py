#!/usr/bin/env python3
"""Gestor de proceso en segundo plano, bloqueo de instancia única y reporte de estado.

Implementa la lógica compartida por:
- `start_background.sh` / `start_background.ps1`
- `status.sh` / `status.ps1`
- `stop.sh` / `stop.ps1`
- `resume.sh` / `resume.ps1`

Garantiza:
1. Bloqueo de exclusión mutua (`results/raw/orchestrator.lock.json`) validando si el
   PID registrado sigue vivo antes de permitir una nueva ejecución en segundo plano.
2. Creación del proceso desacoplado de la terminal actual (`CREATE_NEW_PROCESS_GROUP`
   + `DETACHED_PROCESS` en Windows; `start_new_session=True` en POSIX), redirigiendo
   stdout/stderr a un archivo de log persistente en `results/logs/`.
3. Detención controlada enviando `SIGINT`/`CTRL_BREAK_EVENT` o mediante un archivo
   centinela `results/raw/stop.requested` verificado por el orquestador.
4. Reporte completo de estado (`status`) leyendo atómicamente `checkpoint.json` y el
   estado del proceso en segundo plano.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "config").exists() else Path("/app")
SRC_DIR = Path(__file__).resolve().parents[1]
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from orchestration.matrix import (  # noqa: E402
    DEFAULT_CHECKPOINT_PATH,
    CheckpointStore,
    expand_matrix_scenarios,
)

DEFAULT_LOCK_PATH = ROOT / "results" / "raw" / "orchestrator.lock.json"
DEFAULT_STOP_FLAG_PATH = ROOT / "results" / "raw" / "stop.requested"
DEFAULT_BG_LOG_DIR = ROOT / "results" / "logs"


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_pid_alive(pid: int) -> bool:
    """Comprueba si un proceso con identificador `pid` sigue activo en el sistema."""
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        ERROR_ACCESS_DENIED = 5
        STILL_ACTIVE = 259
        handle = kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid)
        )
        if not handle:
            return ctypes.get_last_error() == ERROR_ACCESS_DENIED
        try:
            exit_code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return int(exit_code.value) == STILL_ACTIVE
            return True
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def read_lock(lock_path: Path = DEFAULT_LOCK_PATH) -> dict[str, Any] | None:
    if not lock_path.exists():
        return None
    try:
        return json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return None


def write_lock(data: dict[str, Any], lock_path: Path = DEFAULT_LOCK_PATH) -> None:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = lock_path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(json.dumps(data, indent=2, sort_keys=True) + "\n")
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, lock_path)


def clear_lock(lock_path: Path = DEFAULT_LOCK_PATH) -> None:
    if lock_path.exists():
        try:
            lock_path.unlink()
        except OSError:
            pass


def get_status_report(
    checkpoint_path: Path = DEFAULT_CHECKPOINT_PATH,
    lock_path: Path = DEFAULT_LOCK_PATH,
) -> dict[str, Any]:
    """Construye el reporte completo requerido por `status.sh` / `status.ps1`."""
    scenarios = expand_matrix_scenarios()
    total = len(scenarios)

    # No mutar el checkpoint en disco si el orquestador está corriendo en vivo:
    # leemos el JSON tal cual si el proceso está activo.
    lock = read_lock(lock_path)
    pid = int(lock["pid"]) if lock and "pid" in lock else 0
    running = is_pid_alive(pid)

    if checkpoint_path.exists():
        raw_state = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    else:
        raw_state = {}

    completed_map: dict[str, Any] = raw_state.get("completed", {})
    failed_map: dict[str, Any] = raw_state.get("failed", {})
    interrupted_list: list[Any] = raw_state.get("interrupted", [])
    in_progress: dict[str, Any] | None = raw_state.get("in_progress")

    completed_count = len(completed_map)
    failed_count = len(failed_map)
    pending_count = max(0, total - completed_count)
    progress_pct = round((completed_count / total) * 100.0, 2) if total > 0 else 0.0

    if running:
        general_state = "RUNNING"
    elif DEFAULT_STOP_FLAG_PATH.exists() or (in_progress is not None and not running):
        general_state = "STOPPED_OR_INTERRUPTED"
    elif completed_count >= total and total > 0:
        general_state = "COMPLETED"
    elif completed_count > 0 or failed_count > 0 or len(interrupted_list) > 0:
        general_state = "PAUSED"
    else:
        general_state = "IDLE"

    # Si no hay corrida en curso, mostrar la siguiente pendiente como referencia
    next_pending_scenario = next(
        (s for s in scenarios if s.scenario_key not in completed_map), None
    )

    current_info: dict[str, Any]
    if in_progress:
        current_info = {
            "run_id": in_progress.get("run_id"),
            "scenario_key": in_progress.get("scenario_key"),
            "engine": in_progress.get("engine"),
            "profile_id": in_progress.get("profile_id"),
            "query_code": in_progress.get("query_code"),
            "variant_id": in_progress.get("variant_id"),
            "concurrency": in_progress.get("concurrency"),
            "repetition": in_progress.get("repetition"),
            "started_at_utc": in_progress.get("started_at_utc"),
        }
    elif next_pending_scenario is not None:
        current_info = {
            "run_id": None,
            "scenario_key": next_pending_scenario.scenario_key,
            "engine": next_pending_scenario.engine,
            "profile_id": next_pending_scenario.profile_id,
            "query_code": next_pending_scenario.query_code,
            "variant_id": next_pending_scenario.variant_id,
            "concurrency": next_pending_scenario.concurrency,
            "repetition": next_pending_scenario.repetition,
            "started_at_utc": None,
        }
    else:
        current_info = {
            "run_id": None,
            "scenario_key": None,
            "engine": None,
            "profile_id": None,
            "query_code": None,
            "variant_id": None,
            "concurrency": None,
            "repetition": None,
            "started_at_utc": None,
        }

    return {
        "general_state": general_state,
        "process_running": running,
        "pid": pid if running else None,
        "log_file": lock.get("log_file") if lock else None,
        "current_engine": current_info["engine"],
        "current_profile": current_info["profile_id"],
        "current_query": current_info["query_code"],
        "current_variant": current_info["variant_id"],
        "current_concurrency": current_info["concurrency"],
        "current_repetition": current_info["repetition"],
        "current_scenario_key": current_info["scenario_key"],
        "current_run_id": current_info["run_id"],
        "completed_runs": completed_count,
        "pending_runs": pending_count,
        "failed_runs": failed_count,
        "interrupted_runs": len(interrupted_list),
        "total_runs": total,
        "progress_percent": progress_pct,
        "first_spill": raw_state.get("first_spill"),
        "updated_at_utc": raw_state.get("updated_at_utc"),
    }


def start_detached(
    runner_args: list[str],
    resume: bool = False,
    lock_path: Path = DEFAULT_LOCK_PATH,
) -> dict[str, Any]:
    """Valida que no exista otra instancia activa e inicia el orquestador en segundo plano."""
    existing = read_lock(lock_path)
    if existing and is_pid_alive(int(existing.get("pid", 0))):
        raise RuntimeError(
            f"Ya existe una ejecución en segundo plano activa con PID={existing['pid']} "
            f"(iniciada en {existing.get('started_at_utc')}, log={existing.get('log_file')})."
        )

    if DEFAULT_STOP_FLAG_PATH.exists():
        DEFAULT_STOP_FLAG_PATH.unlink()

    DEFAULT_BG_LOG_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    mode_tag = "resume" if resume else "start"
    log_file = DEFAULT_BG_LOG_DIR / f"background_{mode_tag}_{ts}.log"

    runner_script = ROOT / "runner" / "python" / "src" / "orchestration" / "runner.py"
    cmd = [sys.executable, "-u", str(runner_script), "--log-file", str(log_file)]
    if resume and "--resume" not in runner_args:
        cmd.append("--resume")
    cmd.extend(runner_args)

    pid = 0
    if os.name == "nt":
        # En Windows, Invoke-CimMethod Win32_Process.Create lanza el proceso bajo
        # WmiPrvSE.exe, completamente fuera de cualquier Job Object de la terminal,
        # por lo que sobrevive al cerrar la sesión o terminal interactiva.
        cmd_line = subprocess.list2cmdline(cmd)
        ps_cmd = (
            "$r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create "
            f"-Arguments @{{CommandLine = {json.dumps(cmd_line)}; CurrentDirectory = {json.dumps(str(ROOT))}}}; "
            "if ($r.ReturnValue -eq 0) { Write-Output $r.ProcessId } else { exit 1 }"
        )
        wmi_proc = subprocess.run(
            ["powershell", "-NoProfile", "-Command", ps_cmd],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        if wmi_proc.returncode == 0 and wmi_proc.stdout.strip().isdigit():
            pid = int(wmi_proc.stdout.strip())
        else:
            CREATE_NEW_PROCESS_GROUP = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0x00000200)
            CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)
            proc = subprocess.Popen(
                cmd,
                cwd=str(ROOT),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                stdin=subprocess.DEVNULL,
                close_fds=True,
                creationflags=CREATE_NEW_PROCESS_GROUP | CREATE_NO_WINDOW,
            )
            pid = proc.pid
    else:
        log_fh = log_file.open("a", encoding="utf-8", buffering=1)
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=log_fh,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            close_fds=True,
            start_new_session=True,
        )
        pid = proc.pid
    lock_info = {
        "pid": pid,
        "mode": mode_tag,
        "command": cmd,
        "started_at_utc": _utc_now(),
        "log_file": str(log_file.relative_to(ROOT)).replace("\\", "/"),
    }
    write_lock(lock_info, lock_path=lock_path)
    return lock_info


def request_graceful_stop(
    lock_path: Path = DEFAULT_LOCK_PATH,
    checkpoint_path: Path = DEFAULT_CHECKPOINT_PATH,
    wait_seconds: float = 10.0,
) -> dict[str, Any]:
    """Solicita una detención controlada, guarda el estado en el checkpoint y termina el proceso."""
    DEFAULT_STOP_FLAG_PATH.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_STOP_FLAG_PATH.write_text(_utc_now() + "\n", encoding="utf-8")

    lock = read_lock(lock_path)
    if not lock:
        return {"stopped": True, "detail": "No había ningún proceso registrado en el lockfile."}

    pid = int(lock.get("pid", 0))
    if not is_pid_alive(pid):
        # Asegurar que si había quedado in_progress en checkpoint se reclasifique a interrupted
        CheckpointStore(path=checkpoint_path, scenarios=expand_matrix_scenarios())
        clear_lock(lock_path)
        return {"stopped": True, "pid": pid, "detail": "El proceso ya había finalizado."}

    if os.name == "nt":
        # En Windows enviamos CTRL_BREAK_EVENT o terminamos el árbol del proceso runner
        try:
            os.kill(pid, getattr(signal, "CTRL_BREAK_EVENT", signal.SIGTERM))
        except Exception:  # noqa: BLE001
            pass
    else:
        try:
            os.kill(pid, signal.SIGINT)
        except Exception:  # noqa: BLE001
            pass

    deadline = time.monotonic() + wait_seconds
    while time.monotonic() < deadline and is_pid_alive(pid):
        time.sleep(0.25)

    if is_pid_alive(pid):
        if os.name == "nt":
            subprocess.run(
                ["taskkill", "/PID", str(pid), "/T", "/F"],
                capture_output=True,
                text=True,
            )
        else:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:  # noqa: BLE001
                pass

    # Reconciliar inmediatamente `in_progress` -> `interrupted` de forma atómica
    CheckpointStore(path=checkpoint_path, scenarios=expand_matrix_scenarios())
    clear_lock(lock_path)
    return {
        "stopped": not is_pid_alive(pid),
        "pid": pid,
        "detail": "Detención controlada completada y estado guardado en checkpoint.json.",
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Controlador de ejecución en segundo plano para ETICORE DB Benchmark."
    )
    sub = parser.add_subparsers(dest="action", required=True)

    p_start = sub.add_parser("start", help="Inicia el benchmark en segundo plano.")
    p_start.add_argument("runner_args", nargs=argparse.REMAINDER)

    p_resume = sub.add_parser("resume", help="Reanuda el benchmark en segundo plano.")
    p_resume.add_argument("runner_args", nargs=argparse.REMAINDER)

    sub.add_parser("status", help="Muestra el progreso y estado actual.")
    sub.add_parser("stop", help="Solicita detención controlada y guarda el estado.")

    known, unknown = parser.parse_known_args(argv)
    if hasattr(known, "runner_args"):
        known.runner_args = list(known.runner_args) + list(unknown)
    elif unknown:
        parser.error(f"Argumentos no reconocidos: {' '.join(unknown)}")
    return known


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.action == "status":
        report = get_status_report()
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    if args.action == "stop":
        res = request_graceful_stop()
        print(json.dumps(res, indent=2, sort_keys=True))
        return 0

    extra = [a for a in getattr(args, "runner_args", []) if a != "--"]
    info = start_detached(extra, resume=(args.action == "resume"))
    print(json.dumps(info, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

