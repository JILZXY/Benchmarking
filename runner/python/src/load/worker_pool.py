"""Ejecutor de carga concurrente con pre-inicialización de conexiones y barrera de sincronización.

Garantiza exactamente el mismo patrón de ejecución en MongoDB y PostgreSQL:
1. Cada uno de los `concurrency` workers abre y valida su conexión (ping / `SELECT 1`)
   ANTES del warmup, de modo que el handshake TCP, TLS/SCRAM y la creación de
   sesiones no contaminen ni el warmup ni la ventana de medición.
2. Todos los workers esperan en un `threading.Barrier(concurrency + 1)` hasta que
   el hilo coordinador abre simultáneamente el warmup.
3. Durante `warmup_seconds`, los workers ejecutan la consulta continuamente sin
   registrar sus muestras en las métricas oficiales de medición.
4. Al cumplirse `warmup_seconds`, el coordinador activa la ventana de medición
   (`measurement_seconds`). Todas las ejecuciones que finalizan dentro de esta
   ventana registran su latencia en milisegundos o su fallo.
5. Al cumplirse `measurement_seconds`, los workers se detienen, cierran sus
   conexiones y el coordinador espera `cooldown_seconds`.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import threading
import time
from typing import Any, Callable, Protocol

from metrics.latency import LatencySummary, summarize_latencies
from metrics.outcome import ExecutionOutcome


class WorkerSession(Protocol):
    """Sesión aislada de un worker contra el motor bajo prueba."""

    def warmup_connection(self) -> None:
        """Abre el socket y realiza un ping ligero antes de la barrera."""

    def execute_once(self) -> int:
        """Ejecuta una vez la consulta experimental y devuelve la cantidad de filas leídas."""

    def close(self) -> None:
        """Cierra la conexión al finalizar la corrida."""


SessionFactory = Callable[[int], WorkerSession]


@dataclass
class _WorkerBuffer:
    worker_id: int
    warmup_ops: int = 0
    warmup_errors: int = 0
    measurement_latencies_ms: list[float] = field(default_factory=list)
    measurement_errors: int = 0
    error_samples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LoadExecutionResult:
    concurrency: int
    warmup_seconds_configured: float
    measurement_seconds_configured: float
    cooldown_seconds_configured: float
    connections_init_elapsed_ms: float
    warmup_actual_seconds: float
    measurement_actual_seconds: float
    cooldown_actual_seconds: float
    warmup_ops: int
    warmup_errors: int
    measurement_start_epoch: float
    measurement_end_epoch: float
    latency_summary: LatencySummary
    execution_outcome: ExecutionOutcome


class _PhaseState:
    """Estado compartido atómico de la fase activa (`warmup` -> `measure` -> `stop`)."""

    def __init__(self) -> None:
        self.phase: str = "init"


def run_concurrent_workload(
    session_factory: SessionFactory,
    concurrency: int,
    warmup_seconds: float = 60.0,
    measurement_seconds: float = 300.0,
    cooldown_seconds: float = 60.0,
) -> LoadExecutionResult:
    """Ejecuta las tres etapas (pre-conexión -> warmup -> medición -> cooldown) con `concurrency` workers."""
    if concurrency <= 0:
        raise ValueError("concurrency debe ser mayor que 0")
    if warmup_seconds < 0 or measurement_seconds <= 0 or cooldown_seconds < 0:
        raise ValueError("Duraciones inválidas para warmup/measurement/cooldown")

    ready_barrier = threading.Barrier(concurrency + 1)
    start_barrier = threading.Barrier(concurrency + 1)
    state = _PhaseState()
    init_errors: list[str] = []
    init_lock = threading.Lock()

    def _worker_main(worker_id: int) -> _WorkerBuffer:
        buf = _WorkerBuffer(worker_id=worker_id)
        session: WorkerSession | None = None
        try:
            session = session_factory(worker_id)
            session.warmup_connection()
        except Exception as exc:  # noqa: BLE001
            with init_lock:
                init_errors.append(f"worker-{worker_id}: {type(exc).__name__}: {exc}")
            try:
                ready_barrier.wait(timeout=30.0)
            except threading.BrokenBarrierError:
                pass
            return buf

        ready_barrier.wait()
        start_barrier.wait()

        try:
            while state.phase != "stop":
                current_phase = state.phase
                t0 = time.perf_counter()
                try:
                    session.execute_once()
                    elapsed_ms = (time.perf_counter() - t0) * 1000.0
                    # Re-leemos la fase al terminar para contabilizar solo dentro de la ventana activa.
                    end_phase = state.phase
                    if current_phase == "warmup" and end_phase == "warmup":
                        buf.warmup_ops += 1
                    elif end_phase == "measure":
                        buf.measurement_latencies_ms.append(elapsed_ms)
                except Exception as exc:  # noqa: BLE001
                    err_msg = f"{type(exc).__name__}: {exc}"
                    if state.phase == "warmup":
                        buf.warmup_errors += 1
                    elif state.phase == "measure":
                        buf.measurement_errors += 1
                        if len(buf.error_samples) < 5:
                            buf.error_samples.append(err_msg)
                    # Pequeña pausa defensiva si el motor cayó (ej. OOM) para no ciclar en vacío.
                    time.sleep(0.05)
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:  # noqa: BLE001
                    pass

        return buf

    t_init_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="bench-worker") as pool:
        futures = [pool.submit(_worker_main, wid) for wid in range(concurrency)]

        # 1. Esperar a que todos los workers tengan su conexión abierta y validada.
        ready_barrier.wait()
        conn_init_ms = round((time.perf_counter() - t_init_start) * 1000.0, 3)

        if init_errors:
            state.phase = "stop"
            ready_barrier.abort()
            start_barrier.abort()
            for f in futures:
                f.result()
            now_epoch = time.time()
            empty_summary = summarize_latencies([], len(init_errors), measurement_seconds)
            outcome = ExecutionOutcome(
                completed=False,
                total_ops=0,
                failed_ops=len(init_errors),
                error_samples=tuple(init_errors[:5]),
            )
            return LoadExecutionResult(
                concurrency=concurrency,
                warmup_seconds_configured=warmup_seconds,
                measurement_seconds_configured=measurement_seconds,
                cooldown_seconds_configured=cooldown_seconds,
                connections_init_elapsed_ms=conn_init_ms,
                warmup_actual_seconds=0.0,
                measurement_actual_seconds=0.0,
                cooldown_actual_seconds=0.0,
                warmup_ops=0,
                warmup_errors=len(init_errors),
                measurement_start_epoch=now_epoch,
                measurement_end_epoch=now_epoch,
                latency_summary=empty_summary,
                execution_outcome=outcome,
            )

        # 2. Etapa de Warmup (conexiones ya abiertas).
        state.phase = "warmup"
        t_warmup_0 = time.perf_counter()
        start_barrier.wait()
        if warmup_seconds > 0:
            time.sleep(warmup_seconds)
        warmup_actual = round(time.perf_counter() - t_warmup_0, 4)

        # 3. Etapa de Medición.
        measurement_start_epoch = time.time()
        t_meas_0 = time.perf_counter()
        state.phase = "measure"
        time.sleep(measurement_seconds)
        state.phase = "stop"
        measurement_actual = round(time.perf_counter() - t_meas_0, 4)
        measurement_end_epoch = time.time()

        buffers = [f.result() for f in futures]

    # 4. Etapa de Cooldown (sin carga activa sobre el motor).
    t_cool_0 = time.perf_counter()
    if cooldown_seconds > 0:
        time.sleep(cooldown_seconds)
    cooldown_actual = round(time.perf_counter() - t_cool_0, 4)

    all_latencies: list[float] = []
    total_warmup_ops = 0
    total_warmup_errors = 0
    total_meas_errors = 0
    error_samples: list[str] = []

    for b in buffers:
        total_warmup_ops += b.warmup_ops
        total_warmup_errors += b.warmup_errors
        all_latencies.extend(b.measurement_latencies_ms)
        total_meas_errors += b.measurement_errors
        for s in b.error_samples:
            if len(error_samples) < 10:
                error_samples.append(s)

    summary = summarize_latencies(
        latencies_ms=all_latencies,
        failed_count=total_meas_errors,
        window_seconds=measurement_actual,
    )
    outcome = ExecutionOutcome(
        completed=len(all_latencies) > 0 or total_meas_errors == 0,
        total_ops=len(all_latencies),
        failed_ops=total_meas_errors,
        error_samples=tuple(error_samples),
    )

    return LoadExecutionResult(
        concurrency=concurrency,
        warmup_seconds_configured=warmup_seconds,
        measurement_seconds_configured=measurement_seconds,
        cooldown_seconds_configured=cooldown_seconds,
        connections_init_elapsed_ms=conn_init_ms,
        warmup_actual_seconds=warmup_actual,
        measurement_actual_seconds=measurement_actual,
        cooldown_actual_seconds=cooldown_actual,
        warmup_ops=total_warmup_ops,
        warmup_errors=total_warmup_errors,
        measurement_start_epoch=measurement_start_epoch,
        measurement_end_epoch=measurement_end_epoch,
        latency_summary=summary,
        execution_outcome=outcome,
    )

