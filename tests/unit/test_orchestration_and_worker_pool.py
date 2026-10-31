"""Pruebas unitarias de la matriz de 108 escenarios, checkpoints atómicos, recuperación y pool de workers."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "runner" / "python" / "src"
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from load.worker_pool import run_concurrent_workload  # noqa: E402
from orchestration.matrix import (  # noqa: E402
    CheckpointStore,
    expand_matrix_scenarios,
    generate_unique_run_id,
)
from orchestration.background_ctl import (  # noqa: E402
    get_status_report,
    write_lock,
)
from reporting.incremental_writer import (  # noqa: E402
    append_normalized_result,
    write_immutable_file,
)


class _FakeSession:
    """Sesión simulada que registra el instante exacto de apertura de conexión vs ejecución."""

    def __init__(
        self,
        wid: int,
        connect_times: list[float],
        exec_times: list[float],
        lock: threading.Lock,
    ) -> None:
        self.wid = wid
        self._connect_times = connect_times
        self._exec_times = exec_times
        self._lock = lock
        self.closed = False

    def warmup_connection(self) -> None:
        time.sleep(0.01)
        with self._lock:
            self._connect_times.append(time.perf_counter())

    def execute_once(self) -> int:
        with self._lock:
            self._exec_times.append(time.perf_counter())
        time.sleep(0.005)
        return 10

    def close(self) -> None:
        self.closed = True


class OrchestrationAndWorkerPoolTests(unittest.TestCase):
    def test_matrix_expands_to_108_official_scenarios_and_162_with_all_q2_variants(self) -> None:
        scenarios = expand_matrix_scenarios(include_all_q2_variants=False)
        self.assertEqual(len(scenarios), 108)

        engines = {s.engine for s in scenarios}
        profiles = {s.profile_id for s in scenarios}
        queries = {s.query_code for s in scenarios}
        concurrencies = {s.concurrency for s in scenarios}
        reps = {s.repetition for s in scenarios}

        self.assertEqual(engines, {"mongodb", "postgresql"})
        self.assertEqual(
            profiles, {"PROFILE_A_BASELINE", "PROFILE_B_MEDIUM", "PROFILE_C_SEVERE"}
        )
        self.assertEqual(queries, {"Q1", "Q2"})
        self.assertEqual(concurrencies, {1, 10, 50})
        self.assertEqual(reps, {1, 2, 3})

        for s in scenarios:
            self.assertEqual(s.warmup_seconds, 60.0)
            self.assertEqual(s.measurement_seconds, 300.0)
            self.assertEqual(s.cooldown_seconds, 60.0)

        extended = expand_matrix_scenarios(include_all_q2_variants=True)
        self.assertEqual(len(extended), 162)

    def test_checkpoint_preserves_ok_spill_oom_and_recovers_interrupted_and_retries_errors(
        self,
    ) -> None:
        scenarios = expand_matrix_scenarios()
        with tempfile.TemporaryDirectory() as tmp:
            cp_path = Path(tmp) / "checkpoint.json"
            cp = CheckpointStore(cp_path, scenarios=scenarios)
            self.assertEqual(len(cp.pending_keys()), 108)

            # 1) Escenario 1 termina en `ok`
            r1 = generate_unique_run_id(1, attempt=1)
            cp.mark_in_progress(scenarios[0], run_id=r1, attempt=1)
            cp.record_completion(
                scenarios[0],
                run_id=r1,
                status="ok",
                spill_detected=False,
                oom_detected=False,
            )

            # 2) Escenario 2 termina en `spill` (debe quedar registrado como first_spill y válido)
            r2 = generate_unique_run_id(2, attempt=1)
            cp.mark_in_progress(scenarios[1], run_id=r2, attempt=1)
            cp.record_completion(
                scenarios[1],
                run_id=r2,
                status="spill",
                spill_detected=True,
                oom_detected=False,
            )

            # 3) Escenario 3 termina en `oom` (debe conservarse como resultado válido y no repetirse)
            r3 = generate_unique_run_id(3, attempt=1)
            cp.mark_in_progress(scenarios[2], run_id=r3, attempt=1)
            cp.record_completion(
                scenarios[2],
                run_id=r3,
                status="oom",
                spill_detected=False,
                oom_detected=True,
            )

            # 4) Escenario 4 termina en `error` de infraestructura
            r4 = generate_unique_run_id(4, attempt=1)
            cp.mark_in_progress(scenarios[3], run_id=r4, attempt=1)
            cp.record_completion(
                scenarios[3],
                run_id=r4,
                status="error",
                spill_detected=False,
                oom_detected=False,
                reason="Docker socket timeout",
            )

            # 5) Escenario 5 queda `in_progress` cuando el proceso muere inesperadamente
            r5 = generate_unique_run_id(5, attempt=1)
            cp.mark_in_progress(scenarios[4], run_id=r5, attempt=1)

            # Al reabrir el CheckpointStore (`--resume`), el escenario 5 debe pasar
            # automáticamente a `interrupted` y volver a estar elegible.
            recovered = CheckpointStore(cp_path, scenarios=scenarios)
            self.assertEqual(recovered.completed_count(), 3)
            self.assertEqual(recovered.failed_count(), 1)
            self.assertEqual(recovered.interrupted_count(), 1)

            # ok, spill y oom NO deben ejecutarse nunca en --resume
            self.assertFalse(recovered.should_execute(scenarios[0].scenario_key, retry_errors=True))
            self.assertFalse(recovered.should_execute(scenarios[1].scenario_key, retry_errors=True))
            self.assertFalse(recovered.should_execute(scenarios[2].scenario_key, retry_errors=True))

            # error NO se ejecuta sin --retry-errors, pero SÍ con --retry-errors
            self.assertFalse(
                recovered.should_execute(scenarios[3].scenario_key, retry_errors=False)
            )
            self.assertTrue(
                recovered.should_execute(scenarios[3].scenario_key, retry_errors=True)
            )

            # El escenario interrumpido SÍ debe ejecutarse al reanudar y su siguiente intento es 2
            self.assertTrue(
                recovered.should_execute(scenarios[4].scenario_key, retry_errors=False)
            )
            self.assertEqual(recovered.next_attempt_number(scenarios[4].scenario_key), 2)

            # Verificar first_spill
            first = recovered.first_spill()
            self.assertIsNotNone(first)
            assert first is not None
            self.assertEqual(first["run_id"], r2)
            self.assertEqual(first["scenario_key"], scenarios[1].scenario_key)

    def test_incremental_writer_never_overwrites_previous_results(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            csv_path = tmp_dir / "benchmark_results.csv"
            jsonl_path = tmp_dir / "benchmark_runs.jsonl"

            append_normalized_result(
                {"run_id": "run-001", "engine": "mongodb", "status": "ok", "throughput_qps": 12.5},
                csv_path=csv_path,
                jsonl_path=jsonl_path,
            )
            append_normalized_result(
                {"run_id": "run-002", "engine": "postgresql", "status": "spill", "throughput_qps": 9.1},
                csv_path=csv_path,
                jsonl_path=jsonl_path,
            )

            with csv_path.open("r", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["run_id"], "run-001")
            self.assertEqual(rows[1]["run_id"], "run-002")

            # write_immutable_file nunca sobrescribe si el archivo ya existe
            p1 = write_immutable_file(tmp_dir / "artifact.json", '{"v": 1}\n')
            p2 = write_immutable_file(tmp_dir / "artifact.json", '{"v": 2}\n')
            self.assertNotEqual(p1, p2)
            self.assertEqual(json.loads(p1.read_text(encoding="utf-8"))["v"], 1)
            self.assertEqual(json.loads(p2.read_text(encoding="utf-8"))["v"], 2)

    def test_worker_pool_initializes_all_connections_before_first_query_and_measures(self) -> None:
        connect_times: list[float] = []
        exec_times: list[float] = []
        lock = threading.Lock()
        sessions: list[_FakeSession] = []

        def factory(wid: int) -> _FakeSession:
            s = _FakeSession(wid, connect_times, exec_times, lock)
            with lock:
                sessions.append(s)
            return s

        res = run_concurrent_workload(
            session_factory=factory,
            concurrency=10,
            warmup_seconds=0.05,
            measurement_seconds=0.10,
            cooldown_seconds=0.02,
        )

        self.assertEqual(len(connect_times), 10)
        self.assertGreater(len(exec_times), 0)
        self.assertLessEqual(max(connect_times), min(exec_times))
        self.assertGreater(res.warmup_ops, 0)
        self.assertGreater(res.latency_summary.sample_count, 0)
        self.assertGreater(res.latency_summary.throughput_qps, 0.0)
        self.assertTrue(all(s.closed for s in sessions))

    def test_background_status_report_shows_all_required_fields(self) -> None:
        scenarios = expand_matrix_scenarios()
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            cp_path = tmp_dir / "checkpoint.json"
            lock_path = tmp_dir / "orchestrator.lock.json"

            cp = CheckpointStore(cp_path, scenarios=scenarios)
            cp.mark_in_progress(scenarios[0], run_id="run-001", attempt=1)
            cp.record_completion(
                scenarios[0],
                run_id="run-001",
                status="ok",
                spill_detected=False,
                oom_detected=False,
            )
            cp.mark_in_progress(scenarios[1], run_id="run-002", attempt=1)

            # Simulamos que el proceso actual (os.getpid()) es el orquestador en segundo plano
            import os

            write_lock(
                {"pid": os.getpid(), "started_at_utc": "2026-10-30T21:00:00Z", "log_file": "results/logs/bg.log"},
                lock_path=lock_path,
            )
            rep = get_status_report(checkpoint_path=cp_path, lock_path=lock_path)
            self.assertEqual(rep["general_state"], "RUNNING")
            self.assertTrue(rep["process_running"])
            self.assertEqual(rep["current_engine"], scenarios[1].engine)
            self.assertEqual(rep["current_profile"], scenarios[1].profile_id)
            self.assertEqual(rep["current_query"], scenarios[1].query_code)
            self.assertEqual(rep["current_concurrency"], scenarios[1].concurrency)
            self.assertEqual(rep["current_repetition"], scenarios[1].repetition)
            self.assertEqual(rep["completed_runs"], 1)
            self.assertEqual(rep["pending_runs"], 107)
            self.assertAlmostEqual(rep["progress_percent"], round(1 / 108 * 100.0, 2))


if __name__ == "__main__":
    unittest.main()
