#!/usr/bin/env python3
"""Orquestador secuencial del benchmark ETICORE (MongoDB vs PostgreSQL JSONB) con checkpoints y recuperación.

Soporta:
- `--resume`: continúa automáticamente desde la primera ejecución pendiente,
  respetando las ejecuciones ya completadas con estado `ok`, `spill` u `oom`.
- `--retry-errors`: vuelve a intentar únicamente los escenarios que habían quedado
  en estado `error` (errores de infraestructura) además de los pendientes/interrumpidos.
- Escritura incremental en `results/normalized/benchmark_results.csv` y
  `results/normalized/benchmark_runs.jsonl` tras cada escenario, sin sobrescribir
  nunca resultados anteriores.
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
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "config").exists() else Path("/app")
SRC_DIR = Path(__file__).resolve().parents[1]
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from adapters.explain_runner import capture_and_save_explain  # noqa: E402
from adapters.mongodb.adapter import (  # noqa: E402
    MongoWorkloadAdapter,
    extract_mongo_spill_evidence,
)
from adapters.postgresql.adapter import (  # noqa: E402
    PostgresWorkloadAdapter,
    extract_postgres_spill_evidence,
)
from adapters.workload_registry import get_workload_definition  # noqa: E402
from load.common import resolve_dataset_and_manifest  # noqa: E402
from load.mongodb_loader import load_mongodb  # noqa: E402
from load.postgresql_loader import load_postgresql  # noqa: E402
from load.worker_pool import LoadExecutionResult, run_concurrent_workload  # noqa: E402
from metrics.latency import LatencySummary  # noqa: E402
from metrics.outcome import ExecutionOutcome, OomEvidence, SpillEvidence, classify_run  # noqa: E402
from metrics.prometheus_client import collect_container_telemetry  # noqa: E402
from orchestration.matrix import (  # noqa: E402
    CheckpointStore,
    MatrixScenario,
    expand_matrix_scenarios,
    generate_unique_run_id,
)
from reporting.incremental_writer import (  # noqa: E402
    DEFAULT_CSV_PATH,
    DEFAULT_JSONL_PATH,
    append_normalized_result,
    write_immutable_file,
)
from reporting.summary_generator import (  # noqa: E402
    DEFAULT_SUMMARY_DIR,
    build_summaries_from_csv,
)

STOP_FLAG_PATH = ROOT / "results" / "raw" / "stop.requested"
LOCK_FILE_PATH = ROOT / "results" / "raw" / "orchestrator.lock.json"


class _GracefulInterruption(Exception):
    """Excepción interna disparada cuando se recibe SIGINT o SIGTERM."""


def _run_compose(profile_short: str, args: list[str]) -> None:
    env_file = ROOT / ".env"
    if not env_file.exists():
        example = ROOT / ".env.example"
        if example.exists():
            env_file.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")

    profile_map = {
        "a": "profile_a_baseline.env",
        "b": "profile_b_medium.env",
        "c": "profile_c_severe.env",
    }
    prof_path = ROOT / "config" / "profiles" / profile_map[profile_short.lower()]
    cmd = [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        "--env-file",
        str(env_file),
        "--env-file",
        str(prof_path),
        *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(
            f"Fallo en docker compose ({' '.join(args)}): {proc.stderr.strip() or proc.stdout.strip()}"
        )


def _ensure_single_engine_with_profile(engine: str, profile_short: str) -> str:
    """Detiene el motor opuesto y levanta el motor activo con el perfil solicitado."""
    other = "postgresql" if engine == "mongodb" else "mongodb"
    _run_compose(profile_short, ["--profile", other, "stop", other])
    _run_compose(
        profile_short,
        ["--profile", engine, "up", "-d", "--wait", "--wait-timeout", "300"],
    )
    proc = subprocess.run(
        [
            "docker",
            "ps",
            "--no-trunc",
            "-q",
            "--filter",
            f"label=com.eticore.engine={engine}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    ids = proc.stdout.strip().split()
    if not ids:
        raise RuntimeError(f"No se encontró el contenedor en ejecución para {engine}")
    return ids[0]


def _inspect_container_oom_and_logs(
    container_id: str, since_iso: str
) -> tuple[OomEvidence, list[str]]:
    """Inspecciona el estado `OOMKilled` del contenedor y extrae sus logs recientes."""
    insp = subprocess.run(
        ["docker", "inspect", container_id],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    oom_sources: list[str] = []
    oom_details: dict[str, Any] = {}
    if insp.returncode == 0:
        data = json.loads(insp.stdout)[0]
        state = data.get("State", {})
        if state.get("OOMKilled") is True or state.get("ExitCode") == 137:
            oom_sources.append("docker.State.OOMKilled")
            oom_details["State"] = {
                "Status": state.get("Status"),
                "ExitCode": state.get("ExitCode"),
                "OOMKilled": state.get("OOMKilled"),
            }

    logs_proc = subprocess.run(
        ["docker", "logs", "--since", since_iso, container_id],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    combined_logs = (logs_proc.stdout or "") + "\n" + (logs_proc.stderr or "")
    lines = [ln.strip() for ln in combined_logs.splitlines() if ln.strip()]

    for ln in lines:
        low = ln.lower()
        if "out of memory" in low or "oom-kill" in low or "tcmalloc: large alloc" in low:
            if "engine.log.oom" not in oom_sources:
                oom_sources.append("engine.log.oom")
            oom_details.setdefault("log_oom_samples", []).append(ln[:300])

    return (
        OomEvidence(detected=len(oom_sources) > 0, sources=tuple(oom_sources), details=oom_details),
        lines,
    )


def _snapshot_engine_counters(engine: str) -> dict[str, Any]:
    """Toma una instantánea de los contadores internos de spill del motor."""
    if engine == "mongodb":
        try:
            from pymongo import MongoClient
            from load.common import MongoConnectionConfig

            cfg = MongoConnectionConfig.from_env()
            with MongoClient(cfg.uri, serverSelectionTimeoutMS=5_000) as client:
                return dict(client[cfg.database].command("serverStatus"))
        except ImportError:
            user = os.environ.get("MONGO_ROOT_USER", "bench")
            pw = os.environ.get("MONGO_ROOT_PASSWORD", "change-me-mongo")
            proc = subprocess.run(
                [
                    "docker",
                    "exec",
                    "eticore-bench-mongodb-1",
                    "mongosh",
                    "--quiet",
                    "-u",
                    user,
                    "-p",
                    pw,
                    "--authenticationDatabase",
                    "admin",
                    "--eval",
                    "JSON.stringify(db.serverStatus())",
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=True,
            )
            return dict(json.loads(proc.stdout.strip().splitlines()[-1]))
    else:
        try:
            import psycopg
            from load.common import PostgresConnectionConfig

            cfg = PostgresConnectionConfig.from_env()
            with psycopg.connect(cfg.conninfo) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT temp_files, temp_bytes FROM pg_stat_database WHERE datname = %s;",
                        (cfg.database,),
                    )
                    row = cur.fetchone()
                    return {
                        "temp_files": int(row[0] or 0) if row else 0,
                        "temp_bytes": int(row[1] or 0) if row else 0,
                    }
        except ImportError:
            user = os.environ.get("POSTGRES_USER", "bench")
            db = os.environ.get("POSTGRES_DB", "eticore")
            proc = subprocess.run(
                [
                    "docker",
                    "exec",
                    "eticore-bench-postgresql-1",
                    "psql",
                    "-U",
                    user,
                    "-d",
                    db,
                    "-Atc",
                    f"SELECT coalesce(temp_files,0)||'|'||coalesce(temp_bytes,0) FROM pg_stat_database WHERE datname='{db}';",
                ],
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                check=True,
            )
            parts = proc.stdout.strip().split("|")
            return {
                "temp_files": int(parts[0]) if len(parts) == 2 else 0,
                "temp_bytes": int(parts[1]) if len(parts) == 2 else 0,
            }


def execute_single_scenario(
    scenario: MatrixScenario,
    run_id: str,
    attempt: int,
    dataset_path: Path,
    manifest_path: Path,
    current_engine_profile: tuple[str | None, str | None],
    loaded_engines: set[str],
    explained_keys: set[str],
    output_root: Path | None = None,
) -> tuple[dict[str, Any], tuple[str, str]]:
    """Ejecuta un escenario completo de la matriz y persiste incrementalmente sus resultados."""
    started_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    target_pair = (scenario.engine, scenario.profile_id)

    container_id = _ensure_single_engine_with_profile(
        scenario.engine, scenario.profile_short_code
    )

    workload = get_workload_definition(
        query_code=scenario.query_code,
        engine=scenario.engine,
        variant_id=scenario.variant_id,
    )

    explain_key = f"{scenario.engine}__{scenario.profile_id}__{scenario.query_code}__{scenario.variant_id}"
    need_load = scenario.engine not in loaded_engines
    need_explain = explain_key not in explained_keys

    since_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    counters_before = _snapshot_engine_counters(scenario.engine)

    has_local_drivers = True
    try:
        if scenario.engine == "mongodb":
            import pymongo  # noqa: F401
        else:
            import psycopg  # noqa: F401
    except ImportError:
        has_local_drivers = False

    explain_summary: dict[str, Any] | None = None

    if has_local_drivers:
        if need_load:
            if scenario.engine == "mongodb":
                load_mongodb(
                    dataset_path=dataset_path,
                    manifest_path=manifest_path,
                    drop_existing=False,
                    verify_manifest_first=False,
                )
            else:
                load_postgresql(
                    dataset_path=dataset_path,
                    manifest_path=manifest_path,
                    drop_existing=False,
                    verify_manifest_first=False,
                )
            loaded_engines.add(scenario.engine)

        if need_explain:
            explain_summary = capture_and_save_explain(
                workload,
                profile_label=scenario.profile_id,
                run_id=run_id,
            )
            explained_keys.add(explain_key)

        if scenario.engine == "mongodb":
            adapter_m = MongoWorkloadAdapter.from_env()
            session_factory = lambda _wid: adapter_m.create_worker_session(workload)
        else:
            adapter_p = PostgresWorkloadAdapter.from_env()
            session_factory = lambda _wid: adapter_p.create_worker_session(workload)

        load_result = run_concurrent_workload(
            session_factory=session_factory,
            concurrency=scenario.concurrency,
            warmup_seconds=scenario.warmup_seconds,
            measurement_seconds=scenario.measurement_seconds,
            cooldown_seconds=scenario.cooldown_seconds,
        )
    else:
        # Ejecutar el worker dentro del contenedor `runner` (que tiene pymongo y psycopg[binary]
        # preinstalados y corre dentro de la red `benchmark` de Docker).
        rel_out = f"results/raw/.worker_{run_id}.json"
        host_out = ROOT / rel_out
        worker_cmd = [
            "--profile",
            "runner",
            "run",
            "--rm",
            "runner",
            "python",
            "/app/runner/src/orchestration/container_worker.py",
            "--engine",
            scenario.engine,
            "--profile-id",
            scenario.profile_id,
            "--query-code",
            scenario.query_code,
            "--variant-id",
            scenario.variant_id,
            "--concurrency",
            str(scenario.concurrency),
            "--warmup-seconds",
            str(scenario.warmup_seconds),
            "--measurement-seconds",
            str(scenario.measurement_seconds),
            "--cooldown-seconds",
            str(scenario.cooldown_seconds),
            "--run-id",
            run_id,
            "--output-json",
            f"/app/{rel_out}",
        ]
        if need_load:
            worker_cmd.append("--ensure-loaded")
        if need_explain:
            worker_cmd.append("--capture-explain")

        _run_compose(scenario.profile_short_code, worker_cmd)
        worker_payload = json.loads(host_out.read_text(encoding="utf-8"))
        try:
            host_out.unlink()
        except OSError:
            pass

        if need_load:
            loaded_engines.add(scenario.engine)
        if need_explain:
            explained_keys.add(explain_key)

        explain_summary = worker_payload.get("explain_summary")
        lr = worker_payload["load_result"]
        ls = lr["latency_summary"]
        eo = lr["execution_outcome"]
        load_result = LoadExecutionResult(
            concurrency=int(lr["concurrency"]),
            warmup_seconds_configured=float(lr["warmup_seconds_configured"]),
            measurement_seconds_configured=float(lr["measurement_seconds_configured"]),
            cooldown_seconds_configured=float(lr["cooldown_seconds_configured"]),
            connections_init_elapsed_ms=float(lr["connections_init_elapsed_ms"]),
            warmup_actual_seconds=float(lr["warmup_actual_seconds"]),
            measurement_actual_seconds=float(lr["measurement_actual_seconds"]),
            cooldown_actual_seconds=float(lr["cooldown_actual_seconds"]),
            warmup_ops=int(lr["warmup_ops"]),
            warmup_errors=int(lr["warmup_errors"]),
            measurement_start_epoch=float(lr["measurement_start_epoch"]),
            measurement_end_epoch=float(lr["measurement_end_epoch"]),
            latency_summary=LatencySummary(**ls),
            execution_outcome=ExecutionOutcome(
                completed=bool(eo["completed"]),
                total_ops=int(eo["total_ops"]),
                failed_ops=int(eo["failed_ops"]),
                error_samples=tuple(eo.get("error_samples", [])),
            ),
        )

    oom_ev, log_lines = _inspect_container_oom_and_logs(container_id, since_iso=since_iso)
    try:
        counters_after = _snapshot_engine_counters(scenario.engine)
    except Exception:  # noqa: BLE001
        counters_after = {}

    if scenario.engine == "mongodb":
        spill_logs = [ln for ln in log_lines if '"usedDisk":true' in ln or '"usedDisk": true' in ln]
        spill_ev = extract_mongo_spill_evidence(
            explain_doc=None,
            server_status_before=counters_before,
            server_status_after=counters_after,
            log_spill_lines=spill_logs,
        )
    else:
        spill_logs = [ln for ln in log_lines if "temporary file:" in ln]
        spill_ev = extract_postgres_spill_evidence(
            explain_json=None,
            pg_stat_db_before=counters_before,
            pg_stat_db_after=counters_after,
            log_temp_lines=spill_logs,
        )

    if explain_summary and explain_summary.get("spill_detected_in_plan") and not spill_ev.detected:
        spill_ev = SpillEvidence(
            detected=True,
            sources=("explain_plan",),
            details={"explain_file": explain_summary.get("saved_to")},
        )

    classification = classify_run(
        spill=spill_ev,
        oom=oom_ev,
        execution=load_result.execution_outcome,
    )

    manifest_data = (
        json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest_path.exists()
        else {}
    )
    completed_at_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    telemetry = collect_container_telemetry(
        container_id=container_id,
        start_epoch=load_result.measurement_start_epoch,
        end_epoch=load_result.measurement_end_epoch,
    )

    record: dict[str, Any] = {
        "run_id": run_id,
        "attempt": attempt,
        "sequence_index": scenario.sequence_index,
        "scenario_key": scenario.scenario_key,
        "engine": scenario.engine,
        "profile": scenario.profile_id,
        "profile_id": scenario.profile_id,
        "profile_cpus": scenario.profile_cpus,
        "profile_memory_mb": scenario.profile_memory_mb,
        "workload": f"{scenario.query_code}:{scenario.variant_id}",
        "query_code": scenario.query_code,
        "variant_id": scenario.variant_id,
        "workload_version": workload.version,
        "workload_sha256": workload.artifact_sha256,
        "concurrency": scenario.concurrency,
        "repetition": scenario.repetition,
        "warmup_seconds": load_result.warmup_actual_seconds,
        "measurement_seconds": load_result.measurement_actual_seconds,
        "cooldown_seconds": load_result.cooldown_actual_seconds,
        "connections_init_elapsed_ms": load_result.connections_init_elapsed_ms,
        "warmup_ops": load_result.warmup_ops,
        "total_ops": load_result.latency_summary.sample_count,
        "failed_ops": load_result.latency_summary.failed_count,
        "throughput": load_result.latency_summary.throughput_qps,
        "throughput_qps": load_result.latency_summary.throughput_qps,
        "latency_min_ms": load_result.latency_summary.latency_min_ms,
        "latency_mean_ms": load_result.latency_summary.latency_mean_ms,
        "latency_p50": load_result.latency_summary.latency_p50_ms,
        "latency_p50_ms": load_result.latency_summary.latency_p50_ms,
        "latency_p95": load_result.latency_summary.latency_p95_ms,
        "latency_p95_ms": load_result.latency_summary.latency_p95_ms,
        "latency_p99": load_result.latency_summary.latency_p99_ms,
        "latency_p99_ms": load_result.latency_summary.latency_p99_ms,
        "latency_max_ms": load_result.latency_summary.latency_max_ms,
        "cpu_throttled_seconds": telemetry.cpu_throttled_seconds,
        "container_cpu_cfs_throttled_seconds_total": telemetry.cpu_throttled_seconds,
        "memory_rss_peak_bytes": telemetry.memory_rss_peak_bytes,
        "container_memory_rss_bytes": telemetry.memory_rss_peak_bytes,
        "memory_rss_avg_bytes": telemetry.memory_rss_avg_bytes,
        "status": classification.status.value,
        "status_reason": classification.reason,
        "disk_spill_detected": classification.spill_detected,
        "spill_sources": list(spill_ev.sources),
        "spill_bytes": spill_ev.bytes_spilled,
        "oom_killed": classification.oom_detected,
        "oom_sources": list(oom_ev.sources),
        "container_id": container_id,
        "measurement_start_epoch": load_result.measurement_start_epoch,
        "measurement_end_epoch": load_result.measurement_end_epoch,
        "started_at_utc": started_at_utc,
        "completed_at_utc": completed_at_utc,
        "dataset_hash": manifest_data.get("sha256"),
        "dataset_sha256": manifest_data.get("sha256"),
        "dataset_records": manifest_data.get("record_count"),
        "dataset_seed": manifest_data.get("seed"),
    }

    # Escritura inmutable de artefactos crudos, telemetría Prometheus, logs, evidencias y CSV
    results_base = output_root or (ROOT / "results")
    raw_dir = results_base / "raw"
    write_immutable_file(
        raw_dir / f"{run_id}.json",
        json.dumps(record, indent=2, sort_keys=True) + "\n",
    )

    telemetry_dir = results_base / "telemetry"
    write_immutable_file(
        telemetry_dir / f"{run_id}.prometheus.json",
        json.dumps(telemetry.raw_series, indent=2, sort_keys=True) + "\n",
    )

    logs_dir = results_base / "logs"
    write_immutable_file(
        logs_dir / f"{run_id}.log",
        "\n".join(log_lines) + "\n",
    )

    if classification.spill_detected:
        write_immutable_file(
            ROOT / "evidence" / "spill" / f"{run_id}.spill.json",
            json.dumps(
                {
                    "run_id": run_id,
                    "scenario_key": scenario.scenario_key,
                    "sources": list(spill_ev.sources),
                    "bytes_spilled": spill_ev.bytes_spilled,
                    "details": spill_ev.details,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )

    if classification.oom_detected:
        write_immutable_file(
            ROOT / "evidence" / "oom" / f"{run_id}.oom.json",
            json.dumps(
                {
                    "run_id": run_id,
                    "scenario_key": scenario.scenario_key,
                    "sources": list(oom_ev.sources),
                    "details": oom_ev.details,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
        )

    csv_path = (results_base / "normalized" / "benchmark_results.csv") if output_root else DEFAULT_CSV_PATH
    jsonl_path = (results_base / "normalized" / "benchmark_runs.jsonl") if output_root else DEFAULT_JSONL_PATH
    sum_dir = (results_base / "summaries") if output_root else DEFAULT_SUMMARY_DIR

    append_normalized_result(record, csv_path=csv_path, jsonl_path=jsonl_path)
    build_summaries_from_csv(csv_path=csv_path, summary_dir=sum_dir)
    return record, target_pair


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orquestador secuencial con checkpoints y recuperación para ETICORE DB Benchmark."
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reanuda la ejecución desde la primera corrida pendiente respetando ok/spill/oom.",
    )
    parser.add_argument(
        "--retry-errors",
        action="store_true",
        help="Reintenta explícitamente los escenarios que terminaron previamente en estado 'error'.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra el estado de checkpoints (completadas, pendientes, interrumpidas, fallidas) sin ejecutar.",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=Path,
        default=None,
        help="Ruta personalizada al archivo de estado checkpoint.json.",
    )
    parser.add_argument(
        "--engine",
        choices=("mongodb", "postgresql"),
        default=None,
        help="Filtrar opcionalmente por un motor.",
    )
    parser.add_argument(
        "--profile",
        choices=("PROFILE_A_BASELINE", "PROFILE_B_MEDIUM", "PROFILE_C_SEVERE"),
        default=None,
        help="Filtrar opcionalmente por un perfil.",
    )
    parser.add_argument(
        "--query",
        choices=("Q1", "Q2"),
        default=None,
        help="Filtrar opcionalmente por una consulta.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        choices=(1, 10, 50),
        default=None,
        help="Filtrar opcionalmente por un nivel de concurrencia.",
    )
    parser.add_argument(
        "--max-runs",
        type=int,
        default=None,
        help="Limitar la cantidad de ejecuciones en esta invocación.",
    )
    parser.add_argument(
        "--warmup-seconds",
        type=float,
        default=None,
        help="Sobrescribir duración de warmup (por defecto 60 s).",
    )
    parser.add_argument(
        "--measurement-seconds",
        type=float,
        default=None,
        help="Sobrescribir duración de medición (por defecto 300 s).",
    )
    parser.add_argument(
        "--cooldown-seconds",
        type=float,
        default=None,
        help="Sobrescribir duración de cooldown (por defecto 60 s).",
    )
    parser.add_argument(
        "--include-all-q2-variants",
        action="store_true",
        help="Expandir las dos variantes de Q2 por motor (162 ejecuciones en lugar de 108).",
    )
    parser.add_argument("--dataset", type=Path, default=None, help="Ruta al archivo JSONL.")
    parser.add_argument("--manifest", type=Path, default=None, help="Ruta al manifiesto JSON.")
    parser.add_argument(
        "--output-root",
        type=Path,
        default=None,
        help="Directorio raíz alternativo de resultados (útil para smoke tests aislados).",
    )
    parser.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="Ruta opcional a archivo de log donde redirigir stdout y stderr.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.log_file is not None:
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
        log_fh = args.log_file.open("a", encoding="utf-8", buffering=1)
        sys.stdout = log_fh
        sys.stderr = log_fh
    all_scenarios = expand_matrix_scenarios(
        include_all_q2_variants=args.include_all_q2_variants,
        warmup_override=args.warmup_seconds,
        measurement_override=args.measurement_seconds,
        cooldown_override=args.cooldown_seconds,
    )

    checkpoint = (
        CheckpointStore(path=args.checkpoint_file, scenarios=all_scenarios)
        if args.checkpoint_file
        else CheckpointStore(scenarios=all_scenarios)
    )

    scenarios = list(all_scenarios)
    if args.engine:
        scenarios = [s for s in scenarios if s.engine == args.engine]
    if args.profile:
        scenarios = [s for s in scenarios if s.profile_id == args.profile]
    if args.query:
        scenarios = [s for s in scenarios if s.query_code == args.query]
    if args.concurrency is not None:
        scenarios = [s for s in scenarios if s.concurrency == args.concurrency]

    runnable = [
        s for s in scenarios if checkpoint.should_execute(s.scenario_key, retry_errors=args.retry_errors)
    ]

    if args.dry_run:
        summary = {
            "resume_mode": bool(args.resume),
            "retry_errors_mode": bool(args.retry_errors),
            "total_scenarios_in_selection": len(scenarios),
            "completed_valid_count": checkpoint.completed_count(),
            "failed_error_count": checkpoint.failed_count(),
            "interrupted_history_count": checkpoint.interrupted_count(),
            "pending_to_run_now": len(runnable),
            "next_scenario_to_run": runnable[0].scenario_key if runnable else None,
            "first_5_scenarios": [s.scenario_key for s in runnable[:5]],
            "last_scenario": runnable[-1].scenario_key if runnable else None,
            "first_spill_recorded": checkpoint.first_spill(),
            "warmup_seconds": scenarios[0].warmup_seconds if scenarios else 0,
            "measurement_seconds": scenarios[0].measurement_seconds if scenarios else 0,
            "cooldown_seconds": scenarios[0].cooldown_seconds if scenarios else 0,
        }
        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0

    ds_path, man_path = resolve_dataset_and_manifest(args.dataset, args.manifest)
    current_pair: tuple[str | None, str | None] = (None, None)
    loaded_engines: set[str] = set()
    explained_keys: set[str] = set()
    executed_now = 0

    active_scenario: MatrixScenario | None = None
    active_run_id: str | None = None

    def _handle_sig(signum: int, _frame: Any) -> None:
        raise _GracefulInterruption(f"Señal recibida: {signum}")

    prev_sigint = signal.signal(signal.SIGINT, _handle_sig)
    prev_sigterm = signal.signal(signal.SIGTERM, _handle_sig)

    try:
        for scenario in runnable:
            if STOP_FLAG_PATH.exists():
                print(
                    json.dumps(
                        {
                            "event": "stopped_by_request",
                            "next_pending_scenario": scenario.scenario_key,
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                break
            if args.max_runs is not None and executed_now >= args.max_runs:
                break

            attempt = checkpoint.next_attempt_number(scenario.scenario_key)
            run_id = generate_unique_run_id(scenario.sequence_index, attempt=attempt)
            active_scenario = scenario
            active_run_id = run_id

            checkpoint.mark_in_progress(scenario, run_id=run_id, attempt=attempt)
            print(
                json.dumps(
                    {
                        "event": "scenario_started",
                        "run_id": run_id,
                        "attempt": attempt,
                        "scenario_key": scenario.scenario_key,
                        "started_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

            try:
                record, current_pair = execute_single_scenario(
                    scenario=scenario,
                    run_id=run_id,
                    attempt=attempt,
                    dataset_path=ds_path,
                    manifest_path=man_path,
                    current_engine_profile=current_pair,
                    loaded_engines=loaded_engines,
                    explained_keys=explained_keys,
                    output_root=args.output_root,
                )
            except _GracefulInterruption as intr:
                checkpoint.mark_interrupted(scenario, run_id=run_id, reason=str(intr))
                print(
                    json.dumps(
                        {
                            "event": "interrupted",
                            "run_id": run_id,
                            "scenario_key": scenario.scenario_key,
                            "reason": str(intr),
                        },
                        sort_keys=True,
                    ),
                    flush=True,
                )
                return 130
            except Exception as exc:  # noqa: BLE001
                # Fallo de infraestructura (p. ej. Docker daemon no responde): se registra
                # como `error` tanto en el checkpoint como en un artefacto inmutable,
                # permitiendo reintentarlo con `--resume --retry-errors`.
                err_reason = f"Error de infraestructura: {type(exc).__name__}: {exc}"
                checkpoint.record_completion(
                    scenario=scenario,
                    run_id=run_id,
                    status="error",
                    spill_detected=False,
                    oom_detected=False,
                    attempt=attempt,
                    reason=err_reason,
                )
                err_record = {
                    "run_id": run_id,
                    "attempt": attempt,
                    "sequence_index": scenario.sequence_index,
                    "scenario_key": scenario.scenario_key,
                    "engine": scenario.engine,
                    "profile_id": scenario.profile_id,
                    "profile_cpus": scenario.profile_cpus,
                    "profile_memory_mb": scenario.profile_memory_mb,
                    "query_code": scenario.query_code,
                    "variant_id": scenario.variant_id,
                    "concurrency": scenario.concurrency,
                    "repetition": scenario.repetition,
                    "status": "error",
                    "status_reason": err_reason,
                    "disk_spill_detected": False,
                    "oom_killed": False,
                    "completed_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
                err_base = args.output_root or (ROOT / "results")
                write_immutable_file(
                    err_base / "raw" / f"{run_id}.json",
                    json.dumps(err_record, indent=2, sort_keys=True) + "\n",
                )
                err_csv = (
                    (err_base / "normalized" / "benchmark_results.csv")
                    if args.output_root
                    else DEFAULT_CSV_PATH
                )
                err_jsonl = (
                    (err_base / "normalized" / "benchmark_runs.jsonl")
                    if args.output_root
                    else DEFAULT_JSONL_PATH
                )
                append_normalized_result(err_record, csv_path=err_csv, jsonl_path=err_jsonl)
                executed_now += 1
                continue

            checkpoint.record_completion(
                scenario=scenario,
                run_id=record["run_id"],
                status=record["status"],
                spill_detected=record["disk_spill_detected"],
                oom_detected=record["oom_killed"],
                attempt=attempt,
                reason=record["status_reason"],
            )
            active_scenario = None
            active_run_id = None
            executed_now += 1

            print(
                json.dumps(
                    {
                        "run_id": record["run_id"],
                        "attempt": attempt,
                        "scenario_key": scenario.scenario_key,
                        "status": record["status"],
                        "throughput_qps": record["throughput_qps"],
                        "latency_p95_ms": record["latency_p95_ms"],
                        "disk_spill_detected": record["disk_spill_detected"],
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    finally:
        signal.signal(signal.SIGINT, prev_sigint)
        signal.signal(signal.SIGTERM, prev_sigterm)
        if LOCK_FILE_PATH.exists():
            try:
                lock_data = json.loads(LOCK_FILE_PATH.read_text(encoding="utf-8"))
                if int(lock_data.get("pid", -1)) == os.getpid():
                    LOCK_FILE_PATH.unlink()
            except Exception:  # noqa: BLE001
                pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
