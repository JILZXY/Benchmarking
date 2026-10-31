"""Escritura incremental y sin sobrescritura de resultados del benchmark (CSV normalizado y JSONL).

Garantiza que:
- Cada ejecución terminada agregue inmediatamente una fila al CSV normalizado
  (`results/normalized/benchmark_results.csv`) y una línea al registro de auditoría
  (`results/normalized/benchmark_runs.jsonl`), con `flush()` y `os.fsync()`.
- Incluya todos los nombres de columnas requeridos por la sección 10 y el README:
  `run_id`, `engine`, `profile`, `workload`, `concurrency`, `repetition`, `status`,
  `throughput`, `latency_p50`, `latency_p95`, `latency_p99`, `cpu_throttled_seconds`,
  `memory_rss_peak_bytes`, `disk_spill_detected`, `oom_killed`, `dataset_hash`,
  además de los alias canónicos de Prometheus (`throughput_qps`, `latency_p50_ms`,
  `latency_p95_ms`, `latency_p99_ms`, `container_cpu_cfs_throttled_seconds_total`,
  `container_memory_rss_bytes`).
- Nunca se sobrescriban ni eliminen filas o archivos de ejecuciones previas.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "results").exists() else Path("/app")
DEFAULT_CSV_PATH = ROOT / "results" / "normalized" / "benchmark_results.csv"
DEFAULT_JSONL_PATH = ROOT / "results" / "normalized" / "benchmark_runs.jsonl"

NORMALIZED_CSV_COLUMNS: tuple[str, ...] = (
    # Columnas mínimas requeridas por el contrato de la Sección 10:
    "run_id",
    "engine",
    "profile",
    "workload",
    "concurrency",
    "repetition",
    "status",
    "throughput",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "cpu_throttled_seconds",
    "memory_rss_peak_bytes",
    "disk_spill_detected",
    "oom_killed",
    "dataset_hash",
    # Alias exactos de los KPIs del README §9 y metadatos completos de reproducibilidad:
    "throughput_qps",
    "latency_p50_ms",
    "latency_p95_ms",
    "latency_p99_ms",
    "latency_min_ms",
    "latency_mean_ms",
    "latency_max_ms",
    "container_cpu_cfs_throttled_seconds_total",
    "container_memory_rss_bytes",
    "memory_rss_avg_bytes",
    "attempt",
    "sequence_index",
    "scenario_key",
    "profile_id",
    "profile_cpus",
    "profile_memory_mb",
    "query_code",
    "variant_id",
    "workload_version",
    "workload_sha256",
    "warmup_seconds",
    "measurement_seconds",
    "cooldown_seconds",
    "connections_init_elapsed_ms",
    "warmup_ops",
    "total_ops",
    "failed_ops",
    "spill_bytes",
    "status_reason",
    "dataset_sha256",
    "dataset_records",
    "dataset_seed",
    "started_at_utc",
    "completed_at_utc",
)


def enrich_record_aliases(record: dict[str, Any]) -> dict[str, Any]:
    """Completa de forma bidireccional los alias de columnas requeridos en el CSV."""
    enriched = dict(record)
    enriched.setdefault("profile", enriched.get("profile_id", ""))
    enriched.setdefault("profile_id", enriched.get("profile", ""))

    q_code = enriched.get("query_code", "")
    variant = enriched.get("variant_id", "")
    default_wl = f"{q_code}:{variant}" if q_code and variant else (q_code or "")
    enriched.setdefault("workload", default_wl)

    qps = enriched.get("throughput_qps", enriched.get("throughput", 0.0))
    enriched.setdefault("throughput", qps)
    enriched.setdefault("throughput_qps", qps)

    for short_k, ms_k in (
        ("latency_p50", "latency_p50_ms"),
        ("latency_p95", "latency_p95_ms"),
        ("latency_p99", "latency_p99_ms"),
    ):
        val = enriched.get(ms_k, enriched.get(short_k, 0.0))
        enriched.setdefault(short_k, val)
        enriched.setdefault(ms_k, val)

    cpu_thr = enriched.get(
        "cpu_throttled_seconds",
        enriched.get("container_cpu_cfs_throttled_seconds_total", 0.0),
    )
    enriched["cpu_throttled_seconds"] = cpu_thr
    enriched["container_cpu_cfs_throttled_seconds_total"] = cpu_thr

    rss_peak = enriched.get(
        "memory_rss_peak_bytes",
        enriched.get("container_memory_rss_bytes", 0),
    )
    enriched["memory_rss_peak_bytes"] = rss_peak
    enriched["container_memory_rss_bytes"] = rss_peak

    ds_hash = enriched.get("dataset_sha256", enriched.get("dataset_hash", ""))
    enriched.setdefault("dataset_hash", ds_hash)
    enriched.setdefault("dataset_sha256", ds_hash)
    return enriched


def append_normalized_result(
    record: dict[str, Any],
    csv_path: Path = DEFAULT_CSV_PATH,
    jsonl_path: Path = DEFAULT_JSONL_PATH,
) -> None:
    """Agrega una fila al CSV normalizado y una línea al JSONL incremental sin sobrescribir."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    jsonl_path.parent.mkdir(parents=True, exist_ok=True)

    enriched = enrich_record_aliases(record)
    write_header = not csv_path.exists() or csv_path.stat().st_size == 0
    row = {col: enriched.get(col, "") for col in NORMALIZED_CSV_COLUMNS}

    with csv_path.open("a", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(NORMALIZED_CSV_COLUMNS))
        if write_header:
            writer.writeheader()
        writer.writerow(row)
        fh.flush()
        os.fsync(fh.fileno())

    with jsonl_path.open("a", encoding="utf-8", newline="\n") as jfh:
        jfh.write(json.dumps(enriched, sort_keys=True) + "\n")
        jfh.flush()
        os.fsync(jfh.fileno())


def write_immutable_file(path: Path, content: str) -> Path:
    """Escribe un archivo de resultado garantizando no sobrescribir nunca uno existente."""
    path.parent.mkdir(parents=True, exist_ok=True)
    target = path
    counter = 1
    while target.exists():
        target = path.with_name(f"{path.stem}__{counter}{path.suffix}")
        counter += 1

    tmp = target.with_suffix(target.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as fh:
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(target)
    return target
