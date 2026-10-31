#!/usr/bin/env python3
"""Ejecuta en el contenedor `runner` (o localmente si hay drivers) la fase de carga concurrente de un escenario.

Escribe el resultado serializado de `LoadExecutionResult` + `explain_summary` en el archivo JSON
indicado por `--output-json`. Permite que el orquestador en Windows ejecute todo el control de
Docker Compose, checkpoints y procesos en segundo plano usando únicamente la biblioteca estándar,
mientras que las 50 conexiones de `pymongo`/`psycopg` corren en Linux dentro de la red `benchmark`.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import json
from pathlib import Path
import sys

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "config").exists() else Path("/app")
SRC_DIR = Path(__file__).resolve().parents[1]
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from adapters.explain_runner import capture_and_save_explain  # noqa: E402
from adapters.mongodb.adapter import MongoWorkloadAdapter  # noqa: E402
from adapters.postgresql.adapter import PostgresWorkloadAdapter  # noqa: E402
from adapters.workload_registry import get_workload_definition  # noqa: E402
from load.common import resolve_dataset_and_manifest  # noqa: E402
from load.mongodb_loader import load_mongodb  # noqa: E402
from load.postgresql_loader import load_postgresql  # noqa: E402
from load.worker_pool import run_concurrent_workload  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Worker interno de carga concurrente para un escenario.")
    parser.add_argument("--engine", choices=("mongodb", "postgresql"), required=True)
    parser.add_argument("--profile-id", required=True)
    parser.add_argument("--query-code", choices=("Q1", "Q2"), required=True)
    parser.add_argument("--variant-id", required=True)
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--warmup-seconds", type=float, required=True)
    parser.add_argument("--measurement-seconds", type=float, required=True)
    parser.add_argument("--cooldown-seconds", type=float, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--ensure-loaded", action="store_true")
    parser.add_argument("--capture-explain", action="store_true")
    parser.add_argument("--dataset", type=Path, default=None)
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--output-json", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ds_path, man_path = resolve_dataset_and_manifest(args.dataset, args.manifest)

    if args.ensure_loaded:
        if args.engine == "mongodb":
            load_mongodb(
                dataset_path=ds_path,
                manifest_path=man_path,
                drop_existing=False,
                verify_manifest_first=False,
            )
        else:
            load_postgresql(
                dataset_path=ds_path,
                manifest_path=man_path,
                drop_existing=False,
                verify_manifest_first=False,
            )

    workload = get_workload_definition(
        query_code=args.query_code,
        engine=args.engine,
        variant_id=args.variant_id,
    )

    explain_summary = None
    if args.capture_explain:
        explain_summary = capture_and_save_explain(
            workload,
            profile_label=args.profile_id,
            run_id=args.run_id,
        )

    if args.engine == "mongodb":
        adapter_m = MongoWorkloadAdapter.from_env()
        session_factory = lambda _wid: adapter_m.create_worker_session(workload)
    else:
        adapter_p = PostgresWorkloadAdapter.from_env()
        session_factory = lambda _wid: adapter_p.create_worker_session(workload)

    load_result = run_concurrent_workload(
        session_factory=session_factory,
        concurrency=args.concurrency,
        warmup_seconds=args.warmup_seconds,
        measurement_seconds=args.measurement_seconds,
        cooldown_seconds=args.cooldown_seconds,
    )

    payload = {
        "explain_summary": explain_summary,
        "load_result": asdict(load_result),
    }
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())

