#!/usr/bin/env python3
"""Captura y conserva los planes de ejecución (`explain`) de Q1 y Q2 junto con la versión de la consulta.

Guarda los artefactos en:
  - `evidence/explain/mongodb/<run_or_profile>__<query>__<variant>.explain.json`
  - `evidence/explain/postgresql/<run_or_profile>__<query>__<variant>.explain.json`
y si detecta spill en el propio plan, guarda además un registro en `evidence/spill/`.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "workloads").exists() else Path("/app")
SRC_DIR = Path(__file__).resolve().parents[1]
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from adapters.mongodb.adapter import MongoWorkloadAdapter, extract_mongo_spill_evidence  # noqa: E402
from adapters.postgresql.adapter import (  # noqa: E402
    PostgresWorkloadAdapter,
    extract_postgres_spill_evidence,
)
from adapters.workload_registry import (  # noqa: E402
    WorkloadDefinition,
    list_workload_definitions,
)


def capture_and_save_explain(
    workload: WorkloadDefinition,
    profile_label: str = "manual",
    run_id: str | None = None,
    output_root: Path | None = None,
) -> dict[str, Any]:
    """Ejecuta `EXPLAIN` para `workload`, extrae evidencia de spill y guarda el artefacto JSON."""
    base_dir = output_root or (ROOT / "evidence" / "explain" / workload.engine)
    base_dir.mkdir(parents=True, exist_ok=True)

    tag = run_id or profile_label
    filename = f"{tag}__{workload.query_code.lower()}__{workload.variant_id}.explain.json"
    out_path = base_dir / filename

    if workload.engine == "mongodb":
        adapter_m = MongoWorkloadAdapter.from_env()
        raw_plan: Any = adapter_m.explain_workload(workload, verbosity="executionStats")
        spill_ev = extract_mongo_spill_evidence(explain_doc=raw_plan)
    elif workload.engine == "postgresql":
        adapter_p = PostgresWorkloadAdapter.from_env()
        raw_plan = adapter_p.explain_workload(workload, analyze=True)
        spill_ev = extract_postgres_spill_evidence(explain_json=raw_plan)
    else:
        raise ValueError(f"Motor no soportado: {workload.engine!r}")

    envelope: dict[str, Any] = {
        "captured_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "run_id": run_id,
        "profile": profile_label,
        "engine": workload.engine,
        "query_code": workload.query_code,
        "workload_id": workload.workload_id,
        "workload_version": workload.version,
        "variant_id": workload.variant_id,
        "artifact_file": str(workload.artifact_path.relative_to(ROOT)).replace("\\", "/"),
        "artifact_sha256": workload.artifact_sha256,
        "options": workload.options,
        "session_settings": workload.session_settings,
        "query_source": workload. parsed_payload if workload.parsed_payload else workload.raw_content,
        "spill_evidence_from_plan": {
            "detected": spill_ev.detected,
            "sources": list(spill_ev.sources),
            "bytes_spilled": spill_ev.bytes_spilled,
            "details": spill_ev.details,
        },
        "explain_plan": raw_plan,
    }

    out_path.write_text(
        json.dumps(envelope, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )

    if spill_ev.detected:
        spill_dir = ROOT / "evidence" / "spill"
        spill_dir.mkdir(parents=True, exist_ok=True)
        spill_file = (
            spill_dir
            / f"{tag}__{workload.engine}__{workload.query_code.lower()}__{workload.variant_id}.spill.json"
        )
        spill_file.write_text(
            json.dumps(envelope["spill_evidence_from_plan"], indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    return {
        "engine": workload.engine,
        "query_code": workload.query_code,
        "variant_id": workload.variant_id,
        "workload_version": workload.version,
        "artifact_sha256": workload.artifact_sha256,
        "spill_detected_in_plan": spill_ev.detected,
        "saved_to": str(out_path.relative_to(ROOT)).replace("\\", "/"),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Captura y conserva los planes EXPLAIN de Q1 y Q2 con sus metadatos de versión."
    )
    parser.add_argument(
        "--engine",
        choices=("mongodb", "postgresql"),
        required=True,
        help="Motor contra el cual ejecutar EXPLAIN.",
    )
    parser.add_argument(
        "--query",
        choices=("Q1", "Q2"),
        default=None,
        help="Consulta específica (por defecto ejecuta todas las variantes de Q1 y Q2).",
    )
    parser.add_argument(
        "--profile",
        default=os.environ.get("BENCH_PROFILE", "manual"),
        help="Etiqueta del perfil de recursos activo.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Identificador opcional de la corrida.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    workloads = list_workload_definitions(engine=args.engine, query_code=args.query)
    summaries = [
        capture_and_save_explain(w, profile_label=args.profile, run_id=args.run_id)
        for w in workloads
    ]
    print(json.dumps(summaries, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

