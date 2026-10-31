"""Registro y cargador de artefactos de consultas (Q1 y Q2) y sus variantes."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "workloads").exists() else Path("/app")
SPEC_PATH = ROOT / "workloads" / "shared" / "workloads_spec.json"


@dataclass(frozen=True)
class WorkloadDefinition:
    query_code: str
    workload_id: str
    version: str
    engine: str
    variant_id: str
    artifact_path: Path
    artifact_sha256: str
    options: dict[str, Any]
    session_settings: dict[str, str]
    raw_content: str
    parsed_payload: dict[str, Any] | None


def load_workloads_spec(spec_path: Path = SPEC_PATH) -> dict[str, Any]:
    return json.loads(spec_path.read_text(encoding="utf-8"))


def get_workload_definition(
    query_code: str,
    engine: str,
    variant_id: str | None = None,
    spec_path: Path = SPEC_PATH,
) -> WorkloadDefinition:
    """Devuelve la definición inmutable de una consulta y variante para el motor indicado."""
    spec = load_workloads_spec(spec_path)
    q_code = query_code.upper()
    if q_code not in spec["queries"]:
        raise KeyError(f"Consulta desconocida: {query_code!r}. Disponibles: {list(spec['queries'])}")

    q_entry = spec["queries"][q_code]
    eng = engine.lower()
    if eng not in q_entry["variants"]:
        raise KeyError(f"Motor desconocido para {q_code}: {engine!r}")

    variants: list[dict[str, Any]] = q_entry["variants"][eng]
    if variant_id is None:
        selected = variants[0]
    else:
        matches = [v for v in variants if v["variant_id"] == variant_id]
        if not matches:
            avail = [v["variant_id"] for v in variants]
            raise KeyError(
                f"Variante {variant_id!r} no encontrada para {q_code}/{eng}. Disponibles: {avail}"
            )
        selected = matches[0]

    artifact_path = ROOT / selected["file"]
    raw_bytes = artifact_path.read_bytes()
    sha256_hex = hashlib.sha256(raw_bytes).hexdigest()
    raw_text = raw_bytes.decode("utf-8")

    parsed_payload = json.loads(raw_text) if artifact_path.suffix == ".json" else None

    return WorkloadDefinition(
        query_code=q_code,
        workload_id=q_entry["id"],
        version=q_entry["version"],
        engine=eng,
        variant_id=selected["variant_id"],
        artifact_path=artifact_path,
        artifact_sha256=sha256_hex,
        options=dict(selected.get("options", {})),
        session_settings=dict(selected.get("session_settings", {})),
        raw_content=raw_text,
        parsed_payload=parsed_payload,
    )


def list_workload_definitions(
    engine: str | None = None,
    query_code: str | None = None,
    spec_path: Path = SPEC_PATH,
) -> list[WorkloadDefinition]:
    """Enumera todas las definiciones de cargas filtradas opcionalmente por motor y consulta."""
    spec = load_workloads_spec(spec_path)
    results: list[WorkloadDefinition] = []
    for q_code, q_entry in spec["queries"].items():
        if query_code and q_code != query_code.upper():
            continue
        for eng, variants in q_entry["variants"].items():
            if engine and eng != engine.lower():
                continue
            for v in variants:
                results.append(get_workload_definition(q_code, eng, v["variant_id"], spec_path))
    return results

