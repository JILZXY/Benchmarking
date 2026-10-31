"""Adaptador de MongoDB para ejecutar Q1 y Q2, capturar planes EXPLAIN y detectar spill a disco."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from adapters.workload_registry import WorkloadDefinition
from load.common import MongoConnectionConfig
from metrics.outcome import SpillEvidence


def coerce_bson_int(value: Any, default: int = 0) -> int:
    """Convierte valores enteros de BSON/pymongo o `mongosh` (`NumberLong` / EJSON) a `int` de Python.

    Soporta:
    - `int` y `float` nativos.
    - Cadenas numéricas (`"42"`, `"42.0"`).
    - Objetos `Long` serializados por `mongosh` (`{"low": int, "high": int, "unsigned": bool}`).
    - Objetos Extended JSON (`{"$numberLong": "..."}`, `{"$numberInt": "..."}`,
      `{"$numberDouble": "..."}`, `{"$numberDecimal": "..."}`).
    """
    if value is None or isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return default
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return default
    if isinstance(value, dict):
        if "low" in value and "high" in value:
            try:
                low_u = coerce_bson_int(value.get("low"), 0) & 0xFFFFFFFF
                high_u = coerce_bson_int(value.get("high"), 0) & 0xFFFFFFFF
                unsigned = bool(value.get("unsigned", False))
                if unsigned:
                    return (high_u << 32) | low_u
                high_s = high_u - 0x100000000 if (high_u & 0x80000000) else high_u
                return (high_s << 32) | low_u
            except Exception:  # noqa: BLE001
                return default
        for ejson_key in ("$numberLong", "$numberInt", "$numberDouble", "$numberDecimal"):
            if ejson_key in value:
                return coerce_bson_int(value[ejson_key], default)
        return default
    return default


def _walk_mongo_explain_for_spill(node: Any) -> tuple[bool, int, list[str]]:
    """Recorre recursivamente un documento `explain('executionStats')` de MongoDB.

    Detecta banderas `usedDisk: true`, `spilledRecords > 0`, `spilledDataStorageSize > 0`
    o `spilledBytes > 0` tanto en planes clásicos como en SBE (Slot-Based Execution).
    """
    used_disk = False
    spilled_bytes = 0
    stages: list[str] = []

    if isinstance(node, dict):
        if ("low" in node and "high" in node) or "$numberLong" in node:
            return False, 0, []

        stage_name = str(node.get("stage") or node.get("$sort") or node.get("$group") or "")
        node_spilled = False

        if node.get("usedDisk") is True:
            used_disk = True
            node_spilled = True

        for byte_key in ("spilledDataStorageSize", "spilledBytes", "totalDataSizeSortedBytesEstimate"):
            val_int = coerce_bson_int(node.get(byte_key), 0)
            if val_int > 0 and byte_key != "totalDataSizeSortedBytesEstimate":
                used_disk = True
                node_spilled = True
                spilled_bytes += val_int

        if coerce_bson_int(node.get("spilledRecords"), 0) > 0:
            used_disk = True
            node_spilled = True

        if node_spilled and stage_name:
            stages.append(stage_name)

        for v in node.values():
            sub_used, sub_bytes, sub_stages = _walk_mongo_explain_for_spill(v)
            used_disk = used_disk or sub_used
            spilled_bytes += sub_bytes
            stages.extend(sub_stages)
    elif isinstance(node, list):
        for item in node:
            sub_used, sub_bytes, sub_stages = _walk_mongo_explain_for_spill(item)
            used_disk = used_disk or sub_used
            spilled_bytes += sub_bytes
            stages.extend(sub_stages)

    return used_disk, spilled_bytes, stages


def extract_mongo_spill_evidence(
    explain_doc: dict[str, Any] | None = None,
    server_status_before: dict[str, Any] | None = None,
    server_status_after: dict[str, Any] | None = None,
    log_spill_lines: list[str] | None = None,
) -> SpillEvidence:
    """Construye `SpillEvidence` combinando `explain`, deltas de `serverStatus` y logs."""
    sources: list[str] = []
    details: dict[str, Any] = {}
    total_bytes: int | None = None

    if explain_doc is not None:
        used_disk, exp_bytes, stages = _walk_mongo_explain_for_spill(explain_doc)
        if used_disk:
            sources.append("mongodb.explain.usedDisk")
            details["explain_spill_stages"] = sorted(set(stages))
            if exp_bytes > 0:
                total_bytes = (total_bytes or 0) + exp_bytes

    if server_status_before and server_status_after:
        m_before = server_status_before.get("metrics", {}).get("queryExecutor", {}) or {}
        m_after = server_status_after.get("metrics", {}).get("queryExecutor", {}) or {}
        sorts_before = server_status_before.get("metrics", {}).get("operation", {}) or {}
        sorts_after = server_status_after.get("metrics", {}).get("operation", {}) or {}

        for key in ("spilledRanges", "spilledRecords", "spilledDataStorageSize"):
            delta = coerce_bson_int(m_after.get(key, 0), 0) - coerce_bson_int(
                m_before.get(key, 0), 0
            )
            if delta > 0:
                if "mongodb.serverStatus.queryExecutor" not in sources:
                    sources.append("mongodb.serverStatus.queryExecutor")
                details[f"delta_{key}"] = delta
                if key == "spilledDataStorageSize":
                    total_bytes = (total_bytes or 0) + delta

        sort_spill_delta = coerce_bson_int(sorts_after.get("writeConflicts", 0), 0) - coerce_bson_int(
            sorts_before.get("writeConflicts", 0), 0
        )
        _ = sort_spill_delta  # reservado para telemetría extendida

    if log_spill_lines:
        sources.append("mongodb.log.usedDisk")
        details["log_used_disk_count"] = len(log_spill_lines)
        details["log_samples"] = log_spill_lines[:5]

    return SpillEvidence(
        detected=len(sources) > 0,
        sources=tuple(sources),
        bytes_spilled=total_bytes,
        details=details,
    )


@dataclass
class MongoWorkloadAdapter:
    """Ejecuta cargas Q1/Q2 y captura planes `explain('executionStats')` en MongoDB."""

    conn_cfg: MongoConnectionConfig

    @classmethod
    def from_env(cls) -> "MongoWorkloadAdapter":
        return cls(conn_cfg=MongoConnectionConfig.from_env())

    def execute_workload(
        self, workload: WorkloadDefinition, client: Any | None = None
    ) -> list[dict[str, Any]]:
        """Ejecuta el pipeline de agregación de `workload` y devuelve la lista de resultados."""
        from pymongo import MongoClient

        if workload.parsed_payload is None:
            raise ValueError(f"El artefacto de MongoDB no contiene JSON: {workload.artifact_path}")

        pipeline: list[dict[str, Any]] = workload.parsed_payload["pipeline"]
        options: dict[str, Any] = dict(workload.options)

        def _run(c: Any) -> list[dict[str, Any]]:
            coll = c[self.conn_cfg.database][self.conn_cfg.collection]
            cursor = coll.aggregate(pipeline, **options)
            return list(cursor)

        if client is not None:
            return _run(client)

        with MongoClient(self.conn_cfg.uri, serverSelectionTimeoutMS=10_000) as own_client:
            return _run(own_client)

    def explain_workload(
        self,
        workload: WorkloadDefinition,
        verbosity: str = "executionStats",
        client: Any | None = None,
    ) -> dict[str, Any]:
        """Ejecuta `db.command('explain', ...)` sobre la agregación y devuelve el plan completo."""
        from pymongo import MongoClient

        if workload.parsed_payload is None:
            raise ValueError(f"El artefacto de MongoDB no contiene JSON: {workload.artifact_path}")

        agg_cmd: dict[str, Any] = {
            "aggregate": self.conn_cfg.collection,
            "pipeline": workload.parsed_payload["pipeline"],
            "cursor": {},
        }
        agg_cmd.update(workload.options)

        def _explain(c: Any) -> dict[str, Any]:
            db = c[self.conn_cfg.database]
            return dict(db.command("explain", agg_cmd, verbosity=verbosity))

        if client is not None:
            return _explain(client)

        with MongoClient(self.conn_cfg.uri, serverSelectionTimeoutMS=10_000) as own_client:
            return _explain(own_client)

    def create_worker_session(self, workload: WorkloadDefinition) -> "MongoWorkerSession":
        return MongoWorkerSession(conn_cfg=self.conn_cfg, workload=workload)


class MongoWorkerSession:
    """Sesión persistente de un worker de MongoDB inicializada antes del warmup."""

    def __init__(self, conn_cfg: MongoConnectionConfig, workload: WorkloadDefinition) -> None:
        if workload.parsed_payload is None:
            raise ValueError(f"El artefacto de MongoDB no contiene JSON: {workload.artifact_path}")
        self._conn_cfg = conn_cfg
        self._pipeline: list[dict[str, Any]] = workload.parsed_payload["pipeline"]
        self._options: dict[str, Any] = dict(workload.options)
        self._client: Any | None = None
        self._coll: Any | None = None

    def warmup_connection(self) -> None:
        from pymongo import MongoClient

        self._client = MongoClient(
            self._conn_cfg.uri,
            maxPoolSize=1,
            minPoolSize=1,
            serverSelectionTimeoutMS=10_000,
        )
        self._client.admin.command("ping")
        self._coll = self._client[self._conn_cfg.database][self._conn_cfg.collection]

    def execute_once(self) -> int:
        if self._coll is None:
            raise RuntimeError("La sesión de MongoDB no fue inicializada")
        cursor = self._coll.aggregate(self._pipeline, **self._options)
        count = 0
        for _ in cursor:
            count += 1
        return count

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None
            self._coll = None

