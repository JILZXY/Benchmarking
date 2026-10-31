"""Adaptador de PostgreSQL (JSONB) para ejecutar Q1 y Q2, capturar EXPLAIN y detectar spill a disco."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from adapters.workload_registry import WorkloadDefinition
from load.common import PostgresConnectionConfig
from metrics.outcome import SpillEvidence

PG_BLOCK_SIZE_BYTES = 8192


def _walk_pg_explain_for_spill(node: Any) -> tuple[bool, int, list[str]]:
    """Recorre recursivamente un plan `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` de PostgreSQL.

    Detecta:
    - `Sort Space Type == 'Disk'` (p. ej. `external merge`).
    - `Temp Read Blocks > 0` o `Temp Written Blocks > 0` (bloques de 8 KiB).
    - `Disk Usage > 0` en nodos Hash/Aggregate.
    """
    used_disk = False
    spilled_bytes = 0
    nodes: list[str] = []

    if isinstance(node, dict):
        node_type = str(node.get("Node Type") or "")
        node_spilled = False

        if str(node.get("Sort Space Type", "")).lower() == "disk":
            used_disk = True
            node_spilled = True
            space_used_kb = int(node.get("Sort Space Used", 0))
            if space_used_kb > 0:
                spilled_bytes += space_used_kb * 1024

        temp_written = int(node.get("Temp Written Blocks", 0))
        temp_read = int(node.get("Temp Read Blocks", 0))
        if temp_written > 0 or temp_read > 0:
            used_disk = True
            node_spilled = True
            # Si Sort Space Used no sumó bytes, estimamos con Temp Written Blocks * 8 KiB.
            if spilled_bytes == 0 and temp_written > 0:
                spilled_bytes += temp_written * PG_BLOCK_SIZE_BYTES

        disk_usage_kb = int(node.get("Disk Usage", 0))
        if disk_usage_kb > 0:
            used_disk = True
            node_spilled = True
            spilled_bytes += disk_usage_kb * 1024

        if node_spilled and node_type:
            sort_method = node.get("Sort Method")
            label = f"{node_type} ({sort_method})" if sort_method else node_type
            nodes.append(label)

        for v in node.values():
            sub_used, sub_bytes, sub_nodes = _walk_pg_explain_for_spill(v)
            used_disk = used_disk or sub_used
            spilled_bytes += sub_bytes
            nodes.extend(sub_nodes)
    elif isinstance(node, list):
        for item in node:
            sub_used, sub_bytes, sub_nodes = _walk_pg_explain_for_spill(item)
            used_disk = used_disk or sub_used
            spilled_bytes += sub_bytes
            nodes.extend(sub_nodes)

    return used_disk, spilled_bytes, nodes


def extract_postgres_spill_evidence(
    explain_json: list[dict[str, Any]] | dict[str, Any] | None = None,
    pg_stat_db_before: dict[str, int] | None = None,
    pg_stat_db_after: dict[str, int] | None = None,
    log_temp_lines: list[str] | None = None,
) -> SpillEvidence:
    """Construye `SpillEvidence` combinando `EXPLAIN (ANALYZE, BUFFERS)`, `pg_stat_database` y logs."""
    sources: list[str] = []
    details: dict[str, Any] = {}
    total_bytes: int | None = None

    if explain_json is not None:
        used_disk, exp_bytes, nodes = _walk_pg_explain_for_spill(explain_json)
        if used_disk:
            sources.append("postgresql.explain.temp_or_disk_sort")
            details["explain_spill_nodes"] = sorted(set(nodes))
            if exp_bytes > 0:
                total_bytes = exp_bytes

    if pg_stat_db_before and pg_stat_db_after:
        delta_files = int(pg_stat_db_after.get("temp_files", 0)) - int(
            pg_stat_db_before.get("temp_files", 0)
        )
        delta_bytes = int(pg_stat_db_after.get("temp_bytes", 0)) - int(
            pg_stat_db_before.get("temp_bytes", 0)
        )
        if delta_files > 0 or delta_bytes > 0:
            sources.append("postgresql.pg_stat_database.temp_bytes")
            details["delta_temp_files"] = delta_files
            details["delta_temp_bytes"] = delta_bytes
            total_bytes = max(total_bytes or 0, delta_bytes)

    if log_temp_lines:
        sources.append("postgresql.log.temporary_file")
        details["log_temp_file_count"] = len(log_temp_lines)
        details["log_samples"] = log_temp_lines[:5]

    return SpillEvidence(
        detected=len(sources) > 0,
        sources=tuple(sources),
        bytes_spilled=total_bytes,
        details=details,
    )


def _strip_sql_comments_and_trailing_semicolon(sql: str) -> str:
    lines: list[str] = []
    for line in sql.splitlines():
        if not line.lstrip().startswith("--"):
            lines.append(line)
    cleaned = "\n".join(lines).strip()
    while cleaned.endswith(";"):
        cleaned = cleaned[:-1].rstrip()
    return cleaned


@dataclass
class PostgresWorkloadAdapter:
    """Ejecuta cargas Q1/Q2 y captura planes `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` en PostgreSQL."""

    conn_cfg: PostgresConnectionConfig

    @classmethod
    def from_env(cls) -> "PostgresWorkloadAdapter":
        return cls(conn_cfg=PostgresConnectionConfig.from_env())

    def _apply_session_settings(self, cur: Any, settings: dict[str, str]) -> None:
        for param, val in settings.items():
            if not param.replace("_", "").isalnum():
                raise ValueError(f"Nombre de parámetro GUC inválido: {param!r}")
            cur.execute("SELECT set_config(%s, %s, true);", (param, str(val)))

    def execute_workload(
        self, workload: WorkloadDefinition, conn: Any | None = None
    ) -> list[dict[str, Any]]:
        """Ejecuta el SQL de `workload` aplicando `session_settings` con `SET LOCAL`."""
        import psycopg
        from psycopg.rows import dict_row

        sql = _strip_sql_comments_and_trailing_semicolon(workload.raw_content)

        def _run(c: Any) -> list[dict[str, Any]]:
            with c.transaction():
                with c.cursor(row_factory=dict_row) as cur:
                    self._apply_session_settings(cur, workload.session_settings)
                    cur.execute(sql)
                    return [dict(r) for r in cur.fetchall()]

        if conn is not None:
            return _run(conn)

        with psycopg.connect(self.conn_cfg.conninfo, autocommit=False) as own_conn:
            return _run(own_conn)

    def explain_workload(
        self,
        workload: WorkloadDefinition,
        analyze: bool = True,
        conn: Any | None = None,
    ) -> list[dict[str, Any]]:
        """Ejecuta `EXPLAIN (ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON)` respetando `work_mem`."""
        import psycopg

        sql = _strip_sql_comments_and_trailing_semicolon(workload.raw_content)
        opts = "ANALYZE, BUFFERS, VERBOSE, SETTINGS, FORMAT JSON" if analyze else "VERBOSE, SETTINGS, FORMAT JSON"
        explain_sql = f"EXPLAIN ({opts}) {sql}"

        def _explain(c: Any) -> list[dict[str, Any]]:
            with c.transaction():
                with c.cursor() as cur:
                    self._apply_session_settings(cur, workload.session_settings)
                    cur.execute(explain_sql)
                    row = cur.fetchone()
                    payload = row[0]
                    return payload if isinstance(payload, list) else [payload]

        if conn is not None:
            return _explain(conn)

        with psycopg.connect(self.conn_cfg.conninfo, autocommit=False) as own_conn:
            return _explain(own_conn)

    def create_worker_session(self, workload: WorkloadDefinition) -> "PostgresWorkerSession":
        return PostgresWorkerSession(conn_cfg=self.conn_cfg, workload=workload)


class PostgresWorkerSession:
    """Sesión persistente de un worker de PostgreSQL inicializada antes del warmup."""

    def __init__(self, conn_cfg: PostgresConnectionConfig, workload: WorkloadDefinition) -> None:
        self._conn_cfg = conn_cfg
        self._sql = _strip_sql_comments_and_trailing_semicolon(workload.raw_content)
        self._session_settings = dict(workload.session_settings)
        self._conn: Any | None = None

    def warmup_connection(self) -> None:
        import psycopg

        self._conn = psycopg.connect(self._conn_cfg.conninfo, autocommit=True)
        with self._conn.cursor() as cur:
            for param, val in self._session_settings.items():
                if not param.replace("_", "").isalnum():
                    raise ValueError(f"Parámetro GUC inválido: {param!r}")
                # Fijamos el parámetro a nivel de sesión (`is_local = false`) antes del warmup
                # para no ejecutar un comando adicional en cada iteración medida.
                cur.execute("SELECT set_config(%s, %s, false);", (param, str(val)))
            cur.execute("SELECT 1;")
            cur.fetchone()

    def execute_once(self) -> int:
        if self._conn is None:
            raise RuntimeError("La sesión de PostgreSQL no fue inicializada")
        with self._conn.cursor() as cur:
            cur.execute(self._sql)
            rows = cur.fetchall()
            return len(rows)

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

