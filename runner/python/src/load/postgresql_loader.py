#!/usr/bin/env python3
"""Cargador por lotes de la tabla `intentos` para PostgreSQL (JSONB).

Responsabilidades:
1. Verificar el archivo JSONL contra su manifiesto (opcional pero activo por defecto).
2. Crear la tabla `public.intentos (id UUID PRIMARY KEY, payload JSONB NOT NULL)`.
3. Importar los documentos en lotes mediante el protocolo `COPY ... FROM STDIN` de
   PostgreSQL (`psycopg 3`), conservando la cadena JSON original íntegra sin
   transformar ningún valor.
4. Crear el índice B-Tree equivalente sobre `(payload->>'fecha_inicio')` y ejecutar
   `ANALYZE public.intentos`.
5. Verificar la cantidad de registros cargados y los índices existentes.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "dataset").exists() else Path("/app")
SRC_DIR = Path(__file__).resolve().parents[1]
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from dataset.generator.generate import verify_manifest  # noqa: E402
from load.common import (  # noqa: E402
    PostgresConnectionConfig,
    iter_jsonl_batches,
    resolve_dataset_and_manifest,
)


def ensure_postgres_table(cur: Any, table: str, drop_existing: bool) -> None:
    """Crea (o recrea) la tabla `intentos` con `id UUID PRIMARY KEY` y `payload JSONB NOT NULL`."""
    if drop_existing:
        cur.execute(f"DROP TABLE IF EXISTS public.{table};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS public.{table} (
            id UUID PRIMARY KEY,
            payload JSONB NOT NULL
        );
        """
    )


def ensure_postgres_indexes(cur: Any, table: str) -> list[str]:
    """Crea el índice B-Tree sobre la fecha extraída del JSONB y devuelve la lista de índices."""
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table}_fecha_inicio
            ON public.{table} USING btree ((payload->>'fecha_inicio'));
        """
    )
    cur.execute(f"ANALYZE public.{table};")
    cur.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
        ORDER BY indexname;
        """,
        (table,),
    )
    return [row[0] for row in cur.fetchall()]


def load_postgresql(
    dataset_path: Path,
    manifest_path: Path | None = None,
    conn_cfg: PostgresConnectionConfig | None = None,
    batch_size: int = 5_000,
    drop_existing: bool = True,
    verify_manifest_first: bool = True,
) -> dict[str, Any]:
    """Importa el archivo JSONL en PostgreSQL por lotes y crea los índices."""
    import psycopg

    if not dataset_path.exists():
        raise FileNotFoundError(f"No existe el dataset: {dataset_path}")

    expected_count: int | None = None
    dataset_sha256: str | None = None
    if manifest_path is not None and manifest_path.exists():
        if verify_manifest_first and not verify_manifest(dataset_path, manifest_path):
            raise RuntimeError(
                f"El archivo {dataset_path} no coincide con el manifiesto {manifest_path}"
            )
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        expected_count = int(manifest["record_count"])
        dataset_sha256 = str(manifest["sha256"])

    cfg = conn_cfg or PostgresConnectionConfig.from_env()
    t0 = time.monotonic()

    with psycopg.connect(cfg.conninfo, autocommit=False) as conn:
        with conn.cursor() as cur:
            ensure_postgres_table(cur, cfg.table, drop_existing=drop_existing)
            conn.commit()

            if not drop_existing and expected_count is not None:
                cur.execute(f"SELECT count(*) FROM public.{cfg.table};")
                existing_count = int(cur.fetchone()[0])
                if existing_count == expected_count:
                    index_names = ensure_postgres_indexes(cur, cfg.table)
                    conn.commit()
                    return {
                        "engine": "postgresql",
                        "status": "already_loaded",
                        "database": cfg.database,
                        "table": cfg.table,
                        "inserted_count": 0,
                        "verified_count": existing_count,
                        "expected_count": expected_count,
                        "dataset_sha256": dataset_sha256,
                        "indexes": index_names,
                        "elapsed_seconds": round(time.monotonic() - t0, 3),
                    }

            inserted = 0
            copy_sql = f"COPY public.{cfg.table} (id, payload) FROM STDIN"
            for batch in iter_jsonl_batches(dataset_path, batch_size=batch_size):
                with cur.copy(copy_sql) as copy:
                    for raw_line, doc in batch:
                        # `raw_line` es el JSON canónico original sin alterar.
                        copy.write_row((doc["intento_id"], raw_line))
                conn.commit()
                inserted += len(batch)

            index_names = ensure_postgres_indexes(cur, cfg.table)
            conn.commit()

            cur.execute(f"SELECT count(*) FROM public.{cfg.table};")
            verified_count = int(cur.fetchone()[0])

    if verified_count != inserted:
        raise RuntimeError(
            f"Discrepancia en PostgreSQL: insertados={inserted}, count(*)={verified_count}"
        )
    if expected_count is not None and verified_count != expected_count:
        raise RuntimeError(
            f"Discrepancia contra manifiesto en PostgreSQL: esperados={expected_count}, "
            f"cargados={verified_count}"
        )

    return {
        "engine": "postgresql",
        "status": "loaded",
        "database": cfg.database,
        "table": cfg.table,
        "inserted_count": inserted,
        "verified_count": verified_count,
        "expected_count": expected_count,
        "dataset_sha256": dataset_sha256,
        "indexes": index_names,
        "elapsed_seconds": round(time.monotonic() - t0, 3),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cargador por lotes de la tabla `intentos` en PostgreSQL (JSONB)."
    )
    parser.add_argument("--dataset", type=Path, default=None, help="Ruta al archivo JSONL.")
    parser.add_argument("--manifest", type=Path, default=None, help="Ruta al manifiesto JSON.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Tamaño de lote (default 5000).")
    parser.add_argument(
        "--no-drop",
        action="store_true",
        help="No recrear la tabla si ya existe con la cantidad esperada de documentos.",
    )
    parser.add_argument(
        "--skip-manifest-check",
        action="store_true",
        help="Omitir el recálculo del SHA-256 del archivo antes de cargar.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ds_path, man_path = resolve_dataset_and_manifest(args.dataset, args.manifest)
    summary = load_postgresql(
        dataset_path=ds_path,
        manifest_path=man_path,
        batch_size=args.batch_size,
        drop_existing=not args.no_drop,
        verify_manifest_first=not args.skip_manifest_check,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

