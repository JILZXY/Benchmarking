#!/usr/bin/env python3
"""Cargador por lotes de la colección `intentos` para MongoDB (WiredTiger).

Responsabilidades:
1. Verificar el archivo JSONL contra su manifiesto (opcional pero activo por defecto).
2. Crear la colección `intentos` (o limpiarla si se indica `--drop`).
3. Importar los documentos en lotes (`insert_many(..., ordered=False)`) sin alterar
   ningún valor del dataset original.
4. Crear los índices oficiales definidos en el README (`intento_id` y `fecha_inicio`).
5. Verificar que la cantidad de documentos en la colección coincida exactamente
   con la cantidad importada y esperada.
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
    MongoConnectionConfig,
    iter_jsonl_batches,
    resolve_dataset_and_manifest,
)


def ensure_mongodb_indexes(collection: Any) -> list[str]:
    """Crea únicamente los dos índices equivalentes autorizados en el README."""
    from pymongo import ASCENDING, IndexModel

    models = [
        IndexModel([("intento_id", ASCENDING)], name="idx_intentos_intento_id", unique=True),
        IndexModel([("fecha_inicio", ASCENDING)], name="idx_intentos_fecha_inicio", unique=False),
    ]
    return list(collection.create_indexes(models))


def load_mongodb(
    dataset_path: Path,
    manifest_path: Path | None = None,
    conn_cfg: MongoConnectionConfig | None = None,
    batch_size: int = 5_000,
    drop_existing: bool = True,
    verify_manifest_first: bool = True,
) -> dict[str, Any]:
    """Importa el archivo JSONL en MongoDB por lotes y crea los índices."""
    from pymongo import MongoClient

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

    cfg = conn_cfg or MongoConnectionConfig.from_env()
    t0 = time.monotonic()

    with MongoClient(cfg.uri, serverSelectionTimeoutMS=10_000) as client:
        client.admin.command("ping")
        db = client[cfg.database]

        if drop_existing and cfg.collection in db.list_collection_names():
            db.drop_collection(cfg.collection)

        if cfg.collection not in db.list_collection_names():
            db.create_collection(cfg.collection)

        coll = db[cfg.collection]

        # Si no se eliminó la colección y ya tiene exatamente los registros esperados,
        # aseguramos índices y retornamos de forma idempotente.
        existing_count = coll.count_documents({})
        if not drop_existing and expected_count is not None and existing_count == expected_count:
            index_names = ensure_mongodb_indexes(coll)
            return {
                "engine": "mongodb",
                "status": "already_loaded",
                "database": cfg.database,
                "collection": cfg.collection,
                "inserted_count": 0,
                "verified_count": existing_count,
                "expected_count": expected_count,
                "dataset_sha256": dataset_sha256,
                "indexes": index_names,
                "elapsed_seconds": round(time.monotonic() - t0, 3),
            }

        inserted = 0
        for batch in iter_jsonl_batches(dataset_path, batch_size=batch_size):
            # Pasamos copias limpias sin transformar valores (insert_many muta el dict
            # en memoria agregando `_id`, pero los valores del documento original no cambian).
            docs = [dict(doc) for _, doc in batch]
            res = coll.insert_many(docs, ordered=False)
            inserted += len(res.inserted_ids)

        index_names = ensure_mongodb_indexes(coll)
        verified_count = coll.count_documents({})

    if verified_count != inserted:
        raise RuntimeError(
            f"Discrepancia en MongoDB: insertados={inserted}, count_documents={verified_count}"
        )
    if expected_count is not None and verified_count != expected_count:
        raise RuntimeError(
            f"Discrepancia contra manifiesto en MongoDB: esperados={expected_count}, "
            f"cargados={verified_count}"
        )

    return {
        "engine": "mongodb",
        "status": "loaded",
        "database": cfg.database,
        "collection": cfg.collection,
        "inserted_count": inserted,
        "verified_count": verified_count,
        "expected_count": expected_count,
        "dataset_sha256": dataset_sha256,
        "indexes": index_names,
        "elapsed_seconds": round(time.monotonic() - t0, 3),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cargador por lotes de la colección `intentos` en MongoDB."
    )
    parser.add_argument("--dataset", type=Path, default=None, help="Ruta al archivo JSONL.")
    parser.add_argument("--manifest", type=Path, default=None, help="Ruta al manifiesto JSON.")
    parser.add_argument("--batch-size", type=int, default=5000, help="Tamaño de lote (default 5000).")
    parser.add_argument(
        "--no-drop",
        action="store_true",
        help="No recrear la colección si ya existe con la cantidad esperada de documentos.",
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
    summary = load_mongodb(
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

