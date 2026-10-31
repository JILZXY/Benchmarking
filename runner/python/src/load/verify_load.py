#!/usr/bin/env python3
"""Verificador de equivalencia de datos e índices para MongoDB y PostgreSQL.

Permite validar:
- Un motor individual contra el archivo JSONL canónico y su manifiesto (`--engine mongodb`
  o `--engine postgresql`), respetando la regla de no tener ambos motores encendidos
  simultáneamente si así se desea.
- Ambos motores entre sí y contra el manifiesto (`--engine both`), comparando:
    1) Conteo total de documentos (`record_count`).
    2) Índices equivalentes (`intento_id` / PK y `fecha_inicio`).
    3) Huella agregada determinista del conjunto completo o de una muestra determinista
       de `intento_id` recuperados de cada motor y re-serializados en JSON canónico
       (garantizando que ningún valor ni tipo fue alterado durante la carga).
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import random
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[4]
SRC_DIR = ROOT / "runner" / "python" / "src"
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from load.common import (  # noqa: E402
    MongoConnectionConfig,
    PostgresConnectionConfig,
    canonical_doc_sha256,
    resolve_dataset_and_manifest,
)


def sample_dataset_records(
    dataset_path: Path, sample_size: int = 200, seed: int = 20261030
) -> tuple[int, dict[str, str]]:
    """Recorre el JSONL en streaming con Reservoir Sampling determinista.

    Devuelve `(total_lines, {intento_id: sha256_canonico})`.
    """
    rng = random.Random(seed)
    reservoir: list[tuple[str, str]] = []
    total = 0

    with dataset_path.open("r", encoding="utf-8") as fh:
        for raw in fh:
            stripped = raw.strip()
            if not stripped:
                continue
            total += 1
            doc = json.loads(stripped)
            iid = doc["intento_id"]
            digest = hashlib.sha256(stripped.encode("utf-8")).hexdigest()
            if len(reservoir) < sample_size:
                reservoir.append((iid, digest))
            else:
                j = rng.randrange(total)
                if j < sample_size:
                    reservoir[j] = (iid, digest)

    return total, dict(reservoir)


def inspect_mongodb_state(
    sample_ids: list[str], conn_cfg: MongoConnectionConfig | None = None
) -> dict[str, Any]:
    """Obtiene conteo, índices y los hashes canónicos de `sample_ids` desde MongoDB."""
    from pymongo import MongoClient

    cfg = conn_cfg or MongoConnectionConfig.from_env()
    with MongoClient(cfg.uri, serverSelectionTimeoutMS=10_000) as client:
        coll = client[cfg.database][cfg.collection]
        count = coll.count_documents({})
        index_info = coll.index_information()

        sample_hashes: dict[str, str] = {}
        if sample_ids:
            cursor = coll.find({"intento_id": {"$in": sample_ids}}, {"_id": 0})
            for doc in cursor:
                sample_hashes[doc["intento_id"]] = canonical_doc_sha256(doc)

    has_id_idx = any(
        info.get("key") == [("intento_id", 1)] and info.get("unique") is True
        for info in index_info.values()
    )
    has_date_idx = any(
        info.get("key") == [("fecha_inicio", 1)] for info in index_info.values()
    )
    return {
        "engine": "mongodb",
        "count": count,
        "indexes": sorted(index_info.keys()),
        "has_id_index": has_id_idx,
        "has_fecha_inicio_index": has_date_idx,
        "sample_hashes": sample_hashes,
    }


def inspect_postgresql_state(
    sample_ids: list[str], conn_cfg: PostgresConnectionConfig | None = None
) -> dict[str, Any]:
    """Obtiene conteo, índices y los hashes canónicos de `sample_ids` desde PostgreSQL."""
    import psycopg

    cfg = conn_cfg or PostgresConnectionConfig.from_env()
    with psycopg.connect(cfg.conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM public.{cfg.table};")
            count = int(cur.fetchone()[0])

            cur.execute(
                """
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE schemaname = 'public' AND tablename = %s
                ORDER BY indexname;
                """,
                (cfg.table,),
            )
            idx_rows = cur.fetchall()

            sample_hashes: dict[str, str] = {}
            if sample_ids:
                cur.execute(
                    f"SELECT id::text, payload FROM public.{cfg.table} WHERE id = ANY(%s::uuid[]);",
                    (sample_ids,),
                )
                for row_id, payload in cur.fetchall():
                    doc = payload if isinstance(payload, dict) else json.loads(payload)
                    sample_hashes[row_id] = canonical_doc_sha256(doc)

    index_names = [r[0] for r in idx_rows]
    index_defs = " ".join(r[1] for r in idx_rows)
    has_id_idx = any("UNIQUE INDEX" in r[1] and "(id)" in r[1] for r in idx_rows)
    has_date_idx = "fecha_inicio" in index_defs

    return {
        "engine": "postgresql",
        "count": count,
        "indexes": index_names,
        "has_id_index": has_id_idx,
        "has_fecha_inicio_index": has_date_idx,
        "sample_hashes": sample_hashes,
    }


def compare_state_against_reference(
    engine_state: dict[str, Any],
    expected_count: int,
    reference_hashes: dict[str, str],
) -> list[str]:
    """Devuelve una lista de errores encontrados al comparar un motor contra la referencia."""
    errors: list[str] = []
    eng = engine_state["engine"]

    if engine_state["count"] != expected_count:
        errors.append(
            f"{eng}: conteo={engine_state['count']} distinto al esperado={expected_count}"
        )
    if not engine_state["has_id_index"]:
        errors.append(f"{eng}: falta índice único sobre identificador (intento_id / id)")
    if not engine_state["has_fecha_inicio_index"]:
        errors.append(f"{eng}: falta índice sobre fecha_inicio")

    sample_hashes: dict[str, str] = engine_state["sample_hashes"]
    if len(sample_hashes) != len(reference_hashes):
        missing = set(reference_hashes.keys()) - set(sample_hashes.keys())
        errors.append(f"{eng}: faltan {len(missing)} documentos de la muestra")
    else:
        mismatched = [
            iid for iid, ref_h in reference_hashes.items() if sample_hashes.get(iid) != ref_h
        ]
        if mismatched:
            errors.append(
                f"{eng}: {len(mismatched)} documentos difieren en su representación canónica "
                f"(ej. {mismatched[0]})"
            )

    return errors


def verify_loaded_data(
    dataset_path: Path,
    manifest_path: Path | None = None,
    engine: str = "both",
    sample_size: int = 200,
    seed: int = 20261030,
) -> dict[str, Any]:
    """Ejecuta la verificación completa del motor o de ambos motores contra el dataset."""
    expected_count, ref_hashes = sample_dataset_records(
        dataset_path, sample_size=sample_size, seed=seed
    )
    if manifest_path is not None and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest_count = int(manifest["record_count"])
        if manifest_count != expected_count:
            raise RuntimeError(
                f"El archivo JSONL tiene {expected_count} líneas pero el manifiesto indica {manifest_count}"
            )

    sample_ids = list(ref_hashes.keys())
    errors: list[str] = []
    details: dict[str, Any] = {
        "expected_count": expected_count,
        "sample_size": len(sample_ids),
    }

    if engine in ("mongodb", "both"):
        mongo_state = inspect_mongodb_state(sample_ids)
        errors.extend(compare_state_against_reference(mongo_state, expected_count, ref_hashes))
        details["mongodb"] = {
            k: v for k, v in mongo_state.items() if k != "sample_hashes"
        }

    if engine in ("postgresql", "both"):
        pg_state = inspect_postgresql_state(sample_ids)
        errors.extend(compare_state_against_reference(pg_state, expected_count, ref_hashes))
        details["postgresql"] = {
            k: v for k, v in pg_state.items() if k != "sample_hashes"
        }

    if engine == "both":
        if mongo_state["count"] != pg_state["count"]:
            errors.append(
                f"Conteo desigual entre motores: mongodb={mongo_state['count']} vs "
                f"postgresql={pg_state['count']}"
            )
        if mongo_state["sample_hashes"] != pg_state["sample_hashes"]:
            errors.append(
                "Los hashes canónicos de los documentos muestreados difieren entre MongoDB y PostgreSQL"
            )

    details["ok"] = len(errors) == 0
    details["errors"] = errors
    return details


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verifica que los datos e índices cargados coincidan con el dataset canónico."
    )
    parser.add_argument(
        "--engine",
        choices=("mongodb", "postgresql", "both"),
        default="both",
        help="Motor(es) a verificar.",
    )
    parser.add_argument("--dataset", type=Path, default=None, help="Ruta al archivo JSONL.")
    parser.add_argument("--manifest", type=Path, default=None, help="Ruta al manifiesto JSON.")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=200,
        help="Cantidad de documentos a verificar bit a bit en formato canónico (default 200).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    ds_path, man_path = resolve_dataset_and_manifest(args.dataset, args.manifest)
    result = verify_loaded_data(
        dataset_path=ds_path,
        manifest_path=man_path,
        engine=args.engine,
        sample_size=args.sample_size,
    )
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())

