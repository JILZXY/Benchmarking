"""Utilidades compartidas de lectura en lotes, serialización canónica y configuración de conexiones."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Iterator

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "dataset").exists() else Path("/app")


@dataclass(frozen=True)
class MongoConnectionConfig:
    host: str = "127.0.0.1"
    port: int = 27018
    user: str = "bench"
    password: str = "change-me-mongo"
    database: str = "eticore"
    collection: str = "intentos"

    @classmethod
    def from_env(cls) -> "MongoConnectionConfig":
        # Si corre dentro del contenedor runner, MONGO_HOST=mongodb y MONGO_PORT=27017.
        # Si corre en el host, usa 127.0.0.1 y MONGO_HOST_PORT (27018 por defecto).
        host = os.environ.get("MONGO_HOST", "127.0.0.1")
        default_port = "27017" if host != "127.0.0.1" else os.environ.get("MONGO_HOST_PORT", "27018")
        port = int(os.environ.get("MONGO_PORT", default_port))
        return cls(
            host=host,
            port=port,
            user=os.environ.get("MONGO_ROOT_USER", "bench"),
            password=os.environ.get("MONGO_ROOT_PASSWORD", "change-me-mongo"),
            database=os.environ.get("MONGO_DB", "eticore"),
            collection=os.environ.get("MONGO_COLLECTION", "intentos"),
        )

    @property
    def uri(self) -> str:
        return (
            f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/"
            f"?authSource=admin"
        )


@dataclass(frozen=True)
class PostgresConnectionConfig:
    host: str = "127.0.0.1"
    port: int = 15432
    user: str = "bench"
    password: str = "change-me-postgres"
    database: str = "eticore"
    table: str = "intentos"

    @classmethod
    def from_env(cls) -> "PostgresConnectionConfig":
        host = os.environ.get("POSTGRES_HOST", "127.0.0.1")
        default_port = (
            "5432" if host != "127.0.0.1" else os.environ.get("POSTGRES_HOST_PORT", "15432")
        )
        port = int(os.environ.get("POSTGRES_PORT", default_port))
        return cls(
            host=host,
            port=port,
            user=os.environ.get("POSTGRES_USER", "bench"),
            password=os.environ.get("POSTGRES_PASSWORD", "change-me-postgres"),
            database=os.environ.get("POSTGRES_DB", "eticore"),
            table=os.environ.get("POSTGRES_TABLE", "intentos"),
        )

    @property
    def conninfo(self) -> str:
        return (
            f"host={self.host} port={self.port} user={self.user} "
            f"password={self.password} dbname={self.database}"
        )


def canonical_json(doc: dict[str, Any]) -> str:
    """Serializa un diccionario a JSON canónico sin espacios y con claves ordenadas."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def canonical_doc_sha256(doc: dict[str, Any]) -> str:
    """Devuelve el SHA-256 hexadecimal del documento en su representación canónica."""
    clean = {k: v for k, v in doc.items() if k != "_id"}
    return hashlib.sha256(canonical_json(clean).encode("utf-8")).hexdigest()


def iter_jsonl_batches(
    dataset_path: Path, batch_size: int = 5_000
) -> Iterator[list[tuple[str, dict[str, Any]]]]:
    """Lee un archivo JSONL en streaming y entrega lotes de `(raw_line, parsed_dict)`.

    Conserva tanto la cadena JSON original como el diccionario decodificado sin
    transformar ningún valor.
    """
    if batch_size <= 0:
        raise ValueError("batch_size debe ser mayor que 0")

    batch: list[tuple[str, dict[str, Any]]] = []
    with dataset_path.open("r", encoding="utf-8") as fh:
        for line_no, raw in enumerate(fh, start=1):
            stripped = raw.strip()
            if not stripped:
                continue
            doc = json.loads(stripped)
            if "intento_id" not in doc:
                raise ValueError(f"Línea {line_no}: falta intento_id")
            batch.append((stripped, doc))
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def resolve_dataset_and_manifest(
    dataset_path: Path | None = None,
    manifest_path: Path | None = None,
) -> tuple[Path, Path]:
    """Resuelve las rutas por defecto del dataset y su manifiesto según las variables de entorno."""
    seed = int(os.environ.get("DATASET_SEED", "20261030"))
    count = int(os.environ.get("DATASET_SIZE", "500000"))
    tag = f"intentos_n{count}_s{seed}"
    ds = dataset_path or (ROOT / "dataset" / "generated" / f"{tag}.jsonl")
    man = manifest_path or (ROOT / "dataset" / "manifests" / f"{tag}.manifest.json")
    return ds, man

