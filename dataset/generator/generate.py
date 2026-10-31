#!/usr/bin/env python3
"""Generador determinista del dataset experimental de ETICORE (`intentos`).

Produce un archivo JSONL canónico (claves ordenadas, separadores compactos, LF)
por lotes para evitar cargar el dataset completo en memoria, valida cada
documento contra el contrato experimental y genera un manifiesto JSON con el
hash SHA-256, cantidad de registros, semilla y versión del generador.

Uso desde la raíz del proyecto:
    python -m dataset.generator.generate --count 500000 --seed 20261030
    python dataset/generator/generate.py --count 1000 --seed 20261030
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import random
import sys
import time
from typing import Any, Iterator
import uuid

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataset.generator import __version__ as GENERATOR_VERSION  # noqa: E402
from dataset.generator.validator import validate_intento  # noqa: E402

SCHEMA_VERSION = "intento.v1"

# Fecha base fija en UTC para que las marcas de tiempo ISO-8601 sean 100 %
# deterministas y nunca dependan del reloj del sistema.
BASE_EPOCH_UTC = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
SPAN_SECONDS = 300 * 24 * 3600  # ~300 días de ventana temporal

ESTADOS_WEIGHTED: tuple[tuple[str, int], ...] = (
    ("completado", 72),
    ("en_progreso", 12),
    ("abandonado", 11),
    ("expirado", 5),
)

NIVELES: tuple[str, ...] = ("basico", "intermedio", "avanzado")
SEGMENTOS: tuple[str, ...] = ("estudiante", "docente", "profesional", "investigador")
DIFICULTADES: tuple[str, ...] = ("baja", "media", "alta")

AREAS_Y_TEMAS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "etica",
        (
            "dilemas_morales",
            "responsabilidad_profesional",
            "integridad_academica",
            "transparencia",
        ),
    ),
    (
        "privacidad",
        (
            "proteccion_datos",
            "consentimiento_informado",
            "anonimizacion",
            "derechos_arco",
        ),
    ),
    (
        "seguridad",
        (
            "control_acceso",
            "cifrado",
            "gestion_incidentes",
            "ingenieria_social",
        ),
    ),
    (
        "cumplimiento",
        (
            "normativa_datos",
            "auditoria_interna",
            "gestion_riesgos",
            "politicas_corporativas",
        ),
    ),
    (
        "gobernanza_ia",
        (
            "sesgo_algoritmico",
            "explicabilidad",
            "uso_responsable_ia",
            "supervision_humana",
        ),
    ),
    (
        "propiedad_intelectual",
        (
            "licencias_software",
            "derechos_autor",
            "plagio",
            "datos_abiertos",
        ),
    ),
)

TEMAS_PREGUNTA: tuple[str, ...] = tuple(
    tema for _, temas in AREAS_Y_TEMAS for tema in temas
)

TIPOS_PREGUNTA: tuple[str, ...] = (
    "opcion_multiple",
    "verdadero_falso",
    "caso_practico",
    "emparejamiento",
)

# Incluye valores <= 5 (2, 4, 5) y > 5 (8, 10, 12, 15, 20) para que el filtro
# de Q1 (`puntaje_maximo > 5`) sea selectivo (~70 % pasa el filtro).
PUNTAJES_MAXIMOS: tuple[int, ...] = (2, 4, 5, 8, 10, 10, 12, 15, 20)
PONDERACIONES: tuple[float, ...] = (0.5, 0.8, 1.0, 1.0, 1.2, 1.5, 2.0)

CANALES: tuple[str, ...] = ("web", "movil", "api", "lms")
FLAGS_CATALOG: tuple[str, ...] = (
    "reintento",
    "tiempo_extendido",
    "modo_accesible",
    "conexion_inestable",
    "auditoria_activa",
)


@dataclass(frozen=True)
class GeneratorConfig:
    """Parámetros inmutables de una ejecución del generador."""

    seed: int = 20261030
    count: int = 500_000
    avg_respuestas: int = 3
    batch_size: int = 5_000
    user_pool_size: int = 50_000
    lesson_pool_size: int = 250
    question_pool_size: int = 2_000

    def __post_init__(self) -> None:
        if self.count <= 0:
            raise ValueError("count debe ser mayor que 0")
        if not (1 <= self.avg_respuestas <= 5):
            raise ValueError("avg_respuestas debe estar entre 1 y 5")
        if self.batch_size <= 0:
            raise ValueError("batch_size debe ser mayor que 0")


def _deterministic_uuid4(rng: random.Random) -> str:
    """Construye un UUID v4 RFC 4122 exclusivamente a partir del PRNG dado."""
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _build_estado_table() -> tuple[str, ...]:
    table: list[str] = []
    for estado, weight in ESTADOS_WEIGHTED:
        table.extend([estado] * weight)
    return tuple(table)


ESTADO_LOOKUP: tuple[str, ...] = _build_estado_table()


def _respuestas_distribution(avg: int) -> tuple[int, ...]:
    """Devuelve una tupla de conteos cuyo valor esperado es exactamente `avg`."""
    if avg == 1:
        return (1, 1, 1, 1)
    if avg == 2:
        return (1, 2, 2, 3)
    if avg == 3:
        # Media exacta = (1 + 2 + 2 + 3 + 3 + 3 + 4 + 4 + 5) / 9 = 27 / 9 = 3.0
        return (1, 2, 2, 3, 3, 3, 4, 4, 5)
    if avg == 4:
        return (2, 3, 4, 4, 5, 6)
    return (4, 5, 5, 6)


def iter_intentos(config: GeneratorConfig) -> Iterator[dict[str, Any]]:
    """Genera documentos `intento` uno a uno de forma determinista y en streaming."""
    rng = random.Random(config.seed)

    user_pool_n = min(config.user_pool_size, max(100, config.count // 5))
    user_ids = tuple(_deterministic_uuid4(rng) for _ in range(user_pool_n))

    lessons: list[tuple[str, str, str, str]] = []
    for idx in range(config.lesson_pool_size):
        area, temas = AREAS_Y_TEMAS[idx % len(AREAS_Y_TEMAS)]
        tema = temas[(idx // len(AREAS_Y_TEMAS)) % len(temas)]
        dificultad = DIFICULTADES[idx % len(DIFICULTADES)]
        lessons.append((_deterministic_uuid4(rng), area, tema, dificultad))
    lesson_pool = tuple(lessons)

    questions: list[tuple[str, str, str]] = []
    for idx in range(config.question_pool_size):
        q_id = _deterministic_uuid4(rng)
        q_tema = TEMAS_PREGUNTA[idx % len(TEMAS_PREGUNTA)]
        q_tipo = TIPOS_PREGUNTA[idx % len(TIPOS_PREGUNTA)]
        questions.append((q_id, q_tema, q_tipo))
    question_pool = tuple(questions)

    resp_counts = _respuestas_distribution(config.avg_respuestas)

    for _ in range(config.count):
        intento_id = _deterministic_uuid4(rng)
        offset_sec = rng.randrange(0, SPAN_SECONDS)
        fecha_inicio = (BASE_EPOCH_UTC + timedelta(seconds=offset_sec)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        usuario_id = user_ids[rng.randrange(len(user_ids))]
        nivel = NIVELES[rng.randrange(len(NIVELES))]
        segmento = SEGMENTOS[rng.randrange(len(SEGMENTOS))]

        leccion_id, area, leccion_tema, dificultad = lesson_pool[
            rng.randrange(len(lesson_pool))
        ]
        estado = ESTADO_LOOKUP[rng.randrange(len(ESTADO_LOOKUP))]

        n_resp = resp_counts[rng.randrange(len(resp_counts))]
        respuestas: list[dict[str, Any]] = []
        suma_ponderada = 0.0
        max_ponderado = 0.0

        for _ in range(n_resp):
            q_id, q_tema, q_tipo = question_pool[rng.randrange(len(question_pool))]
            p_max = PUNTAJES_MAXIMOS[rng.randrange(len(PUNTAJES_MAXIMOS))]
            if estado == "completado":
                p_obt = rng.randint(max(0, p_max // 3), p_max)
            else:
                p_obt = rng.randint(0, p_max)
            pond = PONDERACIONES[rng.randrange(len(PONDERACIONES))]

            suma_ponderada += p_obt * pond
            max_ponderado += p_max * pond

            respuestas.append(
                {
                    "pregunta_id": q_id,
                    "tema": q_tema,
                    "tipo": q_tipo,
                    "evaluacion": {
                        "puntaje_obtenido": p_obt,
                        "puntaje_maximo": p_max,
                        "ponderacion": pond,
                    },
                }
            )

        puntaje_total = round(suma_ponderada, 2)
        porcentaje = (
            round((suma_ponderada / max_ponderado) * 100.0, 2)
            if max_ponderado > 0
            else 0.0
        )
        duracion_segundos = rng.randint(45, 3600)

        canal = CANALES[rng.randrange(len(CANALES))]
        session_id = f"session-{rng.randrange(1_000_000):06d}"

        flag_roll = rng.randrange(100)
        if flag_roll < 70:
            flags: list[str] = []
        elif flag_roll < 92:
            flags = [FLAGS_CATALOG[rng.randrange(len(FLAGS_CATALOG))]]
        else:
            f1 = FLAGS_CATALOG[rng.randrange(len(FLAGS_CATALOG))]
            f2 = FLAGS_CATALOG[rng.randrange(len(FLAGS_CATALOG))]
            flags = [f1] if f1 == f2 else [f1, f2]

        yield {
            "intento_id": intento_id,
            "fecha_inicio": fecha_inicio,
            "usuario_id": usuario_id,
            "leccion_id": leccion_id,
            "estado": estado,
            "usuario": {
                "nivel": nivel,
                "segmento": segmento,
            },
            "leccion": {
                "area": area,
                "tema": leccion_tema,
                "dificultad": dificultad,
            },
            "respuestas": respuestas,
            "resultado": {
                "puntaje_total": puntaje_total,
                "porcentaje": porcentaje,
                "duracion_segundos": duracion_segundos,
            },
            "metadata": {
                "canal": canal,
                "session_id": session_id,
                "flags": flags,
            },
        }


def serialize_canonical(doc: dict[str, Any]) -> str:
    """Serializa un documento en JSON canónico (claves ordenadas, ASCII, compacto)."""
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def generate_dataset(
    config: GeneratorConfig,
    output_path: Path,
    manifest_path: Path,
    force: bool = False,
) -> dict[str, Any]:
    """Genera el archivo JSONL en lotes, valida cada registro y escribe el manifiesto."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and manifest_path.exists() and not force:
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (
            existing.get("seed") == config.seed
            and existing.get("record_count") == config.count
            and existing.get("avg_respuestas_config") == config.avg_respuestas
            and existing.get("generator_version") == GENERATOR_VERSION
            and verify_manifest(output_path, manifest_path)
        ):
            return existing

    tmp_output = output_path.with_suffix(output_path.suffix + ".tmp")
    hasher = hashlib.sha256()

    total_docs = 0
    total_respuestas = 0
    total_respuestas_filtradas_q1 = 0
    total_completados_q2 = 0
    total_bytes = 0
    batch_lines: list[bytes] = []

    t0 = time.monotonic()

    with tmp_output.open("wb") as fh:
        for doc in iter_intentos(config):
            validate_intento(doc)
            line_bytes = (serialize_canonical(doc) + "\n").encode("utf-8")
            batch_lines.append(line_bytes)

            total_docs += 1
            resps = doc["respuestas"]
            total_respuestas += len(resps)
            for r in resps:
                if r["evaluacion"]["puntaje_maximo"] > 5:
                    total_respuestas_filtradas_q1 += 1
            if doc["estado"] == "completado":
                total_completados_q2 += 1

            if len(batch_lines) >= config.batch_size:
                chunk = b"".join(batch_lines)
                fh.write(chunk)
                hasher.update(chunk)
                total_bytes += len(chunk)
                batch_lines.clear()

        if batch_lines:
            chunk = b"".join(batch_lines)
            fh.write(chunk)
            hasher.update(chunk)
            total_bytes += len(chunk)
            batch_lines.clear()

    elapsed_s = round(time.monotonic() - t0, 3)
    tmp_output.replace(output_path)

    sha256_hex = hasher.hexdigest()
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generator_version": GENERATOR_VERSION,
        "seed": config.seed,
        "record_count": total_docs,
        "avg_respuestas_config": config.avg_respuestas,
        "avg_respuestas_observed": round(total_respuestas / total_docs, 4)
        if total_docs
        else 0.0,
        "total_respuestas": total_respuestas,
        "q1_qualifying_respuestas": total_respuestas_filtradas_q1,
        "q2_completed_intentos": total_completados_q2,
        "batch_size": config.batch_size,
        "file_name": output_path.name,
        "file_size_bytes": total_bytes,
        "sha256": sha256_hex,
        "generation_elapsed_seconds": elapsed_s,
    }

    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def compute_file_sha256(path: Path, chunk_size: int = 4 * 1024 * 1024) -> str:
    """Calcula el SHA-256 de un archivo en streaming."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while chunk := fh.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def verify_manifest(dataset_path: Path, manifest_path: Path) -> bool:
    """Verifica que el archivo JSONL exista y coincida en tamaño y SHA-256 con su manifiesto."""
    if not dataset_path.exists() or not manifest_path.exists():
        return False
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if dataset_path.stat().st_size != manifest.get("file_size_bytes"):
        return False
    return compute_file_sha256(dataset_path) == manifest.get("sha256")


def default_paths(count: int, seed: int) -> tuple[Path, Path]:
    tag = f"intentos_n{count}_s{seed}"
    dataset_path = ROOT / "dataset" / "generated" / f"{tag}.jsonl"
    manifest_path = ROOT / "dataset" / "manifests" / f"{tag}.manifest.json"
    return dataset_path, manifest_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generador determinista del dataset sintético de intentos (ETICORE)."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("DATASET_SEED", "20261030")),
        help="Semilla determinista (por defecto DATASET_SEED o 20261030).",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.environ.get("DATASET_SIZE", "500000")),
        help="Cantidad de documentos a generar (por defecto DATASET_SIZE o 500000).",
    )
    parser.add_argument(
        "--avg-respuestas",
        type=int,
        default=int(os.environ.get("DATASET_AVG_RESPUESTAS", "3")),
        help="Promedio de respuestas por intento (1..5, por defecto 3).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=5000,
        help="Tamaño de lote para escritura en streaming (por defecto 5000).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ruta del archivo JSONL de salida.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Ruta del manifiesto JSON de salida.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Regenerar aunque ya exista un dataset válido con el mismo manifiesto.",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Solo verificar el dataset existente contra su manifiesto.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    out_path, man_path = default_paths(args.count, args.seed)
    if args.output is not None:
        out_path = args.output
    if args.manifest is not None:
        man_path = args.manifest

    if args.verify_only:
        ok = verify_manifest(out_path, man_path)
        print(f"Verificación de {out_path.name}: {'OK' if ok else 'FALLÓ'}")
        return 0 if ok else 1

    cfg = GeneratorConfig(
        seed=args.seed,
        count=args.count,
        avg_respuestas=args.avg_respuestas,
        batch_size=args.batch_size,
    )
    manifest = generate_dataset(cfg, out_path, man_path, force=args.force)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

