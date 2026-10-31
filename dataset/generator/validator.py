"""Validador sin dependencias externas para documentos de la colección `intentos`.

Garantiza que cada registro generado cumpla exactamente con el contrato de
`dataset/schema/intentos.schema.json` antes de ser serializado al archivo JSONL.
"""

from __future__ import annotations

import re
from typing import Any

UUID_V4_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
ISO8601_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
SESSION_ID_RE = re.compile(r"^session-[0-9]{6}$")

VALID_ESTADOS = frozenset({"completado", "en_progreso", "abandonado", "expirado"})
VALID_NIVELES = frozenset({"basico", "intermedio", "avanzado"})
VALID_SEGMENTOS = frozenset({"estudiante", "docente", "profesional", "investigador"})
VALID_AREAS = frozenset(
    {
        "etica",
        "privacidad",
        "seguridad",
        "cumplimiento",
        "gobernanza_ia",
        "propiedad_intelectual",
    }
)
VALID_DIFICULTADES = frozenset({"baja", "media", "alta"})
VALID_TIPOS_PREGUNTA = frozenset(
    {"opcion_multiple", "verdadero_falso", "caso_practico", "emparejamiento"}
)
VALID_PUNTAJES_MAXIMOS = frozenset({2, 4, 5, 8, 10, 12, 15, 20})
VALID_CANALES = frozenset({"web", "movil", "api", "lms"})
VALID_FLAGS = frozenset(
    {
        "reintento",
        "tiempo_extendido",
        "modo_accesible",
        "conexion_inestable",
        "auditoria_activa",
    }
)

TOP_LEVEL_KEYS = frozenset(
    {
        "intento_id",
        "fecha_inicio",
        "usuario_id",
        "leccion_id",
        "estado",
        "usuario",
        "leccion",
        "respuestas",
        "resultado",
        "metadata",
    }
)


class ValidationError(ValueError):
    """Error de validación estructural o de dominio en un documento `intento`."""


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def validate_intento(doc: dict[str, Any]) -> None:
    """Valida un documento de `intentos`. Lanza `ValidationError` si es inválido."""
    if not isinstance(doc, dict):
        raise ValidationError("El documento debe ser un diccionario")

    if set(doc.keys()) != TOP_LEVEL_KEYS:
        missing = TOP_LEVEL_KEYS - set(doc.keys())
        extra = set(doc.keys()) - TOP_LEVEL_KEYS
        raise ValidationError(f"Claves raíz inválidas (faltan={missing}, sobran={extra})")

    for id_field in ("intento_id", "usuario_id", "leccion_id"):
        val = doc[id_field]
        if not isinstance(val, str) or not UUID_V4_RE.match(val):
            raise ValidationError(f"{id_field} no es un UUID v4 válido: {val!r}")

    fecha = doc["fecha_inicio"]
    if not isinstance(fecha, str) or not ISO8601_UTC_RE.match(fecha):
        raise ValidationError(f"fecha_inicio no es ISO-8601 UTC válido: {fecha!r}")

    if doc["estado"] not in VALID_ESTADOS:
        raise ValidationError(f"estado inválido: {doc['estado']!r}")

    usuario = doc["usuario"]
    if not isinstance(usuario, dict) or set(usuario.keys()) != {"nivel", "segmento"}:
        raise ValidationError("Objeto usuario inválido")
    if usuario["nivel"] not in VALID_NIVELES:
        raise ValidationError(f"usuario.nivel inválido: {usuario['nivel']!r}")
    if usuario["segmento"] not in VALID_SEGMENTOS:
        raise ValidationError(f"usuario.segmento inválido: {usuario['segmento']!r}")

    leccion = doc["leccion"]
    if not isinstance(leccion, dict) or set(leccion.keys()) != {"area", "tema", "dificultad"}:
        raise ValidationError("Objeto leccion inválido")
    if leccion["area"] not in VALID_AREAS:
        raise ValidationError(f"leccion.area inválida: {leccion['area']!r}")
    if not isinstance(leccion["tema"], str) or not leccion["tema"]:
        raise ValidationError("leccion.tema debe ser un string no vacío")
    if leccion["dificultad"] not in VALID_DIFICULTADES:
        raise ValidationError(f"leccion.dificultad inválida: {leccion['dificultad']!r}")

    respuestas = doc["respuestas"]
    if not isinstance(respuestas, list) or not (1 <= len(respuestas) <= 6):
        raise ValidationError("respuestas debe ser una lista de 1 a 6 elementos")

    for idx, resp in enumerate(respuestas):
        if not isinstance(resp, dict) or set(resp.keys()) != {
            "pregunta_id",
            "tema",
            "tipo",
            "evaluacion",
        }:
            raise ValidationError(f"respuestas[{idx}] tiene claves inválidas")
        if not isinstance(resp["pregunta_id"], str) or not UUID_V4_RE.match(resp["pregunta_id"]):
            raise ValidationError(f"respuestas[{idx}].pregunta_id inválido")
        if not isinstance(resp["tema"], str) or not resp["tema"]:
            raise ValidationError(f"respuestas[{idx}].tema inválido")
        if resp["tipo"] not in VALID_TIPOS_PREGUNTA:
            raise ValidationError(f"respuestas[{idx}].tipo inválido: {resp['tipo']!r}")

        ev = resp["evaluacion"]
        if not isinstance(ev, dict) or set(ev.keys()) != {
            "puntaje_obtenido",
            "puntaje_maximo",
            "ponderacion",
        }:
            raise ValidationError(f"respuestas[{idx}].evaluacion tiene claves inválidas")
        p_max = ev["puntaje_maximo"]
        p_obt = ev["puntaje_obtenido"]
        pond = ev["ponderacion"]
        if not _is_int(p_max) or p_max not in VALID_PUNTAJES_MAXIMOS:
            raise ValidationError(f"puntaje_maximo inválido: {p_max!r}")
        if not _is_int(p_obt) or not (0 <= p_obt <= p_max):
            raise ValidationError(f"puntaje_obtenido fuera de rango [0, {p_max}]: {p_obt!r}")
        if not _is_number(pond) or not (0.5 <= float(pond) <= 2.0):
            raise ValidationError(f"ponderacion fuera de rango [0.5, 2.0]: {pond!r}")

    resultado = doc["resultado"]
    if not isinstance(resultado, dict) or set(resultado.keys()) != {
        "puntaje_total",
        "porcentaje",
        "duracion_segundos",
    }:
        raise ValidationError("Objeto resultado inválido")
    if not _is_number(resultado["puntaje_total"]) or resultado["puntaje_total"] < 0:
        raise ValidationError(f"resultado.puntaje_total inválido: {resultado['puntaje_total']!r}")
    if not _is_number(resultado["porcentaje"]) or not (0 <= float(resultado["porcentaje"]) <= 100):
        raise ValidationError(f"resultado.porcentaje fuera de [0, 100]: {resultado['porcentaje']!r}")
    if not _is_int(resultado["duracion_segundos"]) or not (
        15 <= resultado["duracion_segundos"] <= 7200
    ):
        raise ValidationError(
            f"resultado.duracion_segundos fuera de [15, 7200]: {resultado['duracion_segundos']!r}"
        )

    metadata = doc["metadata"]
    if not isinstance(metadata, dict) or set(metadata.keys()) != {"canal", "session_id", "flags"}:
        raise ValidationError("Objeto metadata inválido")
    if metadata["canal"] not in VALID_CANALES:
        raise ValidationError(f"metadata.canal inválido: {metadata['canal']!r}")
    if not isinstance(metadata["session_id"], str) or not SESSION_ID_RE.match(
        metadata["session_id"]
    ):
        raise ValidationError(f"metadata.session_id inválido: {metadata['session_id']!r}")
    flags = metadata["flags"]
    if not isinstance(flags, list) or any(f not in VALID_FLAGS for f in flags):
        raise ValidationError(f"metadata.flags inválido: {flags!r}")

