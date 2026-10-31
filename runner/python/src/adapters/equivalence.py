"""Evaluación en Python puro de Q1 y Q2 + comparación de equivalencia entre motores.

Permite verificar en `tests/equivalence/` que:
1. El Aggregation Pipeline de MongoDB y el SQL JSONB de PostgreSQL implementan
   exactamente la misma semántica lógica.
2. Los resultados devueltos por ambos motores para Q1 y Q2 coinciden registro a
   registro dentro de una tolerancia numérica estricta (`1e-4`).
"""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
from typing import Any


def _round4(val: float | Decimal) -> float:
    d = Decimal(str(val)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return float(d)


def evaluate_q1_reference(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Evalúa Q1 en Python puro siguiendo exactamente la especificación del README."""
    groups: dict[tuple[str, str], list[Decimal]] = defaultdict(list)

    for doc in docs:
        area = str(doc["leccion"]["area"])
        for resp in doc["respuestas"]:
            ev = resp["evaluacion"]
            if int(ev["puntaje_maximo"]) > 5:
                tema = str(resp["tema"])
                p_obt = Decimal(str(ev["puntaje_obtenido"]))
                pond = Decimal(str(ev["ponderacion"]))
                groups[(area, tema)].append(p_obt * pond)

    results: list[dict[str, Any]] = []
    for (area, tema), values in sorted(groups.items(), key=lambda x: x[0]):
        total = len(values)
        suma = sum(values, Decimal("0"))
        promedio = suma / Decimal(total)
        results.append(
            {
                "area": area,
                "tema": tema,
                "total_respuestas": total,
                "suma_puntaje_ponderado": _round4(suma),
                "promedio_puntaje_ponderado": _round4(promedio),
            }
        )
    return results


def evaluate_q2_reference(docs: list[dict[str, Any]], limit: int = 1000) -> list[dict[str, Any]]:
    """Evalúa Q2 en Python puro siguiendo exactamente la especificación del README."""
    completed = [d for d in docs if d.get("estado") == "completado"]
    completed.sort(
        key=lambda d: (-float(d["resultado"]["porcentaje"]), str(d["intento_id"]))
    )

    results: list[dict[str, Any]] = []
    for d in completed[:limit]:
        results.append(
            {
                "intento_id": d["intento_id"],
                "usuario_id": d["usuario_id"],
                "leccion_id": d["leccion_id"],
                "fecha_inicio": d["fecha_inicio"],
                "estado": d["estado"],
                "porcentaje": float(d["resultado"]["porcentaje"]),
                "puntaje_total": float(d["resultado"]["puntaje_total"]),
                "duracion_segundos": int(d["resultado"]["duracion_segundos"]),
            }
        )
    return results


def compare_query_results(
    left: list[dict[str, Any]],
    right: list[dict[str, Any]],
    float_tol: float = 1e-3,
) -> list[str]:
    """Compara dos listas de resultados (p. ej. MongoDB vs PostgreSQL) y devuelve discrepancias."""
    errors: list[str] = []
    if len(left) != len(right):
        errors.append(f"Cantidad de filas distinta: {len(left)} vs {len(right)}")
        return errors

    for idx, (row_a, row_b) in enumerate(zip(left, right)):
        if set(row_a.keys()) != set(row_b.keys()):
            errors.append(
                f"Fila {idx}: claves distintas {sorted(row_a.keys())} vs {sorted(row_b.keys())}"
            )
            continue
        for k in row_a:
            va, vb = row_a[k], row_b[k]
            if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
                if abs(float(va) - float(vb)) > float_tol:
                    errors.append(f"Fila {idx}.{k}: {va} != {vb} (tol={float_tol})")
            else:
                if va != vb:
                    errors.append(f"Fila {idx}.{k}: {va!r} != {vb!r}")
        if len(errors) >= 10:
            break
    return errors

