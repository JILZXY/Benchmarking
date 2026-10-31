#!/usr/bin/env python3
"""Generador de resúmenes estadísticos y reporte de hallazgos de spill/OOM a partir del CSV normalizado.

Escribe en `results/summaries/`:
- `summary_by_scenario.json`: medias de throughput, P50, P95, P99, CPU throttling, RSS peak
  y conteo de estados (`ok`, `spill`, `oom`, `error`) agrupados por
  `(engine, profile, workload, concurrency)`.
- `spill_and_oom_report.json`: registra explícitamente cuándo ocurrió el primer spill
  (motor, perfil, consulta, concurrencia y repetición) o, si ninguna configuración produjo
  spill en el rango evaluado, deja constancia explícita de que no se observó almacenamiento
  temporal (sin tratar la ausencia de spill como un fallo del experimento).
"""

from __future__ import annotations

from collections import defaultdict
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
from typing import Any

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "results").exists() else Path("/app")
SRC_DIR = Path(__file__).resolve().parents[1]
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from reporting.incremental_writer import DEFAULT_CSV_PATH  # noqa: E402

DEFAULT_SUMMARY_DIR = ROOT / "results" / "summaries"


def _to_float(val: Any) -> float:
    try:
        return float(val) if val not in ("", None) else 0.0
    except (TypeError, ValueError):
        return 0.0


def _to_int(val: Any) -> int:
    try:
        return int(float(val)) if val not in ("", None) else 0
    except (TypeError, ValueError):
        return 0


def _to_bool(val: Any) -> bool:
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("true", "1", "yes")


def build_summaries_from_csv(
    csv_path: Path = DEFAULT_CSV_PATH,
    summary_dir: Path = DEFAULT_SUMMARY_DIR,
) -> dict[str, Any]:
    """Lee el CSV normalizado y genera los resúmenes en `results/summaries/`."""
    summary_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    if csv_path.exists() and csv_path.stat().st_size > 0:
        with csv_path.open("r", encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh))

    grouped: dict[tuple[str, str, str, int], list[dict[str, Any]]] = defaultdict(list)
    first_spill_row: dict[str, Any] | None = None
    spill_runs: list[dict[str, Any]] = []
    oom_runs: list[dict[str, Any]] = []

    # Si un mismo scenario_key fue reintentado con --retry-errors tras un 'error'
    # de infraestructura, el CSV conserva todas las filas históricas (inmutable),
    # pero las medias por escenario deben calcularse sobre el intento más reciente
    # (o el intento válido ok/spill/oom) de cada repetición para no sesgar promedios.
    latest_by_scenario_rep: dict[tuple[str, str, str, int, int], dict[str, Any]] = {}
    for r in rows:
        eng = r.get("engine", "")
        prof = r.get("profile") or r.get("profile_id", "")
        wl = r.get("workload") or f"{r.get('query_code', '')}:{r.get('variant_id', '')}"
        conc = _to_int(r.get("concurrency", 0))
        rep = _to_int(r.get("repetition", 1))
        key = (eng, prof, wl, conc, rep)
        prev = latest_by_scenario_rep.get(key)
        if prev is None:
            latest_by_scenario_rep[key] = r
        else:
            prev_valid = prev.get("status") in ("ok", "spill", "oom")
            curr_valid = r.get("status") in ("ok", "spill", "oom")
            if curr_valid and not prev_valid:
                latest_by_scenario_rep[key] = r
            elif curr_valid == prev_valid and _to_int(r.get("attempt", 1)) >= _to_int(
                prev.get("attempt", 1)
            ):
                latest_by_scenario_rep[key] = r

    for (eng, prof, wl, conc, _rep), r in sorted(latest_by_scenario_rep.items()):
        grouped[(eng, prof, wl, conc)].append(r)

        if _to_bool(r.get("disk_spill_detected")) or r.get("status") == "spill":
            entry = {
                "run_id": r.get("run_id"),
                "sequence_index": _to_int(r.get("sequence_index")),
                "engine": eng,
                "profile": prof,
                "workload": wl,
                "concurrency": conc,
                "repetition": _to_int(r.get("repetition")),
                "spill_bytes": _to_int(r.get("spill_bytes")),
                "status": r.get("status"),
            }
            spill_runs.append(entry)
            if first_spill_row is None:
                first_spill_row = entry

        if _to_bool(r.get("oom_killed")) or r.get("status") == "oom":
            oom_runs.append(
                {
                    "run_id": r.get("run_id"),
                    "sequence_index": _to_int(r.get("sequence_index")),
                    "engine": eng,
                    "profile": prof,
                    "workload": wl,
                    "concurrency": conc,
                    "repetition": _to_int(r.get("repetition")),
                    "status": r.get("status"),
                }
            )

    scenario_summaries: list[dict[str, Any]] = []
    for (eng, prof, wl, conc), items in sorted(grouped.items()):
        n = len(items)
        status_counts: dict[str, int] = {"ok": 0, "spill": 0, "oom": 0, "error": 0}
        for it in items:
            st = it.get("status", "ok")
            status_counts[st] = status_counts.get(st, 0) + 1

        scenario_summaries.append(
            {
                "engine": eng,
                "profile": prof,
                "workload": wl,
                "concurrency": conc,
                "repetitions_recorded": n,
                "status_counts": status_counts,
                "mean_throughput_qps": round(
                    sum(_to_float(i.get("throughput")) for i in items) / n, 4
                ),
                "mean_latency_p50_ms": round(
                    sum(_to_float(i.get("latency_p50")) for i in items) / n, 4
                ),
                "mean_latency_p95_ms": round(
                    sum(_to_float(i.get("latency_p95")) for i in items) / n, 4
                ),
                "mean_latency_p99_ms": round(
                    sum(_to_float(i.get("latency_p99")) for i in items) / n, 4
                ),
                "mean_cpu_throttled_seconds": round(
                    sum(_to_float(i.get("cpu_throttled_seconds")) for i in items) / n, 6
                ),
                "max_memory_rss_peak_bytes": max(
                    _to_int(i.get("memory_rss_peak_bytes")) for i in items
                ),
                "any_spill_detected": any(
                    _to_bool(i.get("disk_spill_detected")) for i in items
                ),
                "any_oom_killed": any(_to_bool(i.get("oom_killed")) for i in items),
            }
        )

    spill_note = (
        f"Se observó el primer spill en run_id={first_spill_row['run_id']} "
        f"(engine={first_spill_row['engine']}, profile={first_spill_row['profile']}, "
        f"workload={first_spill_row['workload']}, concurrency={first_spill_row['concurrency']}, "
        f"repetition={first_spill_row['repetition']})."
        if first_spill_row
        else "No se observó almacenamiento temporal (spill a disco) en ninguna de las "
        "configuraciones de recursos y concurrencia evaluadas. Conforme al criterio "
        "metodológico del experimento, la ausencia de spill es un hallazgo empírico "
        "válido y no constituye un fallo del experimento."
    )

    spill_oom_report = {
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_runs_evaluated": len(rows),
        "spill_observed": first_spill_row is not None,
        "first_spill": first_spill_row,
        "total_spill_runs": len(spill_runs),
        "spill_runs": spill_runs,
        "oom_observed": len(oom_runs) > 0,
        "total_oom_runs": len(oom_runs),
        "oom_runs": oom_runs,
        "empirical_interpretation": spill_note,
    }

    (summary_dir / "summary_by_scenario.json").write_text(
        json.dumps(scenario_summaries, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (summary_dir / "spill_and_oom_report.json").write_text(
        json.dumps(spill_oom_report, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "scenarios_summarized": len(scenario_summaries),
        "total_runs_evaluated": len(rows),
        "spill_observed": spill_oom_report["spill_observed"],
        "oom_observed": spill_oom_report["oom_observed"],
    }


def main() -> int:
    res = build_summaries_from_csv()
    print(json.dumps(res, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())

