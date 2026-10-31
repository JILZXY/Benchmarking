"""Generador de la matriz experimental de 108 escenarios y sistema atómico de checkpoints y recuperación.

Características del sistema de checkpoints (`CheckpointStore`):
1. Archivo de estado persistente con escritura atómica (`tempfile` + `os.fsync()` + `os.replace()`).
2. Registro explícito de:
   - `completed`: escenarios finalizados con resultado terminal válido (`ok`, `spill`, `oom`).
   - `failed`: escenarios que terminaron con error de infraestructura/ejecución (`error`).
   - `interrupted`: ejecuciones que quedaron en curso (`in_progress`) cuando el proceso se
     detuvo inesperadamente o recibieron `SIGINT`/`SIGTERM`, conservando su historial.
   - `pending`: lista ordenada de `scenario_key` aún pendientes de ejecución.
   - `history`: bitácora inmutable de todos los intentos realizados (nunca se sobrescribe).
3. Reglas de reanudación (`--resume` y `--retry-errors`):
   - Los escenarios en `completed` (`ok`, `spill`, `oom`) se consideran resultados experimentales
     válidos y **nunca** se repiten automáticamente.
   - Al iniciar o reanudar, cualquier escenario que hubiera quedado en `in_progress` se
     reclasifica automáticamente como `interrupted` y vuelve a la cola de pendientes.
   - Los escenarios en `failed` (`error`) se reintentan únicamente cuando se pasa
     `retry_errors=True` (`--retry-errors`).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any
import uuid

_CANDIDATE_ROOT = Path(__file__).resolve().parents[4]
ROOT = _CANDIDATE_ROOT if (_CANDIDATE_ROOT / "config").exists() else Path("/app")
DEFAULT_MATRIX_PATH = ROOT / "config" / "benchmark" / "matrix.json"
DEFAULT_CHECKPOINT_PATH = ROOT / "results" / "raw" / "checkpoint.json"

VALID_TERMINAL_STATUSES = frozenset({"ok", "spill", "oom"})


@dataclass(frozen=True)
class MatrixScenario:
    sequence_index: int
    scenario_key: str
    engine: str
    profile_id: str
    profile_short_code: str
    profile_cpus: int
    profile_memory_mb: int
    query_code: str
    variant_id: str
    concurrency: int
    repetition: int
    warmup_seconds: float
    measurement_seconds: float
    cooldown_seconds: float


def _utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_unique_run_id(sequence_index: int, attempt: int = 1) -> str:
    """Genera un identificador único global por ejecución que incluye secuencia, intento, UTC y UUID."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = uuid.uuid4().hex[:8]
    return f"run-{sequence_index:03d}-a{attempt:02d}-{ts}-{suffix}"


def load_matrix_config(matrix_path: Path = DEFAULT_MATRIX_PATH) -> dict[str, Any]:
    return json.loads(matrix_path.read_text(encoding="utf-8"))


def expand_matrix_scenarios(
    matrix_path: Path = DEFAULT_MATRIX_PATH,
    include_all_q2_variants: bool = False,
    warmup_override: float | None = None,
    measurement_override: float | None = None,
    cooldown_override: float | None = None,
) -> list[MatrixScenario]:
    """Expande en orden determinista la matriz experimental (108 escenarios oficiales)."""
    cfg = load_matrix_config(matrix_path)
    durations = cfg["durations_seconds"]
    warmup_s = float(warmup_override if warmup_override is not None else durations["warmup"])
    meas_s = float(
        measurement_override if measurement_override is not None else durations["measurement"]
    )
    cool_s = float(cooldown_override if cooldown_override is not None else durations["cooldown"])

    scenarios: list[MatrixScenario] = []
    seq = 0

    for engine in cfg["engines"]:
        for prof in cfg["profiles"]:
            profile_id = prof["id"]
            short_code = prof["short_code"]
            cpus = int(prof["cpus"])
            mem_mb = int(prof["memory_mb"])

            for query_code in cfg["queries"]:
                if query_code == "Q2" and include_all_q2_variants:
                    variant_list = list(cfg["q2_all_variants"][engine])
                else:
                    variant_list = [cfg["default_variants"][engine][query_code]]

                for variant_id in variant_list:
                    for concurrency in cfg["concurrency_levels"]:
                        for rep in range(1, int(cfg["repetitions"]) + 1):
                            seq += 1
                            key = (
                                f"{engine}__{profile_id}__{query_code}__{variant_id}"
                                f"__c{concurrency}__r{rep}"
                            )
                            scenarios.append(
                                MatrixScenario(
                                    sequence_index=seq,
                                    scenario_key=key,
                                    engine=engine,
                                    profile_id=profile_id,
                                    profile_short_code=short_code,
                                    profile_cpus=cpus,
                                    profile_memory_mb=mem_mb,
                                    query_code=query_code,
                                    variant_id=variant_id,
                                    concurrency=int(concurrency),
                                    repetition=rep,
                                    warmup_seconds=warmup_s,
                                    measurement_seconds=meas_s,
                                    cooldown_seconds=cool_s,
                                )
                            )

    return scenarios


class CheckpointStore:
    """Almacén de estado persistente con escritura atómica y recuperación de interrupciones."""

    def __init__(
        self,
        path: Path = DEFAULT_CHECKPOINT_PATH,
        scenarios: list[MatrixScenario] | None = None,
    ) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._scenarios_by_key: dict[str, MatrixScenario] = (
            {s.scenario_key: s for s in scenarios} if scenarios else {}
        )
        self._data: dict[str, Any] = self._load_and_recover()

    def _default_state(self) -> dict[str, Any]:
        return {
            "version": "1.1.0",
            "updated_at_utc": _utc_now(),
            "in_progress": None,
            "completed": {},
            "failed": {},
            "interrupted": [],
            "pending": [],
            "history": [],
            "first_spill": None,
        }

    def _load_and_recover(self) -> dict[str, Any]:
        if not self.path.exists():
            state = self._default_state()
            self._recompute_pending(state)
            return state

        state = json.loads(self.path.read_text(encoding="utf-8"))
        for key, default_val in self._default_state().items():
            state.setdefault(key, default_val)

        # Recuperación automática: si había una ejecución `in_progress` cuando el proceso
        # anterior murió, la registramos en `interrupted` y limpiamos `in_progress`.
        active = state.get("in_progress")
        if isinstance(active, dict) and active.get("scenario_key"):
            interrupted_entry = {
                **active,
                "interrupted_at_utc": _utc_now(),
                "reason": "Ejecución en curso detectada al iniciar/reanudar el orquestador",
            }
            state["interrupted"].append(interrupted_entry)
            state["history"].append({**interrupted_entry, "event": "recovered_interruption"})
            state["in_progress"] = None
            self._recompute_pending(state)
            self._atomic_write(state)
        else:
            self._recompute_pending(state)

        return state

    def bind_scenarios(self, scenarios: list[MatrixScenario]) -> None:
        """Vincula el catálogo de escenarios de la matriz y recalcula la lista `pending`."""
        self._scenarios_by_key = {s.scenario_key: s for s in scenarios}
        self._recompute_pending(self._data)
        self._atomic_write(self._data)

    def _recompute_pending(self, state: dict[str, Any]) -> None:
        if not self._scenarios_by_key:
            return
        completed = state.get("completed", {})
        in_prog_key = (
            state["in_progress"].get("scenario_key")
            if isinstance(state.get("in_progress"), dict)
            else None
        )
        pending_keys = [
            key
            for key in self._scenarios_by_key
            if key not in completed and key != in_prog_key
        ]
        state["pending"] = pending_keys

    def next_attempt_number(self, scenario_key: str) -> int:
        prev = sum(
            1
            for h in self._data.get("history", [])
            if h.get("scenario_key") == scenario_key and h.get("event") == "started"
        )
        return prev + 1

    def is_completed(self, scenario_key: str) -> bool:
        """Devuelve True si el escenario tiene un resultado válido (`ok`, `spill`, `oom`)."""
        return scenario_key in self._data.get("completed", {})

    def is_failed(self, scenario_key: str) -> bool:
        """Devuelve True si el escenario terminó previamente en `error` y aún no se ha completado."""
        return (
            scenario_key in self._data.get("failed", {})
            and scenario_key not in self._data.get("completed", {})
        )

    def should_execute(self, scenario_key: str, retry_errors: bool = False) -> bool:
        """Determina si un escenario debe ejecutarse en una corrida o reanudación (`--resume`)."""
        if self.is_completed(scenario_key):
            return False
        if self.is_failed(scenario_key) and not retry_errors:
            return False
        return True

    def mark_in_progress(self, scenario: MatrixScenario, run_id: str, attempt: int) -> None:
        """Registra de forma atómica que `scenario` acaba de iniciar."""
        now_utc = _utc_now()
        entry = {
            "run_id": run_id,
            "attempt": attempt,
            "sequence_index": scenario.sequence_index,
            "scenario_key": scenario.scenario_key,
            "engine": scenario.engine,
            "profile_id": scenario.profile_id,
            "query_code": scenario.query_code,
            "variant_id": scenario.variant_id,
            "concurrency": scenario.concurrency,
            "repetition": scenario.repetition,
            "started_at_utc": now_utc,
        }
        self._data["in_progress"] = entry
        self._data["history"].append({**entry, "event": "started"})
        self._recompute_pending(self._data)
        self._atomic_write(self._data)

    def mark_interrupted(
        self, scenario: MatrixScenario, run_id: str, reason: str = "Interrumpido por señal/usuario"
    ) -> None:
        """Registra explícitamente que la ejecución activa fue interrumpida."""
        now_utc = _utc_now()
        active = self._data.get("in_progress") or {}
        entry = {
            "run_id": run_id,
            "attempt": active.get("attempt", 1),
            "sequence_index": scenario.sequence_index,
            "scenario_key": scenario.scenario_key,
            "engine": scenario.engine,
            "profile_id": scenario.profile_id,
            "query_code": scenario.query_code,
            "variant_id": scenario.variant_id,
            "concurrency": scenario.concurrency,
            "repetition": scenario.repetition,
            "started_at_utc": active.get("started_at_utc"),
            "interrupted_at_utc": now_utc,
            "reason": reason,
        }
        self._data["interrupted"].append(entry)
        self._data["history"].append({**entry, "event": "interrupted"})
        self._data["in_progress"] = None
        self._recompute_pending(self._data)
        self._atomic_write(self._data)

    def record_completion(
        self,
        scenario: MatrixScenario,
        run_id: str,
        status: str,
        spill_detected: bool,
        oom_detected: bool,
        attempt: int = 1,
        reason: str = "",
    ) -> None:
        """Registra la finalización de una ejecución diferenciando resultados válidos (`ok|spill|oom`) de `error`."""
        now_utc = _utc_now()
        active = self._data.get("in_progress") or {}
        entry = {
            "run_id": run_id,
            "attempt": attempt,
            "sequence_index": scenario.sequence_index,
            "scenario_key": scenario.scenario_key,
            "engine": scenario.engine,
            "profile_id": scenario.profile_id,
            "query_code": scenario.query_code,
            "variant_id": scenario.variant_id,
            "concurrency": scenario.concurrency,
            "repetition": scenario.repetition,
            "status": status,
            "status_reason": reason,
            "spill_detected": spill_detected,
            "oom_detected": oom_detected,
            "started_at_utc": active.get("started_at_utc"),
            "completed_at_utc": now_utc,
        }
        self._data["in_progress"] = None

        if status in VALID_TERMINAL_STATUSES:
            self._data["completed"][scenario.scenario_key] = entry
            # Si había fallado en un intento anterior, limpiar de `failed` (el intento previo sigue en `history`).
            self._data["failed"].pop(scenario.scenario_key, None)
        else:
            self._data["failed"][scenario.scenario_key] = entry

        self._data["history"].append({**entry, "event": "finished"})

        if spill_detected and self._data.get("first_spill") is None:
            self._data["first_spill"] = {
                "run_id": run_id,
                "scenario_key": scenario.scenario_key,
                "sequence_index": scenario.sequence_index,
                "engine": scenario.engine,
                "profile_id": scenario.profile_id,
                "query_code": scenario.query_code,
                "variant_id": scenario.variant_id,
                "concurrency": scenario.concurrency,
                "repetition": scenario.repetition,
                "observed_at_utc": now_utc,
            }

        self._recompute_pending(self._data)
        self._atomic_write(self._data)

    def first_spill(self) -> dict[str, Any] | None:
        return self._data.get("first_spill")

    def completed_count(self) -> int:
        return len(self._data.get("completed", {}))

    def failed_count(self) -> int:
        return len(self._data.get("failed", {}))

    def interrupted_count(self) -> int:
        return len(self._data.get("interrupted", []))

    def pending_keys(self) -> list[str]:
        return list(self._data.get("pending", []))

    def _atomic_write(self, state: dict[str, Any]) -> None:
        state["updated_at_utc"] = _utc_now()
        tmp = self.path.with_name(f"{self.path.name}.{os.getpid()}.tmp")
        payload = json.dumps(state, indent=2, sort_keys=True) + "\n"
        with tmp.open("w", encoding="utf-8", newline="\n") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._data)
