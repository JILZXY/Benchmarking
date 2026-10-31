"""Clasificación del resultado de una ejecución del benchmark.

Cada ejecución termina en exactamente uno de cuatro estados oficiales:

    ok     -> la ejecución terminó, sin errores y sin almacenamiento temporal.
    spill  -> la ejecución terminó, sin errores, y el motor usó almacenamiento
              temporal en disco (evidencia observada, no forzada).
    oom    -> se detectó un evento Out Of Memory (OOM killer del cgroup,
              contenedor marcado como OOMKilled o fallo de asignación del motor).
    error  -> la ejecución no terminó o registró operaciones fallidas por encima
              del umbral permitido, sin evidencia de OOM.

Precedencia: oom > error > spill > ok.

La ausencia de spill NO es un fallo: una ejecución sin spill y sin errores es
``ok``. La bandera ``spill_detected`` se conserva siempre de forma independiente
para que un spill no se pierda cuando el estado principal es ``oom`` o ``error``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RunStatus(str, Enum):
    OK = "ok"
    SPILL = "spill"
    OOM = "oom"
    ERROR = "error"


@dataclass(frozen=True)
class SpillEvidence:
    """Evidencia de uso de almacenamiento temporal reportada por el motor."""

    detected: bool
    sources: tuple[str, ...] = ()
    bytes_spilled: int | None = None
    details: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def none() -> "SpillEvidence":
        return SpillEvidence(detected=False)


@dataclass(frozen=True)
class OomEvidence:
    """Evidencia de un evento Out Of Memory."""

    detected: bool
    sources: tuple[str, ...] = ()
    details: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def none() -> "OomEvidence":
        return OomEvidence(detected=False)


@dataclass(frozen=True)
class ExecutionOutcome:
    """Resultado operativo de la ventana de medición."""

    completed: bool
    total_ops: int
    failed_ops: int = 0
    error_samples: tuple[str, ...] = ()

    @property
    def error_ratio(self) -> float:
        attempted = self.total_ops + self.failed_ops
        return self.failed_ops / attempted if attempted else 0.0


@dataclass(frozen=True)
class RunClassification:
    status: RunStatus
    reason: str
    spill_detected: bool
    oom_detected: bool


def classify_run(
    spill: SpillEvidence,
    oom: OomEvidence,
    execution: ExecutionOutcome,
    max_error_ratio: float = 0.0,
) -> RunClassification:
    """Asigna el estado oficial de la ejecución aplicando la precedencia."""

    if oom.detected:
        status = RunStatus.OOM
        reason = "OOM detectado por: " + ", ".join(oom.sources)
    elif not execution.completed:
        status = RunStatus.ERROR
        reason = "La ejecución no completó la ventana de medición"
    elif execution.error_ratio > max_error_ratio:
        status = RunStatus.ERROR
        reason = (
            f"{execution.failed_ops} operaciones fallidas "
            f"(ratio {execution.error_ratio:.4f} > {max_error_ratio:.4f})"
        )
    elif spill.detected:
        status = RunStatus.SPILL
        reason = "Almacenamiento temporal detectado por: " + ", ".join(spill.sources)
    else:
        status = RunStatus.OK
        reason = "Sin errores y sin almacenamiento temporal observado"

    return RunClassification(
        status=status,
        reason=reason,
        spill_detected=spill.detected,
        oom_detected=oom.detected,
    )

