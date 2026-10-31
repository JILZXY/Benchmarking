"""Cálculo de percentiles de latencia (P50, P95, P99), throughput (QPS) y estadísticas de ejecución."""

from __future__ import annotations

from dataclasses import dataclass
import math
from typing import Sequence


@dataclass(frozen=True)
class LatencySummary:
    sample_count: int
    failed_count: int
    window_seconds: float
    throughput_qps: float
    latency_min_ms: float
    latency_mean_ms: float
    latency_p50_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_max_ms: float


def compute_percentile(sorted_values: Sequence[float], percentile: float) -> float:
    """Calcula un percentil (0..100) con interpolación lineal sobre una lista ya ordenada."""
    if not sorted_values:
        return 0.0
    if not (0.0 <= percentile <= 100.0):
        raise ValueError("percentile debe estar entre 0 y 100")
    n = len(sorted_values)
    if n == 1:
        return float(sorted_values[0])

    pos = (percentile / 100.0) * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_values[lo])
    weight = pos - lo
    return float(sorted_values[lo] * (1.0 - weight) + sorted_values[hi] * weight)


def summarize_latencies(
    latencies_ms: Sequence[float],
    failed_count: int,
    window_seconds: float,
) -> LatencySummary:
    """Resume una serie de latencias en milisegundos y calcula el throughput QPS."""
    eff_window = max(float(window_seconds), 1e-9)
    n = len(latencies_ms)
    if n == 0:
        return LatencySummary(
            sample_count=0,
            failed_count=failed_count,
            window_seconds=round(window_seconds, 4),
            throughput_qps=0.0,
            latency_min_ms=0.0,
            latency_mean_ms=0.0,
            latency_p50_ms=0.0,
            latency_p95_ms=0.0,
            latency_p99_ms=0.0,
            latency_max_ms=0.0,
        )

    ordered = sorted(float(x) for x in latencies_ms)
    mean_val = sum(ordered) / n
    return LatencySummary(
        sample_count=n,
        failed_count=failed_count,
        window_seconds=round(window_seconds, 4),
        throughput_qps=round(n / eff_window, 4),
        latency_min_ms=round(ordered[0], 4),
        latency_mean_ms=round(mean_val, 4),
        latency_p50_ms=round(compute_percentile(ordered, 50.0), 4),
        latency_p95_ms=round(compute_percentile(ordered, 95.0), 4),
        latency_p99_ms=round(compute_percentile(ordered, 99.0), 4),
        latency_max_ms=round(ordered[-1], 4),
    )

