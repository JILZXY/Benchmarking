"""Cliente de telemetría para Prometheus (`/api/v1/query_range`) y métricas de cAdvisor/Node Exporter.

Consulta con paso de 1 segundo (`step=1s`) la ventana exacta `[start_epoch, end_epoch]`
para el contenedor del motor evaluado (`/docker/<container_id>`) y calcula:
- `container_cpu_cfs_throttled_seconds_total` / `cpu_throttled_seconds`: incremento total
  de segundos de CPU estrangulados por CFS durante la ventana de medición.
- `container_memory_rss_bytes` / `memory_rss_peak_bytes`: pico máximo (en bytes) de
  memoria RSS (`container_memory_rss` en cAdvisor) durante la ventana de medición,
  además del promedio (`memory_rss_avg_bytes`).
- Serie temporal completa guardada en `results/telemetry/<run_id>.prometheus.json`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from typing import Any
import urllib.parse
import urllib.request


@dataclass(frozen=True)
class ContainerTelemetrySummary:
    prometheus_reachable: bool
    container_id: str
    cpu_throttled_seconds: float
    memory_rss_peak_bytes: int
    memory_rss_avg_bytes: int
    cpu_sample_count: int = 0
    memory_sample_count: int = 0
    raw_series: dict[str, Any] = field(default_factory=dict)


def resolve_prometheus_url() -> str:
    """Devuelve la URL base de Prometheus tanto desde dentro de Docker como desde el host."""
    env_url = os.environ.get("PROMETHEUS_URL")
    if env_url:
        return env_url.rstrip("/")
    port = os.environ.get("PROMETHEUS_HOST_PORT", "9090")
    return f"http://127.0.0.1:{port}"


def query_prometheus_range(
    base_url: str,
    promql: str,
    start_epoch: float,
    end_epoch: float,
    step: str = "1s",
    timeout_s: float = 10.0,
) -> list[dict[str, Any]]:
    """Ejecuta una consulta `query_range` contra la API HTTP v1 de Prometheus usando `urllib`."""
    params = urllib.parse.urlencode(
        {
            "query": promql,
            "start": f"{start_epoch:.3f}",
            "end": f"{max(end_epoch, start_epoch + 1.0):.3f}",
            "step": step,
        }
    )
    url = f"{base_url.rstrip('/')}/api/v1/query_range?{params}"
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    if payload.get("status") != "success":
        return []
    return list(payload.get("data", {}).get("result", []))


def compute_counter_delta(series_result: list[dict[str, Any]]) -> tuple[float, int]:
    """Calcula el incremento (`last - first`, manejando reinicios de contador) y la cantidad de muestras."""
    if not series_result:
        return 0.0, 0

    total_delta = 0.0
    total_samples = 0
    for serie in series_result:
        values = serie.get("values", [])
        if not values:
            continue
        nums = [float(v[1]) for v in values]
        total_samples += len(nums)
        delta = 0.0
        prev = nums[0]
        for cur in nums[1:]:
            if cur >= prev:
                delta += cur - prev
            else:
                # Reinicio del contador
                delta += cur
            prev = cur
        total_delta = max(total_delta, delta)

    return round(total_delta, 6), total_samples


def compute_gauge_peak_and_avg(series_result: list[dict[str, Any]]) -> tuple[int, int, int]:
    """Calcula `(peak_bytes, avg_bytes, sample_count)` para una métrica gauge como `container_memory_rss`."""
    if not series_result:
        return 0, 0, 0

    all_vals: list[float] = []
    for serie in series_result:
        for _, raw_val in serie.get("values", []):
            all_vals.append(float(raw_val))

    if not all_vals:
        return 0, 0, 0

    peak = int(round(max(all_vals)))
    avg = int(round(sum(all_vals) / len(all_vals)))
    return peak, avg, len(all_vals)


def collect_container_telemetry(
    container_id: str,
    start_epoch: float,
    end_epoch: float,
    prometheus_url: str | None = None,
) -> ContainerTelemetrySummary:
    """Consulta a Prometheus las series de CPU throttling y RSS del contenedor para la ventana medida."""
    base_url = prometheus_url or resolve_prometheus_url()
    short_id = container_id[:12]

    # Soportamos tanto `/docker/<full_id>` (modo raw_cgroup en Docker Engine 29) como
    # coincidencia por prefijo o etiqueta.
    selector = f'id=~"/docker/{short_id}.*"'
    q_cpu_throttled = f"container_cpu_cfs_throttled_seconds_total{{{selector}}}"
    q_mem_rss = f"container_memory_rss{{{selector}}}"
    q_mem_working_set = f"container_memory_working_set_bytes{{{selector}}}"

    try:
        cpu_series = query_prometheus_range(base_url, q_cpu_throttled, start_epoch, end_epoch)
        rss_series = query_prometheus_range(base_url, q_mem_rss, start_epoch, end_epoch)
        ws_series = query_prometheus_range(base_url, q_mem_working_set, start_epoch, end_epoch)
        reachable = True
    except Exception as exc:  # noqa: BLE001
        return ContainerTelemetrySummary(
            prometheus_reachable=False,
            container_id=container_id,
            cpu_throttled_seconds=0.0,
            memory_rss_peak_bytes=0,
            memory_rss_avg_bytes=0,
            raw_series={"error": f"{type(exc).__name__}: {exc}"},
        )

    throttled_s, cpu_n = compute_counter_delta(cpu_series)
    rss_peak, rss_avg, mem_n = compute_gauge_peak_and_avg(rss_series)
    if rss_peak == 0 and ws_series:
        rss_peak, rss_avg, mem_n = compute_gauge_peak_and_avg(ws_series)

    return ContainerTelemetrySummary(
        prometheus_reachable=reachable,
        container_id=container_id,
        cpu_throttled_seconds=throttled_s,
        memory_rss_peak_bytes=rss_peak,
        memory_rss_avg_bytes=rss_avg,
        cpu_sample_count=cpu_n,
        memory_sample_count=mem_n,
        raw_series={
            "prometheus_url": base_url,
            "window": {"start_epoch": start_epoch, "end_epoch": end_epoch, "step": "1s"},
            "queries": {
                "container_cpu_cfs_throttled_seconds_total": q_cpu_throttled,
                "container_memory_rss_bytes": q_mem_rss,
                "container_memory_working_set_bytes": q_mem_working_set,
            },
            "series": {
                "container_cpu_cfs_throttled_seconds_total": cpu_series,
                "container_memory_rss_bytes": rss_series,
                "container_memory_working_set_bytes": ws_series,
            },
        },
    )

