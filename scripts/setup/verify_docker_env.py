#!/usr/bin/env python3
"""Verifica que Docker, Compose y cgroups v2 soportan los límites del benchmark.

Uso (desde la raíz del proyecto, en el anfitrión):

    python scripts/setup/verify_docker_env.py            # verificación estática
    python scripts/setup/verify_docker_env.py --live     # + límites efectivos de
                                                         #   los motores en ejecución

Solo usa la biblioteca estándar y el CLI de Docker.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
PROFILES_DIR = ROOT / "config" / "profiles"
MIN_COMPOSE = (2, 20, 0)   # depends_on/healthcheck start_interval, multi --env-file
MIN_ENGINE = (25, 0, 0)    # healthcheck start_interval
ENGINES = ("mongodb", "postgresql")

_results: list[tuple[str, str, str]] = []


def report(status: str, check: str, detail: str) -> None:
    _results.append((status, check, detail))
    print(f"[{status:<4}] {check}: {detail}")


def run(cmd: list[str]) -> str:
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} -> {proc.stderr.strip() or proc.stdout.strip()}")
    return proc.stdout.strip()


def parse_version(text: str) -> tuple[int, ...]:
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", text)
    return tuple(int(x) for x in match.groups()) if match else (0, 0, 0)


def parse_size(value: str) -> int:
    """Convierte '4096m', '1g', '512MB' a bytes."""
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*", value.lower())
    if not match:
        raise ValueError(f"Tamaño no válido: {value!r}")
    number, unit = match.groups()
    factor = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}[unit]
    return int(float(number) * factor)


def read_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            env[key.strip()] = value.strip()
    return env


def check_static() -> dict[str, dict[str, str]]:
    info = json.loads(run(["docker", "info", "--format", "{{json .}}"]))

    engine_version = info.get("ServerVersion", "")
    ok = parse_version(engine_version) >= MIN_ENGINE
    report("OK" if ok else "FAIL", "Docker Engine", f"{engine_version} (mínimo {'.'.join(map(str, MIN_ENGINE))})")

    compose_version = run(["docker", "compose", "version", "--short"])
    ok = parse_version(compose_version) >= MIN_COMPOSE
    report("OK" if ok else "FAIL", "Docker Compose", f"{compose_version} (mínimo {'.'.join(map(str, MIN_COMPOSE))})")

    cgroup = str(info.get("CgroupVersion", ""))
    report("OK" if cgroup == "2" else "FAIL", "cgroups", f"v{cgroup} (driver {info.get('CgroupDriver')})")

    cfs = info.get("CpuCfsQuota", info.get("CPUCfsQuota"))
    report("OK" if cfs else "FAIL", "Cuota CFS (cpus)", str(cfs))
    report("OK" if info.get("MemoryLimit", True) else "FAIL", "Límite de memoria", str(info.get("MemoryLimit", True)))
    report("OK" if info.get("SwapLimit") else "FAIL", "Límite de swap (memswap_limit)", str(info.get("SwapLimit")))
    report(
        "INFO",
        "mem_swappiness",
        "no soportado en cgroups v2: el swap se desactiva con memswap_limit == mem_limit",
    )

    os_name = info.get("OperatingSystem", "")
    kernel = info.get("KernelVersion", "")
    if "Docker Desktop" in os_name or "microsoft" in kernel.lower():
        report(
            "WARN",
            "Anfitrión",
            f"{os_name} / {kernel}: Node Exporter mide la VM de WSL2, no el sistema Windows",
        )
    else:
        report("OK", "Anfitrión", f"{os_name} / {kernel}")

    ncpu = int(info.get("NCPU", 0))
    mem_total = int(info.get("MemTotal", 0))
    profiles = {p.stem: read_env(p) for p in sorted(PROFILES_DIR.glob("profile_*.env"))}

    example = read_env(ROOT / ".env.example")
    env = {**example, **(read_env(ROOT / ".env") if (ROOT / ".env").exists() else {})}
    overhead = sum(
        parse_size(env.get(key, default))
        for key, default in (
            ("PROMETHEUS_MEM_LIMIT", "1024m"),
            ("CADVISOR_MEM_LIMIT", "512m"),
            ("NODE_EXPORTER_MEM_LIMIT", "128m"),
            ("RUNNER_MEM_LIMIT", "1536m"),
        )
    )

    for name, prof in profiles.items():
        cpus = float(prof["ENGINE_CPUS"])
        mem = parse_size(prof["ENGINE_MEM_LIMIT"])
        needed = mem + overhead
        cpu_ok = cpus <= ncpu
        mem_ok = needed <= mem_total
        status = "OK" if cpu_ok and mem_ok else "FAIL"
        if status == "OK" and needed > 0.85 * mem_total:
            status = "WARN"
        report(
            status,
            f"Capacidad {prof.get('BENCH_PROFILE', name)}",
            f"motor {cpus:g} CPU/{mem / 2**20:.0f} MiB + monitorización/runner "
            f"{overhead / 2**20:.0f} MiB = {needed / 2**20:.0f} MiB de {mem_total / 2**20:.0f} MiB "
            f"disponibles; {ncpu} CPU",
        )

    for name in profiles:
        try:
            run(
                [
                    "docker", "compose", "--project-directory", str(ROOT),
                    "--env-file", str(ROOT / ".env.example"),
                    "--env-file", str(PROFILES_DIR / f"{name}.env"),
                    "--profile", "*", "config", "--quiet",
                ]
            )
            report("OK", f"compose config ({name})", "válido")
        except RuntimeError as exc:
            report("FAIL", f"compose config ({name})", str(exc))

    return profiles


def check_live(profiles: dict[str, dict[str, str]]) -> None:
    by_profile = {p["BENCH_PROFILE"]: p for p in profiles.values()}
    for engine in ENGINES:
        ids = run(["docker", "ps", "-q", "--filter", f"label=com.eticore.engine={engine}"]).split()
        if not ids:
            report("INFO", f"Límites efectivos {engine}", "contenedor no está en ejecución")
            continue
        cid = ids[0]
        inspect = json.loads(run(["docker", "inspect", cid]))[0]
        host = inspect["HostConfig"]
        env = dict(item.split("=", 1) for item in inspect["Config"]["Env"] if "=" in item)
        profile_name = env.get("BENCH_PROFILE", "unset")
        expected = by_profile.get(profile_name)

        cpu_max = run(["docker", "exec", cid, "cat", "/sys/fs/cgroup/cpu.max"])
        mem_max = run(["docker", "exec", cid, "cat", "/sys/fs/cgroup/memory.max"])
        swap_max = run(["docker", "exec", cid, "cat", "/sys/fs/cgroup/memory.swap.max"])

        quota, period = cpu_max.split()
        eff_cpus = int(quota) / int(period) if quota != "max" else float("inf")
        detail = (
            f"perfil={profile_name} cpu.max='{cpu_max}' (={eff_cpus:g} CPU) "
            f"memory.max={mem_max} memory.swap.max={swap_max} "
            f"NanoCpus={host.get('NanoCpus')} Memory={host.get('Memory')} MemorySwap={host.get('MemorySwap')}"
        )
        problems = []
        if expected:
            if abs(eff_cpus - float(expected["ENGINE_CPUS"])) > 1e-6:
                problems.append("CPU distinta al perfil")
            if mem_max == "max" or int(mem_max) != parse_size(expected["ENGINE_MEM_LIMIT"]):
                problems.append("memoria distinta al perfil")
        else:
            problems.append("BENCH_PROFILE desconocido")
        if swap_max != "0":
            problems.append("swap habilitado")
        report("FAIL" if problems else "OK", f"Límites efectivos {engine}", detail + (f" -> {', '.join(problems)}" if problems else ""))

    running = run(["docker", "ps", "--format", "{{.Label \"com.eticore.engine\"}}"]).split()
    active = sorted({e for e in running if e})
    if len(active) > 1:
        report("WARN", "Aislamiento", f"ambos motores en ejecución ({', '.join(active)}); el benchmark debe usar uno a la vez")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--live", action="store_true", help="verificar límites de los motores en ejecución")
    args = parser.parse_args()

    try:
        profiles = check_static()
        if args.live:
            check_live(profiles)
    except (RuntimeError, FileNotFoundError) as exc:
        report("FAIL", "Docker", str(exc))

    failed = [r for r in _results if r[0] == "FAIL"]
    print(f"\n{len(_results)} verificaciones, {len(failed)} fallidas.")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
