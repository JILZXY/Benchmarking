"""Pruebas de integración sobre los motores reales (MongoDB y PostgreSQL) y el dataset de 500,000 documentos.

Verifica:
1. Que el dataset oficial de 500,000 documentos exista y coincida con su manifiesto SHA-256.
2. Que ambos motores contengan exactamente los 500,000 registros y los índices equivalentes.
3. Que las consultas Q1 y Q2 ejecutadas contra los 500,000 documentos en MongoDB y PostgreSQL
   devuelvan exactamente los mismos resultados (equivalencia empírica en vivo).
"""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "runner" / "python" / "src"
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from adapters.equivalence import compare_query_results  # noqa: E402
from dataset.generator.generate import default_paths, verify_manifest  # noqa: E402


def _compose(profile_short: str, args: list[str]) -> str:
    env_file = ROOT / ".env"
    prof_file = ROOT / "config" / "profiles" / {
        "a": "profile_a_baseline.env",
        "b": "profile_b_medium.env",
        "c": "profile_c_severe.env",
    }[profile_short]
    cmd = [
        "docker",
        "compose",
        "--project-directory",
        str(ROOT),
        "--env-file",
        str(env_file),
        "--env-file",
        str(prof_file),
        *args,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(f"Error en compose ({args}): {proc.stderr or proc.stdout}")
    return proc.stdout.strip()


class LiveEnginesIntegrationTests(unittest.TestCase):
    def test_01_dataset_500k_exists_and_matches_manifest_sha256(self) -> None:
        ds_path, man_path = default_paths(500_000, 20261030)
        self.assertTrue(ds_path.exists(), f"No existe {ds_path}")
        self.assertTrue(man_path.exists(), f"No existe {man_path}")
        manifest = json.loads(man_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["record_count"], 500_000)
        self.assertTrue(verify_manifest(ds_path, man_path))

    def test_02_both_engines_have_500k_records_and_produce_identical_q1_q2_results(self) -> None:
        # 1) Ejecutar Q1 y Q2 + verify_load en MongoDB (secuencialmente)
        _compose("a", ["--profile", "postgresql", "stop", "postgresql"])
        _compose("a", ["--profile", "mongodb", "up", "-d", "--wait", "--wait-timeout", "300"])

        mongo_verify_out = _compose(
            "a",
            [
                "--profile",
                "runner",
                "run",
                "--rm",
                "runner",
                "python",
                "/app/runner/src/load/verify_load.py",
                "--engine",
                "mongodb",
                "--sample-size",
                "50",
            ],
        )
        mongo_verify = json.loads(mongo_verify_out)
        self.assertTrue(mongo_verify["ok"], f"Fallo en verify_load mongodb: {mongo_verify}")
        self.assertEqual(mongo_verify["mongodb"]["count"], 500_000)

        mongo_queries_out = _compose(
            "a",
            [
                "--profile",
                "runner",
                "run",
                "--rm",
                "runner",
                "python",
                "-c",
                (
                    "import json; "
                    "from adapters.workload_registry import get_workload_definition; "
                    "from adapters.mongodb.adapter import MongoWorkloadAdapter; "
                    "ad = MongoWorkloadAdapter.from_env(); "
                    "q1 = ad.execute_workload(get_workload_definition('Q1','mongodb','default')); "
                    "q2 = ad.execute_workload(get_workload_definition('Q2','mongodb','allowDiskUse_false')); "
                    "print(json.dumps({'q1': q1, 'q2': q2}))"
                ),
            ],
        )
        mongo_res = json.loads(mongo_queries_out.splitlines()[-1])
        _compose("a", ["--profile", "mongodb", "stop", "mongodb"])

        # 2) Ejecutar Q1 y Q2 + verify_load en PostgreSQL (secuencialmente)
        _compose("a", ["--profile", "postgresql", "up", "-d", "--wait", "--wait-timeout", "300"])
        pg_verify_out = _compose(
            "a",
            [
                "--profile",
                "runner",
                "run",
                "--rm",
                "runner",
                "python",
                "/app/runner/src/load/verify_load.py",
                "--engine",
                "postgresql",
                "--sample-size",
                "50",
            ],
        )
        pg_verify = json.loads(pg_verify_out)
        self.assertTrue(pg_verify["ok"], f"Fallo en verify_load postgresql: {pg_verify}")
        self.assertEqual(pg_verify["postgresql"]["count"], 500_000)

        pg_queries_out = _compose(
            "a",
            [
                "--profile",
                "runner",
                "run",
                "--rm",
                "runner",
                "python",
                "-c",
                (
                    "import json; "
                    "from adapters.workload_registry import get_workload_definition; "
                    "from adapters.postgresql.adapter import PostgresWorkloadAdapter; "
                    "ad = PostgresWorkloadAdapter.from_env(); "
                    "q1 = ad.execute_workload(get_workload_definition('Q1','postgresql','default')); "
                    "q2 = ad.execute_workload(get_workload_definition('Q2','postgresql','work_mem_4MB')); "
                    "print(json.dumps({'q1': q1, 'q2': q2}))"
                ),
            ],
        )
        pg_res = json.loads(pg_queries_out.splitlines()[-1])
        _compose("a", ["--profile", "postgresql", "stop", "postgresql"])

        # 3) Comparar equivalencia exacta de resultados entre MongoDB y PostgreSQL sobre los 500k docs
        q1_diffs = compare_query_results(mongo_res["q1"], pg_res["q1"], float_tol=1e-3)
        self.assertEqual(q1_diffs, [], f"Diferencias en Q1 entre motores: {q1_diffs}")
        self.assertEqual(len(mongo_res["q2"]), 1000)
        q2_diffs = compare_query_results(mongo_res["q2"], pg_res["q2"], float_tol=1e-3)
        self.assertEqual(q2_diffs, [], f"Diferencias en Q2 entre motores: {q2_diffs}")


if __name__ == "__main__":
    unittest.main()

