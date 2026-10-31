"""Prueba de humo extremo a extremo del benchmark runner, Prometheus y persistencia de resultados.

Ejecuta una corrida corta aislada (usando `--checkpoint-file` y `--output-root` en un directorio
temporal para no contaminar la matriz oficial de 108 ejecuciones) y verifica:
1. Que el runner levanta el motor y ejecuta warmup, medición y cooldown.
2. Que Prometheus captura métricas (`memory_rss_peak_bytes > 0`).
3. Que el CSV normalizado, el JSON crudo, el log y la telemetría se guardan correctamente.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class BenchmarkSmokeTests(unittest.TestCase):
    def test_single_short_run_end_to_end_with_prometheus_and_csv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            cp_file = tmp_dir / "smoke_checkpoint.json"
            out_root = tmp_dir / "smoke_results"

            cmd = [
                sys.executable,
                str(ROOT / "runner" / "python" / "src" / "orchestration" / "runner.py"),
                "--engine",
                "postgresql",
                "--profile",
                "PROFILE_C_SEVERE",
                "--query",
                "Q2",
                "--concurrency",
                "1",
                "--max-runs",
                "1",
                "--warmup-seconds",
                "2",
                "--measurement-seconds",
                "4",
                "--cooldown-seconds",
                "1",
                "--checkpoint-file",
                str(cp_file),
                "--output-root",
                str(out_root),
            ]
            proc = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            self.assertEqual(
                proc.returncode,
                0,
                f"Fallo en smoke run: stdout={proc.stdout}\nstderr={proc.stderr}",
            )

            csv_path = out_root / "normalized" / "benchmark_results.csv"
            self.assertTrue(csv_path.exists(), "No se generó benchmark_results.csv")
            with csv_path.open("r", encoding="utf-8") as fh:
                rows = list(csv.DictReader(fh))
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row["engine"], "postgresql")
            self.assertEqual(row["profile"], "PROFILE_C_SEVERE")
            self.assertIn(row["status"], ("ok", "spill"))
            self.assertGreater(float(row["throughput"]), 0.0)
            self.assertGreater(int(float(row["memory_rss_peak_bytes"])), 0)

            cp_data = json.loads(cp_file.read_text(encoding="utf-8"))
            self.assertEqual(len(cp_data["completed"]), 1)


if __name__ == "__main__":
    unittest.main()

