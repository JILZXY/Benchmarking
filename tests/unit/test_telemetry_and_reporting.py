"""Pruebas unitarias de telemetría Prometheus, columnas del CSV normalizado y generador de resúmenes."""

from __future__ import annotations

import csv
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "runner" / "python" / "src"
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from adapters.mongodb.adapter import (  # noqa: E402
    coerce_bson_int,
    extract_mongo_spill_evidence,
)
from metrics.prometheus_client import (  # noqa: E402
    compute_counter_delta,
    compute_gauge_peak_and_avg,
)
from reporting.incremental_writer import (  # noqa: E402
    NORMALIZED_CSV_COLUMNS,
    append_normalized_result,
)
from reporting.summary_generator import build_summaries_from_csv  # noqa: E402


class TelemetryAndReportingTests(unittest.TestCase):
    def test_prometheus_counter_delta_and_gauge_peak(self) -> None:
        cpu_series = [
            {
                "metric": {"id": "/docker/abc123456789"},
                "values": [
                    [1000.0, "1.250"],
                    [1001.0, "1.500"],
                    [1002.0, "2.125"],
                ],
            }
        ]
        delta, samples = compute_counter_delta(cpu_series)
        self.assertAlmostEqual(delta, 0.875)
        self.assertEqual(samples, 3)

        rss_series = [
            {
                "metric": {"id": "/docker/abc123456789"},
                "values": [
                    [1000.0, "104857600"],
                    [1001.0, "209715200"],
                    [1002.0, "157286400"],
                ],
            }
        ]
        peak, avg, m_samples = compute_gauge_peak_and_avg(rss_series)
        self.assertEqual(peak, 209715200)
        self.assertEqual(avg, 157286400)
        self.assertEqual(m_samples, 3)

    def test_normalized_csv_contains_all_required_section10_columns_and_builds_summaries(
        self,
    ) -> None:
        required_cols = {
            "run_id",
            "engine",
            "profile",
            "workload",
            "concurrency",
            "repetition",
            "status",
            "throughput",
            "latency_p50",
            "latency_p95",
            "latency_p99",
            "cpu_throttled_seconds",
            "memory_rss_peak_bytes",
            "disk_spill_detected",
            "oom_killed",
            "dataset_hash",
            "throughput_qps",
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p99_ms",
            "container_cpu_cfs_throttled_seconds_total",
            "container_memory_rss_bytes",
        }
        self.assertTrue(required_cols.issubset(set(NORMALIZED_CSV_COLUMNS)))

        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            csv_path = tmp_dir / "benchmark_results.csv"
            jsonl_path = tmp_dir / "benchmark_runs.jsonl"
            summary_dir = tmp_dir / "summaries"

            append_normalized_result(
                {
                    "run_id": "run-001",
                    "sequence_index": 1,
                    "engine": "mongodb",
                    "profile_id": "PROFILE_C_SEVERE",
                    "query_code": "Q2",
                    "variant_id": "allowDiskUse_false",
                    "concurrency": 10,
                    "repetition": 1,
                    "status": "ok",
                    "throughput_qps": 45.2,
                    "latency_p50_ms": 180.5,
                    "latency_p95_ms": 240.1,
                    "latency_p99_ms": 290.8,
                    "cpu_throttled_seconds": 1.25,
                    "memory_rss_peak_bytes": 536870912,
                    "disk_spill_detected": False,
                    "oom_killed": False,
                    "dataset_sha256": "d99651024aaa564bfcd8ef1a3d23430c1bbe56f53a49964a3170620213bde073",
                },
                csv_path=csv_path,
                jsonl_path=jsonl_path,
            )

            with csv_path.open("r", encoding="utf-8") as fh:
                row = next(csv.DictReader(fh))

            self.assertEqual(row["profile"], "PROFILE_C_SEVERE")
            self.assertEqual(row["workload"], "Q2:allowDiskUse_false")
            self.assertEqual(row["throughput"], "45.2")
            self.assertEqual(row["latency_p95"], "240.1")
            self.assertEqual(row["cpu_throttled_seconds"], "1.25")
            self.assertEqual(row["memory_rss_peak_bytes"], "536870912")
            self.assertEqual(
                row["dataset_hash"],
                "d99651024aaa564bfcd8ef1a3d23430c1bbe56f53a49964a3170620213bde073",
            )

            summary = build_summaries_from_csv(csv_path=csv_path, summary_dir=summary_dir)
            self.assertEqual(summary["total_runs_evaluated"], 1)
            self.assertFalse(summary["spill_observed"])

            spill_rep = json.loads(
                (summary_dir / "spill_and_oom_report.json").read_text(encoding="utf-8")
            )
            self.assertIn("No se observó almacenamiento temporal", spill_rep["empirical_interpretation"])

    def test_mongo_spill_evidence_handles_mongosh_numberlong_and_ejson(self) -> None:
        self.assertEqual(coerce_bson_int({"low": 0, "high": 0, "unsigned": False}), 0)
        self.assertEqual(coerce_bson_int({"low": 150, "high": 0, "unsigned": False}), 150)
        # 64-bit value: (2 << 32) | 1024 = 8589935616
        self.assertEqual(
            coerce_bson_int({"low": 1024, "high": 2, "unsigned": True}),
            (2 << 32) + 1024,
        )
        # Negative low in signed 32-bit representation (e.g. 0xFFFFFFFF == -1 -> 4294967295 unsigned low)
        self.assertEqual(
            coerce_bson_int({"low": -1, "high": 1, "unsigned": False}),
            (1 << 32) + 0xFFFFFFFF,
        )
        self.assertEqual(coerce_bson_int({"$numberLong": "9876543210"}), 9876543210)

        before = {
            "metrics": {
                "queryExecutor": {
                    "spilledRanges": {"low": 0, "high": 0, "unsigned": False},
                    "spilledRecords": {"low": 10, "high": 0, "unsigned": False},
                    "spilledDataStorageSize": {"low": 4096, "high": 0, "unsigned": False},
                },
                "operation": {
                    "writeConflicts": {"low": 5, "high": 0, "unsigned": False},
                },
            }
        }
        after_no_spill = {
            "metrics": {
                "queryExecutor": {
                    "spilledRanges": {"low": 0, "high": 0, "unsigned": False},
                    "spilledRecords": {"low": 10, "high": 0, "unsigned": False},
                    "spilledDataStorageSize": {"low": 4096, "high": 0, "unsigned": False},
                },
                "operation": {
                    "writeConflicts": {"low": 5, "high": 0, "unsigned": False},
                },
            }
        }
        ev_ok = extract_mongo_spill_evidence(
            explain_doc=None,
            server_status_before=before,
            server_status_after=after_no_spill,
            log_spill_lines=[],
        )
        self.assertFalse(ev_ok.detected)
        self.assertIsNone(ev_ok.bytes_spilled)

        after_with_spill = {
            "metrics": {
                "queryExecutor": {
                    "spilledRanges": {"low": 2, "high": 0, "unsigned": False},
                    "spilledRecords": {"low": 510, "high": 0, "unsigned": False},
                    "spilledDataStorageSize": {"low": 1048576, "high": 0, "unsigned": False},
                },
                "operation": {
                    "writeConflicts": {"low": 5, "high": 0, "unsigned": False},
                },
            }
        }
        ev_spill = extract_mongo_spill_evidence(
            explain_doc=None,
            server_status_before=before,
            server_status_after=after_with_spill,
            log_spill_lines=[],
        )
        self.assertTrue(ev_spill.detected)
        self.assertIn("mongodb.serverStatus.queryExecutor", ev_spill.sources)
        self.assertEqual(ev_spill.bytes_spilled, 1048576 - 4096)
        self.assertEqual(ev_spill.details["delta_spilledRecords"], 500)


if __name__ == "__main__":
    unittest.main()

