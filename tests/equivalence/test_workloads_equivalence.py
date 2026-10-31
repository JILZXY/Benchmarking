"""Pruebas de equivalencia lógica para Q1 y Q2 y verificación de las definiciones de consultas."""

from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "runner" / "python" / "src"
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from adapters.equivalence import (  # noqa: E402
    compare_query_results,
    evaluate_q1_reference,
    evaluate_q2_reference,
)
from adapters.mongodb.adapter import extract_mongo_spill_evidence  # noqa: E402
from adapters.postgresql.adapter import extract_postgres_spill_evidence  # noqa: E402
from adapters.workload_registry import (  # noqa: E402
    get_workload_definition,
    list_workload_definitions,
)


class WorkloadEquivalenceTests(unittest.TestCase):
    def setUp(self) -> None:
        fixture = ROOT / "dataset" / "fixtures" / "intentos_sample_100.jsonl"
        self.docs = [
            json.loads(line)
            for line in fixture.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def test_all_workloads_and_variants_registered_with_versions_and_hashes(self) -> None:
        all_defs = list_workload_definitions()
        # Q1: 1 mongo + 1 pg = 2; Q2: 2 mongo + 2 pg = 4 -> total 6
        self.assertEqual(len(all_defs), 6)

        q2_mongo_false = get_workload_definition("Q2", "mongodb", "allowDiskUse_false")
        q2_mongo_true = get_workload_definition("Q2", "mongodb", "allowDiskUse_true")
        self.assertIs(q2_mongo_false.options["allowDiskUse"], False)
        self.assertIs(q2_mongo_true.options["allowDiskUse"], True)
        self.assertEqual(
            q2_mongo_false.parsed_payload["pipeline"],
            q2_mongo_true.parsed_payload["pipeline"],
        )

        q2_pg_4m = get_workload_definition("Q2", "postgresql", "work_mem_4MB")
        q2_pg_64m = get_workload_definition("Q2", "postgresql", "work_mem_64MB")
        self.assertEqual(q2_pg_4m.session_settings["work_mem"], "4MB")
        self.assertEqual(q2_pg_64m.session_settings["work_mem"], "64MB")

    def test_q1_and_q2_reference_evaluation_on_sample_fixture(self) -> None:
        q1_rows = evaluate_q1_reference(self.docs)
        self.assertGreater(len(q1_rows), 0)
        # El manifiesto de intentos_sample_100.jsonl registra q1_qualifying_respuestas = 203
        self.assertEqual(sum(r["total_respuestas"] for r in q1_rows), 203)
        self.assertEqual(compare_query_results(q1_rows, q1_rows), [])

        q2_rows = evaluate_q2_reference(self.docs, limit=1000)
        # El manifiesto de intentos_sample_100.jsonl registra q2_completed_intentos = 63
        self.assertEqual(len(q2_rows), 63)
        # Verificar orden descendente por porcentaje y desempate ascendente por intento_id
        for a, b in zip(q2_rows, q2_rows[1:]):
            self.assertGreaterEqual(a["porcentaje"], b["porcentaje"])
            if a["porcentaje"] == b["porcentaje"]:
                self.assertLess(a["intento_id"], b["intento_id"])

    def test_spill_detectors_distinguish_memory_vs_disk_sort(self) -> None:
        # MongoDB sin spill
        mongo_plan_mem = {"stages": [{"$sort": {"usedDisk": False, "spilledDataStorageSize": 0}}]}
        ev_m_ok = extract_mongo_spill_evidence(explain_doc=mongo_plan_mem)
        self.assertFalse(ev_m_ok.detected)

        # MongoDB con spill en etapa $sort
        mongo_plan_spill = {
            "stages": [
                {
                    "stage": "SORT",
                    "usedDisk": True,
                    "spilledRecords": 350000,
                    "spilledDataStorageSize": 48234496,
                }
            ]
        }
        ev_m_spill = extract_mongo_spill_evidence(explain_doc=mongo_plan_spill)
        self.assertTrue(ev_m_spill.detected)
        self.assertEqual(ev_m_spill.bytes_spilled, 48234496)

        # PostgreSQL sin spill (top-N heapsort en Memory)
        pg_plan_mem = [
            {
                "Plan": {
                    "Node Type": "Limit",
                    "Plans": [
                        {
                            "Node Type": "Sort",
                            "Sort Method": "top-N heapsort",
                            "Sort Space Used": 192,
                            "Sort Space Type": "Memory",
                            "Temp Read Blocks": 0,
                            "Temp Written Blocks": 0,
                        }
                    ],
                }
            }
        ]
        ev_pg_ok = extract_postgres_spill_evidence(explain_json=pg_plan_mem)
        self.assertFalse(ev_pg_ok.detected)

        # PostgreSQL con spill (external merge en Disk)
        pg_plan_spill = [
            {
                "Plan": {
                    "Node Type": "Sort",
                    "Sort Method": "external merge",
                    "Sort Space Used": 32768,
                    "Sort Space Type": "Disk",
                    "Temp Read Blocks": 4096,
                    "Temp Written Blocks": 4096,
                }
            }
        ]
        ev_pg_spill = extract_postgres_spill_evidence(explain_json=pg_plan_spill)
        self.assertTrue(ev_pg_spill.detected)
        self.assertEqual(ev_pg_spill.bytes_spilled, 32768 * 1024)


if __name__ == "__main__":
    unittest.main()

