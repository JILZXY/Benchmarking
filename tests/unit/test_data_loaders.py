"""Pruebas unitarias de las utilidades de carga y verificación de equivalencia."""

from __future__ import annotations

import copy
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
SRC_DIR = ROOT / "runner" / "python" / "src"
for p in (ROOT, SRC_DIR):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from load.common import canonical_doc_sha256, iter_jsonl_batches  # noqa: E402
from load.verify_load import (  # noqa: E402
    compare_state_against_reference,
    sample_dataset_records,
)


class LoadUtilitiesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = ROOT / "dataset" / "fixtures" / "intentos_sample_100.jsonl"

    def test_iter_jsonl_batches_preserves_exact_canonical_payload(self) -> None:
        batches = list(iter_jsonl_batches(self.fixture, batch_size=30))
        self.assertEqual([len(b) for b in batches], [30, 30, 30, 10])

        raw_line, doc = batches[0][0]
        # Si simulamos un documento devuelto por MongoDB con `_id` adicional,
        # canonical_doc_sha256 debe ignorar `_id` y coincidir con el SHA-256 de la línea original.
        mongo_like = copy.deepcopy(doc)
        mongo_like["_id"] = "507f1f77bcf86cd799439011"
        import hashlib

        self.assertEqual(
            canonical_doc_sha256(mongo_like),
            hashlib.sha256(raw_line.encode("utf-8")).hexdigest(),
        )

    def test_compare_state_against_reference_detects_matched_and_altered_docs(self) -> None:
        total, ref_hashes = sample_dataset_records(self.fixture, sample_size=25, seed=42)
        self.assertEqual(total, 100)
        self.assertEqual(len(ref_hashes), 25)

        good_state = {
            "engine": "postgresql",
            "count": 100,
            "has_id_index": True,
            "has_fecha_inicio_index": True,
            "sample_hashes": dict(ref_hashes),
        }
        self.assertEqual(compare_state_against_reference(good_state, 100, ref_hashes), [])

        tampered = dict(ref_hashes)
        first_id = next(iter(tampered))
        tampered[first_id] = "0" * 64
        bad_state = {
            "engine": "mongodb",
            "count": 99,
            "has_id_index": False,
            "has_fecha_inicio_index": True,
            "sample_hashes": tampered,
        }
        errors = compare_state_against_reference(bad_state, 100, ref_hashes)
        self.assertEqual(len(errors), 3)


if __name__ == "__main__":
    unittest.main()

