"""Pruebas unitarias del generador determinista del dataset `intentos`."""

from __future__ import annotations

import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dataset.generator.generate import (  # noqa: E402
    GENERATOR_VERSION,
    GeneratorConfig,
    generate_dataset,
    iter_intentos,
    serialize_canonical,
    verify_manifest,
)
from dataset.generator.validator import ValidationError, validate_intento  # noqa: E402


class DatasetGeneratorTests(unittest.TestCase):
    def test_determinism_same_seed_produces_identical_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            cfg1 = GeneratorConfig(seed=20261030, count=250, avg_respuestas=3, batch_size=64)
            cfg2 = GeneratorConfig(seed=20261030, count=250, avg_respuestas=3, batch_size=100)

            out1 = tmp_dir / "run1.jsonl"
            man1 = tmp_dir / "run1.manifest.json"
            out2 = tmp_dir / "run2.jsonl"
            man2 = tmp_dir / "run2.manifest.json"

            m1 = generate_dataset(cfg1, out1, man1, force=True)
            m2 = generate_dataset(cfg2, out2, man2, force=True)

            self.assertEqual(m1["sha256"], m2["sha256"])
            self.assertEqual(out1.read_bytes(), out2.read_bytes())
            self.assertEqual(m1["record_count"], 250)
            self.assertEqual(m1["generator_version"], GENERATOR_VERSION)
            self.assertTrue(verify_manifest(out1, man1))
            self.assertTrue(verify_manifest(out2, man2))

    def test_different_seed_produces_different_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            m1 = generate_dataset(
                GeneratorConfig(seed=100, count=100),
                tmp_dir / "a.jsonl",
                tmp_dir / "a.manifest.json",
                force=True,
            )
            m2 = generate_dataset(
                GeneratorConfig(seed=101, count=100),
                tmp_dir / "b.jsonl",
                tmp_dir / "b.manifest.json",
                force=True,
            )
            self.assertNotEqual(m1["sha256"], m2["sha256"])

    def test_canonical_jsonl_and_avg_respuestas(self) -> None:
        cfg = GeneratorConfig(seed=20261030, count=1000, avg_respuestas=3, batch_size=200)
        docs = list(iter_intentos(cfg))
        self.assertEqual(len(docs), 1000)

        total_resp = 0
        for doc in docs:
            validate_intento(doc)
            line = serialize_canonical(doc)
            self.assertNotIn("\n", line)
            reparsed = json.loads(line)
            self.assertEqual(list(reparsed.keys()), sorted(reparsed.keys()))
            total_resp += len(doc["respuestas"])

        avg_resp = total_resp / len(docs)
        self.assertGreater(avg_resp, 2.7)
        self.assertLess(avg_resp, 3.3)

    def test_validator_rejects_invalid_documents(self) -> None:
        valid_doc = next(iter_intentos(GeneratorConfig(seed=1, count=1)))
        validate_intento(valid_doc)

        bad_uuid = copy.deepcopy(valid_doc)
        bad_uuid["intento_id"] = "not-a-uuid"
        with self.assertRaises(ValidationError):
            validate_intento(bad_uuid)

        bad_date = copy.deepcopy(valid_doc)
        bad_date["fecha_inicio"] = "2026-10-30 10:00:00"
        with self.assertRaises(ValidationError):
            validate_intento(bad_date)

        bad_score = copy.deepcopy(valid_doc)
        bad_score["respuestas"][0]["evaluacion"]["puntaje_obtenido"] = 999
        with self.assertRaises(ValidationError):
            validate_intento(bad_score)


if __name__ == "__main__":
    unittest.main()

