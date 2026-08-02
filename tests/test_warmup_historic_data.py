from __future__ import annotations

import unittest
from pathlib import Path

from process.warmup_historic_data import WarmupHistoricCatalog


FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


class WarmupHistoricCatalogTests(unittest.TestCase):
    def test_manifest_maps_dom_and_l2_files_by_symbol(self) -> None:
        root = FIXTURE_DIR / "warmup_manifest"

        catalog = WarmupHistoricCatalog(
            root_dir=root,
            default_market_provider="CME_LOCAL_DBN",
            default_dataset="GLBX.MDP3",
        )

        nq_files = catalog.files_for_symbol(provider_symbol="NQ.FUT")
        es_files = catalog.files_for_symbol(mt5_symbol="ES")

        self.assertEqual(1, len(nq_files))
        self.assertEqual("dom", nq_files[0].data_type)
        self.assertEqual("NQ", nq_files[0].mt5_symbol)
        self.assertEqual(1, len(es_files))
        self.assertEqual("l2", es_files[0].data_type)
        self.assertEqual("ES.FUT", es_files[0].provider_symbol)

    def test_scan_matches_provider_symbol_and_mt5_root(self) -> None:
        root = FIXTURE_DIR / "warmup_scan"

        catalog = WarmupHistoricCatalog(root_dir=root)

        nq_files = catalog.files_for_symbol(provider_symbol="NQ.FUT")
        es_files = catalog.files_for_symbol(mt5_symbol="ES")
        status = catalog.status_payload()

        self.assertEqual(1, len(nq_files))
        self.assertEqual("NQ", nq_files[0].provider_symbol)
        self.assertEqual("dom", nq_files[0].data_type)
        self.assertEqual(1, len(es_files))
        self.assertEqual("l2", es_files[0].data_type)
        self.assertEqual(2, status["file_count"])
        self.assertIn("NQ", status["symbols"])
        self.assertIn("ES.FUT", status["symbols"])


if __name__ == "__main__":
    unittest.main()
