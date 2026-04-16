from __future__ import annotations

import importlib.util
import os
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name: str, module_path: Path):
    if module_name in sys.modules:
        return sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module {module_name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


verifier = load_module("verify_native_fixtures", ROOT / "tools" / "verify_native_fixtures.py")


class NativeFixtureContractTests(unittest.TestCase):
    def test_validate_existing_curated_fixtures(self) -> None:
        results = verifier.verify_many(list(verifier.builder.DATABASE_NAMES), rebuild=False, include_smoke=True)
        self.assertEqual(len(results), len(verifier.builder.DATABASE_NAMES))
        for result in results:
            self.assertEqual(result.source_total_rows, result.target_total_rows, result.database)
            self.assertEqual(result.source_table_count, result.target_table_count, result.database)

    def test_rebuild_small_curated_fixtures(self) -> None:
        small_databases = ["car_database", "chinook", "sakila_master"]
        results = verifier.verify_many(small_databases, rebuild=True, include_smoke=True)
        self.assertEqual([result.database for result in results], small_databases)
        for result in results:
            self.assertTrue(result.rebuilt)
            self.assertIsNotNone(result.build_seconds)

    def test_rebuild_northwind_when_enabled(self) -> None:
        if os.environ.get("FIREBIRDPOD_REBUILD_NORTHWIND") != "1":
            self.skipTest("Set FIREBIRDPOD_REBUILD_NORTHWIND=1 to rebuild the large northwind fixture.")
        result = verifier.verify_database("northwind", rebuild=True, include_smoke=True)
        self.assertEqual(result.database, "northwind")
        self.assertTrue(result.rebuilt)


if __name__ == "__main__":
    unittest.main()
