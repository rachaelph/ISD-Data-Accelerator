from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "resolve_execution_context.py"


def _write_engine_fixture(repo_root: Path) -> None:
    engine = repo_root / "databricks_batch_engine"
    (engine / "metadata").mkdir(parents=True, exist_ok=True)
    (engine / "custom_functions").mkdir(parents=True, exist_ok=True)
    (engine / "datastores").mkdir(parents=True, exist_ok=True)
    (engine / "datastores" / "datastore_DEV.json").write_text(
        json.dumps(
            {
                "environment": "DEV",
                "workspace_id": "1111111111111111",
                "workspace_url": "https://adb-1111111111111111.1.azuredatabricks.net",
                "sql_warehouse_id": "wh-abc123",
                "layers": {
                    "bronze": {"catalog": "dev_bronze"},
                    "silver": {"catalog": "dev_silver"},
                },
                "metadata": {
                    "catalog": "my_catalog",
                    "schema": "metadata",
                },
            }
        ),
        encoding="utf-8",
    )


class ResolveExecutionContextTests(unittest.TestCase):
    def test_resolves_context_from_datastore_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            _write_engine_fixture(repo_root)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--source-directory",
                    str(repo_root),
                    "--environment",
                    "DEV",
                    "--required-variable",
                    "sql_warehouse_id",
                    "--required-variable",
                    "metadata_database",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["Environment"], "DEV")
            self.assertEqual(payload["EngineFolderName"], "databricks_batch_engine")
            self.assertIsNone(payload["GitFolderName"])
            self.assertEqual(payload["Variables"]["sql_warehouse_id"], "wh-abc123")
            self.assertEqual(payload["Variables"]["metadata_database"], "metadata")
            self.assertEqual(payload["Variables"]["metadata_catalog"], "my_catalog")
            self.assertEqual(payload["Variables"]["metadata_schema"], "metadata")
            self.assertFalse(payload["HasOverrides"])
            self.assertIn("base datastore config", payload["OverrideStatusMessage"])


if __name__ == "__main__":
    unittest.main()

