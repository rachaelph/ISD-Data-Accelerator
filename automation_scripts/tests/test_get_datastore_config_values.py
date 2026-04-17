from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "get_datastore_config_values.py"


def _write_engine_fixture(repo_root: Path, *, environment: str = "DEV", extra_variables: dict | None = None) -> Path:
    engine = repo_root / "databricks_batch_engine"
    (engine / "metadata").mkdir(parents=True, exist_ok=True)
    (engine / "custom_functions").mkdir(parents=True, exist_ok=True)
    (engine / "datastores").mkdir(parents=True, exist_ok=True)

    payload = {
        "environment": environment,
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
    if extra_variables is not None:
        payload["variables"] = extra_variables

    (engine / "datastores" / f"datastore_{environment}.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    return engine


class GetDatastoreConfigValuesTests(unittest.TestCase):
    def run_cli(self, repo_root: Path, *required_variables: str, environment: str = "DEV") -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(SCRIPT_PATH),
            "--source-directory",
            str(repo_root),
            "--environment",
            environment,
        ]
        for variable_name in required_variables:
            command.extend(["--required-variable", variable_name])
        return subprocess.run(command, capture_output=True, text=True, check=False)

    def test_reads_core_datastore_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            _write_engine_fixture(repo_root)

            result = self.run_cli(repo_root, "sql_warehouse_id", "metadata_database")

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["variables"]["sql_warehouse_id"], "wh-abc123")
            self.assertEqual(payload["variables"]["metadata_database"], "my_catalog.metadata")
            self.assertIn("datastore_DEV.json", payload["configPath"])

    def test_reads_extra_variables_block(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            _write_engine_fixture(repo_root, extra_variables={"job_id_batch_processing": "12345"})

            result = self.run_cli(repo_root, "job_id_batch_processing")

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["variables"]["job_id_batch_processing"], "12345")

    def test_errors_when_required_variable_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            _write_engine_fixture(repo_root)

            result = self.run_cli(repo_root, "job_id_batch_processing")

            self.assertEqual(result.returncode, 1)
            self.assertIn("job_id_batch_processing", result.stderr)

    def test_errors_when_datastore_config_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            _write_engine_fixture(repo_root, environment="DEV")

            result = self.run_cli(repo_root, "sql_warehouse_id", environment="PROD")

            self.assertEqual(result.returncode, 1)
            self.assertIn("datastore", result.stderr.lower())


if __name__ == "__main__":
    unittest.main()

