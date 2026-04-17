from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "get_variable_library_values.py"


class GetVariableLibraryValuesTests(unittest.TestCase):
    def run_cli(self, repo_root: Path, git_folder_name: str, *required_variables: str) -> subprocess.CompletedProcess[str]:
        command = [
            sys.executable,
            str(SCRIPT_PATH),
            "--source-directory",
            str(repo_root),
            "--git-folder-name",
            git_folder_name,
        ]
        for variable_name in required_variables:
            command.extend(["--required-variable", variable_name])
        return subprocess.run(command, capture_output=True, text=True, check=False)

    def write_workspace_config(self, path: Path, variables: dict[str, str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(variables), encoding="utf-8")

    def test_reads_top_level_workspace_config(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            self.write_workspace_config(
                repo_root / "dev" / "workspace_config.json",
                {
                    "sql_warehouse_id": "abc123",
                    "metadata_database": "my_catalog.metadata",
                },
            )

            result = self.run_cli(repo_root, "dev", "sql_warehouse_id", "metadata_database")

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["variables"]["sql_warehouse_id"], "abc123")
            self.assertEqual(payload["variables"]["metadata_database"], "my_catalog.metadata")
            self.assertIn("workspace_config.json", payload["configPath"])

    def test_falls_back_to_recursive_search_within_git_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            self.write_workspace_config(
                repo_root / "dev" / "nested" / "workspace_config.json",
                {"sql_warehouse_id": "abc123"},
            )

            result = self.run_cli(repo_root, "dev", "sql_warehouse_id")

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(result.stdout)
            self.assertEqual(payload["variables"]["sql_warehouse_id"], "abc123")

    def test_errors_when_required_variable_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            self.write_workspace_config(
                repo_root / "dev" / "workspace_config.json",
                {"sql_warehouse_id": "abc123"},
            )

            result = self.run_cli(repo_root, "dev", "metadata_database")

            self.assertEqual(result.returncode, 1)
            self.assertIn("metadata_database", result.stderr)

    def test_errors_when_multiple_workspace_configs_in_global_search(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            self.write_workspace_config(
                repo_root / "devA" / "workspace_config.json",
                {"sql_warehouse_id": "abc123"},
            )
            self.write_workspace_config(
                repo_root / "devB" / "workspace_config.json",
                {"sql_warehouse_id": "def456"},
            )

            result = self.run_cli(repo_root, "missing", "sql_warehouse_id")

            self.assertEqual(result.returncode, 1)
            self.assertIn("Multiple", result.stderr)


if __name__ == "__main__":
    unittest.main()
