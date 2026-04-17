from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "resolve_execution_context.py"


class ResolveExecutionContextTests(unittest.TestCase):
    def write_workspace_config(self, repo_root: Path) -> None:
        dev_path = repo_root / "dev"
        dev_path.mkdir(parents=True, exist_ok=True)
        (dev_path / "workspace_config.json").write_text(
            json.dumps(
                {
                    "sql_warehouse_id": "wh-abc123",
                    "metadata_database": "my_catalog.metadata",
                }
            ),
            encoding="utf-8",
        )

    def test_resolves_context_for_git_folder(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            repo_root = Path(temp_dir)
            self.write_workspace_config(repo_root)

            result = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT_PATH),
                    "--source-directory",
                    str(repo_root),
                    "--git-folder-name",
                    "dev",
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
            self.assertEqual(payload["GitFolderName"], "dev")
            self.assertEqual(payload["Variables"]["sql_warehouse_id"], "wh-abc123")
            self.assertEqual(payload["Variables"]["metadata_database"], "my_catalog.metadata")
            self.assertFalse(payload["HasOverrides"])
            self.assertIn("base workspace config", payload["OverrideStatusMessage"])


if __name__ == "__main__":
    unittest.main()
