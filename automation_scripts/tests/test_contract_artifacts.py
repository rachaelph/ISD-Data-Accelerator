from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
AGENTS_DIR = SCRIPT_DIR.parent / "agents"
if str(AGENTS_DIR) not in sys.path:
    sys.path.insert(0, str(AGENTS_DIR))

from generate_contract_artifacts import (
    METADATA_SCHEMA_PATH,
    QUERY_CAPABILITIES_PATH,
    WORKFLOW_CAPABILITIES_PATH,
    build_metadata_schema_contract,
    build_query_capability_contract,
    build_workflow_capability_contract,
)
from query_capabilities import QUERY_CAPABILITIES, QUERY_TYPE_CHOICES, validate_query_type_arguments
from workflow_capabilities import COMMAND_CAPABILITIES, PLANNER_RECIPES, STAGE_CAPABILITIES


class ContractArtifactTests(unittest.TestCase):
    def _read_json(self, path: Path) -> dict:
        return json.loads(path.read_text(encoding="utf-8"))

    def test_metadata_schema_contract_matches_generated_output(self) -> None:
        self.assertEqual(self._read_json(METADATA_SCHEMA_PATH), build_metadata_schema_contract())

    def test_workflow_capability_contract_matches_generated_output(self) -> None:
        self.assertEqual(self._read_json(WORKFLOW_CAPABILITIES_PATH), build_workflow_capability_contract())

    def test_query_capability_contract_matches_generated_output(self) -> None:
        self.assertEqual(self._read_json(QUERY_CAPABILITIES_PATH), build_query_capability_contract())

    def test_query_type_choices_cover_registry(self) -> None:
        self.assertEqual(list(QUERY_TYPE_CHOICES), list(QUERY_CAPABILITIES.keys()))

    def test_query_argument_validation_requires_table_id_for_run_status(self) -> None:
        args = SimpleNamespace(query_type="RunStatus", table_id=None)

        self.assertEqual(
            validate_query_type_arguments(args),
            "--table-id is required for query type 'RunStatus'.",
        )

    def test_query_argument_validation_allows_reset_watermark_with_trigger(self) -> None:
        args = SimpleNamespace(query_type="ResetWatermark", table_id=None, table_ids=None, trigger_name="SalesDataProduct")

        self.assertIsNone(validate_query_type_arguments(args))

    def test_workflow_registry_contains_execute_entrypoint(self) -> None:
        command_ids = {entry["id"] for entry in COMMAND_CAPABILITIES}
        self.assertIn("fdp-00-end-to-end", command_ids)
        self.assertIn("author", STAGE_CAPABILITIES)
        self.assertIn("run", STAGE_CAPABILITIES)
        self.assertTrue(any(recipe["id"] == "author_validate_commit_run_investigate" for recipe in PLANNER_RECIPES))

    def test_all_registered_prompt_files_exist(self) -> None:
        for entry in COMMAND_CAPABILITIES:
            prompt_path = REPO_ROOT / entry["prompt_file"]
            self.assertTrue(prompt_path.exists(), f"Missing prompt file for {entry['id']}: {prompt_path}")


if __name__ == "__main__":
    unittest.main()