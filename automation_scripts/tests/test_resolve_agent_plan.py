from __future__ import annotations

import json
import subprocess
import sys
import unittest
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "agents" / "resolve_agent_plan.py"


class ResolveAgentPlanTests(unittest.TestCase):
    def _resolve(self, intent: str) -> dict:
        result = subprocess.run(
            [sys.executable, str(SCRIPT_PATH), "--intent", intent],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_author_verify_request_resolves_full_chain(self) -> None:
        payload = self._resolve("Add a freshness rule to customers and show me it worked end to end")

        self.assertEqual(payload["matchedRecipeId"], "author_validate_commit_run_investigate")
        self.assertEqual(
            [stage["stage_id"] for stage in payload["stages"]],
            ["author", "validate_metadata", "commit", "run", "investigate"],
        )

    def test_convert_verify_request_resolves_convert_chain(self) -> None:
        payload = self._resolve("Convert this legacy SQL and prove it works")

        self.assertEqual(payload["matchedRecipeId"], "convert_validate_commit_run_investigate")
        self.assertEqual(payload["stages"][0]["stage_id"], "convert")

    def test_rerun_request_resolves_run_then_investigate(self) -> None:
        payload = self._resolve("Rerun table 1201 and inspect the outcome")

        self.assertEqual(payload["matchedRecipeId"], "run_investigate")
        self.assertEqual(
            [stage["stage_id"] for stage in payload["stages"]],
            ["run", "investigate"],
        )

    def test_failure_request_routes_to_troubleshoot(self) -> None:
        payload = self._resolve("Why did my pipeline fail?")

        self.assertEqual(payload["matchedRecipeId"], "troubleshoot_only")
        self.assertEqual([stage["stage_id"] for stage in payload["stages"]], ["troubleshoot"])


if __name__ == "__main__":
    unittest.main()