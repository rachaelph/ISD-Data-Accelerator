from __future__ import annotations

import io
import json
import sys
import unittest
from contextlib import redirect_stdout
from unittest.mock import MagicMock, patch

import run_pipeline as runner
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


class RunPipelineTests(unittest.TestCase):
    def test_format_poll_status_message_is_concise(self) -> None:
        message = runner.format_poll_status_message(
            "Job",
            {
                "observedAtUtc": "2026-03-25T21:37:08",
                "status": "RUNNING",
                "startTimeUtc": "2026-03-25T21:37:08.959125",
                "endTimeUtc": None,
            },
        )

        self.assertEqual(message, "Job status is RUNNING.")

    def test_get_poll_status_stream_uses_stdout_for_pretty_output(self) -> None:
        stream = runner.get_poll_status_stream(pretty=True)

        self.assertIs(stream, sys.stdout)

    def test_build_poll_status_entry_with_lifecycle_and_result(self) -> None:
        entry = runner.build_poll_status_entry(
            lifecycle_state=RunLifeCycleState.TERMINATED,
            result_state=RunResultState.SUCCESS,
            start_time_ms=1711400228959,
            end_time_ms=1711400290000,
        )

        self.assertEqual(entry["status"], "TERMINATED/SUCCESS")
        self.assertIsNotNone(entry["startTimeUtc"])
        self.assertIsNotNone(entry["endTimeUtc"])

    def test_build_poll_status_entry_lifecycle_only(self) -> None:
        entry = runner.build_poll_status_entry(
            lifecycle_state=RunLifeCycleState.RUNNING,
            result_state=None,
            start_time_ms=1711400228959,
            end_time_ms=None,
        )

        self.assertEqual(entry["status"], "RUNNING")
        self.assertIsNone(entry["endTimeUtc"])

    def test_poll_job_returns_completed_on_success(self) -> None:
        observed_entries: list[dict[str, object]] = []
        mock_run = MagicMock()
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED
        mock_run.state.result_state = RunResultState.SUCCESS
        mock_run.start_time = 1711400228959
        mock_run.end_time = 1711400290000

        mock_client = MagicMock()
        mock_client.jobs.get_run.return_value = mock_run

        with patch.object(runner, "get_workspace_client", return_value=mock_client):
            status, run_obj = runner.poll_job(
                run_id=12345,
                max_wait_minutes=1,
                poll_interval_seconds=1,
                status_callback=observed_entries.append,
            )

        self.assertEqual(status, "Completed")
        self.assertEqual(len(observed_entries), 1)
        self.assertIn("TERMINATED/SUCCESS", observed_entries[0]["status"])

    def test_poll_job_returns_failed_on_failure(self) -> None:
        mock_run = MagicMock()
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED
        mock_run.state.result_state = RunResultState.FAILED
        mock_run.state.state_message = "Task failed"
        mock_run.start_time = 1711400228959
        mock_run.end_time = 1711400290000

        mock_client = MagicMock()
        mock_client.jobs.get_run.return_value = mock_run

        with patch.object(runner, "get_workspace_client", return_value=mock_client):
            status, _ = runner.poll_job(run_id=12345, max_wait_minutes=1, poll_interval_seconds=1)

        self.assertEqual(status, "Failed")

    def test_poll_job_returns_cancelled(self) -> None:
        mock_run = MagicMock()
        mock_run.state.life_cycle_state = RunLifeCycleState.TERMINATED
        mock_run.state.result_state = RunResultState.CANCELED
        mock_run.start_time = 1711400228959
        mock_run.end_time = 1711400290000

        mock_client = MagicMock()
        mock_client.jobs.get_run.return_value = mock_run

        with patch.object(runner, "get_workspace_client", return_value=mock_client):
            status, _ = runner.poll_job(run_id=12345, max_wait_minutes=1, poll_interval_seconds=1)

        self.assertEqual(status, "Cancelled")

    def test_poll_job_times_out(self) -> None:
        mock_run = MagicMock()
        mock_run.state.life_cycle_state = RunLifeCycleState.RUNNING
        mock_run.state.result_state = None
        mock_run.start_time = 1711400228959
        mock_run.end_time = None

        mock_client = MagicMock()
        mock_client.jobs.get_run.return_value = mock_run

        with patch.object(runner, "get_workspace_client", return_value=mock_client), \
             patch.object(runner.time, "time", side_effect=[0, 0, 61]), \
             patch.object(runner.time, "sleep"):
            status, _ = runner.poll_job(run_id=12345, max_wait_minutes=1, poll_interval_seconds=30)

        self.assertEqual(status, "TimedOut")

    def test_main_emits_json_for_unexpected_exception(self) -> None:
        execution_context = {
            "BranchName": "main",
            "GitFolderName": "dev",
            "GitFolderPath": "C:\\repo\\dev",
            "HasOverrides": False,
            "OverrideStatusMessage": "Using base workspace values (no feature overrides active)",
            "Variables": {
                "sql_warehouse_id": "wh-123",
                "metadata_database": "accelerator.metadata",
                "job_id_batch_processing": "456",
                "orchestration_job_name": "batch_orchestration",
            },
        }
        stdout = io.StringIO()

        with patch.object(sys, "argv", ["run_pipeline.py", "--trigger-name", "Testing"]), \
             patch.object(runner, "resolve_run_context", return_value=(execution_context, None, "Testing")), \
             patch.object(runner, "get_workspace_client", side_effect=RuntimeError("boom")), \
             redirect_stdout(stdout):
            exit_code = runner.main()

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(payload["executionStatus"], "FailedToStart")
        self.assertEqual(payload["errorType"], "RuntimeError")
        self.assertIn("Unexpected error: boom", payload["errorMessage"])
        self.assertIn("RuntimeError: boom", payload["stackTrace"])


if __name__ == "__main__":
    unittest.main()