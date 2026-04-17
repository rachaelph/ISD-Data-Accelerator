#!/usr/bin/env python3
"""Execute a Databricks job run using merged workspace config values.

Supports two execution modes:
- **Job mode** (default): trigger an existing Databricks Job by name with parameters.
- **Notebook mode**: submit a one-time notebook run for a single table.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
import traceback
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from databricks.sdk.service.jobs import (
    RunLifeCycleState,
    RunResultState,
)

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import AgentError, get_workspace_client, resolve_execution_context

JOB_NAME = "batch_orchestration"
TERMINAL_LIFECYCLE_STATES = {
    RunLifeCycleState.TERMINATED,
    RunLifeCycleState.SKIPPED,
    RunLifeCycleState.INTERNAL_ERROR,
}


@dataclass
class TableMetadata:
    table_id: int
    trigger_name: str
    order_of_operations: int
    target_datastore: str
    target_entity: str
    processing_method: str
    ingestion_active: bool
    metadata_file: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute a Databricks job or notebook run using merged workspace config values.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--git-folder-name")
    parser.add_argument("--environment", default="DEV")
    parser.add_argument("--table-id", type=int, default=0)
    parser.add_argument("--trigger-name")
    parser.add_argument("--table-ids")
    parser.add_argument("--start-with-step", type=int, default=0)
    parser.add_argument("--trigger-step", type=int, default=0)
    parser.add_argument("--event-payload", default="")
    parser.add_argument("--folder-path-from-trigger", default="")
    parser.add_argument("--max-wait-minutes", type=int, default=60)
    parser.add_argument("--poll-interval-seconds", type=int, default=30)
    parser.add_argument("--pretty", action="store_true")
    eda_group = parser.add_mutually_exclusive_group()
    eda_group.add_argument("--run-exploratory-analysis", dest="run_exploratory_analysis", action="store_true")
    eda_group.add_argument("--no-run-exploratory-analysis", dest="run_exploratory_analysis", action="store_false")
    parser.set_defaults(run_exploratory_analysis=False)
    return parser.parse_args()


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


# ---------------------------------------------------------------------------
# Job discovery
# ---------------------------------------------------------------------------
def discover_job_id(job_name: str) -> int:
    """Find a Databricks Job by display name and return its ID."""
    client = get_workspace_client()
    for job in client.jobs.list(name=job_name):
        if job.settings and job.settings.name == job_name:
            return job.job_id
    raise AgentError(
        f"Job '{job_name}' was not found in the workspace. "
        "Create the job or check the 'orchestration_job_name' workspace config variable."
    )


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
def build_job_run_url(host: str, run_id: int) -> str:
    return f"{host.rstrip('/')}/#job/0/run/{run_id}"


def build_monitoring_url(host: str) -> str:
    return f"{host.rstrip('/')}/#job"


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------
def build_poll_status_entry(
    lifecycle_state: RunLifeCycleState | None,
    result_state: RunResultState | None,
    start_time_ms: int | None,
    end_time_ms: int | None,
) -> dict[str, Any]:
    status = "Unknown"
    if lifecycle_state:
        status = lifecycle_state.value
        if result_state:
            status = f"{lifecycle_state.value}/{result_state.value}"
    return {
        "observedAtUtc": utc_timestamp(),
        "status": status,
        "startTimeUtc": (
            datetime.fromtimestamp(start_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
            if start_time_ms
            else None
        ),
        "endTimeUtc": (
            datetime.fromtimestamp(end_time_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
            if end_time_ms
            else None
        ),
    }


def format_poll_status_message(execution_mode: str, entry: dict[str, Any]) -> str:
    return f"{execution_mode} status is {entry['status']}."


def get_poll_status_stream(pretty: bool):
    return sys.stdout


def emit_poll_status(execution_mode: str, entry: dict[str, Any], pretty: bool) -> None:
    stream = get_poll_status_stream(pretty)
    print(format_poll_status_message(execution_mode, entry), file=stream, flush=True)


# ---------------------------------------------------------------------------
# Polling
# ---------------------------------------------------------------------------
def poll_job(
    run_id: int,
    max_wait_minutes: int,
    poll_interval_seconds: int,
    status_callback: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[str, Any]:
    """Poll a Databricks job run until terminal or timeout."""
    client = get_workspace_client()
    deadline = time.time() + (max_wait_minutes * 60)
    last_run: Any = None

    while time.time() < deadline:
        run = client.jobs.get_run(run_id=run_id)
        last_run = run
        state = run.state
        if state:
            entry = build_poll_status_entry(
                lifecycle_state=state.life_cycle_state,
                result_state=state.result_state,
                start_time_ms=run.start_time,
                end_time_ms=run.end_time,
            )
            if status_callback is not None:
                status_callback(entry)

            if state.life_cycle_state in TERMINAL_LIFECYCLE_STATES:
                if state.result_state == RunResultState.SUCCESS:
                    return "Completed", run
                if state.result_state == RunResultState.CANCELED:
                    return "Cancelled", run
                if state.result_state == RunResultState.TIMEDOUT:
                    return "TimedOut", run
                return "Failed", run

        time.sleep(poll_interval_seconds)

    return "TimedOut", last_run


# ---------------------------------------------------------------------------
# Table metadata parsing
# ---------------------------------------------------------------------------
def find_table_metadata(metadata_dir: Path, table_id: int) -> TableMetadata:
    pattern = re.compile(
        r"\(\s*'(?P<trigger>[^']+)'\s*,\s*(?P<order>\d+)\s*,\s*(?P<table_id>\d+)\s*,\s*"
        r"'(?P<target_datastore>[^']*)'\s*,\s*'(?P<target_entity>[^']*)'\s*,\s*"
        r"(?:'[^']*'|NULL)\s*,\s*'(?P<processing_method>[^']*)'\s*,\s*(?P<active>[01])\s*\)"
    )
    matches: list[TableMetadata] = []
    for sql_path in metadata_dir.rglob("notebook-content.sql"):
        sql_text = sql_path.read_text(encoding="utf-8")
        for match in pattern.finditer(sql_text):
            if int(match.group("table_id")) != table_id:
                continue
            matches.append(
                TableMetadata(
                    table_id=table_id,
                    trigger_name=match.group("trigger"),
                    order_of_operations=int(match.group("order")),
                    target_datastore=match.group("target_datastore"),
                    target_entity=match.group("target_entity"),
                    processing_method=match.group("processing_method"),
                    ingestion_active=match.group("active") == "1",
                    metadata_file=str(sql_path),
                )
            )
    if not matches:
        raise AgentError(f"Table_ID {table_id} was not found in metadata SQL under '{metadata_dir}'.")
    if len(matches) > 1:
        files = ", ".join(sorted(match.metadata_file for match in matches))
        raise AgentError(f"Table_ID {table_id} matched multiple metadata files: {files}")
    return matches[0]


# ---------------------------------------------------------------------------
# Context resolution
# ---------------------------------------------------------------------------
def resolve_run_context(args: argparse.Namespace) -> tuple[dict[str, Any], TableMetadata | None, str]:
    required_variables = [
        "sql_warehouse_id",
        "metadata_database",
        "job_id_batch_processing",
        "orchestration_job_name",
    ]
    execution_context = resolve_execution_context(
        source_directory=args.source_directory,
        git_folder_name=args.git_folder_name,
        required_variables=required_variables,
    )
    table_metadata: TableMetadata | None = None
    if args.table_id > 0:
        metadata_dir = Path(execution_context["GitFolderPath"]) / "metadata"
        table_metadata = find_table_metadata(metadata_dir, args.table_id)

    if not args.trigger_name and table_metadata is None:
        raise AgentError("Provide either --table-id or --trigger-name.")

    trigger_name = args.trigger_name or (table_metadata.trigger_name if table_metadata else None)
    if not trigger_name:
        raise AgentError("Unable to resolve Trigger_Name from metadata or arguments.")
    return execution_context, table_metadata, trigger_name


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    args = parse_args()
    started_at = time.time()
    result: dict[str, Any] = {
        "executionMode": None,
        "executionStatus": "Skipped",
        "runId": None,
        "errorMessage": None,
        "errorType": None,
        "stackTrace": None,
        "databricksRunUrl": None,
        "pollHistory": [],
        "durationSeconds": 0,
    }

    try:
        execution_context, table_metadata, trigger_name = resolve_run_context(args)
        variables = execution_context["Variables"]
        processing_method = (table_metadata.processing_method if table_metadata else "pipeline").strip().lower()
        is_notebook_mode = bool(
            table_metadata
            and args.table_id > 0
            and processing_method == "batch"
            and not args.table_ids
        )
        execution_mode = "Notebook" if is_notebook_mode else "Job"

        result["executionMode"] = execution_mode
        result["branchName"] = execution_context["BranchName"]
        result["hasFeatureOverrides"] = execution_context["HasOverrides"]
        result["overrideStatusMessage"] = execution_context["OverrideStatusMessage"]
        result["triggerExecutionTime"] = utc_timestamp()
        result["resolvedContext"] = {
            "gitFolderName": execution_context["GitFolderName"],
            "gitFolderPath": execution_context["GitFolderPath"],
            "sqlWarehouseId": variables["sql_warehouse_id"],
            "jobIdBatchProcessing": variables["job_id_batch_processing"],
        }
        if table_metadata is not None:
            result["tableMetadata"] = asdict(table_metadata)

        client = get_workspace_client()
        host = client.config.host or ""
        trigger_execution_id = str(uuid.uuid4())

        if execution_mode == "Notebook":
            assert table_metadata is not None
            job_id = int(variables["job_id_batch_processing"])
            trigger_step = args.trigger_step or table_metadata.order_of_operations
            job_parameters = {
                "sql_warehouse_id": variables["sql_warehouse_id"],
                "metadata_database": variables["metadata_database"],
                "table_id": str(table_metadata.table_id),
                "trigger_name": table_metadata.trigger_name,
                "trigger_step": str(trigger_step),
                "trigger_execution_id": trigger_execution_id,
                "trigger_execution_time": result["triggerExecutionTime"],
                "target_datastore": table_metadata.target_datastore,
                "target_entity": table_metadata.target_entity,
                "event_payload": args.event_payload,
                "folder_path_from_trigger": args.folder_path_from_trigger,
            }
            run_response = client.jobs.run_now(
                job_id=job_id,
                job_parameters=job_parameters,
            )
            run_id = run_response.run_id
            result["databricksRunUrl"] = build_job_run_url(host, run_id)
            result["parameters"] = {
                "table_id": table_metadata.table_id,
                "trigger_name": table_metadata.trigger_name,
                "trigger_step": trigger_step,
                "target_datastore": table_metadata.target_datastore,
                "target_entity": table_metadata.target_entity,
            }
        else:
            job_name = variables.get("orchestration_job_name", JOB_NAME)
            job_id = discover_job_id(job_name)
            start_with_step = args.start_with_step or args.trigger_step or 0
            table_ids = args.table_ids or (str(args.table_id) if args.table_id > 0 else "0")
            job_parameters = {
                "trigger_name": trigger_name,
                "start_with_step": str(start_with_step),
                "table_ids": table_ids,
                "event_payload": args.event_payload,
                "folder_path_from_trigger": args.folder_path_from_trigger,
                "run_exploratory_analysis": str(args.run_exploratory_analysis).lower(),
            }
            run_response = client.jobs.run_now(
                job_id=job_id,
                job_parameters=job_parameters,
            )
            run_id = run_response.run_id
            result["databricksRunUrl"] = build_job_run_url(host, run_id)
            result["parameters"] = job_parameters

        result["runId"] = run_id
        result["jobId"] = job_id
        result["triggerExecutionId"] = trigger_execution_id
        result["executionStatus"] = "Submitted"
        result["latestObservedStatus"] = "Submitted"

        def handle_poll_status(entry: dict[str, Any]) -> None:
            result["pollHistory"].append(entry)
            result["latestObservedStatus"] = entry["status"]
            emit_poll_status(execution_mode, entry, args.pretty)

        status, run_obj = poll_job(
            run_id=run_id,
            max_wait_minutes=args.max_wait_minutes,
            poll_interval_seconds=args.poll_interval_seconds,
            status_callback=handle_poll_status,
        )

        if status == "Completed":
            result["executionStatus"] = "Completed"
        elif status == "Failed":
            result["executionStatus"] = "Failed"
            state_message = ""
            if run_obj and hasattr(run_obj, "state") and run_obj.state:
                state_message = run_obj.state.state_message or ""
            result["errorMessage"] = state_message or "Databricks job execution failed."
        elif status == "Cancelled":
            result["executionStatus"] = "Cancelled"
            result["errorMessage"] = "Databricks job execution was cancelled."
        else:
            result["executionStatus"] = "TimedOut"
            latest_status = result.get("latestObservedStatus")
            if latest_status and latest_status != "Submitted":
                result["errorMessage"] = f"Databricks job did not complete before the timeout. Last observed status: {latest_status}."
            else:
                result["errorMessage"] = "Databricks job did not complete before the timeout."
    except AgentError as exc:
        if result["runId"]:
            result["executionStatus"] = "Failed"
            result["errorMessage"] = (
                f"The Databricks job was submitted, but the local watcher stopped before completion. {exc} "
                "Use the Run ID or Databricks Run URL to confirm the remote run status."
            )
        else:
            if result["executionStatus"] == "Skipped":
                result["executionStatus"] = "FailedToStart"
            result["errorMessage"] = str(exc)
        result["errorType"] = type(exc).__name__
    except Exception as exc:
        if result["runId"]:
            result["executionStatus"] = "Failed"
        elif result["executionStatus"] == "Skipped":
            result["executionStatus"] = "FailedToStart"
        result["errorType"] = type(exc).__name__
        result["errorMessage"] = f"Unexpected error: {exc}"
        result["stackTrace"] = traceback.format_exc()
    finally:
        result["durationSeconds"] = int(time.time() - started_at)

    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result, separators=(",", ":")))
    return 0 if result["executionStatus"] in {"Completed", "TimedOut"} else 1


if __name__ == "__main__":
    raise SystemExit(main())