from __future__ import annotations

from typing import Any


QUERY_CAPABILITIES_VERSION = 1


def _capability(
    domain: str,
    summary: str,
    *,
    required_all_of: list[str] | None = None,
    required_any_of: list[str] | None = None,
    optional_args: list[str] | None = None,
    result_kind: str = "rows",
    preferred_contexts: list[str] | None = None,
    side_effects: str = "none",
    resolve_flag_behavior: str | None = None,
) -> dict[str, Any]:
    capability: dict[str, Any] = {
        "domain": domain,
        "summary": summary,
        "required_all_of": required_all_of or [],
        "required_any_of": required_any_of or [],
        "optional_args": optional_args or [],
        "result_kind": result_kind,
        "preferred_contexts": preferred_contexts or [],
        "side_effects": side_effects,
    }
    if resolve_flag_behavior:
        capability["resolve_flag_behavior"] = resolve_flag_behavior
    return capability


QUERY_CAPABILITIES: dict[str, dict[str, Any]] = {
    "RunStatus": _capability(
        "run_history",
        "Latest table status rows from Data_Pipeline_Logs.",
        required_all_of=["table_id"],
        optional_args=["top"],
        preferred_contexts=["latest status by table", "run health"],
    ),
    "RecentRuns": _capability(
        "run_history",
        "Recent table runs with duration, watermark, and monitor URL.",
        required_all_of=["table_id"],
        optional_args=["top", "days"],
        preferred_contexts=["recent runs", "post-run history"],
    ),
    "RecentFailures": _capability(
        "run_history",
        "Recent failed runs across the metadata warehouse.",
        optional_args=["hours"],
        preferred_contexts=["recent platform failures"],
    ),
    "RunPerformance": _capability(
        "run_history",
        "Run performance aggregates by table and processing phase.",
        required_all_of=["table_id"],
        optional_args=["days"],
        preferred_contexts=["performance", "duration analysis"],
    ),
    "VolumeTrend": _capability(
        "run_history",
        "Daily processed and quarantined record totals for a table.",
        required_all_of=["table_id"],
        optional_args=["days"],
        preferred_contexts=["volume trend", "freshness trend"],
    ),
    "WatermarkProgress": _capability(
        "run_history",
        "Recent watermark values for batch runs of a table.",
        required_all_of=["table_id"],
        optional_args=["top"],
        preferred_contexts=["watermark progress"],
    ),
    "TriggerReliability": _capability(
        "run_history",
        "Failure-rate summary across triggers.",
        optional_args=["days"],
        preferred_contexts=["trigger health", "reliability"],
    ),
    "ActiveRuns": _capability(
        "run_history",
        "Currently started runs that have not completed.",
        optional_args=["top", "hours"],
        preferred_contexts=["active runs", "what is running now"],
    ),
    "RunByExecution": _capability(
        "run_history",
        "Run rows correlated by Trigger_Execution_ID.",
        required_all_of=["trigger_execution_id"],
        optional_args=["table_id", "top"],
        preferred_contexts=["execution-correlated run lookup"],
    ),
    "RunOutcomeWithDQ": _capability(
        "post_run_validation",
        "Execution-correlated run rows plus DQ notifications for those Log_IDs.",
        required_all_of=["trigger_execution_id"],
        optional_args=["table_id", "top"],
        result_kind="run_rows_with_dq",
        preferred_contexts=["failed run analysis", "execution-correlated outcome"],
    ),
    "LatestRunWithDQByTable": _capability(
        "post_run_validation",
        "Most recent relevant run row for a table plus correlated DQ notifications.",
        required_all_of=["table_id"],
        optional_args=["top", "days"],
        result_kind="selected_run_with_dq",
        preferred_contexts=["successful single-table post-run validation"],
    ),
    "LatestRunWithDQByTrigger": _capability(
        "post_run_validation",
        "Latest execution for a trigger plus correlated DQ notifications.",
        required_all_of=["trigger_name"],
        optional_args=["table_ids", "days"],
        result_kind="trigger_run_rows_with_dq",
        preferred_contexts=["successful trigger or multi-table post-run validation"],
    ),
    "DQSummary": _capability(
        "data_quality",
        "All DQ notifications for a table ordered by start time.",
        required_all_of=["table_id"],
        preferred_contexts=["dq summary", "quarantine details"],
    ),
    "DQBreakdown": _capability(
        "data_quality",
        "Grouped DQ counts and impacted rows by category and result.",
        required_all_of=["table_id"],
        optional_args=["days"],
        preferred_contexts=["dq breakdown", "rule distribution"],
    ),
    "DQTrend": _capability(
        "data_quality",
        "Daily DQ trend aggregates for a table.",
        required_all_of=["table_id"],
        optional_args=["days"],
        preferred_contexts=["dq trends"],
    ),
    "DQOverview": _capability(
        "data_quality",
        "Cross-table DQ overview across recent days.",
        optional_args=["top", "days"],
        preferred_contexts=["platform dq overview"],
    ),
    "DQByLogId": _capability(
        "data_quality",
        "DQ notifications for a specific Log_ID.",
        required_all_of=["log_id"],
        preferred_contexts=["single run dq by log id"],
    ),
    "ActivityLogsByLogId": _capability(
        "diagnostics",
        "Activity_Run_Logs rows for a specific Log_ID.",
        required_all_of=["log_id"],
        preferred_contexts=["step logs by log id"],
    ),
    "ActivityLogsByExecution": _capability(
        "diagnostics",
        "Execution-correlated run rows plus activity logs, with fallback by Table_ID when needed.",
        required_all_of=["trigger_execution_id"],
        optional_args=["table_id", "hours", "top"],
        result_kind="run_rows_with_activity_logs",
        preferred_contexts=["failed run log drill-in", "notebook logs for a run"],
    ),
    "ActivityLogsByTableId": _capability(
        "diagnostics",
        "Recent activity logs by Table_ID.",
        required_all_of=["table_id"],
        optional_args=["hours"],
        preferred_contexts=["recent activity logs by table"],
    ),
    "SchemaChanges": _capability(
        "schema",
        "Schema_Changes rows for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["schema drift history"],
    ),
    "SchemaDetails": _capability(
        "schema",
        "Stored procedure-backed schema details for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["current schema details"],
    ),
    "ProfileSummary": _capability(
        "profiling",
        "Latest EDA profile summary for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["eda profile", "column statistics"],
    ),
    "LineageUpstream": _capability(
        "lineage",
        "Upstream lineage rows for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["upstream lineage"],
    ),
    "LineageDownstream": _capability(
        "lineage",
        "Downstream lineage rows for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["downstream lineage"],
    ),
    "FullTriggerLineage": _capability(
        "lineage",
        "Full lineage graph for a trigger.",
        required_all_of=["trigger_name"],
        preferred_contexts=["trigger lineage"],
    ),
    "OrchestrationConfig": _capability(
        "metadata_config",
        "Orchestration metadata row for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["orchestration config"],
    ),
    "ConfigAndStatus": _capability(
        "metadata_config",
        "Configuration and latest run status joined for a table.",
        required_all_of=["table_id"],
        preferred_contexts=["config plus last status"],
    ),
    "FullHealth": _capability(
        "diagnostics",
        "Bundled run, DQ, schema, and profile views for a table.",
        required_all_of=["table_id"],
        result_kind="named_result_sets",
        preferred_contexts=["broad health check"],
    ),
    "FullMetadataConfig": _capability(
        "metadata_config",
        "Bundled orchestration, primary config, and advanced config result sets for a table.",
        required_all_of=["table_id"],
        result_kind="named_result_sets",
        preferred_contexts=["full config inspection"],
    ),
    "DatastoreConfig": _capability(
        "metadata_config",
        "Datastore_Configuration row for a datastore.",
        required_all_of=["datastore_name"],
        preferred_contexts=["datastore endpoint lookup"],
    ),
    "TriggerOverview": _capability(
        "metadata_config",
        "Bundled trigger orchestration and last-run-per-table result sets.",
        required_all_of=["trigger_name"],
        result_kind="named_result_sets",
        preferred_contexts=["trigger summary"],
    ),
    "WatermarkComparison": _capability(
        "metadata_config",
        "Current versus previous watermark values across a trigger.",
        required_all_of=["trigger_name"],
        result_kind="rows",
        preferred_contexts=["watermark comparison"],
    ),
    "StaleLogs": _capability(
        "operations",
        "Find stale staging log rows that block FIFO execution, with optional resolution.",
        required_all_of=["table_id"],
        optional_args=["hours", "resolve"],
        preferred_contexts=["stale logs", "fifo blockage"],
        side_effects="optional_with_resolve",
        resolve_flag_behavior="Insert a Failed staging row for each stale Log_ID to clear the FIFO block without deleting history.",
    ),
    "ResetWatermark": _capability(
        "operations",
        "Preview or insert FULL_RELOAD watermark reset records for a table, set of tables, or trigger.",
        required_any_of=["table_id", "table_ids", "trigger_name"],
        optional_args=["resolve"],
        preferred_contexts=["force full reload", "reset watermark"],
        side_effects="optional_with_resolve",
        resolve_flag_behavior="Insert Processed FULL_RELOAD watermark records for the targeted tables.",
    ),
}


QUERY_TYPE_CHOICES = tuple(QUERY_CAPABILITIES.keys())


def validate_query_type_arguments(args: Any) -> str | None:
    capability = QUERY_CAPABILITIES.get(args.query_type)
    if capability is None:
        return f"Unsupported query type: {args.query_type}"

    for argument_name in capability["required_all_of"]:
        value = getattr(args, argument_name, None)
        if value is None or value == "":
            return f"--{argument_name.replace('_', '-')} is required for query type '{args.query_type}'."

    required_any_of = capability["required_any_of"]
    if required_any_of:
        if not any(getattr(args, argument_name, None) not in (None, "") for argument_name in required_any_of):
            required_display = ", ".join(f"--{name.replace('_', '-')}" for name in required_any_of)
            return f"One of {required_display} is required for query type '{args.query_type}'."

    return None