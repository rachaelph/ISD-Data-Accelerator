#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from automation_scripts.agents.query_capabilities import QUERY_TYPE_CHOICES, validate_query_type_arguments
from automation_scripts.utils.databricks_agent_utils import AgentError, execute_sql_query, execute_sql_queries, resolve_metadata_warehouse_from_datastore, resolve_table_id_from_metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Deterministic one-shot script for metadata-warehouse investigations.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment")
    parser.add_argument("--query-type", required=True, choices=QUERY_TYPE_CHOICES)
    parser.add_argument("--table-id", type=int)
    parser.add_argument("--table-name")
    parser.add_argument("--medallion-layer")
    parser.add_argument("--trigger-name")
    parser.add_argument("--datastore-name")
    parser.add_argument("--table-ids")
    parser.add_argument("--trigger-execution-id")
    parser.add_argument("--log-id")
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--delete", action="store_true", help="Deprecated. Use --resolve instead.")
    parser.add_argument("--resolve", action="store_true", help="For StaleLogs: insert a 'Failed' row for each stale Log_ID to clear the FIFO block without deleting history.")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def sql_string_literal(value: str) -> str:
    return value.replace("'", "''")


def parse_table_ids_csv(table_ids: str | None) -> list[int]:
    if not table_ids:
        return []
    values: list[int] = []
    for raw_part in table_ids.split(","):
        part = raw_part.strip()
        if not part:
            continue
        try:
            values.append(int(part))
        except ValueError as exc:
            raise AgentError(f"Invalid table id '{part}' in --table-ids. Use a comma-separated list of integers.") from exc
    return values


def get_table_ids_filter_sql(table_ids: list[int]) -> str:
    if not table_ids:
        return ""
    joined_ids = ", ".join(str(table_id) for table_id in table_ids)
    return f" AND Table_ID IN ({joined_ids})"


def get_run_by_execution_sql(args: argparse.Namespace) -> str:
    execution_filter = f"Trigger_Execution_ID = '{sql_string_literal(args.trigger_execution_id)}'"
    table_filter = f" AND Table_ID = {args.table_id}" if args.table_id is not None else ""
    return (
        "SELECT TOP {top} Log_ID, Table_ID, Target_Entity, Trigger_Name, Trigger_Step, Ingestion_Status, "
        "Processing_Phase, Records_Processed, Quarantined_Records, Ingestion_Start_Time, Ingestion_End_Time, "
        "DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time) AS Duration_Seconds, Job_Run_URL "
        "FROM dbo.Data_Pipeline_Logs WHERE {execution_filter}{table_filter} "
        "ORDER BY Ingestion_End_Time DESC, Ingestion_Start_Time DESC;"
    ).format(top=args.top, execution_filter=execution_filter, table_filter=table_filter)


def get_recent_runs_sql(table_id: int, top: int, days: int) -> str:
    return (
        "SELECT TOP {top} Log_ID, Table_ID, Target_Entity, Trigger_Name, Ingestion_Status, Processing_Phase, "
        "Records_Processed, Quarantined_Records, Ingestion_Start_Time, Ingestion_End_Time, "
        "DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time) AS Duration_Seconds, Watermark_Value, "
        "Job_Run_URL FROM dbo.Data_Pipeline_Logs WHERE Table_ID = {table_id} "
        "AND Ingestion_Start_Time >= DATEADD(DAY, -{days}, GETUTCDATE()) "
        "ORDER BY COALESCE(Ingestion_End_Time, Ingestion_Start_Time) DESC, Ingestion_Start_Time DESC;"
    ).format(top=top, table_id=table_id, days=days)


def get_latest_run_by_trigger_sql(trigger_name: str, table_ids: list[int], days: int) -> str:
    table_filter = get_table_ids_filter_sql(table_ids)
    escaped_trigger_name = sql_string_literal(trigger_name)
    return (
        "WITH latest_execution AS ("
        " SELECT TOP 1 Trigger_Execution_ID, MAX(COALESCE(Ingestion_End_Time, Ingestion_Start_Time)) AS Run_End_Time"
        " FROM dbo.Data_Pipeline_Logs"
        " WHERE Trigger_Name = '{trigger_name}'"
        "   AND Trigger_Execution_ID IS NOT NULL"
        "   AND Ingestion_Start_Time >= DATEADD(DAY, -{days}, GETUTCDATE())"
        "   {table_filter}"
        " GROUP BY Trigger_Execution_ID"
        " ORDER BY MAX(COALESCE(Ingestion_End_Time, Ingestion_Start_Time)) DESC"
        ")"
        " SELECT l.Log_ID, l.Table_ID, l.Target_Entity, l.Trigger_Name, l.Trigger_Step, l.Trigger_Execution_ID,"
        " l.Ingestion_Status, l.Processing_Phase, l.Records_Processed, l.Quarantined_Records,"
        " l.Ingestion_Start_Time, l.Ingestion_End_Time,"
        " DATEDIFF(SECOND, l.Ingestion_Start_Time, l.Ingestion_End_Time) AS Duration_Seconds,"
        " l.Watermark_Value, l.Job_Run_URL"
        " FROM dbo.Data_Pipeline_Logs l"
        " JOIN latest_execution e ON l.Trigger_Execution_ID = e.Trigger_Execution_ID"
        " WHERE l.Trigger_Name = '{trigger_name}'"
        "   {table_filter}"
        " ORDER BY l.Trigger_Step ASC, l.Table_ID ASC,"
        " CASE WHEN l.Processing_Phase = 'Batch' THEN 0 ELSE 1 END ASC,"
        " COALESCE(l.Ingestion_End_Time, l.Ingestion_Start_Time) DESC;"
    ).format(trigger_name=escaped_trigger_name, days=days, table_filter=table_filter)


def choose_latest_run_row(rows: list[dict[str, object]]) -> dict[str, object] | None:
    if not rows:
        return None
    batch_rows = [row for row in rows if row.get("Processing_Phase") == "Batch"]
    if batch_rows:
        return batch_rows[0]
    return rows[0]


def get_dq_by_log_id_sql(log_id: str) -> str:
    return (
        "SELECT Log_ID, Table_ID, Table_Name, Datastore_Name, Data_Quality_Category, Data_Quality_Result, "
        "Data_Quality_Message, Rows_Impacted, Data_Quarantined, Rows_Quarantined, Ingestion_Start_Time, "
        "Ingestion_End_Time, Job_Run_URL FROM dbo.Data_Quality_Notifications "
        "WHERE Log_ID = '{log_id}' ORDER BY Ingestion_End_Time DESC, Ingestion_Start_Time DESC;"
    ).format(log_id=sql_string_literal(log_id))


def get_activity_logs_by_log_id_sql(log_id: str) -> str:
    return (
        "SELECT Table_ID, Log_ID, Sequence_Number, Log_Timestamp, Log_Level, Step_Name, Step_Number, Message, Source_Type "
        "FROM dbo.Activity_Run_Logs WHERE Log_ID = '{log_id}' ORDER BY Sequence_Number ASC;"
    ).format(log_id=sql_string_literal(log_id))


def get_activity_logs_by_table_id_sql(table_id: int, hours: int) -> str:
    return (
        "SELECT Table_ID, Log_ID, Sequence_Number, Log_Timestamp, Log_Level, Step_Name, Step_Number, Message, Source_Type "
        "FROM dbo.Activity_Run_Logs WHERE Table_ID = {table_id} "
        "AND Log_Timestamp >= DATEADD(HOUR, -{hours}, GETUTCDATE()) "
        "ORDER BY Log_Timestamp DESC, Sequence_Number ASC;"
    ).format(table_id=table_id, hours=hours)


def get_single_query_sql(args: argparse.Namespace) -> str | None:
    if args.query_type == "RunStatus":
        return f"""SELECT TOP {args.top} Log_ID, Table_ID, Target_Entity, Ingestion_Status, Processing_Phase, Records_Processed, Quarantined_Records, Ingestion_Start_Time, Ingestion_End_Time, DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time) AS Duration_Seconds, Watermark_Value, Job_Run_URL FROM dbo.Data_Pipeline_Logs WHERE Table_ID = {args.table_id} ORDER BY Ingestion_End_Time DESC;"""
    if args.query_type == "RecentRuns":
        return get_recent_runs_sql(args.table_id, args.top, args.days)
    if args.query_type == "RecentFailures":
        return f"""SELECT l.Log_ID, l.Table_ID, l.Target_Entity, l.Trigger_Name, l.Ingestion_Start_Time, l.Ingestion_End_Time, l.Job_Run_URL FROM dbo.Data_Pipeline_Logs l WHERE l.Ingestion_Status = 'Failed' AND l.Ingestion_Start_Time >= DATEADD(HOUR, -{args.hours}, GETUTCDATE()) ORDER BY l.Ingestion_Start_Time DESC;"""
    if args.query_type == "RunPerformance":
        return f"""SELECT Table_ID, Target_Entity, Processing_Phase, COUNT(*) AS Run_Count, AVG(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time)) AS Avg_Duration_Seconds, MIN(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time)) AS Min_Duration_Seconds, MAX(DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time)) AS Max_Duration_Seconds, AVG(Records_Processed) AS Avg_Records_Processed FROM dbo.Data_Pipeline_Logs WHERE Table_ID = {args.table_id} AND Ingestion_Status = 'Processed' AND Ingestion_Start_Time >= DATEADD(DAY, -{args.days}, GETUTCDATE()) GROUP BY Table_ID, Target_Entity, Processing_Phase;"""
    if args.query_type == "VolumeTrend":
        return f"""SELECT d.[Date], d.Day_Name, SUM(l.Records_Processed) AS Total_Records, SUM(l.Quarantined_Records) AS Total_Quarantined, COUNT(*) AS Run_Count FROM dbo.Data_Pipeline_Logs l JOIN dbo.Date_Dimension d ON l.Date_Key = d.Date_Key WHERE l.Table_ID = {args.table_id} AND l.Ingestion_Status = 'Processed' AND d.[Date] >= DATEADD(DAY, -{args.days}, GETUTCDATE()) GROUP BY d.[Date], d.Day_Name ORDER BY d.[Date] DESC;"""
    if args.query_type == "WatermarkProgress":
        return f"""SELECT TOP {args.top} Log_ID, Ingestion_Start_Time, Ingestion_Status, Watermark_Value, Records_Processed FROM dbo.Data_Pipeline_Logs WHERE Table_ID = {args.table_id} AND Watermark_Value IS NOT NULL AND Processing_Phase = 'Batch' ORDER BY Ingestion_End_Time DESC;"""
    if args.query_type == "TriggerReliability":
        return f"""SELECT Trigger_Name, COUNT(*) AS Total_Runs, SUM(CASE WHEN Ingestion_Status = 'Failed' THEN 1 ELSE 0 END) AS Failed_Runs, SUM(CASE WHEN Ingestion_Status = 'Processed' THEN 1 ELSE 0 END) AS Successful_Runs, CAST(SUM(CASE WHEN Ingestion_Status = 'Failed' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(5,2)) AS Failure_Rate_Pct FROM dbo.Data_Pipeline_Logs WHERE Processing_Phase = 'Batch' AND Ingestion_Start_Time >= DATEADD(DAY, -{args.days}, GETUTCDATE()) GROUP BY Trigger_Name ORDER BY Failure_Rate_Pct DESC;"""
    if args.query_type == "ActiveRuns":
        return f"""SELECT TOP {args.top} Log_ID, Table_ID, Target_Entity, Trigger_Name, Processing_Phase, Ingestion_Start_Time, DATEDIFF(MINUTE, Ingestion_Start_Time, GETUTCDATE()) AS Minutes_Elapsed, Job_Run_URL FROM dbo.Data_Pipeline_Logs WHERE Ingestion_Status = 'Started' AND Ingestion_Start_Time >= DATEADD(HOUR, -{args.hours}, GETUTCDATE()) ORDER BY Ingestion_Start_Time ASC;"""
    if args.query_type == "RunByExecution":
        return get_run_by_execution_sql(args)
    if args.query_type == "DQSummary":
        return f"""SELECT Log_ID, Table_Name, Data_Quality_Category, Data_Quality_Result, Data_Quality_Message, Rows_Impacted, Data_Quarantined, Rows_Quarantined, Ingestion_Start_Time, Job_Run_URL FROM dbo.Data_Quality_Notifications WHERE Table_ID = {args.table_id} ORDER BY Ingestion_Start_Time DESC;"""
    if args.query_type == "DQBreakdown":
        return f"""SELECT Data_Quality_Category, Data_Quality_Result, COUNT(*) AS Occurrence_Count, SUM(Rows_Impacted) AS Total_Rows_Impacted, SUM(Rows_Quarantined) AS Total_Rows_Quarantined FROM dbo.Data_Quality_Notifications WHERE Table_ID = {args.table_id} AND Ingestion_Start_Time >= DATEADD(DAY, -{args.days}, GETUTCDATE()) GROUP BY Data_Quality_Category, Data_Quality_Result ORDER BY Occurrence_Count DESC;"""
    if args.query_type == "DQTrend":
        return f"""SELECT d.[Date], d.Day_Name, SUM(dq.Rows_Quarantined) AS Total_Quarantined, SUM(dq.Rows_Impacted) AS Total_Impacted, COUNT(*) AS DQ_Notification_Count FROM dbo.Data_Quality_Notifications dq JOIN dbo.Date_Dimension d ON dq.Date_Key = d.Date_Key WHERE dq.Table_ID = {args.table_id} AND d.[Date] >= DATEADD(DAY, -{args.days}, GETUTCDATE()) GROUP BY d.[Date], d.Day_Name ORDER BY d.[Date] DESC;"""
    if args.query_type == "DQOverview":
        return f"""SELECT TOP {args.top} dq.Table_ID, dq.Table_Name, dq.Datastore_Name, COUNT(*) AS Total_Notifications, SUM(CASE WHEN dq.Data_Quality_Result = 'Failure' THEN 1 ELSE 0 END) AS Failure_Count, SUM(CASE WHEN dq.Data_Quality_Result = 'Warning' THEN 1 ELSE 0 END) AS Warning_Count, SUM(dq.Rows_Quarantined) AS Total_Rows_Quarantined FROM dbo.Data_Quality_Notifications dq JOIN dbo.Date_Dimension d ON dq.Date_Key = d.Date_Key WHERE d.[Date] >= DATEADD(DAY, -{args.days}, GETUTCDATE()) GROUP BY dq.Table_ID, dq.Table_Name, dq.Datastore_Name ORDER BY Total_Notifications DESC;"""
    if args.query_type == "DQByLogId":
        return get_dq_by_log_id_sql(args.log_id)
    if args.query_type == "ActivityLogsByLogId":
        return get_activity_logs_by_log_id_sql(args.log_id)
    if args.query_type == "ActivityLogsByTableId":
        return get_activity_logs_by_table_id_sql(args.table_id, args.hours)
    if args.query_type == "SchemaChanges":
        return f"""SELECT Change_Type, Column_Name, Data_Type_Details, Schema_Arrival_Time FROM dbo.Schema_Changes WHERE Table_ID = {args.table_id} ORDER BY Schema_Arrival_Time DESC;"""
    if args.query_type == "SchemaDetails":
        return f"EXEC dbo.Get_Schema_Details @table_id = {args.table_id};"
    if args.query_type == "ProfileSummary":
        return f"""SELECT Table_Name, Column_Name, Data_Type, Total_Rows, Approx_Distinct_Values, Null_Count, Null_Percent, Mean, Std_Dev, [Min], [Max], Data_Profile_Execution_Time, Table_Last_Modified_Time FROM dbo.Exploratory_Data_Analysis_Results WHERE Table_ID = {args.table_id} AND Data_Profile_Execution_Time = (SELECT MAX(Data_Profile_Execution_Time) FROM dbo.Exploratory_Data_Analysis_Results WHERE Table_ID = {args.table_id}) ORDER BY Column_Name;"""
    if args.query_type == "LineageUpstream":
        return f"""SELECT Source_Table_ID, Source_Entity, Source_Datastore, Source_Medallion_Layer, Source_Type, Source_Workspace_Name, Relationship_Type, Transformation_Applied, Lineage_Depth, Lineage_Path, Source_Lineage_Narrative FROM dbo.Data_Pipeline_Lineage WHERE Target_Table_ID = {args.table_id} ORDER BY Lineage_Depth ASC;"""
    if args.query_type == "LineageDownstream":
        return f"""SELECT Target_Table_ID, Target_Entity, Target_Datastore, Target_Medallion_Layer, Target_Type, Target_Workspace_Name, Relationship_Type, Transformation_Applied, Lineage_Depth, Lineage_Path, Target_Lineage_Narrative FROM dbo.Data_Pipeline_Lineage WHERE Source_Table_ID = {args.table_id} ORDER BY Lineage_Depth ASC;"""
    if args.query_type == "FullTriggerLineage":
        return f"""SELECT Lineage_ID, Source_Entity, Source_Medallion_Layer, Target_Entity, Target_Medallion_Layer, Relationship_Type, Transformation_Applied, Lineage_Depth, Lineage_Path, Lineage_Group_Narrative, Is_Cross_Trigger, Cross_Trigger_Dependencies FROM dbo.Data_Pipeline_Lineage WHERE Trigger_Name = '{args.trigger_name}' ORDER BY Lineage_Depth ASC, Lineage_Path ASC;"""
    if args.query_type == "OrchestrationConfig":
        return f"""SELECT Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Table_ID = {args.table_id};"""
    if args.query_type == "ConfigAndStatus":
        return f"""SELECT o.Trigger_Name, o.Table_ID, o.Target_Entity, o.Target_Datastore, o.Ingestion_Active, l.Ingestion_Status AS Last_Status, l.Records_Processed AS Last_Records, l.Ingestion_End_Time AS Last_Run_Time, l.Watermark_Value AS Current_Watermark FROM dbo.Data_Pipeline_Metadata_Orchestration o LEFT JOIN ( SELECT Table_ID, Ingestion_Status, Records_Processed, Ingestion_End_Time, Watermark_Value, ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn FROM dbo.Data_Pipeline_Logs WHERE Processing_Phase = 'Batch' ) l ON o.Table_ID = l.Table_ID AND l.rn = 1 WHERE o.Table_ID = {args.table_id};"""
    if args.query_type == "DatastoreConfig":
        escaped_ds = sql_string_literal(args.datastore_name)
        return f"""SELECT Datastore_Name, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID FROM dbo.Datastore_Configuration WHERE Datastore_Name = '{escaped_ds}';"""
    if args.query_type == "WatermarkComparison":
        escaped_trigger = sql_string_literal(args.trigger_name)
        return f"""SELECT o.Table_ID, o.Target_Entity, o.Target_Datastore, curr.Watermark_Value AS Current_Watermark, curr.Ingestion_End_Time AS Current_Run_Time, prev.Watermark_Value AS Previous_Watermark, prev.Ingestion_End_Time AS Previous_Run_Time, curr.Records_Processed AS Current_Records FROM dbo.Data_Pipeline_Metadata_Orchestration o LEFT JOIN ( SELECT Table_ID, Watermark_Value, Ingestion_End_Time, Records_Processed, ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn FROM dbo.Data_Pipeline_Logs WHERE Processing_Phase = 'Batch' AND Watermark_Value IS NOT NULL ) curr ON o.Table_ID = curr.Table_ID AND curr.rn = 1 LEFT JOIN ( SELECT Table_ID, Watermark_Value, Ingestion_End_Time, ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn FROM dbo.Data_Pipeline_Logs WHERE Processing_Phase = 'Batch' AND Watermark_Value IS NOT NULL ) prev ON o.Table_ID = prev.Table_ID AND prev.rn = 2 WHERE o.Trigger_Name = '{escaped_trigger}' ORDER BY o.Order_Of_Operations, o.Table_ID;"""
    return None


def get_full_health_queries(table_id: int) -> dict[str, str]:
    return {
        "RunStatus": f"SELECT TOP 5 Log_ID, Target_Entity, Ingestion_Status, Processing_Phase, Records_Processed, Quarantined_Records, DATEDIFF(SECOND, Ingestion_Start_Time, Ingestion_End_Time) AS Duration_Seconds, Ingestion_Start_Time FROM dbo.Data_Pipeline_Logs WHERE Table_ID = {table_id} ORDER BY Ingestion_End_Time DESC;",
        "DQSummary": f"SELECT TOP 10 Data_Quality_Category, Data_Quality_Result, Data_Quality_Message, Rows_Impacted, Rows_Quarantined, Ingestion_Start_Time FROM dbo.Data_Quality_Notifications WHERE Table_ID = {table_id} ORDER BY Ingestion_Start_Time DESC;",
        "SchemaChanges": f"SELECT Change_Type, Column_Name, Data_Type_Details, Schema_Arrival_Time FROM dbo.Schema_Changes WHERE Table_ID = {table_id} ORDER BY Schema_Arrival_Time DESC;",
        "ProfileSummary": f"SELECT Column_Name, Data_Type, Null_Percent, Approx_Distinct_Values, Total_Rows, Data_Profile_Execution_Time FROM dbo.Exploratory_Data_Analysis_Results WHERE Table_ID = {table_id} AND Data_Profile_Execution_Time = (SELECT MAX(Data_Profile_Execution_Time) FROM dbo.Exploratory_Data_Analysis_Results WHERE Table_ID = {table_id}) ORDER BY Column_Name;",
    }


def main() -> int:
    args = parse_args()
    if args.table_name and not args.medallion_layer:
        print("--medallion-layer is required when using --table-name.", file=sys.stderr)
        return 1
    if args.medallion_layer and not args.table_name:
        print("--table-name is required when using --medallion-layer.", file=sys.stderr)
        return 1

    try:
        if args.table_id is None and args.table_name:
            args.table_id = resolve_table_id_from_metadata(
                args.source_directory,
                args.engine_folder,
                args.table_name,
                args.medallion_layer,
            )
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    validation_error = validate_query_type_arguments(args)
    if validation_error:
        print(validation_error, file=sys.stderr)
        return 1

    try:
        resolution = resolve_metadata_warehouse_from_datastore(args.source_directory, args.engine_folder, args.environment)
        parsed_table_ids = parse_table_ids_csv(args.table_ids)
        if args.query_type == "FullHealth":
            result = {
                section: execute_sql_query(resolution.metadata_warehouse_id, sql).rows
                for section, sql in get_full_health_queries(args.table_id).items()
            }
        elif args.query_type == "RunOutcomeWithDQ":
            run_rows = execute_sql_query(
                resolution.metadata_warehouse_id,
                resolution.metadata_database_name,
                get_run_by_execution_sql(args),
            ).rows
            log_ids = list(dict.fromkeys(row["Log_ID"] for row in run_rows if row.get("Log_ID")))
            dq_rows: list[dict[str, object]] = []
            if log_ids:
                dq_queries = [(f"dq_{index}", get_dq_by_log_id_sql(log_id)) for index, log_id in enumerate(log_ids)]
                dq_result_sets = execute_sql_queries(
                    resolution.metadata_warehouse_id,
                    resolution.metadata_database_name,
                    dq_queries,
                )
                for label in dq_result_sets:
                    dq_rows.extend(dq_result_sets[label].rows)
            result = {
                "RunRows": run_rows,
                "DQRows": dq_rows,
            }
        elif args.query_type == "LatestRunWithDQByTable":
            candidate_run_rows = execute_sql_query(
                resolution.metadata_warehouse_id,
                resolution.metadata_database_name,
                get_recent_runs_sql(args.table_id, args.top, args.days),
            ).rows
            selected_run_row = choose_latest_run_row(candidate_run_rows)
            dq_rows: list[dict[str, object]] = []
            if selected_run_row and selected_run_row.get("Log_ID"):
                dq_rows = execute_sql_query(
                    resolution.metadata_warehouse_id,
                    resolution.metadata_database_name,
                    get_dq_by_log_id_sql(str(selected_run_row["Log_ID"])),
                ).rows
            result = {
                "SelectedRunRow": selected_run_row,
                "DQRows": dq_rows,
            }
        elif args.query_type == "LatestRunWithDQByTrigger":
            run_rows = execute_sql_query(
                resolution.metadata_warehouse_id,
                resolution.metadata_database_name,
                get_latest_run_by_trigger_sql(args.trigger_name, parsed_table_ids, args.days),
            ).rows
            dq_rows: list[dict[str, object]] = []
            log_ids = list(dict.fromkeys(str(row["Log_ID"]) for row in run_rows if row.get("Log_ID")))
            if log_ids:
                dq_queries = [(f"dq_{index}", get_dq_by_log_id_sql(log_id)) for index, log_id in enumerate(log_ids)]
                dq_result_sets = execute_sql_queries(
                    resolution.metadata_warehouse_id,
                    resolution.metadata_database_name,
                    dq_queries,
                )
                for label in dq_result_sets:
                    dq_rows.extend(dq_result_sets[label].rows)
            result = {
                "TriggerName": args.trigger_name,
                "TableIds": parsed_table_ids,
                "RunRows": run_rows,
                "DQRows": dq_rows,
            }
        elif args.query_type == "ActivityLogsByExecution":
            run_rows = execute_sql_query(
                resolution.metadata_warehouse_id,
                resolution.metadata_database_name,
                get_run_by_execution_sql(args),
            ).rows
            log_ids = list(dict.fromkeys(row["Log_ID"] for row in run_rows if row.get("Log_ID")))
            activity_log_rows: list[dict[str, object]] = []
            if log_ids:
                activity_queries = [
                    (f"activity_logs_{index}", get_activity_logs_by_log_id_sql(log_id))
                    for index, log_id in enumerate(log_ids)
                ]
                activity_result_sets = execute_sql_queries(
                    resolution.metadata_warehouse_id,
                    resolution.metadata_database_name,
                    activity_queries,
                )
                for label in activity_result_sets:
                    activity_log_rows.extend(activity_result_sets[label].rows)
            elif not run_rows and args.table_id is not None:
                # Data_Pipeline_Logs has no rows for this execution (pipeline failed before
                # logging "Started"). Fall back to Activity_Run_Logs by Table_ID so orphaned
                # entries written by the pipeline's error-logging SP are still surfaced.
                activity_log_rows = execute_sql_query(
                    resolution.metadata_warehouse_id,
                    resolution.metadata_database_name,
                    get_activity_logs_by_table_id_sql(args.table_id, args.hours),
                ).rows
            result = {
                "RunRows": run_rows,
                "ActivityLogRows": activity_log_rows,
            }
        elif args.query_type == "FullMetadataConfig":
            metadata_queries = [
                ("Orchestration", f"SELECT Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Table_ID = {args.table_id};"),
                ("PrimaryConfig", f"SELECT Table_ID, Configuration_Category, Configuration_Name, Configuration_Value FROM dbo.Data_Pipeline_Metadata_Primary_Configuration WHERE Table_ID = {args.table_id} ORDER BY Configuration_Category, Configuration_Name;"),
                ("AdvancedConfig", f"SELECT Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration WHERE Table_ID = {args.table_id} ORDER BY Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name;"),
            ]
            metadata_result_sets = execute_sql_queries(
                resolution.metadata_warehouse_id,
                resolution.metadata_database_name,
                metadata_queries,
            )
            result = {label: metadata_result_sets[label].rows for label in metadata_result_sets}
        elif args.query_type == "StaleLogs":
            stale_subquery = (
                f"SELECT Log_ID FROM dbo.Data_Pipeline_Logs "
                f"WHERE Table_ID = {args.table_id} AND Processing_Phase = 'Staging' "
                f"GROUP BY Log_ID HAVING MIN(Ingestion_Status) = 'Started'"
            )
            stale_filter = (
                f"WHERE Table_ID = {args.table_id} "
                f"AND Log_ID IN ({stale_subquery}) "
                f"AND Ingestion_Start_Time > DATEADD(HOUR, -{args.hours}, GETUTCDATE())"
            )
            if args.resolve or args.delete:
                # Find the distinct stale Log_IDs that need a 'Failed' row inserted
                select_sql = (
                    f"SELECT DISTINCT Log_ID, Table_ID, Ingestion_Start_Time "
                    f"FROM dbo.Data_Pipeline_Logs {stale_filter} "
                    f"AND Processing_Phase = 'Staging' AND Ingestion_Status = 'Started' "
                    f"ORDER BY Ingestion_Start_Time DESC;"
                )
                stale_rows = execute_sql_query(resolution.metadata_warehouse_id, select_sql).rows
                if not stale_rows:
                    result = {"resolved": 0, "rows": []}
                else:
                    # Insert a 'Failed' row for each stale Log_ID to clear the FIFO block
                    insert_sql = (
                        f"INSERT INTO dbo.Data_Pipeline_Logs (Log_ID, Table_ID, Processing_Phase, Ingestion_Status, Ingestion_Start_Time, Ingestion_End_Time) "
                        f"SELECT DISTINCT Log_ID, Table_ID, 'Staging', 'Failed', Ingestion_Start_Time, GETUTCDATE() "
                        f"FROM dbo.Data_Pipeline_Logs "
                        f"{stale_filter} "
                        f"AND Processing_Phase = 'Staging' AND Ingestion_Status = 'Started';"
                    )
                    execute_sql_query(resolution.metadata_warehouse_id, insert_sql)
                    result = {"resolved": len(stale_rows), "rows": stale_rows}
            else:
                select_sql = f"SELECT Log_ID, Table_ID, Processing_Phase, Ingestion_Status, Ingestion_Start_Time, Ingestion_End_Time, Records_Processed FROM dbo.Data_Pipeline_Logs {stale_filter} ORDER BY Ingestion_Start_Time DESC;"
                result = execute_sql_query(resolution.metadata_warehouse_id, select_sql).rows
        elif args.query_type == "ResetWatermark":
            # Support single table (--table-id), multiple tables (--table-ids), or full trigger (--trigger-name)
            if args.trigger_name and not args.table_id and not args.table_ids:
                # Get all table IDs for the trigger
                escaped_trigger = sql_string_literal(args.trigger_name)
                trigger_sql = f"SELECT Table_ID, Target_Datastore FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = '{escaped_trigger}' ORDER BY Table_ID;"
                trigger_rows = execute_sql_query(resolution.metadata_warehouse_id, trigger_sql).rows
                if not trigger_rows:
                    raise AgentError(f"No orchestration rows found for Trigger_Name '{args.trigger_name}'.")
                table_entries = [(row["Table_ID"], row["Target_Datastore"]) for row in trigger_rows]
            elif parsed_table_ids:
                # Multiple table IDs provided
                ids_csv = ", ".join(str(t) for t in parsed_table_ids)
                orch_sql = f"SELECT Table_ID, Target_Datastore FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Table_ID IN ({ids_csv}) ORDER BY Table_ID;"
                orch_rows = execute_sql_query(resolution.metadata_warehouse_id, orch_sql).rows
                table_entries = [(row["Table_ID"], row["Target_Datastore"]) for row in orch_rows]
            elif args.table_id is not None:
                orch_sql = f"SELECT Target_Datastore FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Table_ID = {args.table_id};"
                orch_rows = execute_sql_query(resolution.metadata_warehouse_id, orch_sql).rows
                if not orch_rows:
                    raise AgentError(f"No orchestration row found for Table_ID {args.table_id}.")
                table_entries = [(args.table_id, orch_rows[0]["Target_Datastore"])]
            else:
                raise AgentError("ResetWatermark requires --table-id, --table-ids, or --trigger-name.")

            if args.resolve:
                # Batch insert all FULL_RELOAD records in one statement
                values_clauses = ", ".join(
                    f"(NEWID(), {tid}, '{sql_string_literal(ds)}', GETUTCDATE(), 'Processed', 'FULL_RELOAD')"
                    for tid, ds in table_entries
                )
                insert_sql = (
                    f"INSERT INTO dbo.Data_Pipeline_Logs "
                    f"(Log_ID, Table_ID, Target_Datastore, Ingestion_End_Time, Ingestion_Status, Watermark_Value) "
                    f"VALUES {values_clauses};"
                )
                execute_sql_query(resolution.metadata_warehouse_id, insert_sql)
                result = {
                    "reset": True,
                    "count": len(table_entries),
                    "table_ids": [tid for tid, _ in table_entries],
                }
            else:
                result = {
                    "count": len(table_entries),
                    "tables": [{"table_id": tid, "target_datastore": ds, "will_insert": "FULL_RELOAD"} for tid, ds in table_entries],
                }
        elif args.query_type == "TriggerOverview":
            escaped_trigger = sql_string_literal(args.trigger_name)
            overview_queries = [
                ("Orchestration", f"SELECT Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = '{escaped_trigger}' ORDER BY Order_Of_Operations, Table_ID;"),
                ("LastRunPerTable", (
                    f"SELECT o.Table_ID, o.Target_Entity, o.Target_Datastore, o.Ingestion_Active, "
                    f"l.Ingestion_Status AS Last_Status, l.Records_Processed AS Last_Records, "
                    f"l.Ingestion_End_Time AS Last_Run_Time, l.Watermark_Value AS Current_Watermark, "
                    f"DATEDIFF(SECOND, l.Ingestion_Start_Time, l.Ingestion_End_Time) AS Last_Duration_Seconds "
                    f"FROM dbo.Data_Pipeline_Metadata_Orchestration o "
                    f"LEFT JOIN ( SELECT Table_ID, Ingestion_Status, Records_Processed, Ingestion_Start_Time, Ingestion_End_Time, Watermark_Value, "
                    f"ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn "
                    f"FROM dbo.Data_Pipeline_Logs WHERE Processing_Phase = 'Batch' ) l "
                    f"ON o.Table_ID = l.Table_ID AND l.rn = 1 "
                    f"WHERE o.Trigger_Name = '{escaped_trigger}' "
                    f"ORDER BY o.Order_Of_Operations, o.Table_ID;"
                )),
            ]
            overview_result_sets = execute_sql_queries(
                resolution.metadata_warehouse_id,
                resolution.metadata_database_name,
                overview_queries,
            )
            result = {label: overview_result_sets[label].rows for label in overview_result_sets}
        else:
            sql = get_single_query_sql(args)
            if sql is None:
                raise AgentError(f"Unsupported query type: {args.query_type}")
            result = execute_sql_query(resolution.metadata_warehouse_id, sql).rows
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(result, indent=2, default=str))
    else:
        print(json.dumps(result, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
