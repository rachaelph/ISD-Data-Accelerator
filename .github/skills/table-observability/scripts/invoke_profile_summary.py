#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from automation_scripts.utils.databricks_agent_utils import (
    AgentError,
    execute_sql_queries,
    execute_sql_query,
    resolve_datastore_endpoint,
    resolve_metadata_warehouse_from_datastore,
    resolve_table_id_from_metadata,
)


NUMERIC_TYPES = {
    "bigint",
    "bit",
    "decimal",
    "float",
    "int",
    "money",
    "numeric",
    "real",
    "smallint",
    "smallmoney",
    "tinyint",
}

TEMPORAL_TYPES = {
    "date",
    "datetime",
    "datetime2",
    "datetimeoffset",
    "smalldatetime",
    "time",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Return cached profile data when available, otherwise generate live SQL-visible column statistics dynamically."
    )
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--table-id", type=int)
    parser.add_argument("--table-name")
    parser.add_argument("--medallion-layer")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def sql_string_literal(value: str) -> str:
    return value.replace("'", "''")


def sql_identifier(value: str) -> str:
    return value.replace("`", "``")


def get_cached_profile_sql(table_id: int) -> str:
    return (
        "SELECT Table_Name, Column_Name, Data_Type, Total_Rows, Approx_Distinct_Values, Null_Count, "
        "Null_Percent, Mean, Std_Dev, `Min`, `Max`, Data_Profile_Execution_Time, Table_Last_Modified_Time "
        "FROM Exploratory_Data_Analysis_Results "
        f"WHERE Table_ID = {table_id} "
        "AND Data_Profile_Execution_Time = ("
        "SELECT MAX(Data_Profile_Execution_Time) "
        "FROM Exploratory_Data_Analysis_Results "
        f"WHERE Table_ID = {table_id}) "
        "ORDER BY Column_Name;"
    )


def get_schema_sql(schema_name: str, table_name: str) -> str:
    escaped_schema = sql_string_literal(schema_name)
    escaped_table = sql_string_literal(table_name)
    return (
        "SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = '{escaped_schema}' AND TABLE_NAME = '{escaped_table}' "
        "ORDER BY ORDINAL_POSITION;"
    )


def build_live_profile_query(schema_name: str, table_name: str, column_name: str, data_type: str, qualified_table_name: str) -> str:
    escaped_column = sql_identifier(column_name)
    escaped_table_name = sql_string_literal(qualified_table_name)
    escaped_column_name = sql_string_literal(column_name)
    escaped_data_type = sql_string_literal(data_type)
    table_ref = f"`{sql_identifier(schema_name)}`.`{sql_identifier(table_name)}`"

    select_prefix = (
        f"SELECT '{escaped_table_name}' AS Table_Name, "
        f"'{escaped_column_name}' AS Column_Name, "
        f"'{escaped_data_type}' AS Data_Type, "
        "COUNT(*) AS Total_Rows, "
        f"COUNT(DISTINCT `{escaped_column}`) AS Approx_Distinct_Values, "
        f"COUNT(*) - COUNT(`{escaped_column}`) AS Null_Count, "
        f"CAST((COUNT(*) - COUNT(`{escaped_column}`)) * 1.0 / NULLIF(COUNT(*), 0) AS DECIMAL(18,4)) AS Null_Percent, "
    )

    normalized_type = data_type.lower()
    if normalized_type in NUMERIC_TYPES:
        summary_columns = (
            f"CAST(AVG(CAST(`{escaped_column}` AS DOUBLE)) AS DECIMAL(20,4)) AS Mean, "
            f"CAST(STDDEV_SAMP(CAST(`{escaped_column}` AS DOUBLE)) AS DECIMAL(20,4)) AS Std_Dev, "
            f"MIN(`{escaped_column}`) AS `Min`, "
            f"MAX(`{escaped_column}`) AS `Max`, "
        )
    elif normalized_type in TEMPORAL_TYPES:
        summary_columns = (
            "CAST(NULL AS DECIMAL(20,4)) AS Mean, "
            "CAST(NULL AS DECIMAL(20,4)) AS Std_Dev, "
            f"MIN(`{escaped_column}`) AS `Min`, "
            f"MAX(`{escaped_column}`) AS `Max`, "
        )
    else:
        summary_columns = (
            "CAST(NULL AS DECIMAL(20,4)) AS Mean, "
            "CAST(NULL AS DECIMAL(20,4)) AS Std_Dev, "
            "CAST(NULL AS STRING) AS `Min`, "
            "CAST(NULL AS STRING) AS `Max`, "
        )

    return (
        select_prefix
        + summary_columns
        + "current_timestamp() AS Data_Profile_Execution_Time, "
        + "CAST(NULL AS TIMESTAMP) AS Table_Last_Modified_Time "
        + f"FROM {table_ref}"
    )


def split_namespace(database_name: str) -> tuple[str | None, str | None]:
    parts = [part.strip() for part in database_name.split(".") if part.strip()]
    if len(parts) >= 2:
        return parts[0], parts[1]
    if len(parts) == 1:
        return None, parts[0]
    return None, None


def resolve_table_id(args: argparse.Namespace) -> int:
    if args.table_id is not None:
        return args.table_id
    return resolve_table_id_from_metadata(
        args.source_directory,
        args.engine_folder,
        args.table_name,
        args.medallion_layer,
    )


def get_live_profile_rows(args: argparse.Namespace, table_id: int) -> tuple[dict[str, object], list[dict[str, object]]]:
    target_resolution = resolve_datastore_endpoint(
        source_directory=args.source_directory,
        engine_folder=args.engine_folder,
        environment=args.environment,
        table_id=table_id,
        datastore_name=None,
    )
    if not target_resolution.target_entity or "." not in target_resolution.target_entity:
        raise AgentError(
            f"TargetEntity '{target_resolution.target_entity}' must be schema-qualified (for example 'dbo.my_table')."
        )

    schema_name, table_name = target_resolution.target_entity.split(".", 1)
    schema_rows = execute_sql_query(
        target_resolution.endpoint,
        get_schema_sql(schema_name, table_name),
        catalog=target_resolution.datastore_name,
        schema=target_resolution.medallion_layer,
    ).rows
    if not schema_rows:
        raise AgentError(
            f"No SQL-visible columns were found for target entity '{target_resolution.target_entity}'."
        )

    qualified_table_name = f"{target_resolution.datastore_name}.{target_resolution.target_entity}"
    queries: list[tuple[str, str]] = []
    for row in schema_rows:
        column_name = str(row["COLUMN_NAME"])
        data_type = str(row["DATA_TYPE"])
        queries.append(
            (
                column_name,
                build_live_profile_query(schema_name, table_name, column_name, data_type, qualified_table_name),
            )
        )

    query_results = execute_sql_queries(
        target_resolution.endpoint,
        queries,
        catalog=target_resolution.datastore_name,
        schema=target_resolution.medallion_layer,
    )
    ordered_rows: list[dict[str, object]] = []
    for row in schema_rows:
        column_name = str(row["COLUMN_NAME"])
        ordered_rows.extend(query_results[column_name].rows)

    metadata = {
        "targetEntity": target_resolution.target_entity,
        "datastoreName": target_resolution.datastore_name,
        "datastoreType": target_resolution.datastore_type,
        "medallionLayer": target_resolution.medallion_layer,
        "sqlEndpoint": target_resolution.endpoint,
    }
    return metadata, ordered_rows


def main() -> int:
    args = parse_args()
    if args.table_name and not args.medallion_layer:
        print("--medallion-layer is required when using --table-name.", file=sys.stderr)
        return 1
    if args.medallion_layer and not args.table_name:
        print("--table-name is required when using --medallion-layer.", file=sys.stderr)
        return 1
    if args.table_id is None and not args.table_name:
        print("Provide either --table-id or --table-name.", file=sys.stderr)
        return 1

    try:
        table_id = resolve_table_id(args)
        metadata_resolution = resolve_metadata_warehouse_from_datastore(
            args.source_directory,
            args.engine_folder,
            args.environment,
        )
        metadata_catalog, metadata_schema = split_namespace(metadata_resolution.metadata_database_name)
        cached_rows = execute_sql_query(
            metadata_resolution.metadata_warehouse_id,
            get_cached_profile_sql(table_id),
            catalog=metadata_catalog,
            schema=metadata_schema,
        ).rows

        payload: dict[str, object] = {
            "environment": args.environment,
            "tableId": table_id,
            "metadataWarehouse": metadata_resolution.metadata_database_name,
        }
        if cached_rows:
            payload["profileSource"] = "metadata-eda"
            payload["profileRows"] = cached_rows
            payload["notes"] = [
                "ProfileRows came from cached Exploratory_Data_Analysis_Results in the metadata warehouse.",
            ]
        else:
            live_metadata, live_rows = get_live_profile_rows(args, table_id)
            payload.update(live_metadata)
            payload["profileSource"] = "live-sql-fallback"
            payload["profileRows"] = live_rows
            payload["notes"] = [
                "Exploratory_Data_Analysis_Results was empty for this table, so profileRows were generated live from the SQL-visible schema.",
                "This fallback uses INFORMATION_SCHEMA.COLUMNS plus per-column summary queries against the target SQL endpoint.",
            ]
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(json.dumps(payload, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())