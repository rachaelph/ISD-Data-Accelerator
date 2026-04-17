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
    execute_sql_query,
    resolve_datastore_endpoint,
    resolve_metadata_warehouse_from_datastore,
    resolve_table_id_from_metadata,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Resolve one table and return cached column profile plus live sample rows in one call."
    )
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--table-id", type=int)
    parser.add_argument("--table-name")
    parser.add_argument("--medallion-layer")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def get_profile_sql(table_id: int) -> str:
    return (
        "SELECT Table_Name, Column_Name, Data_Type, Total_Rows, Approx_Distinct_Values, Null_Count, "
        "Null_Percent, Mean, Std_Dev, [Min], [Max], Data_Profile_Execution_Time, Table_Last_Modified_Time "
        "FROM Exploratory_Data_Analysis_Results "
        f"WHERE Table_ID = {table_id} "
        "AND Data_Profile_Execution_Time = ("
        "SELECT MAX(Data_Profile_Execution_Time) "
        "FROM Exploratory_Data_Analysis_Results "
        f"WHERE Table_ID = {table_id}) "
        "ORDER BY Column_Name;"
    )


def get_sample_sql(target_entity: str, sample_size: int) -> str:
    schema_name, table_name = target_entity.split(".", 1)
    return f"SELECT TOP {sample_size} * FROM [{schema_name}].[{table_name}]"


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
        table_id = args.table_id
        if table_id is None:
            table_id = resolve_table_id_from_metadata(
                args.source_directory,
                args.engine_folder,
                args.table_name,
                args.medallion_layer,
            )

        metadata_resolution = resolve_metadata_warehouse_from_datastore(
            args.source_directory,
            args.engine_folder,
            args.environment,
        )
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

        profile_rows = execute_sql_query(
            metadata_resolution.metadata_warehouse_id,
            metadata_resolution.metadata_database_name,
            get_profile_sql(table_id),
        ).rows
        sample_rows = execute_sql_query(
            target_resolution.endpoint,
            target_resolution.datastore_name,
            get_sample_sql(target_resolution.target_entity, args.sample_size),
        ).rows
        payload = {
            "environment": args.environment,
            "tableId": table_id,
            "targetEntity": target_resolution.target_entity,
            "datastoreName": target_resolution.datastore_name,
            "datastoreType": target_resolution.datastore_type,
            "medallionLayer": target_resolution.medallion_layer,
            "sqlEndpoint": target_resolution.endpoint,
            "metadataWarehouse": metadata_resolution.metadata_database_name,
            "profileRows": profile_rows,
            "sampleRows": sample_rows,
            "notes": [
                "ProfileRows come from cached Exploratory_Data_Analysis_Results in the metadata warehouse.",
                "SampleRows come from the target SQL endpoint and show only the SQL-visible subset of columns.",
            ],
        }
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