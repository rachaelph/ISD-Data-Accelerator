#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from automation_scripts.utils.databricks_agent_utils import AgentError, enforce_read_only_query, execute_sql_queries, execute_sql_query, resolve_datastore_endpoint, resolve_table_id_from_metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve a table's target endpoint and execute a read-only SQL query against it.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--table-id", type=int)
    parser.add_argument("--table-name")
    parser.add_argument("--medallion-layer")
    parser.add_argument("--datastore-name")
    parser.add_argument("--query-type", action="append", choices=["Schema", "RowCount", "Sample", "SampleOrdered"])
    parser.add_argument("--query")
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--order-by-column")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def sql_identifier(value: str) -> str:
    return value.replace("`", "``")


def qualified_table_name(schema_name: str, table_name: str) -> str:
    return f"`{sql_identifier(schema_name)}`.`{sql_identifier(table_name)}`"


def build_query(query_type: str, args: argparse.Namespace, schema_name: str, table_name: str) -> str:
    if query_type == "Schema":
        return f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION"
    if query_type == "RowCount":
        return f"SELECT COUNT(*) AS Total_Rows FROM {qualified_table_name(schema_name, table_name)}"
    if query_type == "Sample":
        return f"SELECT * FROM {qualified_table_name(schema_name, table_name)} LIMIT {args.sample_size}"
    if query_type == "SampleOrdered":
        return (
            f"SELECT * FROM {qualified_table_name(schema_name, table_name)} "
            f"ORDER BY `{sql_identifier(args.order_by_column)}` LIMIT {args.sample_size}"
        )
    assert args.query is not None
    return args.query.replace("{Schema}", schema_name).replace("{Table}", table_name)


def build_queries(args: argparse.Namespace, schema_name: str, table_name: str) -> list[tuple[str, str]]:
    if args.query:
        return [("CustomQuery", build_query("CustomQuery", args, schema_name, table_name))]

    assert args.query_type is not None
    query_types = args.query_type
    if len(query_types) != len(set(query_types)):
        raise AgentError("Duplicate --query-type values are not supported in a single call.")
    return [(query_type, build_query(query_type, args, schema_name, table_name)) for query_type in query_types]


def main() -> int:
    args = parse_args()
    if args.table_name and not args.medallion_layer:
        print("--medallion-layer is required when using --table-name.", file=sys.stderr)
        return 1
    if args.medallion_layer and not args.table_name:
        print("--table-name is required when using --medallion-layer.", file=sys.stderr)
        return 1
    if args.table_id is None and not args.datastore_name and not args.table_name:
        print("Provide either --table-id, --table-name, or --datastore-name.", file=sys.stderr)
        return 1
    if bool(args.query_type) == bool(args.query):
        print("Provide either --query-type or --query, but not both.", file=sys.stderr)
        return 1
    if args.query_type and "SampleOrdered" in args.query_type and not args.order_by_column:
        print("--order-by-column is required for SampleOrdered.", file=sys.stderr)
        return 1

    try:
        if args.table_id is None and args.table_name:
            args.table_id = resolve_table_id_from_metadata(
                args.source_directory,
                args.engine_folder,
                args.table_name,
                args.medallion_layer,
            )
        resolution = resolve_datastore_endpoint(
            source_directory=args.source_directory,
            engine_folder=args.engine_folder,
            environment=args.environment,
            table_id=args.table_id,
            datastore_name=args.datastore_name,
        )
        if not resolution.target_entity or "." not in resolution.target_entity:
            raise AgentError(
                f"TargetEntity '{resolution.target_entity}' must be schema-qualified (for example 'dbo.my_table')."
            )
        schema_name, table_name = resolution.target_entity.split(".", 1)
        queries = build_queries(args, schema_name, table_name)
        for _, sql in queries:
            enforce_read_only_query(sql)
        if len(queries) == 1:
            query_label, sql = queries[0]
            rows = execute_sql_query(
                resolution.endpoint,
                sql,
                catalog=resolution.datastore_name,
                schema=resolution.medallion_layer,
            ).rows
            payload = {
                "environment": resolution.environment,
                "datastoreName": resolution.datastore_name,
                "datastoreType": resolution.datastore_type,
                "endpoint": resolution.endpoint,
                "targetEntity": resolution.target_entity,
                "medallionLayer": resolution.medallion_layer,
                "queryType": query_label,
                "rows": rows,
            }
        else:
            results = execute_sql_queries(
                resolution.endpoint,
                queries,
                catalog=resolution.datastore_name,
                schema=resolution.medallion_layer,
            )
            payload = {
                "environment": resolution.environment,
                "datastoreName": resolution.datastore_name,
                "datastoreType": resolution.datastore_type,
                "endpoint": resolution.endpoint,
                "targetEntity": resolution.target_entity,
                "medallionLayer": resolution.medallion_layer,
                "queryTypes": [label for label, _ in queries],
                "results": {label: result.rows for label, result in results.items()},
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
