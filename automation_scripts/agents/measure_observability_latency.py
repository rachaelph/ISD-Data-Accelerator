from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import execute_sql_queries, execute_sql_query, resolve_datastore_endpoint


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Measure live-query observability latency for one table.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--git-folder-name")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--table-id", type=int, required=True)
    parser.add_argument("--sample-size", type=int, default=10)
    parser.add_argument("--iterations", type=int, default=2)
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def build_queries(schema_name: str, table_name: str, sample_size: int) -> list[tuple[str, str]]:
    return [
        (
            "Schema",
            f"SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, ORDINAL_POSITION FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}' ORDER BY ORDINAL_POSITION",
        ),
        ("RowCount", f"SELECT COUNT(*) AS Total_Rows FROM [{schema_name}].[{table_name}]"),
        ("Sample", f"SELECT TOP {sample_size} * FROM [{schema_name}].[{table_name}]"),
    ]


def main() -> int:
    args = parse_args()

    total_start = time.perf_counter()
    resolution_start = time.perf_counter()
    resolution = resolve_datastore_endpoint(
        source_directory=args.source_directory,
        git_folder_name=args.git_folder_name,
        environment=args.environment,
        table_id=args.table_id,
        datastore_name=None,
    )
    resolution_seconds = time.perf_counter() - resolution_start

    if not resolution.target_entity or "." not in resolution.target_entity:
        raise ValueError(f"TargetEntity '{resolution.target_entity}' must be schema-qualified.")

    schema_name, table_name = resolution.target_entity.split(".", 1)
    queries = build_queries(schema_name, table_name, args.sample_size)

    single_query_timings: list[dict[str, float | str]] = []
    for iteration in range(1, args.iterations + 1):
        for label, sql in queries:
            start = time.perf_counter()
            execute_sql_query(resolution.endpoint, sql)
            single_query_timings.append(
                {"iteration": iteration, "queryType": label, "seconds": round(time.perf_counter() - start, 4)}
            )

    batch_timings: list[dict[str, float | int]] = []
    for iteration in range(1, args.iterations + 1):
        start = time.perf_counter()
        execute_sql_queries(resolution.endpoint, queries)
        batch_timings.append({"iteration": iteration, "seconds": round(time.perf_counter() - start, 4)})

    payload = {
        "gitFolderName": args.git_folder_name,
        "environment": resolution.environment,
        "tableId": args.table_id,
        "targetEntity": resolution.target_entity,
        "datastoreName": resolution.datastore_name,
        "resolutionSeconds": round(resolution_seconds, 4),
        "singleQueryTimings": single_query_timings,
        "batchTimings": batch_timings,
        "totalSeconds": round(time.perf_counter() - total_start, 4),
    }

    if args.pretty:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())