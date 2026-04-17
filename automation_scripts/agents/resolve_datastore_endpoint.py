#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import AgentError, resolve_datastore_endpoint, resolve_table_id_from_metadata


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve a table's target SQL endpoint from local datastore notebooks and metadata files.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--table-id", type=int)
    parser.add_argument("--table-name")
    parser.add_argument("--medallion-layer")
    parser.add_argument("--datastore-name")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.table_name and not args.medallion_layer:
        print("--medallion-layer is required when using --table-name.", file=sys.stderr)
        return 1
    if args.medallion_layer and not args.table_name:
        print("--table-name is required when using --medallion-layer.", file=sys.stderr)
        return 1

    try:
        table_id = args.table_id
        if table_id is None and args.table_name:
            table_id = resolve_table_id_from_metadata(
                args.source_directory,
                args.engine_folder,
                args.table_name,
                args.medallion_layer,
            )
        result = resolve_datastore_endpoint(
            source_directory=args.source_directory,
            engine_folder=args.engine_folder,
            environment=args.environment,
            table_id=table_id,
            datastore_name=args.datastore_name,
        )
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    payload = result.__dict__
    if args.pretty:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
