#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import AgentError, resolve_metadata_warehouse_from_datastore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve metadata warehouse connection details from datastore notebook SQL files.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = resolve_metadata_warehouse_from_datastore(
            source_directory=args.source_directory,
            engine_folder=args.engine_folder,
            environment=args.environment,
        )
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    payload = result.__dict__ | {
        "source_directory": str(result.source_directory),
        "git_folder_path": str(result.git_folder_path),
        "datastore_sql_path": str(result.datastore_sql_path),
    }
    if args.pretty:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
