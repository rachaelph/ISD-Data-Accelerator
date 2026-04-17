#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import AgentError, resolve_execution_context


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve the shared execution context for local Databricks workflows.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--git-folder-name")
    parser.add_argument("--environment", default="DEV")
    parser.add_argument("--required-variable", dest="required_variables", action="append", default=[])
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    required_variables = args.required_variables or ["sql_warehouse_id"]
    try:
        result = resolve_execution_context(
            source_directory=args.source_directory,
            git_folder_name=args.git_folder_name,
            required_variables=required_variables,
            environment=args.environment,
        )
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(result, indent=2))
    else:
        print(json.dumps(result, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
