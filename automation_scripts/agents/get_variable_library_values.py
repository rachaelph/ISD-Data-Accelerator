#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import AgentError, get_required_variables


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read required values from the workspace configuration file."
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=Path("."),
        help="Repository root containing git folders.",
    )
    parser.add_argument(
        "--git-folder-name",
        required=True,
        help="Git folder containing workspace artifacts, for example 'dev'.",
    )
    parser.add_argument(
        "--required-variable",
        dest="required_variables",
        action="append",
        required=True,
        help="Variable name that must exist and have a non-empty value. Repeat for multiple variables.",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Print indented JSON instead of compact JSON.",
    )
    return parser.parse_args()

def main() -> int:
    args = parse_args()

    try:
        result = get_required_variables(
            source_directory=args.source_directory,
            git_folder_name=args.git_folder_name,
            required_variables=args.required_variables,
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
