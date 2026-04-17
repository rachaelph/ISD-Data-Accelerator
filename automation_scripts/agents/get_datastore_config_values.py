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
    parser = argparse.ArgumentParser(
        description="Read required values from datastore_<ENV>.json (with branch overrides applied)."
    )
    parser.add_argument(
        "--source-directory",
        type=Path,
        default=Path("."),
        help="Repository root.",
    )
    parser.add_argument(
        "--environment",
        default="DEV",
        help="Environment name (selects datastore_<ENV>.json).",
    )
    parser.add_argument(
        "--git-folder-name",
        help="Optional git folder scope; unused for value resolution.",
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
        context = resolve_execution_context(
            source_directory=args.source_directory,
            git_folder_name=args.git_folder_name,
            required_variables=args.required_variables,
            environment=args.environment,
        )
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    payload = {
        "variables": {name: context["Variables"][name] for name in args.required_variables},
        "configPath": context["DatastoreConfigPath"],
    }

    if args.pretty:
        print(json.dumps(payload, indent=2))
    else:
        print(json.dumps(payload, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

