#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import AgentError, apply_variable_overrides


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Resolve workspace config values with branch overrides applied.")
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--git-folder-name", required=True)
    parser.add_argument("--required-variable", dest="required_variables", action="append", required=True)
    parser.add_argument("--branch", help="Override the auto-detected git branch used to pick an override file.")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        result = apply_variable_overrides(
            source_directory=args.source_directory,
            git_folder_name=args.git_folder_name,
            required_variables=args.required_variables,
            branch=args.branch,
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
