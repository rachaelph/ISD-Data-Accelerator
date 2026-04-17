#!/usr/bin/env python3
"""Sync the JSON datastore config into the ``Datastore_Configuration`` Delta table.

Source of truth: ``databricks_batch_engine/datastores/datastore_<ENV>.json``
(plus any active ``overrides/<branch>.json``).

Target: ``{metadata_catalog}.{metadata_schema}.Datastore_Configuration`` on the
metadata SQL warehouse declared in the same config.

This agent is invoked automatically by ``commit_pipeline.py`` but can also be
run standalone (e.g. from ``/fdp-02-feature`` when bootstrapping a branch).
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow direct script execution to find utils/databricks_agent_utils.py
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "utils"))

from databricks_agent_utils import (
    AgentError,
    get_current_branch_name,
    load_datastore_config,
    sync_datastore_config_to_delta,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync datastore_<ENV>.json (+ branch overrides) into the Datastore_Configuration Delta table."
    )
    parser.add_argument("--source-directory", type=Path, default=Path("."))
    parser.add_argument("--engine-folder")
    parser.add_argument("--environment", required=True)
    parser.add_argument("--branch", help="Override auto-detected Git branch used for override lookup.")
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Also DELETE rows whose Datastore_Name is not in the current config.",
    )
    parser.add_argument(
        "--skip-workspace-check",
        action="store_true",
        help="Skip the active-workspace safety check (not recommended).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the MERGE SQL without executing.")
    parser.add_argument("--pretty", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        branch = args.branch if args.branch is not None else get_current_branch_name(args.source_directory)
        config = load_datastore_config(
            source_directory=args.source_directory,
            environment=args.environment,
            branch=branch,
        )

        if args.dry_run:
            # Local import to avoid exporting private helpers at module load.
            from databricks_agent_utils import (
                build_datastore_config_merge_sql,
                build_datastore_config_prune_sql,
                build_datastore_config_rows,
            )
            payload = {
                "environment": config.environment,
                "tableFullyQualifiedName": (
                    f"{config.metadata_catalog}.{config.metadata_schema}.Datastore_Configuration"
                ),
                "activeBranchOverride": config.active_branch_override,
                "overrideSourcePath": (
                    str(config.override_source_path) if config.override_source_path else None
                ),
                "rows": build_datastore_config_rows(config),
                "mergeSql": build_datastore_config_merge_sql(config),
                "pruneSql": build_datastore_config_prune_sql(config) if args.prune else None,
            }
        else:
            payload = sync_datastore_config_to_delta(
                config,
                prune=args.prune,
                verify_workspace=not args.skip_workspace_check,
            )
    except AgentError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(json.dumps(payload, separators=(",", ":"), default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
