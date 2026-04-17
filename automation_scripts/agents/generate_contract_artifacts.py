from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

METADATA_VALIDATION_DIR = REPO_ROOT / ".github" / "skills" / "metadata-validation"
if str(METADATA_VALIDATION_DIR) not in sys.path:
    sys.path.insert(0, str(METADATA_VALIDATION_DIR))

from query_capabilities import QUERY_CAPABILITIES, QUERY_CAPABILITIES_VERSION
from workflow_capabilities import (
    APPROVAL_BOUNDARIES,
    COMMAND_CAPABILITIES,
    PLANNER_RECIPES,
    STAGE_CAPABILITIES,
    WORKFLOW_CAPABILITIES_VERSION,
)


CONTRACTS_DIR = REPO_ROOT / ".github" / "contracts"
METADATA_SCHEMA_PATH = CONTRACTS_DIR / "metadata-schema.v1.json"
WORKFLOW_CAPABILITIES_PATH = CONTRACTS_DIR / "workflow-capabilities.v1.json"
QUERY_CAPABILITIES_PATH = CONTRACTS_DIR / "query-capabilities.v1.json"
VALIDATE_METADATA_SQL_PATH = METADATA_VALIDATION_DIR / "validate_metadata_sql.py"


def _load_metadata_validation_module():
    spec = importlib.util.spec_from_file_location(
        "fdp_validate_metadata_sql_runtime",
        VALIDATE_METADATA_SQL_PATH,
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load metadata validator from {VALIDATE_METADATA_SQL_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sorted_values(mapping: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in mapping.items():
        if isinstance(value, set):
            normalized[key] = sorted(value)
        elif isinstance(value, dict):
            normalized[key] = _sorted_values(value)
        else:
            normalized[key] = value
    return normalized


def build_metadata_schema_contract() -> dict[str, Any]:
    metadata_validator = _load_metadata_validation_module()
    return {
        "version": 1,
        "source_files": [
            ".github/skills/metadata-validation/validate_metadata_sql.py",
            "docs/METADATA_REFERENCE.md",
        ],
        "orchestration_config": {
            "processing_methods": list(metadata_validator.ORCHESTRATION_CONFIG["processing_methods"]),
            "target_datastores": list(metadata_validator.ORCHESTRATION_CONFIG["target_datastores"]),
        },
        "category_placement": {
            "primary_config_categories": sorted(metadata_validator.PRIMARY_CONFIG_CATEGORIES),
            "advanced_config_categories": sorted(metadata_validator.ADVANCED_CONFIG_CATEGORIES),
        },
        "valid_attributes": _sorted_values(metadata_validator.VALID_ATTRIBUTES),
        "transformation_attributes": _sorted_values(metadata_validator.TRANSFORMATION_ATTRIBUTES),
        "required_attributes": _sorted_values(metadata_validator.REQUIRED_ATTRIBUTES),
        "valid_values": _sorted_values(metadata_validator.VALID_VALUES),
        "required_configs_by_source": metadata_validator.REQUIRED_CONFIGS_BY_SOURCE,
        "boolean_configs": [
            {"category": category, "name": name}
            for category, name in metadata_validator.BOOLEAN_CONFIGS
        ],
        "guid_configs": [
            {"category": category, "name": name}
            for category, name in metadata_validator.GUID_CONFIGS
        ],
        "custom_function_signatures": _sorted_values(metadata_validator.custom_transformation_function_SIGNATURES),
        "valid_metadata_keys": _sorted_values(metadata_validator.VALID_METADATA_KEYS),
        "common_workspace_variable_keys": sorted(metadata_validator.COMMON_WORKSPACE_VARIABLE_KEYS),
        "databricks_standard_libraries": sorted(metadata_validator.DATABRICKS_STANDARD_LIBRARIES),
        "accelerator_template_repos": sorted(metadata_validator.ACCELERATOR_TEMPLATE_REPOS),
        "deletion_aware_merge_types": sorted(metadata_validator.DELETION_AWARE_MERGE_TYPES),
        "databricks_cluster_libraries_docs_url": metadata_validator.DATABRICKS_CLUSTER_LIBRARIES_DOCS_URL,
        "merge_types_requiring_delete_columns": [
            "merge_and_delete",
            "merge_mark_all_deleted",
            "merge_mark_unmatched_deleted",
        ],
        "merge_types_requiring_primary_keys": [
            "merge",
            "merge_and_delete",
            "merge_mark_all_deleted",
            "merge_mark_unmatched_deleted",
            "scd2",
        ],
        "processing_methods_requiring_datastore_name": [
            "execute_databricks_job",
            "execute_databricks_notebook",
        ],
        "valid_merge_types_by_layer": {
            "bronze": [
                "append",
                "overwrite",
            ],
        },
        "checks_without_row_level_actions": [
            "validate_batch_size",
            "validate_completeness",
            "validate_freshness",
        ],
    }


def build_workflow_capability_contract() -> dict[str, Any]:
    return {
        "version": WORKFLOW_CAPABILITIES_VERSION,
        "source_files": [
            ".github/prompts/fdp-00-end-to-end.prompt.md",
            ".github/prompts/fdp-03-author.prompt.md",
            ".github/prompts/fdp-03-convert.prompt.md",
            ".github/prompts/fdp-04-commit.prompt.md",
            ".github/prompts/fdp-05-run.prompt.md",
            ".github/prompts/fdp-06-investigate.prompt.md",
            ".github/prompts/fdp-06-troubleshoot.prompt.md",
        ],
        "stage_capabilities": STAGE_CAPABILITIES,
        "command_capabilities": COMMAND_CAPABILITIES,
        "planner_recipes": PLANNER_RECIPES,
        "approval_boundaries": APPROVAL_BOUNDARIES,
    }


def build_query_capability_contract() -> dict[str, Any]:
    return {
        "version": QUERY_CAPABILITIES_VERSION,
        "source_files": [
            ".github/skills/table-observability/SKILL.md",
            ".github/skills/table-observability/scripts/invoke_metadata_query.py",
        ],
        "script": ".github/skills/table-observability/scripts/invoke_metadata_query.py",
        "capabilities": QUERY_CAPABILITIES,
    }


def _serialize(contract: dict[str, Any]) -> str:
    return json.dumps(contract, indent=2) + "\n"


def _write_contract(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(_serialize(payload), encoding="utf-8")


def generate_contract_artifacts(write: bool = True) -> dict[str, dict[str, Any]]:
    metadata_schema_artifact = build_metadata_schema_contract()
    artifacts = {
        str(METADATA_SCHEMA_PATH.relative_to(REPO_ROOT)): metadata_schema_artifact,
        str(WORKFLOW_CAPABILITIES_PATH.relative_to(REPO_ROOT)): build_workflow_capability_contract(),
        str(QUERY_CAPABILITIES_PATH.relative_to(REPO_ROOT)): build_query_capability_contract(),
    }
    if write:
        CONTRACTS_DIR.mkdir(parents=True, exist_ok=True)
        for relative_path, payload in artifacts.items():
            _write_contract(REPO_ROOT / relative_path, payload)
    return artifacts


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate machine-readable metadata, workflow, and query contracts.")
    parser.add_argument("--check", action="store_true", help="Verify that the checked-in contract artifacts match generated output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifacts = generate_contract_artifacts(write=not args.check)
    if not args.check:
        print(json.dumps({"written": list(artifacts.keys())}, indent=2))
        return 0

    mismatches: list[str] = []
    for relative_path, payload in artifacts.items():
        artifact_path = REPO_ROOT / relative_path
        if not artifact_path.exists():
            mismatches.append(f"Missing artifact: {relative_path}")
            continue
        actual_text = artifact_path.read_text(encoding="utf-8")
        expected_text = _serialize(payload)
        if actual_text != expected_text:
            mismatches.append(f"Artifact out of date: {relative_path}")

    if mismatches:
        for mismatch in mismatches:
            print(mismatch, file=sys.stderr)
        return 1

    print(json.dumps({"checked": list(artifacts.keys())}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())