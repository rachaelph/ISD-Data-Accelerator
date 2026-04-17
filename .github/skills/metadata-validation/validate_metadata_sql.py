"""
Validate metadata SQL files against the Data Dictionary.

This script parses metadata SQL files and validates ALL rules from the Data Dictionary
without requiring PySpark or any database connection.

Validations performed:
1. Orchestration validations (Table_ID uniqueness, Processing_Method, Target_Datastore, etc.)
2. Category placement (Primary vs Advanced configuration)
3. All attribute names match the Data Dictionary
4. All configuration values are valid (merge_type, join_type, if_not_compliant, etc.)
5. Table_IDs exist in orchestration before being used in config
6. Instance numbers are sequential and properly ordered
7. Duplicate instance detection (same Configuration_Name + Instance_Number + Attribute within Table_ID)
8. Conflicting instance numbers (different Configuration_Names sharing same Instance_Number within Table_ID/Category)
9. Join conditions use proper a./b. prefixes
10. Required attributes are present for each configuration type
11. Cross-configuration consistency (merge types require certain configs, etc.)
12. GUID format validation
13. Boolean value validation
14. Three-part naming validation for table references
15. Count alignment for multi-value attributes (aggregate_data, entity_resolution, etc.)
16. Conditional required attributes (transform_datetime operations, etc.)
17. Cross-file Table_ID uniqueness (ensures Table_IDs are unique across all metadata notebooks in the same directory)
18. INSERT statement grouping (multiple INSERTs for same table should be combined if < 1000 rows)
19. INSERT statement size (warns when single INSERT exceeds 1000 rows)
20. Hardcoded Table_IDs in DELETE statements (must use Trigger_Name subquery)
21. Multi-line comments /* */ outside strings (must use only -- single-line comments)
22. Multi-line VALUES rows (each row must be on single line)
23. Watermark with merge_type='overwrite' conflict
24. SCD2 with merge_type='overwrite' conflict
25. source attribute with Processing_Method='batch' conflict
26. Invalid source='delta' value detection
27. DB config attributes without pipeline staging route
28. columns_to_rename count mismatch validation
29. DQ/transformation category intermixing within Table_ID
30. File section order (DELETE → Orchestration → Primary → Advanced)
31. DELETE statement order (Advanced → Primary → Orchestration)
32. Missing DELETE statements detection
33. new_type valid values for change_data_types (string, int, long, float, double, boolean, date, timestamp, decimal, binary)
34. transform_datetime conditional requirements (days for add_days/subtract_days, months for add_months, end_date_column for date_diff, date_format for format_date)
35. validate_range requires at least one of min_value OR max_value
36. validate_freshness/validate_completeness if_not_compliant limited to 'warn' or 'fail' only (not 'quarantine')
37. merge_type='scd2' requires source_timestamp_column_name
38. merge_type='replace_where' requires replace_where_column
39. exact_find/exact_replace count alignment in column_cleansing
40. xml_namespaces_keys/xml_namespaces_values count alignment
41. Consolidatable transformations - multiple steps with same settings should use comma-separated columns
42. Order_Of_Operations gap detection (values should be sequential without gaps)
43. Order_Of_Operations parallelization hint (warn if all values are unique - some rows may be able to run in parallel)
46. wildcard_folder_path should not start with 'Files/' (framework prepends this automatically)
44. Table_ID layer gap validation (at least 100 gap between bronze→silver and silver→gold)
45. Cross-file Target_Entity uniqueness (ensures Target_Datastore+Target_Entity is unique across all metadata notebooks)
47. Medallion Architecture Anti-Patterns:
    - Bronze with transformations (over-processing)
    - Gold sourcing from Bronze (should source from Silver)
    - Bronze with non-append/overwrite merge_type (Bronze should preserve raw data)
48. Custom Function Notebook Validation:
    - Verifies notebook file exists at expected path
    - Validates function exists in notebook
    - Checks function signature matches expected parameters (new_data/metadata/spark for transformations,
      metadata/spark for ingestion, file_paths/all_metadata/spark for file ingestion)
    - Ensures function has a return statement (must return DataFrame)
    - Validates return type annotation if present
    - Validates metadata dictionary access uses valid keys (orchestration_metadata, primary_config,
      advanced_config, workspace_variables, event_payload, function_config, watermark_filter, etc.)
    - Validates SQL workspace references use variables with backticks (not hardcoded names)
49. Non-Standard Library Import Validation:
51. Duplicate Transformation Detection:
    - Detects if the exact same transformation is defined twice on the same column within a Table_ID
    - Checks for duplicate (Configuration_Name, column_name, key attributes) combinations
    - Helps prevent redundant processing and potential data issues
52. Order_Of_Operations Dependency Validation:
    - Ensures tables that source from other tables have higher Order_Of_Operations values
    - Parses join_data.right_table_name, attach_dimension_surrogate_key.dimension_table_name,
      union_data.union_tables to detect dependencies
    - Warns when a table depends on another table with equal or higher Order_Of_Operations
53. Instance Number Sequential Validation:
    - Ensures instance numbers within each (Table_ID, Category) start at 1
    - Ensures instance numbers are sequential without gaps (1, 2, 3... not 1, 3, 5)
    - Helps maintain consistency and predictability in transformation ordering
54. Folder Path Validation:
    - staging_folder_path must not start with '/' (should be relative path)
    - wildcard_folder_path must not start with '/' (should be relative path)
    - sftp_wildcard_folder_path must not end with '/' (no trailing slash)
    - Prevents absolute path issues in catalog table file system
55. Merge Type Primary Keys Validation (enhanced):
    - Any merge_type containing 'merge' requires Primary_Keys in orchestration
    - Catches merge, merge_and_delete, merge_mark_unmatched_deleted, merge_mark_all_deleted
    - Checks all imports against Databricks Runtime standard libraries
    - Warns when imports require a Databricks cluster library configuration
    - Provides link to Environment documentation
50. Datastore Configuration Validation:
    - Verifies all datastores referenced in metadata have corresponding entries in Datastore_Configuration table
    - Checks the following references:
      - Target_Datastore (Orchestration)
      - datastore_name (source_details)
      - staging_catalog_name (source_details)
      - Lakehouse names from three-part table references:
        right_table_name, dimension_table_name, reference_table_name, union_tables
    - Looks for datastore config at: */datastores/datastore_<ENV>.Notebook/notebook-content.sql
    - Provides actionable fix instructions for missing datastores
56. Join Semantics Validation:
    - join_condition columns must be present in left_columns/right_columns
57. Column List Hygiene:
    - Detect duplicate column names in comma-delimited lists (e.g., group_by_columns, left_columns)
58. Target Naming Constraints:
    - Target_Entity schema/table parts may only contain letters, numbers, and underscores
59. Warehouse Write Mode Validation:
    - warehouse_write_mode only allowed when merge_type='warehouse_spark_connector'
60. DQ Numeric Range Validation:
    - validate_completeness.min_completeness_percent must be 0-100
    - validate_batch_size.min_rows must be > 0
    - validate_freshness.max_age must be > 0
61. Column Name Hygiene:
    - Warns if new columns (output_column_name, new_column_name) contain spaces or special characters
    - Promotes snake_case best practices
62. Regex Syntax Validation:
    - Validates Python regex syntax for regex_find
63. Window Function Requirements:
    - Functions like row_number, rank, lead, lag require order_by
    - Prevents runtime Spark exceptions
64. spark.read.table() Usage Detection:
    - Detects spark.read.table() calls in custom function notebooks
    - This API does not support cross-workspace table access
    - Must use spark.sql() with fully-qualified 4-part names including workspace
    - Example: spark.sql(f"SELECT * FROM `{workspace_name}`.lakehouse.schema.table")
65. Tier 1 First Guardrail:
    - Warns when data_transformation_steps are > 50% custom_transformation_function for a Table_ID
    - Reinforces: only the logic that truly requires custom code may use custom_transformation_function; all other steps must be Tier 1
66. CASE WHEN in derived_column Detection:
    - Warns when derived_column expression contains CASE WHEN
    - Recommends conditional_column instead, but allows derived_column when
      the expression returns column references or NULL (which conditional_column cannot express)
    - conditional_column uses structured conditions/values/default_value attributes
67. DataFrame .count() Usage Detection:
    - Errors when custom transformation/ingestion function uses .count() on DataFrames
    - Warns (instead of error) for custom staging functions, which need .count() for rows_copied
    - .count() forces full data scan and kills performance
    - Use df.isEmpty() for emptiness checks or avoid counting entirely
    - For iteration control, use while loop with isEmpty() instead
68. Customer Clone Detection:
    - Detects if repo is connected to mcaps-microsoft/ISD-Data-AI-Platform-Accelerator template
    - If detected, skips workspace variable validation (variables.json won't exist)
    - Warns that datastore names may not be 'bronze', 'silver', 'gold'
    - Prompts LLM to confirm actual datastore names with user
69. Platform File Existence:
    - Every .Notebook folder must have a .platform file (metadata AND custom function notebooks)
    - Errors if .platform is missing (required for Git integration)
70. Platform displayName Validation:
    - displayName must exactly match folder name (without .Notebook suffix)
    - Example: metadata_SalesData.Notebook → displayName: "metadata_SalesData"
71. Platform logicalId Format:
    - logicalId must be a valid lowercase GUID with hyphens (hex digits only: 0-9, a-f)
    - Format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
    - Non-hex characters (e.g., 'g', 'h', 'z') are invalid
    - Validated for both metadata notebooks and custom function notebooks
72. Platform logicalId Uniqueness:
    - logicalId must be unique across all .Notebook folders in the workspace
    - Scans all .Notebook/.platform files to detect duplicates
73. Warehouse Reference Validation:
    - Validates default_warehouse in notebook META header matches metadata_warehouse.Warehouse/.platform logicalId
    - Validates known_warehouses[].id matches the same logicalId
    - Ensures metadata notebooks execute against the correct warehouse
74. Databricks notebook Format Validation (Strict JSON Structure):
    - File must start with '-- Databricks notebook source'
    - Header META block must have EXACT structure with no extra keys:
      - kernel_info: { "name": "sqldatawarehouse" } (no extra keys)
      - dependencies: { "warehouse": { ... } } (no extra keys)
      - dependencies.warehouse: { "default_warehouse": "<guid>", "known_warehouses": [...] } (no extra keys)
      - known_warehouses entries: { "id": "<guid>", "type": "Datawarehouse" } (no extra keys)
    - Must have '-- CELL ********************' marker before SQL content
    - Footer META block must have EXACT structure with no extra keys:
      - { "language": "sql", "language_group": "sqldatawarehouse" }
    - All GUIDs validated for proper format (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)
75. Python Notebook Lakehouse Header Validation:
    - Custom function notebooks (.py) must have correct META header with kernel_info.name = "synapse_pyspark"
    - Lakehouse dependency is optional — "dependencies": {} is valid (populated at deployment time)
    - If lakehouse dependency is present, validates it has default_lakehouse_name and default_lakehouse_workspace_id keys
    - Empty string values are acceptable (populated at deployment time)
76. Direct Logging/Print Prohibition:
    - Custom function notebooks must NOT use logger.info(), logger.warn(), logger.error(), logger.warning(), or print() directly
    - Must use log_and_print(message, level) from NB_Helper_Functions_1 instead
    - log_and_print handles both logging and printing with proper formatting
    - Catches: logger.info(), logger.warn(), logger.error(), logger.warning(), print()
77. Duplicate Datastore_Name Validation:
    - Detects duplicate Datastore_Name entries within a datastore configuration notebook
    - The accelerator resolves datastores by name only, so duplicates cause ambiguous resolution
    - Errors for each name that appears more than once, with occurrence count
78. Datastore-Based Resolution for execute_databricks_notebook/execute_databricks_job:
    - When Processing_Method is execute_databricks_notebook or execute_databricks_job, the table must have
      source_details.datastore_name referencing a Notebook or Dataflow entry in Datastore_Configuration
    - The Datastore_Configuration entry stores the item GUID (Datastore_ID) and workspace GUID (Workspace_ID)
      per environment, so no raw GUIDs or parameter.yml tokens are needed in metadata SQL
    - Errors if execute_databricks_* table has item_id directly (must use datastore_name instead)
    - Errors if execute_databricks_* table is missing datastore_name
    - The datastore validation (Rule 50) ensures the referenced datastore_name exists in config
79. Reserved Colon Character Validation:
    - Target_Datastore and Target_Entity (Orchestration) must not contain ':' characters
    - datastore_name and staging_catalog_name (Primary Config) must not contain ':' characters
    - Colons are reserved as delimiters in Get_Pipeline_Tables STRING_AGG output
    - A colon in any value breaks @split(item(), ':') parsing in PL_01b pipeline
80. Custom Ingestion Function + table_name Conflict:
    - custom_table_ingestion_function, custom_file_ingestion_function, or custom_source_function cannot coexist with table_name in source_details
    - In parse_source_configuration(), custom ingestion functions route to Branch 2 which skips table_name parsing entirely
    - table_name is only read in Branch 3 (standard table-based ingestion) so it becomes dead/ignored metadata
    - Detects silent misconfiguration where user expects table_name to influence the custom function
81. CDF Overrides Soft Delete Columns:
    - use_change_data_feed=true cannot coexist with user-set column_to_mark_source_data_deletion or delete_rows_with_value
    - In parse_watermark_configuration() (NB_Helper_Functions_3), CDF hardcodes _change_type/delete, discarding user values
    - User's custom soft delete column name and value are silently overwritten — no error at runtime
82. CDF Overrides Watermark Column:
    - use_change_data_feed=true cannot coexist with watermark column_name or data_type in watermark_details
    - In parse_watermark_configuration(), CDF clears watermark_column_name and data_type to empty strings
    - CDF uses _commit_version internally; user's watermark config becomes dead metadata
83. Staging Folder Path Overrides Watermark Column:
    - staging_folder_path cannot coexist with watermark column_name or data_type
    - In parse_watermark_configuration(), staging clears watermark column settings to empty strings
    - Staging uses folder timestamps for watermarking; column-based watermark is silently discarded
84. Staging Folder Path Overrides Wildcard Folder Path:
    - staging_folder_path and wildcard_folder_path cannot both be set
    - source_path = staging_folder_path or wildcard_folder_path — staging wins by Python or short-circuit
    - wildcard_folder_path is completely ignored; file pattern matching never occurs
85. Staging Folder Path Disables Custom Table Ingestion Function:
    - staging_folder_path cannot coexist with custom_table_ingestion_function
    - route_to_ingestion_method() checks using_source_folder_path first, routing to ingest_raw_files()
    - custom_table_ingestion_function is only checked in ingest_from_table() (the else branch)
    - The custom table function is dead metadata — never instantiated or invoked
86. replace_where_column Without merge_type='replace_where' (Reverse of Rule 38):
    - replace_where_column is dead metadata unless merge_type='replace_where'
    - merge_data() only passes replace_where_column to _handle_replace_where() when merge_type matches
    - All other merge branches ignore it entirely
87. Soft Delete Config With Explicit merge_type='merge':
    - column_to_mark_source_data_deletion + delete_rows_with_value with explicit merge_type='merge'
    - parse_watermark_configuration() defaults to merge_and_delete when delete_rows_with_value is set
    - But explicit merge_type='merge' overrides the default, and _handle_merge() ignores soft delete columns
    - Soft deletes from source are upserted as normal rows instead of being tracked
88. Both compute_statistics_on_columns and compute_statistics_on_first_n_columns:
    - Cannot set both; compute_statistics_on_columns takes precedence
    - build_statistics_columns_config() checks specific columns first (if branch), silently skipping the first-N elif
    - compute_statistics_on_first_n_columns becomes dead metadata
89. if_duplicate_primary_keys Without Primary_Keys:
    - if_duplicate_primary_keys requires non-empty Primary_Keys in orchestration
    - _validate_primary_keys() wraps all logic in 'if primary_keys:' — entire validation is skipped when empty
    - Duplicate checking never occurs; the config is dead metadata

Usage:
    python validate_metadata_sql.py [path_to_sql_file]
    python validate_metadata_sql.py                           # validates all metadata notebooks in auto-discovered metadata folder
    python validate_metadata_sql.py --base-dir "MyWorkspace"  # validates all metadata notebooks in MyWorkspace/metadata/

Author: Generated for Databricks Data Platform Accelerator
"""

import re
import sys
import ast
import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Optional, Dict, Set, List, Tuple
from collections import defaultdict, Counter


# =============================================================================
# DATA DICTIONARY DEFINITIONS
#
# The metadata schema artifact in .github/contracts/metadata-schema.v1.json is
# the runtime source of truth for declarative metadata policy.
# =============================================================================

METADATA_SCHEMA_CONTRACT_PATH_ENV_VAR = "FDP_METADATA_SCHEMA_CONTRACT_PATH"


def _resolve_contract_path(env_var: str, default_path: Path) -> Path:
    override = os.environ.get(env_var)
    return Path(override).expanduser() if override else default_path


def _load_contract_document(path: Path, description: str) -> dict:
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Failed to parse {description} '{path}': {exc}") from exc


METADATA_SCHEMA_CONTRACT_PATH = _resolve_contract_path(
    METADATA_SCHEMA_CONTRACT_PATH_ENV_VAR,
    Path(__file__).resolve().parents[2] / "contracts" / "metadata-schema.v1.json",
)
METADATA_CONTRACT_SLICE_PATH = METADATA_SCHEMA_CONTRACT_PATH

def _require_string_list(section_name: str, values, *, allow_empty: bool = False) -> list[str]:
    if not isinstance(values, list) or (not allow_empty and not values) or not all(isinstance(value, str) and value for value in values):
        requirement = 'a list of strings' if allow_empty else 'a non-empty list of strings'
        raise ValueError(f"metadata contract section '{section_name}' must be {requirement}.")
    return values


def _require_string_value(section_name: str, value) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"metadata contract section '{section_name}' must be a non-empty string.")
    return value


def _require_mapping_of_string_lists(section_name: str, mapping, *, allow_empty_values: bool = False) -> dict[str, list[str]]:
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError(f"metadata contract section '{section_name}' must be a non-empty object.")

    normalized: dict[str, list[str]] = {}
    for key, values in mapping.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"metadata contract section '{section_name}' contains an invalid key: {key!r}")
        normalized[key] = _require_string_list(f"{section_name}.{key}", values, allow_empty=allow_empty_values)
    return normalized


def _require_named_pair_list(section_name: str, values) -> list[dict[str, str]]:
    if not isinstance(values, list):
        raise ValueError(f"metadata contract section '{section_name}' must be a list of objects.")

    normalized: list[dict[str, str]] = []
    for entry in values:
        if not isinstance(entry, dict):
            raise ValueError(f"metadata contract section '{section_name}' contains a non-object entry.")

        category = entry.get('category')
        name = entry.get('name')
        if not isinstance(category, str) or not category or not isinstance(name, str) or not name:
            raise ValueError(
                f"metadata contract section '{section_name}' entries must define non-empty 'category' and 'name' strings."
            )

        normalized.append({"category": category, "name": name})

    return normalized


def _require_signature_mapping(section_name: str, mapping) -> dict[str, dict[str, Any]]:
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError(f"metadata contract section '{section_name}' must be a non-empty object.")

    normalized: dict[str, dict[str, Any]] = {}
    for key, value in mapping.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"metadata contract section '{section_name}' contains an invalid key: {key!r}")
        if not isinstance(value, dict):
            raise ValueError(f"metadata contract section '{section_name}.{key}' must be an object.")

        normalized[key] = {
            'params': _require_string_list(f"{section_name}.{key}.params", value.get('params')),
            'required_params': _require_string_list(f"{section_name}.{key}.required_params", value.get('required_params')),
            'return_type': _require_string_value(f"{section_name}.{key}.return_type", value.get('return_type')),
            'description': _require_string_value(f"{section_name}.{key}.description", value.get('description')),
            'metadata_param': _require_string_value(f"{section_name}.{key}.metadata_param", value.get('metadata_param')),
        }

    return normalized


def _load_set_mapping(contract: dict, section_name: str, *, allow_empty_values: bool = False) -> dict[str, Set[str]]:
    return {
        key: set(values)
        for key, values in _require_mapping_of_string_lists(
            section_name,
            contract.get(section_name),
            allow_empty_values=allow_empty_values,
        ).items()
    }


def _load_list_mapping(contract: dict, section_name: str) -> dict[str, list[str]]:
    return {
        key: list(values)
        for key, values in _require_mapping_of_string_lists(section_name, contract.get(section_name)).items()
    }


def _load_named_pairs(contract: dict, section_name: str) -> list[tuple[str, str]]:
    return [
        (entry['category'], entry['name'])
        for entry in _require_named_pair_list(section_name, contract.get(section_name))
    ]


def _load_string_set(contract: dict, section_name: str) -> Set[str]:
    return set(_require_string_list(section_name, contract.get(section_name)))


def _load_signature_mapping(contract: dict, section_name: str) -> dict[str, dict[str, Any]]:
    return {
        key: {
            'params': list(value['params']),
            'required_params': list(value['required_params']),
            'return_type': value['return_type'],
            'description': value['description'],
            'metadata_param': value['metadata_param'],
        }
        for key, value in _require_signature_mapping(section_name, contract.get(section_name)).items()
    }


def load_metadata_schema_contract(
    path: Path = METADATA_SCHEMA_CONTRACT_PATH,
) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"metadata schema contract not found: {path}")

    contract = _load_contract_document(path, 'metadata schema contract')

    orchestration_config = contract.get('orchestration_config')
    if not isinstance(orchestration_config, dict):
        raise ValueError("metadata contract slice is missing 'orchestration_config'.")

    processing_methods = _require_string_list(
        'orchestration_config.processing_methods',
        orchestration_config.get('processing_methods'),
    )
    target_datastores = _require_string_list(
        'orchestration_config.target_datastores',
        orchestration_config.get('target_datastores'),
    )
    category_placement = contract.get('category_placement')
    if not isinstance(category_placement, dict):
        raise ValueError("metadata contract slice is missing 'category_placement'.")

    primary_categories = _require_string_list(
        'category_placement.primary_config_categories',
        category_placement.get('primary_config_categories'),
    )
    advanced_categories = _require_string_list(
        'category_placement.advanced_config_categories',
        category_placement.get('advanced_config_categories'),
    )

    overlap = set(primary_categories) & set(advanced_categories)
    if overlap:
        overlap_display = ', '.join(sorted(overlap))
        raise ValueError(
            "metadata contract slice defines overlapping primary/advanced categories: "
            f"{overlap_display}"
        )

    _require_mapping_of_string_lists('valid_attributes', contract.get('valid_attributes'))
    _require_mapping_of_string_lists('transformation_attributes', contract.get('transformation_attributes'))
    _require_mapping_of_string_lists('required_attributes', contract.get('required_attributes'))
    _require_mapping_of_string_lists('valid_values', contract.get('valid_values'))
    _require_mapping_of_string_lists('required_configs_by_source', contract.get('required_configs_by_source'))
    _require_named_pair_list('boolean_configs', contract.get('boolean_configs'))
    _require_named_pair_list('guid_configs', contract.get('guid_configs'))
    _require_signature_mapping('custom_function_signatures', contract.get('custom_function_signatures'))
    _require_mapping_of_string_lists('valid_metadata_keys', contract.get('valid_metadata_keys'), allow_empty_values=True)
    _require_string_list('common_workspace_variable_keys', contract.get('common_workspace_variable_keys'))
    _require_string_list('databricks_standard_libraries', contract.get('databricks_standard_libraries'))
    _require_string_list('accelerator_template_repos', contract.get('accelerator_template_repos'))
    _require_string_list('deletion_aware_merge_types', contract.get('deletion_aware_merge_types'))
    _require_string_value('databricks_cluster_libraries_docs_url', contract.get('databricks_cluster_libraries_docs_url'))
    merge_types_requiring_delete_columns = _require_string_list(
        'merge_types_requiring_delete_columns',
        contract.get('merge_types_requiring_delete_columns'),
    )
    merge_types_requiring_primary_keys = _require_string_list(
        'merge_types_requiring_primary_keys',
        contract.get('merge_types_requiring_primary_keys'),
    )
    processing_methods_requiring_datastore_name = _require_string_list(
        'processing_methods_requiring_datastore_name',
        contract.get('processing_methods_requiring_datastore_name'),
    )
    valid_merge_types_by_layer = _require_mapping_of_string_lists(
        'valid_merge_types_by_layer',
        contract.get('valid_merge_types_by_layer'),
    )
    _require_string_list(
        'checks_without_row_level_actions',
        contract.get('checks_without_row_level_actions'),
    )

    valid_merge_types = set(contract['valid_values'].get('merge_type', []))
    unknown_datastore_name_methods = set(processing_methods_requiring_datastore_name) - set(processing_methods)
    if unknown_datastore_name_methods:
        raise ValueError(
            "metadata contract slice defines processing_methods_requiring_datastore_name outside orchestration_config.processing_methods: "
            f"{', '.join(sorted(unknown_datastore_name_methods))}"
        )

    for section_name, merge_types in (
        ('merge_types_requiring_delete_columns', merge_types_requiring_delete_columns),
        ('merge_types_requiring_primary_keys', merge_types_requiring_primary_keys),
    ):
        unknown_merge_types = set(merge_types) - valid_merge_types
        if unknown_merge_types:
            raise ValueError(
                f"metadata contract section '{section_name}' references unknown merge_type values: "
                f"{', '.join(sorted(unknown_merge_types))}"
            )

    unknown_layer_names = set(valid_merge_types_by_layer) - set(target_datastores)
    if unknown_layer_names:
        raise ValueError(
            "metadata contract section 'valid_merge_types_by_layer' defines unknown target_datastores: "
            f"{', '.join(sorted(unknown_layer_names))}"
        )

    for layer_name, merge_types in valid_merge_types_by_layer.items():
        unknown_merge_types = set(merge_types) - valid_merge_types
        if unknown_merge_types:
            raise ValueError(
                "metadata contract section 'valid_merge_types_by_layer' references unknown merge_type values for "
                f"layer '{layer_name}': {', '.join(sorted(unknown_merge_types))}"
            )

    return contract


def load_metadata_contract_slice(
    path: Path = METADATA_CONTRACT_SLICE_PATH,
) -> dict:
    return load_metadata_schema_contract(path)


METADATA_CONTRACT_SLICE = load_metadata_contract_slice()

ORCHESTRATION_CONFIG = {
    "processing_methods": list(METADATA_CONTRACT_SLICE['orchestration_config']['processing_methods']),
    "target_datastores": list(METADATA_CONTRACT_SLICE['orchestration_config']['target_datastores']),
}

PRIMARY_CONFIG_CATEGORIES = set(
    METADATA_CONTRACT_SLICE['category_placement']['primary_config_categories']
)

ADVANCED_CONFIG_CATEGORIES = set(
    METADATA_CONTRACT_SLICE['category_placement']['advanced_config_categories']
)

VALID_ATTRIBUTES = _load_set_mapping(METADATA_CONTRACT_SLICE, 'valid_attributes')

TRANSFORMATION_ATTRIBUTES = _load_set_mapping(METADATA_CONTRACT_SLICE, 'transformation_attributes')

VALID_VALUES = _load_set_mapping(METADATA_CONTRACT_SLICE, 'valid_values')

VALID_NEW_TYPE_BASE_TYPES = {value.lower() for value in VALID_VALUES.get('new_type', set())}

REQUIRED_ATTRIBUTES = _load_set_mapping(METADATA_CONTRACT_SLICE, 'required_attributes')

REQUIRED_CONFIGS_BY_SOURCE = _load_list_mapping(METADATA_CONTRACT_SLICE, 'required_configs_by_source')

BOOLEAN_CONFIGS = _load_named_pairs(METADATA_CONTRACT_SLICE, 'boolean_configs')

GUID_CONFIGS = _load_named_pairs(METADATA_CONTRACT_SLICE, 'guid_configs')

custom_transformation_function_SIGNATURES = _load_signature_mapping(METADATA_CONTRACT_SLICE, 'custom_function_signatures')

VALID_METADATA_KEYS = _load_set_mapping(METADATA_CONTRACT_SLICE, 'valid_metadata_keys', allow_empty_values=True)

COMMON_WORKSPACE_VARIABLE_KEYS = _load_string_set(METADATA_CONTRACT_SLICE, 'common_workspace_variable_keys')

DATABRICKS_STANDARD_LIBRARIES = _load_string_set(METADATA_CONTRACT_SLICE, 'databricks_standard_libraries')

ACCELERATOR_TEMPLATE_REPOS = _load_string_set(METADATA_CONTRACT_SLICE, 'accelerator_template_repos')

DELETION_AWARE_MERGE_TYPES = _load_string_set(METADATA_CONTRACT_SLICE, 'deletion_aware_merge_types')

DATABRICKS_CLUSTER_LIBRARIES_DOCS_URL = _require_string_value(
    'databricks_cluster_libraries_docs_url',
    METADATA_CONTRACT_SLICE.get('databricks_cluster_libraries_docs_url'),
)

MERGE_TYPES_REQUIRING_DELETE_COLUMNS = _load_string_set(
    METADATA_CONTRACT_SLICE,
    'merge_types_requiring_delete_columns',
)

MERGE_TYPES_REQUIRING_PRIMARY_KEYS = _load_string_set(
    METADATA_CONTRACT_SLICE,
    'merge_types_requiring_primary_keys',
)

PROCESSING_METHODS_REQUIRING_DATASTORE_NAME = _load_string_set(
    METADATA_CONTRACT_SLICE,
    'processing_methods_requiring_datastore_name',
)

VALID_MERGE_TYPES_BY_LAYER = _load_set_mapping(
    METADATA_CONTRACT_SLICE,
    'valid_merge_types_by_layer',
)

CHECKS_WITHOUT_ROW_LEVEL_ACTIONS = _load_string_set(
    METADATA_CONTRACT_SLICE,
    'checks_without_row_level_actions',
)


# =============================================================================
# VALIDATION RESULT CLASSES
# =============================================================================

@dataclass
class ValidationIssue:
    severity: str
    category: str
    message: str
    table_id: Optional[int] = None
    line_number: Optional[int] = None
    suggestion: Optional[str] = None


@dataclass
class ValidationResult:
    file_path: str
    issues: list = field(default_factory=list)

    def add_issue(self, severity: str, category: str, message: str,
                  table_id: int = None, line_number: int = None, suggestion: str = None):
        self.issues.append(ValidationIssue(
            severity=severity, category=category, message=message,
            table_id=table_id, line_number=line_number, suggestion=suggestion
        ))

    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == 'error')

    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == 'warning')


# =============================================================================
# SQL PARSING
# =============================================================================

def find_insert_sections(content: str) -> dict:
    sections = {}
    lines = content.split('\n')
    current_section = None
    section_start = 0

    for i, line in enumerate(lines, 1):
        match = re.search(r'INSERT INTO\s+(?:\[?dbo\]?\.)?\[?(Data_Pipeline_\w+)\]?', line, re.IGNORECASE)
        if match:
            if current_section:
                sections[current_section] = {'start': section_start, 'end': i - 1, 'lines': lines[section_start-1:i-1]}

            table_name = match.group(1).lower()
            if 'orchestration' in table_name:
                current_section = 'orchestration'
            elif 'primary' in table_name:
                current_section = 'primary_config'
            elif 'advanced' in table_name:
                current_section = 'advanced_config'
            else:
                current_section = None
            section_start = i

    if current_section:
        sections[current_section] = {'start': section_start, 'end': len(lines), 'lines': lines[section_start-1:]}

    return sections


def parse_orchestration_rows(section_lines: list, start_line: int) -> list:
    rows = []
    # Pattern handles SQL escaped quotes ('') inside string values
    pattern = r"\(\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*,\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*\)"

    def _extract_sql_line_comment(sql_line: str) -> str:
        """Extract '--' line comment outside of quoted strings."""
        in_quote = False
        i = 0
        while i < len(sql_line) - 1:
            ch = sql_line[i]
            nxt = sql_line[i + 1]

            if ch == "'":
                # Handle SQL escaped quote ('')
                if in_quote and nxt == "'":
                    i += 2
                    continue
                in_quote = not in_quote
                i += 1
                continue

            if not in_quote and ch == '-' and nxt == '-':
                return sql_line[i + 2:].strip()

            i += 1

        return ""

    def _extract_target_exception_reason(sql_line: str) -> Optional[str]:
        """Extract reason from row-level comment: '-- exception: reason'."""
        comment = _extract_sql_line_comment(sql_line)
        if not comment:
            return None

        match = re.search(r'^\s*exception\s*:\s*(.+?)\s*$', comment, re.IGNORECASE)
        if match:
            reason = match.group(1).strip()
            return reason if reason else None
        return None

    for i, line in enumerate(section_lines):
        line_num = start_line + i
        exception_reason = _extract_target_exception_reason(line)
        for match in re.finditer(pattern, line):
            rows.append({
                'trigger_name': match.group(1),
                'order': int(match.group(2)),
                'table_id': int(match.group(3)),
                'target_datastore': match.group(4),
                'target_entity': match.group(5),
                'primary_keys': match.group(6),
                'processing_method': match.group(7),
                'ingestion_active': int(match.group(8)),
                'line_number': line_num,
                'target_entity_exception_reason': exception_reason,
            })
    return rows


def parse_primary_config_rows(section_lines: list, start_line: int) -> list:
    rows = []
    # Pattern handles SQL escaped quotes ('') inside string values
    pattern = r"\(\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*\)"

    for i, line in enumerate(section_lines):
        line_num = start_line + i
        if line.strip().startswith('--') or not line.strip():
            continue
        for match in re.finditer(pattern, line):
            rows.append({
                'table_id': int(match.group(1)),
                'category': match.group(2),
                'name': match.group(3),
                'value': match.group(4),
                'line_number': line_num,
            })
    return rows


def parse_advanced_config_rows(section_lines: list, start_line: int) -> list:
    rows = []
    # Pattern handles SQL escaped quotes ('') inside string values
    pattern = r"\(\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')+)'\s*,\s*(\d+)\s*,\s*'((?:[^']|'')+)'\s*,\s*'((?:[^']|'')*)'\s*\)"

    for i, line in enumerate(section_lines):
        line_num = start_line + i
        if line.strip().startswith('--') or not line.strip():
            continue
        for match in re.finditer(pattern, line):
            rows.append({
                'table_id': int(match.group(1)),
                'category': match.group(2),
                'name': match.group(3),
                'instance': int(match.group(4)),
                'attribute': match.group(5),
                'value': match.group(6),
                'line_number': line_num,
            })
    return rows


# =============================================================================
# SQL SYNTAX VALIDATIONS
# =============================================================================

def validate_sql_syntax(result: ValidationResult, content: str):
    """Check for common SQL syntax errors like missing commas between VALUES rows."""
    lines = content.split('\n')
    in_values_block = False

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        # Detect start of VALUES block
        if re.search(r'\bVALUES\b', stripped, re.IGNORECASE):
            in_values_block = True
            continue

        # Skip empty lines and comments
        if not stripped or stripped.startswith('--'):
            continue

        # Detect end of INSERT (next INSERT or end of file)
        if re.search(r'\bINSERT\b', stripped, re.IGNORECASE):
            in_values_block = False
            continue

        if in_values_block:
            # Check if line starts with ( but previous non-empty, non-comment line doesn't end with comma
            if stripped.startswith('('):
                # Look back for previous VALUES row
                for j in range(i - 2, -1, -1):
                    prev_line = lines[j].strip()
                    if not prev_line or prev_line.startswith('--'):
                        continue
                    if prev_line.upper().startswith('VALUES'):
                        break  # First row after VALUES - no comma needed
                    if prev_line.endswith(')'):
                        # Previous row ends with ) but no comma - syntax error
                        result.add_issue('error', 'sql_syntax', f"Missing comma after VALUES row - line {j+1} ends with ')' but next row starts on line {i}", line_number=j+1)
                    break  # Only check immediate previous row


def validate_insert_statement_grouping(result: ValidationResult, content: str):
    """
    Validate that INSERT statements are properly grouped:
    1. Multiple INSERT statements for the same table should be combined if total rows < 1000
    2. Single INSERT statements should not exceed 1000 rows
    """
    lines = content.split('\n')
    
    # Track INSERT statements by target table
    # Key: table name (orchestration, primary_config, advanced_config)
    # Value: list of (line_number, row_count) tuples
    insert_statements: Dict[str, list] = defaultdict(list)
    
    current_table = None
    current_insert_line = None
    current_row_count = 0
    in_values_block = False
    
    for i, line in enumerate(lines, 1):
        stripped = line.strip()
        
        # Detect INSERT INTO statement
        match = re.search(r'INSERT INTO (?:\[?dbo\]?\.)?\[?(Data_Pipeline_\w+|Datastore_Configuration)\]?', stripped, re.IGNORECASE)
        if match:
            # Save previous INSERT if exists
            if current_table and current_insert_line:
                insert_statements[current_table].append((current_insert_line, current_row_count))
            
            # Start tracking new INSERT
            table_name = match.group(1).lower()
            if 'orchestration' in table_name:
                current_table = 'orchestration'
            elif 'primary' in table_name:
                current_table = 'primary_config'
            elif 'advanced' in table_name:
                current_table = 'advanced_config'
            elif 'datastore_configuration' in table_name:
                current_table = 'datastore_configuration'
            else:
                current_table = None
            
            current_insert_line = i
            current_row_count = 0
            in_values_block = False
            continue
        
        # Detect VALUES keyword
        if re.search(r'\bVALUES\b', stripped, re.IGNORECASE):
            in_values_block = True
            continue
        
        # Skip empty lines and comments
        if not stripped or stripped.startswith('--'):
            continue
        
        # Count rows in VALUES block (lines starting with '(')
        if in_values_block and stripped.startswith('('):
            current_row_count += 1
    
    # Save last INSERT
    if current_table and current_insert_line:
        insert_statements[current_table].append((current_insert_line, current_row_count))
    
    # Validate: Multiple INSERTs for same table that could be combined
    for table_name, inserts in insert_statements.items():
        if len(inserts) > 1:
            total_rows = sum(row_count for _, row_count in inserts)
            
            # If total rows < 1000, they should be combined into one INSERT
            if total_rows < 1000:
                insert_lines = [str(line_num) for line_num, _ in inserts]
                friendly_name = {
                    'orchestration': 'Data_Pipeline_Metadata_Orchestration',
                    'primary_config': 'Data_Pipeline_Metadata_Primary_Configuration',
                    'advanced_config': 'Data_Pipeline_Metadata_Advanced_Configuration',
                    'datastore_configuration': 'Datastore_Configuration'
                }.get(table_name, table_name)
                
                result.add_issue('error', 'insert_grouping',
                    f"Multiple INSERT statements for {friendly_name} found on lines {', '.join(insert_lines)} with {total_rows} total rows. "
                    f"These should be combined into a single INSERT statement (split only required when exceeding 1000 rows).",
                    line_number=inserts[1][0],  # Point to second INSERT
                    suggestion="Combine all VALUES rows into a single INSERT statement for this table. "
                               "Multiple INSERTs break CI/CD diff tracking and reduce maintainability.")
        
        # Validate: Single INSERT exceeding 1000 rows
        for line_num, row_count in inserts:
            if row_count > 1000:
                friendly_name = {
                    'orchestration': 'Data_Pipeline_Metadata_Orchestration',
                    'primary_config': 'Data_Pipeline_Metadata_Primary_Configuration',
                    'advanced_config': 'Data_Pipeline_Metadata_Advanced_Configuration',
                    'datastore_configuration': 'Datastore_Configuration'
                }.get(table_name, table_name)
                
                result.add_issue('warning', 'insert_size',
                    f"INSERT statement for {friendly_name} on line {line_num} has {row_count} rows, exceeding the 1000 row limit.",
                    line_number=line_num,
                    suggestion=f"Split into multiple INSERT statements with up to 1000 rows each.")


def validate_unbalanced_quotes(result: ValidationResult, content: str):
    """Check for unbalanced single quotes in SQL string literals.
    
    In SQL, single quotes within a string are escaped by doubling them ('').
    This means the total number of internal quotes (after stripping outer quotes)
    must be even for the string to be valid.
    
    Examples:
        'hello'           -> valid (no internal quotes)
        'it''s'           -> valid (one escaped quote = 2 chars)
        'bs_yn = ''Y'''   -> valid (two escaped quotes = 4 chars)
        'bs_yn = ''Y'     -> INVALID (3 internal quotes - unbalanced)
    """
    lines = content.split('\n')
    
    # Pattern to find SQL string literals: 'content'
    # We need to be careful about escaped quotes within strings
    # This pattern finds opening quotes and tracks to find proper closing
    
    for line_num, line in enumerate(lines, 1):
        # Skip comments
        if line.strip().startswith('--'):
            continue
        
        # Check for unbalanced quotes in this line
        # We'll scan character by character to properly handle escaped quotes
        i = 0
        while i < len(line):
            if line[i] == "'":
                # Found start of a string literal
                start_pos = i
                i += 1
                string_content = []
                
                while i < len(line):
                    if line[i] == "'":
                        # Check if this is an escaped quote ('') or end of string
                        if i + 1 < len(line) and line[i + 1] == "'":
                            # Escaped quote - add both and skip
                            string_content.append("''")
                            i += 2
                        else:
                            # End of string
                            break
                    else:
                        string_content.append(line[i])
                        i += 1
                else:
                    # Reached end of line without finding closing quote
                    # Extract context for error message
                    context_start = max(0, start_pos - 10)
                    context_end = min(len(line), start_pos + 40)
                    context = line[context_start:context_end]
                    if context_start > 0:
                        context = "..." + context
                    if context_end < len(line):
                        context = context + "..."
                    
                    result.add_issue('error', 'unbalanced_quotes', 
                        f"Unbalanced single quotes - string starting at position {start_pos + 1} has no closing quote. Context: {context!r}",
                        line_number=line_num)
            i += 1


# =============================================================================
# ORCHESTRATION VALIDATIONS
# =============================================================================

def validate_orchestration(result: ValidationResult, orch_rows: list, environment: str = "DEV"):
    if not orch_rows:
        return

    # Dynamically discover valid datastores from datastore config files.
    # No fallback to hardcoded defaults: missing/invalid datastore config must fail validation.
    valid_datastores: Optional[Set[str]] = None
    try:
        file_path = Path(result.file_path)
        datastore_config_path = find_datastore_config_path(file_path, environment)
        if not datastore_config_path:
            result.add_issue(
                'error',
                'datastore_config',
                f"Could not locate datastore configuration notebook for environment '{environment}'.",
                suggestion=(
                    "Datastore validation requires a datastore notebook (no hardcoded fallback).\n"
                    "Expected path pattern: <workspace>/datastores/datastore_<ENV>.Notebook/notebook-content.sql"
                )
            )
        else:
            discovered = get_defined_datastores_from_config(datastore_config_path)
            if not discovered:
                result.add_issue(
                    'error',
                    'datastore_config',
                    f"Datastore configuration file was found at '{datastore_config_path}' but no datastore names could be parsed.",
                    suggestion="Ensure notebook-content.sql has INSERT INTO [dbo].[Datastore_Configuration] VALUES (...) rows."
                )
            else:
                valid_datastores = discovered
    except Exception as ex:
        result.add_issue(
            'error',
            'datastore_config',
            f"Failed to load datastore configuration: {ex}",
            suggestion="Fix datastore notebook discovery/parsing before validating orchestration target datastores."
        )
    table_ids_seen = set()

    for row in orch_rows:
        table_id = row['table_id']
        line_num = row['line_number']

        if table_id in table_ids_seen:
            result.add_issue('error', 'orchestration', f"Duplicate Table_ID: {table_id}", table_id=table_id, line_number=line_num,
                suggestion="Each Table_ID must be unique. Change this to a new unique ID or remove the duplicate row.")
        table_ids_seen.add(table_id)

        if table_id <= 0:
            result.add_issue('error', 'orchestration', f"Table_ID must be > 0: {table_id}", table_id=table_id, line_number=line_num,
                suggestion="Table_ID must be a positive integer. Use convention: Bronze 1-99, Silver 101-199, Gold 201-299.")

        if row['order'] <= 0:
            result.add_issue('error', 'orchestration', f"Order_Of_Operations must be > 0: {row['order']}", table_id=table_id, line_number=line_num,
                suggestion="Order_Of_Operations controls execution sequence. Use 1, 2, 3... Tables with same value run in parallel.")

        if row['processing_method'] not in ORCHESTRATION_CONFIG['processing_methods']:
            result.add_issue('error', 'orchestration', f"Invalid Processing_Method: '{row['processing_method']}'", table_id=table_id, line_number=line_num, suggestion=f"Valid: {ORCHESTRATION_CONFIG['processing_methods']}")

        if valid_datastores is not None and row['target_datastore'] not in valid_datastores:
            result.add_issue('error', 'orchestration', f"Invalid Target_Datastore: '{row['target_datastore']}'", table_id=table_id, line_number=line_num, suggestion=f"Valid: {sorted(valid_datastores)}")

        if row['ingestion_active'] not in [0, 1]:
            result.add_issue('error', 'orchestration', f"Ingestion_Active must be 0 or 1: {row['ingestion_active']}", table_id=table_id, line_number=line_num,
                suggestion="Use 1 to enable this table for processing, 0 to disable it temporarily.")

        # Colon is used as a delimiter in Get_Pipeline_Tables STRING_AGG output
        # and parsed via @split(item(), ':') in PL_01b. A colon in any value shifts
        # all downstream indexes and silently breaks pipeline parameter passing.
        if ':' in row['target_datastore']:
            result.add_issue('error', 'orchestration',
                f"Target_Datastore contains a colon character: '{row['target_datastore']}'",
                table_id=table_id, line_number=line_num,
                suggestion="Colons are reserved as delimiters in the pipeline table details string. Remove the colon from Target_Datastore.")
        if ':' in row['target_entity']:
            result.add_issue('error', 'orchestration',
                f"Target_Entity contains a colon character: '{row['target_entity']}'",
                table_id=table_id, line_number=line_num,
                suggestion="Colons are reserved as delimiters in the pipeline table details string. Remove the colon from Target_Entity.")

        # When Target_Entity contains '/', it's a file output path (e.g., 'exports/%Y/%m/%d/data.csv')
        # Skip schema and identifier checks - file paths legitimately use /, %, . for extensions, etc.
        is_file_output_path = '/' in row['target_entity']

        if not is_file_output_path:
            # Target_Entity can be a bare table name (recommended). The runtime
            # qualifies it as <catalog>.<schema>.<table> using the row in
            # Datastore_Configuration that matches Target_Datastore. 2-part
            # 'schema.table' and 3-part 'catalog.schema.table' forms are also
            # accepted.
            parts = [p.strip() for p in row['target_entity'].split('.')]
            if any(not p for p in parts):
                result.add_issue('error', 'orchestration',
                    f"Target_Entity has empty schema/table part: '{row['target_entity']}'",
                    table_id=table_id, line_number=line_num)
            else:
                invalid_parts = [p for p in parts if not _IDENTIFIER_RE.match(p)]
                if invalid_parts:
                    result.add_issue('error', 'orchestration',
                        f"Target_Entity contains invalid characters in schema/table part(s): {invalid_parts}. Only letters, numbers, and underscores are allowed.",
                        table_id=table_id, line_number=line_num)

        if not re.match(r'^[a-zA-Z0-9_]+$', row['trigger_name']):
            result.add_issue('error', 'orchestration', f"Trigger_Name has special chars: '{row['trigger_name']}'", table_id=table_id, line_number=line_num,
                suggestion="Trigger_Name can only contain letters, numbers, and underscores. Example: 'Daily_Load' or 'Hourly_Refresh'")

    # Validate Order_Of_Operations has no gaps (unique values must be sequential)
    # Note: Multiple tables CAN share the same Order_Of_Operations for parallel execution
    unique_orders = sorted(set(row['order'] for row in orch_rows))
    if unique_orders:
        expected_orders = list(range(unique_orders[0], unique_orders[0] + len(unique_orders)))
        if unique_orders != expected_orders:
            missing = set(expected_orders) - set(unique_orders)
            result.add_issue('warning', 'orchestration',
                f"Order_Of_Operations has gaps. Unique values found: {unique_orders}. Missing values to make contiguous: {sorted(missing)}",
                suggestion="Order_Of_Operations values should be contiguous (e.g., 1,2,3 or 10,11,12 - not 1,2,5). Multiple tables can share the same order for parallel execution.")

    # Check if all Order_Of_Operations values are unique (no parallelization)
    # If so, warn that some rows within the same layer might be able to run in parallel
    order_counts = defaultdict(int)
    for row in orch_rows:
        order_counts[row['order']] += 1
    
    all_unique = all(count == 1 for count in order_counts.values())
    if all_unique and len(orch_rows) > 1:
        # Group by target_datastore to suggest parallelization within layers
        by_layer = defaultdict(list)
        for row in orch_rows:
            by_layer[row['target_datastore']].append(row['order'])
        
        parallel_candidates = [layer for layer, orders in by_layer.items() if len(orders) > 1]
        if parallel_candidates:
            result.add_issue('warning', 'orchestration',
                f"Parallelization opportunity: All Order_Of_Operations values are sequential (1,2,3,...) and multiple tables exist in the same layer. "
                f"This may indicate unnecessary sequential execution if some of these tables are independent of each other.",
                suggestion=f"Consider assigning the SAME Order_Of_Operations value to tables within a layer that have no dependencies between them to enable parallel execution. "
                f"Layers where parallel execution may be possible: {parallel_candidates}. "
                f"Example pattern (if appropriate for your dependencies): all Bronze tables Order=1, all Silver tables Order=2, all Gold tables Order=3.")

    # Validate Table_IDs have at least 100 gap between layers (bronze → silver → gold)
    # If a layer is skipped (e.g., bronze → gold with no silver), gap must be 100 * skipped layers
    layer_order = [layer.lower() for layer in ORCHESTRATION_CONFIG['target_datastores']]
    table_ids_by_layer = defaultdict(list)
    for row in orch_rows:
        layer = row['target_datastore'].lower()
        table_ids_by_layer[layer].append(row['table_id'])
    
    # Check gaps between all pairs of layers that have Table_IDs
    layers_with_ids = [l for l in layer_order if table_ids_by_layer.get(l)]
    for i in range(len(layers_with_ids) - 1):
        current_layer = layers_with_ids[i]
        next_layer = layers_with_ids[i + 1]
        
        # Calculate how many layers are skipped (e.g., bronze→gold skips silver = 1 skipped)
        current_idx = layer_order.index(current_layer)
        next_idx = layer_order.index(next_layer)
        layers_between = next_idx - current_idx  # 1 for adjacent, 2 for skipping one layer
        required_gap = 100 * layers_between
        
        current_ids = table_ids_by_layer[current_layer]
        next_ids = table_ids_by_layer[next_layer]
        
        max_current = max(current_ids)
        min_next = min(next_ids)
        gap = min_next - max_current
        
        if gap < required_gap:
            skipped_layers = [layer_order[j] for j in range(current_idx + 1, next_idx)]
            skip_note = f" (skipping {', '.join(skipped_layers)})" if skipped_layers else ""
            result.add_issue('warning', 'orchestration',
                f"Table_ID gap between {current_layer} (max: {max_current}) and {next_layer} (min: {min_next}){skip_note} is only {gap}. "
                f"Consider a gap of at least {required_gap} to allow room for future tables and avoid ID conflicts across developers.",
                suggestion=f"If this is intentional, you can ignore this warning. Otherwise, increase {next_layer} Table_IDs to start at {max_current + required_gap} or higher.")


def validate_orchestration_primary_config_coverage(result: ValidationResult, orch_rows: list, primary_rows: list):
    """Validate that every Table_ID in orchestration has at least one row in primary config."""
    if not orch_rows or not primary_rows:
        return

    # Get all Table_IDs from orchestration
    orch_table_ids = {row['table_id'] for row in orch_rows}

    # Get all Table_IDs from primary config
    primary_table_ids = {row['table_id'] for row in primary_rows}

    # Find Table_IDs in orchestration without primary config rows
    missing_in_primary = orch_table_ids - primary_table_ids
    for table_id in missing_in_primary:
        result.add_issue('error', 'cross_config',
            f"Table_ID {table_id} is in orchestration but has no rows in primary config",
            table_id=table_id)


# =============================================================================
# PRIMARY CONFIG VALIDATIONS
# =============================================================================

def validate_primary_config(result: ValidationResult, primary_rows: list, orch_rows: list):
    if not primary_rows:
        return

    orch_table_ids = {r['table_id'] for r in orch_rows}
    orch_by_table_id = {r['table_id']: r for r in orch_rows}
    configs_by_table: Dict[int, Dict[str, Dict[str, str]]] = defaultdict(lambda: defaultdict(dict))

    for row in primary_rows:
        table_id = row['table_id']
        category = row['category']
        name = row['name']
        value = row['value']
        line_num = row['line_number']

        configs_by_table[table_id][category][name] = value

        if orch_table_ids and table_id not in orch_table_ids:
            result.add_issue('warning', 'primary_config', f"Table_ID {table_id} not in Orchestration", table_id=table_id, line_number=line_num,
                suggestion=f"Add a row to Data_Pipeline_Orchestration with Table_ID={table_id}, or change this to match an existing Table_ID.")

        if category in ADVANCED_CONFIG_CATEGORIES:
            result.add_issue('error', 'category_placement', f"Category '{category}' belongs in Advanced Config", table_id=table_id, line_number=line_num,
                suggestion=f"Move this row from Data_Pipeline_Primary_Config to Data_Pipeline_Advanced_Config. Category '{category}' requires Instance_Number.")
        elif category not in PRIMARY_CONFIG_CATEGORIES:
            result.add_issue('error', 'invalid_category', f"Unknown category: '{category}'", table_id=table_id, line_number=line_num,
                suggestion=f"Valid primary config categories: {', '.join(sorted(PRIMARY_CONFIG_CATEGORIES))}")

        if category in VALID_ATTRIBUTES:
            # Catch deprecated connection_id pattern — must use datastore_name instead
            if category == 'source_details' and name == 'connection_id':
                result.add_issue('error', 'deprecated_connection_id',
                    f"'connection_id' is deprecated. Use 'datastore_name' instead.",
                    table_id=table_id, line_number=line_num,
                    suggestion=(
                        "The accelerator now resolves connections via Datastore_Configuration at runtime.\n"
                        "   1. Register the connection in your datastore config notebook:\n"
                        "      INSERT INTO [dbo].[Datastore_Configuration] VALUES\n"
                        "      ('<name>', 'Connection', NULL, NULL, NULL, NULL, NULL, '<connection-id-guid>')\n"
                        "   2. Replace this line with: (Table_ID, 'source_details', 'datastore_name', '<name>')\n"
                        "   3. The pipeline resolves the Connection_ID at runtime via _get_datastore_config()"
                    ))
            # Allow custom_table_ingestion_function_*, custom_file_ingestion_function_*, custom_staging_function_*, and custom_source_function_* prefixed attributes in source_details
            elif name not in VALID_ATTRIBUTES[category]:
                is_custom_table_ingestion_attr = category == 'source_details' and name.startswith('custom_table_ingestion_function_')
                is_custom_file_ingestion_attr = category == 'source_details' and name.startswith('custom_file_ingestion_function_')
                is_custom_staging_attr = category == 'source_details' and name.startswith('custom_staging_function_')
                is_custom_source_attr = category == 'source_details' and name.startswith('custom_source_function_')
                if not is_custom_table_ingestion_attr and not is_custom_file_ingestion_attr and not is_custom_staging_attr and not is_custom_source_attr:
                    similar = find_similar(name, VALID_ATTRIBUTES[category])
                    result.add_issue('error', 'invalid_attribute', f"Invalid attribute '{name}' for '{category}'", table_id=table_id, line_number=line_num, suggestion=f"Did you mean: {similar}?" if similar else None)

        if name in VALID_VALUES and value:
            if value.lower() not in {v.lower() for v in VALID_VALUES[name]}:
                result.add_issue('error', 'invalid_value', f"Invalid value '{value}' for '{name}'", table_id=table_id, line_number=line_num, suggestion=f"Valid: {sorted(VALID_VALUES[name])}")

        if (category, name) in GUID_CONFIGS and value:
            if not re.match(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$', value):
                result.add_issue('error', 'invalid_guid', f"Invalid GUID for {name}: '{value}'", table_id=table_id, line_number=line_num,
                    suggestion="GUID must be in format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (e.g., '12345678-1234-1234-1234-123456789abc')")

        if (category, name) in BOOLEAN_CONFIGS and value:
            if value.lower() not in ['true', 'false']:
                result.add_issue('error', 'invalid_boolean', f"{name} must be 'true' or 'false': '{value}'", table_id=table_id, line_number=line_num,
                    suggestion="Use lowercase 'true' or 'false' (not 'True', '1', 'yes', etc.)")

        # Colon is used as a delimiter in Get_Pipeline_Tables STRING_AGG output.
        # datastore_name and staging_catalog_name are included in the colon-delimited string,
        # so a colon in their values would break @split(item(), ':') parsing in PL_01b.
        if name in ('datastore_name', 'staging_catalog_name') and value and ':' in value:
            result.add_issue('error', 'reserved_character',
                f"'{name}' contains a colon character: '{value}'",
                table_id=table_id, line_number=line_num,
                suggestion="Colons are reserved as delimiters in the pipeline table details string. Remove the colon from the value.")

        # wildcard_folder_path should never start with "Files/" - the framework prepends this automatically
        if name == 'wildcard_folder_path' and value:
            if value.startswith('Files/'):
                result.add_issue('error', 'invalid_path', f"wildcard_folder_path should not start with 'Files/' - the framework prepends this automatically. Use '{value[6:]}' instead.", table_id=table_id, line_number=line_num)
            # wildcard_folder_path should not start with '/' (absolute path)
            if value.startswith('/'):
                result.add_issue('error', 'invalid_path', f"wildcard_folder_path should not start with '/' - use relative path instead. Use '{value[1:]}' instead.", table_id=table_id, line_number=line_num)

        # staging_folder_path should not start with '/' (absolute path)
        if name == 'staging_folder_path' and value:
            if value.startswith('/'):
                result.add_issue('error', 'invalid_path', f"staging_folder_path should not start with '/' - use relative path instead. Use '{value[1:]}' instead.", table_id=table_id, line_number=line_num)
            if value.startswith('Files/'):
                result.add_issue('error', 'invalid_path', f"staging_folder_path should not start with 'Files/' - the framework prepends this automatically. Use '{value[6:]}' instead.", table_id=table_id, line_number=line_num)

        # sftp_wildcard_folder_path validation (trailing slash only)
        if name == 'sftp_wildcard_folder_path' and value:
            if value.endswith('/'):
                if value == '/':
                    # Special case: root path '/' - suggest using '.' for current directory or provide clearer guidance
                    result.add_issue('error', 'invalid_path', "sftp_wildcard_folder_path is set to '/' (root). If you mean the root folder, use '.' instead. If you need a specific subfolder, provide the path without a trailing slash (e.g., '/uploads' or 'data').", table_id=table_id, line_number=line_num)
                else:
                    result.add_issue('error', 'invalid_path', f"sftp_wildcard_folder_path should not end with '/' - remove trailing slash. Use '{value[:-1]}' instead.", table_id=table_id, line_number=line_num)

    # Cross-configuration validations
    for table_id, table_configs in configs_by_table.items():
        source_configs = table_configs.get('source_details', {})
        target_configs = table_configs.get('target_details', {})
        watermark_configs = table_configs.get('watermark_details', {})
        orch_row = orch_by_table_id.get(table_id, {})

        source_type = source_configs.get('source')
        if source_type and source_type in REQUIRED_CONFIGS_BY_SOURCE:
            for req in REQUIRED_CONFIGS_BY_SOURCE[source_type]:
                if req not in source_configs:
                    result.add_issue('error', 'missing_required', f"Missing '{req}' for source '{source_type}'", table_id=table_id)

        merge_type = target_configs.get('merge_type')
        if merge_type in MERGE_TYPES_REQUIRING_DELETE_COLUMNS:
            if 'column_to_mark_source_data_deletion' not in target_configs:
                result.add_issue('error', 'missing_required', f"merge_type='{merge_type}' requires 'column_to_mark_source_data_deletion'", table_id=table_id)
            if 'delete_rows_with_value' not in target_configs:
                result.add_issue('error', 'missing_required', f"merge_type='{merge_type}' requires 'delete_rows_with_value'", table_id=table_id)

        if merge_type == 'replace_where' and 'replace_where_column' not in target_configs:
            result.add_issue('error', 'missing_required', "merge_type='replace_where' requires 'replace_where_column'", table_id=table_id)

        if merge_type in MERGE_TYPES_REQUIRING_PRIMARY_KEYS:
            pk = orch_row.get('primary_keys', '')
            if not pk or not pk.strip():
                result.add_issue('error', 'missing_required', f"merge_type='{merge_type}' requires Primary_Keys in orchestration", table_id=table_id)

        if target_configs.get('enable_scd2_dimension') == 'true':
            result.add_issue('warning', 'deprecated_config', "enable_scd2_dimension is deprecated. Use merge_type='scd2' instead.", table_id=table_id)

        merge_type = target_configs.get('merge_type')
        if merge_type == 'scd2':
            if 'source_timestamp_column_name' not in target_configs:
                result.add_issue('error', 'missing_required', "merge_type='scd2' requires 'source_timestamp_column_name'", table_id=table_id)
            if orch_row.get('target_datastore', '').lower() == 'bronze':
                result.add_issue('warning', 'best_practice', "SCD2 is typically for Silver/Gold, not Bronze", table_id=table_id)

        if watermark_configs.get('use_change_data_feed') == 'true' and source_type:
            result.add_issue('error', 'invalid_config', "use_change_data_feed cannot be used with external databases", table_id=table_id)

        processing_method = orch_row.get('processing_method')
        if processing_method in PROCESSING_METHODS_REQUIRING_DATASTORE_NAME:
            if 'datastore_name' not in source_configs:
                result.add_issue('error', 'missing_required',
                    f"Processing_Method='{processing_method}' requires 'datastore_name' in source_details. "
                    f"Register the notebook/dataflow in Datastore_Configuration and reference it by name.",
                    table_id=table_id,
                    suggestion=(
                        f"1. Add the notebook/dataflow to your datastore_<ENV>.Notebook/notebook-content.sql:\n"
                        f"   ('my_notebook', 'Notebook', '<item-guid>', '<workspace-guid>', '<workspace-name>', NULL, NULL, NULL)\n"
                        f"2. Reference it in metadata: (Table_ID, 'source_details', 'datastore_name', 'my_notebook')"
                    ))
            if 'item_id' in source_configs:
                result.add_issue('error', 'invalid_config',
                    f"Processing_Method='{processing_method}' should not use 'item_id' directly. "
                    f"Use 'datastore_name' to reference a Datastore_Configuration entry instead (CI/CD safe).",
                    table_id=table_id,
                    suggestion=(
                        f"Replace item_id with datastore_name:\n"
                        f"  REMOVE: (Table_ID, 'source_details', 'item_id', '<guid>')\n"
                        f"  ADD:    (Table_ID, 'source_details', 'datastore_name', '<name>')\n"
                        f"Then add the GUID to your datastore config per environment."
                    ))
            if 'workspace_id' in source_configs:
                result.add_issue('error', 'invalid_config',
                    f"Processing_Method='{processing_method}' should not use 'workspace_id' directly. "
                    f"The workspace_id is resolved from the Datastore_Configuration entry referenced by 'datastore_name'.",
                    table_id=table_id,
                    suggestion=(
                        f"Remove workspace_id from metadata and set it in the Datastore_Configuration entry instead:\n"
                        f"  ('my_notebook', 'Notebook', '<item-guid>', '<workspace-guid>', '<workspace-name>', NULL, NULL, NULL)"
                    ))

        # Validate exact_find and exact_replace count alignment
        column_cleansing_configs = table_configs.get('column_cleansing', {})
        exact_find = column_cleansing_configs.get('exact_find')
        exact_replace = column_cleansing_configs.get('exact_replace')
        if exact_find and not exact_replace:
            result.add_issue('error', 'missing_required', "exact_find requires exact_replace to be provided", table_id=table_id)
        if exact_replace and not exact_find:
            result.add_issue('error', 'missing_required', "exact_replace requires exact_find to be provided", table_id=table_id)
        if exact_find and exact_replace:
            find_count = len([f.strip() for f in exact_find.split(',') if f.strip()])
            replace_count = len([r.strip() for r in exact_replace.split(',') if r.strip()])
            # exact_replace can be single value (applies to all) OR must match exact_find count
            if replace_count != 1 and replace_count != find_count:
                result.add_issue('error', 'count_mismatch', f"exact_find has {find_count} items but exact_replace has {replace_count} items. exact_replace must be a single value OR have exactly {find_count} items.", table_id=table_id)

        # Validate regex_find and regex_replace must be provided together
        regex_find = column_cleansing_configs.get('regex_find')
        regex_replace = column_cleansing_configs.get('regex_replace')
        if regex_find and not regex_replace:
            result.add_issue('error', 'missing_required', "regex_find requires regex_replace to be provided", table_id=table_id)
        if regex_replace and not regex_find:
            result.add_issue('error', 'missing_required', "regex_replace requires regex_find to be provided", table_id=table_id)

        # Validate xml_namespaces_keys and xml_namespaces_values count alignment
        xml_keys = source_configs.get('xml_namespaces_keys')
        xml_values = source_configs.get('xml_namespaces_values')
        if xml_keys and not xml_values:
            result.add_issue('error', 'missing_required', "xml_namespaces_keys requires xml_namespaces_values to be provided", table_id=table_id)
        if xml_values and not xml_keys:
            result.add_issue('error', 'missing_required', "xml_namespaces_values requires xml_namespaces_keys to be provided", table_id=table_id)
        if xml_keys and xml_values:
            keys_count = len([k.strip() for k in xml_keys.split(',') if k.strip()])
            values_count = len([v.strip() for v in xml_values.split(',') if v.strip()])
            if keys_count != values_count:
                result.add_issue('error', 'count_mismatch', f"xml_namespaces_keys has {keys_count} items but xml_namespaces_values has {values_count} items. Counts must match.", table_id=table_id)

    # Validate staging_folder_path uniqueness across all Table_IDs
    staging_paths: Dict[str, list] = defaultdict(list)
    for table_id, table_configs in configs_by_table.items():
        staging_path = table_configs.get('source_details', {}).get('staging_folder_path')
        if staging_path:
            staging_paths[staging_path].append(table_id)
    for path, table_ids in staging_paths.items():
        if len(table_ids) > 1:
            result.add_issue('error', 'duplicate_config', f"staging_folder_path '{path}' is used by multiple Table_IDs: {table_ids}. Each Table_ID must have a unique staging_folder_path.", table_id=table_ids[0])

    # Validate source config uniqueness (same datastore_name + schema_name + table_name shouldn't appear twice)
    source_configs_seen: Dict[tuple, list] = defaultdict(list)
    for table_id, table_configs in configs_by_table.items():
        source = table_configs.get('source_details', {})
        ds_name = source.get('datastore_name')
        schema_name = source.get('schema_name')
        table_name = source.get('table_name')
        if ds_name and schema_name and table_name:
            key = (ds_name, schema_name, table_name)
            source_configs_seen[key].append(table_id)
    for key, table_ids in source_configs_seen.items():
        if len(table_ids) > 1:
            ds_name, schema_name, table_name = key
            result.add_issue('error', 'duplicate_config', f"Source configuration (datastore_name='{ds_name}', schema_name='{schema_name}', table_name='{table_name}') is used by multiple Table_IDs: {table_ids}. Each source table should only be configured once.", table_id=table_ids[0])

    # Validate Table_IDs are grouped together (all rows for same Table_ID should be consecutive)
    seen_table_ids = set()
    last_table_id = None
    for row in primary_rows:
        table_id = row['table_id']
        if table_id != last_table_id:
            if table_id in seen_table_ids:
                result.add_issue('warning', 'row_ordering',
                    f"Table_ID {table_id} rows are not grouped together - rows for the same Table_ID should be consecutive for maintainability",
                    table_id=table_id, line_number=row['line_number'])
            seen_table_ids.add(table_id)
            last_table_id = table_id


# =============================================================================
# ADVANCED CONFIG VALIDATIONS
# =============================================================================

def validate_advanced_config(result: ValidationResult, advanced_rows: list, orch_rows: list):
    if not advanced_rows:
        return

    orch_table_ids = {r['table_id'] for r in orch_rows}
    instances: Dict[tuple, Dict[str, str]] = defaultdict(dict)
    instance_attr_lines: Dict[tuple, int] = {}

    # Track (table_id, category, name, instance, attribute) to detect duplicate attribute definitions
    seen_instance_attributes: Dict[tuple, int] = {}  # key -> line_number of first occurrence

    # Track (table_id, category, instance) to detect different Configuration_Names sharing same instance number
    # key = (table_id, category, instance), value = (config_name, line_number)
    seen_category_instances: Dict[tuple, tuple] = {}

    duplicate_list_attributes = {
        'group_by_columns', 'left_columns', 'right_columns', 'partition_by', 'order_by',
        'column_name', 'output_column_name', 'id_columns', 'value_columns',
        'columns_to_rename', 'remove_columns', 'select_columns', 'value_column',
    }

    for row in advanced_rows:
        table_id = row['table_id']
        category = row['category']
        name = row['name']
        instance = row['instance']
        attribute = row['attribute']
        value = row['value']
        line_num = row['line_number']

        key = (table_id, name, instance)
        instances[key][attribute] = value
        instance_attr_lines[(table_id, name, instance, attribute)] = line_num

        # Check for duplicate (table_id, category, name, instance, attribute) combinations
        instance_attr_key = (table_id, category, name, instance, attribute)
        if instance_attr_key in seen_instance_attributes:
            first_line = seen_instance_attributes[instance_attr_key]
            result.add_issue('error', 'duplicate_instance',
                f"Duplicate instance: '{name}' instance {instance} has attribute '{attribute}' defined twice (first on line {first_line})",
                table_id=table_id, line_number=line_num,
                suggestion=f"Each (Configuration_Name, Instance_Number, Attribute) must be unique within a Table_ID. Use a different instance number for the second occurrence.")
        else:
            seen_instance_attributes[instance_attr_key] = line_num

        # Check for different Configuration_Names sharing the same instance number within (Table_ID, Category)
        category_instance_key = (table_id, category, instance)
        if category_instance_key in seen_category_instances:
            existing_name, first_line = seen_category_instances[category_instance_key]
            if existing_name != name:
                result.add_issue('error', 'duplicate_instance_number',
                    f"Instance number {instance} used by multiple {category}: '{existing_name}' (line {first_line}) and '{name}'",
                    table_id=table_id, line_number=line_num,
                    suggestion=f"Each transformation/DQ check must have a unique instance number within its category. Change '{name}' to use a different instance number.")
        else:
            seen_category_instances[category_instance_key] = (name, line_num)

        if orch_table_ids and table_id not in orch_table_ids:
            result.add_issue('warning', 'advanced_config', f"Table_ID {table_id} not in Orchestration", table_id=table_id, line_number=line_num,
                suggestion=f"Add a row to Data_Pipeline_Orchestration with Table_ID={table_id}, or change this to match an existing Table_ID.")

        if category in PRIMARY_CONFIG_CATEGORIES:
            result.add_issue('error', 'category_placement', f"Category '{category}' belongs in Primary Config", table_id=table_id, line_number=line_num,
                suggestion=f"Move this row from Data_Pipeline_Advanced_Config to Data_Pipeline_Primary_Config. Category '{category}' does not use Instance_Number.")
        elif category not in ADVANCED_CONFIG_CATEGORIES:
            result.add_issue('error', 'invalid_category', f"Unknown category: '{category}'", table_id=table_id, line_number=line_num,
                suggestion=f"Valid advanced config categories: {', '.join(sorted(ADVANCED_CONFIG_CATEGORIES))}")

        if category in VALID_ATTRIBUTES:
            if name not in VALID_ATTRIBUTES[category]:
                similar = find_similar(name, VALID_ATTRIBUTES[category])
                result.add_issue('error', 'invalid_type', f"Invalid {category} type '{name}'", table_id=table_id, line_number=line_num, suggestion=f"Did you mean: {similar}?" if similar else None)

        if name in TRANSFORMATION_ATTRIBUTES:
            # custom_transformation_function allows arbitrary custom attributes to be passed to user's function
            # custom_table_ingestion_function and custom_file_ingestion_function also allow arbitrary custom attributes
            if name not in ('custom_transformation_function', 'custom_table_ingestion_function', 'custom_file_ingestion_function') and attribute not in TRANSFORMATION_ATTRIBUTES[name]:
                similar = find_similar(attribute, TRANSFORMATION_ATTRIBUTES[name])
                result.add_issue('error', 'invalid_attribute', f"Invalid attribute '{attribute}' for '{name}'", table_id=table_id, line_number=line_num, suggestion=f"Did you mean: {similar}?" if similar else None)

        if attribute in VALID_VALUES and value:
            # Skip new_type here — it supports decimal(p,s) and is validated separately by validate_new_type_values
            if attribute == 'new_type':
                pass
            else:
                # These attributes support comma-separated values where each individual value must be valid
                comma_separated_attributes = ['aggregation', 'comparison_types', 'operations', 'sort_direction', 'order_direction']
                values_to_check = [v.strip() for v in value.split(',')] if attribute in comma_separated_attributes else [value]
                for v in values_to_check:
                    if v and v.lower() not in {x.lower() for x in VALID_VALUES[attribute]}:
                        result.add_issue('error', 'invalid_value', f"Invalid value '{v}' for '{attribute}'", table_id=table_id, line_number=line_num, suggestion=f"Valid: {sorted(VALID_VALUES[attribute])}")

        if name == 'join_data' and attribute == 'join_condition':
            if 'a.' not in value or 'b.' not in value:
                result.add_issue('error', 'join_condition', "join_condition must use 'a.' and 'b.' aliases", table_id=table_id, line_number=line_num,
                    suggestion="Use 'a.' prefix for left table columns and 'b.' for right table. Example: 'a.customer_id = b.customer_id'")

        # Double quotes in expressions/filter_logic break SQL parsing - use single quotes (escaped as '') instead
        if name == 'derived_column' and attribute == 'expression' and '"' in value:
            result.add_issue('error', 'invalid_expression', "derived_column expression must not contain double quotes - use SQL escaped single quotes ('') instead", table_id=table_id, line_number=line_num,
                suggestion="Replace double quotes with escaped single quotes. Example: CASE WHEN col = ''value'' THEN ''Y'' ELSE ''N'' END")

        if name == 'filter_data' and attribute == 'filter_logic' and '"' in value:
            result.add_issue('error', 'invalid_expression', "filter_logic must not contain double quotes - use SQL escaped single quotes ('') instead", table_id=table_id, line_number=line_num,
                suggestion="Replace double quotes with escaped single quotes. Example: status = ''Active'' AND region = ''US''")

        # derived_column uses f.expr() behind the scenes, so lit() won't work - use SQL string literals instead
        if name == 'derived_column' and attribute == 'expression':
            # Check for lit() function call - must match lit( as a function, not part of another word like 'split'
            if re.search(r'\blit\s*\(', value, re.IGNORECASE):
                result.add_issue('error', 'invalid_expression', "derived_column expression must not use lit() - expressions are passed to f.expr(), use SQL string literals instead (e.g., ''Y'' not lit(''Y''))", table_id=table_id, line_number=line_num,
                    suggestion="Remove lit() and use SQL syntax. Instead of lit(''Y''), use ''Y''. Instead of lit(1), use 1.")
            # Check for CASE WHEN - recommend conditional_column but allow derived_column
            # when the expression returns column references or NULL (which conditional_column cannot express)
            if re.search(r'\bCASE\s+WHEN\b', value, re.IGNORECASE):
                result.add_issue('warning', 'use_conditional_column', "derived_column contains CASE WHEN logic - prefer conditional_column when all branches return literal values", table_id=table_id, line_number=line_num,
                    suggestion="If all THEN/ELSE branches return string/numeric literals, use conditional_column instead (structured conditions/values/default_value). "
                               "If any branch returns a column reference, NULL, or a complex expression, derived_column with CASE WHEN is acceptable. "
                               "See docs/METADATA_GENERATION_GUIDE.md section '##### 32. conditional_column'.")

        if name == 'attach_dimension_surrogate_key' and attribute == 'dimension_table_join_logic':
            if 'a.' not in value or 'b.' not in value:
                result.add_issue('error', 'join_condition', "dimension_table_join_logic must use 'a.' and 'b.' aliases", table_id=table_id, line_number=line_num,
                    suggestion="Use 'a.' for fact table (left) and 'b.' for dimension table (right). Example: 'a.product_id = b.product_id'")

        if attribute in ['right_table_name', 'dimension_table_name', 'reference_table_name']:
            if value and len(value.split('.')) != 3:
                result.add_issue('error', 'three_part_naming', f"{attribute} should use 3-part naming: '{value}'", table_id=table_id, line_number=line_num,
                    suggestion="Use format: catalog.schema.table_name (e.g., 'silver.silver.dim_customer')")

        if attribute == 'union_tables' and value:
            for table_ref in value.split(','):
                table_ref = table_ref.strip()
                if table_ref and len(table_ref.split('.')) != 3:
                    result.add_issue('error', 'three_part_naming', f"union_tables entry needs 3-part naming: '{table_ref}'", table_id=table_id, line_number=line_num)

        if name == 'change_data_types' and attribute == 'new_type':
            # Split on commas, but skip commas that appear inside parentheses (e.g., preserve decimal(18,6) as a single token)
            dtypes = re.split(r',(?![^()]*\))', value)
            for dtype in dtypes:
                dtype = dtype.strip().lower()
                if dtype not in VALID_NEW_TYPE_BASE_TYPES and not dtype.startswith('decimal('):
                    result.add_issue('error', 'invalid_data_type', f"Invalid data type '{dtype}'", table_id=table_id, line_number=line_num)

    # Validate DQ checks come after transformations (per Table_ID)
    # Build per-table tracking of max transformation instance and min DQ instance
    table_transform_instances: Dict[int, list] = defaultdict(list)
    table_dq_instances: Dict[int, list] = defaultdict(list)

    for row in advanced_rows:
        table_id = row['table_id']
        category = row['category']
        instance = row['instance']

        if category == 'data_transformation_steps':
            table_transform_instances[table_id].append(instance)
        elif category == 'data_quality':
            table_dq_instances[table_id].append(instance)

    # Note: Instance numbers are interpreted WITHIN each category (data_transformation_steps vs data_quality).
    # DQ instance numbers do NOT need to be greater than transformation instance numbers.
    # The framework executes all transformations first, then all DQ checks - regardless of instance numbers.
    # Instance numbers only define order WITHIN their own category.

    # Required attributes and conditional validations
    for (table_id, name, instance), attrs in instances.items():
        if name in REQUIRED_ATTRIBUTES:
            missing = REQUIRED_ATTRIBUTES[name] - set(attrs.keys())
            if missing:
                result.add_issue('error', 'missing_required', f"'{name}' instance {instance} missing: {sorted(missing)}", table_id=table_id)

        for attr_name in duplicate_list_attributes:
            attr_value = attrs.get(attr_name)
            if attr_value and ',' in attr_value:
                duplicates = _find_duplicate_columns(attr_value)
                if duplicates:
                    line_num = instance_attr_lines.get((table_id, name, instance, attr_name))
                    result.add_issue('warning', 'duplicate_columns',
                        f"{attr_name} contains duplicate columns: {duplicates}",
                        table_id=table_id, line_number=line_num,
                        suggestion="Remove duplicate columns to avoid redundant processing")

        # Note: join_condition column validation removed - validator lacks schema context to verify column existence

        if name == 'transform_datetime':
            op = attrs.get('operation', '')
            if op in ['add_days', 'subtract_days'] and 'days' not in attrs:
                result.add_issue('error', 'missing_required', f"operation='{op}' requires 'days'", table_id=table_id)
            if op == 'add_months' and 'months' not in attrs:
                result.add_issue('error', 'missing_required', "operation='add_months' requires 'months'", table_id=table_id)
            if op == 'date_diff' and 'end_date_column' not in attrs:
                result.add_issue('error', 'missing_required', "operation='date_diff' requires 'end_date_column'", table_id=table_id)
            if op == 'format_date' and 'date_format' not in attrs:
                result.add_issue('error', 'missing_required', "operation='format_date' requires 'date_format'", table_id=table_id)

        if name == 'apply_null_handling':
            if attrs.get('action') == 'replace_with_default' and 'default_value' not in attrs:
                result.add_issue('error', 'missing_required', "action='replace_with_default' requires 'default_value'", table_id=table_id)

        if name == 'aggregate_data':
            col_count = len([c for c in attrs.get('column_name', '').split(',') if c.strip()])
            agg_count = len([a for a in attrs.get('aggregation', '').split(',') if a.strip()])
            out_count = len([o for o in attrs.get('output_column_name', '').split(',') if o.strip()])
            if col_count > 0 and agg_count > 0 and out_count > 0 and not (col_count == agg_count == out_count):
                result.add_issue('error', 'count_mismatch', f"aggregate_data counts mismatch: col={col_count}, agg={agg_count}, out={out_count}", table_id=table_id,
                    suggestion=f"Each attribute must have same number of comma-separated values. Example: column_name='sales,qty', aggregation='sum,avg', output_column_name='total_sales,avg_qty'")

        if name == 'entity_resolution':
            p = len([f for f in attrs.get('primary_dataset_comparison_fields', '').split(',') if f.strip()])
            s = len([f for f in attrs.get('secondary_dataset_comparison_fields', '').split(',') if f.strip()])
            t = len([f for f in attrs.get('comparison_types', '').split(',') if f.strip()])
            if p > 0 and s > 0 and t > 0 and not (p == s == t):
                result.add_issue('error', 'count_mismatch', f"entity_resolution field counts mismatch: primary={p}, secondary={s}, types={t}", table_id=table_id,
                    suggestion="Each field list must have same count. Example: primary='name,city', secondary='customer_name,location', comparison_types='fuzzy,exact'")

    # Validate Table_IDs are grouped together (all rows for same Table_ID should be consecutive)
    seen_table_ids = set()
    last_table_id = None
    for row in advanced_rows:
        table_id = row['table_id']
        if table_id != last_table_id:
            if table_id in seen_table_ids:
                result.add_issue('warning', 'row_ordering',
                    f"Table_ID {table_id} rows are not grouped together - rows for the same Table_ID should be consecutive for maintainability",
                    table_id=table_id, line_number=row['line_number'])
            seen_table_ids.add(table_id)
            last_table_id = table_id

    # Validate Instance_Numbers are in ascending order within each (Table_ID, Category)
    # Instance numbers are interpreted WITHIN each category independently
    # Group rows by (Table_ID, Category) and track instance numbers in order of appearance
    table_category_instances_in_order: Dict[tuple, list] = defaultdict(list)
    for row in advanced_rows:
        table_id = row['table_id']
        category = row['category']
        instance = row['instance']
        line_num = row['line_number']
        key = (table_id, category)
        # Only track first occurrence of each instance (attributes share same instance)
        if not table_category_instances_in_order[key] or table_category_instances_in_order[key][-1][0] != instance:
            table_category_instances_in_order[key].append((instance, line_num))

    for (table_id, category), instance_list in table_category_instances_in_order.items():
        instances_only = [i[0] for i in instance_list]
        first_line = instance_list[0][1] if instance_list else None
        
        # Check that instance numbers start at 1
        if instances_only and instances_only[0] != 1:
            result.add_issue('warning', 'instance_start',
                f"Instance_Numbers in '{category}' should start at 1, but first instance is {instances_only[0]}",
                table_id=table_id, line_number=first_line,
                suggestion=f"Renumber instances to start at 1 for consistency")
        
        # Check that instance numbers are sequential (no gaps)
        if len(instances_only) > 1:
            sorted_instances = sorted(set(instances_only))
            expected_sequence = list(range(sorted_instances[0], sorted_instances[0] + len(sorted_instances)))
            if sorted_instances != expected_sequence:
                gaps = set(expected_sequence) - set(sorted_instances)
                result.add_issue('warning', 'instance_gaps',
                    f"Instance_Numbers in '{category}' have gaps. Found: {sorted(set(instances_only))}, missing: {sorted(gaps)}",
                    table_id=table_id, line_number=first_line,
                    suggestion=f"Instance numbers should be sequential (1, 2, 3...) without gaps")
        
        # Existing check: ascending order
        for i in range(1, len(instances_only)):
            if instances_only[i] < instances_only[i-1]:
                result.add_issue('warning', 'instance_ordering',
                    f"Instance_Numbers in '{category}' are not in ascending order: instance {instances_only[i]} appears after instance {instances_only[i-1]}",
                    table_id=table_id, line_number=instance_list[i][1],
                    suggestion=f"Reorder rows within '{category}' so Instance_Numbers are in ascending order for readability")
                break  # Only report once per (Table_ID, Category)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def find_similar(name: str, valid_names: Set[str], threshold: float = 0.5) -> Optional[str]:
    name_lower = name.lower()
    best_match = None
    best_score = 0

    for valid in valid_names:
        valid_lower = valid.lower()
        common = set(name_lower) & set(valid_lower)
        score = len(common) / max(len(name_lower), len(valid_lower))
        if name_lower in valid_lower or valid_lower in name_lower:
            score += 0.4
        if score > best_score and score >= threshold:
            best_score = score
            best_match = valid

    return best_match


_IDENTIFIER_RE = re.compile(r'^[A-Za-z0-9_]+$')


def _normalize_column_token(token: str) -> str:
    token = token.strip()
    token = re.sub(r'^(a|b)\.', '', token, flags=re.IGNORECASE)
    if (token.startswith('[') and token.endswith(']')) or (token.startswith('`') and token.endswith('`')):
        token = token[1:-1].strip()
    if (token.startswith('"') and token.endswith('"')) or (token.startswith("'") and token.endswith("'")):
        token = token[1:-1].strip()
    return token


def _is_simple_identifier(token: str) -> bool:
    return bool(_IDENTIFIER_RE.match(token))


def _split_column_list(value: str) -> List[str]:
    return [_normalize_column_token(v) for v in value.split(',') if v.strip()]


def _find_duplicate_columns(value: str) -> List[str]:
    cols = [c for c in _split_column_list(value) if _is_simple_identifier(c)]
    seen = set()
    duplicates = set()
    for col in cols:
        col_lower = col.lower()
        if col_lower in seen:
            duplicates.add(col)
        else:
            seen.add(col_lower)
    return sorted(duplicates, key=str.lower)


def _normalize_column_set(value: str) -> Set[str]:
    return {c.lower() for c in _split_column_list(value) if _is_simple_identifier(c)}


def _extract_join_condition_columns(join_condition: str) -> Dict[str, Set[str]]:
    result = {'a': set(), 'b': set()}
    matches = re.findall(r'\b([ab])\s*\.\s*(`[^`]+`|\[[^\]]+\]|[A-Za-z_][A-Za-z0-9_]*)', join_condition, flags=re.IGNORECASE)
    for alias, col in matches:
        col_norm = _normalize_column_token(col)
        if _is_simple_identifier(col_norm):
            result[alias.lower()].add(col_norm.lower())
    return result


def get_table_ids_from_file(file_path: Path) -> Dict[int, str]:
    """Extract Table_IDs from a metadata SQL file and return dict of {table_id: file_name}."""
    table_ids = {}
    try:
        content = file_path.read_text(encoding='utf-8')
        sections = find_insert_sections(content)
        if 'orchestration' in sections:
            orch_rows = parse_orchestration_rows(
                sections['orchestration']['lines'],
                sections['orchestration']['start']
            )
            for row in orch_rows:
                table_ids[row['table_id']] = file_path.name
    except FileNotFoundError:
        print(f"⚠️  Warning: File not found during cross-file check: {file_path}")
    except PermissionError:
        print(f"⚠️  Warning: Permission denied reading file: {file_path}")
    except UnicodeDecodeError as e:
        print(f"⚠️  Warning: Encoding error reading file {file_path}: {e}")
    except Exception as e:
        print(f"⚠️  Warning: Could not read file {file_path} for cross-file validation: {e}")
    return table_ids


def precompute_cross_file_table_ids(metadata_dir: Path) -> Dict[int, str]:
    """
    Precompute a map of Table_ID -> notebook folder name for all metadata notebooks in a directory.
    
    This function should be called once when validating multiple files to avoid O(N²) scanning.
    
    Args:
        metadata_dir: Path to the metadata/ directory containing .Notebook folders
        
    Returns:
        Dict mapping Table_ID to the .Notebook folder name that defines it
    """
    all_table_ids: Dict[int, str] = {}
    
    for sql_file in metadata_dir.glob("*.Notebook/notebook-content.sql"):
        file_table_ids = get_table_ids_from_file(sql_file)
        # Store with the .Notebook folder name for better error messages
        for table_id in file_table_ids:
            all_table_ids[table_id] = sql_file.parent.name
    
    return all_table_ids


def precompute_cross_file_target_entities(metadata_dir: Path) -> Dict[str, str]:
    """
    Precompute a map of "datastore|entity" -> notebook folder name for all metadata notebooks.
    
    This function should be called once when validating multiple files to avoid O(N²) scanning.
    
    Args:
        metadata_dir: Path to the metadata/ directory containing .Notebook folders
        
    Returns:
        Dict mapping "datastore|entity" key to the .Notebook folder name that defines it
    """
    all_target_entities: Dict[str, str] = {}
    
    for sql_file in metadata_dir.glob("*.Notebook/notebook-content.sql"):
        file_entities = get_target_entities_from_file(sql_file)
        # Store with the .Notebook folder name for better error messages
        for key in file_entities:
            all_target_entities[key] = sql_file.parent.name
    
    return all_target_entities


def get_target_entities_from_file(file_path: Path) -> Dict[str, str]:
    """Extract Target_Datastore+Target_Entity combinations from a metadata SQL file.
    
    Returns dict of {"datastore|entity": file_name}.
    """
    target_entities = {}
    try:
        content = file_path.read_text(encoding='utf-8')
        sections = find_insert_sections(content)
        if 'orchestration' in sections:
            orch_rows = parse_orchestration_rows(
                sections['orchestration']['lines'],
                sections['orchestration']['start']
            )
            for row in orch_rows:
                key = f"{row['target_datastore'].lower()}|{row['target_entity'].lower()}"
                target_entities[key] = file_path.name
    except FileNotFoundError:
        print(f"⚠️  Warning: File not found during cross-file check: {file_path}")
    except PermissionError:
        print(f"⚠️  Warning: Permission denied reading file: {file_path}")
    except UnicodeDecodeError as e:
        print(f"⚠️  Warning: Encoding error reading file {file_path}: {e}")
    except Exception as e:
        print(f"⚠️  Warning: Could not read file {file_path} for cross-file validation: {e}")
    return target_entities


def validate_table_id_uniqueness_across_directory(
    result: ValidationResult,
    current_file: Path,
    current_orch_rows: list,
    precomputed_table_id_map: Optional[Dict[int, str]] = None
):
    """
    Validate that Table_IDs in the current file are unique across all metadata SQL files
    in the same directory. This prevents accidental Table_ID collisions when multiple
    files define entities for the same data platform.
    
    Args:
        result: ValidationResult to add issues to
        current_file: Path to the current metadata SQL file
        current_orch_rows: Parsed orchestration rows from the current file
        precomputed_table_id_map: Optional precomputed map from precompute_cross_file_table_ids().
            If provided, avoids rescanning all sibling files (O(1) lookup instead of O(N)).
    """
    if not current_orch_rows:
        return

    # metadata/<TriggerName>.Notebook/notebook-content.sql
    if not current_file.parent.name.endswith('.Notebook'):
        return  # Not in expected format
    
    current_notebook_name = current_file.parent.name
    
    # Use precomputed map if available, otherwise compute on the fly
    if precomputed_table_id_map is not None:
        all_table_ids = precomputed_table_id_map
    else:
        # Fallback: compute the map for single-file validation
        directory = current_file.parent.parent  # Go up to metadata/
        all_table_ids = precompute_cross_file_table_ids(directory)

    # Check current file's Table_IDs against other files (exclude self)
    for row in current_orch_rows:
        table_id = row['table_id']
        if table_id in all_table_ids:
            owning_notebook = all_table_ids[table_id]
            # Only report if owned by a DIFFERENT notebook
            if owning_notebook != current_notebook_name:
                result.add_issue(
                    'error',
                    'cross_file_duplicate',
                    f"Table_ID {table_id} already exists in '{owning_notebook}'",
                    table_id=table_id,
                    line_number=row['line_number'],
                    suggestion=f"Choose a unique Table_ID. Check other metadata files in the directory for existing IDs."
                )


def validate_target_entity_uniqueness_across_directory(
    result: ValidationResult,
    current_file: Path,
    current_orch_rows: list,
    precomputed_target_entity_map: Optional[Dict[str, str]] = None
):
    """
    Validate that Target_Datastore + Target_Entity combinations are unique across all
    metadata SQL files in the same directory. This prevents multiple pipelines from
    writing to the same target table.
    
    Args:
        result: ValidationResult to add issues to
        current_file: Path to the current metadata SQL file
        current_orch_rows: Parsed orchestration rows from the current file
        precomputed_target_entity_map: Optional precomputed map from precompute_cross_file_target_entities().
            If provided, avoids rescanning all sibling files (O(1) lookup instead of O(N)).
    """
    if not current_orch_rows:
        return

    # metadata/<TriggerName>.Notebook/notebook-content.sql
    if not current_file.parent.name.endswith('.Notebook'):
        return  # Not in expected format
    
    current_notebook_name = current_file.parent.name
    
    # Use precomputed map if available, otherwise compute on the fly
    if precomputed_target_entity_map is not None:
        all_target_entities = precomputed_target_entity_map
    else:
        # Fallback: compute the map for single-file validation
        directory = current_file.parent.parent  # Go up to metadata/
        all_target_entities = precompute_cross_file_target_entities(directory)

    # Check current file's target entities against other files (exclude self)
    for row in current_orch_rows:
        key = f"{row['target_datastore'].lower()}|{row['target_entity'].lower()}"
        if key in all_target_entities:
            owning_notebook = all_target_entities[key]
            # Only report if owned by a DIFFERENT notebook
            if owning_notebook != current_notebook_name:
                exception_reason = row.get('target_entity_exception_reason')
                if exception_reason:
                    result.add_issue(
                        'warning',
                        'cross_file_duplicate_exception',
                        f"Target entity '{row['target_datastore']}.{row['target_entity']}' already exists in '{owning_notebook}', but this row is explicitly marked with '-- exception: ...'",
                        table_id=row['table_id'],
                        line_number=row['line_number'],
                        suggestion=f"Exception reason: {exception_reason}. Ensure triggers are orchestrated to avoid concurrent writes and keep this exception comment up to date."
                    )
                else:
                    result.add_issue(
                        'error',
                        'cross_file_duplicate',
                        f"Target entity '{row['target_datastore']}.{row['target_entity']}' already exists in '{owning_notebook}'",
                        table_id=row['table_id'],
                        line_number=row['line_number'],
                        suggestion=(
                            f"Default rule: one writer per target table. This Table_ID targets the same table as another Table_ID already defined in '{owning_notebook}'. "
                            f"If this duplicate writer is intentional (for example: historical backfill or late-arriving correction), annotate THIS orchestration row with a same-line exception comment using '-- exception: <reason>'. "
                            f"Example: ('LateArriving_Repair', 1, {row['table_id']}, '{row['target_datastore']}', '{row['target_entity']}', '<primary_keys>', 'batch', 1) -- exception: late-arriving correction process. "
                            f"If not intentional, redesign metadata so only one Table_ID writes '{row['target_datastore']}.{row['target_entity']}'."
                        )
                    )


# =============================================================================
# NEW FORMAT/STYLE VALIDATIONS (Rules 20-33)
# =============================================================================

def validate_delete_statements(result: ValidationResult, content: str):
    """
    Validate DELETE statements use Trigger_Name subquery, not hardcoded Table_IDs.
    """
    lines = content.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        line_upper = line.upper().strip()
        
        # Look for DELETE FROM [dbo.]Data_Pipeline_* statements (dbo. prefix optional)
        if line_upper.startswith('DELETE FROM') and 'DATA_PIPELINE_' in line_upper:
            # Check if this line or subsequent lines contain WHERE Table_ID IN (number)
            # That's the WRONG pattern - should use subquery
            
            # Look ahead for the WHERE clause
            check_lines = []
            for i in range(line_num - 1, min(line_num + 5, len(lines))):
                check_lines.append(lines[i])
            check_text = ' '.join(check_lines)
            
            # Pattern for hardcoded IDs: WHERE Table_ID IN (1, 2, 3) or WHERE Table_ID = 5
            hardcoded_pattern = r'WHERE\s+Table_ID\s+(IN\s*\(\s*\d+|=\s*\d+)'
            if re.search(hardcoded_pattern, check_text, re.IGNORECASE):
                # Make sure it's not a subquery (would have SELECT in it)
                if 'SELECT' not in check_text.upper():
                    result.add_issue('error', 'delete_format',
                        f"DELETE statement uses hardcoded Table_IDs instead of Trigger_Name subquery",
                        line_number=line_num,
                        suggestion="Use: WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Orchestration WHERE Trigger_Name = 'YourTrigger')")


def validate_comment_style(result: ValidationResult, content: str):
    """
    Validate only single-line comments (--) are used, not multi-line comments.
    Ignores /* inside string literals (e.g., wildcard paths like 'oracle/sales/*/*.parquet').
    """
    lines = content.split('\n')
    in_block_comment = False
    
    for line_num, line in enumerate(lines, 1):
        # Skip lines that are pure comments
        stripped = line.strip()
        if stripped.startswith('--'):
            continue
        
        # Remove string literals before checking for /*
        # This handles wildcard paths like 'oracle/sales/*/*.parquet'
        line_without_strings = re.sub(r"'[^']*'", '', line)
        
        # Check for block comment start outside of strings
        if '/*' in line_without_strings:
            in_block_comment = True
            result.add_issue('error', 'comment_style',
                f"Multi-line comment /* */ detected. Use only -- single-line comments.",
                line_number=line_num,
                suggestion="Replace /* comment */ with -- comment (one per line)")
        
        # Check for block comment end
        if '*/' in line_without_strings and in_block_comment:
            in_block_comment = False


def validate_multiline_values_rows(result: ValidationResult, content: str):
    """
    Validate that each VALUES row is on a single line (not split across lines).
    """
    lines = content.split('\n')
    in_values_block = False
    
    for line_num, line in enumerate(lines, 1):
        stripped = line.strip()
        
        # Detect start of VALUES block
        if stripped.upper().startswith('VALUES'):
            in_values_block = True
            continue
        
        if in_values_block:
            # Check if we've exited the VALUES block (hit a new statement)
            if stripped.upper().startswith(('INSERT', 'DELETE', 'UPDATE', 'SELECT', '--')):
                in_values_block = False
                continue
            
            # Skip empty lines
            if not stripped:
                continue
            
            # A VALUES row should start with ( and end with ) or ),
            # If it starts with ( but doesn't end with ) or ), it's split
            if stripped.startswith('(') and not stripped.startswith('(('):
                # Check if this row is complete (ends with ) or ), or ); which we'd catch separately)
                if not (stripped.endswith(')') or stripped.endswith('),')):
                    result.add_issue('warning', 'multiline_values',
                        f"VALUES row appears to span multiple lines. Each row should be on one line.",
                        line_number=line_num,
                        suggestion="Combine the row onto a single line for clarity")


def validate_conflicting_configs(result: ValidationResult, primary_rows: list, advanced_rows: list):
    """
    Validate for conflicting configuration combinations.
    - Watermark_Column + merge_type='overwrite' is invalid
    - SCD2 columns + merge_type='overwrite' is invalid
    """
    # Build lookup for merge_type per Table_ID from primary config
    merge_types = {}
    for row in primary_rows:
        if row['name'].lower() == 'merge_type':
            merge_types[row['table_id']] = row['value'].lower().strip("'\"")

    watermark_tables = set()
    for row in primary_rows:
        if row.get('watermark_column'):
            watermark_tables.add(row['table_id'])
    
    # Build lookup for SCD2 columns per Table_ID from advanced config
    scd2_tables = set()
    scd2_attrs = {'scd2_effective_date_column', 'scd2_end_date_column', 'scd2_current_flag_column'}
    for row in advanced_rows:
        if row['attribute'].lower() in scd2_attrs:
            scd2_tables.add(row['table_id'])

    # Check conflicts
    for table_id, merge_type in merge_types.items():
        if merge_type == 'overwrite':
            if table_id in watermark_tables:
                result.add_issue('error', 'config_conflict',
                    f"Table_ID {table_id} has Watermark_Column but merge_type='overwrite'. "
                    "Watermark requires incremental processing (merge_type='upsert' or 'append').",
                    table_id=table_id,
                    suggestion="Change merge_type to 'upsert' or 'append', or remove Watermark_Column")
            if table_id in scd2_tables:
                result.add_issue('error', 'config_conflict',
                    f"Table_ID {table_id} has SCD2 columns but merge_type='overwrite'. "
                    "SCD2 requires merge_type='scd2'.",
                    table_id=table_id,
                    suggestion="Change merge_type to 'scd2' for SCD Type 2 processing")


def validate_custom_ingestion_table_name_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 80: Validate that custom ingestion functions don't coexist with table_name in source_details.
    
    In parse_source_configuration() (NB_Helper_Functions_3), the routing logic is:
      - Branch 2: if custom_table_ingestion_function or custom_file_ingestion_function → skips table_name
      - Branch 3: else → reads table_name to build source_query
    
    When both are set, table_name is silently ignored (dead metadata), which causes confusion.
    """
    # Build per-Table_ID lookup for source_details attributes
    source_attrs_by_table: Dict[int, Dict[str, int]] = defaultdict(dict)
    for row in primary_rows:
        if row['category'] == 'source_details':
            source_attrs_by_table[row['table_id']][row['name']] = row['line_number']

    custom_ingestion_attrs = {'custom_table_ingestion_function', 'custom_file_ingestion_function', 'custom_source_function'}

    for table_id, attrs in source_attrs_by_table.items():
        has_table_name = 'table_name' in attrs
        custom_funcs = custom_ingestion_attrs & set(attrs.keys())

        if has_table_name and custom_funcs:
            func_names = ', '.join(sorted(custom_funcs))
            # Report on the table_name line since that's the ignored attribute
            line_num = attrs.get('table_name', 0)
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has both 'table_name' and '{func_names}' in source_details. "
                "REASON: In NB_Helper_Functions_3.parse_source_configuration(), custom ingestion functions "
                "cause Branch 2 to execute, which completely skips reading table_name (Branch 3). "
                "The table_name value is never used — source_query stays empty and the custom function "
                "manages its own data retrieval. This means table_name is dead metadata that creates a "
                "false impression of where data is sourced from.",
                table_id=table_id,
                line_number=line_num,
                suggestion=f"Remove 'table_name' from source_details for Table_ID {table_id}. "
                    "The custom ingestion function manages its own data retrieval via the all_metadata dict.")


def validate_cdf_soft_delete_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 81: Validate that use_change_data_feed=true doesn't coexist with user-set soft delete columns.

    In parse_watermark_configuration() (NB_Helper_Functions_3), when use_change_data_feed is enabled,
    column_to_mark_source_data_deletion is hardcoded to '_change_type' and delete_rows_with_value
    is hardcoded to 'delete', discarding any user-provided values without error.
    """
    configs_by_table: Dict[int, Dict[str, Tuple[str, int]]] = defaultdict(dict)
    for row in primary_rows:
        cat, name, val, ln = row['category'], row['name'], row['value'], row['line_number']
        if cat in ('watermark_details', 'target_details'):
            configs_by_table[row['table_id']][(cat, name)] = (val, ln)

    for table_id, attrs in configs_by_table.items():
        cdf_val, _ = attrs.get(('watermark_details', 'use_change_data_feed'), ('', 0))
        if cdf_val.lower() != 'true':
            continue

        del_col_val, del_col_ln = attrs.get(('target_details', 'column_to_mark_source_data_deletion'), ('', 0))
        del_val_val, del_val_ln = attrs.get(('target_details', 'delete_rows_with_value'), ('', 0))

        if del_col_val:
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has use_change_data_feed=true AND "
                f"column_to_mark_source_data_deletion='{del_col_val}'. "
                "REASON: In NB_Helper_Functions_3.parse_watermark_configuration(), CDF unconditionally "
                "overwrites column_to_mark_source_data_deletion to '_change_type' and delete_rows_with_value "
                f"to 'delete'. Your value '{del_col_val}' is silently discarded at runtime.",
                table_id=table_id, line_number=del_col_ln,
                suggestion=f"Remove 'column_to_mark_source_data_deletion' for Table_ID {table_id}. "
                    "CDF automatically uses '_change_type' column for tracking deletions.")

        if del_val_val and del_val_val.lower() != 'delete':
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has use_change_data_feed=true AND "
                f"delete_rows_with_value='{del_val_val}'. "
                "REASON: In NB_Helper_Functions_3.parse_watermark_configuration(), CDF unconditionally "
                f"overwrites delete_rows_with_value to 'delete'. Your value '{del_val_val}' is silently "
                "discarded at runtime.",
                table_id=table_id, line_number=del_val_ln,
                suggestion=f"Remove 'delete_rows_with_value' for Table_ID {table_id}. "
                    "CDF automatically uses 'delete' as the deletion marker value.")


def validate_cdf_watermark_column_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 82: Validate that use_change_data_feed=true doesn't coexist with watermark column settings.

    In parse_watermark_configuration() (NB_Helper_Functions_3), when CDF is enabled,
    watermark_column_name and watermark_column_data_type are both set to empty strings.
    CDF uses _commit_version internally for incremental processing.
    """
    configs_by_table: Dict[int, Dict[str, Tuple[str, int]]] = defaultdict(dict)
    for row in primary_rows:
        if row['category'] == 'watermark_details':
            configs_by_table[row['table_id']][row['name']] = (row['value'], row['line_number'])

    for table_id, attrs in configs_by_table.items():
        cdf_val, _ = attrs.get('use_change_data_feed', ('', 0))
        if cdf_val.lower() != 'true':
            continue

        col_val, col_ln = attrs.get('column_name', ('', 0))
        dtype_val, dtype_ln = attrs.get('data_type', ('', 0))

        if col_val:
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has use_change_data_feed=true AND "
                f"watermark column_name='{col_val}'. "
                "REASON: In NB_Helper_Functions_3.parse_watermark_configuration(), CDF clears "
                "watermark_column_name to '' because CDF uses _commit_version for incremental tracking. "
                f"Your watermark column '{col_val}' is silently discarded and never used for filtering.",
                table_id=table_id, line_number=col_ln,
                suggestion=f"Remove 'column_name' from watermark_details for Table_ID {table_id}. "
                    "CDF uses _commit_version automatically for incremental processing.")

        if dtype_val:
            result.add_issue('warning', 'config_conflict',
                f"Table_ID {table_id} has use_change_data_feed=true AND "
                f"watermark data_type='{dtype_val}'. "
                "REASON: CDF clears watermark_column_data_type to '' in parse_watermark_configuration(). "
                "This is dead metadata since CDF manages its own watermark tracking.",
                table_id=table_id, line_number=dtype_ln,
                suggestion=f"Remove 'data_type' from watermark_details for Table_ID {table_id}.")


def validate_staging_wildcard_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 84: Validate that staging_folder_path and wildcard_folder_path aren't both set.

    In parse_source_configuration() (NB_Helper_Functions_3), source_path is computed as:
        source_path = staging_folder_path or wildcard_folder_path or folder_path_from_trigger
    Python's `or` short-circuits — if staging_folder_path is truthy, wildcard_folder_path
    is never evaluated, making it dead metadata.
    """
    configs_by_table: Dict[int, Dict[str, Tuple[str, int]]] = defaultdict(dict)
    for row in primary_rows:
        if row['category'] == 'source_details' and row['name'] in ('staging_folder_path', 'wildcard_folder_path'):
            configs_by_table[row['table_id']][row['name']] = (row['value'], row['line_number'])

    for table_id, attrs in configs_by_table.items():
        staging_val, _ = attrs.get('staging_folder_path', ('', 0))
        wildcard_val, wildcard_ln = attrs.get('wildcard_folder_path', ('', 0))

        if staging_val and wildcard_val:
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has both staging_folder_path and wildcard_folder_path. "
                "REASON: In NB_Helper_Functions_3.parse_source_configuration(), source_path is "
                "computed as 'staging_folder_path or wildcard_folder_path'. Python's `or` short-circuits: "
                f"staging_folder_path='{staging_val}' is truthy, so wildcard_folder_path='{wildcard_val}' "
                "is never used. Your wildcard pattern is dead metadata — file pattern matching never occurs.",
                table_id=table_id, line_number=wildcard_ln,
                suggestion=f"Remove 'wildcard_folder_path' for Table_ID {table_id} if using staging ingestion, "
                    "or remove 'staging_folder_path' if using wildcard file pattern matching.")


def validate_staging_custom_table_ingestion_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 85: Validate that staging_folder_path doesn't coexist with custom_table_ingestion_function.

    In route_to_ingestion_method() (NB_Helper_Functions_1), the first check is
    source_config['using_source_folder_path'] — when staging_folder_path is set, this is True,
    routing to ingest_raw_files(). custom_table_ingestion_function is only checked inside
    ingest_from_table() (the else branch), which is never reached.
    """
    configs_by_table: Dict[int, Dict[str, Tuple[str, int]]] = defaultdict(dict)
    source_types: Dict[int, str] = {}
    for row in primary_rows:
        if row['category'] == 'source_details' and row['name'] in ('staging_folder_path', 'custom_table_ingestion_function'):
            configs_by_table[row['table_id']][row['name']] = (row['value'], row['line_number'])
        if row['category'] == 'source_details' and row['name'] == 'source':
            source_types[row['table_id']] = (row['value'] or '').strip().lower()

    for table_id, attrs in configs_by_table.items():
        staging_val, _ = attrs.get('staging_folder_path', ('', 0))
        custom_val, custom_ln = attrs.get('custom_table_ingestion_function', ('', 0))
        source_type = source_types.get(table_id, '')

        if staging_val and custom_val:
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has both staging_folder_path and custom_table_ingestion_function. "
                "REASON: In NB_Helper_Functions_1.route_to_ingestion_method(), staging_folder_path makes "
                "using_source_folder_path=True, which routes to ingest_raw_files(). "
                f"custom_table_ingestion_function='{custom_val}' is only checked inside "
                "ingest_from_table() (the else branch), which is never reached. "
                "Your custom table function is dead metadata — never instantiated or invoked.",
                table_id=table_id, line_number=custom_ln,
                suggestion=f"Remove 'custom_table_ingestion_function' for Table_ID {table_id} if using staging, "
                    "or remove 'staging_folder_path' if using custom table ingestion. "
                    "Note: custom_file_ingestion_function IS compatible with staging (it's passed through ingest_raw_files).")


def validate_replace_where_column_without_merge_type(result: ValidationResult, primary_rows: list):
    """
    Rule 86: Validate that replace_where_column is not set without merge_type='replace_where'.

    In merge_data() (NB_Helper_Functions_1), replace_where_column is only consumed when
    merge_type='replace_where' — the _handle_replace_where() function. All other merge
    branches completely ignore it, making it dead metadata.

    Note: The existing Rule 38 checks the forward direction (replace_where merge requires column).
    This rule checks the reverse (column present without replace_where merge type).
    """
    merge_types: Dict[int, str] = {}
    replace_where_cols: Dict[int, Tuple[str, int]] = {}

    for row in primary_rows:
        if row['category'] == 'target_details':
            if row['name'] == 'merge_type':
                merge_types[row['table_id']] = row['value'].lower().strip("'\"")
            elif row['name'] == 'replace_where_column':
                replace_where_cols[row['table_id']] = (row['value'], row['line_number'])

    for table_id, (col_val, col_ln) in replace_where_cols.items():
        mt = merge_types.get(table_id, '')
        if mt and mt != 'replace_where':
            result.add_issue('warning', 'config_conflict',
                f"Table_ID {table_id} has replace_where_column='{col_val}' but merge_type='{mt}'. "
                "REASON: In NB_Helper_Functions_1.merge_data(), replace_where_column is only passed to "
                "_handle_replace_where() when merge_type='replace_where'. With merge_type='{mt}', "
                "the replace_where branch is never entered and this column config is dead metadata.".format(mt=mt),
                table_id=table_id, line_number=col_ln,
                suggestion=f"Remove 'replace_where_column' if merge_type should stay '{mt}', "
                    "or change merge_type to 'replace_where' to use partition-based replacement.")


# Merge types that actually use delete_rows_with_value / column_to_mark_source_data_deletion
# in NB_Helper_Functions_1.merge_data(). All other merge types silently ignore these columns.


def validate_soft_delete_merge_type_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 87: Validate that soft delete configs aren't used with a merge_type that ignores them.

    In parse_watermark_configuration() (NB_Helper_Functions_3), when delete_rows_with_value is set,
    the default merge_type becomes 'merge_and_delete'. But if the user explicitly sets
    merge_type to a value that doesn't use deletion columns (e.g. 'merge', 'append', 'overwrite',
    'replace_where', 'output_file'), the columns are silently ignored.

    Only these merge types actually use soft delete columns in merge_data():
        - merge_and_delete
        - merge_mark_unmatched_deleted
        - merge_mark_all_deleted
        - scd2
    """
    merge_types: Dict[int, Tuple[str, int]] = {}
    soft_delete_configs: Dict[int, Dict[str, Tuple[str, int]]] = defaultdict(dict)

    for row in primary_rows:
        if row['category'] == 'target_details':
            if row['name'] == 'merge_type':
                merge_types[row['table_id']] = (row['value'].lower().strip("'\""), row['line_number'])
            elif row['name'] in ('column_to_mark_source_data_deletion', 'delete_rows_with_value'):
                soft_delete_configs[row['table_id']][row['name']] = (row['value'], row['line_number'])

    for table_id, sd_attrs in soft_delete_configs.items():
        mt_val, mt_ln = merge_types.get(table_id, ('', 0))
        has_delete_col = 'column_to_mark_source_data_deletion' in sd_attrs
        has_delete_val = 'delete_rows_with_value' in sd_attrs

        # Only flag when user explicitly set a merge_type that ignores deletion columns
        if mt_val and mt_val not in DELETION_AWARE_MERGE_TYPES and (has_delete_col or has_delete_val):
            sd_names = ', '.join(sorted(sd_attrs.keys()))
            deletion_aware_list = ', '.join(sorted(DELETION_AWARE_MERGE_TYPES))
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has soft delete config ({sd_names}) but merge_type='{mt_val}'. "
                f"REASON: In NB_Helper_Functions_1.merge_data(), only these merge types use soft delete "
                f"columns: [{deletion_aware_list}]. The '{mt_val}' handler completely ignores "
                f"column_to_mark_source_data_deletion and delete_rows_with_value. Source deletions "
                f"will not be tracked or processed.",
                table_id=table_id, line_number=mt_ln,
                suggestion=f"Change merge_type to one of [{deletion_aware_list}], "
                    "or remove the soft delete configs if this merge type is intended.")


def validate_compute_statistics_conflict(result: ValidationResult, primary_rows: list):
    """
    Rule 88: Validate that both compute_statistics_on_columns and compute_statistics_on_first_n_columns
    aren't set simultaneously.

    In build_statistics_columns_config() (NB_Helper_Functions_1), the logic is:
        if compute_statistics_on_columns: → use specific columns
        elif compute_statistics_on_first_n_columns: → use first N columns
    The elif means compute_statistics_on_first_n_columns is silently ignored when specific columns are set.
    """
    stats_cols: Dict[int, Tuple[str, int]] = {}
    stats_n: Dict[int, Tuple[str, int]] = {}

    for row in primary_rows:
        if row['category'] == 'target_details':
            if row['name'] == 'compute_statistics_on_columns':
                stats_cols[row['table_id']] = (row['value'], row['line_number'])
            elif row['name'] == 'compute_statistics_on_first_n_columns':
                stats_n[row['table_id']] = (row['value'], row['line_number'])

    for table_id in stats_cols.keys() & stats_n.keys():
        n_val, n_ln = stats_n[table_id]
        result.add_issue('warning', 'config_conflict',
            f"Table_ID {table_id} has both compute_statistics_on_columns and "
            f"compute_statistics_on_first_n_columns='{n_val}'. "
            "REASON: In NB_Helper_Functions_1.build_statistics_columns_config(), specific columns are checked first "
            "(if branch), and first-N is an elif branch that is never reached when specific columns exist. "
            f"compute_statistics_on_first_n_columns='{n_val}' is dead metadata.",
            table_id=table_id, line_number=n_ln,
            suggestion=f"Remove 'compute_statistics_on_first_n_columns' for Table_ID {table_id} since "
                "specific columns are already listed in compute_statistics_on_columns.")


def validate_duplicate_pk_check_without_pks(result: ValidationResult, primary_rows: list, orch_rows: list):
    """
    Rule 89: Validate that if_duplicate_primary_keys isn't set without Primary_Keys.

    In _validate_primary_keys() (NB_Helper_Functions_1), the entire validation is wrapped in
    'if primary_keys:' — when Primary_Keys is empty, duplicate checking never occurs and
    if_duplicate_primary_keys is dead metadata regardless of its value.
    """
    pk_by_table: Dict[int, str] = {}
    for row in orch_rows:
        pk_by_table[row['table_id']] = row.get('primary_keys', '').strip()

    dup_pk_check: Dict[int, Tuple[str, int]] = {}
    for row in primary_rows:
        if row['category'] == 'target_details' and row['name'] == 'if_duplicate_primary_keys':
            dup_pk_check[row['table_id']] = (row['value'], row['line_number'])

    for table_id, (val, ln) in dup_pk_check.items():
        pks = pk_by_table.get(table_id, '')
        if not pks:
            result.add_issue('error', 'config_conflict',
                f"Table_ID {table_id} has if_duplicate_primary_keys='{val}' but Primary_Keys is empty. "
                "REASON: In NB_Helper_Functions_1._validate_primary_keys(), the entire duplicate check "
                "is wrapped in 'if primary_keys:'. With no Primary_Keys defined in orchestration, "
                "the duplicate check is completely skipped — this config is dead metadata and duplicates "
                "will silently pass through.",
                table_id=table_id, line_number=ln,
                suggestion=f"Add Primary_Keys in orchestration for Table_ID {table_id} to enable "
                    "duplicate checking, or remove 'if_duplicate_primary_keys' if no PK validation is needed.")


def validate_source_attribute_alignment(result: ValidationResult, orch_rows: list, advanced_rows: list):
    """
    Validate that advanced source_config attributes align with the orchestration route.
    - source attribute requires pipeline-owned staging methods
    - source='delta' is not a valid value
    - Database config attrs require pipeline-owned staging methods
    """
    # Build lookup for Processing_Method per Table_ID
    processing_methods = {}
    for row in orch_rows:
        processing_methods[row['table_id']] = row['processing_method'].lower()

    source_attrs_by_table = {}
    for row in advanced_rows:
        if row['attribute'].lower() == 'source':
            source_attrs_by_table[row['table_id']] = row['value'].lower().strip("'\"")
    
    # Database config attributes that require an external source route
    db_config_attrs = {'datastore_name', 'database_name', 'schema_name', 'query'}
    
    for row in advanced_rows:
        table_id = row['table_id']
        attr_name = row['attribute'].lower()
        attr_value = row['value'].lower().strip("'\"")
        line_num = row['line_number']
        
        processing_method = processing_methods.get(table_id, '')
        source_attr = source_attrs_by_table.get(table_id, '')
        # source attribute requires a staging-capable route
        if attr_name == 'source' and processing_method != 'batch' and processing_method != '':
            result.add_issue('error', 'source_alignment',
                f"Table_ID {table_id} uses 'source' attribute but Processing_Method='{processing_method}'. "
                "'source' requires Processing_Method='batch'.",
                table_id=table_id,
                line_number=line_num,
                suggestion="Change Processing_Method to 'batch' in Orchestration, or remove 'source' for notebook/job-owned ingestion patterns.")
        
        # source='delta' is invalid
        if attr_name == 'source' and attr_value == 'delta':
            result.add_issue('error', 'invalid_source',
                f"Table_ID {table_id} has source='delta' which is not a valid source type. "
                "Valid sources: parquet, csv, json, xml, sql_server, oracle, mysql, postgresql, etc.",
                table_id=table_id,
                line_number=line_num,
                suggestion="Use the actual data source format, not 'delta'. Delta is the target format.")
        
        # Database config attrs require an external source route
        if attr_name in db_config_attrs and processing_method != 'batch' and processing_method != '':
            result.add_issue('error', 'db_config_alignment',
                f"Table_ID {table_id} uses '{row['attribute']}' but Processing_Method='{processing_method}'. "
            "Database connection attributes require Processing_Method='batch'.",
                table_id=table_id,
                line_number=line_num,
            suggestion="Change Processing_Method to 'batch' in Orchestration, or remove source connection attributes from notebook/job-owned ingestion.")


def validate_columns_to_rename_count(result: ValidationResult, advanced_rows: list):
    """
    Validate that existing_column_name and new_column_name have matching counts per Table_ID.
    """
    # Group by Table_ID
    existing_counts = {}
    new_counts = {}
    
    for row in advanced_rows:
        table_id = row['table_id']
        attr_name = row['attribute'].lower()
        attr_value = row['value']
        
        if attr_name == 'existing_column_name':
            # Count items (comma-separated)
            items = [x.strip() for x in attr_value.strip("'\"").split(',') if x.strip()]
            existing_counts[table_id] = existing_counts.get(table_id, 0) + len(items)
        
        elif attr_name == 'new_column_name':
            items = [x.strip() for x in attr_value.strip("'\"").split(',') if x.strip()]
            new_counts[table_id] = new_counts.get(table_id, 0) + len(items)
    
    # Check for mismatches
    all_table_ids = set(existing_counts.keys()) | set(new_counts.keys())
    for table_id in all_table_ids:
        existing = existing_counts.get(table_id, 0)
        new = new_counts.get(table_id, 0)
        
        if existing != new:
            result.add_issue('error', 'rename_count_mismatch',
                f"Table_ID {table_id}: existing_column_name has {existing} columns but new_column_name has {new}. "
                "These must match.",
                table_id=table_id,
                suggestion="Ensure each existing_column_name has a corresponding new_column_name")


def validate_category_ordering(result: ValidationResult, advanced_rows: list):
    """
    Validate that all data_transformation_steps come before data_quality per Table_ID.
    """
    # Group rows by Table_ID preserving order
    table_rows = {}
    for row in advanced_rows:
        table_id = row['table_id']
        if table_id not in table_rows:
            table_rows[table_id] = []
        table_rows[table_id].append(row)
    
    for table_id, rows in table_rows.items():
        seen_dq = False
        for row in rows:
            category = row.get('category', '').lower()
            
            if category == 'data_quality':
                seen_dq = True
            elif category == 'data_transformation_steps' and seen_dq:
                result.add_issue('warning', 'category_order',
                    f"Table_ID {table_id}: data_transformation_steps found after data_quality. "
                    "Convention is transformations before quality checks.",
                    table_id=table_id,
                    line_number=row['line_number'],
                    suggestion="Reorder Advanced Config rows so all data_transformation_steps come before data_quality")


def validate_file_section_order(result: ValidationResult, content: str):
    """
    Validate that file sections appear in correct order:
    DELETE statements → Orchestration INSERT → Primary Config INSERT → Advanced Config INSERT
    Note: Matches both short (Data_Pipeline_Advanced_Config) and full (Data_Pipeline_Metadata_Advanced_Configuration) names.
    """
    # Find positions of each section type
    positions = {
        'delete_advanced': None,
        'delete_primary': None,
        'delete_orchestration': None,
        'insert_orchestration': None,
        'insert_primary': None,
        'insert_advanced': None
    }
    
    lines = content.split('\n')
    for line_num, line in enumerate(lines, 1):
        line_upper = line.upper().strip()
        
        # Match DELETE statements (flexible pattern for various table naming)
        if line_upper.startswith('DELETE FROM') and 'DATA_PIPELINE_' in line_upper:
            if 'ADVANCED' in line_upper:
                if positions['delete_advanced'] is None:
                    positions['delete_advanced'] = line_num
            elif 'PRIMARY' in line_upper:
                if positions['delete_primary'] is None:
                    positions['delete_primary'] = line_num
            elif 'ORCHESTRATION' in line_upper:
                if positions['delete_orchestration'] is None:
                    positions['delete_orchestration'] = line_num
        
        # Match INSERT statements (flexible pattern for various table naming)
        elif line_upper.startswith('INSERT INTO') and 'DATA_PIPELINE_' in line_upper:
            if 'ORCHESTRATION' in line_upper:
                if positions['insert_orchestration'] is None:
                    positions['insert_orchestration'] = line_num
            elif 'PRIMARY' in line_upper:
                if positions['insert_primary'] is None:
                    positions['insert_primary'] = line_num
            elif 'ADVANCED' in line_upper:
                if positions['insert_advanced'] is None:
                    positions['insert_advanced'] = line_num
    
    # Expected order: DELETE (Advanced → Primary → Orch) → INSERT (Orch → Primary → Advanced)
    expected_order = [
        ('delete_advanced', 'delete_primary'),
        ('delete_primary', 'delete_orchestration'),
        ('delete_orchestration', 'insert_orchestration'),
        ('insert_orchestration', 'insert_primary'),
        ('insert_primary', 'insert_advanced')
    ]
    
    for first, second in expected_order:
        pos1 = positions.get(first)
        pos2 = positions.get(second)
        
        if pos1 and pos2 and pos1 > pos2:
            result.add_issue('warning', 'section_order',
                f"Section order issue: {first.replace('_', ' ')} (line {pos1}) should come before "
                f"{second.replace('_', ' ')} (line {pos2})",
                line_number=pos1,
                suggestion="Reorder sections: DELETE (Advanced→Primary→Orch) then INSERT (Orch→Primary→Advanced)")


def validate_delete_order(result: ValidationResult, content: str):
    """
    Validate DELETE statements appear in correct order: Advanced → Primary → Orchestration.
    Note: Matches both short (Data_Pipeline_Advanced_Config) and full (Data_Pipeline_Metadata_Advanced_Configuration) names.
    """
    positions = []
    lines = content.split('\n')
    
    for line_num, line in enumerate(lines, 1):
        line_upper = line.upper().strip()
        
        if line_upper.startswith('DELETE FROM') and 'DATA_PIPELINE_' in line_upper:
            if 'ADVANCED' in line_upper:
                positions.append(('Advanced_Config', line_num))
            elif 'PRIMARY' in line_upper:
                positions.append(('Primary_Config', line_num))
            elif 'ORCHESTRATION' in line_upper:
                positions.append(('Orchestration', line_num))
    
    # Check order: Advanced should come before Primary, Primary before Orchestration
    advanced_pos = primary_pos = orch_pos = None
    for table, pos in positions:
        if table == 'Advanced_Config' and advanced_pos is None:
            advanced_pos = pos
        elif table == 'Primary_Config' and primary_pos is None:
            primary_pos = pos
        elif table == 'Orchestration' and orch_pos is None:
            orch_pos = pos
    
    if advanced_pos and primary_pos and advanced_pos > primary_pos:
        result.add_issue('error', 'delete_order',
            f"DELETE from Advanced_Config (line {advanced_pos}) must come BEFORE DELETE from Primary_Config (line {primary_pos})",
            line_number=advanced_pos,
            suggestion="Delete child tables before parent tables to avoid FK constraint issues")
    
    if primary_pos and orch_pos and primary_pos > orch_pos:
        result.add_issue('error', 'delete_order',
            f"DELETE from Primary_Config (line {primary_pos}) must come BEFORE DELETE from Orchestration (line {orch_pos})",
            line_number=primary_pos,
            suggestion="Delete child tables before parent tables to avoid FK constraint issues")


def validate_delete_presence(result: ValidationResult, content: str):
    """
    Validate that all three DELETE statements are present.
    Note: Matches both short names (Data_Pipeline_Advanced_Config) and full names 
    (Data_Pipeline_Metadata_Advanced_Configuration).
    """
    content_upper = content.upper()
    
    has_delete_advanced = bool(re.search(r'DELETE\s+FROM\s+(?:dbo\.)?Data_Pipeline_\w*Advanced', content, re.IGNORECASE))
    has_delete_primary = bool(re.search(r'DELETE\s+FROM\s+(?:dbo\.)?Data_Pipeline_\w*Primary', content, re.IGNORECASE))
    has_delete_orch = bool(re.search(r'DELETE\s+FROM\s+(?:dbo\.)?Data_Pipeline_\w*Orchestration', content, re.IGNORECASE))
    
    if not has_delete_advanced:
        result.add_issue('warning', 'missing_delete',
            "Missing DELETE FROM Data_Pipeline_*_Advanced_Config* statement",
            suggestion="Add DELETE statement for clean re-runs: DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'YourTrigger')")
    
    if not has_delete_primary:
        result.add_issue('warning', 'missing_delete',
            "Missing DELETE FROM Data_Pipeline_*_Primary_Config* statement",
            suggestion="Add DELETE statement for clean re-runs: DELETE FROM Data_Pipeline_Metadata_Primary_Configuration WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'YourTrigger')")
    
    if not has_delete_orch:
        result.add_issue('warning', 'missing_delete',
            "Missing DELETE FROM Data_Pipeline_*_Orchestration statement",
            suggestion="Add DELETE statement for clean re-runs: DELETE FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'YourTrigger'")


# =============================================================================
# NEW VALIDATIONS (Rules 34-41)
# =============================================================================

def validate_new_type_values(result: ValidationResult, advanced_rows: list):
    """
    Validate that change_data_types.new_type has valid PySpark data type values.
    """
    for row in advanced_rows:
        if row['name'].lower() == 'change_data_types' and row['attribute'].lower() == 'new_type':
            new_type = row['value'].strip().strip("'\"").lower()
            
            # Check if it's a valid base type
            if new_type in VALID_NEW_TYPE_BASE_TYPES:
                continue
            
            # Check if it's a valid decimal type (decimal(precision,scale))
            if re.match(r'^decimal\s*\(\s*\d+\s*,\s*\d+\s*\)$', new_type):
                continue
            
            result.add_issue('error', 'invalid_new_type',
                f"Table_ID {row['table_id']}: Invalid new_type '{new_type}' for change_data_types.",
                table_id=row['table_id'],
                line_number=row['line_number'],
                suggestion=(
                    f"Valid types: {', '.join(sorted(VALID_NEW_TYPE_BASE_TYPES))}, decimal(p,s)"
                ))


def validate_transform_datetime_conditionals(result: ValidationResult, advanced_rows: list):
    """
    Validate transform_datetime conditional requirements.
    Certain operations require specific attributes:
    - add_days/subtract_days require 'days'
    - add_months requires 'months'
    - date_diff requires 'end_date_column'
    - format_date requires 'date_format'
    """
    # Group transform_datetime rows by Table_ID and instance
    transform_datetime_instances = {}
    
    for row in advanced_rows:
        if row['name'].lower() == 'transform_datetime':
            key = (row['table_id'], row['instance'])
            if key not in transform_datetime_instances:
                transform_datetime_instances[key] = {}
            transform_datetime_instances[key][row['attribute'].lower()] = row['value'].strip().strip("'\"").lower()
            transform_datetime_instances[key]['line_number'] = row['line_number']
    
    # Check each instance for required conditional attributes
    for (table_id, instance), attrs in transform_datetime_instances.items():
        operation = attrs.get('operation', '')
        line_num = attrs.get('line_number')
        
        if operation in ('add_days', 'subtract_days'):
            if 'days' not in attrs:
                result.add_issue('error', 'missing_conditional_attr',
                    f"Table_ID {table_id}: transform_datetime operation '{operation}' requires 'days' attribute.",
                    table_id=table_id,
                    line_number=line_num,
                    suggestion="Add a 'days' attribute with an integer value")
        
        elif operation == 'add_months':
            if 'months' not in attrs:
                result.add_issue('error', 'missing_conditional_attr',
                    f"Table_ID {table_id}: transform_datetime operation 'add_months' requires 'months' attribute.",
                    table_id=table_id,
                    line_number=line_num,
                    suggestion="Add a 'months' attribute with an integer value")
        
        elif operation == 'date_diff':
            if 'end_date_column' not in attrs:
                result.add_issue('error', 'missing_conditional_attr',
                    f"Table_ID {table_id}: transform_datetime operation 'date_diff' requires 'end_date_column' attribute.",
                    table_id=table_id,
                    line_number=line_num,
                    suggestion="Add an 'end_date_column' attribute specifying the column for the end date")
        
        elif operation == 'format_date':
            if 'date_format' not in attrs:
                result.add_issue('error', 'missing_conditional_attr',
                    f"Table_ID {table_id}: transform_datetime operation 'format_date' requires 'date_format' attribute.",
                    table_id=table_id,
                    line_number=line_num,
                    suggestion="Add a 'date_format' attribute (e.g., 'yyyy-MM-dd')")


def validate_range_requires_bound(result: ValidationResult, advanced_rows: list):
    """
    Validate that validate_range has at least one of min_value OR max_value.
    """
    # Group validate_range rows by Table_ID and instance
    validate_range_instances = {}
    
    for row in advanced_rows:
        if row['name'].lower() == 'validate_range':
            key = (row['table_id'], row['instance'])
            if key not in validate_range_instances:
                validate_range_instances[key] = {'attrs': set(), 'line_number': row['line_number']}
            validate_range_instances[key]['attrs'].add(row['attribute'].lower())
    
    for (table_id, instance), data in validate_range_instances.items():
        attrs = data['attrs']
        if 'min_value' not in attrs and 'max_value' not in attrs:
            result.add_issue('error', 'validate_range_no_bounds',
                f"Table_ID {table_id}: validate_range requires at least one of 'min_value' or 'max_value'.",
                table_id=table_id,
                line_number=data['line_number'],
                suggestion="Add 'min_value' and/or 'max_value' attribute to define the valid range")


def validate_limited_if_not_compliant(result: ValidationResult, advanced_rows: list):
    """
    Validate that validate_freshness and validate_completeness only use 'warn' or 'fail' for if_not_compliant.
    """
    for row in advanced_rows:
        config_name = row['name'].lower()
        if config_name in CHECKS_WITHOUT_ROW_LEVEL_ACTIONS and row['attribute'].lower() == 'if_not_compliant':
            value = row['value'].strip().strip("'\"").lower()
            if value in {'quarantine', 'mark'}:
                result.add_issue('error', 'invalid_if_not_compliant',
                    f"Table_ID {row['table_id']}: {config_name} does not support if_not_compliant='{value}'.",
                    table_id=row['table_id'],
                    line_number=row['line_number'],
                    suggestion="Use 'warn' or 'fail' instead. Row-level actions are not applicable for these checks.")


def validate_scd2_requires_timestamp(result: ValidationResult, primary_rows: list):
    """
    Validate that merge_type='scd2' has a corresponding source_timestamp_column_name.
    """
    # Group by Table_ID
    table_configs = {}
    for row in primary_rows:
        table_id = row['table_id']
        if table_id not in table_configs:
            table_configs[table_id] = {'scd2_enabled': False, 'has_timestamp': False, 'line_number': row['line_number']}
        
        config_name = row['name'].lower()
        config_value = row['value'].strip().strip("'\"").lower()
        
        if config_name == 'merge_type' and config_value == 'scd2':
            table_configs[table_id]['scd2_enabled'] = True
            table_configs[table_id]['line_number'] = row['line_number']
        
        if config_name == 'source_timestamp_column_name':
            table_configs[table_id]['has_timestamp'] = True
    
    for table_id, config in table_configs.items():
        if config['scd2_enabled'] and not config['has_timestamp']:
            result.add_issue('error', 'scd2_missing_timestamp',
                f"Table_ID {table_id}: merge_type='scd2' requires 'source_timestamp_column_name' to be specified.",
                table_id=table_id,
                line_number=config['line_number'],
                suggestion="Add source_timestamp_column_name to specify which column contains the change timestamp")


def validate_replace_where_requires_column(result: ValidationResult, primary_rows: list, advanced_rows: list):
    """
    Validate that merge_type='replace_where' has a corresponding replace_where_column.
    """
    # Find tables with merge_type='replace_where' from advanced config
    replace_where_tables = set()
    replace_where_lines = {}
    for row in advanced_rows:
        if row['attribute'].lower() == 'merge_type':
            value = row['value'].strip().strip("'\"").lower()
            if value == 'replace_where':
                replace_where_tables.add(row['table_id'])
                replace_where_lines[row['table_id']] = row['line_number']
    
    # Find tables that have replace_where_column defined
    tables_with_column = set()
    for row in primary_rows:
        if row['name'].lower() == 'replace_where_column':
            tables_with_column.add(row['table_id'])
    
    # Also check advanced config for replace_where_column
    for row in advanced_rows:
        if row['attribute'].lower() == 'replace_where_column':
            tables_with_column.add(row['table_id'])
    
    # Report missing
    for table_id in replace_where_tables:
        if table_id not in tables_with_column:
            result.add_issue('error', 'replace_where_missing_column',
                f"Table_ID {table_id}: merge_type='replace_where' requires 'replace_where_column' to be specified.",
                table_id=table_id,
                line_number=replace_where_lines.get(table_id),
                suggestion="Add replace_where_column to specify which column determines the data to replace")


def validate_exact_find_replace_count(result: ValidationResult, primary_rows: list):
    """
    Validate that exact_find and exact_replace have matching counts (or exact_replace is a single value).
    """
    # Group by Table_ID
    table_configs = {}
    for row in primary_rows:
        if row['category'].lower() != 'column_cleansing':
            continue
        
        table_id = row['table_id']
        if table_id not in table_configs:
            table_configs[table_id] = {'exact_find': None, 'exact_replace': None, 'line_number': row['line_number']}
        
        config_name = row['name'].lower()
        if config_name == 'exact_find':
            value = row['value'].strip().strip("'\"")
            table_configs[table_id]['exact_find'] = len([x for x in value.split(',') if x.strip()]) if value else 0
            table_configs[table_id]['line_number'] = row['line_number']
        elif config_name == 'exact_replace':
            value = row['value'].strip().strip("'\"")
            # exact_replace can be single value (applies to all) or comma-separated matching exact_find
            table_configs[table_id]['exact_replace'] = len([x for x in value.split(',') if x.strip()]) if value else 0
    
    for table_id, config in table_configs.items():
        find_count = config.get('exact_find')
        replace_count = config.get('exact_replace')
        
        if find_count is not None and replace_count is not None:
            # exact_replace must be 1 (single value for all) or match exact_find count
            if replace_count != 1 and replace_count != find_count:
                result.add_issue('error', 'exact_find_replace_mismatch',
                    f"Table_ID {table_id}: exact_find has {find_count} values but exact_replace has {replace_count}. "
                    "exact_replace must be a single value OR match exact_find count exactly.",
                    table_id=table_id,
                    line_number=config['line_number'],
                    suggestion="Use a single replacement value for all, or provide comma-separated values matching exact_find count")


def validate_xml_namespace_count(result: ValidationResult, primary_rows: list):
    """
    Validate that xml_namespaces_keys and xml_namespaces_values have matching counts.
    """
    # Group by Table_ID
    table_configs = {}
    for row in primary_rows:
        table_id = row['table_id']
        if table_id not in table_configs:
            table_configs[table_id] = {'keys': None, 'values': None, 'line_number': row['line_number']}
        
        config_name = row['name'].lower()
        if config_name == 'xml_namespaces_keys':
            value = row['value'].strip().strip("'\"")
            table_configs[table_id]['keys'] = len([x for x in value.split(',') if x.strip()]) if value else 0
            table_configs[table_id]['line_number'] = row['line_number']
        elif config_name == 'xml_namespaces_values':
            value = row['value'].strip().strip("'\"")
            table_configs[table_id]['values'] = len([x for x in value.split(',') if x.strip()]) if value else 0
    
    for table_id, config in table_configs.items():
        keys_count = config.get('keys')
        values_count = config.get('values')
        
        # If one is provided, the other must be too, and counts must match
        if keys_count is not None and values_count is None:
            result.add_issue('error', 'xml_namespace_mismatch',
                f"Table_ID {table_id}: xml_namespaces_keys provided but xml_namespaces_values is missing.",
                table_id=table_id,
                line_number=config['line_number'],
                suggestion="Add xml_namespaces_values with matching number of namespace URIs")
        
        elif values_count is not None and keys_count is None:
            result.add_issue('error', 'xml_namespace_mismatch',
                f"Table_ID {table_id}: xml_namespaces_values provided but xml_namespaces_keys is missing.",
                table_id=table_id,
                line_number=config['line_number'],
                suggestion="Add xml_namespaces_keys with matching number of namespace prefixes")
        
        elif keys_count is not None and values_count is not None and keys_count != values_count:
            result.add_issue('error', 'xml_namespace_mismatch',
                f"Table_ID {table_id}: xml_namespaces_keys has {keys_count} entries but xml_namespaces_values has {values_count}. "
                "These must match.",
                table_id=table_id,
                line_number=config['line_number'],
                suggestion="Ensure xml_namespaces_keys and xml_namespaces_values have the same number of comma-separated entries")


def validate_duplicate_transformations(result: ValidationResult, advanced_rows: list):
    """
    Detect if the exact same transformation is defined twice on the same column within a Table_ID.
    Duplicate transformations waste processing and may cause unexpected results.
    
    Checks for duplicates based on:
    - Same Table_ID
    - Same Configuration_Name (e.g., 'derived_column', 'change_data_types')
    - Same column_name
    - Same key attributes (expression for derived_column, new_type for change_data_types, etc.)
    """
    # Define which attributes make a transformation "unique" for each config type
    # If these attributes are identical, it's a duplicate
    UNIQUENESS_KEYS = {
        'derived_column': ['column_name', 'expression'],
        'change_data_types': ['column_name', 'new_type'],
        'filter_data': ['filter_logic'],
        'remove_columns': ['column_name'],
        'select_columns': ['column_name'],
        'apply_null_handling': ['column_name', 'action', 'default_value'],
        'replace_values': ['column_name', 'values_to_replace', 'replacement_value'],
        'mask_sensitive_data': ['column_name', 'upper_char', 'lower_char', 'digit_char', 'other_char'],
        'add_row_hash': ['column_name', 'output_column_name', 'hash_algorithm'],
        'drop_duplicates': ['column_name', 'order_by', 'order_direction'],
        'sort_data': ['column_name', 'sort_direction'],
        'string_functions': ['column_name', 'function', 'output_column_name'],
        'normalize_text': ['column_name', 'operations', 'output_column_name'],
        'transform_datetime': ['column_name', 'operation', 'output_column_name'],
        'explode_array': ['column_name', 'output_column_name'],
        'flatten_struct': ['column_name', 'prefix'],
        'split_column': ['column_name', 'delimiter', 'output_column_names'],
        'concat_columns': ['column_name', 'output_column_name', 'separator'],
        'conditional_column': ['column_name', 'conditions', 'values', 'default_value'],
        'columns_to_rename': ['existing_column_name', 'new_column_name'],
        # DQ validations
        'validate_not_null': ['column_name'],
        'validate_unique': ['column_name'],
        'validate_pattern': ['column_name', 'pattern_type'],
        'validate_range': ['column_name', 'min_value', 'max_value'],
        'validate_condition': ['condition'],
    }
    
    # Group advanced rows by (Table_ID, Configuration_Name, Instance_Number)
    instances_by_table_config: Dict[tuple, Dict[str, str]] = defaultdict(dict)
    instance_lines: Dict[tuple, int] = {}
    
    for row in advanced_rows:
        key = (row['table_id'], row['name'], row['instance'])
        instances_by_table_config[key][row['attribute']] = row['value']
        if key not in instance_lines:
            instance_lines[key] = row['line_number']
    
    # Now group by (Table_ID, Configuration_Name) and check for duplicates
    # Build signature for each instance based on uniqueness keys
    seen_signatures: Dict[tuple, tuple] = {}  # {(table_id, config_name, signature): (instance_num, line_num)}
    
    for (table_id, config_name, instance_num), attrs in instances_by_table_config.items():
        if config_name not in UNIQUENESS_KEYS:
            continue
        
        # Build signature from uniqueness keys
        sig_parts = []
        for key in UNIQUENESS_KEYS[config_name]:
            sig_parts.append(attrs.get(key, '').lower().strip())
        signature = tuple(sig_parts)
        
        # Skip if signature is all empty (incomplete config)
        if all(p == '' for p in signature):
            continue
        
        lookup_key = (table_id, config_name, signature)
        line_num = instance_lines.get((table_id, config_name, instance_num))
        
        if lookup_key in seen_signatures:
            first_instance, first_line = seen_signatures[lookup_key]
            
            # Build description of what's duplicated
            sig_desc = ", ".join(f"{k}='{v}'" for k, v in zip(UNIQUENESS_KEYS[config_name], signature) if v)
            
            result.add_issue('warning', 'duplicate_transformation',
                f"Table_ID {table_id}: Duplicate '{config_name}' detected. "
                f"Instance #{instance_num} has same key attributes as instance #{first_instance}: [{sig_desc}]",
                table_id=table_id,
                line_number=line_num,
                suggestion=f"Remove the duplicate instance or modify it if different behavior is intended. "
                           f"First occurrence at line {first_line}.")
        else:
            seen_signatures[lookup_key] = (instance_num, line_num)


def validate_order_of_operations_dependencies(result: ValidationResult, orch_rows: list, advanced_rows: list):
    """
    Ensure tables that source from other tables have higher Order_Of_Operations values.
    
    Parses dependencies from:
    - join_data.right_table_name
    - attach_dimension_surrogate_key.dimension_table_name
    - union_data.union_tables
    - validate_referential_integrity.reference_table_name
    
    If Table A depends on Table B (via join/union), then:
    - Table A's Order_Of_Operations should be > Table B's Order_Of_Operations
    - OR Table B should be in a lower medallion layer (processed in earlier trigger)
    """
    if not orch_rows or not advanced_rows:
        return
    
    # Build lookup: target_entity (lowercase, normalized) -> (table_id, order, target_datastore)
    # Normalize to handle both 3-part (workspace.lakehouse.table) and 2-part (schema.table) names
    entity_to_info: Dict[str, tuple] = {}
    table_id_to_info: Dict[int, dict] = {}
    
    for row in orch_rows:
        target_entity = row['target_entity'].lower().strip()
        table_id = row['table_id']
        order = row['order']
        datastore = row['target_datastore'].lower()
        line_num = row['line_number']
        
        # Store full entity name
        entity_to_info[target_entity] = (table_id, order, datastore, line_num)
        
        # Also store just the table name (last part) for fuzzy matching
        parts = target_entity.split('.')
        if len(parts) >= 1:
            table_name_only = parts[-1]
            # Only store if not already present (prefer full match)
            if table_name_only not in entity_to_info:
                entity_to_info[table_name_only] = (table_id, order, datastore, line_num)
        
        table_id_to_info[table_id] = {
            'target_entity': target_entity,
            'order': order,
            'datastore': datastore,
            'line_number': line_num,
        }
    
    # Collect dependencies from advanced config
    # Structure: {table_id: [(dependency_table_ref, dependency_type, line_number), ...]}
    dependencies: Dict[int, list] = defaultdict(list)
    
    # Attributes that reference other tables
    DEPENDENCY_ATTRIBUTES = {
        ('join_data', 'right_table_name'),
        ('attach_dimension_surrogate_key', 'dimension_table_name'),
        ('validate_referential_integrity', 'reference_table_name'),
    }
    
    for row in advanced_rows:
        table_id = row['table_id']
        config_name = row['name']
        attr_name = row['attribute']
        value = row['value'].strip()
        line_num = row['line_number']
        
        if (config_name, attr_name) in DEPENDENCY_ATTRIBUTES:
            if value:
                dependencies[table_id].append((value, f"{config_name}.{attr_name}", line_num))
        
        # Handle union_tables (comma-separated list)
        if config_name == 'union_data' and attr_name == 'union_tables' and value:
            for table_ref in value.split(','):
                table_ref = table_ref.strip()
                if table_ref:
                    dependencies[table_id].append((table_ref, 'union_data.union_tables', line_num))
    
    # Validate dependencies
    for table_id, deps in dependencies.items():
        if table_id not in table_id_to_info:
            continue
        
        current_info = table_id_to_info[table_id]
        current_order = current_info['order']
        current_datastore = current_info['datastore']
        current_entity = current_info['target_entity']
        
        for dep_table_ref, dep_type, line_num in deps:
            # Normalize the dependency reference
            dep_ref_lower = dep_table_ref.lower().strip()
            
            # Try to find the dependency in our orchestration
            dep_info = None
            
            # Try full match first
            if dep_ref_lower in entity_to_info:
                dep_info = entity_to_info[dep_ref_lower]
            else:
                # Try matching just the table name (last part of 3-part name)
                dep_parts = dep_ref_lower.split('.')
                if len(dep_parts) >= 1:
                    dep_table_name = dep_parts[-1]
                    if dep_table_name in entity_to_info:
                        dep_info = entity_to_info[dep_table_name]
            
            if dep_info is None:
                # Dependency references a table not in this metadata file
                # This is OK - it might be in another trigger or a pre-existing table
                continue
            
            dep_table_id, dep_order, dep_datastore, dep_line = dep_info
            
            # Skip self-references (shouldn't happen but be safe)
            if dep_table_id == table_id:
                continue
            
            # Check if dependency is in a different (lower) layer - that's OK
            layer_order = {'bronze': 1, 'silver': 2, 'gold': 3}
            current_layer_num = layer_order.get(current_datastore, 0)
            dep_layer_num = layer_order.get(dep_datastore, 0)
            
            if dep_layer_num < current_layer_num:
                # Dependency is in a lower layer (e.g., current is Gold, dep is Silver)
                # This is expected and OK - lower layers are processed first
                continue
            
            # Same layer - check Order_Of_Operations
            if current_order <= dep_order:
                result.add_issue('error', 'dependency_order',
                    f"Table_ID {table_id} ({current_entity}) depends on Table_ID {dep_table_id} "
                    f"via {dep_type}, but has Order_Of_Operations={current_order} which is "
                    f"{'equal to' if current_order == dep_order else 'less than'} the dependency's order ({dep_order}). "
                    f"The dependency must be processed BEFORE this table.",
                    table_id=table_id,
                    line_number=line_num,
                    suggestion=f"Change Table_ID {table_id}'s Order_Of_Operations to be greater than {dep_order}, "
                               f"or change Table_ID {dep_table_id}'s order to be less than {current_order}. "
                               f"Tables with dependencies must have higher Order_Of_Operations than their dependencies.")


def validate_consolidatable_transformations(result: ValidationResult, advanced_rows: list):
    """
    Validate that transformations supporting comma-separated columns are consolidated.
    Multiple transformation steps with the same type/settings should use comma-separated columns.
    
    For example, instead of:
        (1001, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'col_a'),
        (1001, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'int'),
        (1001, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'col_b'),
        (1001, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'int'),
    
    Should be consolidated to:
        (1001, 'data_transformation_steps', 'change_data_types', 1, 'column_name', 'col_a, col_b'),
        (1001, 'data_transformation_steps', 'change_data_types', 1, 'new_type', 'int'),
    """
    # Define configurations that support consolidation and what attributes define "sameness"
    # Format: config_name -> (column_attr, grouping_attrs) 
    # column_attr: the attribute that can be comma-separated
    # grouping_attrs: if these are the same, the columns should be consolidated
    CONSOLIDATABLE_CONFIGS = {
        # Transformations
        'change_data_types': ('column_name', ['new_type']),
        'apply_null_handling': ('column_name', ['action', 'logic', 'default_value', 'values_to_convert', 'values_delimiter', 'convert_empty_space']),
        'replace_values': ('column_name', ['values_to_replace', 'replacement_value', 'values_delimiter']),
        'mask_sensitive_data': ('column_name', ['upper_char', 'lower_char', 'digit_char', 'other_char']),
        'string_functions': ('column_name', ['function', 'output_column_name']),
        'normalize_text': ('column_name', ['operations', 'output_column_name']),
        'remove_columns': ('column_name', []),
        # Data quality validations
        'validate_pattern': ('column_name', ['if_not_compliant', 'pattern_type', 'allow_null', 'message']),
        'validate_anomaly': ('column_name', ['if_not_compliant', 'method', 'threshold', 'minimum_baseline_records', 'message']),
        'validate_not_null': ('column_name', ['if_not_compliant', 'message']),
        'validate_range': ('column_name', ['if_not_compliant', 'min_value', 'max_value', 'message']),
        'validate_completeness': ('column_name', ['if_not_compliant', 'min_completeness_percent', 'message']),
    }
    
    # Group rows by Table_ID, Configuration_Name, and Instance_Number
    # Structure: {table_id: {config_name: {instance: {attr: value}}}}
    grouped = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    line_numbers = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    for row in advanced_rows:
        table_id = row['table_id']
        config_name = row['name'].lower()
        instance = row['instance']
        attr = row['attribute'].lower()
        value = row['value'].strip().strip("'\"")
        
        grouped[table_id][config_name][instance][attr] = value
        if attr == 'column_name':
            line_numbers[table_id][config_name][instance] = row['line_number']
    
    # Now check for consolidation opportunities within each Table_ID
    for table_id, configs in grouped.items():
        for config_name, instances in configs.items():
            if config_name not in CONSOLIDATABLE_CONFIGS:
                continue
            
            column_attr, grouping_attrs = CONSOLIDATABLE_CONFIGS[config_name]
            
            # Build a signature for each instance based on grouping attributes
            # Signature -> list of (instance_number, column_value, line_number)
            signature_groups = defaultdict(list)
            
            for instance_num, attrs in instances.items():
                column_value = attrs.get(column_attr, '')
                
                # Build signature from grouping attributes
                sig_parts = []
                for ga in grouping_attrs:
                    sig_parts.append(attrs.get(ga, ''))
                signature = tuple(sig_parts)
                
                line_num = line_numbers[table_id][config_name].get(instance_num, 0)
                signature_groups[signature].append((instance_num, column_value, line_num))
            
            # Check each signature group - if multiple instances have the same signature,
            # they should be consolidated
            for signature, instance_list in signature_groups.items():
                if len(instance_list) > 1:
                    # Multiple instances with same settings - should consolidate
                    instance_nums = sorted([i[0] for i in instance_list])
                    columns = [i[1] for i in instance_list]
                    first_line = min(i[2] for i in instance_list if i[2] > 0) or None
                    
                    # Build a description of what's duplicated
                    if grouping_attrs:
                        settings_desc = ", ".join(f"{ga}='{signature[i]}'" for i, ga in enumerate(grouping_attrs) if signature[i])
                        if not settings_desc:
                            settings_desc = "same settings"
                    else:
                        settings_desc = "same operation"
                    
                    result.add_issue('warning', 'unconsolidated_transformation',
                        f"Table_ID {table_id}: {config_name} has {len(instance_list)} separate instance(s) "
                        f"(#{', #'.join(map(str, instance_nums))}) with {settings_desc}. "
                        f"Columns [{', '.join(columns)}] should be consolidated into a single comma-separated column_name.",
                        table_id=table_id,
                        line_number=first_line,
                        suggestion=f"Combine into single instance: ('{column_attr}', '{', '.join(columns)}')")


def validate_dq_numeric_ranges(result: ValidationResult, advanced_rows: list):
    """
    Validate numeric ranges for Data Quality parameters.
    Rule: min_completeness_percent (0-100), min_rows (>0), max_age (>0).
    """
    for row in advanced_rows:
        config_name = row['name'].lower()
        attr_name = row['attribute'].lower()
        value = row['value'].strip().strip("'\"")
        line_num = row['line_number']
        
        try:
            if config_name == 'validate_completeness' and attr_name == 'min_completeness_percent':
                val = float(value)
                if not (0 <= val <= 100):
                    result.add_issue('error', 'invalid_range',
                        f"Table_ID {row['table_id']}: min_completeness_percent must be between 0 and 100. Found: {val}",
                        table_id=row['table_id'], line_number=line_num,
                        suggestion="Use a percentage value like 95 for 95% minimum completeness.")
            
            elif config_name == 'validate_batch_size' and attr_name == 'min_rows':
                val = int(value)
                if val <= 0:
                    result.add_issue('error', 'invalid_range',
                        f"Table_ID {row['table_id']}: min_rows must be greater than 0. Found: {val}",
                        table_id=row['table_id'], line_number=line_num,
                        suggestion="Use a positive integer like 1 or 100 for minimum expected rows.")

            elif config_name == 'validate_freshness' and attr_name == 'max_age':
                val = int(value)
                if val <= 0:
                    result.add_issue('error', 'invalid_range',
                        f"Table_ID {row['table_id']}: max_age must be greater than 0. Found: {val}",
                        table_id=row['table_id'], line_number=line_num,
                        suggestion="Use a positive integer for hours of allowed data staleness (e.g., 24 for 24 hours).")
        except ValueError:
            result.add_issue('error', 'invalid_number',
                f"Table_ID {row['table_id']}: {attr_name} must be a number. Found: '{value}'",
                table_id=row['table_id'], line_number=line_num,
                suggestion="Remove any quotes or non-numeric characters. Use integers for counts, decimals for percentages.")


def validate_column_name_format(result: ValidationResult, advanced_rows: list):
    """
    Validate that new column names follow best practices (snake_case, no spaces).
    Rule: output_column_name, new_column_name should be alphanumeric + underscore.
    """
    column_config_attrs = [
        ('columns_to_rename', 'new_column_name'),
        ('derived_column', 'column_name'),
        ('add_row_hash', 'output_column_name'),
        ('aggregate_data', 'output_column_name'),
        ('transform_datetime', 'output_column_name'),
        ('string_functions', 'output_column_name'),
        ('concat_columns', 'output_column_name'),
    ]
    
    # Regex for safe column names (letters, numbers, underscores)
    safe_col_re = re.compile(r'^[a-zA-Z0-9_]+$')
    
    # Attributes that might contain comma-separated column lists
    comma_sep_attrs = {'output_column_name', 'new_column_name'}
    
    for row in advanced_rows:
        config_name = row['name'].lower()
        attr_name = row['attribute'].lower()
        
        # Check if this attribute defines a NEW column name
        is_target_attr = False
        for cfg, attr in column_config_attrs:
            if config_name == cfg and attr_name == attr:
                is_target_attr = True
                break
        
        if is_target_attr:
            # Handle comma-separated lists for some transforms
            raw_value = row['value'].strip().strip("'\"")
            values = [v.strip() for v in raw_value.split(',')]
            
            for val in values:
                if not val: continue
                if not safe_col_re.match(val):
                    result.add_issue('warning', 'column_naming_convention',
                        f"Table_ID {row['table_id']}: Column name '{val}' contains special characters or spaces. "
                        f"Best practice is snake_case (e.g., 'my_column_name') to avoid downstream issues.",
                        table_id=row['table_id'], line_number=row['line_number'],
                        suggestion=f"Rename to: '{re.sub(r'[^a-zA-Z0-9_]', '_', val).lower()}'")


def validate_window_function_requirements(result: ValidationResult, advanced_rows: list):
    """
    Validate that order-sensitive window functions include an order_by clause.
    Rule: functions like row_number, rank, lead, lag require order_by.
    """
    # Functions that absolutely require ORDER BY in Spark SQL
    order_sensitive_functions = {
        'row_number', 'rank', 'dense_rank', 'ntile', 'percent_rank', 'cume_dist', 'lead', 'lag'
    }
    
    # Group by instance to check attributes together
    window_instances = defaultdict(dict)
    
    for row in advanced_rows:
        if row['name'].lower() == 'add_window_function':
            key = (row['table_id'], row['instance'])
            window_instances[key][row['attribute'].lower()] = row['value']
            if 'line_number' not in window_instances[key]:
                window_instances[key]['line_number'] = row['line_number']
    
    for (table_id, _), attrs in window_instances.items():
        func = attrs.get('window_function', '').lower().strip("'\"")
        order_by = attrs.get('order_by')
        line_num = attrs.get('line_number')
        
        if func in order_sensitive_functions:
            if not order_by or not order_by.strip():
                result.add_issue('error', 'missing_order_by',
                    f"Table_ID {table_id}: Window function '{func}' requires 'order_by' attribute to be specified.",
                    table_id=table_id, line_number=line_num,
                    suggestion="Add 'order_by' attribute with the column(s) to order by")


def validate_regex_syntax(result: ValidationResult, primary_rows: list, advanced_rows: list):
    """
    Validate Python regex syntax for regex paths and patterns.
    """
    # Check regex_find in Primary Config (column_cleansing)
    for row in primary_rows:
        if row['name'] == 'regex_find':
            try:
                re.compile(row['value'].strip("'\""))
            except re.error as e:
                result.add_issue('error', 'invalid_regex',
                    f"Table_ID {row['table_id']}: Invalid regex in regex_find: {e}",
                    table_id=row['table_id'], line_number=row['line_number'],
                    suggestion="Test your regex at https://regex101.com/ (select Python flavor). Common issues: unescaped special chars like . * + ( )")


def validate_medallion_anti_patterns(result: ValidationResult, orch_rows: list, primary_rows: list, advanced_rows: list):
    """
    Validate medallion architecture best practices.
    Detect common anti-patterns that violate medallion architecture principles.
    
    Anti-patterns detected:
    1. Bronze with transformations (over-processing) - Bronze should store raw data only
    2. Gold sourcing from Bronze - Gold should source from Silver
    3. Bronze with non-append/overwrite merge_type - Bronze should preserve raw data
    
    See: docs/Medallion_Architecture_Best_Practices.md
    """
    # Build lookup of Target_Datastore by Table_ID (case-insensitive wildcard match for bronze/silver/gold)
    table_layers: Dict[int, tuple] = {}  # {table_id: (layer, line_number, target_entity)}
    for row in orch_rows:
        datastore = row['target_datastore'].lower()
        # Wildcard match: if datastore contains bronze, silver, or gold anywhere
        if 'bronze' in datastore:
            layer = 'bronze'
        elif 'silver' in datastore:
            layer = 'silver'
        elif 'gold' in datastore:
            layer = 'gold'
        else:
            layer = datastore  # Unknown layer
        table_layers[row['table_id']] = (layer, row['line_number'], row['target_entity'])
    
    # Build lookup of merge_type by Table_ID from primary config
    table_merge_types: Dict[int, tuple] = {}  # {table_id: (merge_type, line_number)}
    for row in primary_rows:
        if row['name'].lower() == 'merge_type':
            table_merge_types[row['table_id']] = (row['value'].lower(), row['line_number'])
    
    # Build lookup of datastore_name by Table_ID from primary config (source details)
    table_source_datastores: Dict[int, tuple] = {}  # {table_id: (datastore_name, line_number)}
    for row in primary_rows:
        if row['name'].lower() == 'datastore_name':
            table_source_datastores[row['table_id']] = (row['value'].lower(), row['line_number'])
    
    # Track which Table_IDs have transformations or DQ checks
    tables_with_transformations: Dict[int, list] = defaultdict(list)  # {table_id: [(config_name, line_number), ...]}
    tables_with_dq: Dict[int, list] = defaultdict(list)  # {table_id: [(config_name, line_number), ...]}
    
    for row in advanced_rows:
        table_id = row['table_id']
        category = row['category'].lower()
        config_name = row['name']
        line_num = row['line_number']
        
        if category == 'data_transformation_steps':
            # Only track first occurrence per (table_id, config_name)
            existing = [t[0] for t in tables_with_transformations[table_id]]
            if config_name not in existing:
                tables_with_transformations[table_id].append((config_name, line_num))
        elif category == 'data_quality':
            existing = [t[0] for t in tables_with_dq[table_id]]
            if config_name not in existing:
                tables_with_dq[table_id].append((config_name, line_num))
    
    # ==========================================================================
    # ANTI-PATTERN 1: Bronze with transformations (over-processing)
    # ==========================================================================
    for table_id, transformations in tables_with_transformations.items():
        layer_info = table_layers.get(table_id)
        if layer_info and layer_info[0] == 'bronze':
            transform_names = [t[0] for t in transformations]
            first_line = transformations[0][1] if transformations else None
            target_entity = layer_info[2]
            
            result.add_issue('warning', 'medallion_anti_pattern',
                f"🏗️ MEDALLION ANTI-PATTERN: Table_ID {table_id} ({target_entity}) targets Bronze but has "
                f"data_transformation_steps: [{', '.join(transform_names)}]. "
                f"Bronze should store RAW data with NO transformations.",
                table_id=table_id,
                line_number=first_line,
                suggestion="⚠️ LLM ACTION REQUIRED: Either (1) you made a conversion error - move these transformations "
                           "to a Silver table that sources from this Bronze table, OR (2) ask the user: "
                           "'This Bronze table has transformations which violates medallion best practices. "
                           "Is this intentional? If yes, I'll add a comment to document the exception.'")
    
    # ==========================================================================
    # ANTI-PATTERN 2: Gold sourcing from Bronze (should source from Silver)
    # ==========================================================================
    for table_id, (source_datastore, line_num) in table_source_datastores.items():
        layer_info = table_layers.get(table_id)
        if layer_info and layer_info[0] == 'gold' and 'bronze' in source_datastore:
            target_entity = layer_info[2]
            
            result.add_issue('warning', 'medallion_anti_pattern',
                f"🏗️ MEDALLION ANTI-PATTERN: Table_ID {table_id} ({target_entity}) targets Gold but sources "
                f"from datastore_name='{source_datastore}'. Gold should source from SILVER (validated data), "
                f"not Bronze (raw/unvalidated data).",
                table_id=table_id,
                line_number=line_num,
                suggestion="⚠️ LLM ACTION REQUIRED: Either (1) you made a conversion error - change datastore_name "
                           "to reference the Silver layer, OR (2) ask the user: "
                           "'This Gold table sources directly from Bronze, skipping Silver validation. "
                           "Is this intentional? If yes, I'll add a comment to document the exception.'")
    
    # ==========================================================================
    # ANTI-PATTERN 3: Bronze with non-append/overwrite merge_type
    # ==========================================================================
    # Bronze should use 'append' or 'overwrite' to preserve raw data fidelity
    # Merge patterns (merge, merge_and_delete, etc.) modify data which violates Bronze principles
    allowed_bronze_merge_types = VALID_MERGE_TYPES_BY_LAYER.get('bronze', set())
    
    for table_id, (merge_type, line_num) in table_merge_types.items():
        layer_info = table_layers.get(table_id)
        if layer_info and layer_info[0] == 'bronze':
            if merge_type not in allowed_bronze_merge_types:
                target_entity = layer_info[2]
                
                result.add_issue('warning', 'medallion_anti_pattern',
                    f"🏗️ MEDALLION ANTI-PATTERN: Table_ID {table_id} ({target_entity}) targets Bronze but uses "
                    f"merge_type='{merge_type}'. Bronze should use 'append' or 'overwrite' to preserve raw data. "
                    f"Merge patterns modify data which violates Bronze's role as immutable raw storage.",
                    table_id=table_id,
                    line_number=line_num,
                    suggestion="⚠️ LLM ACTION REQUIRED: Either (1) you made a conversion error - change merge_type "
                               "to 'append' or 'overwrite', OR (2) ask the user: "
                               "'This Bronze table uses a merge pattern which modifies data. "
                               "Is this intentional? If yes, I'll add a comment to document the exception.'")


def validate_custom_transformation_function_ratio(result: ValidationResult, advanced_rows: list):
    """
    Warn when custom_transformation_function dominates data_transformation_steps (> 50%) across all Table_IDs.
    Reinforces Tier 1 First non-negotiable.
    """
    if not advanced_rows:
        return

    transform_instances: Set[Tuple[int, str, int]] = set()
    custom_instances: Set[Tuple[int, str, int]] = set()
    first_line = None

    for row in advanced_rows:
        if row['category'].lower() != 'data_transformation_steps':
            continue
        table_id = row['table_id']
        key = (table_id, row['name'].lower(), row['instance'])
        transform_instances.add(key)
        if first_line is None:
            first_line = row['line_number']
        if row['name'].lower() == 'custom_transformation_function':
            custom_instances.add(key)

    total = len(transform_instances)
    custom = len(custom_instances)
    if total == 0:
        return

    non_negotiable_text = (
        "**Custom Only When Needed** - Use built-in transformations for **all standard tasks** "
        "(renames, casting, filtering, joins, aggregations, column cleansing, etc.). "
        "Only use Tier 2 **for the specific logic that cannot be expressed with built-ins**. "
        "Everything else must remain Tier 1. Custom code adds maintenance burden and generates additional unit tests. "
        "If Tier 2 is required, CREATE the notebook (don't just say \"requires custom code\")."
    )

    ratio = custom / total
    if ratio > 0.5:
        percent = round(ratio * 100, 1)
        result.add_issue(
            'warning',
            'tier1_first_guardrail',
            f"Custom_transformation_function makes up {custom}/{total} ({percent}%) of data_transformation_steps across all Table_IDs. "
            f"This exceeds the 50% threshold. "
            f"This is one of the 4 non-negotiables when generating metadata and the LLM must align to it: {non_negotiable_text}",
            line_number=first_line,
            suggestion="Review transformations and replace custom_transformation_function steps with built-in transformations when possible"
        )


# =============================================================================
# CUSTOM FUNCTION NOTEBOOK VALIDATION
# =============================================================================

# Custom-function policy and runtime environment allow-lists are loaded from the
# machine-readable metadata contract so the validator does not need to carry
# duplicate declarative policy in code.

# URL for creating Databricks cluster library configurations (shown in warning messages) is loaded
# from the machine-readable metadata contract.


def validate_python_notebook_lakehouse_header(
    notebook_content: str,
    notebook_name: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that a Python custom function notebook has the correct META header
    with kernel_info set to synapse_pyspark. The lakehouse dependency is optional since
    it gets populated at deployment time — "dependencies": {} is valid.

    Valid minimal structure:
        # META {
        # META   "kernel_info": {
        # META     "name": "synapse_pyspark"
        # META   },
        # META   "dependencies": {}
        # META }

    If lakehouse dependency is present, validates it has the expected keys:
        # META   "dependencies": {
        # META     "lakehouse": {
        # META       "default_lakehouse_name": "",
        # META       "default_lakehouse_workspace_id": ""
        # META     }
        # META   }

    Args:
        notebook_content: Python source code from notebook
        notebook_name: Name of the notebook (for error messages)

    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []

    # Extract META JSON block from the header comments
    # Pattern: lines starting with "# META" between the first METADATA and first CELL markers
    meta_lines = []
    in_meta_block = False
    found_first_metadata = False
    for line in notebook_content.split('\n'):
        stripped = line.strip()
        if stripped == '# METADATA ********************':
            if not found_first_metadata:
                found_first_metadata = True
                in_meta_block = True
                continue
            else:
                # Second METADATA marker = end of header meta block
                break
        if stripped == '# CELL ********************':
            break
        if in_meta_block and stripped.startswith('# META'):
            # Remove "# META " prefix to extract JSON content
            json_part = stripped[len('# META'):].strip() if len(stripped) > len('# META') else ''
            meta_lines.append(json_part)

    if not meta_lines:
        issues.append((
            'error',
            f"Notebook '{notebook_name}' is missing the META header block.",
            "Add the required META header with kernel_info and lakehouse dependencies. "
            "See docs/METADATA_GENERATION_GUIDE.md for the template."
        ))
        return issues

    # Reconstruct JSON from meta lines
    meta_json_str = '\n'.join(meta_lines)
    try:
        meta_json = json.loads(meta_json_str)
    except json.JSONDecodeError:
        issues.append((
            'warning',
            f"Notebook '{notebook_name}' has a META header that is not valid JSON.",
            "Ensure the META header block contains valid JSON structure."
        ))
        return issues

    # Check kernel_info
    kernel_info = meta_json.get('kernel_info', {})
    kernel_name = kernel_info.get('name', '')
    if kernel_name != 'synapse_pyspark':
        issues.append((
            'error',
            f"Notebook '{notebook_name}' has kernel_info.name='{kernel_name}', expected 'synapse_pyspark'.",
            "Set kernel_info.name to 'synapse_pyspark' for Python notebooks."
        ))

    # Check dependencies.lakehouse structure (optional - empty dependencies is valid)
    # Lakehouse details get populated at deployment time, so "dependencies": {} is acceptable
    dependencies = meta_json.get('dependencies', {})
    lakehouse = dependencies.get('lakehouse', None)

    if lakehouse is not None:
        if not isinstance(lakehouse, dict):
            issues.append((
                'error',
                f"Notebook '{notebook_name}' META header has 'dependencies.lakehouse' but it is not a dict.",
                "Set lakehouse to an object with default_lakehouse_name and default_lakehouse_workspace_id keys."
            ))
            return issues

        # If lakehouse is present, validate it has the expected keys
        missing_keys = []
        if 'default_lakehouse_name' not in lakehouse:
            missing_keys.append('default_lakehouse_name')
        if 'default_lakehouse_workspace_id' not in lakehouse:
            missing_keys.append('default_lakehouse_workspace_id')

        if missing_keys:
            issues.append((
                'error',
                f"Notebook '{notebook_name}' META header lakehouse is missing required key(s): "
                f"{', '.join(missing_keys)}. Found: {json.dumps(lakehouse)}",
                "Add the missing key(s) to the lakehouse dependency. Empty string values are acceptable: "
                '{"default_lakehouse_name": "", "default_lakehouse_workspace_id": ""}'
            ))

    return issues


def find_notebook_path(notebook_name: str, sql_file_path: Path) -> Optional[Path]:
    """
    Find the notebook file path based on the notebook name referenced in metadata.
    
    Searches in common locations relative to the SQL file:
    1. Same metadata parent directory (e.g., <workspace>/metadata/<Notebook>.Notebook/)
    2. Sibling of the metadata folder
    3. Any folder in the same workspace that contains .Notebook folders
    
    Supports notebooks with or without the NB_ prefix convention.
    
    Args:
        notebook_name: Name of the notebook (with or without NB_ prefix)
        sql_file_path: Path to the SQL metadata file
        
    Returns:
        Path to notebook-content.py if found, None otherwise
    """
    # Build list of notebook name variations to search for
    notebook_variations = []
    
    # Add the original name with .Notebook suffix if needed
    base_name = notebook_name
    if not base_name.endswith('.Notebook'):
        base_name = f'{base_name}.Notebook'
    notebook_variations.append(base_name)
    
    # Also try with NB_ prefix if not already present
    if not notebook_name.startswith('NB_'):
        nb_prefixed = f'NB_{notebook_name}'
        if not nb_prefixed.endswith('.Notebook'):
            nb_prefixed = f'{nb_prefixed}.Notebook'
        notebook_variations.append(nb_prefixed)
    
    sql_dir = sql_file_path.parent
    workspace_folder = infer_workspace_folder(sql_file_path)
    
    # Try each notebook name variation
    for nb_name in notebook_variations:
        # Common search locations
        search_paths = [
            sql_dir.parent / nb_name / 'notebook-content.py',  # sibling of warehouse/notebook folder
            workspace_folder / nb_name / 'notebook-content.py',  # sibling of metadata folder
        ]
        
        for path in search_paths:
            if path.exists():
                return path
    
    # If still not found, search within the workspace folder (not the git root)
    # This ensures we only find notebooks from the same workspace folder, not from
    # other workspace folders that may exist in the same repo.
    if workspace_folder.exists():
        for folder in workspace_folder.iterdir():
            if folder.is_dir():
                for nb_name in notebook_variations:
                    candidate = folder / nb_name / 'notebook-content.py'
                    if candidate.exists():
                        return candidate
    
    return None


def extract_imports_from_content(content: str) -> List[Tuple[str, int]]:
    """
    Extract all import statements from Python code.
    
    Handles:
    - import module
    - import module as alias
    - from module import name
    - from module.submodule import name
    
    Args:
        content: Python source code as string
        
    Returns:
        List of (module_name, line_number) tuples
    """
    imports = []
    
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return imports
    
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                # Get the top-level module name
                module_name = alias.name.split('.')[0]
                imports.append((alias.name, node.lineno, module_name))
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                # Get the full module path and the top-level module
                module_name = node.module.split('.')[0]
                imports.append((node.module, node.lineno, module_name))
    
    return imports


def is_standard_databricks_library(module_name: str) -> bool:
    """
    Check if a module is part of the standard Databricks runtime.
    
    Args:
        module_name: The full module name (e.g., 'pandas.core.frame')
        
    Returns:
        True if the module is in the standard Databricks runtime
    """
    # Get the top-level module name
    top_level = module_name.split('.')[0]
    
    # Check against our list of known Databricks libraries
    # Also check the full module name for specific sub-modules
    return (
        top_level in DATABRICKS_STANDARD_LIBRARIES or
        module_name in DATABRICKS_STANDARD_LIBRARIES or
        # Handle common variations (underscores vs hyphens)
        top_level.replace('-', '_') in DATABRICKS_STANDARD_LIBRARIES or
        top_level.replace('_', '-') in DATABRICKS_STANDARD_LIBRARIES
    )


def validate_notebook_imports(
    notebook_content: str,
    notebook_name: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that all imports in a notebook are available in the standard Databricks runtime.
    
    Args:
        notebook_content: Python source code from notebook
        notebook_name: Name of the notebook (for error messages)
        
    Returns:
        List of (severity, message, suggestion) tuples for non-standard imports
    """
    issues = []
    non_standard_imports = []
    
    imports = extract_imports_from_content(notebook_content)
    
    for full_module, line_num, top_level_module in imports:
        if not is_standard_databricks_library(full_module):
            non_standard_imports.append((full_module, line_num))
    
    if non_standard_imports:
        # Group by module for cleaner output
        unique_modules = sorted(set(m for m, _ in non_standard_imports))
        module_list = ', '.join(unique_modules[:5])  # Show first 5
        if len(unique_modules) > 5:
            module_list += f", ... ({len(unique_modules) - 5} more)"
        
        issues.append((
            'error',
            f"Notebook '{notebook_name}' imports non-standard library(s) not included in Databricks Runtime: "
            f"{module_list}. These imports will fail unless you create a Databricks cluster library configuration.",
            f"⚠️ ACTION REQUIRED: Create a Databricks cluster library configuration with these packages and attach it to "
            f"NB_Batch_Processing notebook. See: {DATABRICKS_CLUSTER_LIBRARIES_DOCS_URL}"
        ))
        
        # Add individual warnings for each import
        for module_name, line_num in non_standard_imports[:10]:  # Limit to first 10
            issues.append((
                'warning',
                f"Non-standard import '{module_name}' at line {line_num}",
                "Add this package to your Databricks cluster library configuration"
            ))
    
    return issues


def validate_custom_notebook_import_scope(
    notebook_content: str,
    notebook_name: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that custom notebooks place all import statements inside functions,
    not at the module/top level.

    Custom notebooks are loaded via get_ipython().run_cell() into the shared
    IPython namespace alongside all helper functions. Top-level imports pollute
    this shared namespace and can shadow critical symbols (e.g., the 'f' alias
    for pyspark.sql.functions) — especially dangerous at scale with hundreds
    of custom notebooks.

    This rule detects any import or from-import statement that is NOT inside
    a function body and reports it as an error.

    Args:
        notebook_content: Python source code from notebook
        notebook_name: Name of the notebook (for error messages)

    Returns:
        List of (severity, message, suggestion) tuples
    """
    issues = []

    # Strip Databricks notebook markup (# CELL, # METADATA, # META blocks)
    # so that ast.parse only sees pure Python code
    clean_lines = []
    in_meta_block = False
    for line in notebook_content.splitlines():
        stripped = line.strip()
        if stripped.startswith('# META'):
            in_meta_block = stripped == '# META {'
            clean_lines.append('')
            continue
        if in_meta_block:
            if stripped == '# META }':
                in_meta_block = False
            clean_lines.append('')
            continue
        if stripped in ('# CELL ********************',
                        '# METADATA ********************',
                        '# Databricks notebook source'):
            clean_lines.append('')
            continue
        clean_lines.append(line)

    clean_content = '\n'.join(clean_lines)

    try:
        tree = ast.parse(clean_content)
    except SyntaxError:
        return issues

    class ImportScopeVisitor(ast.NodeVisitor):
        def __init__(self):
            self.ancestors = []

        def generic_visit(self, node):
            self.ancestors.append(node)
            super().generic_visit(node)
            self.ancestors.pop()

        def _inside_function(self) -> bool:
            return any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in self.ancestors)

        def _record_issue(self, node):
            if self._inside_function():
                return

            if isinstance(node, ast.Import):
                module_names = [alias.name for alias in node.names]
            else:
                module_names = [node.module] if node.module else ['(unknown)']

            for mod in module_names:
                issues.append((
                    'error',
                    f"Top-level import '{mod}' at line {node.lineno} in notebook "
                    f"'{notebook_name}'. Custom notebooks execute in a shared namespace — "
                    f"top-level imports risk shadowing critical symbols (e.g., pyspark.sql.functions alias 'f').",
                    f"Move 'import {mod}' inside the function that uses it. "
                    f"Python's LEGB scoping ensures the function can still access "
                    f"helper functions (log_and_print, etc.) from the shared namespace."
                ))

        def visit_Import(self, node):
            self._record_issue(node)

        def visit_ImportFrom(self, node):
            self._record_issue(node)

    ImportScopeVisitor().visit(tree)

    return issues


def parse_python_functions(content: str) -> Dict[str, dict]:
    """
    Parse Python file and extract function definitions with their signatures.
    
    Args:
        content: Python source code as string
        
    Returns:
        Dictionary mapping function names to their details:
        {
            'function_name': {
                'params': ['param1', 'param2', ...],
                'has_return': bool,
                'return_annotation': str or None,
                'line_number': int,
                'docstring': str or None,
            }
        }
    """
    functions = {}
    
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return functions
    
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            # Extract parameter names
            params = []
            for arg in node.args.args:
                params.append(arg.arg)
            
            # Check for return statement
            has_return = False
            for child in ast.walk(node):
                if isinstance(child, ast.Return) and child.value is not None:
                    has_return = True
                    break
            
            # Get return annotation if present
            return_annotation = None
            if node.returns:
                if isinstance(node.returns, ast.Name):
                    return_annotation = node.returns.id
                elif isinstance(node.returns, ast.Subscript):
                    # Handle Optional[DataFrame], etc.
                    if isinstance(node.returns.value, ast.Name):
                        return_annotation = node.returns.value.id
            
            # Get docstring
            docstring = ast.get_docstring(node)
            
            functions[node.name] = {
                'params': params,
                'has_return': has_return,
                'return_annotation': return_annotation,
                'line_number': node.lineno,
                'docstring': docstring,
            }
    
    return functions


def extract_metadata_access_patterns(content: str, function_name: str, metadata_param_name: str) -> List[Tuple[str, int, str]]:
    """
    Extract metadata dictionary access patterns from a function's code.
    
    Looks for patterns like:
    - metadata.get('key')
    - metadata['key']
    - metadata.get('key1')['key2']
    - metadata.get('key1').get('key2')
    
    Args:
        content: Python source code
        function_name: Name of the function to analyze
        metadata_param_name: Name of the metadata parameter (e.g., 'metadata', 'all_metadata')
        
    Returns:
        List of (accessed_key, line_number, full_access_pattern) tuples
    """
    accesses = []
    
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return accesses
    
    # Find the function node
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            func_node = node
            break
    
    if func_node is None:
        return accesses
    
    # Walk through the function body looking for metadata access
    for node in ast.walk(func_node):
        # Pattern 1: metadata.get('key') or metadata['key']
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute):
                if node.func.attr == 'get':
                    # Check if it's metadata.get(...)
                    if isinstance(node.func.value, ast.Name) and node.func.value.id == metadata_param_name:
                        if node.args and isinstance(node.args[0], ast.Constant):
                            key = node.args[0].value
                            accesses.append((key, node.lineno, f"{metadata_param_name}.get('{key}')"))
                    # Check if it's metadata.get('x').get('y')
                    elif isinstance(node.func.value, ast.Call):
                        inner_call = node.func.value
                        if isinstance(inner_call.func, ast.Attribute) and inner_call.func.attr == 'get':
                            if isinstance(inner_call.func.value, ast.Name) and inner_call.func.value.id == metadata_param_name:
                                if inner_call.args and isinstance(inner_call.args[0], ast.Constant):
                                    key = inner_call.args[0].value
                                    accesses.append((key, node.lineno, f"{metadata_param_name}.get('{key}').get(...)"))
        
        # Pattern 2: metadata['key']
        elif isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name) and node.value.id == metadata_param_name:
                if isinstance(node.slice, ast.Constant):
                    key = node.slice.value
                    accesses.append((key, node.lineno, f"{metadata_param_name}['{key}']"))
            # Pattern: metadata.get('x')['y']
            elif isinstance(node.value, ast.Call):
                call = node.value
                if isinstance(call.func, ast.Attribute) and call.func.attr == 'get':
                    if isinstance(call.func.value, ast.Name) and call.func.value.id == metadata_param_name:
                        if call.args and isinstance(call.args[0], ast.Constant):
                            key = call.args[0].value
                            accesses.append((key, node.lineno, f"{metadata_param_name}.get('{key}')[...]"))
    
    return accesses


def extract_nested_config_access(content: str, function_name: str) -> Dict[str, List[Tuple[str, int]]]:
    """
    Extract nested config access patterns from a function.
    
    Looks for patterns like:
    - function_config.get('attribute_name')
    - primary_config.get('attribute_name')  
    - primary_config['attribute_name']
    - config = metadata.get('primary_config'); config.get('x')
    
    Args:
        content: Python source code
        function_name: Name of the function to analyze
        
    Returns:
        Dictionary mapping config type to list of (attribute_name, line_number) tuples:
        {
            'function_config': [('my_attribute', 10), ...],
            'primary_config': [('source', 5), ...],
            'advanced_config': [('some_key', 7), ...],
        }
    """
    accesses = defaultdict(list)
    
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return dict(accesses)
    
    # Find the function node
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            func_node = node
            break
    
    if func_node is None:
        return dict(accesses)
    
    # Track variable assignments to config dicts
    # e.g., primary_config = metadata.get('primary_config')
    config_aliases = {}  # variable_name -> config_type
    
    for node in ast.walk(func_node):
        # Track assignments like: primary_config = metadata.get('primary_config')
        if isinstance(node, ast.Assign):
            if len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
                var_name = node.targets[0].id
                # Check if RHS is metadata.get('config_type')
                if isinstance(node.value, ast.Call):
                    if isinstance(node.value.func, ast.Attribute) and node.value.func.attr == 'get':
                        if node.value.args and isinstance(node.value.args[0], ast.Constant):
                            config_type = node.value.args[0].value
                            if config_type in ('primary_config', 'advanced_config', 'function_config',
                                             'orchestration_metadata', 'workspace_variables'):
                                config_aliases[var_name] = config_type
        
        # Look for config.get('attribute') patterns
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == 'get':
                if isinstance(node.func.value, ast.Name):
                    var_name = node.func.value.id
                    # Direct name like function_config.get('x')
                    if var_name in ('function_config', 'primary_config', 'advanced_config',
                                   'orchestration_metadata', 'workspace_variables'):
                        if node.args and isinstance(node.args[0], ast.Constant):
                            attr_name = node.args[0].value
                            accesses[var_name].append((attr_name, node.lineno))
                    # Aliased name like cfg.get('x') where cfg = metadata.get('primary_config')
                    elif var_name in config_aliases:
                        if node.args and isinstance(node.args[0], ast.Constant):
                            attr_name = node.args[0].value
                            config_type = config_aliases[var_name]
                            accesses[config_type].append((attr_name, node.lineno))
        
        # Look for config['attribute'] patterns
        elif isinstance(node, ast.Subscript):
            if isinstance(node.value, ast.Name):
                var_name = node.value.id
                if isinstance(node.slice, ast.Constant):
                    attr_name = node.slice.value
                    if var_name in ('function_config', 'primary_config', 'advanced_config',
                                   'orchestration_metadata', 'workspace_variables'):
                        accesses[var_name].append((attr_name, node.lineno))
                    elif var_name in config_aliases:
                        config_type = config_aliases[var_name]
                        accesses[config_type].append((attr_name, node.lineno))
    
    return dict(accesses)


def validate_config_attribute_references(
    function_name: str,
    content: str,
    function_type: str,
    table_id: int,
    primary_rows: list,
    advanced_rows: list,
    instance: int = None,
) -> List[Tuple[str, str, str]]:
    """
    Validate that config attribute references in a custom function actually exist in metadata.
    
    For example, if function does `function_config.get('threshold_value')`, verify that
    the custom_transformation_function instance in metadata actually defines 'threshold_value'.
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function
        table_id: Table_ID from metadata
        primary_rows: Parsed primary config rows
        advanced_rows: Parsed advanced config rows
        instance: Instance number for transformation functions
        
    Returns:
        List of (severity, message, suggestion) tuples
    """
    issues = []
    
    # Extract nested config accesses from the function
    config_accesses = extract_nested_config_access(content, function_name)
    
    # Build lookup of actual attributes defined in metadata for this Table_ID
    
    # 1. Primary config attributes for this Table_ID
    primary_attrs_by_category = defaultdict(set)
    for row in primary_rows:
        if row['table_id'] == table_id:
            primary_attrs_by_category[row['category']].add(row['name'])
    
    # Flatten all primary config attribute names
    all_primary_attrs = set()
    for attrs in primary_attrs_by_category.values():
        all_primary_attrs.update(attrs)
    
    # 2. For transformation functions, get the custom_transformation_function instance attributes
    function_config_attrs = set()
    if function_type == 'custom_transformation_function' and instance is not None:
        for row in advanced_rows:
            if (row['table_id'] == table_id and 
                row['name'] == 'custom_transformation_function' and 
                row.get('instance') == instance):
                # Each attribute of this custom_transformation_function instance is available
                function_config_attrs.add(row['attribute'])
    
    # Validate function_config accesses (for transformation functions)
    if 'function_config' in config_accesses:
        for attr_name, line_num in config_accesses['function_config']:
            if attr_name not in function_config_attrs:
                # Check if it might be defined in primary config instead
                if attr_name in all_primary_attrs:
                    issues.append((
                        'warning',
                        f"Function '{function_name}' accesses function_config.get('{attr_name}') "
                        f"(line {line_num}) but '{attr_name}' is in primary_config, not the custom_transformation_function attributes.",
                        f"Use primary_config.get('{attr_name}') instead, or add '{attr_name}' to the "
                        f"custom_transformation_function instance attributes in metadata."
                    ))
                else:
                    # Suggest available attributes
                    if function_config_attrs:
                        available = ', '.join(sorted(function_config_attrs - {'functions_to_execute', 'notebooks_to_run'}))
                        suggestion = f"Available custom_transformation_function attributes for this instance: {available}"
                    else:
                        suggestion = "Add this attribute to the custom_transformation_function instance in metadata SQL."
                    
                    issues.append((
                        'warning',
                        f"Function '{function_name}' accesses function_config.get('{attr_name}') "
                        f"(line {line_num}) but this attribute is not defined in the custom_transformation_function metadata.",
                        suggestion
                    ))
    
    # Validate primary_config accesses  
    if 'primary_config' in config_accesses:
        for attr_name, line_num in config_accesses['primary_config']:
            if attr_name not in all_primary_attrs:
                # Check if it might be in function_config
                if attr_name in function_config_attrs:
                    issues.append((
                        'warning',
                        f"Function '{function_name}' accesses primary_config.get('{attr_name}') "
                        f"(line {line_num}) but '{attr_name}' is defined in the custom_transformation_function attributes.",
                        f"Use function_config.get('{attr_name}') instead."
                    ))
                else:
                    if all_primary_attrs:
                        available = ', '.join(sorted(list(all_primary_attrs)[:10]))
                        suggestion = f"Available primary_config attributes: {available}..."
                    else:
                        suggestion = "No primary_config attributes defined for this Table_ID."
                    
                    issues.append((
                        'warning',
                        f"Function '{function_name}' accesses primary_config.get('{attr_name}') "
                        f"(line {line_num}) but this attribute is not defined in metadata.",
                        suggestion
                    ))
    
    return issues


def validate_metadata_access(
    function_name: str,
    content: str,
    function_type: str,
    metadata_param_name: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that a custom function accesses metadata dictionary with valid keys.
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function ('custom_transformation_function', 'custom_table_ingestion_function', etc.)
        metadata_param_name: Name of the metadata parameter
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    
    # Get valid keys for this function type
    valid_keys = VALID_METADATA_KEYS['common'].copy()
    if function_type in VALID_METADATA_KEYS:
        valid_keys.update(VALID_METADATA_KEYS[function_type])
    
    # Extract metadata access patterns
    accesses = extract_metadata_access_patterns(content, function_name, metadata_param_name)
    
    for key, line_num, pattern in accesses:
        if key not in valid_keys:
            # Check if it might be a typo
            similar = find_similar(key, list(valid_keys))
            suggestion = f"Did you mean '{similar}'?" if similar else f"Valid keys are: {', '.join(sorted(valid_keys))}"
            
            issues.append((
                'error',
                f"Function '{function_name}' accesses invalid metadata key '{key}' (line {line_num}). "
                f"Pattern: {pattern}",
                suggestion
            ))
        
        # Warn about advanced_config usage in transformation functions
        if key == 'advanced_config' and function_type == 'custom_transformation_function':
            issues.append((
                'warning',
                f"Function '{function_name}' accesses 'advanced_config' (line {line_num}). "
                f"This is difficult to parse as it's a list of dictionaries.",
                "For transformation functions, use 'function_config' instead - it contains the "
                "transformation step attributes as a simple key-value dictionary. "
                "Example: function_config.get('my_custom_attribute')"
            ))
    
    return issues


def extract_sql_table_references(content: str, function_name: str) -> List[Tuple[str, int, str]]:
    """
    Extract SQL table references from a function's code.
    
    Looks for patterns like:
    - FROM workspace.lakehouse.schema.table
    - FROM `workspace`.lakehouse.schema.table
    - JOIN workspace.lakehouse.schema.table
    
    Args:
        content: Python source code
        function_name: Name of the function to analyze
        
    Returns:
        List of (table_reference, line_number, full_match) tuples
    """
    references = []
    
    # Find the function in the content
    func_pattern = rf'def\s+{re.escape(function_name)}\s*\([^)]*\).*?(?=\ndef\s|\Z)'
    func_match = re.search(func_pattern, content, re.DOTALL)
    
    if not func_match:
        return references
    
    func_content = func_match.group(0)
    func_start_line = content[:func_match.start()].count('\n') + 1
    
    # Extract all string literals from the function (including multi-line strings)
    # Pattern to find all types of strings: single, double, triple-quoted
    # NOTE: Single/double-quoted patterns exclude newlines ([^"\n] / [^'\n]) to
    # prevent matching between apostrophes across lines (e.g., "Python's" in a
    # docstring matched to a closing quote 3 lines later).
    string_patterns = [
        r'f"""(.*?)"""',  # f-string triple double
        r"f'''(.*?)'''",  # f-string triple single
        r'"""(.*?)"""',   # triple double
        r"'''(.*?)'''",   # triple single
        r'f"([^"\n]*)"',    # f-string double
        r"f'([^'\n]*)'",    # f-string single
        r'"([^"\n]*)"',     # double
        r"'([^'\n]*)'",     # single
    ]
    
    # Pattern to match SQL table references
    # Matches: FROM/JOIN followed by identifier.identifier.identifier (3-part name)
    # or identifier.identifier.identifier.identifier (4-part name)
    # Captures whether backticks are present around the workspace name
    sql_table_pattern = r'''
        (?:FROM|JOIN)\s+
        (`?)(\{?\w+\}?)(`?)\.      # workspace name with optional backticks (groups 1,2,3)
        [`"]?(\w+)[`"]?\.          # lakehouse/schema name
        [`"]?(\w+)[`"]?            # schema/table name
        (?:\.`?(\w+)`?)?           # optional table name (for 4-part names)
    '''
    
    for pattern in string_patterns:
        for match in re.finditer(pattern, func_content, re.DOTALL):
            string_content = match.group(1)
            string_start = match.start()
            
            # Calculate line number for the start of this string
            string_line = func_start_line + func_content[:string_start].count('\n')
            
            table_matches = re.finditer(sql_table_pattern, string_content, re.IGNORECASE | re.VERBOSE)
            for table_match in table_matches:
                full_ref = table_match.group(0).strip()
                leading_backtick = table_match.group(1)
                workspace_name = table_match.group(2)  # The workspace name (may include {})
                trailing_backtick = table_match.group(3)
                
                # Calculate actual line number within the string
                prefix = string_content[:table_match.start()]
                actual_line = string_line + prefix.count('\n')
                
                # Check if backticks are present
                has_backticks = leading_backtick == '`' and trailing_backtick == '`'
                
                references.append((workspace_name, actual_line, full_ref, has_backticks))
    
    # Deduplicate references (same workspace_name, line, full_ref, has_backticks)
    seen = set()
    unique_references = []
    for ref in references:
        key = (ref[0], ref[1], ref[2], ref[3])
        if key not in seen:
            seen.add(key)
            unique_references.append(ref)
    
    return unique_references


def validate_sql_workspace_references(
    function_name: str,
    content: str,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that SQL queries in custom functions use workspace variables instead of hardcoded names,
    and that workspace names are wrapped in backticks.
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    
    references = extract_sql_table_references(content, function_name)
    
    for workspace_name, line_num, full_ref, has_backticks in references:
        # Check if it's using a variable substitution pattern (f-string with {})
        is_variable = '{' in workspace_name and '}' in workspace_name
        
        if not is_variable:
            # This is a hardcoded workspace name - ERROR because it breaks cross-workspace deployments
            issues.append((
                'error',
                f"Function '{function_name}' has hardcoded workspace/datastore name '{workspace_name}' "
                f"in SQL query (line {line_num}): {full_ref}",
                "Use workspace_variables to get the workspace name dynamically. "
                "Example: workspace_variables.get('silver_datastore_workspace_name') "
                "then use f-string: f\"SELECT * FROM `{workspace_name}`.lakehouse.schema.table\""
            ))
        elif not has_backticks:
            # Using a variable but missing backticks
            issues.append((
                'warning',
                f"Function '{function_name}' has workspace variable without backticks "
                f"in SQL query (line {line_num}): {full_ref}",
                "Workspace names should always be wrapped in backticks to handle special characters. "
                f"Change to: `{workspace_name}` (e.g., f\"SELECT * FROM `{{workspace_name}}`.lakehouse.schema.table\")"
            ))
    
    return issues


def validate_spark_read_table_usage(
    function_name: str,
    content: str,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that custom functions do not use spark.read.table() API.
    
    spark.read.table() does not support cross-workspace table access and relies on
    the default lakehouse context. Custom functions should use spark.sql() with
    fully-qualified 4-part table names including the workspace name.
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    
    # Find the function in the content
    func_pattern = rf'def\s+{re.escape(function_name)}\s*\([^)]*\).*?(?=\ndef\s|\Z)'
    func_match = re.search(func_pattern, content, re.DOTALL)
    
    if not func_match:
        return issues
    
    func_content = func_match.group(0)
    func_start_line = content[:func_match.start()].count('\n') + 1
    
    # Pattern to detect spark.read.table() calls
    # Matches: spark.read.table(...), spark .read.table(...), etc.
    spark_read_table_pattern = r'spark\s*\.\s*read\s*\.\s*table\s*\(([^)]+)\)'
    
    for match in re.finditer(spark_read_table_pattern, func_content):
        table_arg = match.group(1).strip()
        match_start = match.start()
        line_num = func_start_line + func_content[:match_start].count('\n')
        
        issues.append((
            'error',
            f"Function '{function_name}' uses spark.read.table({table_arg}) at line {line_num}. "
            f"This API does not support cross-workspace table access.",
            "Use spark.sql() with a fully-qualified 4-part table name including workspace. "
            "Example: spark.sql(f\"SELECT * FROM `{{workspace_name}}`.lakehouse.schema.table\") "
            "where workspace_name comes from metadata['workspace_variables'].get('silver_datastore_workspace_name')"
        ))
    
    return issues


def validate_dataframe_count_usage(
    function_name: str,
    content: str,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that custom functions do not use .count() on DataFrames.
    
    .count() forces a full data scan which kills performance on large datasets.
    This is especially problematic in loops where .count() is called repeatedly.
    
    For custom_staging_function, this is downgraded to a warning because staging
    functions legitimately need .count() to report rows_copied.
    
    Bad patterns:
    - df.count() > 0  # Forces full scan to check if data exists
    - while df.count() > 0:  # Forces full scan every iteration
    - row_count = df.count()  # Full scan just to get count
    
    Good alternatives:
    - df.isEmpty()  # Efficient check, stops at first row
    - df.limit(1).count() > 0  # Only scans one row
    - while not df.isEmpty():  # Efficient emptiness check
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    
    # Find the function in the content
    func_pattern = rf'def\s+{re.escape(function_name)}\s*\([^)]*\).*?(?=\ndef\s|\Z)'
    func_match = re.search(func_pattern, content, re.DOTALL)
    
    if not func_match:
        return issues
    
    func_content = func_match.group(0)
    func_start_line = content[:func_match.start()].count('\n') + 1
    
    # Pattern to detect .count() calls on DataFrames
    # Matches: df.count(), new_data.count(), result_df.count(), etc.
    # Excludes: .countDistinct which is an aggregation function
    count_pattern = r'(\w+)\s*\.\s*count\s*\(\s*\)'
    
    for match in re.finditer(count_pattern, func_content):
        var_name = match.group(1)
        match_start = match.start()
        line_num = func_start_line + func_content[:match_start].count('\n')
        
        # Get the line context for better error message
        line_start = func_content.rfind('\n', 0, match_start) + 1
        line_end = func_content.find('\n', match_start)
        if line_end == -1:
            line_end = len(func_content)
        line_context = func_content[line_start:line_end].strip()
        
        # Custom staging functions need .count() for rows_copied — downgrade to warning
        severity = 'warning' if function_type == 'custom_staging_function' else 'error'
        
        if severity == 'warning':
            msg_suffix = "This forces a full data scan and can hurt performance."
            suggestion_suffix = "If you need .count() for rows_copied reporting, this warning can be ignored."
        else:
            msg_suffix = "This forces a full data scan and kills performance!"
            suggestion_suffix = "If you truly need a count, consider if the logic can be restructured to avoid it."
        
        issues.append((
            severity,
            f"Function '{function_name}' uses .count() at line {line_num}: `{line_context}`. "
            f"{msg_suffix}",
            "Use df.isEmpty() for emptiness checks (stops at first row). "
            "For iteration: use 'while not df.isEmpty():' instead of 'while df.count() > 0:'. "
            f"{suggestion_suffix}"
        ))
    
    return issues


def _contains_write_attribute(node: ast.AST) -> bool:
    """Return True if the AST node chain contains a `.write` attribute access."""
    if isinstance(node, ast.Attribute):
        if node.attr == 'write':
            return True
        return _contains_write_attribute(node.value)
    if isinstance(node, ast.Call):
        return _contains_write_attribute(node.func)
    return False


def validate_custom_staging_spark_write_usage(
    function_name: str,
    content: str,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate custom staging functions do not use direct Spark DataFrame write APIs.

    For custom staging functions, all Spark DataFrame output writes must go through
    `_write_spark_file_with_rename` to ensure deterministic single-file behavior and
    consistent output naming. Direct Spark write calls (e.g., `.write.parquet(...)`,
    `.write.csv(...)`, `.write.format(...).save(...)`) are disallowed.

    Note:
    - This rule only applies to `custom_staging_function`.
    - Non-Spark writes (e.g., pandas `.to_*`, `dbutils.fs.*`) are allowed.
    """
    issues = []

    if function_type != 'custom_staging_function':
        return issues

    try:
        tree = ast.parse(content)
    except SyntaxError:
        return issues

    # Find target function node
    func_node = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            func_node = node
            break

    if func_node is None:
        return issues

    direct_write_methods = {
        'save', 'saveAsTable', 'insertInto',
        'parquet', 'csv', 'json', 'text', 'orc'
    }

    for node in ast.walk(func_node):
        if not isinstance(node, ast.Call):
            continue

        # Detect direct DataFrame writer terminal methods chained from `.write`
        if isinstance(node.func, ast.Attribute):
            terminal_method = node.func.attr
            if terminal_method in direct_write_methods and _contains_write_attribute(node.func.value):
                issues.append((
                    'error',
                    f"Function '{function_name}' uses direct Spark write API '.write.{terminal_method}(...)' "
                    f"at line {node.lineno}. Custom staging functions must not call Spark write commands directly.",
                    "Replace direct Spark writes with `_write_spark_file_with_rename(data_to_merge=..., output_path=..., file_format=...)`. "
                    "This keeps single-file naming and output behavior consistent across staging functions."
                ))

    return issues


def validate_direct_logging_usage(
    function_name: str,
    content: str,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that custom functions do not use logger or print() directly.
    
    Custom function notebooks run inside the accelerator framework which provides
    log_and_print(message, level) from NB_Helper_Functions_1. This function handles
    both logging (via Log4j spark_application_logger) and console output with
    proper formatting. Using logger or print() directly bypasses this and causes
    inconsistent log output.
    
    Bad patterns:
    - logger.info(msg)
    - logger.warn(msg)
    - logger.error(msg)
    - logger.warning(msg)
    - print(msg)
    
    Good pattern:
    - log_and_print(msg)           # defaults to level="info"
    - log_and_print(msg, "warn")
    - log_and_print(msg, "error")
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    
    # Find the function in the content
    func_pattern = rf'def\s+{re.escape(function_name)}\s*\([^)]*\).*?(?=\ndef\s|\Z)'
    func_match = re.search(func_pattern, content, re.DOTALL)
    
    if not func_match:
        return issues
    
    func_content = func_match.group(0)
    func_start_line = content[:func_match.start()].count('\n') + 1
    
    # Skip docstring and comment lines
    # Check for logger.xxx() calls
    logger_pattern = r'\blogger\s*\.\s*(info|warn|error|warning)\s*\('
    for match in re.finditer(logger_pattern, func_content):
        match_start = match.start()
        # Check if this line is a comment
        line_start = func_content.rfind('\n', 0, match_start) + 1
        line_end = func_content.find('\n', match_start)
        if line_end == -1:
            line_end = len(func_content)
        line_text = func_content[line_start:line_end].strip()
        if line_text.startswith('#'):
            continue
        # Check if inside a docstring (between triple quotes)
        preceding = func_content[:match_start]
        triple_double = preceding.count('\"\"\"')
        triple_single = preceding.count("'''")
        if triple_double % 2 != 0 or triple_single % 2 != 0:
            continue
        
        line_num = func_start_line + func_content[:match_start].count('\n')
        method = match.group(1)
        issues.append((
            'error',
            f"Function '{function_name}' uses logger.{method}() directly at line {line_num}: `{line_text}`. "
            f"Use log_and_print() from NB_Helper_Functions_1 instead.",
            f"Replace `logger.{method}(msg)` with `log_and_print(msg, '{method}')`" +
            (" (use level='info' or omit level parameter)" if method == 'info' else "") +
            ". log_and_print handles both logging and printing with proper formatting."
        ))
    
    # Check for direct print() calls
    print_pattern = r'\bprint\s*\('
    for match in re.finditer(print_pattern, func_content):
        match_start = match.start()
        line_start = func_content.rfind('\n', 0, match_start) + 1
        line_end = func_content.find('\n', match_start)
        if line_end == -1:
            line_end = len(func_content)
        line_text = func_content[line_start:line_end].strip()
        if line_text.startswith('#'):
            continue
        preceding = func_content[:match_start]
        triple_double = preceding.count('\"\"\"')
        triple_single = preceding.count("'''")
        if triple_double % 2 != 0 or triple_single % 2 != 0:
            continue
        
        line_num = func_start_line + func_content[:match_start].count('\n')
        issues.append((
            'error',
            f"Function '{function_name}' uses print() directly at line {line_num}: `{line_text}`. "
            f"Use log_and_print() from NB_Helper_Functions_1 instead.",
            "Replace `print(msg)` with `log_and_print(msg)`. "
            "log_and_print handles both logging and printing with proper formatting."
        ))
    
    return issues


def validate_exception_handling(
    function_name: str,
    content: str,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate that custom functions do not silently swallow exceptions.
    
    This is a CRITICAL validation - custom functions that catch exceptions and only
    print/log them without re-raising cause silent pipeline failures.
    
    Patterns that cause issues:
    - try: ... except Exception as e: print(f"Error: {e}")  # BAD - silently swallowed
    - try: ... except: pass  # BAD - silently swallowed
    
    Patterns that are OK:
    - try: ... except Exception as e: raise  # re-raised
    - try: ... except Exception as e: errors.append(e); ... if errors: raise  # tracked and raised
    - try: ... except Exception as e: raise ValueError(f"...{e}")  # wrapped and re-raised
    
    Args:
        function_name: Name of the function to validate
        content: Python source code
        function_type: Type of custom function
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    
    # Parse the content
    try:
        tree = ast.parse(content)
    except SyntaxError:
        return issues
    
    # Find the function definition
    func_def = None
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            func_def = node
            break
    
    if not func_def:
        return issues
    
    # Find all try/except blocks in the function
    for node in ast.walk(func_def):
        if isinstance(node, ast.Try):
            for handler in node.handlers:
                # Analyze this exception handler
                handler_line = handler.lineno
                exception_name = handler.name  # The 'e' in 'except Exception as e'
                
                # Check if the handler body contains a raise statement
                has_raise = False
                has_error_tracking = False
                only_prints_or_logs = True
                
                for stmt in ast.walk(handler):
                    if isinstance(stmt, ast.Raise):
                        has_raise = True
                        only_prints_or_logs = False
                    
                    # Check for error tracking patterns like: errors.append(...) or error_list.append(...)
                    if isinstance(stmt, ast.Call):
                        if isinstance(stmt.func, ast.Attribute):
                            if stmt.func.attr == 'append':
                                # Check if appending to something that looks like an error list
                                if isinstance(stmt.func.value, ast.Name):
                                    var_name = stmt.func.value.id.lower()
                                    if any(x in var_name for x in ['error', 'err', 'fail', 'exception', 'issue']):
                                        has_error_tracking = True
                                        only_prints_or_logs = False
                        
                        # Check for print/log calls
                        if isinstance(stmt.func, ast.Name):
                            if stmt.func.id not in ('print',):
                                only_prints_or_logs = False
                        elif isinstance(stmt.func, ast.Attribute):
                            # Allow logging calls but still flag if that's all there is
                            if stmt.func.attr not in ('info', 'debug', 'warning', 'error', 'critical', 
                                                       'warn', 'exception', 'log'):
                                only_prints_or_logs = False
                    
                    # Check for continue, break, return (without raise)
                    if isinstance(stmt, ast.Continue):
                        only_prints_or_logs = False  # Not just prints, but still might be swallowing
                    if isinstance(stmt, ast.Break):
                        only_prints_or_logs = False
                    if isinstance(stmt, ast.Return):
                        only_prints_or_logs = False
                    
                    # Check for pass statement
                    if isinstance(stmt, ast.Pass):
                        pass  # This doesn't change the analysis
                
                # Determine the exception type being caught
                exception_type = "Exception"
                if handler.type:
                    if isinstance(handler.type, ast.Name):
                        exception_type = handler.type.id
                    elif isinstance(handler.type, ast.Tuple):
                        # Multiple exception types
                        types = []
                        for elt in handler.type.elts:
                            if isinstance(elt, ast.Name):
                                types.append(elt.id)
                        exception_type = ", ".join(types) if types else "Exception"
                else:
                    exception_type = "all exceptions (bare except)"
                
                # Issue warning if exception is caught but not re-raised or tracked
                if not has_raise and not has_error_tracking:
                    severity = 'error'  # This is a critical issue that causes silent failures
                    
                    # Build a more specific message based on what we found
                    if only_prints_or_logs:
                        message = (
                            f"Function '{function_name}' catches {exception_type} (line {handler_line}) "
                            f"and only prints/logs the error without re-raising. "
                            f"This causes SILENT PIPELINE FAILURES."
                        )
                        suggestion = (
                            "🔴 FORBIDDEN: try/except blocks that swallow errors.\n\n"
                            "The accelerator requires ALL exceptions to propagate so pipeline status reflects reality.\n\n"
                            "❌ BAD - Silent failure (pipeline shows SUCCESS but data is wrong):\n"
                            "    try:\n"
                            "        df = spark.sql(f\"SELECT * FROM `{workspace}`.lakehouse.schema.table\")\n"
                            "    except Exception as e:\n"
                            "        print(f'Error: {e}')  # ← Swallowed! Pipeline continues with wrong data\n"
                            "        df = spark.createDataFrame([], schema)\n\n"
                            "✅ GOOD - Use workspace_variables to get workspace name dynamically:\n"
                            "    workspace_variables = metadata.get('workspace_variables', {})\n"
                            "    silver_workspace = workspace_variables.get('silver_datastore_workspace_name', '')\n"
                            "    df = spark.sql(f\"SELECT * FROM `{silver_workspace}`.silver.dbo.my_table\")\n\n"
                            "✅ ALSO GOOD - Re-raise after logging:\n"
                            "    try:\n"
                            "        df = spark.sql(f\"SELECT * FROM `{workspace}`.lakehouse.schema.table\")\n"
                            "    except Exception as e:\n"
                            "        print(f'FATAL: {e}')\n"
                            "        raise  # ← Pipeline correctly shows FAILED\n\n"
                            "See: docs/resources/Custom_Table_Ingestion_Function.py for the correct pattern."
                        )
                    else:
                        message = (
                            f"Function '{function_name}' catches {exception_type} (line {handler_line}) "
                            f"but does not re-raise or track the error. "
                            f"This may cause silent pipeline failures."
                        )
                        suggestion = (
                            "🔴 Exception handling in custom functions MUST either:\n"
                            "    1. Re-raise: `except Exception as e: print(e); raise`\n"
                            "    2. Track and raise later: `errors.append(e); ... if errors: raise`\n"
                            "    3. Avoid try/except entirely - use workspace_variables for dynamic table access\n\n"
                            "Preferred pattern (using workspace_variables):\n"
                            "    workspace_variables = metadata.get('workspace_variables', {})\n"
                            "    silver_workspace = workspace_variables.get('silver_datastore_workspace_name', '')\n"
                            "    df = spark.sql(f\"SELECT * FROM `{silver_workspace}`.silver.dbo.my_table\")\n\n"
                            "See: docs/resources/Custom_Table_Ingestion_Function.py for examples."
                        )
                    
                    issues.append((severity, message, suggestion))
    
    return issues


def validate_custom_transformation_function_signature(
    function_name: str,
    function_info: dict,
    expected_signature: dict,
    function_type: str,
) -> List[Tuple[str, str, str]]:
    """
    Validate a custom function's signature against expected parameters.
    
    Args:
        function_name: Name of the function being validated
        function_info: Parsed function information from parse_python_functions
        expected_signature: Expected signature from custom_transformation_function_SIGNATURES
        function_type: Type of function for error messages
        
    Returns:
        List of (severity, message, suggestion) tuples for issues found
    """
    issues = []
    actual_params = function_info['params']
    required_params = expected_signature['required_params']
    
    # Check required parameters are present
    missing_params = [p for p in required_params if p not in actual_params]
    if missing_params:
        issues.append((
            'error',
            f"Function '{function_name}' is missing required parameters: {missing_params}. "
            f"Expected signature: def {function_name}({', '.join(required_params)}) -> DataFrame",
            f"Add the missing parameters to match the expected {expected_signature['description']} signature."
        ))
    
    # Check parameter order (if all required params present)
    if not missing_params:
        # Find positions of required params in actual params
        actual_positions = {p: i for i, p in enumerate(actual_params) if p in required_params}
        expected_positions = {p: i for i, p in enumerate(required_params)}
        
        wrong_order = False
        for p1, p2 in zip(required_params[:-1], required_params[1:]):
            if p1 in actual_positions and p2 in actual_positions:
                if actual_positions[p1] > actual_positions[p2]:
                    wrong_order = True
                    break
        
        if wrong_order:
            issues.append((
                'error',
                f"Function '{function_name}' has parameters in unexpected order. "
                f"Expected: ({', '.join(required_params)}), found: ({', '.join(actual_params)})",
                f"Reorder parameters to match the standard {expected_signature['description']} signature."
            ))
    
    # Check for return statement
    if not function_info['has_return']:
        expected_return = expected_signature.get('return_type', 'DataFrame')
        issues.append((
            'error',
            f"Function '{function_name}' does not have a return statement. "
            f"Custom functions MUST return a {expected_return}.",
            "Add 'return df' (or equivalent) at the end of the function."
            if expected_return == 'DataFrame' else
            "Add a return statement that returns the expected result."
        ))
    
    # Check return type annotation if present
    if function_info['return_annotation']:
        expected_return = expected_signature.get('return_type', 'DataFrame')
        if function_info['return_annotation'] != expected_return:
            issues.append((
                'warning',
                f"Function '{function_name}' has return type annotation '{function_info['return_annotation']}' "
                f"but should return '{expected_return}'.",
                f"Change return type annotation to '-> {expected_return}'."
            ))
    
    return issues


def validate_custom_transformation_function_notebooks(
    result: ValidationResult,
    primary_rows: list,
    advanced_rows: list,
    sql_file_path: Path,
) -> None:
    """
    Validate custom function notebooks referenced in metadata.
    
    Checks:
    1. Notebook file exists
    2. Function exists in notebook
    3. Function has correct signature (parameters)
    4. Function returns a value (DataFrame)
    
    Args:
        result: ValidationResult to add issues to
        primary_rows: Parsed primary configuration rows
        advanced_rows: Parsed advanced configuration rows  
        sql_file_path: Path to the SQL file being validated
    """
    # Collect custom_transformation_function references from advanced config (data_transformation_steps)
    custom_transformation_functions = []  # List of (table_id, notebook_name, function_names, line_number, func_type, instance)
    
    # Group advanced rows by table_id and instance to find custom_transformation_function configs
    advanced_by_table_instance = defaultdict(dict)
    for row in advanced_rows:
        if row['name'] == 'custom_transformation_function':
            key = (row['table_id'], row['instance'])
            advanced_by_table_instance[key][row['attribute']] = row['value']
            if 'line_number' not in advanced_by_table_instance[key]:
                advanced_by_table_instance[key]['line_number'] = row['line_number']
    
    for (table_id, instance), attrs in advanced_by_table_instance.items():
        functions_to_execute = attrs.get('functions_to_execute', '')
        notebooks_to_run = attrs.get('notebooks_to_run', '')
        line_number = attrs.get('line_number')
        
        if functions_to_execute and notebooks_to_run:
            # Parse comma-separated values
            func_list = [f.strip() for f in functions_to_execute.split(',')]
            notebook_list = [n.strip() for n in notebooks_to_run.split(',')]
            
            for notebook_name in notebook_list:
                custom_transformation_functions.append((
                    table_id,
                    notebook_name,
                    func_list,
                    line_number,
                    'custom_transformation_function',  # transformation function type
                    instance,  # Instance number for config attribute validation
                ))
    
    # Collect custom_table_ingestion_function and custom_file_ingestion_function references from primary config (source_details)
    primary_by_table = defaultdict(dict)
    for row in primary_rows:
        if row['category'] == 'source_details':
            primary_by_table[row['table_id']][row['name']] = row['value']
            if 'line_number' not in primary_by_table[row['table_id']]:
                primary_by_table[row['table_id']]['line_number'] = row['line_number']
    
    for table_id, attrs in primary_by_table.items():
        line_number = attrs.get('line_number')
        
        # Check for custom_table_ingestion_function (table/SQL sources)
        if 'custom_table_ingestion_function' in attrs and 'custom_table_ingestion_function_notebook' in attrs:
            function_name = attrs['custom_table_ingestion_function']
            notebook_name = attrs['custom_table_ingestion_function_notebook']
            
            custom_transformation_functions.append((
                table_id,
                notebook_name,
                [function_name],
                line_number,
                'custom_table_ingestion_function',
                None,
            ))
        
        # Check for custom_file_ingestion_function (file sources)
        if 'custom_file_ingestion_function' in attrs and 'custom_file_ingestion_function_notebook' in attrs:
            function_name = attrs['custom_file_ingestion_function']
            notebook_name = attrs['custom_file_ingestion_function_notebook']
            
            custom_transformation_functions.append((
                table_id,
                notebook_name,
                [function_name],
                line_number,
                'custom_file_ingestion_function',
                None,
            ))
        
        # Check for custom_staging_function (custom staging via NB_Custom_Staging_Processing)
        if 'custom_staging_function' in attrs and 'custom_staging_function_notebook' in attrs:
            function_name = attrs['custom_staging_function']
            notebook_name = attrs['custom_staging_function_notebook']
            
            custom_transformation_functions.append((
                table_id,
                notebook_name,
                [function_name],
                line_number,
                'custom_staging_function',
                None,
            ))
    
    # Validate each custom function reference
    for table_id, notebook_name, function_names, line_number, func_type, instance in custom_transformation_functions:
        # Find notebook file
        notebook_path = find_notebook_path(notebook_name, sql_file_path)
        
        if notebook_path is None:
            # Determine expected path relative to SQL file location
            sql_parent = sql_file_path.parent.parent.name if sql_file_path else 'dev'
            result.add_issue('warning', 'custom_transformation_function_notebook',
                f"Cannot find notebook '{notebook_name}' referenced by Table_ID {table_id}. "
                f"Expected at: {sql_parent}/{notebook_name}.Notebook/notebook-content.py",
                table_id=table_id,
                line_number=line_number,
                suggestion="Create the notebook file or verify the notebook name is correct.")
            continue
        
        # Validate .platform file for the custom notebook (Rules 70-72: existence, displayName, logicalId GUID format)
        notebook_folder = notebook_path.parent
        validate_platform_file_for_notebook_folder(
            result, notebook_folder,
            table_id=table_id,
            line_number=line_number,
            context_prefix=f"Table_ID {table_id}, notebook '{notebook_name}': ",
        )
        
        # Read and parse notebook content
        try:
            notebook_content = notebook_path.read_text(encoding='utf-8')
        except Exception as e:
            result.add_issue('warning', 'custom_transformation_function_notebook',
                f"Cannot read notebook '{notebook_name}': {e}",
                table_id=table_id,
                line_number=line_number)
            continue
        
        # Validate notebook imports (check for non-standard libraries)
        import_issues = validate_notebook_imports(notebook_content, notebook_name)
        for severity, message, suggestion in import_issues:
            result.add_issue(severity, 'custom_transformation_function_non_standard_import',
                f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                table_id=table_id,
                line_number=line_number,
                suggestion=suggestion)
        
        # Validate import scope (all imports must be inside functions, not at top level)
        import_scope_issues = validate_custom_notebook_import_scope(notebook_content, notebook_name)
        for severity, message, suggestion in import_scope_issues:
            result.add_issue(severity, 'custom_notebook_import_scope',
                f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                table_id=table_id,
                line_number=line_number,
                suggestion=suggestion)
        
        # Validate Python notebook lakehouse header
        header_issues = validate_python_notebook_lakehouse_header(notebook_content, notebook_name)
        for severity, message, suggestion in header_issues:
            result.add_issue(severity, 'python_notebook_lakehouse_header',
                f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                table_id=table_id,
                line_number=line_number,
                suggestion=suggestion)
        
        # Parse functions from notebook
        parsed_functions = parse_python_functions(notebook_content)
        
        # Validate each function
        expected_signature = custom_transformation_function_SIGNATURES[func_type]
        
        for function_name in function_names:
            if function_name not in parsed_functions:
                result.add_issue('error', 'custom_transformation_function_missing',
                    f"Function '{function_name}' not found in notebook '{notebook_name}' "
                    f"(referenced by Table_ID {table_id}).",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=f"Add the function to the notebook with signature: "
                               f"def {function_name}({', '.join(expected_signature['params'])}) -> DataFrame")
                continue
            
            # Validate function signature
            func_info = parsed_functions[function_name]
            signature_issues = validate_custom_transformation_function_signature(
                function_name,
                func_info,
                expected_signature,
                func_type,
            )
            
            for severity, message, suggestion in signature_issues:
                result.add_issue(severity, 'custom_transformation_function_signature',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate metadata access patterns
            metadata_param = expected_signature.get('metadata_param', 'metadata')
            access_issues = validate_metadata_access(
                function_name,
                notebook_content,
                func_type,
                metadata_param,
            )
            
            for severity, message, suggestion in access_issues:
                result.add_issue(severity, 'custom_transformation_function_metadata_access',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate SQL workspace references (no hardcoded workspace names)
            workspace_issues = validate_sql_workspace_references(
                function_name,
                notebook_content,
                func_type,
            )
            
            for severity, message, suggestion in workspace_issues:
                result.add_issue(severity, 'custom_transformation_function_hardcoded_workspace',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate spark.read.table() usage
            spark_read_issues = validate_spark_read_table_usage(
                function_name,
                notebook_content,
                func_type,
            )
            
            for severity, message, suggestion in spark_read_issues:
                result.add_issue(severity, 'custom_transformation_function_spark_read_table',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate config attribute references against metadata
            config_attr_issues = validate_config_attribute_references(
                function_name,
                notebook_content,
                func_type,
                table_id,
                primary_rows,
                advanced_rows,
                instance,
            )
            
            for severity, message, suggestion in config_attr_issues:
                result.add_issue(severity, 'custom_transformation_function_config_attribute',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate exception handling (no silent error swallowing)
            exception_issues = validate_exception_handling(
                function_name,
                notebook_content,
                func_type,
            )
            
            for severity, message, suggestion in exception_issues:
                result.add_issue(severity, 'custom_transformation_function_silent_error',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate .count() usage - kills performance
            count_issues = validate_dataframe_count_usage(
                function_name,
                notebook_content,
                func_type,
            )
            
            for severity, message, suggestion in count_issues:
                result.add_issue(severity, 'custom_transformation_function_count_usage',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)

            # Validate custom staging Spark write behavior
            staging_write_issues = validate_custom_staging_spark_write_usage(
                function_name,
                notebook_content,
                func_type,
            )

            for severity, message, suggestion in staging_write_issues:
                result.add_issue(severity, 'custom_transformation_function_staging_write_pattern',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)
            
            # Validate direct logging/print usage - must use log_and_print
            logging_issues = validate_direct_logging_usage(
                function_name,
                notebook_content,
                func_type,
            )
            
            for severity, message, suggestion in logging_issues:
                result.add_issue(severity, 'custom_transformation_function_direct_logging',
                    f"Table_ID {table_id}, notebook '{notebook_name}': {message}",
                    table_id=table_id,
                    line_number=line_number,
                    suggestion=suggestion)


# =============================================================================
# GIT REMOTE DETECTION - Customer Clone Detection
# =============================================================================

# Known accelerator template repository URLs are loaded from the machine-readable
# metadata contract so clone-detection policy stays out of the validator code.


def find_git_root(file_path: Path) -> Optional[Path]:
    """
    Find the git repository root by looking for .git directory.
    
    Args:
        file_path: Starting path to search from
        
    Returns:
        Path to git root directory, or None if not in a git repo
    """
    current = file_path.parent
    while current != current.parent:
        git_dir = current / '.git'
        if git_dir.exists():
            return current
        current = current.parent
    return None


def infer_workspace_folder(file_path: Path) -> Path:
    """Infer the workspace folder for a metadata notebook path.

    This avoids assuming a specific workspace folder name such as ``src``.
    The standard layout is:
    ``<workspace>/metadata/<metadata_notebook>.Notebook/notebook-content.sql``.

    Args:
        file_path: Path to a metadata SQL file or a path nested beneath it.

    Returns:
        Path to the inferred workspace folder.
    """
    current = file_path if file_path.is_dir() else file_path.parent

    for ancestor in (current, *current.parents):
        if ancestor.name.lower() == 'metadata' and ancestor.parent != ancestor:
            return ancestor.parent

    if current.name.endswith('.Notebook') and current.parent != current:
        return current.parent

    return current.parent if current.parent != current else current


def get_git_remote_url(git_root: Path) -> Optional[str]:
    """
    Get the 'origin' remote URL from git config.
    
    Args:
        git_root: Path to the git repository root
        
    Returns:
        Remote URL string, or None if not found
    """
    git_config = git_root / '.git' / 'config'
    if not git_config.exists():
        return None
    
    try:
        content = git_config.read_text(encoding='utf-8')
        # Parse git config to find origin URL
        in_origin_section = False
        for line in content.split('\n'):
            line = line.strip()
            if line == '[remote "origin"]':
                in_origin_section = True
            elif line.startswith('['):
                in_origin_section = False
            elif in_origin_section and line.startswith('url ='):
                return line.split('=', 1)[1].strip()
    except Exception:
        pass
    return None


def is_customer_clone_repo(file_path: Path) -> Tuple[bool, Optional[str]]:
    """
    Detect if this is the accelerator template repo (not a customer deployment).
    
    Template repos are connected to known accelerator template URLs.
    Customer deployments would have their own URLs or be disconnected.
    
    Args:
        file_path: Path to the metadata SQL file being validated
        
    Returns:
        Tuple of (is_clone, remote_url)
    """
    git_root = find_git_root(file_path)
    if not git_root:
        return False, None
    
    remote_url = get_git_remote_url(git_root)
    if not remote_url:
        return False, None
    
    # Check if remote URL matches known template repos
    remote_normalized = remote_url.rstrip('/')
    for template_url in ACCELERATOR_TEMPLATE_REPOS:
        template_normalized = template_url.rstrip('/')
        if remote_normalized.lower() == template_normalized.lower():
            return True, remote_url
    
    return False, remote_url


def validate_customer_clone_context(result: ValidationResult, file_path: Path) -> bool:
    """
    Check if this is the accelerator template repo (not a customer deployment).
    
    If so, skip certain validations that only apply to deployed environments
    (e.g., workspace variables, datastore validation).
    
    Args:
        result: ValidationResult to add issues to
        file_path: Path to the metadata SQL file
        
    Returns:
        bool: True if this is the template repo (caller should skip certain validations)
    """
    is_template, remote_url = is_customer_clone_repo(file_path)
    
    if is_template:
        # This is the template repo itself - silently skip variable validation
        # No warning needed since we're in the source template
        return True
    
    return False


# =============================================================================
# DATASTORE CONFIGURATION VALIDATION
# =============================================================================

def find_datastore_config_path(file_path: Path, environment: str = "DEV") -> Optional[Path]:
    """
    Locate a datastore configuration notebook in the workspace.

    The file must match the environment-specific notebook path:
    <workspace>/datastores/datastore_<ENV>.Notebook/notebook-content.sql

    Discovery order:
    1. Inferred workspace folder (derived from file_path: metadata_*.Notebook -> metadata -> workspace)
    2. Git root scoped to the same workspace folder name
    3. Git root wildcard (fallback, but only if no ambiguity)

    When git root cannot be determined (e.g., temp/test paths), this also checks the
    inferred workspace root directly.
    
    Args:
        file_path: Path to the metadata SQL file being validated
        environment: Target environment (DEV, QA, PROD). Defaults to DEV.
        
    Returns:
        Path to the matching datastore_<ENV>.Notebook/notebook-content.sql file, or None if not found
    """
    env_upper = environment.upper()
    env_pattern = f"datastore_{env_upper}.Notebook"
    git_root = find_git_root(file_path)
    workspace_dir = infer_workspace_folder(file_path)

    print(f"   Datastore discovery: workspace_dir={workspace_dir.name}, environment={env_upper}")

    # 1. Direct check in the inferred workspace folder (highest priority, most specific)
    if workspace_dir.exists():
        direct_match = workspace_dir / "datastores" / env_pattern / "notebook-content.sql"
        if direct_match.exists():
            print(f"   Datastore config found (workspace-scoped): {direct_match}")
            return direct_match

    # 2. If we have a git root, try scoped search using workspace folder name first
    if git_root:
        workspace_name = workspace_dir.name
        scoped_match = git_root / workspace_name / "datastores" / env_pattern / "notebook-content.sql"
        if scoped_match.exists():
            print(f"   Datastore config found (git-root-scoped): {scoped_match}")
            return scoped_match

        # 3. Fallback: wildcard search from git root, but error if ambiguous
        matches = list(git_root.glob(f"*/datastores/{env_pattern}/notebook-content.sql"))
        if len(matches) == 1:
            print(f"   Datastore config found (git-root-fallback): {matches[0]}")
            return matches[0]
        if len(matches) > 1:
            folder_names = [str(m.parent.parent.parent.name) for m in matches]
            print(f"   Datastore config ERROR: multiple matches in folders: {', '.join(sorted(folder_names))}")
            raise ValueError(
                f"Multiple datastore configuration notebooks found for environment '{environment}' "
                f"in folders: {', '.join(sorted(folder_names))}. "
                f"Use --base-dir to specify which workspace folder to use, or ensure your metadata "
                f"notebooks are inside the correct workspace folder."
            )

    print(f"   Datastore config NOT FOUND for environment '{env_upper}'")
    return None


def get_defined_datastores_from_config(datastore_config_path: Path) -> Set[str]:
    """
    Read the datastore configuration SQL file and extract all defined datastore names.
    
    Parses INSERT INTO [dbo].[Datastore_Configuration] statements and extracts the
    Datastore_Name values from the VALUES clause.
    
    Args:
        datastore_config_path: Path to the datastore_<ENV>.Notebook/notebook-content.sql file
        
    Returns:
        set: Set of datastore names defined in the configuration (lowercase)
    """
    if not datastore_config_path or not datastore_config_path.exists():
        return set()
    
    try:
        content = datastore_config_path.read_text(encoding='utf-8')
        
        # Extract datastore names from INSERT statements
        # Pattern matches VALUES rows like: ('bronze', 'Lakehouse', ...)
        # The first value in each row is the Datastore_Name
        datastore_names = set()
        
        # Find INSERT INTO Datastore_Configuration
        in_insert = False
        in_values = False
        
        for line in content.split('\n'):
            line_stripped = line.strip()
            
            # Skip comments
            if line_stripped.startswith('--'):
                continue
            
            # Detect start of INSERT INTO Datastore_Configuration
            if 'INSERT INTO' in line.upper() and 'DATASTORE_CONFIGURATION' in line.upper():
                in_insert = True
                # Check if VALUES is on the same line
                if 'VALUES' in line.upper():
                    in_values = True
                continue
            
            # Detect VALUES keyword
            if in_insert and 'VALUES' in line.upper():
                in_values = True
                continue
            
            # Parse VALUES rows - extract the first string value (Datastore_Name)
            if in_values and line_stripped.startswith('('):
                # Extract the first quoted value
                # Pattern: ('datastore_name', ...)
                match = re.match(r"\(\s*'([^']+)'", line_stripped)
                if match:
                    datastore_name = match.group(1).lower()
                    datastore_names.add(datastore_name)
            
            # Detect end of INSERT (semicolon outside of string)
            if in_insert and ';' in line_stripped and not line_stripped.startswith('--'):
                in_insert = False
                in_values = False
        
        return datastore_names
    except Exception:
        return set()


def get_duplicate_datastores_from_config(datastore_config_path: Path) -> Dict[str, int]:
    """
    Read the datastore configuration SQL file and detect duplicate Datastore_Name entries.
    
    The accelerator resolves datastores by name only (via Datastore_Name), so duplicate
    names cause ambiguous resolution at runtime.
    
    Args:
        datastore_config_path: Path to the datastore_<ENV>.Notebook/notebook-content.sql file
        
    Returns:
        dict: Mapping of {datastore_name: count} for names appearing more than once (lowercase)
    """
    if not datastore_config_path or not datastore_config_path.exists():
        return {}
    
    try:
        content = datastore_config_path.read_text(encoding='utf-8')
        all_names: list = []
        
        in_insert = False
        in_values = False
        
        for line in content.split('\n'):
            line_stripped = line.strip()
            
            if line_stripped.startswith('--'):
                continue
            
            if 'INSERT INTO' in line.upper() and 'DATASTORE_CONFIGURATION' in line.upper():
                in_insert = True
                if 'VALUES' in line.upper():
                    in_values = True
                continue
            
            if in_insert and 'VALUES' in line.upper():
                in_values = True
                continue
            
            if in_values and line_stripped.startswith('('):
                match = re.match(r"\(\s*'([^']+)'", line_stripped)
                if match:
                    all_names.append(match.group(1).lower())
            
            if in_insert and ';' in line_stripped and not line_stripped.startswith('--'):
                in_insert = False
                in_values = False
        
        # Return only names that appear more than once
        counts = Counter(all_names)
        return {name: count for name, count in counts.items() if count > 1}
    except Exception:
        return {}


def count_datastore_config_inserts(datastore_config_path: Path) -> Tuple[int, int]:
    """
    Count the number of INSERT INTO Datastore_Configuration statements and total rows.
    
    Args:
        datastore_config_path: Path to the datastore config SQL file
        
    Returns:
        Tuple of (insert_count, total_row_count)
    """
    if not datastore_config_path or not datastore_config_path.exists():
        return (0, 0)
    
    try:
        content = datastore_config_path.read_text(encoding='utf-8')
        insert_count = 0
        total_rows = 0
        in_values = False
        
        for line in content.split('\n'):
            stripped = line.strip()
            
            if stripped.startswith('--'):
                continue
            
            if 'INSERT INTO' in stripped.upper() and 'DATASTORE_CONFIGURATION' in stripped.upper():
                insert_count += 1
                in_values = 'VALUES' in stripped.upper()
                continue
            
            if in_values is False and 'VALUES' in stripped.upper():
                in_values = True
                continue
            
            if in_values and stripped.startswith('('):
                total_rows += 1
            
            if in_values and ';' in stripped and not stripped.startswith('--'):
                in_values = False
        
        return (insert_count, total_rows)
    except Exception:
        return (0, 0)


def validate_datastore_configuration(result: ValidationResult, orch_rows: list, primary_rows: list, file_path: Path, is_template_repo: bool = False, environment: str = "DEV"):
    """
    Validate that all datastores referenced in metadata have corresponding
    entries in the Datastore_Configuration table.

    Checks for configuration entries for:
    - Target_Datastore values from Orchestration table
    - source_details.datastore_name values from Primary Config
    - source_details.staging_catalog_name values from Primary Config
    - Lakehouse names from three-part table references (right_table_name,
      dimension_table_name, reference_table_name, union_tables)
    
    Args:
        result: ValidationResult to add issues to
        orch_rows: Parsed orchestration rows
        primary_rows: Parsed primary config rows
        file_path: Path to the SQL file being validated
        is_template_repo: True if working in the template repo (not connected to deployed workspace)
    """
    # Collect all unique datastores from metadata first (needed for warning message)
    datastores_used: Dict[str, int] = {}  # {datastore: first_line_number}
    
    # 1. From Orchestration: Target_Datastore column
    for row in orch_rows:
        datastore = row['target_datastore'].lower()
        if datastore and datastore not in datastores_used:
            datastores_used[datastore] = row['line_number']
    
    # 2. From Primary Config: source_details.datastore_name
    for row in primary_rows:
        if row['category'].lower() == 'source_details' and row['name'].lower() == 'datastore_name':
            datastore = row['value'].lower()
            if datastore and datastore not in datastores_used:
                datastores_used[datastore] = row['line_number']
    
    # 3. From Primary Config: source_details.staging_catalog_name
    for row in primary_rows:
        if row['category'].lower() == 'source_details' and row['name'].lower() == 'staging_catalog_name':
            datastore = row['value'].lower()
            if datastore and datastore not in datastores_used:
                datastores_used[datastore] = row['line_number']
    
    # 4. From Primary Config: lakehouse names in three-part table references
    #    (join_data.right_table_name, attach_dimension_surrogate_key.dimension_table_name,
    #     attach_dimension_surrogate_key.reference_table_name, union_data.union_tables)
    three_part_attributes = {'right_table_name', 'dimension_table_name', 'reference_table_name'}
    for row in primary_rows:
        attr = row['name'].lower()
        value = row['value']
        if not value:
            continue

        if attr in three_part_attributes:
            parts = value.split('.')
            if len(parts) == 3:
                lakehouse = parts[0].strip().lower()
                if lakehouse and lakehouse not in datastores_used:
                    datastores_used[lakehouse] = row['line_number']

        elif attr == 'union_tables':
            for table_ref in value.split(','):
                table_ref = table_ref.strip()
                parts = table_ref.split('.')
                if len(parts) == 3:
                    lakehouse = parts[0].strip().lower()
                    if lakehouse and lakehouse not in datastores_used:
                        datastores_used[lakehouse] = row['line_number']
    
    # If in template repo, always show warning about datastore names needing confirmation
    if is_template_repo and datastores_used:
        datastores_list = ", ".join(sorted(datastores_used.keys()))
        result.add_issue('warning', 'template_repo_datastores',
            f"Template repo detected. Datastores used: [{datastores_list}]. These are your TARGET LAKEHOUSE NAMES.",
            suggestion=(
                "🔴 CONFIRM YOUR LAKEHOUSE NAMES:\n"
                "   You're working in the template repo (not connected to a deployed Databricks workspace).\n"
                "   The datastore names in your metadata (Target_Datastore, datastore_name) become your\n"
                "   catalog table names at runtime.\n"
                "\n"
                "   → Did you name your lakehouses 'bronze', 'silver', 'gold'? Great, no changes needed.\n"
                "   → Using different names (e.g., 'raw', 'curated', 'analytics')? Tell the LLM your actual names.\n"
                "\n"
                "   This warning appears because we can't verify against your datastore configuration."
            ))
        return  # Skip further validation for template repo
    
    # Find the datastore configuration file
    try:
        datastore_config_path = find_datastore_config_path(file_path, environment)
    except ValueError as ex:
        result.add_issue('error', 'datastore_config_ambiguous',
            str(ex),
            suggestion=(
                "🔴 ACTION REQUIRED - Disambiguate datastore configuration:\n"
                "   Your repo has multiple workspace folders with datastore notebooks.\n"
                "   The validator doesn't know which one applies to this metadata file.\n"
                "   \n"
                "   Fix: Run the validator with --base-dir <folder> to specify your workspace folder,\n"
                "   or ensure your metadata notebooks live inside the correct workspace folder\n"
                "   (the datastores/ sibling folder will then be found automatically)."
            ))
        return
    
    if not datastore_config_path:
        result.add_issue('error', 'datastore_config',
            "Could not locate datastore configuration notebook. Datastore validation cannot continue.",
            suggestion=(
                "🔴 ACTION REQUIRED - Create datastore configuration:\n"
                "   Target_Datastore in metadata = your catalog table name\n"
                "   \n"
                "   Expected path: <workspace_folder>/datastores/datastore_<ENV>.Notebook/notebook-content.sql\n"
                "   \n"
                "   Create a datastore configuration notebook with INSERT statements like:\n"
                "   INSERT INTO [dbo].[Datastore_Configuration]\n"
                "       (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)\n"
                "   VALUES\n"
                "   ('bronze', 'Lakehouse', '<guid>', '<workspace-guid>', '<workspace-name>', 'Bronze', NULL, NULL),"
            ))
        return
    
    # Get defined datastores from configuration
    defined_datastores = get_defined_datastores_from_config(datastore_config_path)
    
    if not defined_datastores:
        result.add_issue('error', 'datastore_config',
            f"Datastore configuration exists at {datastore_config_path} but contains no datastore entries or could not be parsed",
            suggestion="Check that the notebook has INSERT INTO [dbo].[Datastore_Configuration] with VALUES rows.")
        return
    
    # Check for duplicate Datastore_Name entries in the config file
    duplicate_datastores = get_duplicate_datastores_from_config(datastore_config_path)
    for dup_name, dup_count in duplicate_datastores.items():
        result.add_issue('error', 'duplicate_datastore_name',
            f"Datastore_Name '{dup_name}' appears {dup_count} times in Datastore_Configuration. "
            f"The accelerator resolves datastores by name only — duplicates cause ambiguous resolution at runtime.",
            suggestion=(
                f"Remove duplicate entries so '{dup_name}' appears exactly once in:\n"
                f"   {datastore_config_path.parent.name}/notebook-content.sql\n"
                f"\n"
                f"   Each Datastore_Name must be unique across all entries in the Datastore_Configuration table.\n"
                f"   If you have two different artifacts that happen to share a name, rename one of them\n"
                f"   and update the corresponding Datastore_Configuration entry."
            ))

    # Check for multiple INSERT INTO Datastore_Configuration statements (should be combined unless > 1000 rows)
    insert_count, total_rows = count_datastore_config_inserts(datastore_config_path)
    if insert_count > 1 and total_rows < 1000:
        result.add_issue('error', 'insert_grouping',
            f"Multiple INSERT statements ({insert_count}) for Datastore_Configuration in "
            f"{datastore_config_path.parent.name}/notebook-content.sql with {total_rows} total rows. "
            f"Combine into a single INSERT (split only required when exceeding 1000 rows).",
            suggestion=(
                "Combine all VALUES rows into a single INSERT INTO [dbo].[Datastore_Configuration] statement.\n"
                "   Use comments within the VALUES block to separate logical groups (e.g., -- Core Lakehouses, -- External Connections).\n"
                "   Multiple INSERTs break CI/CD diff tracking and reduce maintainability."
            ))

    # Validate each datastore has configuration entry
    for datastore, line_num in datastores_used.items():
        if datastore not in defined_datastores:
            defined_list = ", ".join(sorted(defined_datastores))
            
            result.add_issue('error', 'datastore_not_found',
                f"Datastore '{datastore}' is referenced in metadata but not found in Datastore_Configuration table",
                line_number=line_num,
                suggestion=(
                    f"Datastores defined in configuration: [{defined_list}]\n"
                    f"\n"
                    f"   Every datastore referenced in metadata (Target_Datastore, datastore_name,\n"
                    f"   staging_catalog_name, and lakehouse names\n"
                    f"   in three-part table references like right_table_name, dimension_table_name,\n"
                    f"   reference_table_name, union_tables) MUST have a matching\n"
                    f"   entry in Datastore_Configuration.\n"
                    f"\n"
                    f"   To fix this, add '{datastore}' to your datastore configuration notebook:\n"
                    f"     1. Open {datastore_config_path.parent.name}/notebook-content.sql\n"
                    f"     2. Add an INSERT/VALUES row for '{datastore}':\n"
                    f"        For a catalog table:    ('{datastore}', 'Lakehouse', '<guid>', '<workspace-guid>', '<workspace-name>', '<layer>', NULL, NULL)\n"
                    f"        For an external source:    ('{datastore}', 'Connection', NULL, NULL, NULL, NULL, NULL, '<connection-id-guid>')\n"
                    f"        For a Databricks notebook:     ('{datastore}', 'Notebook', '<item-guid>', '<workspace-guid>', '<workspace-name>', NULL, NULL, NULL)\n"
                    f"        For a Databricks job:     ('{datastore}', 'Dataflow', '<item-guid>', '<workspace-guid>', '<workspace-name>', NULL, NULL, NULL)\n"
                    f"     3. Get the GUIDs from Databricks portal (Lakehouse settings or Manage Connections)"
                ))


# =============================================================================
# PLATFORM FILE VALIDATION (Rules 70-73)
# =============================================================================

GUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')


def validate_platform_file_for_notebook_folder(
    result: ValidationResult,
    notebook_folder: Path,
    *,
    table_id: Optional[int] = None,
    line_number: Optional[int] = None,
    context_prefix: str = "",
) -> None:
    """
    Validate the .platform file for any .Notebook folder (metadata or custom function notebook).
    Rules 70-72: Existence, displayName match, logicalId format (must be valid hex GUID).
    
    Args:
        result: ValidationResult to add issues to
        notebook_folder: Path to the .Notebook folder (e.g., NB_Custom_Test.Notebook/)
        table_id: Optional Table_ID for error context
        line_number: Optional line number for error context
        context_prefix: Optional prefix for error messages (e.g., "Table_ID 75, notebook 'NB_X': ")
    """
    platform_file = notebook_folder / ".platform"
    
    # .platform file must exist
    if not platform_file.exists():
        result.add_issue('error', 'platform_missing',
            f"{context_prefix}.platform file missing in {notebook_folder.name}/",
            table_id=table_id,
            line_number=line_number,
            suggestion="Create .platform file with $schema, metadata.type, metadata.displayName, config.version, config.logicalId")
        return
    
    # Parse the .platform file
    try:
        platform_content = platform_file.read_text(encoding='utf-8')
        platform_data = json.loads(platform_content)
    except json.JSONDecodeError as e:
        result.add_issue('error', 'platform_invalid_json',
            f"{context_prefix}.platform file has invalid JSON: {e}",
            table_id=table_id,
            line_number=line_number)
        return
    except Exception as e:
        result.add_issue('error', 'platform_read_error',
            f"{context_prefix}Could not read .platform file: {e}",
            table_id=table_id,
            line_number=line_number)
        return
    
    # displayName must match folder name (without .Notebook suffix)
    expected_display_name = notebook_folder.name.replace('.Notebook', '')
    actual_display_name = platform_data.get('metadata', {}).get('displayName', '')
    
    if not actual_display_name:
        result.add_issue('error', 'platform_missing_displayname',
            f"{context_prefix}.platform file missing metadata.displayName",
            table_id=table_id,
            line_number=line_number,
            suggestion=f"Add: \"metadata\": {{\"type\": \"Notebook\", \"displayName\": \"{expected_display_name}\"}}")
    elif actual_display_name != expected_display_name:
        result.add_issue('error', 'platform_displayname_mismatch',
            f"{context_prefix}.platform displayName '{actual_display_name}' does not match folder name '{expected_display_name}'",
            table_id=table_id,
            line_number=line_number,
            suggestion=f"Change displayName to: \"{expected_display_name}\"")
    
    # logicalId must be valid lowercase GUID format (hex chars only: 0-9, a-f)
    logical_id = platform_data.get('config', {}).get('logicalId', '')
    
    if not logical_id:
        result.add_issue('error', 'platform_missing_logicalid',
            f"{context_prefix}.platform file missing config.logicalId",
            table_id=table_id,
            line_number=line_number,
            suggestion="Add: \"config\": {\"version\": \"2.0\", \"logicalId\": \"<generate-new-guid>\"}")
    elif logical_id != logical_id.lower():
        result.add_issue('error', 'platform_logicalid_not_lowercase',
            f"{context_prefix}.platform logicalId '{logical_id}' must be lowercase",
            table_id=table_id,
            line_number=line_number,
            suggestion=f"Change to: \"{logical_id.lower()}\"")
    elif not GUID_PATTERN.match(logical_id):
        result.add_issue('error', 'platform_invalid_logicalid',
            f"{context_prefix}.platform logicalId '{logical_id}' is not a valid GUID format (only hex digits 0-9, a-f allowed)",
            table_id=table_id,
            line_number=line_number,
            suggestion="Use format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (lowercase hex). Non-hex characters like 'g' are not valid.")
    
    # logicalId uniqueness across workspace (checked in validate_platform_uniqueness_across_workspace)


def validate_platform_file(result: ValidationResult, file_path: Path) -> None:
    """
    Validate the .platform file for a metadata notebook.
    Validates: Existence, displayName match, logicalId format, logicalId uniqueness.
    Delegates to validate_platform_file_for_notebook_folder.
    """
    # file_path is the notebook-content.sql file
    notebook_folder = file_path.parent  # e.g., metadata_SalesData.Notebook
    validate_platform_file_for_notebook_folder(result, notebook_folder)


def precompute_logical_id_map(repo_root: Path) -> Dict[str, List[str]]:
    """
    Precompute a map of logicalId -> list of notebook folder names for all .Notebook folders in the repo.
    
    This function should be called once when validating multiple files to avoid O(N²) scanning.
    
    Args:
        repo_root: Root directory of the repository (typically where .git is located)
        
    Returns:
        Dict mapping lowercase logicalId to list of notebook folder names that have that logicalId
    """
    logical_id_locations: Dict[str, List[str]] = defaultdict(list)
    
    # Find all .platform files in .Notebook folders
    all_platform_files = list(repo_root.glob('**/*.Notebook/.platform'))
    
    for pf in all_platform_files:
        try:
            pf_data = json.loads(pf.read_text(encoding='utf-8'))
            pf_logical_id = pf_data.get('config', {}).get('logicalId', '').lower()
            if pf_logical_id:
                logical_id_locations[pf_logical_id].append(pf.parent.name)
        except:
            continue
    
    return dict(logical_id_locations)


def find_repo_root(file_path: Path) -> Path:
    """
    Find the repository root by walking up from the given file path.
    
    Args:
        file_path: Any file path within the repository
        
    Returns:
        Path to the repository root (where .git is located), or the filesystem root if not found
    """
    repo_root = file_path
    for _ in range(10):  # Safety limit
        parent = repo_root.parent
        if parent == repo_root:
            break
        repo_root = parent
        if (repo_root / '.git').exists():
            break
    return repo_root


def validate_platform_uniqueness_across_workspace(
    result: ValidationResult,
    file_path: Path,
    precomputed_logical_id_map: Optional[Dict[str, List[str]]] = None
) -> None:
    """
    Ensure logicalId is unique across all .Notebook folders in the workspace.
    
    Args:
        result: ValidationResult to add issues to
        file_path: Path to the metadata SQL file being validated
        precomputed_logical_id_map: Optional precomputed map from precompute_logical_id_map().
            If provided, avoids rescanning the entire repo (O(1) lookup instead of O(N)).
            If not provided, performs the scan (backwards compatible for single-file validation).
    """
    notebook_folder = file_path.parent
    platform_file = notebook_folder / ".platform"
    
    if not platform_file.exists():
        return  # Already reported in validate_platform_file
    
    try:
        platform_data = json.loads(platform_file.read_text(encoding='utf-8'))
        current_logical_id = platform_data.get('config', {}).get('logicalId', '').lower()
    except:
        return  # Already reported in validate_platform_file
    
    if not current_logical_id:
        return  # Already reported in validate_platform_file
    
    # Use precomputed map if available, otherwise compute on the fly
    if precomputed_logical_id_map is not None:
        logical_id_locations = precomputed_logical_id_map
    else:
        # Fallback: compute the map for this single file validation
        repo_root = find_repo_root(file_path)
        logical_id_locations = precompute_logical_id_map(repo_root)
    
    # Check if current logicalId has duplicates
    if current_logical_id in logical_id_locations:
        locations = logical_id_locations[current_logical_id]
        if len(locations) > 1:
            other_locations = [loc for loc in locations if loc != notebook_folder.name]
            if other_locations:
                result.add_issue('error', 'platform_duplicate_logicalid',
                    f".platform logicalId '{current_logical_id}' is duplicated in: {', '.join(other_locations)}",
                    suggestion="Each notebook must have a unique logicalId. Generate a new GUID for this notebook.")


def validate_notebook_format(result: ValidationResult, content: str) -> None:
    """
    Validate SQL notebook format requirements.
    
    SQL notebooks require EXACT header and footer structures:
    
    Header META block (after '-- Databricks notebook source' and '-- METADATA **'):
    {
      "kernel_info": { "name": "sqldatawarehouse" },
      "dependencies": {
        "warehouse": {
          "default_warehouse": "<guid>",
          "known_warehouses": [{ "id": "<guid>", "type": "Datawarehouse" }]
        }
      }
    }
    
    Footer META block (at end of file):
    {
      "language": "sql",
      "language_group": "sqldatawarehouse"
    }
    
    No extra keys are allowed in any of these structures.
    """
    lines = content.strip().split('\n')
    
    if not lines:
        return
    
    # Check 1: File must start with '-- Databricks notebook source'
    if not lines[0].strip() == '-- Databricks notebook source':
        result.add_issue('error', 'notebook_format',
            "File must start with '-- Databricks notebook source' on the first line",
            suggestion="Add '-- Databricks notebook source' as the very first line of the file")
    
    # Check 2: Must have '-- CELL ********************' marker before SQL content
    has_cell_marker = any('-- CELL ********************' in line for line in lines)
    if not has_cell_marker:
        result.add_issue('error', 'notebook_format',
            "Missing '-- CELL ********************' marker before SQL content",
            suggestion="Add '-- CELL ********************' marker after the META header block")
    
    # Extract and parse the header META block (first META block in the file)
    header_meta_json = _extract_meta_block(lines, is_header=True)
    if header_meta_json:
        _validate_header_meta_structure(result, header_meta_json)
    else:
        result.add_issue('error', 'notebook_format',
            "Could not find or parse header META block",
            suggestion="Ensure the file has a valid META block after '-- METADATA ********************' at the top")
    
    # Extract and parse the footer META block (last META block in the file)
    footer_meta_json = _extract_meta_block(lines, is_header=False)
    if footer_meta_json:
        _validate_footer_meta_structure(result, footer_meta_json)
    else:
        result.add_issue('error', 'notebook_format',
            "Could not find or parse footer META block at end of file",
            suggestion="Add footer META block with language info at the end of the file")


def _extract_meta_block(lines: list, is_header: bool) -> dict:
    """Extract and parse a META block from the notebook content.
    
    Args:
        lines: All lines of the file
        is_header: If True, extract the first META block; if False, extract the last META block
    """
    # Find all META block boundaries
    meta_starts = []
    for i, line in enumerate(lines):
        if '-- METADATA ********************' in line:
            meta_starts.append(i)
    
    if not meta_starts:
        return None
    
    # Select which META block to parse
    if is_header:
        start_idx = meta_starts[0] if meta_starts else None
    else:
        start_idx = meta_starts[-1] if meta_starts else None
    
    if start_idx is None:
        return None
    
    # Collect JSON lines (lines starting with '-- META')
    json_lines = []
    for i in range(start_idx + 1, len(lines)):
        line = lines[i].strip()
        if line.startswith('-- META'):
            # Extract the JSON part after '-- META'
            json_part = line[7:].strip()  # Remove '-- META' prefix
            json_lines.append(json_part)
        elif line.startswith('-- CELL') or (line.startswith('--') and 'META' not in line):
            # End of META block
            break
        elif line == '':
            continue
        elif not line.startswith('--'):
            # Non-comment line = end of META block
            break
    
    if not json_lines:
        return None
    
    # Join and parse as JSON
    json_str = '\n'.join(json_lines)
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        return None


def _validate_header_meta_structure(result: ValidationResult, meta: dict) -> None:
    """Validate the header META block has the exact required structure with no extra keys."""
    
    # Expected top-level keys
    required_top_keys = {'kernel_info'}
    optional_top_keys = {'dependencies'}  # dependencies is optional - populated at deployment time
    allowed_top_keys = required_top_keys | optional_top_keys
    actual_top_keys = set(meta.keys())
    
    # Check for missing required keys
    missing_top = required_top_keys - actual_top_keys
    for key in missing_top:
        result.add_issue('error', 'notebook_format',
            f"Header META block missing required key: '{key}'",
            suggestion=f"Add '{key}' to the header META block")
    
    # Check for extra keys
    extra_top = actual_top_keys - allowed_top_keys
    for key in extra_top:
        result.add_issue('error', 'notebook_format',
            f"Header META block has unexpected key: '{key}'",
            suggestion=f"Remove '{key}' from the header META block - only 'kernel_info' and 'dependencies' are allowed")
    
    # Validate kernel_info structure
    if 'kernel_info' in meta:
        kernel_info = meta['kernel_info']
        if not isinstance(kernel_info, dict):
            result.add_issue('error', 'notebook_format',
                "kernel_info must be an object",
                suggestion='Set kernel_info to: { "name": "sqldatawarehouse" }')
        else:
            expected_kernel_keys = {'name'}
            actual_kernel_keys = set(kernel_info.keys())
            
            # Check for extra keys in kernel_info
            extra_kernel = actual_kernel_keys - expected_kernel_keys
            for key in extra_kernel:
                result.add_issue('error', 'notebook_format',
                    f"kernel_info has unexpected key: '{key}'",
                    suggestion=f"Remove '{key}' from kernel_info - only 'name' is allowed")
            
            # Check kernel name value
            if 'name' not in kernel_info:
                result.add_issue('error', 'notebook_format',
                    "kernel_info missing 'name' key",
                    suggestion='Add "name": "sqldatawarehouse" to kernel_info')
            elif kernel_info['name'] != 'sqldatawarehouse':
                result.add_issue('error', 'notebook_format',
                    f"kernel_info.name must be 'sqldatawarehouse', found '{kernel_info['name']}'",
                    suggestion='Set kernel_info.name to "sqldatawarehouse"')
    
    # Validate dependencies structure
    if 'dependencies' in meta:
        dependencies = meta['dependencies']
        if not isinstance(dependencies, dict):
            result.add_issue('error', 'notebook_format',
                "dependencies must be an object",
                suggestion='Set dependencies to contain a "warehouse" object')
        else:
            expected_dep_keys = {'warehouse'}
            actual_dep_keys = set(dependencies.keys())
            
            # Check for extra keys in dependencies
            extra_dep = actual_dep_keys - expected_dep_keys
            for key in extra_dep:
                result.add_issue('error', 'notebook_format',
                    f"dependencies has unexpected key: '{key}'",
                    suggestion=f"Remove '{key}' from dependencies - only 'warehouse' is allowed")
            
            if 'warehouse' not in dependencies:
                result.add_issue('error', 'notebook_format',
                    "dependencies missing 'warehouse' key",
                    suggestion='Add "warehouse" object to dependencies')
            else:
                _validate_warehouse_structure(result, dependencies['warehouse'])


def _validate_warehouse_structure(result: ValidationResult, warehouse: dict) -> None:
    """Validate the warehouse object within dependencies."""
    if not isinstance(warehouse, dict):
        result.add_issue('error', 'notebook_format',
            "dependencies.warehouse must be an object",
            suggestion='Set warehouse to contain "default_warehouse" and "known_warehouses"')
        return
    
    expected_wh_keys = {'default_warehouse', 'known_warehouses'}
    actual_wh_keys = set(warehouse.keys())
    
    # Check for missing keys
    missing_wh = expected_wh_keys - actual_wh_keys
    for key in missing_wh:
        result.add_issue('error', 'notebook_format',
            f"dependencies.warehouse missing required key: '{key}'",
            suggestion=f"Add '{key}' to dependencies.warehouse")
    
    # Check for extra keys
    extra_wh = actual_wh_keys - expected_wh_keys
    for key in extra_wh:
        result.add_issue('error', 'notebook_format',
            f"dependencies.warehouse has unexpected key: '{key}'",
            suggestion=f"Remove '{key}' from dependencies.warehouse - only 'default_warehouse' and 'known_warehouses' are allowed")
    
    # Validate default_warehouse is a GUID string
    if 'default_warehouse' in warehouse:
        dw = warehouse['default_warehouse']
        if not isinstance(dw, str):
            result.add_issue('error', 'notebook_format',
                "default_warehouse must be a string (GUID)",
                suggestion='Set default_warehouse to a valid GUID string')
        elif not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', dw.lower()):
            result.add_issue('error', 'notebook_format',
                f"default_warehouse is not a valid GUID format: '{dw}'",
                suggestion='Set default_warehouse to a valid GUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)')
    
    # Validate known_warehouses array
    if 'known_warehouses' in warehouse:
        kw = warehouse['known_warehouses']
        if not isinstance(kw, list):
            result.add_issue('error', 'notebook_format',
                "known_warehouses must be an array",
                suggestion='Set known_warehouses to an array of warehouse objects')
        elif len(kw) == 0:
            result.add_issue('error', 'notebook_format',
                "known_warehouses array is empty",
                suggestion='Add at least one warehouse object to known_warehouses array')
        else:
            for i, wh_entry in enumerate(kw):
                _validate_known_warehouse_entry(result, wh_entry, i)


def _validate_known_warehouse_entry(result: ValidationResult, entry: dict, index: int) -> None:
    """Validate a single entry in the known_warehouses array."""
    if not isinstance(entry, dict):
        result.add_issue('error', 'notebook_format',
            f"known_warehouses[{index}] must be an object",
            suggestion='Each known_warehouses entry must have "id" and "type" keys')
        return
    
    expected_entry_keys = {'id', 'type'}
    actual_entry_keys = set(entry.keys())
    
    # Check for missing keys
    missing_entry = expected_entry_keys - actual_entry_keys
    for key in missing_entry:
        result.add_issue('error', 'notebook_format',
            f"known_warehouses[{index}] missing required key: '{key}'",
            suggestion=f"Add '{key}' to known_warehouses[{index}]")
    
    # Check for extra keys
    extra_entry = actual_entry_keys - expected_entry_keys
    for key in extra_entry:
        result.add_issue('error', 'notebook_format',
            f"known_warehouses[{index}] has unexpected key: '{key}'",
            suggestion=f"Remove '{key}' from known_warehouses[{index}] - only 'id' and 'type' are allowed")
    
    # Validate id is a GUID string
    if 'id' in entry:
        wh_id = entry['id']
        if not isinstance(wh_id, str):
            result.add_issue('error', 'notebook_format',
                f"known_warehouses[{index}].id must be a string (GUID)",
                suggestion='Set id to a valid GUID string')
        elif not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', wh_id.lower()):
            result.add_issue('error', 'notebook_format',
                f"known_warehouses[{index}].id is not a valid GUID format: '{wh_id}'",
                suggestion='Set id to a valid GUID (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)')
    
    # Validate type is exactly 'Datawarehouse'
    if 'type' in entry:
        wh_type = entry['type']
        if wh_type != 'Datawarehouse':
            result.add_issue('error', 'notebook_format',
                f"known_warehouses[{index}].type must be 'Datawarehouse', found '{wh_type}'",
                suggestion='Set type to "Datawarehouse"')


def _validate_footer_meta_structure(result: ValidationResult, meta: dict) -> None:
    """Validate the footer META block has the exact required structure with no extra keys."""
    
    # Expected keys
    expected_keys = {'language', 'language_group'}
    actual_keys = set(meta.keys())
    
    # Check for missing keys
    missing = expected_keys - actual_keys
    for key in missing:
        result.add_issue('error', 'notebook_format',
            f"Footer META block missing required key: '{key}'",
            suggestion=f"Add '{key}' to the footer META block")
    
    # Check for extra keys
    extra = actual_keys - expected_keys
    for key in extra:
        result.add_issue('error', 'notebook_format',
            f"Footer META block has unexpected key: '{key}'",
            suggestion=f"Remove '{key}' from footer META block - only 'language' and 'language_group' are allowed")
    
    # Validate language value
    if 'language' in meta:
        if meta['language'] != 'sql':
            result.add_issue('error', 'notebook_format',
                f"Footer META language must be 'sql', found '{meta['language']}'",
                suggestion='Set language to "sql"')
    
    # Validate language_group value
    if 'language_group' in meta:
        if meta['language_group'] != 'sqldatawarehouse':
            result.add_issue('error', 'notebook_format',
                f"Footer META language_group must be 'sqldatawarehouse', found '{meta['language_group']}'",
                suggestion='Set language_group to "sqldatawarehouse"')


def validate_warehouse_reference(result: ValidationResult, file_path: Path, content: str) -> None:
    """
    Validate that default_warehouse and known_warehouses.id in the notebook META header
    match the logicalId in metadata_warehouse.Warehouse/.platform.
    """
    # Extract default_warehouse from META header
    default_warehouse_match = re.search(r'"default_warehouse"\s*:\s*"([^"]+)"', content)
    if not default_warehouse_match:
        # No default_warehouse in META header - this is OK for non-SQL notebooks
        return
    
    default_warehouse = default_warehouse_match.group(1).lower()
    
    # Extract known_warehouses ids
    known_warehouse_ids = []
    known_warehouses_pattern = re.findall(r'"known_warehouses"\s*:\s*\[([^\]]+)\]', content, re.DOTALL)
    if known_warehouses_pattern:
        id_matches = re.findall(r'"id"\s*:\s*"([^"]+)"', known_warehouses_pattern[0])
        known_warehouse_ids = [id.lower() for id in id_matches]
    
    # Find the metadata_warehouse.Warehouse/.platform file
    # Scope search to the workspace folder (not the entire repo) to avoid
    # matching warehouse .platform files from other workspace folders.
    workspace_dir = infer_workspace_folder(file_path)
    
    # Search within the workspace folder first, then fall back to repo root
    search_roots = [workspace_dir]
    git_root = find_git_root(file_path)
    if git_root and git_root != workspace_dir:
        # Also try git_root/<workspace_name>/ in case workspace_dir inference is off
        scoped_root = git_root / workspace_dir.name
        if scoped_root.exists() and scoped_root != workspace_dir:
            search_roots.append(scoped_root)
    
    metadata_warehouse_platform = None
    
    for search_root in search_roots:
        if not search_root.exists():
            continue
        warehouse_platform_files = list(search_root.glob('*[Ww]arehouse*/.platform'))
        for wpf in warehouse_platform_files:
            # Look for the metadata warehouse specifically
            if 'metadata' in wpf.parent.name.lower() or wpf.parent.name.lower().endswith('.warehouse'):
                try:
                    wpf_data = json.loads(wpf.read_text(encoding='utf-8'))
                    if wpf_data.get('metadata', {}).get('type') == 'Warehouse':
                        metadata_warehouse_platform = wpf
                        break
                except:
                    continue
        if metadata_warehouse_platform:
            break
    
    if not metadata_warehouse_platform:
        # Could not find metadata warehouse .platform - skip validation
        return
    
    try:
        warehouse_data = json.loads(metadata_warehouse_platform.read_text(encoding='utf-8'))
        warehouse_logical_id = warehouse_data.get('config', {}).get('logicalId', '').lower()
    except:
        return
    
    if not warehouse_logical_id:
        return
    
    # Validate default_warehouse matches
    if default_warehouse != warehouse_logical_id:
        result.add_issue('warning', 'warehouse_reference_mismatch',
            f"default_warehouse '{default_warehouse}' does not match metadata_warehouse.Warehouse logicalId '{warehouse_logical_id}'",
            suggestion=f"Update default_warehouse in META header to: \"{warehouse_logical_id}\"")
    
    # Validate known_warehouses ids match
    for known_id in known_warehouse_ids:
        if known_id != warehouse_logical_id:
            result.add_issue('warning', 'warehouse_reference_mismatch',
                f"known_warehouses id '{known_id}' does not match metadata_warehouse.Warehouse logicalId '{warehouse_logical_id}'",
                suggestion=f"Update known_warehouses id in META header to: \"{warehouse_logical_id}\"")


def format_results(result: ValidationResult) -> str:
    output = []
    output.append("=" * 70)
    # Use the .Notebook folder name since all files are named notebook-content.sql
    file_path = Path(result.file_path)
    if file_path.parent.name.endswith('.Notebook'):
        display_name = file_path.parent.name
    else:
        display_name = file_path.name
    output.append(f"Metadata Validation Results: {display_name}")
    output.append("=" * 70)
    output.append("")

    errors = [i for i in result.issues if i.severity == 'error']
    warnings = [i for i in result.issues if i.severity == 'warning']

    if errors:
        output.append(f"❌ ERRORS ({len(errors)})")
        output.append("-" * 40)
        for issue in errors:
            line_info = f" (line {issue.line_number})" if issue.line_number else ""
            table_info = f" [Table_ID: {issue.table_id}]" if issue.table_id else ""
            output.append(f"  • {issue.message}{table_info}{line_info}")
            if issue.suggestion:
                output.append(f"    → {issue.suggestion}")
        output.append("")

    if warnings:
        output.append(f"⚠️  WARNINGS ({len(warnings)})")
        output.append("-" * 40)
        for issue in warnings:
            line_info = f" (line {issue.line_number})" if issue.line_number else ""
            table_info = f" [Table_ID: {issue.table_id}]" if issue.table_id else ""
            output.append(f"  • {issue.message}{table_info}{line_info}")
            if issue.suggestion:
                output.append(f"    → {issue.suggestion}")
        output.append("")

    if not errors and not warnings:
        output.append("✅ All validation checks passed!")
        output.append("")

    output.append("-" * 70)
    output.append(f"Summary: {len(errors)} errors, {len(warnings)} warnings")
    output.append("")

    return "\n".join(output)


# =============================================================================
# MAIN
# =============================================================================

def validate_metadata_file(
    file_path: Path,
    precomputed_logical_id_map: Optional[Dict[str, List[str]]] = None,
    precomputed_table_id_map: Optional[Dict[int, str]] = None,
    precomputed_target_entity_map: Optional[Dict[str, str]] = None,
    environment: str = "DEV",
) -> ValidationResult:
    """
    Validate a metadata SQL file against the Data Dictionary.
    
    Args:
        file_path: Path to the metadata SQL file to validate
        precomputed_logical_id_map: Optional precomputed map from precompute_logical_id_map().
            When validating multiple files, pass this to avoid O(N²) scanning.
        precomputed_table_id_map: Optional precomputed map from precompute_cross_file_table_ids().
            When validating multiple files, pass this to avoid O(N²) scanning.
        precomputed_target_entity_map: Optional precomputed map from precompute_cross_file_target_entities().
            When validating multiple files, pass this to avoid O(N²) scanning.
        environment: Target environment for datastore config lookup (DEV, QA, PROD). Defaults to DEV.
            
    Returns:
        ValidationResult containing all errors and warnings found
    """
    result = ValidationResult(file_path=str(file_path))

    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        result.add_issue('error', 'file_read', f"Could not read file: {e}")
        return result

    # Check if file is empty or nearly empty (unsaved file detection)
    if len(content.strip()) == 0:
        result.add_issue('error', 'file_empty',
            "File is empty (0 bytes). The file may not have been saved to disk yet. "
            "Please save the file in VS Code (Ctrl+S) and re-run validation.")
        return result

    # Databricks notebook Format Validation - runs early before other checks
    # This validates kernel_info, dependencies, warehouse type, etc.
    validate_notebook_format(result, content)

    sections = find_insert_sections(content)

    if not sections:
        result.add_issue('error', 'structure', "No INSERT INTO dbo.Data_Pipeline_* statements found")
        return result

    orch_rows = []
    primary_rows = []
    advanced_rows = []

    if 'orchestration' in sections:
        orch_rows = parse_orchestration_rows(sections['orchestration']['lines'], sections['orchestration']['start'])

    if 'primary_config' in sections:
        primary_rows = parse_primary_config_rows(sections['primary_config']['lines'], sections['primary_config']['start'])

    if 'advanced_config' in sections:
        advanced_rows = parse_advanced_config_rows(sections['advanced_config']['lines'], sections['advanced_config']['start'])

    # SQL syntax validation (missing commas, etc.)
    validate_sql_syntax(result, content)

    # Check for unbalanced quotes in string literals
    validate_unbalanced_quotes(result, content)

    # Check for improper INSERT statement grouping (multiple INSERTs that should be combined)
    validate_insert_statement_grouping(result, content)

    # Format/style validations
    validate_delete_statements(result, content)  # No hardcoded Table_IDs in DELETE
    validate_comment_style(result, content)  # Only -- comments (ignores strings)
    validate_multiline_values_rows(result, content)  # Each VALUES row on one line
    validate_delete_presence(result, content)  # All 3 DELETE statements present
    validate_delete_order(result, content)  # DELETE order (Advanced→Primary→Orch)
    validate_file_section_order(result, content)  # Section order

    validate_orchestration(result, orch_rows, environment)

    # Cross-file Table_ID uniqueness check
    validate_table_id_uniqueness_across_directory(result, file_path, orch_rows, precomputed_table_id_map)
    
    # Cross-file Target_Entity uniqueness check
    validate_target_entity_uniqueness_across_directory(result, file_path, orch_rows, precomputed_target_entity_map)

    validate_primary_config(result, primary_rows, orch_rows)
    validate_orchestration_primary_config_coverage(result, orch_rows, primary_rows)
    validate_advanced_config(result, advanced_rows, orch_rows)

    # Cross-table validations
    validate_conflicting_configs(result, primary_rows, advanced_rows)  # Watermark/SCD2 vs overwrite
    validate_custom_ingestion_table_name_conflict(result, primary_rows)  # Rule 80: Custom ingestion function vs table_name
    validate_cdf_soft_delete_conflict(result, primary_rows)  # Rule 81: CDF overrides soft delete columns
    validate_cdf_watermark_column_conflict(result, primary_rows)  # Rule 82: CDF overrides watermark column
    validate_staging_wildcard_conflict(result, primary_rows)  # Rule 84: Staging overrides wildcard path
    validate_staging_custom_table_ingestion_conflict(result, primary_rows)  # Rule 85: Staging disables custom table ingestion
    validate_replace_where_column_without_merge_type(result, primary_rows)  # Rule 86: replace_where_column without replace_where merge
    validate_soft_delete_merge_type_conflict(result, primary_rows)  # Rule 87: Soft delete with explicit merge_type='merge'
    validate_compute_statistics_conflict(result, primary_rows)  # Rule 88: Both compute_statistics configs set
    validate_duplicate_pk_check_without_pks(result, primary_rows, orch_rows)  # Rule 89: if_duplicate_primary_keys without PKs
    validate_source_attribute_alignment(result, orch_rows, advanced_rows)  # source/DB config alignment
    validate_columns_to_rename_count(result, advanced_rows)  # Rename column count match
    validate_category_ordering(result, advanced_rows)  # Transformations before DQ

    # Additional validations
    validate_new_type_values(result, advanced_rows)  # change_data_types.new_type enum
    validate_transform_datetime_conditionals(result, advanced_rows)  # transform_datetime conditional attrs
    validate_range_requires_bound(result, advanced_rows)  # validate_range needs min or max
    validate_limited_if_not_compliant(result, advanced_rows)  # freshness/completeness no quarantine
    validate_scd2_requires_timestamp(result, primary_rows)  # SCD2 requires timestamp column
    validate_replace_where_requires_column(result, primary_rows, advanced_rows)  # replace_where needs column
    validate_exact_find_replace_count(result, primary_rows)  # exact_find/exact_replace count match
    validate_xml_namespace_count(result, primary_rows)  # xml namespace keys/values count match
    validate_consolidatable_transformations(result, advanced_rows)  # Consolidate multi-column transformations
    validate_duplicate_transformations(result, advanced_rows)  # Detect exact duplicate transformations
    validate_order_of_operations_dependencies(result, orch_rows, advanced_rows)  # Validate dependency ordering
    
    # NEW: Safety & Hygiene validations
    validate_dq_numeric_ranges(result, advanced_rows)
    validate_column_name_format(result, advanced_rows)
    validate_window_function_requirements(result, advanced_rows)
    validate_regex_syntax(result, primary_rows, advanced_rows)

    # Medallion Architecture Anti-Pattern Detection
    validate_medallion_anti_patterns(result, orch_rows, primary_rows, advanced_rows)  # Bronze/Silver/Gold best practices

    # Tier 1 First Guardrail
    validate_custom_transformation_function_ratio(result, advanced_rows)

    # Custom Function Notebook Validation
    validate_custom_transformation_function_notebooks(result, primary_rows, advanced_rows, file_path)  # Validate custom function notebooks exist and have correct signatures

    # Customer Clone Detection - Check if this is the template repo
    is_template_repo = validate_customer_clone_context(result, file_path)  # Detect template repo

    # Datastore Configuration Validation
    # Always run validation, but pass is_template_repo flag so it can show appropriate warning
    validate_datastore_configuration(result, orch_rows, primary_rows, file_path, is_template_repo, environment)  # Validate datastores exist in Datastore_Configuration table

    # Platform File Validation
    validate_platform_file(result, file_path)  # Existence, displayName, logicalId format
    validate_platform_uniqueness_across_workspace(result, file_path, precomputed_logical_id_map)  # logicalId uniqueness

    # Warehouse Reference Validation
    validate_warehouse_reference(result, file_path, content)  # default_warehouse/known_warehouses match warehouse .platform

    # Note: validate_notebook_format is called earlier in the function
    # to ensure kernel_info, dependencies, and warehouse type are checked before INSERT statement validation

    return result


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate metadata SQL files against the Data Dictionary.')
    parser.add_argument('file', nargs='?', help='Path to a specific SQL file to validate')
    parser.add_argument('--base-dir', dest='base_dir', help='Base directory containing the metadata folder with .Notebook subdirectories (e.g., workspace git folder name)')
    parser.add_argument('--environment', dest='environment', default='DEV',
                        help='Target environment for datastore config lookup (DEV, QA, PROD). Defaults to DEV.')
    
    args = parser.parse_args()
    
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent.parent.parent  # Go up from .github/skills/metadata-validation to repo root

    if args.file:
        file_path = Path(args.file)
        if not file_path.exists():
            print(f"Error: File not found: {file_path}")
            sys.exit(1)
        files = [file_path]
    else:
        if args.base_dir:
            metadata_dir = repo_root / args.base_dir / "metadata"
            if not metadata_dir.exists():
                print(f"Error: Metadata directory not found: {metadata_dir}")
                print(f"Expected: {metadata_dir}/metadata_<TriggerName>.Notebook/notebook-content.sql")
                sys.exit(1)

            files = list(metadata_dir.glob("*.Notebook/notebook-content.sql"))
            if not files:
                print(f"No metadata notebooks found in {metadata_dir}")
                print(f"Expected: {metadata_dir}/metadata_<TriggerName>.Notebook/notebook-content.sql")
                sys.exit(1)
        else:
            # Auto-discover metadata directories without defaulting to any specific base dir.
            # Root-level metadata is intentionally excluded; users should pass --base-dir
            # when they need to target a specific workspace folder.
            candidate_dirs = []
            for pattern in ("*/metadata", "*/*/metadata"):
                for path in repo_root.glob(pattern):
                    if path.is_dir() and path not in candidate_dirs:
                        candidate_dirs.append(path)

            if not candidate_dirs:
                print("Error: No metadata directory found.")
                print("Use --base-dir <folder> when metadata is nested under a specific workspace folder.")
                sys.exit(1)

            candidates_with_files = []
            for candidate in candidate_dirs:
                candidate_files = list(candidate.glob("*.Notebook/notebook-content.sql"))
                if candidate_files:
                    candidates_with_files.append((candidate, candidate_files))

            if not candidates_with_files:
                print("Error: Metadata directories were found, but no metadata notebooks were found in any of them.")
                for candidate in candidate_dirs:
                    print(f"  - {candidate}")
                print("Expected: <base-dir>/metadata/metadata_<TriggerName>.Notebook/notebook-content.sql")
                print("Use --base-dir <folder> to target the intended workspace folder.")
                sys.exit(1)

            if len(candidates_with_files) > 1:
                print("Error: Multiple metadata directories contain notebooks. Specify --base-dir to disambiguate.")
                for candidate, _ in candidates_with_files:
                    print(f"  - {candidate}")
                sys.exit(1)

            metadata_dir, files = candidates_with_files[0]

    total_errors = 0
    total_warnings = 0

    # Precompute maps once for all files to avoid O(N²) scanning
    # This is only needed when validating multiple files
    precomputed_logical_id_map = None
    precomputed_table_id_map = None
    precomputed_target_entity_map = None
    
    if len(files) > 1:
        # Find repo root from the first file for logicalId map
        repo_root_path = find_repo_root(files[0])
        precomputed_logical_id_map = precompute_logical_id_map(repo_root_path)
        
        # Find metadata directory for cross-file Table_ID and Target_Entity maps
        # All files should be in the same metadata directory
        metadata_dir = files[0].parent.parent  # Go from notebook-content.sql -> .Notebook -> metadata/
        precomputed_table_id_map = precompute_cross_file_table_ids(metadata_dir)
        precomputed_target_entity_map = precompute_cross_file_target_entities(metadata_dir)

    for file_path in files:
        result = validate_metadata_file(
            file_path,
            precomputed_logical_id_map,
            precomputed_table_id_map,
            precomputed_target_entity_map,
            environment=args.environment,
        )
        print(format_results(result))
        total_errors += result.error_count()
        total_warnings += result.warning_count()

    if len(files) > 1:
        print("=" * 70)
        print(f"TOTAL: {len(files)} files, {total_errors} errors, {total_warnings} warnings")
        print("=" * 70)

    sys.exit(1 if total_errors > 0 else 0)


if __name__ == "__main__":
    main()
