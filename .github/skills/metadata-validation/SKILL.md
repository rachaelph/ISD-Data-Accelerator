---
name: metadata-validation
description: "Validate metadata SQL files against the canonical metadata reference and 40+ structural rules. Run this before delivering any metadata SQL file."
---

# Metadata Validation Skill

Executable validation utility for metadata SQL files.

## Quick Start

## Usage

```powershell
# Validate a single file
python .github/skills/metadata-validation/validate_metadata_sql.py <path_to_sql_file>

# Validate all metadata notebooks under a workspace git folder
python .github/skills/metadata-validation/validate_metadata_sql.py --base-dir <git_folder>
```

## What It Checks (40+ rules)

- All attribute names and values against the canonical metadata reference in `docs/METADATA_REFERENCE.md`
- Processing method values: `batch`, `pipeline_stage_and_batch`, `pipeline_stage_only`, `execute_warehouse_sp`, `execute_databricks_notebook`, `execute_databricks_job`
- Execute-method source host metadata such as `source_details.datastore_name`
- Required `source_details.stored_procedure_name` for `execute_warehouse_sp`, paired with `source_details.datastore_name`
- Boolean metadata such as `source_details.exit_after_staging`
- Category placement (Primary vs Advanced)
- Instance numbering and sequencing
- Required attributes per transformation type
- Cross-config consistency (SCD2, merge types, etc.)
- SQL syntax issues (semicolons, multi-line VALUES, etc.)
- Orchestration validations
- GUID format validation
- Boolean validation
- Three-part naming
- Multi-value count alignment
- Cross-file Table_ID uniqueness
- INSERT grouping
- Hardcoded DELETE detection

## When to Run

- **BEFORE delivering** any metadata SQL file to the user
- **AFTER every edit** to a metadata SQL file, including "quick fixes"
- Fix ALL errors before delivery. No exceptions.

## Files

| File | Purpose |
|------|---------|
| `validate_metadata_sql.py` | Main validator script (standalone Python, no PySpark needed) |
| `test_validate_metadata_sql.py` | Unit test suite for the validator |
