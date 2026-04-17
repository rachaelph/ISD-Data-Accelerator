---
applyTo: "**/*.Notebook/**,**/databricks_batch_engine/**"
---

# Module & Notebook Instructions

> Auto-applied when working on engine modules (`src/databricks_batch_engine/`) or any `*.Notebook/` folder.

---

## Architecture Context

Python modules are the processing engine of the accelerator. They sit in the **Processing Layer** of this flow:

```
Orchestration (Databricks Jobs) → Metadata (SQL Warehouse) → Processing (batch_processing.py) → Logging
```

---

## Core Modules (Require Full Checklist)

| Module | Purpose | Key Functions |
|--------|---------|---------------|
| `helper_functions_1.py` | Transformations, data quality, merge logic, SCD2 | `transformation_functions()`, `execute_data_quality_checks()`, `merge_data()` |
| `helper_functions_2.py` | DDL generation, file ingestion utilities | `build_source_column_definitions()`, `apply_source_type_casts()`, `ingest_raw_files()` |
| `helper_functions_3.py` | Orchestration, configuration, Spark session setup | `parse_json_metadata()`, `parse_source_configuration()`, `parse_spark_configuration()` |
| `batch_processing.py` | Main processing engine — calls all helper functions | Entry point; orchestrates entire batch flow |

### batch_processing.py — Orchestration Only

`batch_processing.py` is a **pure orchestration module**. It must contain **ONLY** helper-function calls, variable assignments from their return values, and control flow (`if`/`try`/`for`) that decides *which* helpers to call. **All code logic — including data transformations, JSON serialization, string formatting, conditional computations, and any other business/processing logic — MUST live in helper functions** (`helper_functions_1/2/3.py`).

If you need to add new behavior that is invoked from `batch_processing.py`, implement it as a helper function and call it from the orchestration module.

---

## Utility Modules

| Module | Purpose |
|--------|---------|  
| `runtime.py` | Databricks-native path/table abstraction layer (UC tables vs volume paths) |

---

## Key Relationships (Impact Matrix)

| If You Change... | You May Also Need to Change... |
|------------------|-------------------------------|
| **Metadata Table schema** | Stored procedures that read/write it, modules that consume the data |
| **Stored Procedure output** | Jobs that call it, modules that use the results |
| **Job parameters** | Stored procedures providing the params, modules receiving them |
| **Helper functions** | Other modules that call them, tests that validate them |
| **Config attributes** | Metadata reference docs, validation script, all consuming code |

---

## 🚫 FORBIDDEN

| ❌ Never Do This | Why |
|------------------|-----|
| Put code logic in `batch_processing.py` | It is a pure orchestration module — all logic belongs in helper functions |
| Put top-level imports in custom function files (`custom_functions/*.py`) | Custom functions execute via `run_cell()` in the shared namespace — top-level imports pollute `globals()` and can shadow critical symbols (e.g., the `f` alias for `pyspark.sql.functions`). Move all imports inside the function body. CI/CD validation enforces this. |
| Use `from X import *` in custom function files | Wildcard imports dump arbitrary names into the shared namespace — especially dangerous at scale with hundreds of custom functions |
| Make module changes without loading checklist | Will miss required sync steps |
| Add config attributes without updating canonical metadata reference | Validation and docs will be wrong |
| Change function signatures without updating docs | Breaks examples and user understanding |
| Deliver changes without listing follow-up items | User won't know what else needs updating |
| Skip unit test updates | Regressions will go undetected |

---

## Documentation Lookup

Grep across `docs/` for the user's keywords. Module-specific search patterns:

| Topic | File | Search Pattern |
|-------|------|----------------|
| Core Components | `docs/CORE_COMPONENTS_REFERENCE.md` | `## Notebooks for Data Movement` |
| Custom Transformation Functions | `docs/DATA_TRANSFORMATION_GUIDE.md` | `## Using a custom function as a data transformation step` |
| Custom Function Selection | `docs/CUSTOM_FUNCTION_SELECTION.md` | `## Canonical Selection Rules` |

---

## 🔴 MANDATORY: After Making Changes to Core Modules

You MUST complete ALL steps before delivering to the user:

### Step 1: Refresh Published Examples
Update documentation that references the changed functionality:
- `docs/DATA_INGESTION_GUIDE.md` — ingestion walkthroughs
- `docs/DATA_TRANSFORMATION_GUIDE.md` — transformation walkthroughs
- `docs/DATA_MODELING_GUIDE.md` — dimension/fact modeling
- `docs/CORE_COMPONENTS_REFERENCE.md` — component details
- `docs/MONITORING_AND_LOGGING.md` — logging and reports
- `docs/METADATA_GENERATION_GUIDE.md` — core rules, end-to-end examples
- `docs/LEGACY_CODE_CONVERSION_GUIDE.md` — legacy code conversion workflow
- `docs/TRANSFORMATION_PATTERNS_REFERENCE.md` — transformation SQL INSERT patterns
- `docs/METADATA_SQL_FILE_FORMAT.md` — SQL file format and error prevention

### Step 2: Sync Metadata Reference
If you added/changed/removed configuration options:
- Update the canonical contract in `docs/METADATA_REFERENCE.md`
- Update helper function documentation if signatures changed

### Step 3: Update Validation Logic
If you added new metadata parameters:
- Update the metadata validation logic so `.github/skills/metadata-validation/validate_metadata_sql.py` accepts and validates the new configuration attributes

### Step 4: Re-align Unit Tests
- Add/update tests in `integration_tests/` for new behavior
- Remove/rewrite assertions that relied on old implementation
- Run tests to confirm no regressions

### Verification Checklist

Before completing the request, confirm:

- [ ] Documentation examples are updated (if applicable)
- [ ] Metadata reference reflects any new or changed attributes
- [ ] Metadata validation accepts new configurations (if applicable)
- [ ] Unit tests added/updated for changed behavior
- [ ] User is informed of any manual follow-up needed
