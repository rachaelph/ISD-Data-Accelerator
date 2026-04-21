# Metadata Generation Guide

> Last updated: 2026-03-29

Use this file when the question is about how to generate, author, or structure metadata SQL records for the accelerator.

## Table of Contents

- [Purpose](#purpose)
- [Golden Rules for LLM Metadata Generation](#golden-rules-for-llm-metadata-generation)
- [🔴 Transformation Hierarchy (CRITICAL - MUST FOLLOW)](#-transformation-hierarchy-critical---must-follow)
  - [Tier Escalation Rules](#tier-escalation-rules)
  - [Quick Decision Checklist](#quick-decision-checklist)
- [⚠️ CRITICAL: Use Only Valid Attributes from the Metadata Reference](#️-critical-use-only-valid-attributes-from-the-metadata-reference)
  - [Rules for Attribute Usage](#rules-for-attribute-usage)
  - [Before Generating Metadata](#before-generating-metadata)
  - [Common Mistakes to Avoid](#common-mistakes-to-avoid)
- [Overview](#overview)
- [Core Principles for LLM Metadata Generation](#core-principles-for-llm-metadata-generation)
  - [1. Always Start with Table_ID](#1-always-start-with-table_id)
  - [2. Follow the Pattern-Based Approach](#2-follow-the-pattern-based-approach)
  - [3. Always Generate Complete SQL](#3-always-generate-complete-sql)
  - [4. Validate Against Rules](#4-validate-against-rules)
- [Metadata Table Reference](#metadata-table-reference)
  - [Quick Reference: Which Source Configuration?](#quick-reference-which-source-configuration)
  - [System Columns Added Automatically](#system-columns-added-automatically)
  - [Watermark Column Name Rule](#watermark-column-name-rule)
  - [Custom Function Notebooks](#custom-function-notebooks)
- **[TRANSFORMATION_PATTERNS_REFERENCE.md](TRANSFORMATION_PATTERNS_REFERENCE.md)** — all 34 transformation SQL INSERT examples
- **[LEGACY_CODE_CONVERSION_GUIDE.md](LEGACY_CODE_CONVERSION_GUIDE.md)** — decomposition, medallion mapping, operation-to-transformation table, verification
- [Complete End-to-End Examples](#complete-end-to-end-examples)
- [Validation Rules Checklist](#validation-rules-checklist)
- [Tips for LLM Success](#tips-for-llm-success)
- **[METADATA_SQL_FILE_FORMAT.md](METADATA_SQL_FILE_FORMAT.md)** — SQL file structure, naming, formatting, and error prevention
- [Quick Reference: Configuration Decision Tree](#quick-reference-configuration-decision-tree)
- [Example User Interactions](#example-user-interactions)
- [Writing Effective Prompts for Metadata Generation](#writing-effective-prompts-for-metadata-generation)
- [Summary](#summary)
- [Reference: Canonical Metadata Reference](#reference-canonical-metadata-reference)

---

## Purpose
This guide enables GitHub Copilot, agents, and other LLM-driven workflows to generate accurate, complete, and compliant metadata table records for the Databricks Data Platform Solution Accelerator based on user requirements. The metadata drives a metadata-driven data ingestion and processing framework using Databricks's medallion architecture (Bronze, Silver, Gold layers).

## Golden Rules for LLM Metadata Generation

**Keywords:** golden rules, Table_ID, INSERT statement, naming convention, accepted values, merge_type, catalog names, SQL comments, column reference, watermark column, metadata generation rules
1.  **Always Start with `Table_ID`**: Every data movement requires a unique, positive integer `Table_ID` that links all three metadata tables. **Maintain at least 100 gap between layers** (e.g., Bronze: 1001-1006, Silver: 1101-1102, Gold: 1201-1202) to allow room for future tables without renumbering.
2.  **Generate Complete and Executable SQL**: Provide full `INSERT` statements with no placeholders. Use correct data types (numbers for numeric IDs, strings for text).
3.  **Use Exact Accepted Values**: Configuration values must be an exact match from the lists provided in this guide (e.g., `merge_type` must be `'merge'`, not `'upsert'`).
4.  **Follow the Naming Conventions**: Use three-part names for Delta tables (`datastore.schema.table`) and specified paths for files.
5.  **Use Correct catalog Names**: The values `'bronze'`, `'silver'`, `'gold'` are logical layer names. Actual catalog names vary by deployment (e.g., `'raw_data'`, `'curated'`, `'reporting'`). If the user mentions a specific catalog name, use it. **Note:** The metadata validator (Rule 50) automatically checks that all datastore names have corresponding entries in the `Datastore_Configuration` table - any mismatches will be caught during validation.
6.  **Add Explanations**: Include SQL comments (`--`) on their own line immediately above or below the statement to explain the purpose of each configuration value. **IMPORTANT: Use only single-line comments (`--`). Do NOT use multi-line comments (`/* */`) in the output file.**
7.  **⚠️ CRITICAL - Reference Column Names Based on Context**: Column name references must match the context where they are used:
    - **`watermark_details.column_name`**: Use the **ORIGINAL source column name EXACTLY as it appears in the source system** (e.g., `ModifiedDate` for SQL Server, `LAST_MODIFIED` for Oracle). **DO NOT use transformed names here.** Watermark filtering happens during extraction, BEFORE column name transformations are applied. Using a transformed name will cause query failures because the source system doesn't have that column name.
    - **Orchestration `Primary_Keys`**: Use **post-cleansing column names** after column name transformations are applied (e.g., `customer_id` not `CustomerID`). Primary keys are used for merge operations on the target table after cleansing.
    - **Advanced configuration references** (in `data_transformation_steps`, `data_quality` checks): Use **post-cleansing column names** after column name transformations are applied. Transformations and quality checks run after cleansing.
    - **Example**: If SQL Server source column `ModifiedDate` is transformed to `modified_date`, use `ModifiedDate` in `watermark_details.column_name` and in the source query WHERE clause, but use `modified_date` in `Primary_Keys` and transformation steps.
8.  **Prioritize Built-in Transformations**: Always prefer using a specific, built-in transformation pattern (like `add_row_hash`) over a generic `derived_column` expression if a specific pattern exists for the task. Use `derived_column` only for custom logic that is not covered by other patterns.
9.  **🔴 CRITICAL - NEVER Default to Custom Functions**: Before recommending any custom function, you **MUST** verify that the user's requirements **CANNOT** be met using built-in transformations. **Custom functions should be the LAST RESORT, not the first choice.**

    **Canonical selection rules:** See [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md). Treat that file as the single source of truth for choosing between custom function types, reviewing selection criteria, and understanding function signatures.
10.  **Use `SELECT *` for Database Queries**: When reading from source databases, always use `SELECT *` unless the user explicitly specifies particular columns to extract.
11.  **Validate Before Outputting**: Ensure no conflicting configurations are present (e.g., do not use a watermark with `merge_type = 'overwrite'`).
12.  **File Location Discovery & File Output**: Before creating files, use the `file_search` tool to search for the `**/metadata/*.sql**` pattern in the workspace. If not found, create the metadata folder at `<workspace_folder>/metadata/`. Organize files by trigger name (e.g., `metadata_SalesDataProduct.sql`). **⚠️ CRITICAL: Each trigger gets its own `.sql` folder in the `metadata/` directory.** Always use `create_file` to write the metadata SQL to this location. Never output SQL only in chat – the `.sql` file in the repo is the required deliverable.
13. **DELETE Statement Patterns**: All DELETE statements must use `WHERE Trigger_Name = '...'` with a subquery. **NEVER** hardcode Table_ID values in DELETE statements. See [METADATA_SQL_FILE_FORMAT.md - DELETE Statement Pattern](METADATA_SQL_FILE_FORMAT.md#file-structure) for the exact pattern and examples.
14. **INSERT Statement Grouping and Formatting**: Group all configurations for each table together in a single INSERT per metadata table type. Format each row on a single line. Maximum 1000 records per INSERT. See [METADATA_SQL_FILE_FORMAT.md - INSERT Statement Formatting Rules](METADATA_SQL_FILE_FORMAT.md#insert-statement-formatting-rules) for detailed rules and examples.
15. **Column Name Transformation**: Apply `column_cleansing` only when the processing flow writes to a Delta table (`batch` or `pipeline_stage_and_batch`). `pipeline_stage_only`-only flows land files and do not apply column name transformations. For eligible flows, include `column_cleansing` configs (`trim`, `apply_case`, `replace_non_alphanumeric_with_underscore`) only when the user explicitly requests column name standardization; **no automatic defaults are applied**.
16. **Do NOT duplicate system timestamps**: The notebooks automatically add `delta__created_datetime` and `delta__modified_datetime` for every Delta table write. Do not create derived columns (e.g., `created_on`, `modified_on`) that simply copy these values—the duplication bloats schemas and causes confusion about the canonical lineage columns. Only add new timestamps when the business logic genuinely differs from the built-in lineage semantics.
17. **Semicolon Placement**: Each INSERT statement should end with a semicolon (`;`) on the **last VALUES row only**. All other VALUES rows end with commas.
18. **🔴 CRITICAL - Category Grouping in Advanced Configuration**: Group by Table_ID first, then by category within each Table_ID. For each Table_ID, ALL `data_transformation_steps` entries MUST come BEFORE ALL `data_quality` entries. See [METADATA_SQL_FILE_FORMAT.md - Group Configuration Categories Together](METADATA_SQL_FILE_FORMAT.md#file-structure) for the canonical formatting rules and examples.
19. **🔴 CRITICAL - Use Comma-Separated Column Names Instead of Duplicating Instances**: Many transformation and data quality configurations accept **comma-separated column names** in the `column_name` attribute. When applying the same transformation to multiple columns, use ONE instance with comma-separated column names instead of creating duplicate instances.

    **Transformations that support comma-separated `column_name`:**
    - `mask_sensitive_data` - Mask multiple columns with identical settings
    - `columns_to_rename` - Rename multiple columns (parallel lists in `existing_column_name` and `new_column_name`)
    - `remove_columns` - Remove multiple columns at once
    - `select_columns` - Select multiple columns to keep
    - `change_data_types` - Change type for multiple columns to the same target type
    - `apply_null_handling` - Apply same null handling to multiple columns
    - `replace_values` - Apply same value replacement to multiple columns
    - `add_row_hash` - Include multiple columns in hash calculation
    - `drop_duplicates` - Deduplicate on multiple columns
    - `aggregate_data` - Multiple `column_name` values with matching `aggregation` values

    **Example - WRONG (duplicating instances):**
    ```sql
    -- ❌ Creates unnecessary duplication - 10 rows instead of 5
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'column_name', 'email'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'upper_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'lower_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'digit_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'other_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 8, 'column_name', 'phone'),  -- ❌ Duplicate instance!
    (101, 'data_transformation_steps', 'mask_sensitive_data', 8, 'upper_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 8, 'lower_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 8, 'digit_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 8, 'other_char', 'X')
    ```

    **Example - CORRECT (comma-separated columns):**
    ```sql
    -- ✅ Single instance handles both columns - 5 rows total
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'column_name', 'email,phone'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'upper_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'lower_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'digit_char', 'X'),
    (101, 'data_transformation_steps', 'mask_sensitive_data', 7, 'other_char', 'X')
    ```

    **When to use separate instances:** Only create separate instances when columns need **different** transformation settings (e.g., different masking patterns for SSN vs credit card).

20. **🔴 CRITICAL - Delta-to-Delta Transformations**: When reading from existing Delta tables (Bronze → Silver → Gold):
    - ❌ **DO NOT** use the `source` attribute (e.g., `'delta'` is NOT a valid value - valid values are `'oracle'`, `'sql_server'`, `'azure_sql'`, `'sftp'`, etc.)
    - ❌ **DO NOT** use `query`, `schema_name`, `staging_volume_name`, or `staging_folder_path`
    - ✅ **ONLY** use `table_name` with 3-part naming: `'silver.dbo.orders'`
    - ✅ Use `Processing_Method = 'batch'` in Orchestration (NOT `'pipeline_stage_and_batch'` or `'pipeline_stage_only'`)

    The `source` attribute is for external source ingestion routed through `pipeline_stage_and_batch` or `pipeline_stage_only`. Notebook-owned custom staging uses `Processing_Method='batch'` with `custom_staging_function` metadata instead of a `source` value. Delta tables don't need external connections.

    **Example - WRONG:**
    ```sql
    (101, 'source_details', 'source', 'delta'),  -- ❌ 'delta' is NOT a valid source value!
    (101, 'source_details', 'datastore', 'silver'),  -- ❌ Invalid attribute
    (101, 'source_details', 'source_entity', 'dbo.orders')  -- ❌ Invalid attribute
    ```

    **Example - CORRECT:**
    ```sql
    (101, 'source_details', 'table_name', 'silver.dbo.orders')  -- ✅ Only table_name needed
    ```

21. **🔴 CRITICAL - Self-Verification for Large Metadata Files**: When generating metadata with **5+ Table_IDs**, you MUST perform a conceptual self-verification check BEFORE running the validator:

    **Re-Read Your Thought Process:**
    - Go back to your initial analysis/plan - did you implement EVERYTHING you said you would?
    - If you listed "I'll create Table_IDs for X, Y, Z" - verify X, Y, and Z are ALL present
    - If you noted "this needs a join" or "this needs cleansing" - verify you actually added those transformations

    **Cross-Reference Against Source:**
    - **From XML/documentation**: Did you include ALL entities/tables mentioned? Count them in the source, count them in your output
    - **From user request**: Did you address every part of what the user asked for?
    - **From existing patterns**: If converting from another format, did anything get dropped?

    **Quick Sanity Check:**
    ```
    Source had: [list what you saw]
    I created:  [list what you generated]
    Missing:    [anything?]
    ```

    **If you find gaps**: Fix them silently before delivering - don't tell the user "I forgot X"

---

## 🔴 Transformation Hierarchy (CRITICAL - MUST FOLLOW)

> **Canonical reference:** See [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) for the full 3-tier hierarchy, escalation rules, and decision checklist. That file is the single source of truth.

> **⚠️ MANDATORY:** Always exhaust Tier 1 built-in transformations before suggesting Tier 2 custom notebooks. Exhaust Tier 2 before suggesting Tier 3 standalone execution. See [Golden Rule #8](#golden-rules-for-llm-metadata-generation) for the complete checklist of built-in transformations to review before escalating.

---

## ⚠️ CRITICAL: Use Only Valid Attributes from the Metadata Reference

> **Canonical metadata reference entry point:** Use [METADATA_REFERENCE.md](METADATA_REFERENCE.md) first. That file now owns the metadata reference used for authoring and validation.

**DO NOT invent, guess, or extrapolate configuration names or attribute names that are not explicitly documented.**

The complete and authoritative list of all valid configuration categories, configuration names, configuration attribute names, and accepted values is defined in [METADATA_REFERENCE.md](METADATA_REFERENCE.md). Use the split workflow guides (e.g., `DATA_INGESTION_GUIDE.md`, `DATA_TRANSFORMATION_GUIDE.md`) for walkthroughs and scenarios, not as the authoritative contract.

- **Configuration Categories** (e.g., `source_details`, `target_details`, `watermark_details`, `data_transformation_steps`, `data_quality`)
- **Configuration Names** (e.g., `table_name`, `merge_type`, `derived_column`, `validate_condition`)
- **Configuration Attribute Names** (e.g., `column_name`, `expression`, `condition`, `if_not_compliant`)
- **Accepted Values** for each configuration (e.g., `merge_type` accepts: `overwrite`, `append`, `merge`, `merge_and_delete`, `merge_mark_unmatched_deleted`, `merge_mark_all_deleted`, `replace_where`, `warehouse_spark_connector`, `scd2`, `output_file`)

### Rules for Attribute Usage:
1. ✅ **ONLY use configuration names and attributes that appear in the Metadata Reference**
2. ❌ **NEVER create variations** (e.g., don't use `query_expression` when the correct name is `expression`)
3. ❌ **NEVER assume attributes exist** based on similar patterns (e.g., don't assume `min_value` exists just because you see `validate_batch_size`)
4. ✅ **Use exact spelling, casing, and underscores** as shown in the dictionary (e.g., `merge_type` not `mergeType` or `merge-type`)
5. ✅ **Check the "Accepted Values" column** to ensure your values are valid
6. ✅ **Verify that configuration names are valid for their category** (e.g., `merge_type` belongs to `target_details`, not `source_details`)

### Before Generating Metadata:
1. Identify which configuration category is needed (e.g., `source_details`, `data_transformation_steps`)
2. Look up the exact configuration name in the Metadata Reference
3. Verify all required attributes for that configuration name
4. Use only the accepted values listed in the dictionary
5. If a configuration or attribute you think you need doesn't exist in the dictionary, **do not use it** - ask the user for clarification or use an alternative approach with valid configurations
6. **CRITICAL: Always verify attributes in the Metadata Reference.** For example, `mask_sensitive_data` does NOT have a `masking_type` attribute - it uses `upper_char`, `lower_char`, `digit_char`, and `other_char` for character replacement.

### Common Mistakes to Avoid:
❌ Using `source_query` instead of `query` in `source_details`
❌ Using `target_schema` when you should use schema as part of `Target_Entity`
❌ Creating custom attributes like `filter_expression` when `condition` is the valid attribute name
❌ Using `upsert` as a `merge_type` value when only `merge` is valid
❌ Inventing new transformation types that don't exist in `data_transformation_steps`
❌ Using `masking_type` for `mask_sensitive_data` - this is NOT a valid attribute. Use `upper_char`, `lower_char`, `digit_char`, `other_char` instead

**When in doubt, refer back to [METADATA_REFERENCE.md](METADATA_REFERENCE.md). If it's not there, it's not valid.**

## Overview

The Databricks Data Platform uses three core metadata tables to configure data pipelines:

1. **Orchestration Metadata Table** (`Data_Pipeline_Metadata_Orchestration`)
   - Defines **what** tables/entities to process, in **what order**, and **where** to write them

2. **Primary Configuration Table** (`Data_Pipeline_Metadata_Primary_Configuration`)
   - Contains core configurations for each Table_ID: source details, target details, watermark settings, performance settings

3. **Advanced Configuration Table** (`Data_Pipeline_Metadata_Advanced_Configuration`)
   - Stores complex multi-row configurations for transformations, data quality checks, and surrogate key logic

## Core Principles for LLM Metadata Generation

### 1. Always Start with Table_ID
- Every data movement requires a **unique Table_ID** (positive integer)
- One Table_ID = one target table/file destination
- Table_ID links all three metadata tables together

#### Table_ID Numbering Convention (REQUIRED)
When assigning Table_IDs across medallion layers, **maintain at least 100 gap between layers** to allow room for future tables:

| Layer | Table_ID Range | Example |
|-------|---------------|---------|
| Bronze | 1-99 or 1001-1099 | Start at 1 or 1001 |
| Silver | 101-199 or 1101-1199 | Start at least 100 after max Bronze ID |
| Gold | 201-299 or 1201-1299 | Start at least 100 after max Silver ID |

**Why?** This convention:
- Allows adding new Bronze tables (up to 99 more) without renumbering Silver/Gold
- Allows adding new Silver tables without renumbering Gold
- Makes it visually clear which layer a Table_ID belongs to
- Is enforced by the metadata validator

**Example for a trigger with 6 Bronze, 2 Silver, 2 Gold tables:**
```sql
-- Bronze Layer: 1001-1006
('MyTrigger', 1, 1001, 'bronze', 'dbo.raw_table_1', ...),
('MyTrigger', 2, 1002, 'bronze', 'dbo.raw_table_2', ...),
...
('MyTrigger', 6, 1006, 'bronze', 'dbo.raw_table_6', ...),
-- Silver Layer: 1101-1102 (gap of 95+ from Bronze max)
('MyTrigger', 7, 1101, 'silver', 'dbo.cleaned_table_1', ...),
('MyTrigger', 8, 1102, 'silver', 'dbo.cleaned_table_2', ...),
-- Gold Layer: 1201-1202 (gap of 99+ from Silver max)
('MyTrigger', 9, 1201, 'gold', 'dbo.fact_table_1', ...),
('MyTrigger', 10, 1202, 'gold', 'dbo.fact_table_2', ...)
```

### 2. Follow the Pattern-Based Approach
User requests will generally fall into these categories:
- **Data Ingestion from Databases** (Oracle, SQL Server, PostgreSQL, MySQL, DB2, Azure SQL)
- **Data Ingestion from Files** (CSV, JSON, XML, Excel, Parquet from UC Volumes, including shortcuts)
- **Data Transformation** (Bronze → Silver or Silver → Gold with transformations)
- **Custom Processing** (using custom Python functions)

### 3. Always Generate Complete SQL
- Generate INSERT statements that are ready to execute
- Include **inline comments** explaining each configuration value
- Use actual values, not placeholders (except for user-specific values like GUIDs)
- Include proper SQL formatting
- **CRITICAL**: Create metadata SQL files in the `metadata/` folder as `.sql` folders — see [METADATA_SQL_FILE_FORMAT.md](METADATA_SQL_FILE_FORMAT.md) for exact structure
- **REQUIRED**: Organize by trigger name (one file per trigger)
- **REQUIRED**: Include DELETE statements at the top for the trigger
- **REQUIRED**: Group multiple Table_IDs in same INSERT statements (max 1000 records)
- **REQUIRED**: Format each row on a single line for CI/CD compatibility

### 4. Validate Against Rules
Before generating, mentally validate:
- Are all configuration categories, names, and attributes valid per [METADATA_REFERENCE.md](METADATA_REFERENCE.md)?
- Is this configuration allowed for this source type?
- Are all required fields present?
- Do the values conform to the accepted values list from [METADATA_REFERENCE.md](METADATA_REFERENCE.md)?
- Are there conflicting configurations?
- Have I avoided inventing any custom attributes not in [METADATA_REFERENCE.md](METADATA_REFERENCE.md)?

### 5. Always Search for the Metadata Folder First
Before creating any metadata SQL files:
- **First**: Use `file_search` to search for `**/metadata/*.sql**` pattern in the workspace
- **If found**: Create files in that `metadata/` folder following the `.sql` structure
- **If not found**: Create the `metadata/` folder in the workspace git folder, or ask the user for the correct location
- **Never assume a default path** - Always search first
- **Creating files in wrong location disrupts CI/CD workflows**
- See [METADATA_SQL_FILE_FORMAT.md](METADATA_SQL_FILE_FORMAT.md) for the complete file and folder structure

---


---

## Metadata Table Reference

**Keywords:** metadata tables, orchestration table, primary configuration, advanced configuration, source configuration, Table_ID, configuration category, system columns, watermark column, custom function notebooks

> **All valid configuration categories, names, attributes, and accepted values are defined in [METADATA_REFERENCE.md](METADATA_REFERENCE.md).** That file is the single source of truth for the metadata contract. Do not invent, guess, or extrapolate configuration names or attribute values — if it is not in METADATA_REFERENCE.md, it is not valid.

### Quick Reference: Which Source Configuration?

```
Is the source an EXTERNAL database (Oracle, SQL Server, Azure SQL, etc.)?
├── YES → Do you want to stage AND write to a Delta table?
│   ├── YES → Use `source`, `datastore_name`, `query`, `staging_*` attributes
│   │         Processing_Method = 'pipeline_stage_and_batch'
│   └── NO (stage files only) → Same source_details, Processing_Method = 'pipeline_stage_only'
│
└── NO → Is it an SFTP server?
    ├── YES → Use `source='sftp'`, `datastore_name`, `sftp_wildcard_folder_path`, `staging_*`
    │         Processing_Method = 'pipeline_stage_and_batch' or 'pipeline_stage_only'
    │         Incremental loading is AUTOMATIC (no watermark_details needed)
    │
    └── NO → Is it a FILE (CSV, JSON, Parquet, Excel)?
        ├── YES → Use `wildcard_folder_path`, `datastore_name`
        │         Processing_Method = 'batch'
        │
        └── NO → It's a DELTA TABLE (Bronze/Silver/Gold layer)
                  Use ONLY `table_name` attribute (3-part naming: `datastore.schema.table`)
                  Processing_Method = 'batch'
                  ❌ DO NOT use `source`, `query`, `schema_name`, `staging_*`
```

| Source Type | `source` Attribute? | Processing_Method |
|-------------|---------------------|-------------------|
| External DB (Oracle, SQL Server, etc.) | ✅ Required | `pipeline_stage_and_batch` or `pipeline_stage_only` |
| SFTP Server | ✅ Required (`'sftp'`) | `pipeline_stage_and_batch` or `pipeline_stage_only` |
| Custom Staging Function | ❌ No | `batch` |
| Files (CSV, JSON, Parquet, Excel) | ❌ No | `batch` |
| Delta Tables (Bronze→Silver→Gold) | ❌ No (`'delta'` is invalid!) | `batch` |

### System Columns Added Automatically

The batch notebook automatically stamps these columns on every Delta table write — do not create derived columns that duplicate them:

- `delta__created_datetime` — set on first write, carried forward on merges
- `delta__modified_datetime` — stamped on every write
- `delta__raw_folderpath` — populated for file-based ingestions
- `delta__schema_id` — schema hash for drift investigations

These columns (except `delta__schema_id`) are added **before** `data_transformation_steps` run. Transformations that reshape the projection (`aggregate_data`, `select_columns`, `union_data`) can accidentally drop them. If that happens, re-add `delta__modified_datetime` and `delta__created_datetime` via trailing `derived_column` steps.

### Watermark Column Name Rule

`watermark_details.column_name` must use the **original source column name** (pre-transformation). Watermark filtering happens during extraction, BEFORE column name cleansing. Primary keys and advanced config references use post-cleansing names.

### Custom Function Notebooks

When metadata references a `custom_transformation_function`, `custom_table_ingestion_function`, `custom_source_function`, `custom_staging_function`, or `custom_file_ingestion_function`, you **must create the actual notebook file** — not just the metadata. See [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for choosing the right function type and [Extending_the_Accelerator.md](Extending_the_Accelerator.md) for notebook templates.

For detailed walkthroughs with SQL examples, see:
- [DATA_INGESTION_GUIDE.md](DATA_INGESTION_GUIDE.md) for ingestion scenarios
- [DATA_TRANSFORMATION_GUIDE.md](DATA_TRANSFORMATION_GUIDE.md) for transformation scenarios
- [DATA_MODELING_GUIDE.md](DATA_MODELING_GUIDE.md) for dimension and fact table patterns
- [Complete End-to-End Examples](#complete-end-to-end-examples) below

> **📋 Transformation SQL patterns (all 34 types with INSERT examples) have been moved to [TRANSFORMATION_PATTERNS_REFERENCE.md](TRANSFORMATION_PATTERNS_REFERENCE.md).**

> **📋 Legacy code conversion workflow (decomposition, medallion layer assignment, operation mapping, verification) has been moved to [LEGACY_CODE_CONVERSION_GUIDE.md](LEGACY_CODE_CONVERSION_GUIDE.md).**

---

## Complete End-to-End Examples

> **Purpose of these examples:** The patterns below show what *correctly generated metadata output* looks like — use them as reference targets when generating or reviewing metadata. For scenario-based walkthroughs that explain *when and why* to use each pattern, see the workflow guides listed above.

**Keywords:** full example, end-to-end, Oracle Bronze, CSV Silver, transformation example, fact table, dimension table, SCD2, CDF, Change Data Feed, sample metadata, pattern

### Pattern 1: Oracle Database → Bronze (Incremental with Watermark)
```sql
-- Orchestration
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES ('OracleSales', 1, 101, 'bronze', 'dbo.sales', 'sale_id', 'pipeline_stage_and_batch', 1);
-- Primary Config with watermark
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (101, 'source_details', 'source', 'oracle'),
    (101, 'source_details', 'datastore_name', 'oracle_sales'),
    (101, 'source_details', 'schema_name', 'SALES_SCHEMA'),
    (101, 'source_details', 'table_name', 'SALES'),
    -- Query with {WATERMARKVALUE} placeholder for incremental loading
    (101, 'source_details', 'query', 'SELECT * FROM SALES_SCHEMA.SALES WHERE MODIFIED_DATE > TIMESTAMP ''{WATERMARKVALUE}'' '),
    (101, 'source_details', 'staging_volume_name', 'bronze'),
    (101, 'source_details', 'staging_folder_path', 'oracle/sales/'),
    -- Watermark configuration
    (101, 'watermark_details', 'column_name', 'MODIFIED_DATE'),
    (101, 'watermark_details', 'data_type', 'datetime');
```

### Pattern 2: CSV Files → Silver (with cleansing)
```sql
-- Orchestration
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES ('CSVSales', 1, 102, 'silver', 'dbo.sales', 'sale_id', 'batch', 1);
-- Primary Config
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Source: CSV files from UC Volumes (can be uploaded or via shortcut)
    (102, 'source_details', 'wildcard_folder_path', 'sales/data/*.csv'),
    (102, 'source_details', 'datastore_name', 'bronze'),
    (102, 'source_details', 'file_has_header_row', 'true'),
    (102, 'source_details', 'delimiter', ','),
    (102, 'source_details', 'on_bad_records', 'quarantine'),
    -- Column name cleansing (Silver layer defaults)
    (102, 'column_cleansing', 'trim', 'true'),
    (102, 'column_cleansing', 'apply_case', 'lower'),
    (102, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true'),
    -- Data value cleansing (use '*' for all string columns or comma-separated column names)
    (102, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
    (102, 'data_cleansing', 'trim_data_in_string_columns', '*');
```

### Pattern 3: Bronze → Silver (with transformations and DQ)
```sql
-- Orchestration
INSERT INTO Data_Pipeline_Metadata_Orchestration
([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('SalesTransform', 1, 103, 'silver', 'dbo.sales_clean', 'sale_id', 'batch', 1);

-- Primary Config
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Source: Bronze delta table
    (103, 'source_details', 'table_name', 'bronze.dbo.sales'),
    -- Target: Merge strategy
    (103, 'target_details', 'merge_type', 'merge');

-- Advanced Config: Transformations AND Data Quality (all in one INSERT per Table_ID)
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- TRANSFORMATIONS
    -- Step 1: Rename columns
    (103, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'cust_id,prod_id'),
    (103, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_id,product_id'),
    -- Step 2: Add calculated column
    (103, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'total_amount'),
    (103, 'data_transformation_steps', 'derived_column', 2, 'expression', 'quantity * unit_price'),
    -- Step 3: Filter invalid data
    (103, 'data_transformation_steps', 'filter_data', 3, 'filter_logic', 'quantity > 0 AND unit_price > 0'),
    -- DATA QUALITY CHECKS
    -- DQ Rule 1: Validate amounts
    (103, 'data_quality', 'validate_condition', 1, 'condition', 'total_amount > 0'),
    (103, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
    -- DQ Rule 2: Validate email format
    (103, 'data_quality', 'validate_pattern', 2, 'column_name', 'customer_email'),
    (103, 'data_quality', 'validate_pattern', 2, 'pattern_type', 'email'),
    (103, 'data_quality', 'validate_pattern', 2, 'allow_null', 'true'),
    (103, 'data_quality', 'validate_pattern', 2, 'if_not_compliant', 'quarantine'),
    -- DQ Rule 3: Batch size check
    (103, 'data_quality', 'validate_batch_size', 3, 'min_rows', '10'),
    (103, 'data_quality', 'validate_batch_size', 3, 'if_not_compliant', 'warn');
```

### Pattern 4: Silver → Gold (Fact Tables & Snowflake Dimensions with Surrogate Keys)
```sql
-- Orchestration
INSERT INTO Data_Pipeline_Metadata_Orchestration
([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('SalesFact', 1, 104, 'gold', 'dbo.fact_sales', 'sale_id', 'batch', 1);

-- Primary Config
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (104, 'source_details', 'table_name', 'silver.dbo.sales_clean'),
    (104, 'target_details', 'merge_type', 'merge'),
    (104, 'target_details', 'liquid_clustering_columns', 'customer_sk,product_sk,date_sk');

-- Advanced Config: Add Surrogate Keys from Dimensions
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- Step 1: Join with customer dimension
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_name', 'gold.dbo.dim_customer'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_join_logic', 'a.customer_id = b.customer_id'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_key_column_name', 'customer_key'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_key_output_column_name', 'customer_sk_fk'),
    -- Step 2: Join with product dimension
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 2, 'dimension_table_name', 'gold.dbo.dim_product'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 2, 'dimension_table_join_logic', 'a.product_id = b.product_id'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 2, 'dimension_table_key_column_name', 'product_key'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 2, 'dimension_key_output_column_name', 'product_sk_fk'),
    -- Step 3: Join with date dimension
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 3, 'dimension_table_name', 'gold.dbo.dim_date'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 3, 'dimension_table_join_logic', 'a.sale_date = b.date'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 3, 'dimension_table_key_column_name', 'date_key'),
    (104, 'data_transformation_steps', 'attach_dimension_surrogate_key', 3, 'dimension_key_output_column_name', 'date_sk_fk');
```

### Pattern 5: Create Dimension Table with SCD Type 2
```sql
-- Orchestration
INSERT INTO Data_Pipeline_Metadata_Orchestration
([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('CustomerDim', 1, 105, 'gold', 'dbo.dim_customer', 'customer_id', 'batch', 1);

-- Primary Config
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    (105, 'source_details', 'table_name', 'silver.dbo.customers'),
    (105, 'target_details', 'merge_type', 'scd2'),
    -- source_timestamp_column_name: The column from your source data containing the timestamp
    -- when the record was created or modified in the source system. This drives SCD2 versioning:
    -- - Sets scd_start_date when a new version is created
    -- - Sets scd_end_date on the previous version when changes are detected
    -- Use the source system's business timestamp (e.g., 'modified_date', 'last_updated')
    -- rather than technical timestamps. Default: 'delta__modified_datetime'
    (105, 'target_details', 'source_timestamp_column_name', 'delta__modified_datetime');

-- Advanced Config: Create Surrogate Key
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (105, 'data_transformation_steps', 'create_surrogate_key', 1, 'type', 'auto_increment'),
    (105, 'data_transformation_steps', 'create_surrogate_key', 1, 'column_name', 'customer_key');
```

### Pattern 6: Delta Tables with Change Data Feed (Incremental with Delete Tracking)
```sql
-- Orchestration
INSERT INTO Data_Pipeline_Metadata_Orchestration
([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
    ('CustomerCDF', 1, 106, 'silver', 'dbo.customers_clean', 'customer_id', 'batch', 1);

-- Primary Config with Change Data Feed
-- Prerequisite: Enable CDF on source with: ALTER TABLE bronze.dbo.customers SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
    -- Source: Bronze delta table with CDF enabled
    (106, 'source_details', 'table_name', 'bronze.dbo.customers'),
    -- Enable Change Data Feed - uses _commit_version for watermarking automatically
    (106, 'watermark_details', 'use_change_data_feed', 'true'),
    -- Target: Merge strategy
    (106, 'target_details', 'merge_type', 'merge_and_delete');

-- Advanced Config: Optional transformations (CDF metadata columns available)
-- Note: CDF adds _change_type, _commit_version (used for watermarking), _commit_timestamp (for observability)
-- Note: The framework automatically filters OUT 'update_preimage' rows (before state)
-- Only 'insert', 'update_postimage', and 'delete' rows are included by default
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration
(Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    -- RECOMMENDED: Drop duplicates on primary keys to handle multiple changes per record
    -- If a record is updated multiple times between runs, keep only the latest change
    (106, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'customer_id'),
    (106, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', '_commit_version');

-- ⚠️ IMPORTANT: DO NOT manually remove CDF metadata columns (_change_type, _commit_version, _commit_timestamp)
-- These columns are automatically removed by the framework during finalization.
-- You SHOULD NOT add a remove_columns step for these columns - the framework handles this automatically.
-- If you manually remove them in your transformation steps, you may break the framework's watermarking and logging logic.

-- CDF Advantages for this pattern:
-- ✅ Captures ALL changes including deletes (impossible with traditional watermarks)
-- ✅ No need to identify/maintain a ModifiedDate or LastUpdated column
-- ✅ Transaction-level guarantees - never miss a change
-- ✅ _commit_version provides precise watermarking with transaction-level tracking
-- ✅ Framework automatically filters out 'update_preimage' (before state) - only latest state kept
-- ✅ CDF metadata columns (_change_type, _commit_version, _commit_timestamp) are automatically removed during finalization

-- Important Considerations:
-- ⚠️ CDF returns ONE ROW per change event - if a record changes 3 times, you get 3 rows
-- ⚠️ Use drop_duplicates with column_name='<primary_keys>' and order_by='_commit_version' to keep only latest change
-- ⚠️ Without deduplication, merge operations may process same primary key multiple times
-- ⚠️ DO NOT manually remove CDF metadata columns - let the framework handle this automatically

-- When to use this pattern:
-- ✓ Source is a Delta table (Bronze/Silver/Gold layer)
-- ✓ Need to track deleted records
-- ✓ Source doesn't have reliable ModifiedDate column
-- ✓ Need complete audit trail with transaction timestamps
```

---

## Validation Rules Checklist

**Keywords:** validation, checklist, validate metadata, pre-deployment check, compliance, required fields, configuration rules, metadata errors

Before generating, validate against these rules:

### Critical: Metadata Reference Compliance
- [ ] **ALL configuration categories are from [METADATA_REFERENCE.md](METADATA_REFERENCE.md)** (e.g., `source_details`, `target_details`, not invented names)
- [ ] **ALL configuration names are from [METADATA_REFERENCE.md](METADATA_REFERENCE.md)** (e.g., `merge_type`, `derived_column`, not invented names)
- [ ] **ALL configuration attribute names are from [METADATA_REFERENCE.md](METADATA_REFERENCE.md)** (e.g., `column_name`, `expression`, not invented names)
- [ ] **ALL accepted values match [METADATA_REFERENCE.md](METADATA_REFERENCE.md) exactly** (e.g., `merge` not `upsert`)
- [ ] **No custom or assumed attributes** that don't appear in [METADATA_REFERENCE.md](METADATA_REFERENCE.md)

### Orchestration Table
- [ ] Table_ID is unique positive integer
- [ ] Target_Datastore matches user's **actual catalog name** (not assumed `bronze`/`silver`/`gold` - these are just examples)
- [ ] Processing_Method is valid (`batch`, `pipeline_stage_and_batch`, `pipeline_stage_only`, `execute_warehouse_sp`, `execute_databricks_notebook`, `execute_databricks_job`)
- [ ] Primary_Keys are provided for `batch` and `pipeline_stage_and_batch` when the target merge pattern requires them (not required for `pipeline_stage_only`, `execute_warehouse_sp`, `execute_databricks_notebook`, or `execute_databricks_job`)
- [ ] Ingestion_Active is 0 or 1

### Primary Configuration
- [ ] Table_ID matches orchestration record
- [ ] Configuration_Category is valid per [METADATA_REFERENCE.md](METADATA_REFERENCE.md)
- [ ] Configuration_Name is valid per [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for that category
- [ ] Required configs present for source type
- [ ] Database name required for azure_sql/sql_server
- [ ] Boolean values are `'true'` or `'false'` (lowercase)
- [ ] GUIDs are valid format
- [ ] Merge types match [METADATA_REFERENCE.md](METADATA_REFERENCE.md) accepted values
- [ ] If watermark: column_name and data_type provided
- [ ] **Database queries use `SELECT *` unless specific columns are requested**

### Advanced Configuration
- [ ] Table_ID matches orchestration record
- [ ] Configuration_Name is from [METADATA_REFERENCE.md](METADATA_REFERENCE.md)
- [ ] Configuration_Attribute_Name is from [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for that Configuration_Name
- [ ] All required attributes present per [METADATA_REFERENCE.md](METADATA_REFERENCE.md)
- [ ] **Configuration_Name_Instance_Number is unique for each (Table_ID, Configuration_Category, Configuration_Name) combination**
- [ ] **Configuration_Name_Instance_Number defines execution order** (1 executes first, then 2, then 3, etc.)
- [ ] Join conditions use proper table aliases (a. and b.)
- [ ] Column lists are comma-separated (no spaces after commas)

### Custom Function Notebooks (ALL Types)
The validator checks **all custom function types** (`custom_transformation_function`, `custom_source_function`, `custom_table_ingestion_function`, `custom_file_ingestion_function`, `custom_staging_function`):

- [ ] **Notebook exists** at `src/{notebook_name}.sqlnotebook-content.py`
- [ ] **Function exists** with the correct name in the notebook
- [ ] **Function signature** matches expected parameters:
  - `custom_transformation_function`: `(new_data, metadata, spark) -> DataFrame`
    - `custom_source_function`: `(metadata, spark) -> DataFrame` or `(DataFrame, next_watermark_value)`
  - `custom_table_ingestion_function`: `(metadata, spark) -> DataFrame`
  - `custom_file_ingestion_function`: `(file_paths, all_metadata, spark) -> DataFrame`
  - `custom_staging_function`: `(metadata, spark) -> dict`
- [ ] **Function returns correctly** (DataFrame for transformation/source/table/file ingestion functions, dict for staging functions)
- [ ] **Metadata key access** uses valid keys (`orchestration_metadata`, `primary_config`, `advanced_config`, `datastore_config`, `function_config`, `watermark_filter`, etc.)
- [ ] **Config attribute references exist in metadata**:
  - For `custom_transformation_function`: `function_config.get('attr')` → attribute must exist in the `custom_transformation_function` instance metadata
  - For table ingestion functions: `primary_config.get('custom_table_ingestion_function_attr')`
   → attribute must exist in `source_details`
  - For file ingestion functions: `primary_config.get('custom_file_ingestion_function_attr')`
   → attribute must exist in `source_details`
  - For staging functions: `primary_config.get('source_details_custom_staging_function_attr')`
   → attribute must exist in `source_details`
- [ ] **No non-standard library imports** unless a custom Databricks compute cluster is configured
- [ ] **All imports inside functions** — no top-level `import` statements in custom notebooks (shared namespace pollution risk). Never use `from X import *`.
- [ ] **SQL workspace references** use backticks with variables: `` `{workspace_name}.dbo.table` ``

---

## Tips for LLM Success

1. **Always Create a New SQL File**
   - Generate a new `.sql` file with a descriptive name based on the use case
   - Examples: `metadata_oracle_sales_ingestion.sql`, `metadata_customer_dimension_scd2.sql`, `metadata_bronze_to_silver_sales.sql`
   - Include a header comment in the file with:
     - Purpose/description of the metadata configuration
     - Date generated
     - Table_ID(s) used
     - Source and target information
     - Any special notes or prerequisites (e.g., "Requires connection GUID to be updated")

2. **Always Ask Clarifying Questions**
   - **"What are the actual names of your Databricks catalogs?"** (e.g., raw_data, curated, analytics - NOT assumed bronze/silver/gold)
   - "Do you need incremental loading or full reload?"
   - "What are the primary keys?"
   - "Do you need any data transformations?"
   - "What data quality rules should be applied?"
   - "Do you need specific columns, or should I extract all columns with SELECT *?"

3. **Provide Complete Solutions**
   - Create a new SQL file with all required configurations
   - Don't generate partial SQL - always include all required configs
   - Include comments explaining each config
   - Add a summary comment at the top of the file
   - Suggest next steps (e.g., "Ensure the external source datastore is registered under `external_datastores` in `databricks_batch_engine/datastores/datastore_<ENV>.json` with the correct `connection_details`, then execute this SQL in your Databricks SQL Warehouse (`/fdp-04-commit` will sync the datastore entry automatically)")
   - **Default to `SELECT *` in database queries unless user specifies columns**
   - **Group all configurations for each Table_ID in ONE INSERT statement** - do not create separate INSERT statements for each configuration category (source_details, watermark_details, target_details, etc.)

4. **Use Realistic Examples**
   - Use actual column names from user's context
   - Use appropriate data types
   - Suggest reasonable default values

5. **Explain Trade-offs**
   - "Using `merge` will upsert based on primary keys"
   - "Using `quarantine` for bad records allows you to review them later"
   - "Enabling SCD2 will track historical changes"

6. **Validate User Input**
   - If user says "SQL Server", confirm they mean `sql_server` (not `azure_sql`)
   - If user provides a table name without schema, ask for clarification
   - If user doesn't mention primary keys, prompt for them

7. **🔴 CRITICAL - Ask for Actual catalog Names**
   - If the user specifies a catalog name, use it in `Target_Datastore`
   - The metadata validator (Rule 50) checks all datastore names against the Datastore_Configuration table and reports any mismatches

8. **Suggest Optimizations**
   - Recommend liquid clustering for large tables
   - Suggest computing statistics on join/filter columns
   - Recommend watermark columns for incremental loads

8. **Handle Edge Cases**
   - Multiple watermark columns: comma-separated (ONLY for Delta tables, not external databases)
   - Databricks compute clusters: spark_environment_id config
   - External locations: external_location path
   - Custom merge strategies: explain differences

> **📋 SQL file format, structure, and error prevention have been moved to [METADATA_SQL_FILE_FORMAT.md](METADATA_SQL_FILE_FORMAT.md).**

---

## Quick Reference: Configuration Decision Tree

```
START: What is the user trying to do?

├─ Ingest from external database?
│  ├─ Full load → Section A (omit watermark_details)
│  ├─ Incremental load → Section A + watermark_details (see End-to-End Pattern 1)
│  └─ With source partitioning → Add partitioning_option
│     Optional: Add copy_pattern_suffix='csv' for CSV-specific copy routing
│     Optional: If staging requires CSV output, add convert_csv_to_parquet='true'
│
├─ Ingest from files (UC Volumes or shortcuts)?
│  ├─ CSV → Section B (see End-to-End Pattern 2)
│  ├─ JSON → Section B, add multiline if needed
│  ├─ Excel → Section B, MUST add sheet_name (single name, comma-separated list, or `*` for all sheets)
│  └─ XML → Section B, add xml_xpath if needed
│
├─ Transform existing Delta tables?
│  ├─ Simple cleansing → Section C with minimal transformations (see End-to-End Pattern 3)
│  ├─ Complex transformations → Section C with full transformations
│  └─ Create fact table or snowflake dimension → See End-to-End Pattern 4
│
└─ Create dimension table?
   ├─ Standard dimension → Basic config
   ├─ With SCD Type 2 → See End-to-End Pattern 5
   └─ With surrogate keys → Add create_surrogate_key
```

---

## Example User Interactions

### Example 1: Simple Request
**User:** "I need to ingest sales data from an Oracle database into Bronze"

**LLM Response:**
I'll help you create the metadata configuration for ingesting Oracle sales data into Bronze. I need a few more details:

1. What is the name of the datastore registered for your Oracle connection? (e.g., `'oracle_sales'` in your `Datastore_Configuration` table)
2. What is the schema and table name in Oracle? (e.g., SCHEMA_NAME.TABLE_NAME)
3. What are the primary key column(s)?
4. Should this be a full load or incremental load? If incremental, what column should be used as the watermark?

[After receiving answers, generate a new SQL file with complete configuration using Pattern 1 or 2]

I've created `metadata_oracle_sales_ingestion.sql` with your configuration. Please:
1. Ensure the external source datastore is registered under `external_datastores` in `databricks_batch_engine/datastores/datastore_<ENV>.json` with the correct `connection_details` for each environment (synced to the Delta table on `/fdp-04-commit`)
2. Verify the table and column names match your Oracle database
3. Execute this SQL notebook in the workspace (open the notebook and click Run All)
4. Run your data pipeline to begin ingestion

### Example 2: Complex Request
**User:** "I need to transform Bronze sales data into Silver, including renaming columns, filtering invalid records, and adding a total amount calculation. Also need to validate that amounts are positive."

**LLM Response:**
I'll create a comprehensive transformation configuration from Bronze to Silver with the transformations and data quality checks you described. Here's what I'll include:

**Transformations:**
1. Column renaming
2. Derived column (total_amount calculation)
3. Data filtering (remove invalid records)

**Data Quality:**
1. Amount validation with quarantine option

I've created `metadata_bronze_to_silver_sales_transform.sql` with:
- All transformations in the correct execution order
- Data quality checks with quarantine for non-compliant records
- Comprehensive comments explaining each step

Please review the file and execute the SQL notebook in the workspace to add this configuration.

---

## Writing Effective Prompts for Metadata Generation

> **Audience:** This section is for *humans* using Copilot to generate metadata. It teaches prompting techniques — the rest of this guide teaches the LLM itself how to produce correct output.

> **Philosophy:** This section teaches *how* to write great prompts, not what every prompt should look like. Learn the pattern, then apply it to any scenario.

### The Anatomy of a Good Prompt

Every effective prompt answers three questions: **Where is the data coming from?** (source type, connection), **What should happen to the data?** (transformations, DQ), **Where should the data go?** (target, merge strategy).

**Minimal viable prompt:**
```
Ingest data from Oracle table HR.EMPLOYEES into Bronze.
Primary key: employee_id
Connection ID: abc123-def456-...
```

Copilot fills in sensible defaults. Add more detail to override them (trigger name, Table_ID, incremental loading, etc.).

### Essential Details Checklist

**For Ingestion:** Source type, source location (table/file path), connection ID or datastore name, primary key(s), incremental details (watermark column + type), file format details (delimiter, header, sheet name), bad record handling.

**For Transformations:** Source table, target table, what to do (rename, filter, join, aggregate, etc.), business logic, DQ rules, merge strategy.

**For Orchestration:** Trigger name, order of operations, parallel execution grouping.

### Common Prompt Patterns

| Pattern | Template |
|---------|----------|
| **Database → Bronze** | `Ingest [TABLE] from [DB_TYPE] into Bronze. PK: [KEY]. Connection: [ID]. [Optional: incremental on COLUMN]` |
| **Files → Bronze** | `Ingest [FORMAT] files from [PATH] in Bronze catalog into a Bronze table. PK: [KEY]. [File details]` |
| **Bronze → Silver** | `Transform [BRONZE_TABLE] to [SILVER_TABLE]. PK: [KEY]. Apply: [renames, filters, DQ checks, etc.]` |
| **Silver → Gold** | `Create [description] from [SILVER_TABLE(s)]. PK: [KEY]. [Joins, aggregations, dimensional modeling]` |
| **End-to-End** | `Build complete pipeline for [DOMAIN]: Bronze: [sources], Silver: [cleansing], Gold: [analytics]. Trigger: "[NAME]"` |
| **Convert Code** | `Convert this [SQL/Python/ADF] code to accelerator metadata: [paste code]` |

### Converting Existing Code

Paste your code and tell Copilot to convert it. Copilot will decompose the code into atomic operations, assign each to the correct medallion layer, map to built-in transformations (or create custom notebooks), verify 100% coverage, and generate validated metadata SQL. Include all dependencies and flag business-critical logic.

Works with: T-SQL, PL/SQL, PL/pgSQL, Python/Pandas, PySpark, ADF/Synapse pipelines, SSIS packages.

### Bad Prompts vs Good Prompts

| Too Vague | Actionable |
|-----------|------------|
| *"Load my data"* | *"Ingest Oracle CRM.CUSTOMERS into Bronze. PK: customer_id. Connection: abc123..."* |
| *"Clean the data"* | *"Transform bronze.dbo.customers to silver. Validate email format, quarantine bad records, rename cust_nm to customer_name"* |
| *"Make a report table"* | *"Aggregate silver.dbo.sales to gold.dbo.monthly_sales_summary. Group by customer_id, month. Sum sales_amount"* |

> **The one rule:** If Copilot would have to guess it, say it. If Copilot already knows it (metadata syntax, validator rules), skip it.

---

## Summary

This guide provides everything an LLM needs to generate accurate, complete, and compliant metadata table records for the Databricks Data Platform Solution Accelerator. Key principles:

1. **Understand the pattern** - Match user request to one of the common patterns
2. **Generate complete SQL in a new file** - Create a well-structured `.sql` file with header, comments, and all required configurations
3. **Validate rigorously** - Check against all rules before outputting, especially metadata reference compliance
4. **Use only valid attributes** - Never invent configuration names or attributes not in [METADATA_REFERENCE.md](METADATA_REFERENCE.md)
5. **Explain clearly** - Add comments and context for every configuration
6. **Ask questions** - Clarify ambiguities before generating
7. **Provide actionable deliverables** - Output a file that users can review, customize, and execute immediately

Remember: The metadata drives the entire data platform. Accuracy, completeness, and usability are critical.

---

## Reference: Canonical Metadata Reference

Use these files as the durable reference set when generating metadata:

1. [METADATA_REFERENCE.md](METADATA_REFERENCE.md) for valid configuration categories, names, attributes, and accepted values
2. [TRANSFORMATION_DECISION_TREE.md](TRANSFORMATION_DECISION_TREE.md) for escalation from built-ins to custom code
3. [CUSTOM_FUNCTION_SELECTION.md](CUSTOM_FUNCTION_SELECTION.md) for selecting the correct custom function type
4. [METADATA_AUTHORING_WORKFLOW.md](METADATA_AUTHORING_WORKFLOW.md) for the authoring and delivery procedure

**Always refer to this canonical reference set when generating metadata to ensure you're using valid, documented configurations.**
