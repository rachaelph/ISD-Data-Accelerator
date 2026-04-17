# Metadata SQL File Format and Error Prevention

> Last updated: 2026-04-15

> **How to structure, name, and validate metadata SQL files.** Covers the required file structure, INSERT formatting rules, common validator errors, and recovery procedures.
>
> **Related docs:**
> - [METADATA_GENERATION_GUIDE.md](METADATA_GENERATION_GUIDE.md) — core generation rules, golden rules, validation checklist
> - [METADATA_REFERENCE.md](METADATA_REFERENCE.md) — canonical attribute spec
> - [TRANSFORMATION_PATTERNS_REFERENCE.md](TRANSFORMATION_PATTERNS_REFERENCE.md) — SQL examples for each transformation type

## Table of Contents

- [SQL File Format and Structure](#sql-file-format-and-structure)
  - [File Location](#file-location)
  - [File Naming Convention](#file-naming-convention)
  - [Required File Header](#required-file-header)
  - [File Structure](#file-structure)
  - [INSERT Statement Formatting Rules](#insert-statement-formatting-rules)
  - [Complete Example File](#complete-example-file)
  - [Best Practices for File Content](#best-practices-for-file-content)
  - [Critical Rules When Editing Existing Metadata Files](#critical-rules-when-editing-existing-metadata-files)
  - [Recovery from Corrupted File Structure](#recovery-from-corrupted-file-structure)
- [Error Prevention](#error-prevention)
  - [Common Validator Errors and Fixes](#common-validator-errors-and-fixes)
  - [Common Mistakes to Avoid](#common-mistakes-to-avoid)

## SQL File Format and Structure

**Keywords:** SQL file, metadata file, file structure, file naming, INSERT format, SQL metadata, metadata folder, file header, file layout, formatting rules

When generating metadata records, always create a new `.sql` file with the following structure:

### Table naming — use UNQUALIFIED names

> **Rule:** Metadata tables in your `.sql` files are referenced by **bare table name only** — no `dbo.`, no `<catalog>.<schema>.` prefix.
>
> **Why:** `commit_pipeline.py` issues `USE CATALOG`/`USE SCHEMA` on the SQL warehouse session from `databricks_batch_engine/datastores/datastore_<ENV>.json` (`metadata.catalog` / `metadata.schema`) **before** executing each metadata SQL file. Bare names resolve automatically against those values, so the same file deploys correctly to every environment.

```sql
-- ✅ CORRECT - bare table name, environment-portable
INSERT INTO Data_Pipeline_Metadata_Orchestration ( ... ) VALUES ( ... );
DELETE FROM Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'MyTrigger');

-- ❌ WRONG - hardcoded schema; breaks across environments
INSERT INTO dbo.Data_Pipeline_Metadata_Orchestration ( ... ) VALUES ( ... );
```

The same principle applies to `Target_Entity` on orchestration rows: use the **bare table name** (e.g. `housing_price`). The framework qualifies it at runtime as `<catalog>.<schema>.<table>` from the `Datastore_Configuration` row matching `Target_Datastore`. A 2-part (`schema.table`) or 3-part (`catalog.schema.table`) form is also accepted if you need to override.

### File Location
**CRITICAL**: All metadata SQL files MUST be created as **plain `.sql` files** in the **metadata folder**.

**⚠️ Each trigger gets its own `.sql` file.** This is the standard format for metadata files.

**Finding the Metadata Folder:**
1. **First**: Search for a folder matching the pattern `**/metadata/*.sql` in the workspace.
2. **If not found**: Create the `metadata/` folder inside the batch engine folder — the folder that already contains `datastores/` and `custom_functions/` (in this repo, `databricks_batch_engine/metadata/`). Set `FDP_BATCH_ENGINE_FOLDER` if you have multiple engine folders.
3. **Never guess** — always verify the folder exists before file creation.

**Common folder patterns to search for:**
- `<engine_folder>/metadata/` (e.g. `databricks_batch_engine/metadata/`)
- Any folder named `metadata` that sits alongside `datastores/` and `custom_functions/` and contains `.sql` files.

**✅ CORRECT - Flat file structure:**
    - `<engine_folder>/metadata/metadata_SalesDataProduct.sql`
    - `<engine_folder>/metadata/metadata_OracleSales.sql`
    - `<engine_folder>/metadata/metadata_CustomerDimensions.sql`

### File Naming Convention
**Organize by Trigger Name** - Each trigger gets its own SQL file:
- `metadata_<TriggerName>.sql`
- Examples:
  - `metadata/metadata_SalesDataProduct.sql` (for Trigger_Name = 'SalesDataProduct')
  - `metadata/metadata_OracleSales.sql` (for Trigger_Name = 'OracleSales')
  - `metadata/metadata_CustomerETL.sql` (for Trigger_Name = 'CustomerETL')

**Benefits of file-based organization:**
- All Table_IDs for a trigger are in one file
- Easy to manage related data movements together
- Executed against Databricks SQL warehouse
- Simplifies CI/CD and version control
- Clear DELETE statements scope entire trigger

### Required File Header
Every SQL file should start with a header comment block:

```sql
-- =====================================================================
-- Metadata Configuration: [Brief Description]
-- Trigger Name: [TriggerName]
-- Generated: [Date]
-- =====================================================================
--
-- Purpose:
--   [Detailed description of what this configuration does]
--
-- Source: [Source system/table/file]
-- Target: [Target layer.schema.table]
--
-- Prerequisites:
--   - [Any required connections, GUIDs, or setup steps]
--   - [Any dependencies on other Table_IDs]
--
-- Notes:
--   - [Any special considerations or warnings]
--   - [Instructions for customization]
-- =====================================================================
```

### File Structure

> These are the canonical formatting rules for metadata SQL files. [METADATA_GENERATION_GUIDE.md - Golden Rules #13, #14, #17, #18](METADATA_GENERATION_GUIDE.md#golden-rules-for-llm-metadata-generation) reference this section.

**CRITICAL FORMATTING RULES:**
1. **DELETE statements first** - Remove existing records for this trigger using `WHERE Trigger_Name = '...'` with subquery. **NEVER hardcode Table_ID values in DELETE statements.**
2. **Multiple Table_IDs in single INSERTs** - Group all Table_IDs together
3. **1000 record maximum per INSERT** - Split into multiple INSERTs if needed
4. **Each row on a single line** - Essential for CI/CD diff tracking
5. **Order**: Orchestration → Primary Config → Advanced Config
6. **Comment Placement**: Keep `--` comments on dedicated lines positioned directly above the SQL they describe. Avoid end-of-line comments and ensure each comment captures the rationale behind the configuration so future maintainers understand the decision.
7. **🔴 CRITICAL - Group Configuration Categories Together**:
   - **Primary Configuration**: Group ALL configuration categories for each Table_ID together (source_details, watermark_details, target_details, column_cleansing, data_cleansing, other_settings). Do NOT scatter the same Table_ID's configs throughout the file.
   - **Advanced Configuration**: Group by Table_ID first, then by category within each Table_ID. For each Table_ID, ALL `data_transformation_steps` rows MUST come BEFORE ALL `data_quality` rows. Never intermix categories within a Table_ID.
   - **Correct order**: Table_ID 101 (transformations → DQ) → Table_ID 102 (transformations → DQ) → Table_ID 103 (transformations → DQ)
   - **Why this matters**: Grouped categories make files readable, maintainable, and enable proper CI/CD diff tracking. See [Error Prevention #11](#error-prevention) for examples.

**DELETE Statement Pattern (ALWAYS use this exact pattern):**
```sql
-- ❌ WRONG - Never hardcode Table_IDs
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration WHERE Table_ID IN (101, 102, 103);

-- ✅ CORRECT - Always use Trigger_Name with subquery
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'YourTriggerName');
```

**Why This Matters:**
- ✅ **Trigger-based DELETE** allows easy re-deployment of all tables in a trigger
- ✅ Automatically handles Table_IDs even if they change
- ✅ Supports CI/CD workflows where Table_IDs may vary between environments
- ❌ **Hardcoded Table_IDs** break when Table_IDs are reassigned or when deploying to different environments
- ❌ Requires manual tracking of which Table_IDs belong to which trigger

**Correct DELETE Statement Order:**
```sql
-- Delete in reverse dependency order (Advanced → Primary → Orchestration)
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'YourTriggerName');

DELETE FROM Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'YourTriggerName');

DELETE FROM Data_Pipeline_Metadata_Orchestration
WHERE Trigger_Name = 'YourTriggerName';
```

**Organize the SQL statements in this order:**

1. **Header Comment** (as shown above)
2. **DELETE Statements** - Remove existing records for this trigger from all tables
3. **Orchestration Metadata** - INSERT into `Data_Pipeline_Metadata_Orchestration`
4. **Primary Configuration** - INSERT into `Data_Pipeline_Metadata_Primary_Configuration`
5. **Advanced Configuration** (if needed) - INSERT into `Data_Pipeline_Metadata_Advanced_Configuration`

### INSERT Statement Formatting Rules

**✅ CORRECT - Each row on single line, multiple Table_IDs grouped:**
```sql
-- pipeline_stage_and_batch for external sources (Oracle), process for Delta tables (Silver)
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
('OracleSales', 1, 101, 'bronze', 'dbo.sales', 'sale_id', 'pipeline_stage_and_batch', 1),
('OracleSales', 2, 102, 'bronze', 'dbo.customers', 'customer_id', 'pipeline_stage_and_batch', 1),
('OracleSales', 3, 103, 'silver', 'dbo.sales_clean', 'sale_id', 'batch', 1);
```

**❌ INCORRECT - Multi-line rows:**
```sql
VALUES (
    'OracleSales',
    1,
    101,
    'bronze'
);
```

**✅ CORRECT - Split when exceeding 1000 records:**
```sql
-- First batch (records 1-1000)
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(101, 'source_details', 'source', 'oracle'),
(101, 'source_details', 'datastore_name', 'oracle_sales'),
-- ... up to 1000 total rows ...
(150, 'target_details', 'merge_type', 'merge');

-- Second batch (records 1001+)
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(151, 'source_details', 'source', 'sql_server'),
-- ... remaining rows ...
(200, 'target_details', 'merge_type', 'append');
```

### Complete Example File

> **⚠️ CRITICAL NOTICE: Comment Style**
>
> This example uses **ONLY single-line comments (`--`)** throughout the entire file.
>
> - ✅ **Every line starts with `--`** - even for file headers and documentation blocks
> - ❌ **DO NOT use `/* */` style comments** anywhere in your metadata SQL files
> - ❌ **DO NOT use `/*******/` for visual headers** - use `-- ====...====` instead
>
> **Why?** Multi-line comments (`/* */`) violate repository standards and can break CI/CD parsing, diff tracking, and automated metadata processing tools.

**File Location**: `<engine_folder>/metadata/metadata_OracleSales.sql`

```sql
-- =====================================================================
-- Metadata Configuration: Oracle Sales Data Ingestion Pipeline
-- Trigger Name: OracleSales
-- Generated: 2024-01-15
-- =====================================================================
--
-- Purpose:
--   Complete pipeline for Oracle sales data ingestion and transformation:
--   - Table_ID 101: Oracle SALES table → Bronze (incremental)
--   - Table_ID 102: Oracle CUSTOMERS table → Bronze (full load)
--   - Table_ID 103: Bronze sales → Silver (with transformations)
--
-- Source: Oracle Database - SALES_SCHEMA
-- Target: bronze.dbo.sales, bronze.dbo.customers, silver.dbo.sales_clean
--
-- Prerequisites:
--   - Ensure external source datastore is registered in Datastore_Configuration
--   - Ensure Oracle connection is configured
--   - Ensure Bronze and Silver datastores exist
--   - Create folder structure: bronze/Files/oracle/sales/, bronze/Files/oracle/customers/
--
-- Notes:
--   - Table_ID 101 uses watermark for incremental loading
--   - Table_ID 102 does full refresh each run (no watermark)
--   - Table_ID 103 includes data quality checks and transformations
--   - All records for this trigger can be modified/deleted together
-- =====================================================================

-- =====================================================================
-- STEP 1: DELETE existing records for this trigger
-- =====================================================================
-- Remove all existing metadata for 'OracleSales' trigger to allow clean re-deployment
DELETE FROM Data_Pipeline_Metadata_Advanced_Configuration WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'OracleSales');
DELETE FROM Data_Pipeline_Metadata_Primary_Configuration WHERE Table_ID IN (SELECT Table_ID FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'OracleSales');
DELETE FROM Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'OracleSales';

-- =====================================================================
-- STEP 2: Orchestration Metadata (Multiple Table_IDs)
-- =====================================================================
-- Each VALUES row MUST be on a single line for CI/CD diff tracking
-- pipeline_stage_and_batch for external sources (Oracle), process for Delta table sources (Silver reads from Bronze)
INSERT INTO Data_Pipeline_Metadata_Orchestration ([Trigger_Name],[Order_Of_Operations],[Table_ID],[Target_Datastore],[Target_Entity],[Primary_Keys],[Processing_Method],[Ingestion_Active])
VALUES
('OracleSales', 1, 101, 'bronze', 'dbo.sales', 'sale_id', 'pipeline_stage_and_batch', 1),
('OracleSales', 1, 102, 'bronze', 'dbo.customers', 'customer_id', 'pipeline_stage_and_batch', 1),
('OracleSales', 2, 103, 'silver', 'dbo.sales_clean', 'sale_id', 'batch', 1);

-- =====================================================================
-- STEP 3: Primary Configuration (All Table_IDs grouped together)
-- =====================================================================
INSERT INTO Data_Pipeline_Metadata_Primary_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
-- Table_ID 101: Oracle SALES table with incremental loading
(101, 'source_details', 'source', 'oracle'),
(101, 'source_details', 'datastore_name', 'oracle_sales'),
(101, 'source_details', 'schema_name', 'SALES_SCHEMA'),
(101, 'source_details', 'table_name', 'SALES'),
(101, 'source_details', 'query', 'SELECT * FROM SALES_SCHEMA.SALES WHERE MODIFIED_DATE > TIMESTAMP ''{WATERMARKVALUE}'' '),
(101, 'source_details', 'staging_volume_name', 'bronze'),
(101, 'source_details', 'staging_folder_path', 'oracle/sales/'),
(101, 'watermark_details', 'column_name', 'MODIFIED_DATE'),
(101, 'watermark_details', 'data_type', 'datetime'),
-- Table_ID 102: Oracle CUSTOMERS table (full load)
(102, 'source_details', 'source', 'oracle'),
(102, 'source_details', 'datastore_name', 'oracle_sales'),
(102, 'source_details', 'schema_name', 'SALES_SCHEMA'),
(102, 'source_details', 'table_name', 'CUSTOMERS'),
(102, 'source_details', 'query', 'SELECT * FROM SALES_SCHEMA.CUSTOMERS'),
(102, 'source_details', 'staging_volume_name', 'bronze'),
(102, 'source_details', 'staging_folder_path', 'oracle/customers/'),
(102, 'target_details', 'merge_type', 'overwrite'),
-- Table_ID 103: Bronze to Silver transformation
(103, 'source_details', 'table_name', 'bronze.dbo.sales'),
(103, 'target_details', 'merge_type', 'merge'),
(103, 'data_cleansing', 'replace_blank_with_null_in_string_columns', '*'),
(103, 'data_cleansing', 'trim_data_in_string_columns', '*');

-- =====================================================================
-- STEP 4: Advanced Configuration (Data Quality & Transformations)
-- =====================================================================
-- Table_ID 103: Transformations and data quality checks
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- Rename columns
(103, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'cust_id,prod_id'),
(103, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_id,product_id'),
-- Create derived column for total amount
(103, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'total_amount'),
(103, 'data_transformation_steps', 'derived_column', 2, 'expression', 'quantity * unit_price'),
-- Data quality: validate amounts are positive
(103, 'data_quality', 'validate_condition', 1, 'condition', 'total_amount > 0 AND quantity > 0'),
(103, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine');
```

### Best Practices for File Content

1. **File Location - ALWAYS SEARCH FIRST**: Use `file_search` tool to find `**/metadata/*.sql` pattern in the workspace. If not found, create the `metadata/` folder in the workspace git folder or ask the user. Never assume a path.
2. **Trigger-Based Organization**: One file per trigger name, containing all related Table_IDs
3. **DELETE Statements First**: Always include DELETE statements for the trigger at the top
4. **Group Multiple Table_IDs**: Combine all Table_IDs for a trigger in single INSERT statements

5. **Single INSERT per Metadata Table per Trigger**:
   - **Orchestration**: One INSERT statement with all Table_IDs for the trigger (multiple rows in VALUES)
   - **Primary Configuration**: One INSERT statement per trigger containing ALL configuration categories for all Table_IDs (do NOT split into separate INSERTs for each Table_ID or config)
   - **Advanced Configuration**: One INSERT statement per trigger containing ALL transformations and data quality checks for all Table_IDs (do NOT split into separate INSERTs for each Table_ID or transformation/check)
   - **Only split into multiple INSERTs if the total number of rows exceeds 1000.**

   > **Warning:** Creating multiple INSERT statements for Advanced Configuration in the same file (unless exceeding 1000 rows) will break CI/CD diff tracking and may cause deployment errors.

   **Example (CORRECT) - Group by Table_ID, then by category within each Table_ID:**
   ```sql
   INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (
       Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value
   )
   VALUES
       -- Table_ID 101: transformations first, then DQ
       (101, 'data_transformation_steps', 'select_columns', 1, 'column_name', 'sale_id,product_id,sale_date,amount'),
       (101, 'data_quality', 'validate_condition', 1, 'condition', 'amount > 0'),
       (101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
       -- Table_ID 102: transformations first, then DQ
       (102, 'data_transformation_steps', 'select_columns', 1, 'column_name', 'customer_id,customer_name'),
       (102, 'data_quality', 'validate_condition', 1, 'condition', 'customer_name IS NOT NULL'),
       (102, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine');
   ```

   **Example (INCORRECT) - Table_IDs scattered or categories intermixed within Table_ID:**
   ```sql
   -- WRONG: Table_ID 102 transformation appears after Table_ID 101 DQ
   INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (...)
   VALUES
       (101, 'data_transformation_steps', 'select_columns', 1, 'column_name', 'sale_id'),
       (102, 'data_transformation_steps', 'select_columns', 1, 'column_name', 'customer_id'),  -- ❌ Table_ID 102 before 101 is complete
       (101, 'data_quality', 'validate_condition', 1, 'condition', 'amount > 0')  -- ❌ Back to Table_ID 101
   ```

6. **1000 Record Limit**: Split INSERT statements when exceeding 1000 rows
7. **Single-Line Rows**: Each VALUES row must be on a single line (critical for CI/CD)
8. **Section Dividers**: Use `-- ====...====` to clearly separate sections
9. **Inline Comments**: Add `--` comments explaining each configuration value
10. **Action Items**: Mark any values that need user customization with `**ACTION REQUIRED**`
11. **Formatting Consistency**: Keep SQL readable with proper indentation and alignment
12. **Complete Solution**: Include all three metadata table inserts in one file for a complete configuration

**❌ ANTI-PATTERN - DO NOT DO THIS:**
```sql
-- DON'T split configurations into multiple INSERT statements per Table_ID
INSERT INTO Primary_Configuration VALUES
    (101, 'source_details', 'source', 'oracle'),
    (101, 'source_details', 'datastore_name', 'oracle_sales');

INSERT INTO Primary_Configuration VALUES  -- ❌ WRONG - separate INSERT
    (101, 'watermark_details', 'column_name', 'date_col');

INSERT INTO Primary_Configuration VALUES  -- ❌ WRONG - separate INSERT
    (101, 'target_details', 'merge_type', 'merge');
```

**✅ CORRECT PATTERN - DO THIS:**
```sql
-- Group ALL configurations for Table_ID 101 in ONE INSERT statement
INSERT INTO Primary_Configuration VALUES
    -- source_details configurations
    (101, 'source_details', 'source', 'oracle'),
    (101, 'source_details', 'datastore_name', 'oracle_sales'),
    (101, 'source_details', 'schema_name', 'SCHEMA'),
    (101, 'source_details', 'table_name', 'TABLE'),
    -- watermark_details configurations
    (101, 'watermark_details', 'column_name', 'date_col'),
    (101, 'watermark_details', 'data_type', 'datetime'),
    -- target_details configurations
    (101, 'target_details', 'merge_type', 'merge'),
    -- column_cleansing configurations
    (101, 'column_cleansing', 'trim', 'true'),
    (101, 'column_cleansing', 'apply_case', 'lower'),
    (101, 'column_cleansing', 'replace_non_alphanumeric_with_underscore', 'true');
```

**Benefits of This Format:**
- ✅ Easy CI/CD diff tracking (single-line rows show exactly what changed)
- ✅ Atomic deployments (all Table_IDs for a trigger in one file)
- ✅ Clean re-deployment (DELETE statements remove old config)
- ✅ Reduced file clutter (multiple Table_IDs in one file)
- ✅ Better version control (clear trigger-based organization)
- ✅ All configurations for a Table_ID are together, making it easier to understand and maintain

### Critical Rules When Editing Existing Metadata Files

**WHEN USER ASKS TO ADD OR UPDATE METADATA:**

1. **Read the ENTIRE file first** to understand current structure
2. **Identify which section** the change belongs to (Orchestration/Primary/Advanced)
3. **⚠️ NEVER DELETE UNRELATED METADATA** - Only modify/delete rows explicitly mentioned in the user's request. If user asks to change Table 2, DO NOT touch Table 1's metadata rows.
4. **Add rows to the existing INSERT statement** for that section
5. **Semicolon placement when editing** - Each INSERT statement must end with a semicolon (`;`) on the last VALUES row. When adding rows to an existing INSERT statement, you MUST:
   - Remove the semicolon from the current last row and replace it with a comma (`,`)
   - Add your new rows
   - End the **new** last row with a semicolon (`;`)
   - **Common mistake: forgetting to move the semicolon from the old last row to the new last row**
6. **PRESERVE ALL EXISTING ROWS** - When editing an INSERT statement, keep ALL existing rows intact and only add/modify the specific rows mentioned by the user
7. **DO NOT create new INSERT statements** unless the existing one has 1000+ rows
8. **Verify the final structure** matches the 5-section order: Header → DELETEs → Orchestration → Primary → Advanced
9. If file structure is corrupted, reconstruct the ENTIRE file with correct structure INCLUDING ALL EXISTING TABLE_IDs

**Example: User asks "Add a filter to remove customer_id values that start with 1"**

**✅ CORRECT Process:**
1. Read the entire file to locate the Advanced Configuration section
2. Find the existing `INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration` statement
3. **Replace the semicolon on the last existing row with a comma** (`,`)
4. Add new rows to the VALUES clause of that existing INSERT
5. End the new last row with a semicolon (`;`)
6. Verify the structure remains: Header → DELETEs → Orchestration → Primary → Advanced

```sql
-- CORRECT: Add to existing INSERT statement
-- BEFORE EDIT (existing file ends with last row):
-- VALUES
--     (103, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'cust_id'),
--     (103, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_id');
--                                                                                                      ^
--                                                                          LAST ROW ENDS WITH SEMICOLON

-- AFTER EDIT (comma added to last row, new rows added):
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
    (103, 'data_transformation_steps', 'columns_to_rename', 1, 'existing_column_name', 'cust_id'),
    (103, 'data_transformation_steps', 'columns_to_rename', 1, 'new_column_name', 'customer_id'),
    -- ↑ SEMICOLON REPLACED WITH COMMA ON OLD LAST ROW ↑
    -- NEW ROWS ADDED BELOW
    (103, 'data_transformation_steps', 'filter_data', 2, 'filter_logic', 'NOT (customer_id LIKE ''1%'')');
    -- ↑ NEW LAST ROW ENDS WITH SEMICOLON ↑
```

**❌ INCORRECT Process - DO NOT DO THIS:**
```sql
-- WRONG: Creating a separate INSERT statement
INSERT INTO Data_Pipeline_Metadata_Advanced_Configuration (...)
VALUES
    (103, 'data_transformation_steps', 'filter_data', 2, 'filter_logic', 'NOT (customer_id LIKE ''1%'')');
```

**❌ WRONG: Placing rows outside of INSERT statement:**
```sql
-- WRONG: Standalone VALUES rows without INSERT INTO
(103, 'data_transformation_steps', 'filter_data', 2, 'filter_logic', 'NOT (customer_id LIKE ''1%'')');
```

**❌ WRONG: Placing Advanced Config before Primary Config:**
```sql
-- File structure becomes: Header → DELETEs → Orchestration → Advanced → Primary
-- This breaks the required order!
```

### Recovery from Corrupted File Structure

**If a file structure becomes corrupted (sections out of order, multiple INSERT statements, etc.):**

1. **Read the entire file** to understand what content exists
2. **Extract all the data rows** from each section
3. **Reconstruct the file** using `replace_string_in_file` with the correct structure:
   - Header comment block
   - DELETE statements (Advanced → Primary → Orchestration)
   - Orchestration INSERT with all rows
   - Primary Configuration INSERT with all rows
   - Advanced Configuration INSERT with all rows (if any)
4. **Never use incremental patches** on corrupted files - always do a full reconstruction

**Example Recovery:**
```sql
-- If you find the file has sections out of order or multiple INSERT statements:
-- 1. Identify all Table_IDs and their configurations
-- 2. Rewrite the entire file in the correct order
-- 3. Ensure each section has ONE INSERT statement (unless > 1000 rows)
```

---

## Error Prevention

**Keywords:** validator errors, validation, common mistakes, error fix, corrupted file, recovery, validate_metadata_sql, deployment error, INSERT error, syntax error

### Common Validator Errors and Fixes

The metadata validator (`validate_metadata_sql.py`) catches many errors before deployment. Here are the most common errors and how to fix them:

| Error Message | Cause | Fix |
|---------------|-------|-----|
| `Invalid value 'delta' for 'source'` | Using `source` attribute for table reads | Remove `source`, use only `table_name` with 3-part naming |
| `Invalid attribute 'datastore'` | Wrong attribute name for file sources | Use `datastore_name` (not `datastore`) for files, or remove entirely for Delta |
| `Invalid attribute 'source_entity'` | Invented/hallucinated attribute | Use `table_name` for Unity Catalog sources, not `source_entity` |
| `Invalid value 'upsert' for 'merge_type'` | Wrong merge type value | Use `'merge'` (not `'upsert'`) |
| `Did you mean: datastore_name?` | Typo in attribute name | Check spelling - validator suggests correct name |
| `Invalid attribute 'connection_id'` | Using deprecated `connection_id` attribute | Use `datastore_name` instead — register the connection in `Datastore_Configuration` and reference it by name. Unity Catalog tables don't need either. |

> 💡 **Pro tip**: Always run the validator before delivering metadata:
> ```powershell
> python .github/skills/metadata-validation/validate_metadata_sql.py <file>
> ```

### Common Mistakes to Avoid:

1. **❌ Hardcoding Table_IDs in DELETE statements**
   - **NEVER** use `WHERE Table_ID IN (101, 102, 103)` in DELETE statements
   - **ALWAYS** use `WHERE Table_ID IN (SELECT Table_ID FROM ... WHERE Trigger_Name = '...')`
   - Reason: Table_IDs may change between environments or during re-deployment

2. **❌ Forgetting required fields**
   - Always check source-specific requirements
   - Azure SQL/SQL Server MUST have database_name

3. **❌ Wrong accepted values**
   - `merge_type` must be exact: `'merge'` not `'upsert'`
   - Boolean must be lowercase: `'true'` not `'True'`

4. **❌ Inconsistent Table_ID**
   - Table_ID must match across all three tables

5. **❌ Invalid join syntax**
   - Must use `a.` for left table, `b.` for right table
   - Example: `'a.customer_id = b.customer_id'`

6. **❌ Missing watermark config**
   - If using `{WATERMARKVALUE}` in query, MUST add watermark_details

7. **❌ Wrong naming conventions**
   - Table names: 3-part `datastore.schema.table`
   - File paths: relative to staging volume, no leading slash

8. **❌ Using `filter_data` transformation for data quality validation**
   - **`data_transformation_steps` → `filter_data`**: Silently **removes** rows that don't match the filter condition. The filtered rows are permanently dropped with no record or audit trail. Use this for **business logic filtering** where you intentionally want to exclude data from processing (e.g., "only orders from 2024", "exclude test accounts", "filter to specific regions").
   - **`data_quality` → `validate_condition`**: **Validates** data against a condition and takes a configurable action (`quarantine`, `warn`, `fail`) for non-compliant records. Use this when you want to **track, review, or alert** on data that doesn't meet quality standards.

   **When to use which:**
   | Scenario | Use | Category |
   |----------|-----|----------|
   | Filter to specific date range (business scope) | `filter_data` | `data_transformation_steps` |
   | Filter to specific status values (business logic) | `filter_data` | `data_transformation_steps` |
   | Validate that amounts are positive (data integrity) | `validate_condition` | `data_quality` |
   | Check for NULL in required fields (data integrity) | `validate_condition` | `data_quality` |
   | Validate foreign key references exist | `validate_referential_integrity` | `data_quality` |
   | Validate email/phone format patterns | `validate_pattern` | `data_quality` |

   **Example - WRONG (data quality check using transformation):**
   ```sql
   -- ❌ This silently drops records - no way to review what was removed
   (101, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', 'order_total > 0')
   ```

   **Example - CORRECT (data quality check with quarantine):**
   ```sql
   -- ✅ Non-compliant records go to quarantine table for review
   (101, 'data_quality', 'validate_condition', 1, 'condition', 'order_total > 0'),
   (101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine')
   ```

9. **❌ Combining multiple DQ conditions with AND when granular tracking is needed**
    - **WRONG**: Combining unrelated validations loses root cause visibility
    ```sql
    -- ❌ If this fails, you won't know if customer_id, product_id, or order_id was NULL
    (101, 'data_quality', 'validate_condition', 1, 'condition', 'customer_id IS NOT NULL AND product_id IS NOT NULL AND order_id IS NOT NULL'),
    (101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine')
    ```
    - **CORRECT**: Split into individual checks for granular tracking
    ```sql
    -- ✅ Each failure is tracked separately in quarantine table
    (101, 'data_quality', 'validate_condition', 1, 'condition', 'customer_id IS NOT NULL'),
    (101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
    (101, 'data_quality', 'validate_condition', 2, 'condition', 'product_id IS NOT NULL'),
    (101, 'data_quality', 'validate_condition', 2, 'if_not_compliant', 'quarantine'),
    (101, 'data_quality', 'validate_condition', 3, 'condition', 'order_id IS NOT NULL'),
    (101, 'data_quality', 'validate_condition', 3, 'if_not_compliant', 'quarantine')
    ```
    - **Rule of thumb**: When converting from PySpark, each `.filter()` call = one `validate_condition` instance

10. **❌ Intermixing `data_transformation_steps` and `data_quality` categories in Advanced Configuration**
    - **WRONG**: Placing `data_quality` rows between `data_transformation_steps` rows within a Table_ID, or scattering Table_IDs throughout the file
    ```sql
    -- ❌ Categories are intermixed within Table_ID - hard to read and maintain
    (101, 'data_transformation_steps', 'join_data', 1, 'join_type', 'left'),
    (101, 'data_transformation_steps', 'join_data', 1, 'right_table_name', 'silver.dbo.customers'),
    (101, 'data_quality', 'validate_condition', 1, 'condition', 'customer_id IS NOT NULL'),  -- ❌ DQ in middle of transformations
    (101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
    (101, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'total'),  -- ❌ More transformations after DQ
    (101, 'data_transformation_steps', 'derived_column', 2, 'expression', 'quantity * price')
    ```
    - **CORRECT**: Group by Table_ID first, then within each Table_ID: ALL transformations first, then ALL data quality checks
    ```sql
    -- ✅ Table_ID 101: All transformations grouped together, then all DQ
    (101, 'data_transformation_steps', 'join_data', 1, 'join_type', 'left'),
    (101, 'data_transformation_steps', 'join_data', 1, 'right_table_name', 'silver.dbo.customers'),
    (101, 'data_transformation_steps', 'derived_column', 2, 'column_name', 'total'),
    (101, 'data_transformation_steps', 'derived_column', 2, 'expression', 'quantity * price'),
    (101, 'data_quality', 'validate_condition', 1, 'condition', 'customer_id IS NOT NULL'),
    (101, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine'),
    -- ✅ Table_ID 102: Same pattern - all transformations, then all DQ
    (102, 'data_transformation_steps', 'select_columns', 1, 'column_name', 'customer_id,customer_name'),
    (102, 'data_quality', 'validate_condition', 1, 'condition', 'customer_name IS NOT NULL'),
    (102, 'data_quality', 'validate_condition', 1, 'if_not_compliant', 'quarantine')
    ```
    - **Why this matters**:
      - The framework processes transformations first, then data quality checks
      - Grouping by Table_ID keeps related configurations together for easier maintenance
      - Instance numbers within each category define execution order within that category

11. **❌ Using complex subqueries or EXISTS in `attach_dimension_surrogate_key` join logic**
    - **The `dimension_table_join_logic` attribute can ONLY reference columns from two tables**: `a.` (current dataset) and `b.` (the dimension table being joined)
    - Complex expressions like `EXISTS`, `IN`, subqueries, or references to other tables cause `Py4JJavaError: java.lang.ClassCastException` at runtime
    - **WRONG**: Trying to validate snowflake relationships from fact table
    ```sql
    -- ❌ This causes Py4JJavaError: java.lang.ClassCastException
    -- Cannot reference 'ds' table or use subqueries - only 'a.' and 'b.' are valid
    (304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_join_logic', 'a.season_id IS NOT NULL AND EXISTS (SELECT 1 FROM gold.dbo.dim_season ds WHERE ds.id = a.season_id AND ds.tv_show_id = b.id)')
    ```
    - **CORRECT for Snowflake Schema**: The fact table joins ONLY to its direct dimension (`dim_season`). The relationship to `dim_tv_show` exists within `dim_season` itself (snowflake pattern).

    **Fact Table Configuration** (joins only to immediate dimension):
    ```sql
    -- ✅ Fact table gets season_key - that's the only surrogate key it needs
    -- The snowflake relationship (season → tv_show) is handled in dim_season, not the fact
    (304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_name', 'gold.dbo.dim_season'),
    (304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_join_logic', 'a.season_id = b.id'),
    (304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_key_column_name', 'season_key'),
    (304, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_key_output_column_name', 'season_sk')
    ```

    **Dimension Table Configuration** (`dim_season` gets the FK to its parent `dim_tv_show`):
    ```sql
    -- ✅ dim_season gets tv_show_key surrogate key pointing to its parent dimension
    -- This is configured in the Table_ID that builds dim_season, NOT the fact table
    (305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_name', 'gold.dbo.dim_tv_show'),
    (305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_join_logic', 'a.tv_show_id = b.id'),
    (305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_table_key_column_name', 'tv_show_key'),
    (305, 'data_transformation_steps', 'attach_dimension_surrogate_key', 1, 'dimension_key_output_column_name', 'tv_show_sk')
    ```

    - **Key principle**: In a snowflake schema, each table only references its immediate parent. The fact table references `dim_season`, and `dim_season` references `dim_tv_show`. Do NOT denormalize parent dimension keys into the fact table.

12. **❌ Duplicating transformation instances when comma-separated column names would work**
    - Many transformations accept comma-separated column names in a single `column_name` attribute
    - Creating separate instances for each column bloats metadata and increases maintenance burden
    - **WRONG**: Separate instances for columns with identical settings
    ```sql
    -- ❌ Unnecessary duplication - uses 2 instance numbers and twice the rows
    (101, 'data_transformation_steps', 'remove_columns', 5, 'column_name', 'temp_col1'),
    (101, 'data_transformation_steps', 'remove_columns', 6, 'column_name', 'temp_col2')
    ```
    - **CORRECT**: Single instance with comma-separated columns
    ```sql
    -- ✅ One instance handles multiple columns
    (101, 'data_transformation_steps', 'remove_columns', 5, 'column_name', 'temp_col1,temp_col2')
    ```
    - **Applies to**: `mask_sensitive_data`, `columns_to_rename`, `remove_columns`, `select_columns`, `change_data_types`, `apply_null_handling`, `replace_values`, `add_row_hash`, `drop_duplicates`, `aggregate_data`, and others
    - **Exception**: Use separate instances only when columns need **different** transformation settings

13. **❌ Ignoring Oracle `ParquetInvalidDecimalPrecisionScale` runtime errors**
    - Symptom: Copy from Oracle fails with decimal precision/scale errors such as `Precision: 38 Scale: 127`
    - Most common cause: Oracle `NUMBER` metadata with unspecified precision/scale can surface as invalid Parquet decimal scale
    - **Preferred fix**: Explicitly `CAST` affected Oracle `NUMBER` columns in `source_details.query`
    - **Operational fallback**: Route to CSV copy pattern and convert staged CSV to Parquet:
        - Add `(Table_ID, 'source_details', 'copy_pattern_suffix', 'csv')`
        - Add `(Table_ID, 'source_details', 'convert_csv_to_parquet', 'true')`
    - **Decision rule**: Use one strategy per Table_ID (CAST **or** CSV fallback), not both together
    - Keep `source_details.source = 'oracle'` (do not invent source types like `oracle_csv` in metadata)

15. **❌ Unbalanced single quotes in SQL string values**
    - SQL strings use single quotes as delimiters, and internal quotes must be escaped by doubling them (`''`)
    - A common mistake is forgetting to properly close strings that contain embedded quotes
    - **WRONG**: Unbalanced quotes - string ends prematurely
    ```sql
    -- ❌ The string 'bs_yn = ''Y' has 3 internal quotes - unbalanced!
    -- SQL parser sees: 'bs_yn = ' as the value, then 'Y' as a syntax error
    (101, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', 'bs_yn = ''Y'')
    ```
    - **CORRECT**: Properly escaped quotes - total internal quotes must be even
    ```sql
    -- ✅ The string 'bs_yn = ''Y''' has 4 internal quotes (2 escaped quotes)
    -- This represents the value: bs_yn = 'Y'
    (101, 'data_transformation_steps', 'filter_data', 1, 'filter_logic', 'bs_yn = ''Y''')
    ```
    - **Rule**: Count single quotes inside your string value - must be an even number (each `''` = one literal quote)
    - **Common patterns**:
      | Desired Value | SQL String |
      |---------------|------------|
      | `status = 'A'` | `'status = ''A'''` |
      | `name LIKE '%O''Brien%'` | `'name LIKE ''%O''''Brien%'''` |
      | `flag = 'Y'` | `'flag = ''Y'''` |
