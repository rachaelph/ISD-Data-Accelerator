# Metadata Configuration — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to an MC-* template.

## MC-1: Full Configuration for a Table

> **Progressive execution:** Run each part as a **separate terminal call**. Present results to the user after each part before proceeding to the next. These queries are independent — they all filter by `Table_ID` and do not depend on each other's output.
>
> **When to use:** User asks "how is table X configured?" or "show me the metadata for table X" or "what's the source for this table?"

### MC-1a: Orchestration (what, where, order)

```sql
-- Part A: Orchestration for Table_ID = {Table_ID}
SELECT
    Trigger_Name,
    Order_Of_Operations,
    Table_ID,
    Target_Datastore,
    Target_Entity,
    Primary_Keys,
    Processing_Method,
    Ingestion_Active
FROM dbo.Data_Pipeline_Metadata_Orchestration
WHERE Table_ID = {Table_ID};
```
> Present orchestration context: trigger, target, processing method, active status.

### MC-1b: Primary Configuration (key-value pairs)

```sql
-- Part B: Primary configuration for Table_ID = {Table_ID}
SELECT
    Configuration_Category,
    Configuration_Name,
    Configuration_Value
FROM dbo.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID = {Table_ID}
ORDER BY Configuration_Category, Configuration_Name;
```
> Present source details, target details, watermark config, etc.

### MC-1c: Advanced Configuration (transformations, DQ rules)

```sql
-- Part C: Advanced configuration for Table_ID = {Table_ID}
SELECT
    Configuration_Category,
    Configuration_Name,
    Configuration_Name_Instance_Number,
    Configuration_Attribute_Name,
    Configuration_Attribute_Value
FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID = {Table_ID}
ORDER BY Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name;
```
> Present transformation steps, DQ rules, and other advanced config. After all 3 parts, summarize the complete configuration.

## MC-2: All Tables in a Trigger

```sql
-- All tables configured under Trigger_Name = '{Trigger_Name}'
SELECT
    o.Table_ID,
    o.Target_Entity,
    o.Target_Datastore,
    o.Order_Of_Operations,
    o.Primary_Keys,
    o.Processing_Method,
    o.Ingestion_Active,
    pc_source.Configuration_Value AS Source_Table,
    pc_merge.Configuration_Value AS Merge_Type
FROM dbo.Data_Pipeline_Metadata_Orchestration o
LEFT JOIN dbo.Data_Pipeline_Metadata_Primary_Configuration pc_source
    ON o.Table_ID = pc_source.Table_ID
    AND pc_source.Configuration_Category = 'source_details'
    AND pc_source.Configuration_Name = 'table_name'
LEFT JOIN dbo.Data_Pipeline_Metadata_Primary_Configuration pc_merge
    ON o.Table_ID = pc_merge.Table_ID
    AND pc_merge.Configuration_Category = 'target_details'
    AND pc_merge.Configuration_Name = 'merge_type'
WHERE o.Trigger_Name = '{Trigger_Name}'
ORDER BY o.Order_Of_Operations, o.Table_ID;
```
> **When to use:** User asks "what tables are in trigger X?" or "show me the pipeline for trigger X"

## MC-3: Source Details for a Table

```sql
-- Source configuration for Table_ID = {Table_ID}
SELECT
    Configuration_Name,
    Configuration_Value
FROM dbo.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID = {Table_ID}
  AND Configuration_Category = 'source_details'
ORDER BY Configuration_Name;
```
> **When to use:** User asks "where does this table pull data from?" or "what's the source?"

## MC-4: Target/Write Details for a Table

```sql
-- Target configuration for Table_ID = {Table_ID}
SELECT
    Configuration_Name,
    Configuration_Value
FROM dbo.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID = {Table_ID}
  AND Configuration_Category = 'target_details'
ORDER BY Configuration_Name;
```
> **When to use:** User asks "how does this table get written?" or "what merge type is used?"

## MC-5: Data Quality Rules Configured for a Table

```sql
-- DQ rules configured for Table_ID = {Table_ID}
SELECT
    Configuration_Name AS DQ_Check_Type,
    Configuration_Name_Instance_Number AS Rule_Number,
    Configuration_Attribute_Name,
    Configuration_Attribute_Value
FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID = {Table_ID}
  AND Configuration_Category = 'data_quality'
ORDER BY Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name;
```
> **When to use:** User asks "what DQ rules are configured?" or "how is data quality enforced for this table?"
>
> **🔴 CRITICAL — Built-in DQ Checks (run automatically, NOT in Advanced_Configuration):**
> The MC-5 query above only returns **user-configured** DQ rules from metadata. The accelerator also runs these checks **automatically on every pipeline run** — they require NO configuration and will NOT appear in MC-5 results:
>
> | Built-in Check | What It Does | Default Action | Configurable Via |
> |---------------|-------------|----------------|------------------|
> | **Duplicate Primary Keys** | Detects duplicate values in the columns defined in `Orchestration.Primary_Keys` | `fail` (stops pipeline) | `if_duplicate_primary_keys` in `target_details` (`warn`, `quarantine`, `fail`) |
> | **Schema Change Detection** | Detects added/removed/type-changed columns vs. previously logged schema | Logs to `Schema_Logs` + `Schema_Changes` | Always runs; optionally block with `fail_on_new_schema = 'true'` in `target_details` |
> | **Schema Contract Validation** | Validates source DataFrame matches expected column names/types | `fail` (stops pipeline) | Only runs when `schema` is defined in `source_details` |
>
> **When recommending new DQ checks:** Do NOT recommend `validate_not_null` or `validate_unique` on the primary key column(s) — the built-in Duplicate Primary Keys check already handles PK integrity (null PKs and duplicate PKs). You can verify this by checking `Data_Quality_Notifications` for `Data_Quality_Category = 'Duplicate Primary Keys'` entries. If the user wants to change the PK check behavior (e.g., quarantine duplicates instead of failing), the correct approach is to set `if_duplicate_primary_keys` in `target_details` — NOT to add a separate `validate_unique` or `validate_not_null` rule.

## MC-6: Transformation Steps for a Table

```sql
-- Transformation pipeline for Table_ID = {Table_ID}
SELECT
    Configuration_Name AS Transformation_Type,
    Configuration_Name_Instance_Number AS Step_Number,
    Configuration_Attribute_Name,
    Configuration_Attribute_Value
FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration
WHERE Table_ID = {Table_ID}
  AND Configuration_Category = 'data_transformation_steps'
ORDER BY Configuration_Name_Instance_Number, Configuration_Attribute_Name;
```
> **When to use:** User asks "what transformations are applied?" or "how is this table transformed?"

## MC-7: Watermark / Incremental Load Configuration

```sql
-- Watermark configuration for Table_ID = {Table_ID}
SELECT
    Configuration_Name,
    Configuration_Value
FROM dbo.Data_Pipeline_Metadata_Primary_Configuration
WHERE Table_ID = {Table_ID}
  AND Configuration_Category = 'watermark_details'
ORDER BY Configuration_Name;
```
> **When to use:** User asks "is this table loaded incrementally?" or "what's the watermark setup?"

## MC-8: Disabled / Inactive Tables

```sql
-- All disabled tables (Ingestion_Active = 0)
SELECT
    Trigger_Name,
    Table_ID,
    Target_Entity,
    Target_Datastore,
    Order_Of_Operations
FROM dbo.Data_Pipeline_Metadata_Orchestration
WHERE Ingestion_Active = 0
ORDER BY Trigger_Name, Order_Of_Operations;
```
> **When to use:** User asks "which tables are disabled?" or "what's not running?"

## MC-9: Datastore Connection Details

```sql
-- Connection details for a datastore
SELECT
    Datastore_Name,
    Datastore_Type,
    Datastore_ID,
    Workspace_ID,
    Workspace_Name,
    Medallion_Layer,
    Endpoint,
    Connection_ID
FROM dbo.Datastore_Configuration
WHERE Datastore_Name = '{Datastore_Name}';
```
> **When to use:** User asks "what datastore does this table write to?" or "show me the connection details"

## MC-10: Config + Last Run Combined (Metadata ↔ Logging)

```sql
-- Metadata config + latest run status for Table_ID = {Table_ID}
SELECT
    o.Trigger_Name,
    o.Table_ID,
    o.Target_Entity,
    o.Target_Datastore,
    o.Ingestion_Active,
    l.Ingestion_Status AS Last_Status,
    l.Records_Processed AS Last_Records,
    l.Ingestion_End_Time AS Last_Run_Time,
    l.Watermark_Value AS Current_Watermark
FROM dbo.Data_Pipeline_Metadata_Orchestration o
LEFT JOIN (
    SELECT Table_ID, Ingestion_Status, Records_Processed, Ingestion_End_Time, Watermark_Value,
           ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn
    FROM dbo.Data_Pipeline_Logs
    WHERE Processing_Phase = 'Batch'
) l ON o.Table_ID = l.Table_ID AND l.rn = 1
WHERE o.Table_ID = {Table_ID};
```
> **When to use:** User asks "what's the config AND status for table X?" — combines metadata configuration with latest execution result.

## MC-11: All Tables Overview (Config + Last Run)

```sql
-- Overview of all active tables with their last run status
SELECT
    o.Trigger_Name,
    o.Table_ID,
    o.Target_Entity,
    o.Target_Datastore,
    o.Order_Of_Operations,
    o.Ingestion_Active,
    l.Ingestion_Status AS Last_Status,
    l.Records_Processed AS Last_Records,
    l.Ingestion_End_Time AS Last_Run_Time
FROM dbo.Data_Pipeline_Metadata_Orchestration o
LEFT JOIN (
    SELECT Table_ID, Ingestion_Status, Records_Processed, Ingestion_End_Time,
           ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn
    FROM dbo.Data_Pipeline_Logs
    WHERE Processing_Phase = 'Batch'
) l ON o.Table_ID = l.Table_ID AND l.rn = 1
ORDER BY o.Trigger_Name, o.Order_Of_Operations;
```
> **When to use:** User asks "show me all tables" or "give me an overview of everything"

## MC-12: Metadata Deployment History

```sql
-- Which metadata files have been deployed and when?
SELECT
    File_Name,
    Last_Modified
FROM dbo.Metadata_Files
ORDER BY Last_Modified DESC;
```
> **When to use:** User asks "when was the metadata last deployed?" or "which metadata files have been pushed?"
