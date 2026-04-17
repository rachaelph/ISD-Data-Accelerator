# Live Data Queries — Query Templates

> Part of the [Table Observability Skill](../SKILL.md). Load this file when the investigation sub-mode routes to an LD-* template.
>
> **🔴 IMPORTANT:** Before running any live data query, you MUST:
> 1. If the environment is not already known, ask the user which environment to query (scan `<git_folder>/datastores/` for available `datastore_{ENV}.Notebook` folders); otherwise reuse the provided environment
> 2. Follow the EDA-first rules in [live-querying.md](live-querying.md) — check cached profiling data before hitting the target endpoint
>
> **🔴 PREFERRED: Use `invoke_target_query.py` for all live data queries.** This deterministic helper handles endpoint resolution, token acquisition, and query execution in a single atomic call.
> It resolves investigation targets from datastore notebooks rather than workspace config.
>
> **Repo-specific helper contract:** Run the helper from the repo root with `--source-directory .` and set `--git-folder-name` to the environment folder that contains `datastores/` and `metadata/`, such as `dev`.

> **Fast lookup path:** When the user already supplied the table name and medallion layer, prefer `--table-name <name> --medallion-layer <layer>` instead of grepping metadata to find `Table_ID` first.
>
> **Schema visibility guardrail:** Results from the target SQL endpoint reflect what the SQL endpoint can expose. If the user asks about missing columns or unsupported Spark data types, pair the live query with `invoke_metadata_query.py --query-type SchemaDetails` and explain that the SQL result is the SQL-visible subset.
>
> ```powershell
> # Built-in query types:
> python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query-type Schema --pretty
> python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query-type RowCount --pretty
> python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query-type Sample --sample-size 20 --pretty
> python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-name oracle_customers --medallion-layer silver --query-type Sample --sample-size 20 --pretty
> python .github/skills/table-observability/scripts/invoke_table_snapshot.py --source-directory . --environment DEV --table-name oracle_customers --medallion-layer silver --sample-size 20 --pretty
>
> # Custom SELECT queries — use {Schema} and {Table} as placeholders:
> python .github/skills/table-observability/scripts/invoke_target_query.py --source-directory . --environment DEV --table-id {Table_ID} --query "SELECT ..." --pretty
> ```
>
> **Fallback (manual):** For fine-grained control, resolve the endpoint first then execute manually:
> ```powershell
> python .\automation_scripts\agents\resolve_datastore_endpoint.py --source-directory . --environment DEV --table-id {Table_ID} --pretty
> python .\automation_scripts\agents\resolve_datastore_endpoint.py --source-directory . --environment DEV --table-name oracle_customers --medallion-layer silver --pretty
> ```
>
> **All live queries execute against the TARGET data endpoint** (Databricks SQL Warehouse), NOT the metadata warehouse.
>
> **Read-only enforcement:** Only `SELECT` statements are permitted via live queries. `invoke_target_query.py` enforces this automatically. NEVER execute `INSERT`, `UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, `ALTER`, or any DDL/DML.

## LD-1: Table Summary

> **EDA-first:** Check the EDA decision tree before running this live. If fresh EDA data exists, use `Total_Rows`, `Total_Columns`, `Table_Last_Modified_Time`, and column details from EDA instead.

**Live variant (against target endpoint):**
```sql
-- Table row count
SELECT COUNT(*) AS Total_Rows FROM [dbo].[{Target_Entity}];
```

```sql
-- Column inventory with data types
SELECT
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE,
    ORDINAL_POSITION
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'dbo'
  AND TABLE_NAME = '{Target_Entity}'
ORDER BY ORDINAL_POSITION;
```
> **When to use:** User asks "how many rows in table X?", "how big is table X?", "what columns does table X have?", or "summarize table X"
>
> **Important:** `INFORMATION_SCHEMA.COLUMNS` returns the SQL-visible schema. It may not include Spark columns whose data types are not supported by the SQL endpoint.

## LD-2: Sample / Preview Rows

```sql
-- Preview top {N} rows from the actual table (default N=10)
SELECT TOP {N} *
FROM [dbo].[{Target_Entity}];
```
> **When to use:** User asks "show me data from table X", "preview table X", "what does the data look like?", or "sample rows from table X"
>
> **Notes:**
> - Default `N` = 10. User can specify a different number.
> - Databricks SQL Warehouse endpoints do not guarantee row ordering without `ORDER BY`. If the user needs deterministic results, add an `ORDER BY` on a key column (get from `Orchestration.Primary_Keys`).
> - For wide tables (>20 columns), consider selecting specific columns or using `Format-List` instead of `Format-Table`.
> - `SELECT *` shows the SQL-visible columns only. Do not describe it as proof that no additional Spark-only columns exist.
>
> **Speed rule:** If the user asks for sample data and already specified the environment and exact table layer, do not spend extra terminal calls rediscovering the workspace shape. Resolve `Table_ID` once and run the sample query immediately.
>
> **One-shot rule:** If the user asks for sample rows and column statistics together, prefer `invoke_table_snapshot.py` over separate metadata and target helper calls.
>
> **Ad hoc SQL rule:** If the user wants a custom `SELECT`, use `invoke_target_query.py --query` or `resolve_datastore_endpoint.py --table-name ... --medallion-layer ...` directly. Do not grep metadata just to discover a known table.

## LD-3: Column-Level Statistics (Live)

> **EDA-first:** If fresh EDA data exists with the requested column(s), present EDA stats. Only fall back to live if EDA is stale, missing, or user explicitly asks for "live"/"fresh" data.

**Single column:**
```sql
-- Live statistics for a specific column
SELECT
    COUNT(*) AS Total_Rows,
    COUNT([{column}]) AS Non_Null_Count,
    COUNT(*) - COUNT([{column}]) AS Null_Count,
    CAST((COUNT(*) - COUNT([{column}])) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)) AS Null_Percent,
    COUNT(DISTINCT [{column}]) AS Distinct_Values,
    MIN([{column}]) AS Min_Value,
    MAX([{column}]) AS Max_Value
FROM [dbo].[{Target_Entity}];
```

**Numeric column (add mean/stddev):**
```sql
-- Live statistics for a numeric column (INT, DECIMAL, FLOAT, BIGINT, etc.)
SELECT
    COUNT(*) AS Total_Rows,
    COUNT([{column}]) AS Non_Null_Count,
    COUNT(*) - COUNT([{column}]) AS Null_Count,
    CAST((COUNT(*) - COUNT([{column}])) * 100.0 / NULLIF(COUNT(*), 0) AS DECIMAL(10,2)) AS Null_Percent,
    COUNT(DISTINCT [{column}]) AS Distinct_Values,
    MIN([{column}]) AS Min_Value,
    MAX([{column}]) AS Max_Value,
    CAST(AVG(CAST([{column}] AS FLOAT)) AS DECIMAL(20,4)) AS Mean_Value,
    CAST(STDEV(CAST([{column}] AS FLOAT)) AS DECIMAL(20,4)) AS Std_Dev
FROM [dbo].[{Target_Entity}];
```

**All columns summary (for small-to-medium tables):**
```sql
-- Quick profile: null counts for all columns in one pass
SELECT
    COUNT(*) AS Total_Rows,
    {for_each_column}
    COUNT([{column}]) AS [{column}_non_null],
    COUNT(*) - COUNT([{column}]) AS [{column}_nulls]
    {end_for_each}
FROM [dbo].[{Target_Entity}];
```
> **When to use:** User asks "column stats for X", "how many nulls in column Y?", "what's the mean of column Z?", or "profile table X columns live"
>
> **⚠️ Performance warning:** For tables with >1M rows, running stats on ALL columns can be slow. Prefer querying specific columns unless the user explicitly asks for a full profile. Check `Total_Rows` from EDA or a quick `COUNT(*)` first.

## LD-4: Value Distribution (Top N Values)

```sql
-- Top {N} most frequent values for a column (default N=20)
SELECT TOP {N}
    [{column}] AS Value,
    COUNT(*) AS Frequency,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [dbo].[{Target_Entity}]) AS DECIMAL(10,2)) AS Percent_Of_Total
FROM [dbo].[{Target_Entity}]
GROUP BY [{column}]
ORDER BY COUNT(*) DESC;
```

**With filters:**
```sql
-- Value distribution with WHERE clause
SELECT TOP {N}
    [{column}] AS Value,
    COUNT(*) AS Frequency,
    CAST(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM [dbo].[{Target_Entity}] WHERE {filter}) AS DECIMAL(10,2)) AS Percent_Of_Total
FROM [dbo].[{Target_Entity}]
WHERE {filter}
GROUP BY [{column}]
ORDER BY COUNT(*) DESC;
```
> **When to use:** User asks "what values does column X have?", "show me the distribution", "top values for column X", "how are values spread across column X?", or "what's the most common value?"
>
> **⚠️ Performance warning:** For high-cardinality columns (>100K distinct values), this query can be heavy. Check `Approx_Distinct_Values` from EDA first. If cardinality is very high, warn the user and suggest adding a `WHERE` clause or reducing `N`.

## LD-5: Custom Ad-Hoc Query

> **🔴 Read-only enforcement:** Before executing, validate the query is SELECT-only. Reject if it contains any of: `INSERT`, `UPDATE`, `DELETE`, `DROP`, `TRUNCATE`, `ALTER`, `CREATE`, `EXEC`, `EXECUTE`, `MERGE`, `GRANT`, `REVOKE`, `DENY`.

```powershell
# Validate read-only — parse the user's query before execution
$userQuery = @"
{user_provided_query}
"@
$forbidden = @('INSERT','UPDATE','DELETE','DROP','TRUNCATE','ALTER','CREATE','EXEC ','EXECUTE ','MERGE','GRANT','REVOKE','DENY')
$upperQuery = $userQuery.ToUpper()
foreach ($keyword in $forbidden) {
    if ($upperQuery -match "\b$keyword\b") {
        throw "🔴 BLOCKED: Query contains '$keyword'. Only SELECT queries are permitted for live data investigation."
    }
}

# Execute the validated query
Invoke-Sqlcmd -ServerInstance $targetEndpoint -Database $targetDatabase `
    -AccessToken $sqlAccessToken -Query $userQuery -ErrorAction Stop
```
> **When to use:** User provides their own SQL query (e.g., "run `SELECT ... FROM ... WHERE ...`"), or asks a complex question that doesn't match any template.
>
> **Notes:**
> - Always show the user the exact query being executed for transparency
> - If the query references tables not in the target datastore, inform the user and suggest the correct datastore
> - Add `TOP 1000` if the user's query has no limit and the table is large

## LD-6: Data Freshness Comparison (EDA + Live)

> Uses **both** EDA and metadata to give a complete freshness picture without hitting the target endpoint.

```sql
-- Comprehensive freshness check: last pipeline run + last profiling + table modification time
SELECT
    o.Table_ID,
    o.Target_Entity,
    o.Target_Datastore,
    d.Medallion_Layer,
    -- Last pipeline run
    l.Ingestion_Status AS Last_Run_Status,
    l.Ingestion_End_Time AS Last_Run_Time,
    l.Records_Processed AS Last_Run_Records,
    DATEDIFF(HOUR, l.Ingestion_End_Time, GETUTCDATE()) AS Hours_Since_Last_Run,
    -- Last EDA profiling
    e.Data_Profile_Execution_Time AS Last_Profiled,
    DATEDIFF(HOUR, e.Data_Profile_Execution_Time, GETUTCDATE()) AS Hours_Since_Profiled,
    e.Total_Rows AS Profiled_Row_Count,
    e.Table_Last_Modified_Time AS Table_Last_Modified,
    DATEDIFF(HOUR, e.Table_Last_Modified_Time, GETUTCDATE()) AS Hours_Since_Table_Modified
FROM Data_Pipeline_Metadata_Orchestration o
JOIN Datastore_Configuration d
    ON LOWER(o.Target_Datastore) = LOWER(d.Datastore_Name)
LEFT JOIN (
    SELECT Table_ID, Ingestion_Status, Ingestion_End_Time, Records_Processed,
           ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Ingestion_End_Time DESC) AS rn
    FROM Data_Pipeline_Logs
    WHERE Processing_Phase = 'Batch'
) l ON o.Table_ID = l.Table_ID AND l.rn = 1
LEFT JOIN (
    SELECT Table_ID, Data_Profile_Execution_Time, Total_Rows, Table_Last_Modified_Time,
           ROW_NUMBER() OVER (PARTITION BY Table_ID ORDER BY Data_Profile_Execution_Time DESC) AS rn
    FROM Exploratory_Data_Analysis_Results
) e ON o.Table_ID = e.Table_ID AND e.rn = 1
WHERE o.Table_ID = {Table_ID};
```
> **When to use:** User asks "is the data fresh?", "when was this table last updated?", "how stale is the data?", or "when was the last run vs the last profile?"

## LD-7: Compare EDA Stats vs Live Stats

> **When to use:** User asks "are the profiling stats still accurate?" or "has the data changed since last profiling?" This runs BOTH EDA and live queries, then presents a side-by-side comparison.

**Workflow:**
1. Run the EDA Check Query to get cached stats
2. Run LD-1 (live row count) and LD-3 (live column stats) against the target endpoint
3. Present comparison:

```
📊 EDA vs Live Comparison
Table: {Table_Name} (Table_ID: {Table_ID})
EDA Profiled: {timestamp} ({N} hours ago)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
| Metric          | EDA (Cached) | Live (Now)  | Delta     |
|-----------------|--------------|-------------|-----------|
| Total Rows      | 1,234,567    | 1,235,100   | +533 ↑    |
| Column: order_id                                         |
|   Null %        | 0.00%        | 0.00%       | —         |
|   Distinct      | 1,234,567    | 1,235,100   | +533 ↑    |
|   Min           | 1            | 1           | —         |
|   Max           | 1234567      | 1235100     | +533 ↑    |
| ...
```
